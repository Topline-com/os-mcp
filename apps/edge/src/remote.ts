/// <reference types="@cloudflare/workers-types" />

// Cloudflare Worker entry point — hosts a remote MCP server that:
//  - Speaks MCP over HTTP (JSON-RPC POST /mcp)
//  - Implements OAuth 2.1 so claude.ai's custom-connector UI can connect
//  - Accepts raw Authorization: Bearer <PIT> as a fallback for direct clients
//    (mcp-inspector, curl, etc.)
//
// Auth backend:
//  - Customer credentials (PIT + Location ID) are stored encrypted in the
//    CONNECTIONS KV namespace, keyed by a UUID (connection_id / cid).
//  - Access tokens issued by OAuth and /connect are HMAC-signed envelopes
//    containing only { cid, exp } — no plaintext PIT leaves the worker.
//  - Legacy tokens that embed { pit, locationId, exp } are still accepted
//    so deploys don't break existing Claude / ChatGPT sessions. They keep
//    working until they expire naturally.
//
// Secrets: TOKEN_SIGNING_SECRET doubles as the KEK for PIT encryption via
// HKDF. Rotating it invalidates every token AND every encrypted PIT in one
// step.

import { ALL_TOOLS, toolsByName } from "./registry.js";
import { credentialsContext, ToplineApiError } from "@topline/shared";
import {
  signToken,
  verifyToken,
  verifyPkce,
  isCidAccess,
  isLegacyAccess,
  createConnection,
  loadAndDecryptConnection,
  touchConnection,
  type AuthCodePayload,
  type AccessTokenPayload,
  type LegacyAccessTokenPayload,
} from "@topline/shared-auth";
import {
  authorizeFormHtml,
  connectFormHtml,
  connectResultHtml,
} from "./remote-oauth.js";
import { LocationDO } from "@topline/shared-do";

// Re-export the DO class so wrangler can bind it to this Worker script.
// The class implementation lives in packages/shared-do so the (future)
// sync worker can import the same type surface without circular deps.
export { LocationDO };

interface Env {
  TOKEN_SIGNING_SECRET: string;
  TOPLINE_BRAND_NAME?: string;
  CONNECTIONS: KVNamespace;
  LOCATION_DO: DurableObjectNamespace<LocationDO>;
  ADMIN_TOKEN?: string;
}

const PROTOCOL_VERSION = "2024-11-05";
const ACCESS_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 30; // 30 days (OAuth flow)
const SELFSERVE_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 365; // 1 year (/connect)
const AUTH_CODE_TTL_SECONDS = 60 * 10; // 10 minutes

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (!env.TOKEN_SIGNING_SECRET) {
      return plain(500, "Worker is missing TOKEN_SIGNING_SECRET. Run: wrangler secret put TOKEN_SIGNING_SECRET");
    }
    if (!env.CONNECTIONS) {
      return plain(500, "Worker is missing CONNECTIONS KV binding. Check wrangler.toml.");
    }

    const brand = env.TOPLINE_BRAND_NAME?.trim() || "Topline OS";
    const url = new URL(request.url);

    // CORS preflight for the MCP endpoint
    if (request.method === "OPTIONS") return cors(new Response(null, { status: 204 }));

    // Routing
    switch (url.pathname) {
      case "/":
        return cors(landing(brand, url.origin));
      case "/.well-known/oauth-authorization-server":
      case "/.well-known/oauth-protected-resource":
        return cors(oauthMetadata(url.origin));
      case "/register":
        return cors(await handleRegister(request));
      case "/authorize":
        return cors(await handleAuthorize(request, env, brand));
      case "/token":
        return cors(await handleToken(request, env, brand));
      case "/connect":
        return cors(await handleConnect(request, env, brand));
      case "/mcp":
        return cors(await handleMcp(request, env, ctx));
      case "/admin/do-info":
        return cors(await handleAdminDoInfo(request, env));
      default:
        return cors(plain(404, "Not found"));
    }
  },
};

// ---------------------------------------------------------------------------
// Landing page (so visitors don't see a raw 404)
// ---------------------------------------------------------------------------
function landing(brand: string, origin: string): Response {
  const h = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>${brand} MCP</title>
<style>body{font-family:-apple-system,sans-serif;max-width:640px;margin:48px auto;padding:0 20px;line-height:1.5}code{background:#f4f4f4;padding:2px 6px;border-radius:4px}h2{font-size:16px;margin-top:28px}</style>
</head><body>
<h1>${brand} MCP</h1>
<p>Remote MCP server. MCP Server URL: <code>${origin}/mcp</code></p>

<h2>Claude.ai (OAuth flow)</h2>
<ol>
<li>Claude → Settings → Connectors → Add custom connector.</li>
<li>Paste <code>${origin}/mcp</code> as the URL.</li>
<li>Click Add, then Connect. Paste your PIT and Location ID in the popup.</li>
</ol>

<h2>ChatGPT / other Bearer-only clients</h2>
<p>Go to <a href="/connect">/connect</a> — paste your PIT and Location ID, get back a single signed token. Paste that token into ChatGPT's Bearer field.</p>

<h2>Claude Desktop / Code</h2>
<p>Install as a local stdio MCP — see <a href="https://github.com/topline-com/os-mcp">the repo</a>.</p>
</body></html>`;
  return new Response(h, { headers: { "Content-Type": "text/html; charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Self-serve token generator — for ChatGPT Apps and other Bearer-only clients
// that can't complete the full OAuth dance. Creates a connection and issues
// a {cid, exp} access token referencing it.
// ---------------------------------------------------------------------------
async function handleConnect(request: Request, env: Env, brand: string): Promise<Response> {
  const url = new URL(request.url);
  if (request.method === "GET") {
    return html(200, connectFormHtml({ brand, origin: url.origin }));
  }
  if (request.method !== "POST") return plain(405, "Method not allowed");

  const form = await request.formData();
  const pit = String(form.get("pit") ?? "").trim();
  const locationId = String(form.get("locationId") ?? "").trim();

  if (!pit.startsWith("pit-")) {
    return html(
      400,
      connectFormHtml({
        brand,
        origin: url.origin,
        error: `Private Integration Token should start with "pit-". Re-copy it from ${brand} → Settings → Private Integrations.`,
      }),
    );
  }
  if (!locationId) {
    return html(400, connectFormHtml({ brand, origin: url.origin, error: "Location ID is required." }));
  }

  const cid = await createConnection(
    env.CONNECTIONS,
    { location_id: locationId, pit, brand_name: brand, source: "self-serve" },
    env.TOKEN_SIGNING_SECRET,
  );

  const payload: AccessTokenPayload = {
    cid,
    exp: Math.floor(Date.now() / 1000) + SELFSERVE_TOKEN_TTL_SECONDS,
  };
  const token = await signToken(payload, env.TOKEN_SIGNING_SECRET);
  return html(200, connectResultHtml({ brand, origin: url.origin, token }));
}

// ---------------------------------------------------------------------------
// OAuth 2.1 Authorization Server metadata (RFC 8414)
// ---------------------------------------------------------------------------
function oauthMetadata(origin: string): Response {
  const meta = {
    issuer: origin,
    authorization_endpoint: `${origin}/authorize`,
    token_endpoint: `${origin}/token`,
    registration_endpoint: `${origin}/register`,
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code"],
    code_challenge_methods_supported: ["S256", "plain"],
    token_endpoint_auth_methods_supported: ["none"],
    scopes_supported: ["mcp"],
  };
  return json(200, meta);
}

// ---------------------------------------------------------------------------
// Dynamic Client Registration (RFC 7591)
// ---------------------------------------------------------------------------
async function handleRegister(request: Request): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  let body: Record<string, unknown> = {};
  try {
    body = (await request.json()) as Record<string, unknown>;
  } catch {
    return json(400, { error: "invalid_request", error_description: "Body must be JSON" });
  }
  const clientId = `mcp-client-${crypto.randomUUID()}`;
  return json(201, {
    client_id: clientId,
    client_id_issued_at: Math.floor(Date.now() / 1000),
    redirect_uris: (body.redirect_uris as string[] | undefined) ?? [],
    grant_types: ["authorization_code"],
    response_types: ["code"],
    token_endpoint_auth_method: "none",
  });
}

// ---------------------------------------------------------------------------
// Authorization endpoint — shows form, then issues a short-lived auth code.
// The code still embeds PIT+LocId (short TTL, single use) — the connection
// record is created when the code is exchanged at /token.
// ---------------------------------------------------------------------------
async function handleAuthorize(request: Request, env: Env, brand: string): Promise<Response> {
  if (request.method === "GET") {
    const url = new URL(request.url);
    const redirect_uri = url.searchParams.get("redirect_uri") ?? "";
    const code_challenge = url.searchParams.get("code_challenge") ?? "";
    const code_challenge_method = (url.searchParams.get("code_challenge_method") ?? "S256") as
      | "S256"
      | "plain";
    const state = url.searchParams.get("state") ?? "";
    const client_id = url.searchParams.get("client_id") ?? "";

    if (!redirect_uri) return plain(400, "Missing redirect_uri");

    return html(
      200,
      authorizeFormHtml({ brand, redirect_uri, code_challenge, code_challenge_method, state, client_id }),
    );
  }

  if (request.method !== "POST") return plain(405, "Method not allowed");

  const form = await request.formData();
  const pit = String(form.get("pit") ?? "").trim();
  const locationId = String(form.get("locationId") ?? "").trim();
  const redirect_uri = String(form.get("redirect_uri") ?? "").trim();
  const code_challenge = String(form.get("code_challenge") ?? "").trim();
  const code_challenge_method = String(form.get("code_challenge_method") ?? "S256") as
    | "S256"
    | "plain";
  const state = String(form.get("state") ?? "").trim();
  const client_id = String(form.get("client_id") ?? "").trim();

  const rerender = (error: string) =>
    html(
      400,
      authorizeFormHtml({
        brand,
        error,
        redirect_uri,
        code_challenge,
        code_challenge_method,
        state,
        client_id,
      }),
    );

  if (!pit.startsWith("pit-")) {
    return rerender(
      `Private Integration Token should start with "pit-". Re-copy it from ${brand} → Settings → Private Integrations.`,
    );
  }
  if (!locationId) return rerender("Location ID is required.");
  if (!redirect_uri) return plain(400, "Missing redirect_uri");

  const payload: AuthCodePayload = {
    pit,
    locationId,
    redirect_uri,
    code_challenge,
    code_challenge_method,
    exp: Math.floor(Date.now() / 1000) + AUTH_CODE_TTL_SECONDS,
  };
  const code = await signToken(payload, env.TOKEN_SIGNING_SECRET);

  const redirect = new URL(redirect_uri);
  redirect.searchParams.set("code", code);
  if (state) redirect.searchParams.set("state", state);
  return new Response(null, { status: 302, headers: { Location: redirect.toString() } });
}

// ---------------------------------------------------------------------------
// Token endpoint — exchange auth code (+ PKCE verifier) for access token.
// This is where we create the ConnectionDirectory record.
// ---------------------------------------------------------------------------
async function handleToken(request: Request, env: Env, brand: string): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");

  const contentType = request.headers.get("Content-Type") ?? "";
  let grant_type: string;
  let code: string;
  let code_verifier: string;
  let redirect_uri: string;

  if (contentType.includes("application/x-www-form-urlencoded")) {
    const form = await request.formData();
    grant_type = String(form.get("grant_type") ?? "");
    code = String(form.get("code") ?? "");
    code_verifier = String(form.get("code_verifier") ?? "");
    redirect_uri = String(form.get("redirect_uri") ?? "");
  } else {
    let body: Record<string, string> = {};
    try {
      body = (await request.json()) as Record<string, string>;
    } catch {
      return json(400, { error: "invalid_request" });
    }
    grant_type = body.grant_type ?? "";
    code = body.code ?? "";
    code_verifier = body.code_verifier ?? "";
    redirect_uri = body.redirect_uri ?? "";
  }

  if (grant_type !== "authorization_code") {
    return json(400, { error: "unsupported_grant_type" });
  }

  const payload = await verifyToken<AuthCodePayload>(code, env.TOKEN_SIGNING_SECRET);
  if (!payload) return json(400, { error: "invalid_grant", error_description: "Code invalid or expired" });

  if (payload.redirect_uri !== redirect_uri) {
    return json(400, { error: "invalid_grant", error_description: "redirect_uri mismatch" });
  }

  if (payload.code_challenge) {
    if (!code_verifier) return json(400, { error: "invalid_grant", error_description: "PKCE code_verifier required" });
    const ok = await verifyPkce(code_verifier, payload.code_challenge, payload.code_challenge_method);
    if (!ok) return json(400, { error: "invalid_grant", error_description: "PKCE verification failed" });
  }

  // Create the connection record and issue a cid-referencing token.
  const cid = await createConnection(
    env.CONNECTIONS,
    { location_id: payload.locationId, pit: payload.pit, brand_name: brand, source: "oauth" },
    env.TOKEN_SIGNING_SECRET,
  );

  const accessPayload: AccessTokenPayload = {
    cid,
    exp: Math.floor(Date.now() / 1000) + ACCESS_TOKEN_TTL_SECONDS,
  };
  const access_token = await signToken(accessPayload, env.TOKEN_SIGNING_SECRET);

  return json(200, {
    access_token,
    token_type: "Bearer",
    expires_in: ACCESS_TOKEN_TTL_SECONDS,
    scope: "mcp",
  });
}

// ---------------------------------------------------------------------------
// MCP JSON-RPC endpoint
// ---------------------------------------------------------------------------
interface JsonRpcRequest {
  jsonrpc: string;
  id?: number | string | null;
  method: string;
  params?: Record<string, unknown>;
}

/**
 * Resolve a bearer string to (pit, locationId, cid?). Three shapes:
 *   - "pit-..."                         → raw PIT, location from header
 *   - signed { cid, exp }               → look up connection, decrypt PIT
 *   - signed { pit, locationId, exp }   → legacy, use embedded values
 */
async function resolveBearer(
  bearer: string,
  env: Env,
): Promise<{ pit: string; locationId?: string; cid?: string } | { error: string }> {
  if (bearer.startsWith("pit-")) {
    return { pit: bearer };
  }

  // Try verifying as a signed token. The payload is either new-shape or legacy.
  const payload = await verifyToken<unknown>(bearer, env.TOKEN_SIGNING_SECRET);
  if (!payload) return { error: "Access token invalid or expired" };

  if (isCidAccess(payload)) {
    const decrypted = await loadAndDecryptConnection(
      env.CONNECTIONS,
      payload.cid,
      env.TOKEN_SIGNING_SECRET,
    );
    if (!decrypted) return { error: "Access token references an unknown or revoked connection" };
    return { pit: decrypted.pit, locationId: decrypted.location_id, cid: payload.cid };
  }

  if (isLegacyAccess(payload)) {
    const p = payload as LegacyAccessTokenPayload;
    return { pit: p.pit, locationId: p.locationId };
  }

  return { error: "Access token payload is not recognized" };
}

async function handleMcp(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");

  const authHeader = request.headers.get("Authorization") ?? "";
  const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (!bearer) {
    return jsonRpcError(-32001, "Missing Authorization header", null, 401);
  }

  const resolved = await resolveBearer(bearer, env);
  if ("error" in resolved) {
    return jsonRpcError(-32001, resolved.error, null, 401);
  }
  let { pit, locationId, cid } = resolved;

  // For raw-PIT bearers, location may come from a side-channel header.
  if (!locationId) {
    locationId = request.headers.get("X-Topline-Location-Id")?.trim() || undefined;
  }

  let rpc: JsonRpcRequest;
  try {
    rpc = (await request.json()) as JsonRpcRequest;
  } catch {
    return jsonRpcError(-32700, "Parse error", null);
  }

  if (rpc.jsonrpc !== "2.0") {
    return jsonRpcError(-32600, "Invalid Request", rpc.id ?? null);
  }

  try {
    const response = await credentialsContext.run({ pit, locationId }, async () => {
      return dispatch(rpc, env);
    });
    // Best-effort last_verified_at update for cid-based tokens. Non-blocking.
    if (cid) ctx.waitUntil(touchConnection(env.CONNECTIONS, cid));
    return response;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = err instanceof ToplineApiError ? -32002 : -32603;
    return jsonRpcError(code, message, rpc.id ?? null);
  }
}

async function dispatch(rpc: JsonRpcRequest, env: Env): Promise<Response> {
  const brand = env.TOPLINE_BRAND_NAME?.trim() || "Topline OS";
  const serverName = `${brand.toLowerCase().replace(/\s+/g, "-")}-mcp`;

  switch (rpc.method) {
    case "initialize":
      return jsonRpcResult(rpc.id ?? null, {
        protocolVersion: PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: serverName, version: "0.1.0" },
      });

    case "notifications/initialized":
    case "notifications/cancelled":
      return new Response(null, { status: 202 });

    case "ping":
      return jsonRpcResult(rpc.id ?? null, {});

    case "tools/list":
      return jsonRpcResult(rpc.id ?? null, {
        tools: ALL_TOOLS.map(({ name, description, inputSchema }) => ({
          name,
          description,
          inputSchema,
        })),
      });

    case "tools/call": {
      const params = rpc.params as { name?: string; arguments?: Record<string, unknown> } | undefined;
      const name = params?.name;
      if (!name) return jsonRpcError(-32602, "Missing tool name", rpc.id ?? null);
      const tool = toolsByName.get(name);
      if (!tool) {
        return jsonRpcResult(rpc.id ?? null, {
          isError: true,
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
        });
      }
      try {
        const result = await tool.handler((params?.arguments ?? {}) as Record<string, unknown>);
        return jsonRpcResult(rpc.id ?? null, {
          content: [
            {
              type: "text",
              text: typeof result === "string" ? result : JSON.stringify(result, null, 2),
            },
          ],
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return jsonRpcResult(rpc.id ?? null, {
          isError: true,
          content: [{ type: "text", text: message }],
        });
      }
    }

    default:
      return jsonRpcError(-32601, `Method not found: ${rpc.method}`, rpc.id ?? null);
  }
}

// ---------------------------------------------------------------------------
// /admin/do-info — diagnostic for Phase 1 rollout. Gated by ADMIN_TOKEN
// secret. Returns that tenant's LocationDO schema overview + sync state so
// we can verify migrations ran cleanly without shipping a visible surface.
//
// Usage:
//   curl "https://os-mcp.topline.com/admin/do-info?location=loc-test&token=<ADMIN_TOKEN>"
//
// The endpoint creates a DO instance on first access for the given location.
// That instance sticks around (DO storage is persistent). For a clean test
// use a throwaway location_id like "debug-2026-04-23".
// ---------------------------------------------------------------------------
async function handleAdminDoInfo(request: Request, env: Env): Promise<Response> {
  if (request.method !== "GET") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const token = url.searchParams.get("token") ?? "";
  const location = url.searchParams.get("location") ?? "";

  if (!env.ADMIN_TOKEN) {
    return plain(503, "ADMIN_TOKEN not configured. Set with: wrangler secret put ADMIN_TOKEN");
  }
  if (token !== env.ADMIN_TOKEN) {
    return plain(401, "Invalid or missing token");
  }
  if (!location) {
    return plain(400, "Missing ?location=<location_id>");
  }

  const id = env.LOCATION_DO.idFromName(location);
  const stub = env.LOCATION_DO.get(id);
  const [ping, schema, state] = await Promise.all([
    stub.ping(),
    stub.describeSchema(),
    stub.getSyncState(),
  ]);
  return json(200, { location, ping, schema, sync_state: state });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function html(status: number, body: string): Response {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function plain(status: number, body: string): Response {
  return new Response(body, { status, headers: { "Content-Type": "text/plain" } });
}

function jsonRpcResult(id: number | string | null, result: unknown): Response {
  return json(200, { jsonrpc: "2.0", id, result });
}

function jsonRpcError(code: number, message: string, id: number | string | null, httpStatus = 200): Response {
  return json(httpStatus, { jsonrpc: "2.0", id, error: { code, message } });
}

function cors(response: Response): Response {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
  response.headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, X-Topline-Location-Id, Mcp-Session-Id, Mcp-Protocol-Version",
  );
  response.headers.set("Access-Control-Expose-Headers", "Mcp-Session-Id");
  return response;
}
