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
//  - OAuth grants contain only a connection ID; provider access and refresh
//    tokens never contain the PIT. /connect issues an HMAC-signed { cid, exp }
//    envelope for clients that cannot run OAuth.
//  - Legacy tokens that embed { pit, locationId, exp } are still accepted
//    so deploys don't break existing Claude / ChatGPT sessions. They keep
//    working until they expire naturally.
//
// Secrets: TOKEN_SIGNING_SECRET doubles as the KEK for PIT encryption via
// HKDF. Rotating it invalidates every token AND every encrypted PIT in one
// step.

import { ALL_TOOLS, toolsByName, ANALYTICS_TOOL_NAMES } from "./registry.js";
import {
  credentialsContext,
  safeErrorFields,
  safeLog,
  ToplineApiError,
} from "@topline/shared";
import {
  signToken,
  verifyToken,
  isCidAccess,
  isLegacyAccess,
  createConnection,
  deleteConnection,
  loadAndDecryptConnection,
  touchConnection,
  type AccessTokenPayload,
  type LegacyAccessTokenPayload,
} from "@topline/shared-auth";
import {
  connectFormHtml,
  connectResultHtml,
} from "./remote-oauth.js";
import { LocationDO } from "@topline/shared-do";
import type { OAuthHelpers } from "@cloudflare/workers-oauth-provider";
import { edgeContext } from "./request-context.js";
import { locationClient } from "./location-do-client.js";
import { sanitizeQuery, enforceExposedTables, SqlSafetyError } from "./sql-safety.js";
import { buildCatalog } from "@topline/shared-schema";
import {
  handleAuthorizationRequest,
  type AuthorizationDependencies,
  type OAuthGrantProps,
} from "./oauth/authorization.js";
import { OAuthFlowDO } from "./oauth/flow-do.js";
import {
  AUTHORIZATION_SERVER_ORIGIN,
  MCP_RESOURCE,
  OAUTH_SCOPE,
  createOAuthWorker,
  type OAuthProviderEnv,
} from "./oauth/provider.js";

// Re-export the DO class so wrangler can bind it to this Worker script.
// The class implementation lives in packages/shared-do so the (future)
// sync worker can import the same type surface without circular deps.
export { LocationDO, OAuthFlowDO };

interface Env extends OAuthProviderEnv {
  TOKEN_SIGNING_SECRET: string;
  TOPLINE_BRAND_NAME?: string;
  OAUTH_PROVIDER?: OAuthHelpers;
  MCP_ALLOWED_ORIGINS?: string;
  CONNECTIONS: KVNamespace;
  LOCATION_DO: DurableObjectNamespace<LocationDO>;
  ADMIN_TOKEN?: string;
  /**
   * Service binding to the sync worker. Used by kickoffInitialBackfill
   * to fire-and-forget the first backfill-all right after a connection
   * is created. Optional at the type level so local / stdio builds still
   * typecheck; at runtime the worker refuses to run without it when a
   * new connection is created.
   */
  SYNC_WORKER?: Fetcher;
}

const PROTOCOL_VERSION = "2024-11-05";
const SELFSERVE_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 365; // 1 year (/connect)
const CREDENTIAL_FORM_MAX_BYTES = 16 * 1024;

const oauthWorker = createOAuthWorker<Env>({
  apiHandler: { fetch: handleOAuthMcp },
  defaultHandler: { fetch: handleDefaultRequest },
  async resolveExternalToken({ token, env }) {
    const resolved = await resolveBearer(token, env);
    return "error" in resolved ? null : { props: resolved, audience: MCP_RESOURCE };
  },
});

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (!env.TOKEN_SIGNING_SECRET) {
      return plain(500, "Worker is missing TOKEN_SIGNING_SECRET. Run: wrangler secret put TOKEN_SIGNING_SECRET");
    }
    if (!env.CONNECTIONS || !env.OAUTH_KV || !env.OAUTH_FLOW_DO) {
      return plain(500, "Worker OAuth or connection storage bindings are missing. Check wrangler.toml.");
    }
    return oauthWorker.fetch(request, env, ctx);
  },
};

async function handleDefaultRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const brand = env.TOPLINE_BRAND_NAME?.trim() || "Topline OS";
  const url = new URL(request.url);
  switch (url.pathname) {
    case "/":
      return landing(brand, url.origin);
    case "/authorize":
      return handleAuthorizationRequest(request, env, brand, authorizationDependencies(ctx));
    case "/connect":
      return handleConnect(request, env, brand, ctx);
    case "/admin/do-info":
      return handleAdminDoInfo(request, env);
    case "/admin/do-query":
      return handleAdminDoQuery(request, env);
    case "/admin/do-exec":
      return handleAdminDoExec(request, env);
    case "/query/api/get-overview":
      return handleQueryApiOverview(request, env);
    case "/query/api/catalog":
      return handleQueryApiCatalog(request, env);
    case "/query/api/explain-tables":
      return handleQueryApiExplainTables(request, env);
    case "/query/api/execute-sql":
      return handleQueryApiExecuteSql(request, env);
    default:
      return plain(404, "Not found");
  }
}

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
async function handleConnect(request: Request, env: Env, brand: string, ctx: ExecutionContext): Promise<Response> {
  const url = new URL(request.url);
  if (request.method === "GET") {
    return html(200, connectFormHtml({ brand, origin: url.origin }));
  }
  if (request.method !== "POST") return plain(405, "Method not allowed");

  if (!request.headers.get("Content-Type")?.startsWith("application/x-www-form-urlencoded")) {
    return plain(415, "Unsupported media type");
  }
  const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
  if (Number.isFinite(declaredLength) && declaredLength > CREDENTIAL_FORM_MAX_BYTES) {
    return plain(413, "Request body too large");
  }
  const encodedForm = await request.text();
  if (encodedForm.length > CREDENTIAL_FORM_MAX_BYTES) {
    return plain(413, "Request body too large");
  }
  const form = new URLSearchParams(encodedForm);
  const pit = (form.get("pit") ?? "").trim();
  const locationId = (form.get("locationId") ?? "").trim();

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

  // Fire the initial full backfill in the background. The cron would
  // eventually seed this connection on its next 15-min tick, but running
  // it now means the customer can query data the moment they finish
  // setup instead of staring at an empty DO.
  ctx.waitUntil(kickoffInitialBackfill(env, cid));

  const payload: AccessTokenPayload = {
    cid,
    exp: Math.floor(Date.now() / 1000) + SELFSERVE_TOKEN_TTL_SECONDS,
  };
  const token = await signToken(payload, env.TOKEN_SIGNING_SECRET);
  return html(200, connectResultHtml({ brand, origin: url.origin, token }));
}

function authorizationDependencies(
  ctx: ExecutionContext,
): AuthorizationDependencies<Env> {
  return {
    async verifyCredentials(pit, locationId) {
      const response = await fetch(
        `https://services.leadconnectorhq.com/locations/${encodeURIComponent(locationId)}`,
        {
          headers: {
            Authorization: `Bearer ${pit}`,
            Version: "2021-07-28",
          },
          signal: AbortSignal.timeout(10_000),
        },
      );
      if (!response.ok) throw new Error("credential_verification_failed");
    },
    async createConnection(pit, locationId, _oauthClientId, env) {
      const brand = env.TOPLINE_BRAND_NAME?.trim() || "Topline OS";
      return createConnection(
        env.CONNECTIONS,
        { location_id: locationId, pit, brand_name: brand, source: "oauth" },
        env.TOKEN_SIGNING_SECRET,
      );
    },
    async deleteConnection(cid, env) {
      await deleteConnection(env.CONNECTIONS, cid);
    },
    async connectionCreated(cid, env) {
      ctx.waitUntil(kickoffInitialBackfill(env, cid));
    },
  };
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

interface ResolvedCredentials {
  pit: string;
  locationId?: string;
  cid?: string;
  rawPitBearer: boolean;
}

type ProviderCredentialResolution =
  | {
      kind: "credentials";
      credentials: ResolvedCredentials & { locationId: string; cid: string };
    }
  | { kind: "not_provider" }
  | { kind: "invalid_token" }
  | { kind: "insufficient_scope" };

/**
 * Resolve a bearer string to (pit, locationId, cid?). Three shapes:
 *   - "pit-..."                         → raw PIT, location from header
 *   - signed { cid, exp }               → look up connection, decrypt PIT
 *   - signed { pit, locationId, exp }   → legacy, use embedded values
 */
async function resolveBearer(
  bearer: string,
  env: Env,
): Promise<ResolvedCredentials | { error: string }> {
  if (bearer.startsWith("pit-")) {
    return { pit: bearer, rawPitBearer: true };
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
    return {
      pit: decrypted.pit,
      locationId: decrypted.location_id,
      cid: payload.cid,
      rawPitBearer: false,
    };
  }

  if (isLegacyAccess(payload)) {
    const p = payload as LegacyAccessTokenPayload;
    return { pit: p.pit, locationId: p.locationId, rawPitBearer: false };
  }

  return { error: "Access token payload is not recognized" };
}

async function handleOAuthMcp(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const props = (ctx as ExecutionContext & {
    props?: OAuthGrantProps | ResolvedCredentials;
  }).props;
  if (!props) return jsonRpcError(-32001, "Invalid access token", null, 401);

  let resolved: ResolvedCredentials;
  if ("pit" in props) {
    resolved = props;
  } else {
    const provider = await resolveProviderCredentials(bearerToken(request), env);
    if (provider.kind === "credentials") {
      resolved = provider.credentials;
    } else if (provider.kind === "insufficient_scope") {
      return oauthInsufficientScope();
    } else {
      return queryUnauthorized("Invalid OAuth access token");
    }
  }
  return handleMcp(request, env, ctx, resolved);
}

function bearerToken(request: Request): string {
  return (request.headers.get("Authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
}

function isOAuthGrantProps(value: unknown): value is OAuthGrantProps {
  return Boolean(
    value &&
    typeof value === "object" &&
    typeof (value as OAuthGrantProps).cid === "string" &&
    typeof (value as OAuthGrantProps).oauthClientId === "string",
  );
}

async function resolveProviderCredentials(
  bearer: string,
  env: Env,
): Promise<ProviderCredentialResolution> {
  if (!bearer || !env.OAUTH_PROVIDER) return { kind: "not_provider" };
  const token = await env.OAUTH_PROVIDER.unwrapToken<OAuthGrantProps>(bearer);
  if (!token) return { kind: "not_provider" };
  if (!hasExactAudience(token.audience, MCP_RESOURCE)) return { kind: "invalid_token" };
  const props = token.grant.props;
  if (!isOAuthGrantProps(props)) return { kind: "invalid_token" };
  const tokenScope = Array.isArray(token.scope) ? token.scope : [];
  const grantScope = Array.isArray(token.grant.scope) ? token.grant.scope : [];
  if (!tokenScope.includes(OAUTH_SCOPE) || !grantScope.includes(OAUTH_SCOPE)) {
    return { kind: "insufficient_scope" };
  }
  if (token.userId !== props.cid || token.grant.clientId !== props.oauthClientId) {
    return { kind: "invalid_token" };
  }
  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    props.cid,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) return { kind: "invalid_token" };
  return {
    kind: "credentials",
    credentials: {
      pit: connection.pit,
      locationId: connection.location_id,
      cid: props.cid,
      rawPitBearer: false,
    },
  };
}

async function handleMcp(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  resolved: ResolvedCredentials,
): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  let { pit, locationId, cid } = resolved;

  // Raw-PIT bearers only (detected by the bearer starting with "pit-").
  // Action tools tolerate raw PITs because the CRM validates them upstream;
  // analytics tools (SQL surface) never call the CRM, so an unvalidated PIT
  // plus a caller-supplied location header is an auth bypass. Tracked
  // as a flag so dispatch can reject analytics calls for raw-PIT sessions.
  const rawPitBearer = resolved.rawPitBearer;

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
    const response = await edgeContext.run({ location_do: env.LOCATION_DO }, () =>
      credentialsContext.run({ pit, locationId }, async () => {
        return dispatch(rpc, env, { rawPitBearer });
      }),
    );
    // Best-effort last_verified_at update for cid-based tokens. Non-blocking.
    if (cid) ctx.waitUntil(touchConnection(env.CONNECTIONS, cid));
    return response;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = err instanceof ToplineApiError ? -32002 : -32603;
    return jsonRpcError(code, message, rpc.id ?? null);
  }
}

async function dispatch(
  rpc: JsonRpcRequest,
  env: Env,
  auth: { rawPitBearer: boolean },
): Promise<Response> {
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

    case "tools/list": {
      // Hide analytics tools from raw-PIT sessions. tools/call will
      // reject them anyway (see below), but filtering them out of the
      // list avoids confusing the client into trying them.
      const visible = auth.rawPitBearer
        ? ALL_TOOLS.filter((t) => !ANALYTICS_TOOL_NAMES.has(t.name))
        : ALL_TOOLS;
      return jsonRpcResult(rpc.id ?? null, {
        tools: visible.map(({ name, description, inputSchema }) => ({
          name,
          description,
          inputSchema,
        })),
      });
    }

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
      // Analytics tools (the SQL surface) don't touch the CRM and therefore
      // can't implicitly validate a raw-PIT bearer the way action tools
      // do. Block them under raw-PIT sessions — caller must upgrade to
      // an OAuth-issued cid token or a /connect-minted signed bearer.
      if (auth.rawPitBearer && ANALYTICS_TOOL_NAMES.has(name)) {
        return jsonRpcResult(rpc.id ?? null, {
          isError: true,
          content: [
            {
              type: "text",
              text: `Tool '${name}' is not available under a raw PIT bearer. The analytics / SQL surface requires a connection-bound token. Use an OAuth-issued access token or mint one at /connect.`,
            },
          ],
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
//   curl -H "Authorization: Bearer $ADMIN_TOKEN" \
//        "https://os-mcp.topline.com/admin/do-info?location=loc-test"
//
// Secrets go in the Authorization header, never the query string —
// query strings leak to access logs, referrers, shell history, and
// observability tooling. The endpoint creates a DO instance on first
// access for the given location. That instance sticks around (DO
// storage is persistent). For a clean test use a throwaway location_id
// like "debug-2026-04-23".
// ---------------------------------------------------------------------------
async function handleAdminDoInfo(request: Request, env: Env): Promise<Response> {
  if (request.method !== "GET") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const location = url.searchParams.get("location") ?? "";

  if (!env.ADMIN_TOKEN) {
    return plain(503, "ADMIN_TOKEN not configured. Set with: wrangler secret put ADMIN_TOKEN");
  }
  const authHeader = request.headers.get("Authorization") ?? "";
  const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (!bearer || bearer !== env.ADMIN_TOKEN) {
    return plain(401, "Invalid or missing Authorization: Bearer <ADMIN_TOKEN>");
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
// /query/api/* — customer-facing HTTP query API. Same SQL surface as the
// SQL MCP tools, but accessible from anything that speaks HTTP with a
// Bearer token. Dashboard tools (Looker, Retool, Lovable, n8n, curl...)
// don't need to be MCP clients to hit this.
//
// All three endpoints share the same auth model: the Bearer token
// resolves to a connection (new-style cid token or legacy raw PIT),
// and the query runs against that connection's LocationDO.
// ---------------------------------------------------------------------------

interface QueryAuthorization {
  locationId: string;
  cid?: string;
}

async function authorizeQueryRequest(
  request: Request,
  env: Env,
): Promise<QueryAuthorization | Response> {
  const bearer = bearerToken(request);
  if (!bearer) return queryUnauthorized("Missing Authorization: Bearer token");

  // The SQL surface requires a CONNECTION-BOUND bearer (cid token from
  // OAuth/connect, or the legacy signed { pit, locationId } token).
  // Raw PITs are NOT accepted here: the action-tool path validates raw
  // PITs implicitly by forwarding them to the CRM, but the SQL path never
  // calls the CRM — it just picks a LocationDO by caller-supplied
  // location_id. Without upstream validation, a raw "pit-" string plus
  // any location header would let the caller read whatever SQLite DB
  // they name. Reject at the door.
  if (bearer.startsWith("pit-")) {
    return queryUnauthorized(
      "Raw PIT bearers are not accepted on the SQL surface. Use an OAuth-issued access token or one minted at /connect.",
    );
  }

  const provider = await resolveProviderCredentials(bearer, env);
  if (provider.kind === "credentials") {
    return {
      locationId: provider.credentials.locationId,
      cid: provider.credentials.cid,
    };
  }
  if (provider.kind === "insufficient_scope") return oauthInsufficientScope();
  if (provider.kind === "invalid_token") {
    return queryUnauthorized("Invalid OAuth access token");
  }

  const resolved = await resolveBearer(bearer, env);
  if ("error" in resolved) return queryUnauthorized(resolved.error);
  // After the raw-PIT guard above, resolved.locationId MUST come from
  // the signed token's payload (cid → connection record, or legacy
  // { pit, locationId }). We never fall back to the X-Topline-
  // Location-Id header on the SQL path.
  if (!resolved.locationId) {
    return queryUnauthorized(
      "Token does not carry a location. Reconnect via OAuth or /connect to mint a connection-bound token.",
    );
  }
  return { locationId: resolved.locationId, cid: resolved.cid };
}

function hasExactAudience(audience: unknown, expected: string): boolean {
  const values = Array.isArray(audience) ? audience : [audience];
  return values.some((value) => value === expected);
}

function queryUnauthorized(body: string): Response {
  return new Response(body, {
    status: 401,
    headers: {
      "Content-Type": "text/plain",
      "Cache-Control": "no-store",
      "WWW-Authenticate": `Bearer realm="OAuth", resource_metadata="${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-protected-resource/mcp", scope="${OAUTH_SCOPE}"`,
    },
  });
}

function oauthInsufficientScope(): Response {
  return new Response(
    JSON.stringify({
      error: "insufficient_scope",
      error_description: `The ${OAUTH_SCOPE} scope is required`,
    }),
    {
      status: 403,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "WWW-Authenticate": `Bearer realm="OAuth", resource_metadata="${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-protected-resource/mcp", error="insufficient_scope", scope="${OAUTH_SCOPE}"`,
      },
    },
  );
}

async function handleQueryApiOverview(
  request: Request,
  env: Env,
): Promise<Response> {
  if (request.method !== "GET") return plain(405, "Method not allowed");
  const auth = await authorizeQueryRequest(request, env);
  if (auth instanceof Response) return auth;
  try {
    const client = locationClient(env.LOCATION_DO, auth.locationId);
    const overview = await client.describeSchema();
    return json(200, overview);
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

async function handleQueryApiCatalog(
  request: Request,
  env: Env,
): Promise<Response> {
  if (request.method !== "GET") return plain(405, "Method not allowed");
  const auth = await authorizeQueryRequest(request, env);
  if (auth instanceof Response) return auth;
  try {
    return json(200, { entries: buildCatalog() });
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

async function handleQueryApiExplainTables(
  request: Request,
  env: Env,
): Promise<Response> {
  if (request.method !== "GET") return plain(405, "Method not allowed");
  const auth = await authorizeQueryRequest(request, env);
  if (auth instanceof Response) return auth;
  const url = new URL(request.url);
  const tables = url.searchParams.getAll("table");
  if (tables.length === 0) {
    return plain(400, "Missing table params. Repeat `?table=<name>` for each.");
  }
  try {
    const client = locationClient(env.LOCATION_DO, auth.locationId);
    const details = await client.explainTables(tables);
    return json(200, details);
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

async function handleQueryApiExecuteSql(
  request: Request,
  env: Env,
): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const auth = await authorizeQueryRequest(request, env);
  if (auth instanceof Response) return auth;

  let body: { sql?: string } = {};
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json(400, { error: "Body must be JSON: { sql: string }" });
  }
  const sqlInput = String(body.sql ?? "");

  let safe;
  try {
    safe = sanitizeQuery(sqlInput);
    enforceExposedTables(safe.sql);
  } catch (err) {
    if (err instanceof SqlSafetyError) {
      return json(400, { error: err.message, rejected_query: sqlInput });
    }
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }

  try {
    const client = locationClient(env.LOCATION_DO, auth.locationId);
    const result = await client.executeQuery(safe.sql, []);
    const hitCap = result.rows.length >= safe.effective_limit;
    return json(200, {
      columns: result.columns,
      rows: result.rows,
      elapsed_ms: result.elapsed_ms,
      truncated: result.truncated || hitCap,
      effective_limit: safe.effective_limit,
      rewritten_sql: safe.sql,
    });
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

// ---------------------------------------------------------------------------
// /admin/do-query — run a SELECT against a tenant's DO. Gated by
// ADMIN_TOKEN.
//
// SQL passes through sanitizeQuery() so DDL/DML/PRAGMA/ATTACH/multi-
// statement are blocked the same way customer-facing paths are. The
// exposed-table allowlist is intentionally NOT applied here, so admins
// can still SELECT from bookkeeping tables (_sync_state, _schema_log)
// and SQLite metadata (sqlite_master) for diagnostics.
//
// Usage:
//   curl -H "Authorization: Bearer $ADMIN_TOKEN" \
//        -H "Content-Type: application/json" \
//        -d '{"sql":"SELECT COUNT(*) FROM contacts"}' \
//        "https://os-mcp.topline.com/admin/do-query?location=X"
// ---------------------------------------------------------------------------
async function handleAdminDoQuery(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  if (!env.ADMIN_TOKEN) return plain(503, "ADMIN_TOKEN not configured");
  const authHeader = request.headers.get("Authorization") ?? "";
  const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (!bearer || bearer !== env.ADMIN_TOKEN) {
    return plain(401, "Invalid or missing Authorization: Bearer <ADMIN_TOKEN>");
  }

  const url = new URL(request.url);
  const location = url.searchParams.get("location") ?? "";
  if (!location) return plain(400, "Missing ?location=<location_id>");

  let body: { sql?: string; params?: unknown[] } = {};
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json(400, { error: "Body must be JSON: { sql, params? }" });
  }
  if (!body.sql) return json(400, { error: "Missing sql" });

  // Run the query through the SELECT-only parser even on the admin path.
  // Admins sometimes copy-paste from chat threads; the parser catches
  // accidental DDL/DML before it reaches SQLite. The exposed-table
  // allowlist is intentionally NOT applied here so ops can still
  // inspect bookkeeping tables (_sync_state, _schema_log) and SQLite
  // metadata (sqlite_master) for diagnostics.
  let safe;
  try {
    safe = sanitizeQuery(body.sql);
  } catch (err) {
    if (err instanceof SqlSafetyError) {
      return json(400, { error: err.message, rejected_query: body.sql });
    }
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }

  const id = env.LOCATION_DO.idFromName(location);
  const stub = env.LOCATION_DO.get(id);
  try {
    const result = await stub.executeQuery(safe.sql, (body.params as never[]) ?? []);
    return json(200, result);
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

// ---------------------------------------------------------------------------
// /admin/do-exec — run an arbitrary write statement against a tenant's DO.
// Gated by ADMIN_TOKEN. Intended for one-off maintenance: cleaning up
// stale rows after a source-path change, dropping a bad index, etc.
//
// No SQL-safety gate — the caller is trusted. Don't expose this surface
// to any customer path.
//
// Usage:
//   curl -H "Authorization: Bearer $ADMIN_TOKEN" \
//        -H "Content-Type: application/json" \
//        -d '{"sql":"DELETE FROM messages"}' \
//        "https://os-mcp.topline.com/admin/do-exec?location=X"
// ---------------------------------------------------------------------------
async function handleAdminDoExec(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  if (!env.ADMIN_TOKEN) return plain(503, "ADMIN_TOKEN not configured");
  const authHeader = request.headers.get("Authorization") ?? "";
  const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (!bearer || bearer !== env.ADMIN_TOKEN) {
    return plain(401, "Invalid or missing Authorization: Bearer <ADMIN_TOKEN>");
  }

  const url = new URL(request.url);
  const location = url.searchParams.get("location") ?? "";
  if (!location) return plain(400, "Missing ?location=<location_id>");

  let body: { sql?: string; params?: unknown[] } = {};
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json(400, { error: "Body must be JSON: { sql, params? }" });
  }
  if (!body.sql) return json(400, { error: "Missing sql" });

  const id = env.LOCATION_DO.idFromName(location);
  const stub = env.LOCATION_DO.get(id);
  try {
    const result = await stub.adminExecute(body.sql, (body.params as never[]) ?? []);
    return json(200, result);
  } catch (err) {
    return json(500, { error: err instanceof Error ? err.message : String(err) });
  }
}

// ---------------------------------------------------------------------------
// Fire a single backfill-all against the sync worker right after a
// connection is minted. Runs inside ctx.waitUntil so customers don't wait
// on the HTTP round-trip, and any per-entity failures are surfaced in
// the sync worker's logs where they belong — this function only cares
// that the kickoff POST was accepted.
//
// Falls back to a no-op (with a log line) when either binding is absent,
// so stdio / local builds without the service binding still work.
// ---------------------------------------------------------------------------
async function kickoffInitialBackfill(env: Env, connectionId: string): Promise<void> {
  if (!env.SYNC_WORKER || !env.ADMIN_TOKEN) {
    safeLog("log", "backfill_kickoff_skipped", {
      missing_sync_binding: !env.SYNC_WORKER,
      missing_admin_token: !env.ADMIN_TOKEN,
    });
    return;
  }
  try {
    // Service-binding fetch ignores URL host; any valid URL works. We
    // route to the same path the sync worker exposes for external
    // admin calls so the behavior and logging stay identical.
    const res = await env.SYNC_WORKER.fetch(
      `https://sync/sync/backfill-all?connection_id=${encodeURIComponent(connectionId)}`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${env.ADMIN_TOKEN}` },
      },
    );
    safeLog("log", "backfill_kickoff_completed", { status: res.status });
  } catch (err) {
    safeLog("warn", "backfill_kickoff_failed", { error: safeErrorFields(err) });
  }
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
