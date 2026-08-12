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

import { ACTION_TOOLS, ALL_TOOLS } from "./registry.js";
import {
  credentialsContext,
  safeErrorFields,
  safeLog,
} from "@topline/shared";
import {
  signToken,
  verifyToken,
  isCidAccess,
  isLegacyAccess,
  createConnection,
  deleteConnection,
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
import { handleMcpHttpRequest } from "./mcp-http.js";
import { createRemoteMcpHandler } from "./mcp-server.js";
import { locationClient } from "./location-do-client.js";
import { ConnectionAuthDO } from "./connection-auth-do.js";
import {
  authorizationStub,
  initializeConnectionAuthorization,
  loadActiveConnectionAuthorization,
  loadConnectionAuthorizationForManagement,
} from "./connection-auth-client.js";
import { buildToolAccess } from "./tool-access.js";
import {
  requireQueryApiOperation,
  type QueryApiOperation,
} from "./query-api-access.js";
import {
  PolicyReauthorizationRequiredError,
  ToolPolicyError,
  type ConnectionAuthorizationSnapshot,
} from "./tool-policy.js";
import {
  buildToolSelectionView,
  compilePolicyUpdate,
  parseToolSelectionForm,
} from "./tool-selection-view.js";
import {
  assessClientCompatibility,
  type ToolSelection,
} from "./tool-presets.js";
import { sanitizeQuery, enforceExposedTables, SqlSafetyError } from "./sql-safety.js";
import { verifyCredentials } from "./credential-verification.js";
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
export { ConnectionAuthDO, LocationDO, OAuthFlowDO };

interface Env extends OAuthProviderEnv {
  TOKEN_SIGNING_SECRET: string;
  TOPLINE_BRAND_NAME?: string;
  OAUTH_PROVIDER?: OAuthHelpers;
  MCP_ALLOWED_ORIGINS?: string;
  CONNECTIONS: KVNamespace;
  CONNECTION_AUTH_DO: DurableObjectNamespace<ConnectionAuthDO>;
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
    if (!env.CONNECTIONS || !env.CONNECTION_AUTH_DO || !env.OAUTH_KV || !env.OAUTH_FLOW_DO) {
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
    case "/connection/policy":
      return handlePolicyRoute(request, env);
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
  const toolSelection = buildToolSelectionView(ALL_TOOLS);
  if (request.method === "GET") {
    return html(200, connectFormHtml({ brand, origin: url.origin, toolSelection }));
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
        toolSelection,
        error: `Private Integration Token should start with "pit-". Re-copy it from ${brand} → Settings → Private Integrations.`,
      }),
    );
  }
  if (!locationId) {
    return html(400, connectFormHtml({ brand, origin: url.origin, toolSelection, error: "Location ID is required." }));
  }

  let selected;
  try {
    selected = parseToolSelectionForm(form, ALL_TOOLS);
    const compatibility = assessClientCompatibility(selected.selected_count, selected.target);
    if (!compatibility.compatible) throw new Error(compatibility.error);
  } catch (error) {
    return html(
      400,
      connectFormHtml({
        brand,
        origin: url.origin,
        toolSelection,
        error: error instanceof Error ? error.message : "Invalid tool selection.",
      }),
    );
  }

  const cid = await createConnection(
    env.CONNECTIONS,
    { location_id: locationId, pit, brand_name: brand, source: "self-serve" },
    env.TOKEN_SIGNING_SECRET,
  );
  try {
    await initializeConnectionAuthorization(
      env,
      cid,
      locationId,
      selected.policy,
      selected.target,
    );
  } catch {
    await deleteConnection(env.CONNECTIONS, cid);
    return plain(500, "Connection authorization could not be initialized.");
  }

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
    verifyCredentials,
    async createConnection(pit, locationId, _oauthClientId, policy, clientTarget, env) {
      const brand = env.TOPLINE_BRAND_NAME?.trim() || "Topline OS";
      const cid = await createConnection(
        env.CONNECTIONS,
        { location_id: locationId, pit, brand_name: brand, source: "oauth" },
        env.TOKEN_SIGNING_SECRET,
      );
      try {
        await initializeConnectionAuthorization(
          env,
          cid,
          locationId,
          policy,
          clientTarget,
        );
      } catch (error) {
        await deleteConnection(env.CONNECTIONS, cid);
        throw error;
      }
      return cid;
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
interface ResolvedCredentials {
  pit: string;
  locationId?: string;
  cid?: string;
  rawPitBearer: boolean;
  authorization: ConnectionAuthorizationSnapshot | null;
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
    return { pit: bearer, rawPitBearer: true, authorization: null };
  }

  // Try verifying as a signed token. The payload is either new-shape or legacy.
  const payload = await verifyToken<unknown>(bearer, env.TOKEN_SIGNING_SECRET);
  if (!payload) return { error: "Access token invalid or expired" };

  if (isCidAccess(payload)) {
    let loaded;
    try {
      loaded = await loadActiveConnectionAuthorization(env, payload.cid);
    } catch {
      return { error: "Access token references an unknown or revoked connection" };
    }
    return {
      pit: loaded.connection.pit,
      locationId: loaded.connection.location_id,
      cid: payload.cid,
      rawPitBearer: false,
      authorization: loaded.authorization,
    };
  }

  if (isLegacyAccess(payload)) {
    const p = payload as LegacyAccessTokenPayload;
    return {
      pit: p.pit,
      locationId: p.locationId,
      rawPitBearer: false,
      authorization: null,
    };
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
  if (!props) return queryUnauthorized("Invalid access token");

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
  let loaded;
  try {
    loaded = await loadActiveConnectionAuthorization(env, props.cid);
  } catch {
    return { kind: "invalid_token" };
  }
  return {
    kind: "credentials",
    credentials: {
      pit: loaded.connection.pit,
      locationId: loaded.connection.location_id,
      cid: props.cid,
      rawPitBearer: false,
      authorization: loaded.authorization,
    },
  };
}

async function handleMcp(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  resolved: ResolvedCredentials,
): Promise<Response> {
  let { pit, locationId } = resolved;
  if (!locationId) {
    locationId = request.headers.get("X-Topline-Location-Id")?.trim() || undefined;
  }

  const registry = resolved.rawPitBearer ? ACTION_TOOLS : ALL_TOOLS;
  const toolAccess = buildToolAccess(resolved.authorization, registry);
  const handler = createRemoteMcpHandler({ tools: toolAccess.advertised });
  const allowedOrigins = new Set(
    (env.MCP_ALLOWED_ORIGINS ?? "")
      .split(",")
      .map((origin) => origin.trim())
      .filter(Boolean),
  );

  const response = await handleMcpHttpRequest(
    request,
    () => edgeContext.run(
      { location_do: env.LOCATION_DO, authorization: resolved.authorization },
      () => credentialsContext.run(
        { pit, locationId },
        () => handler.fetch(request, {
          authInfo: {
            token: "authenticated",
            clientId: resolved.cid ?? (resolved.rawPitBearer ? "legacy-raw-pit" : "legacy"),
            scopes: [OAUTH_SCOPE],
            extra: { rawPitBearer: resolved.rawPitBearer },
          },
        }),
      ),
    ),
    { allowedOrigins },
  );

  ctx.waitUntil(handler.close());
  if (resolved.cid && locationId) {
    ctx.waitUntil(
      authorizationStub(env, resolved.cid).touch(locationId),
    );
  }
  return response;
}

async function handlePolicyRoute(request: Request, env: Env): Promise<Response> {
  const authHeader = request.headers.get("Authorization") ?? "";
  const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
  const payload = bearer
    ? await verifyToken<unknown>(bearer, env.TOKEN_SIGNING_SECRET)
    : null;
  if (!isCidAccess(payload)) {
    return json(401, { error: "Connection-bound authorization required." });
  }

  let active;
  try {
    active = await loadConnectionAuthorizationForManagement(env, payload.cid);
  } catch {
    return json(401, { error: "Connection is unavailable." });
  }
  const stub = authorizationStub(env, payload.cid);

  if (request.method === "GET") {
    return json(200, policyManagementView(active.authorization));
  }

  let body: {
    expected_policy_version?: number;
    selection?: ToolSelection;
    target_client?: "generic" | "copilot_studio";
  };
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json(400, { error: "Body must be JSON." });
  }
  if (!Number.isSafeInteger(body.expected_policy_version)) {
    return json(400, { error: "expected_policy_version is required." });
  }

  if (request.method === "PUT") {
    if (active.authorization.policy_version !== body.expected_policy_version) {
      return json(409, { error: "Policy changed or connection is unavailable. Refresh and retry." });
    }
    let policy;
    let target;
    try {
      ({ policy, target } = compilePolicyUpdate(
        body.selection as ToolSelection,
        body.target_client,
        active.authorization.client_target ?? "generic",
        ALL_TOOLS,
        active.authorization.policy,
      ));
    } catch (error) {
      if (error instanceof PolicyReauthorizationRequiredError) {
        return json(403, {
          error: "reauthorization_required",
          message: "Broadening tool access requires a new authorization.",
        });
      }
      return json(400, {
        error: error instanceof Error ? error.message : "Choose a valid tool set.",
      });
    }
    try {
      const updated = await stub.updatePolicy({
        location_id: active.connection.location_id,
        expected_policy_version: body.expected_policy_version!,
        policy,
        client_target: target,
      });
      return json(200, policyManagementView(updated));
    } catch {
      return json(409, { error: "Policy changed or connection is unavailable. Refresh and retry." });
    }
  }

  if (request.method === "DELETE") {
    let revoked;
    try {
      revoked = await stub.revoke({
        location_id: active.connection.location_id,
        expected_policy_version: body.expected_policy_version!,
      });
    } catch {
      return json(409, { error: "Policy changed or connection is unavailable. Refresh and retry." });
    }
    try {
      await deleteConnection(env.CONNECTIONS, payload.cid);
      return new Response(null, { status: 204 });
    } catch {
      return json(202, {
        status: "revoked",
        policy_version: revoked.policy_version,
        credential_cleanup: "pending",
      });
    }
  }

  return plain(405, "Method not allowed");
}

function policyManagementView(snapshot: ConnectionAuthorizationSnapshot): object {
  const catalogIds = new Set(ALL_TOOLS.map((tool) => tool.name));
  const selectedIds = snapshot.policy.mode === "all"
    ? ALL_TOOLS.map((tool) => tool.name)
    : snapshot.policy.tool_ids.filter((id) => catalogIds.has(id));
  const staleIds = snapshot.policy.mode === "allow"
    ? snapshot.policy.tool_ids.filter((id) => !catalogIds.has(id))
    : [];
  return {
    status: snapshot.status,
    client_target: snapshot.client_target ?? "generic",
    policy: snapshot.policy,
    policy_version: snapshot.policy_version,
    selected_count: snapshot.status === "active" ? selectedIds.length : 0,
    stale_tool_ids: staleIds,
    cache: {
      scope: "private",
      revision: snapshot.policy_version,
      effective: "next_request",
    },
  };
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
  authorization: ConnectionAuthorizationSnapshot | null;
}

async function authorizeQueryRequest(
  request: Request,
  env: Env,
  operation: QueryApiOperation,
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
    return requireQueryPolicy({
      locationId: provider.credentials.locationId,
      cid: provider.credentials.cid,
      authorization: provider.credentials.authorization,
    }, operation);
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
  return requireQueryPolicy({
    locationId: resolved.locationId,
    cid: resolved.cid,
    authorization: resolved.authorization,
  }, operation);
}

function requireQueryPolicy(
  authorization: QueryAuthorization,
  operation: QueryApiOperation,
): QueryAuthorization | Response {
  try {
    requireQueryApiOperation(authorization.authorization, operation, ALL_TOOLS);
    return authorization;
  } catch (error) {
    if (error instanceof ToolPolicyError) {
      return json(403, { error: "This connection is not authorized for that operation." });
    }
    throw error;
  }
}

function hasExactAudience(audience: unknown, expected: string): boolean {
  return audience === expected
    || (Array.isArray(audience) && audience.length === 1 && audience[0] === expected);
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
  const auth = await authorizeQueryRequest(request, env, "overview");
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
  const auth = await authorizeQueryRequest(request, env, "catalog");
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
  const auth = await authorizeQueryRequest(request, env, "explain_tables");
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
  const auth = await authorizeQueryRequest(request, env, "execute_sql");
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
