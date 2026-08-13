import OAuthProvider, {
  type AuthRequest,
  type OAuthProviderOptions,
  type ResolveExternalTokenInput,
  type ResolveExternalTokenResult,
} from "@cloudflare/workers-oauth-provider";
import { safeLog } from "@topline/shared";
import {
  AUTHORIZATION_SERVER_ORIGIN,
  MCP_RESOURCE,
  OAUTH_SCOPE,
} from "./constants.js";
import { randomSecret, sha256Base64Url } from "./crypto.js";

export {
  AUTHORIZATION_SERVER_ORIGIN,
  MCP_RESOURCE,
  OAUTH_SCOPE,
} from "./constants.js";

const TOKEN_REQUEST_MAX_BYTES = 16 * 1024;
const AUTHORIZATION_CODE_TTL_MS = 10 * 60 * 1000;

interface OAuthFlowStub {
  createConsent(request: AuthRequest, csrfHash: string, expiresAt: number): Promise<boolean>;
  consumeConsent(csrfHash: string, now: number): Promise<AuthRequest | null>;
  reserveCode(
    leaseHash: string,
    expiresAt: number,
    now: number,
  ): Promise<"reserved" | "pending" | "spent">;
  releaseCode(leaseHash: string): Promise<boolean>;
  commitCode(leaseHash: string): Promise<boolean>;
}

interface OAuthFlowNamespace {
  idFromName(name: string): unknown;
  get(id: unknown): OAuthFlowStub;
}

export interface OAuthProviderEnv {
  OAUTH_KV: KVNamespace;
  OAUTH_PROVIDER?: unknown;
  OAUTH_FLOW_DO?: OAuthFlowNamespace;
  MCP_ALLOWED_ORIGINS?: string;
}

interface FetchHandler<Env> {
  fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Response | Promise<Response>;
}

interface CreateOAuthWorkerOptions<Env extends OAuthProviderEnv> {
  apiHandler: FetchHandler<Env>;
  defaultHandler: FetchHandler<Env>;
  resolveExternalToken?: (
    input: ResolveExternalTokenInput<Env>,
  ) => Promise<ResolveExternalTokenResult | null>;
}

export function createOAuthWorker<Env extends OAuthProviderEnv>(
  handlers: CreateOAuthWorkerOptions<Env>,
): FetchHandler<Env> {
  const options: OAuthProviderOptions<Env> = {
    apiRoute: MCP_RESOURCE,
    apiHandler: handlers.apiHandler,
    defaultHandler: handlers.defaultHandler,
    authorizeEndpoint: `${AUTHORIZATION_SERVER_ORIGIN}/authorize`,
    tokenEndpoint: `${AUTHORIZATION_SERVER_ORIGIN}/token`,
    clientRegistrationEndpoint: `${AUTHORIZATION_SERVER_ORIGIN}/register`,
    scopesSupported: [OAUTH_SCOPE],
    resourceMetadata: {
      resource: MCP_RESOURCE,
      authorization_servers: [AUTHORIZATION_SERVER_ORIGIN],
      scopes_supported: [OAUTH_SCOPE],
      bearer_methods_supported: ["header"],
      resource_name: "Topline OS MCP",
    },
    clientIdMetadataDocumentEnabled: true,
    allowPlainPKCE: false,
    allowImplicitFlow: false,
    resourceMatchOriginOnly: false,
    resolveExternalToken: handlers.resolveExternalToken,
    onError(error) {
      safeLog("warn", "oauth_error", {
        status: error.status,
        has_internal_detail: Boolean(error.internal),
      });
    },
  };

  const provider = new OAuthProvider(options);
  return {
    async fetch(request, env, ctx) {
      const url = new URL(request.url);
      const origin = request.headers.get("Origin");
      if (origin && isBrowserSensitivePath(url.pathname)) {
        if (!isAllowedOrigin(origin, url.pathname, request.method, env)) {
          return new Response("Forbidden", { status: 403 });
        }
        if (request.method === "OPTIONS") return preflightResponse(request, origin);
      }

      let response: Response;
      if (url.pathname === "/token" && request.method === "POST") {
        response = await redeemAuthorizationCode(provider, request, env, ctx);
      } else {
        response = await provider.fetch(request, env, ctx);
      }
      return origin && isBrowserSensitivePath(url.pathname)
        ? withCors(response, origin)
        : response;
    },
  };
}

function isBrowserSensitivePath(pathname: string): boolean {
  const protectedResourceMetadata = "/.well-known/oauth-protected-resource";
  return pathname.startsWith("/mcp")
    || pathname === "/.well-known/oauth-authorization-server"
    || pathname === protectedResourceMetadata
    || pathname.startsWith(`${protectedResourceMetadata}/`)
    || ["/token", "/register"].includes(pathname)
    || pathname.startsWith("/query/api/");
}

function isAllowedOrigin(
  origin: string,
  _pathname: string,
  _method: string,
  env: OAuthProviderEnv,
): boolean {
  if (origin === AUTHORIZATION_SERVER_ORIGIN) return true;
  const allowed = new Set(
    (env.MCP_ALLOWED_ORIGINS ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
  return allowed.has(origin);
}

function preflightResponse(request: Request, origin: string): Response {
  const requestedMcpParams = (request.headers.get("Access-Control-Request-Headers") ?? "")
    .split(",")
    .map((header) => header.trim())
    .filter((header) => /^mcp-param-[a-z0-9-]+$/i.test(header));
  const allowHeaders = [
    "Authorization",
    "Content-Type",
    "MCP-Protocol-Version",
    "Mcp-Method",
    "Mcp-Name",
    "X-Topline-Location-Id",
    ...requestedMcpParams,
  ];
  return withCors(
    new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": allowHeaders.join(", "),
        "Access-Control-Expose-Headers": "MCP-Protocol-Version, Mcp-Method, Mcp-Name, WWW-Authenticate",
        "Access-Control-Max-Age": "86400",
        Vary: "Access-Control-Request-Headers",
      },
    }),
    origin,
  );
}

function withCors(response: Response, origin: string): Response {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", origin);
  const vary = headers.get("Vary");
  headers.set("Vary", vary ? `${vary}, Origin` : "Origin");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function redeemAuthorizationCode<Env extends OAuthProviderEnv>(
  provider: OAuthProvider<Env>,
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
  if (Number.isFinite(declaredLength) && declaredLength > TOKEN_REQUEST_MAX_BYTES) {
    return oauthError("invalid_request", 413);
  }

  const body = await request.clone().text();
  if (body.length > TOKEN_REQUEST_MAX_BYTES) return oauthError("invalid_request", 413);
  const form = new URLSearchParams(body);
  if (form.get("grant_type") !== "authorization_code") {
    return provider.fetch(request, env, ctx);
  }

  const code = form.get("code");
  if (!code) return provider.fetch(request, env, ctx);
  if (!env.OAUTH_FLOW_DO) return oauthError("server_error", 500);

  const codeHash = await sha256Base64Url(code);
  const leaseHash = randomSecret();
  const now = Date.now();
  const flowId = env.OAUTH_FLOW_DO.idFromName(codeHash);
  const flow = env.OAUTH_FLOW_DO.get(flowId);
  const reservation = await flow.reserveCode(
    leaseHash,
    now + AUTHORIZATION_CODE_TTL_MS,
    now,
  );
  if (reservation !== "reserved") return oauthError("invalid_grant", 400);

  const response = await provider.fetch(request, env, ctx);
  if (response.ok) {
    await flow.commitCode(leaseHash);
  } else if (response.status >= 400 && response.status < 500) {
    await flow.releaseCode(leaseHash);
  }
  return response;
}

function oauthError(error: string, status: number): Response {
  return Response.json(
    { error },
    {
      status,
      headers: {
        "Cache-Control": "no-store",
        Pragma: "no-cache",
      },
    },
  );
}
