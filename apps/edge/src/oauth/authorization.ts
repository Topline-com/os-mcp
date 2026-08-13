import {
  AuthorizationError,
  CimdFetchError,
  type AuthRequest,
  type OAuthHelpers,
} from "@cloudflare/workers-oauth-provider";
import { authorizeFormHtml } from "../remote-oauth.js";
import { ALL_TOOLS } from "../registry.js";
import {
  buildToolSelectionView,
  parseToolSelectionForm,
} from "../tool-selection-view.js";
import { assessClientCompatibility, type ClientTarget } from "../tool-presets.js";
import type { PersistedToolPolicy } from "../tool-policy.js";
import { AUTHORIZATION_SERVER_ORIGIN } from "./constants.js";
import { randomSecret, sha256Base64Url } from "./crypto.js";

export { sha256Base64Url } from "./crypto.js";

const CONSENT_TTL_MS = 30 * 60 * 1000;
const AUTHORIZATION_FORM_MAX_BYTES = 16 * 1024;

interface ConsentFlowStub {
  createConsent(request: AuthRequest, csrfHash: string, expiresAt: number): Promise<boolean>;
  consumeConsent(csrfHash: string, now: number): Promise<AuthRequest | null>;
}

interface ConsentFlowNamespace {
  idFromName(name: string): unknown;
  get(id: unknown): ConsentFlowStub;
}

interface AuthorizationEnv {
  OAUTH_PROVIDER?: OAuthHelpers;
  OAUTH_FLOW_DO?: ConsentFlowNamespace;
}

export interface AuthorizationDependencies<Env> {
  createConnection(
    pit: string,
    locationId: string,
    oauthClientId: string,
    policy: PersistedToolPolicy,
    clientTarget: ClientTarget,
    env: Env,
  ): Promise<string>;
  deleteConnection(cid: string, env: Env): Promise<void>;
  connectionCreated(cid: string, env: Env): Promise<void>;
}

export interface OAuthGrantProps {
  cid: string;
  oauthClientId: string;
}

export async function handleAuthorizationRequest<Env extends AuthorizationEnv>(
  request: Request,
  env: Env,
  brand: string,
  dependencies?: AuthorizationDependencies<Env>,
): Promise<Response> {
  if (!env.OAUTH_PROVIDER) {
    return new Response("OAuth provider unavailable", { status: 500 });
  }
  if (request.method === "POST") {
    if (!dependencies) return new Response("OAuth authorization unavailable", { status: 500 });
    return handleAuthorizationPost(request, env, brand, dependencies);
  }
  if (request.method !== "GET") return new Response("Method not allowed", { status: 405 });

  try {
    const oauthRequest = await env.OAUTH_PROVIDER.parseAuthRequest(request);
    const client = await env.OAUTH_PROVIDER.lookupClient(oauthRequest.clientId);
    if (!client) return localAuthorizationError("Unknown OAuth client");
    if (!env.OAUTH_FLOW_DO) {
      return new Response("OAuth flow storage unavailable", { status: 500 });
    }

    const continuation = crypto.randomUUID();
    const csrf = randomSecret();
    const scriptNonce = randomSecret();
    const csrfHash = await sha256Base64Url(csrf);
    const flowId = env.OAUTH_FLOW_DO.idFromName(continuation);
    const created = await env.OAUTH_FLOW_DO.get(flowId).createConsent(
      oauthRequest,
      csrfHash,
      Date.now() + CONSENT_TTL_MS,
    );
    if (!created) return new Response("OAuth flow collision", { status: 503 });

    return new Response(
      authorizeFormHtml({
        brand,
        continuation,
        csrf,
        scriptNonce,
        toolSelection: buildToolSelectionView(ALL_TOOLS),
      }),
      {
        status: 200,
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store",
          "Content-Security-Policy": `default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${scriptNonce}'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'`,
          "Referrer-Policy": "no-referrer",
        },
      },
    );
  } catch (error) {
    if (error instanceof AuthorizationError) {
      if (!error.redirectUri) return localAuthorizationError(error.description);
      const redirect = new URL(error.redirectUri);
      redirect.searchParams.set("error", error.code);
      redirect.searchParams.set("error_description", error.description);
      if (error.state) redirect.searchParams.set("state", error.state);
      if (error.issuer) redirect.searchParams.set("iss", error.issuer);
      return Response.redirect(redirect.toString(), 302);
    }
    if (error instanceof CimdFetchError) {
      return localAuthorizationError("OAuth client metadata could not be validated");
    }
    throw error;
  }
}

async function handleAuthorizationPost<Env extends AuthorizationEnv>(
  request: Request,
  env: Env,
  brand: string,
  dependencies: AuthorizationDependencies<Env>,
): Promise<Response> {
  if (!request.headers.get("Content-Type")?.startsWith("application/x-www-form-urlencoded")) {
    return new Response("Unsupported media type", { status: 415 });
  }
  if (!env.OAUTH_FLOW_DO || !env.OAUTH_PROVIDER) {
    return new Response("OAuth flow storage unavailable", { status: 500 });
  }

  const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
  if (Number.isFinite(declaredLength) && declaredLength > AUTHORIZATION_FORM_MAX_BYTES) {
    return new Response("Request body too large", { status: 413 });
  }
  const encodedForm = await request.text();
  if (encodedForm.length > AUTHORIZATION_FORM_MAX_BYTES) {
    return new Response("Request body too large", { status: 413 });
  }
  const form = new URLSearchParams(encodedForm);
  const continuation = stringField(form, "continuation");
  const csrf = stringField(form, "csrf");
  const pit = stringField(form, "pit");
  const locationId = stringField(form, "locationId");
  if (
    !continuation ||
    !csrf ||
    !pit?.startsWith("pit-") ||
    !locationId ||
    locationId.length > 128
  ) {
    return localAuthorizationError("Invalid authorization submission");
  }

  let selected;
  try {
    selected = parseToolSelectionForm(form, ALL_TOOLS);
    const compatibility = assessClientCompatibility(selected.selected_count, selected.target);
    if (!compatibility.compatible) {
      return localAuthorizationError(
        compatibility.error ?? "Tool selection is incompatible with the selected client",
      );
    }
  } catch {
    return localAuthorizationError("Invalid tool selection");
  }

  const flowId = env.OAUTH_FLOW_DO.idFromName(continuation);
  const oauthRequest = await env.OAUTH_FLOW_DO.get(flowId).consumeConsent(
    await sha256Base64Url(csrf),
    Date.now(),
  );
  if (!oauthRequest) return localAuthorizationError("Authorization continuation is invalid or expired");

  const cid = await dependencies.createConnection(
    pit,
    locationId,
    oauthRequest.clientId,
    selected.policy,
    selected.target,
    env,
  );
  try {
    const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
      request: oauthRequest,
      userId: cid,
      metadata: { brand },
      scope: oauthRequest.scope,
      props: { cid, oauthClientId: oauthRequest.clientId } satisfies OAuthGrantProps,
    });
    await dependencies.connectionCreated(cid, env);
    return Response.redirect(redirectTo, 302);
  } catch (error) {
    await dependencies.deleteConnection(cid, env);
    throw error;
  }
}

function localAuthorizationError(description: string): Response {
  return new Response(description, {
    status: 400,
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}

function stringField(form: URLSearchParams, name: string): string | null {
  const value = form.get(name)?.trim();
  return value || null;
}
