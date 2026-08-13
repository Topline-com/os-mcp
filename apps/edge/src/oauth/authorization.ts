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
  createConsent(
    request: AuthRequest,
    connectionId: string,
    csrfHash: string,
    expiresAt: number,
  ): Promise<boolean>;
  reserveConsent(
    csrfHash: string,
    submissionHash: string,
    processingLeaseHash: string,
    connectionId: string,
    now: number,
  ): Promise<
    | { status: "reserved"; request: AuthRequest }
    | { status: "processing" }
    | { status: "completed"; redirectTo: string }
    | { status: "invalid" }
  >;
  completeConsent(
    submissionHash: string,
    processingLeaseHash: string,
    redirectTo: string,
  ): Promise<boolean>;
  abortConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean>;
  releaseConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean>;
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
    connectionId: string,
    pit: string,
    locationId: string,
    oauthClientId: string,
    policy: PersistedToolPolicy,
    clientTarget: ClientTarget,
    env: Env,
  ): Promise<string>;
  deleteConnection(cid: string, env: Env): Promise<void>;
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
      continuation,
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
          "Content-Security-Policy": `default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${scriptNonce}'; frame-ancestors 'none'; base-uri 'none'`,
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
  const flow = env.OAUTH_FLOW_DO.get(flowId);
  const csrfHash = await sha256Base64Url(csrf);
  const submissionHash = await sha256Base64Url(encodedForm);
  const processingLeaseHash = await sha256Base64Url(randomSecret());
  const reservation = await reserveConsentWithRetry(
    flow,
    csrfHash,
    submissionHash,
    processingLeaseHash,
    continuation,
  );
  if (reservation.status === "completed") {
    return Response.redirect(reservation.redirectTo, 302);
  }
  if (reservation.status === "processing") {
    return new Response("Authorization is still processing. Please wait and try again.", {
      status: 409,
      headers: { "Retry-After": "2" },
    });
  }
  if (reservation.status === "invalid") {
    return localAuthorizationError("Authorization continuation is invalid or expired");
  }
  const oauthRequest = reservation.request;

  try {
    const cid = await dependencies.createConnection(
      continuation,
      pit,
      locationId,
      oauthRequest.clientId,
      selected.policy,
      selected.target,
      env,
    );
    const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
      request: oauthRequest,
      userId: cid,
      metadata: { brand },
      scope: oauthRequest.scope,
      props: { cid, oauthClientId: oauthRequest.clientId } satisfies OAuthGrantProps,
    });
    if (!await flow.completeConsent(submissionHash, processingLeaseHash, redirectTo)) {
      await flow.abortConsent(submissionHash, processingLeaseHash);
      await dependencies.deleteConnection(continuation, env);
      return new Response("OAuth authorization state could not be completed", { status: 503 });
    }
    return Response.redirect(redirectTo, 302);
  } catch (error) {
    // Any failure after reservation is terminal for this continuation. The
    // deterministic ID may already exist, and provider completion may already
    // have persisted a grant/code, so releasing it for reuse is unsafe.
    await flow.abortConsent(submissionHash, processingLeaseHash);
    await dependencies.deleteConnection(continuation, env);
    throw error;
  }
}

async function reserveConsentWithRetry(
  flow: ConsentFlowStub,
  csrfHash: string,
  submissionHash: string,
  processingLeaseHash: string,
  connectionId: string,
): Promise<Awaited<ReturnType<ConsentFlowStub["reserveConsent"]>>> {
  for (let attempt = 0; attempt < 40; attempt++) {
    const reservation = await flow.reserveConsent(
      csrfHash,
      submissionHash,
      processingLeaseHash,
      connectionId,
      Date.now(),
    );
    if (reservation.status !== "processing") return reservation;
    if (attempt < 39) await new Promise((resolve) => setTimeout(resolve, 250));
  }
  return { status: "processing" };
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
