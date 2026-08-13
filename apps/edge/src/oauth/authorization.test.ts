import { test } from "node:test";
import assert from "node:assert/strict";
import type { OAuthHelpers } from "@cloudflare/workers-oauth-provider";
import {
  handleAuthorizationRequest,
  sha256Base64Url,
  type AuthorizationDependencies,
} from "./authorization.js";
import {
  AUTHORIZATION_SERVER_ORIGIN,
  createOAuthWorker,
  type OAuthProviderEnv,
} from "./provider.js";

class MemoryKv {
  readonly values = new Map<string, string>();

  async get(key: string, options?: string | { type?: string }): Promise<unknown> {
    const value = this.values.get(key);
    if (value === undefined) return null;
    const type = typeof options === "string" ? options : options?.type;
    return type === "json" ? JSON.parse(value) : value;
  }

  async put(key: string, value: string): Promise<void> {
    this.values.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this.values.delete(key);
  }

  async list(options?: { prefix?: string }): Promise<{ keys: Array<{ name: string }>; list_complete: true }> {
    const keys = [...this.values.keys()]
      .filter((name) => !options?.prefix || name.startsWith(options.prefix))
      .map((name) => ({ name }));
    return { keys, list_complete: true };
  }
}

interface TestEnv extends OAuthProviderEnv {
  OAUTH_KV: KVNamespace;
  OAUTH_PROVIDER?: OAuthHelpers;
}

const executionContext = {
  waitUntil() {},
  passThroughOnException() {},
  props: undefined,
} as unknown as ExecutionContext;

function createWorker(
  dependencies: AuthorizationDependencies<TestEnv> = testDependencies(),
) {
  const defaultHandler = {
    fetch(request: Request, env: TestEnv) {
      if (new URL(request.url).pathname === "/authorize") {
        return handleAuthorizationRequest(request, env, "Topline OS", dependencies);
      }
      return new Response("not found", { status: 404 });
    },
  };
  return createOAuthWorker<TestEnv>({
    apiHandler: defaultHandler,
    defaultHandler,
  });
}

function testDependencies(
  overrides: Partial<AuthorizationDependencies<TestEnv>> = {},
): AuthorizationDependencies<TestEnv> {
  return {
    async createConnection() { return "cid-test"; },
    async deleteConnection() {},
    ...overrides,
  };
}

async function registerClient(
  worker: ReturnType<typeof createWorker>,
  env: TestEnv,
  redirectUri = "https://client.example/callback",
): Promise<string> {
  const response = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_name: "Test Client",
        redirect_uris: [redirectUri],
        grant_types: ["authorization_code"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
      }),
    }),
    env,
    executionContext,
  );
  const body = await response.text();
  assert.equal(response.status, 201, body);
  const client = JSON.parse(body) as { client_id: string };
  return client.client_id;
}

function authorizationUrl(clientId: string, redirectUri: string): URL {
  const url = new URL(`${AUTHORIZATION_SERVER_ORIGIN}/authorize`);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", clientId);
  url.searchParams.set("redirect_uri", redirectUri);
  url.searchParams.set("scope", "mcp");
  url.searchParams.set("state", "state-123");
  return url;
}

test("unknown OAuth clients are rejected locally before credential entry", async () => {
  const env = { OAUTH_KV: new MemoryKv() as unknown as KVNamespace };
  const url = new URL(`${AUTHORIZATION_SERVER_ORIGIN}/authorize`);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", "unpersisted-client");
  url.searchParams.set("redirect_uri", "https://attacker.example/callback");
  url.searchParams.set("code_challenge", "A".repeat(43));
  url.searchParams.set("code_challenge_method", "S256");

  const response = await createWorker().fetch(new Request(url), env, executionContext);
  const body = await response.text();

  assert.equal(response.status, 400);
  assert.doesNotMatch(body, /name="pit"|name="locationId"/);
  assert.equal(response.headers.get("Location"), null);
});

test("redirect URI mismatch is rejected locally before credential entry", async () => {
  const env = { OAUTH_KV: new MemoryKv() as unknown as KVNamespace };
  const worker = createWorker();
  const clientId = await registerClient(worker, env);
  const url = authorizationUrl(clientId, "https://attacker.example/callback");
  url.searchParams.set("code_challenge", "A".repeat(43));
  url.searchParams.set("code_challenge_method", "S256");

  const response = await worker.fetch(new Request(url), env, executionContext);
  const body = await response.text();

  assert.equal(response.status, 400);
  assert.doesNotMatch(body, /name="pit"|name="locationId"/);
  assert.equal(response.headers.get("Location"), null);
});

test("public clients cannot reach credential entry without S256 PKCE", async () => {
  const redirectUri = "https://client.example/callback";
  const env = { OAUTH_KV: new MemoryKv() as unknown as KVNamespace };
  const worker = createWorker();
  const clientId = await registerClient(worker, env, redirectUri);

  for (const method of [undefined, "plain"] as const) {
    const url = authorizationUrl(clientId, redirectUri);
    if (method) {
      url.searchParams.set("code_challenge", "A".repeat(43));
      url.searchParams.set("code_challenge_method", method);
    }
    const response = await worker.fetch(new Request(url), env, executionContext);
    const body = await response.text();
    assert.notEqual(response.status, 200);
    assert.doesNotMatch(body, /name="pit"|name="locationId"/);
  }
});

test("valid persisted clients receive only an opaque one-time continuation form", async () => {
  const redirectUri = "https://client.example/callback";
  const env = {
    OAUTH_KV: new MemoryKv() as unknown as KVNamespace,
    OAUTH_FLOW_DO: new MemoryFlowNamespace(),
  } as unknown as TestEnv;
  const worker = createWorker();
  const clientId = await registerClient(worker, env, redirectUri);
  const url = authorizationUrl(clientId, redirectUri);
  url.searchParams.set("code_challenge", "A".repeat(43));
  url.searchParams.set("code_challenge_method", "S256");

  const startedAt = Date.now();
  const response = await worker.fetch(new Request(url), env, executionContext);
  const body = await response.text();

  assert.equal(response.status, 200);
  const flow = [...(env.OAUTH_FLOW_DO as unknown as MemoryFlowNamespace).flows.values()][0];
  assert.ok(flow?.consent, "consent flow was not created");
  assert.ok(
    flow.consent.expiresAt >= startedAt + 30 * 60 * 1000,
    "consent form must remain valid for at least 30 minutes",
  );
  assert.match(body, /name="pit"/);
  assert.match(body, /name="locationId"/);
  assert.match(body, /name="continuation"/);
  assert.match(body, /name="csrf"/);
  assert.match(body, /name="toolPreset"/);
  assert.match(body, /name="targetClient"/);
  const scriptNonce = body.match(/<script nonce="([^"]+)">/)?.[1];
  assert.ok(scriptNonce, "authorization form script must carry a nonce");
  assert.match(
    response.headers.get("Content-Security-Policy") ?? "",
    new RegExp(`script-src 'nonce-${scriptNonce}'`),
  );
  assert.match(body, /form\.addEventListener\("submit"/);
  assert.match(body, /submit\.disabled = true/);
  assert.match(body, /Connecting…/);
  assert.doesNotMatch(body, /name="(?:client_id|redirect_uri|code_challenge|code_challenge_method|state|resource)"/);
});

test("credential POST creates the connection without a CRM preflight and consumes once", async () => {
  const redirectUri = "https://client.example/callback";
  const env = {
    OAUTH_KV: new MemoryKv() as unknown as KVNamespace,
    OAUTH_FLOW_DO: new MemoryFlowNamespace(),
  } as unknown as TestEnv;
  let creates = 0;
  let createdPolicy: unknown;
  let createdTarget: unknown;
  const worker = createWorker(testDependencies({
    async createConnection(...args: unknown[]) {
      creates += 1;
      createdPolicy = args[4];
      createdTarget = args[5];
      return String(args[0]);
    },
  }));
  const clientId = await registerClient(worker, env, redirectUri);
  const url = authorizationUrl(clientId, redirectUri);
  url.searchParams.set("code_challenge", "A".repeat(43));
  url.searchParams.set("code_challenge_method", "S256");
  const formResponse = await worker.fetch(new Request(url), env, executionContext);
  const form = await formResponse.text();
  const continuation = hiddenValue(form, "continuation");
  const csrf = hiddenValue(form, "csrf");
  const body = new URLSearchParams({
    continuation,
    csrf,
    pit: "pit-test-secret",
    locationId: "location-123",
    toolPreset: "analytics",
    targetClient: "copilot_studio",
  });

  const submit = () => worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/authorize`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Origin: "https://claude.ai",
      },
      body,
    }),
    env,
    executionContext,
  );
  const [authorized, replay] = await Promise.all([submit(), submit()]);
  assert.equal(authorized.status, 302);
  assert.equal(replay.status, 302);
  assert.equal(replay.headers.get("Location"), authorized.headers.get("Location"));
  const redirect = new URL(authorized.headers.get("Location") ?? "");
  assert.equal(redirect.origin + redirect.pathname, redirectUri);
  assert.ok(redirect.searchParams.get("code"));
  assert.equal(redirect.searchParams.get("state"), "state-123");
  assert.equal(redirect.searchParams.get("iss"), AUTHORIZATION_SERVER_ORIGIN);
  assert.equal(creates, 1);
  const consentFlow = [...(env.OAUTH_FLOW_DO as unknown as MemoryFlowNamespace).flows.values()][0];
  assert.equal(consentFlow?.consent?.backfillStatus, "pending");
  assert.deepEqual(createdPolicy, {
    version: 1,
    mode: "allow",
    tool_ids: [
      "topline_contact_audit",
      "topline_describe_data_catalog",
      "topline_describe_schema",
      "topline_execute_query",
      "topline_explain_tables",
      "topline_find_references",
      "topline_owner_audit",
      "topline_pipeline_audit",
      "topline_pipeline_snapshot",
      "topline_query_doctor",
      "topline_utilize_api",
      "topline_warehouse_freshness",
    ],
  });
  assert.equal(createdTarget, "copilot_studio");

  const completedReplay = await submit();
  assert.equal(completedReplay.status, 302);
  assert.equal(completedReplay.headers.get("Location"), authorized.headers.get("Location"));
  assert.equal(creates, 1);
  assert.equal(consentFlow?.consent?.backfillStatus, "pending");
});

test("connection creation failure terminalizes the continuation before cleanup", async () => {
  const redirectUri = "https://client.example/callback";
  const env = {
    OAUTH_KV: new MemoryKv() as unknown as KVNamespace,
    OAUTH_FLOW_DO: new MemoryFlowNamespace(),
  } as unknown as TestEnv;
  const deleted: string[] = [];
  const worker = createWorker(testDependencies({
    async createConnection() {
      throw new Error("connection persistence failed");
    },
    async deleteConnection(connectionId) { deleted.push(connectionId); },
  }));
  const clientId = await registerClient(worker, env, redirectUri);
  const url = authorizationUrl(clientId, redirectUri);
  url.searchParams.set("code_challenge", "A".repeat(43));
  url.searchParams.set("code_challenge_method", "S256");
  const formResponse = await worker.fetch(new Request(url), env, executionContext);
  const form = await formResponse.text();
  const body = new URLSearchParams({
    continuation: hiddenValue(form, "continuation"),
    csrf: hiddenValue(form, "csrf"),
    pit: "pit-test-secret",
    locationId: "location-123",
  });
  const submit = () => worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/authorize`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    }),
    env,
    executionContext,
  );

  await assert.rejects(async () => submit(), /connection persistence failed/);
  const flow = [...(env.OAUTH_FLOW_DO as unknown as MemoryFlowNamespace).flows.values()][0];
  assert.equal(flow?.consent?.status, "expiring");
  assert.deepEqual(deleted, [body.get("continuation")]);
  const replay = await submit();
  assert.equal(replay.status, 400);
});

test("concurrent authorization-code redemption has one winner and replay fails", async () => {
  const redirectUri = "https://client.example/callback";
  const verifier = "v".repeat(43);
  const oauthKv = new MemoryKv();
  const env = {
    OAUTH_KV: oauthKv as unknown as KVNamespace,
    OAUTH_FLOW_DO: new MemoryFlowNamespace(),
  } as unknown as TestEnv;
  const worker = createWorker();
  const clientId = await registerClient(worker, env, redirectUri);
  const persistedClient = JSON.parse(oauthKv.values.get(`client:${clientId}`) ?? "null") as {
    tokenEndpointAuthMethod?: string;
  } | null;
  assert.equal(persistedClient?.tokenEndpointAuthMethod, "none");
  const url = authorizationUrl(clientId, redirectUri);
  url.searchParams.set("code_challenge", await sha256Base64Url(verifier));
  url.searchParams.set("code_challenge_method", "S256");
  const formResponse = await worker.fetch(new Request(url), env, executionContext);
  const form = await formResponse.text();
  const authorization = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/authorize`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Origin: AUTHORIZATION_SERVER_ORIGIN,
      },
      body: new URLSearchParams({
        continuation: hiddenValue(form, "continuation"),
        csrf: hiddenValue(form, "csrf"),
        pit: "pit-test-secret",
        locationId: "location-123",
      }),
    }),
    env,
    executionContext,
  );
  const code = new URL(authorization.headers.get("Location") ?? "").searchParams.get("code");
  assert.ok(code);

  const redeem = (
    resource = `${AUTHORIZATION_SERVER_ORIGIN}/mcp`,
    codeVerifier = verifier,
  ) => worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        client_id: clientId,
        redirect_uri: redirectUri,
        code,
        code_verifier: codeVerifier,
        resource,
      }),
    }),
    env,
    executionContext,
  );

  const wrongResource = await redeem("https://attacker.example/mcp");
  assert.equal(wrongResource.status, 400);
  const wrongVerifier = await redeem(`${AUTHORIZATION_SERVER_ORIGIN}/mcp`, "x".repeat(43));
  assert.equal(wrongVerifier.status, 400);

  const concurrent = await Promise.all([redeem(), redeem()]);
  const statuses = concurrent.map((response) => response.status).sort();
  assert.deepEqual(statuses, [200, 400]);
  const winner = concurrent.find((response) => response.status === 200);
  assert.ok(winner);
  const token = await winner.json() as { access_token?: string };
  assert.ok(token.access_token);

  const replay = await redeem();
  assert.equal(replay.status, 400);
  assert.deepEqual(await replay.json(), { error: "invalid_grant" });
});

function hiddenValue(html: string, name: string): string {
  const match = html.match(new RegExp(`name="${name}" value="([^"]+)"`));
  assert.ok(match, `missing hidden input ${name}`);
  return match[1];
}

class MemoryFlowNamespace {
  readonly flows = new Map<string, MemoryFlow>();

  idFromName(name: string): string {
    return name;
  }

  get(id: string): MemoryFlow {
    let flow = this.flows.get(id);
    if (!flow) {
      flow = new MemoryFlow();
      this.flows.set(id, flow);
    }
    return flow;
  }
}

class MemoryFlow {
  consent?: {
    request: unknown;
    connectionId: string;
    csrfHash: string;
    expiresAt: number;
    status: "pending" | "processing" | "completed" | "expiring";
    submissionHash?: string;
    processingLeaseHash?: string;
    redirectTo?: string;
    backfillStatus?: "pending" | "processing" | "completed";
    backfillLeaseHash?: string;
  };
  code?: { leaseHash: string; expiresAt: number; status: "pending" | "spent" };

  async createConsent(
    request: unknown,
    connectionId: string,
    csrfHash: string,
    expiresAt: number,
  ): Promise<boolean> {
    if (this.consent) return false;
    this.consent = { request, connectionId, csrfHash, expiresAt, status: "pending" };
    return true;
  }

  async reserveConsent(
    csrfHash: string,
    submissionHash: string,
    processingLeaseHash: string,
    _connectionId: string,
    now: number,
  ): Promise<
    | { status: "reserved"; request: unknown }
    | { status: "processing" }
    | { status: "completed"; redirectTo: string }
    | { status: "invalid" }
  > {
    if (
      !this.consent ||
      this.consent.csrfHash !== csrfHash ||
      this.consent.expiresAt <= now
    ) {
      return { status: "invalid" };
    }
    if (this.consent.submissionHash && this.consent.submissionHash !== submissionHash) {
      return { status: "invalid" };
    }
    if (this.consent.status === "processing") return { status: "processing" };
    if (this.consent.status === "expiring") return { status: "invalid" };
    if (this.consent.status === "completed") {
      return this.consent.redirectTo
        ? { status: "completed", redirectTo: this.consent.redirectTo }
        : { status: "invalid" };
    }
    this.consent.status = "processing";
    this.consent.submissionHash = submissionHash;
    this.consent.processingLeaseHash = processingLeaseHash;
    return { status: "reserved", request: this.consent.request };
  }

  async completeConsent(
    submissionHash: string,
    processingLeaseHash: string,
    redirectTo: string,
  ): Promise<boolean> {
    if (
      !this.consent ||
      this.consent.status !== "processing" ||
      this.consent.submissionHash !== submissionHash ||
      this.consent.processingLeaseHash !== processingLeaseHash
    ) return false;
    this.consent.status = "completed";
    this.consent.redirectTo = redirectTo;
    this.consent.backfillStatus = "pending";
    return true;
  }

  async abortConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean> {
    if (
      !this.consent ||
      !["processing", "completed", "expiring"].includes(this.consent.status) ||
      this.consent.submissionHash !== submissionHash ||
      this.consent.processingLeaseHash !== processingLeaseHash
    ) return false;
    this.consent.status = "expiring";
    return true;
  }

  async releaseConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean> {
    if (
      !this.consent ||
      this.consent.status !== "processing" ||
      this.consent.submissionHash !== submissionHash ||
      this.consent.processingLeaseHash !== processingLeaseHash
    ) return false;
    this.consent.status = "pending";
    this.consent.processingLeaseHash = undefined;
    return true;
  }

  async reserveBackfill(leaseHash: string): Promise<
    | { status: "reserved"; connectionId: string }
    | { status: "processing" | "completed" | "invalid" }
  > {
    if (!this.consent || this.consent.status !== "completed") return { status: "invalid" };
    if (this.consent.backfillStatus === "completed") return { status: "completed" };
    if (this.consent.backfillStatus === "processing") return { status: "processing" };
    this.consent.backfillStatus = "processing";
    this.consent.backfillLeaseHash = leaseHash;
    return { status: "reserved", connectionId: this.consent.connectionId };
  }

  async completeBackfill(leaseHash: string): Promise<boolean> {
    if (
      !this.consent ||
      this.consent.backfillStatus !== "processing" ||
      this.consent.backfillLeaseHash !== leaseHash
    ) return false;
    this.consent.backfillStatus = "completed";
    return true;
  }

  async releaseBackfill(leaseHash: string): Promise<boolean> {
    if (
      !this.consent ||
      this.consent.backfillStatus !== "processing" ||
      this.consent.backfillLeaseHash !== leaseHash
    ) return false;
    this.consent.backfillStatus = "pending";
    this.consent.backfillLeaseHash = undefined;
    return true;
  }

  async reserveCode(
    leaseHash: string,
    expiresAt: number,
    now: number,
  ): Promise<"reserved" | "pending" | "spent"> {
    if (this.code) return this.code.expiresAt <= now ? "spent" : this.code.status;
    this.code = { leaseHash, expiresAt, status: "pending" };
    return "reserved";
  }

  async releaseCode(leaseHash: string): Promise<boolean> {
    if (!this.code || this.code.status !== "pending" || this.code.leaseHash !== leaseHash) {
      return false;
    }
    this.code = undefined;
    return true;
  }

  async commitCode(leaseHash: string): Promise<boolean> {
    if (!this.code || this.code.status !== "pending" || this.code.leaseHash !== leaseHash) {
      return false;
    }
    this.code.status = "spent";
    return true;
  }
}
