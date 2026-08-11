import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createConnection,
  signToken,
  type AccessTokenPayload,
  type LegacyAccessTokenPayload,
} from "@topline/shared-auth";
import type { AuthRequest, OAuthHelpers } from "@cloudflare/workers-oauth-provider";
import worker from "../remote.js";
import type {
  ConnectionClientTarget,
  ConnectionAuthorizationSnapshot,
  PersistedToolPolicy,
} from "../tool-policy.js";
import { AUTHORIZATION_SERVER_ORIGIN, MCP_RESOURCE } from "./constants.js";
import { sha256Base64Url } from "./crypto.js";

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

const ctx = {
  waitUntil() {},
  passThroughOnException() {},
  props: undefined,
} as unknown as ExecutionContext;

function mcpRequest(token: string, locationId?: string): Request {
  const headers = new Headers({
    Authorization: `Bearer ${token}`,
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
  });
  if (locationId) headers.set("X-Topline-Location-Id", locationId);
  return new Request(`${AUTHORIZATION_SERVER_ORIGIN}/mcp`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "compatibility-test", version: "1.0.0" },
      },
    }),
  });
}

function modernMcpRequest(
  token: string,
  method: string,
  params: Record<string, unknown> = {},
): Request {
  const headers = new Headers({
    Authorization: `Bearer ${token}`,
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
    "MCP-Protocol-Version": "2026-07-28",
    "Mcp-Method": method,
  });
  if (method === "tools/call" && typeof params.name === "string") {
    headers.set("Mcp-Name", params.name);
  }
  return new Request(`${AUTHORIZATION_SERVER_ORIGIN}/mcp`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method,
      params: {
        ...params,
        _meta: {
          "io.modelcontextprotocol/protocolVersion": "2026-07-28",
          "io.modelcontextprotocol/clientInfo": {
            name: "compatibility-test",
            version: "1.0.0",
          },
          "io.modelcontextprotocol/clientCapabilities": {},
        },
      },
    }),
  });
}

async function mcpResponseBody(
  response: Response,
): Promise<{ result?: { protocolVersion?: string } }> {
  if ((response.headers.get("Content-Type") ?? "").includes("application/json")) {
    return response.json() as Promise<{ result?: { protocolVersion?: string } }>;
  }
  const data = (await response.text())
    .split("\n")
    .find((line) => line.startsWith("data: "));
  assert.ok(data, "legacy MCP response includes an SSE data event");
  return JSON.parse(data.slice("data: ".length)) as {
    result?: { protocolVersion?: string };
  };
}

test("existing cid, legacy, and raw-PIT bearer shapes remain valid", async () => {
  const kv = new MemoryKv();
  const secret = "test-secret-that-is-long-enough-for-hmac-and-hkdf";
  const env = {
    TOKEN_SIGNING_SECRET: secret,
    TOPLINE_BRAND_NAME: "Topline OS",
    CONNECTIONS: kv as unknown as KVNamespace,
    OAUTH_KV: kv as unknown as KVNamespace,
    OAUTH_FLOW_DO: {},
    CONNECTION_AUTH_DO: new MemoryConnectionAuthNamespace(),
    LOCATION_DO: {},
  } as never;
  const exp = Math.floor(Date.now() / 1000) + 3600;
  const cid = await createConnection(
    kv as unknown as KVNamespace,
    {
      location_id: "location-123",
      pit: "pit-existing-secret",
      brand_name: "Topline OS",
      source: "oauth",
    },
    secret,
  );
  const cidToken = await signToken({ cid, exp } satisfies AccessTokenPayload, secret);
  const legacyToken = await signToken({
    pit: "pit-legacy-secret",
    locationId: "location-123",
    exp,
  } satisfies LegacyAccessTokenPayload, secret);

  for (const request of [
    mcpRequest(cidToken),
    mcpRequest(legacyToken),
    mcpRequest("pit-raw-secret", "location-123"),
  ]) {
    const response = await worker.fetch(request, env, ctx);
    assert.equal(response.status, 200);
    const body = await mcpResponseBody(response);
    assert.equal(body.result?.protocolVersion, "2025-06-18");
  }

  for (const token of [cidToken, legacyToken]) {
    const response = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
        headers: { Authorization: `Bearer ${token}` },
      }),
      env,
      ctx,
    );
    assert.equal(response.status, 200, await response.clone().text());
  }
  const rawPitQuery = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: {
        Authorization: "Bearer pit-raw-secret",
        "X-Topline-Location-Id": "location-123",
      },
    }),
    env,
    ctx,
  );
  assert.equal(rawPitQuery.status, 401);
});

test("invalid bearer receives a canonical protected-resource challenge", async () => {
  const kv = new MemoryKv();
  const response = await worker.fetch(
    mcpRequest("not-a-valid-token"),
    {
      TOKEN_SIGNING_SECRET: "test-secret-that-is-long-enough-for-hmac-and-hkdf",
      CONNECTIONS: kv as unknown as KVNamespace,
      OAUTH_KV: kv as unknown as KVNamespace,
      OAUTH_FLOW_DO: {},
      CONNECTION_AUTH_DO: new MemoryConnectionAuthNamespace(),
      LOCATION_DO: {},
    } as never,
    ctx,
  );

  assert.equal(response.status, 401);
  assert.match(
    response.headers.get("WWW-Authenticate") ?? "",
    /resource_metadata="https:\/\/os-mcp\.topline\.com\/\.well-known\/oauth-protected-resource\/mcp"/,
  );
});

test("query APIs fail closed when provider token scope metadata is missing", async () => {
  const connections = new MemoryKv();
  const oauth = new MemoryKv();
  const secret = "test-secret-that-is-long-enough-for-hmac-and-hkdf";
  const cid = await createConnection(
    connections as unknown as KVNamespace,
    {
      location_id: "location-123",
      pit: "pit-existing-secret",
      brand_name: "Topline OS",
      source: "oauth",
    },
    secret,
  );
  const env = {
    TOKEN_SIGNING_SECRET: secret,
    CONNECTIONS: connections as unknown as KVNamespace,
    OAUTH_KV: oauth as unknown as KVNamespace,
    OAUTH_FLOW_DO: {},
    CONNECTION_AUTH_DO: new MemoryConnectionAuthNamespace(),
    OAUTH_PROVIDER: {
      async unwrapToken(token: string) {
        if (token !== "provider-token") return null;
        return {
          audience: MCP_RESOURCE,
          grant: { props: { cid, oauthClientId: "client-1" } },
        };
      },
    },
    LOCATION_DO: {
      idFromName(value: string) { return value; },
      get() {
        return { async describeSchema() { return { tables: [] }; } };
      },
    },
  } as never;

  const response = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/get-overview`, {
      headers: { Authorization: ["Bear", "er provider-token"].join("") },
    }),
    env,
    ctx,
  );
  assert.equal(response.status, 403);
  assert.equal(
    response.headers.get("WWW-Authenticate"),
    `Bearer realm="OAuth", resource_metadata="${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-protected-resource/mcp", error="insufficient_scope", scope="mcp"`,
  );
});

test("query APIs require one exact canonical provider-token audience", async () => {
  const connections = new MemoryKv();
  const oauth = new MemoryKv();
  const secret = "test-secret-that-is-long-enough-for-hmac-and-hkdf";
  const cid = await createConnection(
    connections as unknown as KVNamespace,
    {
      location_id: "location-123",
      pit: "pit-existing-secret",
      brand_name: "Topline OS",
      source: "oauth",
    },
    secret,
  );
  const audiences: Record<string, unknown> = {
    exact: MCP_RESOURCE,
    single: [MCP_RESOURCE],
    mixed: [MCP_RESOURCE, "https://attacker.example/other"],
    duplicate: [MCP_RESOURCE, MCP_RESOURCE],
    wrong: ["https://attacker.example/other"],
  };
  const env = {
    TOKEN_SIGNING_SECRET: secret,
    CONNECTIONS: connections as unknown as KVNamespace,
    OAUTH_KV: oauth as unknown as KVNamespace,
    OAUTH_FLOW_DO: {},
    CONNECTION_AUTH_DO: new MemoryConnectionAuthNamespace(),
    OAUTH_PROVIDER: {
      async unwrapToken(token: string) {
        const audience = audiences[token];
        if (audience === undefined) return null;
        return {
          audience,
          userId: cid,
          scope: ["mcp"],
          grant: {
            clientId: "client-1",
            scope: ["mcp"],
            props: { cid, oauthClientId: "client-1" },
          },
        };
      },
    },
    LOCATION_DO: {},
  } as never;

  for (const [token, expectedStatus] of [
    ["exact", 200],
    ["single", 200],
    ["mixed", 401],
    ["duplicate", 401],
    ["wrong", 401],
  ] as const) {
    const response = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
        headers: { Authorization: ["Bear", "er ", token].join("") },
      }),
      env,
      ctx,
    );
    assert.equal(response.status, expectedStatus, token);
  }
});

test("the same connection bearer may narrow policy but cannot broaden it", async () => {
  const connections = new MemoryKv();
  const secret = "test-secret-that-is-long-enough-for-hmac-and-hkdf";
  const connectionAuth = new MemoryConnectionAuthNamespace({
    version: 1,
    mode: "allow",
    tool_ids: ["topline_get_contact", "topline_ping"],
  });
  const cid = await createConnection(
    connections as unknown as KVNamespace,
    {
      location_id: "location-policy",
      pit: "pit-policy-secret",
      brand_name: "Topline OS",
      source: "self-serve",
    },
    secret,
  );
  const token = await signToken(
    { cid, exp: Math.floor(Date.now() / 1000) + 3600 } satisfies AccessTokenPayload,
    secret,
  );
  const env = {
    TOKEN_SIGNING_SECRET: secret,
    CONNECTIONS: connections as unknown as KVNamespace,
    CONNECTION_AUTH_DO: connectionAuth,
    OAUTH_KV: new MemoryKv() as unknown as KVNamespace,
    OAUTH_FLOW_DO: {},
    LOCATION_DO: {},
  } as never;
  const update = (body: object) => worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/connection/policy`, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    }),
    env,
    ctx,
  );

  const narrowed = await update({
    expected_policy_version: 1,
    selection: { kind: "custom", tool_ids: ["topline_ping"] },
    target_client: "generic",
  });
  assert.equal(narrowed.status, 200, await narrowed.clone().text());

  const broadened = await update({
    expected_policy_version: 2,
    selection: { kind: "all" },
    target_client: "generic",
  });
  assert.equal(broadened.status, 403, await broadened.clone().text());
  assert.deepEqual(await broadened.json(), {
    error: "reauthorization_required",
    message: "Broadening tool access requires a new authorization.",
  });
});

test("provider-issued OAuth tokens authenticate MCP and query API routes", async () => {
  const connections = new MemoryKv();
  const oauth = new MemoryKv();
  const connectionAuth = new MemoryConnectionAuthNamespace({
    version: 1,
    mode: "allow",
    tool_ids: ["topline_ping"],
  }, "copilot_studio");
  const secret = "test-secret-that-is-long-enough-for-hmac-and-hkdf";
  const env = {
    TOKEN_SIGNING_SECRET: secret,
    TOPLINE_BRAND_NAME: "Topline OS",
    CONNECTIONS: connections as unknown as KVNamespace,
    OAUTH_KV: oauth as unknown as KVNamespace,
    OAUTH_FLOW_DO: new MemoryFlowNamespace(),
    CONNECTION_AUTH_DO: connectionAuth,
    LOCATION_DO: {},
  } as never;
  const cid = await createConnection(
    connections as unknown as KVNamespace,
    {
      location_id: "location-provider",
      pit: "pit-provider-secret",
      brand_name: "Topline OS",
      source: "oauth",
    },
    secret,
  );
  const redirectUri = "https://client.example/callback";
  const registration = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_name: "Route Test Client",
        redirect_uris: [redirectUri],
        grant_types: ["authorization_code"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
      }),
    }),
    env,
    ctx,
  );
  assert.equal(registration.status, 201, await registration.clone().text());
  const clientId = ((await registration.json()) as { client_id: string }).client_id;

  await worker.fetch(new Request(`${AUTHORIZATION_SERVER_ORIGIN}/`), env, ctx);
  const oauthApi = (env as unknown as { OAUTH_PROVIDER?: OAuthHelpers }).OAUTH_PROVIDER;
  assert.ok(oauthApi);
  const verifier = "v".repeat(43);
  const authRequest: AuthRequest = {
    responseType: "code",
    clientId,
    redirectUri,
    scope: ["mcp"],
    state: "route-state",
    codeChallenge: await sha256Base64Url(verifier),
    codeChallengeMethod: "S256",
    resource: MCP_RESOURCE,
    issuer: AUTHORIZATION_SERVER_ORIGIN,
  };
  const authorization = await oauthApi.completeAuthorization({
    request: authRequest,
    userId: cid,
    metadata: { connection_id: cid },
    scope: ["mcp"],
    props: { cid, oauthClientId: clientId },
  });
  const code = new URL(authorization.redirectTo).searchParams.get("code");
  assert.ok(code);
  const tokenResponse = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        client_id: clientId,
        redirect_uri: redirectUri,
        code,
        code_verifier: verifier,
        resource: MCP_RESOURCE,
      }),
    }),
    env,
    ctx,
  );
  assert.equal(tokenResponse.status, 200, await tokenResponse.clone().text());
  const accessToken = ((await tokenResponse.json()) as { access_token: string }).access_token;

  const mcp = await worker.fetch(modernMcpRequest(accessToken, "tools/list"), env, ctx);
  assert.equal(mcp.status, 200, await mcp.clone().text());
  const mcpBody = await mcp.json() as {
    result?: { tools?: Array<{ name: string }> };
  };
  assert.deepEqual(mcpBody.result?.tools?.map((tool) => tool.name), ["topline_ping"]);

  const hiddenCall = await worker.fetch(
    modernMcpRequest(accessToken, "tools/call", {
      name: "topline_describe_data_catalog",
      arguments: {},
    }),
    env,
    ctx,
  );
  assert.equal(hiddenCall.status, 200);
  const hiddenBody = await hiddenCall.json() as { error?: { code?: number } };
  assert.equal(hiddenBody.error?.code, -32602);

  const query = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    }),
    env,
    ctx,
  );
  assert.equal(query.status, 403, await query.clone().text());

  const downscopedAuthorization = await oauthApi.completeAuthorization({
    request: { ...authRequest, state: "downscoped-state" },
    userId: cid,
    metadata: { connection_id: cid },
    scope: ["mcp"],
    props: { cid, oauthClientId: clientId },
  });
  const downscopedCode = new URL(downscopedAuthorization.redirectTo).searchParams.get("code");
  assert.ok(downscopedCode);
  const downscopedTokenResponse = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        client_id: clientId,
        redirect_uri: redirectUri,
        code: downscopedCode,
        code_verifier: verifier,
        resource: MCP_RESOURCE,
        scope: "not-mcp",
      }),
    }),
    env,
    ctx,
  );
  assert.equal(
    downscopedTokenResponse.status,
    200,
    await downscopedTokenResponse.clone().text(),
  );
  const downscopedToken = (
    (await downscopedTokenResponse.json()) as { access_token: string }
  ).access_token;
  const downscopedMcp = await worker.fetch(mcpRequest(downscopedToken), env, ctx);
  assert.equal(downscopedMcp.status, 403);
  const downscopedQuery = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: { Authorization: `Bearer ${downscopedToken}` },
    }),
    env,
    ctx,
  );
  assert.equal(downscopedQuery.status, 403);
});

class MemoryConnectionAuthNamespace {
  readonly states = new Map<string, ConnectionAuthorizationSnapshot>();

  constructor(
    private readonly defaultPolicy: PersistedToolPolicy = { version: 1, mode: "all" },
    private readonly defaultTarget: ConnectionClientTarget = "generic",
  ) {}

  idFromName(name: string): string {
    return name;
  }

  get(id: string) {
    const states = this.states;
    const defaults = { policy: this.defaultPolicy, target: this.defaultTarget };
    const ensure = (locationId: string): ConnectionAuthorizationSnapshot => {
      const existing = states.get(id);
      if (existing) return existing;
      const created: ConnectionAuthorizationSnapshot = {
        schema_version: 1,
        status: "active",
        location_id: locationId,
        client_target: defaults.target,
        policy: defaults.policy,
        policy_version: 1,
        created_at: "2026-08-11T00:00:00.000Z",
        updated_at: "2026-08-11T00:00:00.000Z",
      };
      states.set(id, created);
      return created;
    };
    return {
      async getOrBootstrap(input: { location_id: string }) {
        return ensure(input.location_id);
      },
      async getForManagement(input: { location_id: string }) {
        return ensure(input.location_id);
      },
      async initialize(input: {
        location_id: string;
        policy: PersistedToolPolicy;
        client_target?: ConnectionClientTarget;
      }) {
        const created = ensure(input.location_id);
        const initialized = {
          ...created,
          policy: input.policy,
          client_target: input.client_target ?? "generic",
        };
        states.set(id, initialized);
        return initialized;
      },
      async updatePolicy(input: {
        location_id: string;
        expected_policy_version: number;
        policy: PersistedToolPolicy;
        client_target?: ConnectionClientTarget;
      }) {
        const current = ensure(input.location_id);
        assert.equal(current.policy_version, input.expected_policy_version);
        const updated = {
          ...current,
          policy: input.policy,
          client_target: input.client_target ?? current.client_target,
          policy_version: current.policy_version + 1,
        };
        states.set(id, updated);
        return updated;
      },
      async touch(locationId: string) {
        return ensure(locationId);
      },
    };
  }
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
  code?: { leaseHash: string; expiresAt: number; status: "pending" | "spent" };

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
