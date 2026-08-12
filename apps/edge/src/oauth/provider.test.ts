import { test } from "node:test";
import assert from "node:assert/strict";
import {
  AUTHORIZATION_SERVER_ORIGIN,
  MCP_RESOURCE,
  createOAuthWorker,
} from "./provider.js";

const noopHandler = {
  fetch() {
    return new Response("not found", { status: 404 });
  },
};

const executionContext = {
  waitUntil() {},
  passThroughOnException() {},
  props: undefined,
} as unknown as ExecutionContext;

function createWorker() {
  return createOAuthWorker({
    apiHandler: noopHandler,
    defaultHandler: noopHandler,
  });
}

test("OAuth discovery publishes separate RFC 9728 and RFC 8414 documents", async () => {
  const worker = createWorker();
  const env = { OAUTH_KV: {} as KVNamespace };

  const protectedResponse = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-protected-resource/mcp`),
    env,
    executionContext,
  );
  assert.equal(protectedResponse.status, 200);
  const protectedMetadata = await protectedResponse.json() as Record<string, unknown>;
  assert.equal(protectedMetadata.resource, MCP_RESOURCE);
  assert.deepEqual(protectedMetadata.authorization_servers, [AUTHORIZATION_SERVER_ORIGIN]);
  assert.deepEqual(protectedMetadata.scopes_supported, ["mcp"]);
  assert.deepEqual(protectedMetadata.bearer_methods_supported, ["header"]);
  assert.equal("issuer" in protectedMetadata, false);

  const authorizationResponse = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-authorization-server`),
    env,
    executionContext,
  );
  assert.equal(authorizationResponse.status, 200);
  const authorizationMetadata = await authorizationResponse.json() as Record<string, unknown>;
  assert.equal(authorizationMetadata.issuer, AUTHORIZATION_SERVER_ORIGIN);
  assert.equal(authorizationMetadata.authorization_endpoint, `${AUTHORIZATION_SERVER_ORIGIN}/authorize`);
  assert.equal(authorizationMetadata.token_endpoint, `${AUTHORIZATION_SERVER_ORIGIN}/token`);
  assert.equal(authorizationMetadata.registration_endpoint, `${AUTHORIZATION_SERVER_ORIGIN}/register`);
  assert.deepEqual(authorizationMetadata.code_challenge_methods_supported, ["S256"]);
  assert.equal(authorizationMetadata.authorization_response_iss_parameter_supported, true);
  assert.equal(authorizationMetadata.client_id_metadata_document_supported, true);
  assert.equal("resource" in authorizationMetadata, false);
});

test("unauthorized MCP requests advertise canonical protected-resource metadata", async () => {
  const response = await createWorker().fetch(
    new Request(MCP_RESOURCE, { method: "POST" }),
    { OAUTH_KV: {} as KVNamespace },
    executionContext,
  );

  assert.equal(response.status, 401);
  assert.equal(
    response.headers.get("WWW-Authenticate"),
    `Bearer realm="OAuth", resource_metadata="${AUTHORIZATION_SERVER_ORIGIN}/.well-known/oauth-protected-resource/mcp", scope="mcp"`,
  );
});

test("browser MCP requests use an exact configured Origin allowlist", async () => {
  const worker = createWorker();
  const env = {
    OAUTH_KV: {} as KVNamespace,
    MCP_ALLOWED_ORIGINS: "https://trusted.example,https://other.example",
  };

  const trusted = await worker.fetch(
    new Request(MCP_RESOURCE, {
      method: "POST",
      headers: { Origin: "https://trusted.example" },
    }),
    env,
    executionContext,
  );
  assert.equal(trusted.status, 401);
  assert.equal(trusted.headers.get("Access-Control-Allow-Origin"), "https://trusted.example");
  assert.match(trusted.headers.get("Vary") ?? "", /Origin/);

  const untrusted = await worker.fetch(
    new Request(MCP_RESOURCE, {
      method: "POST",
      headers: { Origin: "https://attacker.example" },
    }),
    env,
    executionContext,
  );
  assert.equal(untrusted.status, 403);
  assert.equal(untrusted.headers.get("Access-Control-Allow-Origin"), null);

  const preflight = await worker.fetch(
    new Request(MCP_RESOURCE, {
      method: "OPTIONS",
      headers: {
        Origin: "https://trusted.example",
        "Access-Control-Request-Headers": "Mcp-Param-Trace, X-Not-Mcp",
      },
    }),
    env,
    executionContext,
  );
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.get("Access-Control-Allow-Origin"), "https://trusted.example");
  assert.match(
    preflight.headers.get("Access-Control-Allow-Headers") ?? "",
    /X-Topline-Location-Id/,
  );
  const allowHeaders = preflight.headers.get("Access-Control-Allow-Headers") ?? "";
  assert.match(allowHeaders, /Mcp-Method/);
  assert.match(allowHeaders, /Mcp-Name/);
  assert.match(allowHeaders, /Mcp-Param-Trace/);
  assert.doesNotMatch(allowHeaders, /X-Not-Mcp/);
  assert.match(preflight.headers.get("Access-Control-Allow-Methods") ?? "", /DELETE/);
  const exposeHeaders = preflight.headers.get("Access-Control-Expose-Headers") ?? "";
  assert.match(exposeHeaders, /MCP-Protocol-Version/);
  assert.match(exposeHeaders, /Mcp-Method/);
  assert.match(exposeHeaders, /Mcp-Name/);
  assert.doesNotMatch(exposeHeaders, /Mcp-Session-Id/);

  const query = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: { Origin: "https://trusted.example" },
    }),
    env,
    executionContext,
  );
  assert.equal(query.status, 404);
  assert.equal(query.headers.get("Access-Control-Allow-Origin"), "https://trusted.example");

  const queryPreflight = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      method: "OPTIONS",
      headers: {
        Origin: "https://trusted.example",
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "authorization,x-topline-location-id",
      },
    }),
    env,
    executionContext,
  );
  assert.equal(queryPreflight.status, 204);
  assert.equal(queryPreflight.headers.get("Access-Control-Allow-Origin"), "https://trusted.example");
  assert.match(
    queryPreflight.headers.get("Access-Control-Allow-Headers") ?? "",
    /X-Topline-Location-Id/,
  );

  const blockedQuery = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: { Origin: "https://evil.example" },
    }),
    env,
    executionContext,
  );
  assert.equal(blockedQuery.status, 403);

  const nullOrigin = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`, {
      headers: { Origin: "null" },
    }),
    env,
    executionContext,
  );
  assert.equal(nullOrigin.status, 403);
  assert.equal(nullOrigin.headers.get("Access-Control-Allow-Origin"), null);

  const absentOrigin = await worker.fetch(
    new Request(`${AUTHORIZATION_SERVER_ORIGIN}/query/api/catalog`),
    env,
    executionContext,
  );
  assert.notEqual(absentOrigin.status, 403);
  assert.equal(absentOrigin.headers.get("Access-Control-Allow-Origin"), null);
});

test("provider-routed MCP and metadata paths share the strict Origin decision", async () => {
  const worker = createWorker();
  const env = {
    OAUTH_KV: {} as KVNamespace,
    MCP_ALLOWED_ORIGINS: "https://trusted.example",
  };
  const paths = [
    "/mcp",
    "/mcp/",
    "/mcp/foo",
    "/mcp.json",
    "/mcp-attacker",
    "/.well-known/oauth-protected-resource",
    "/.well-known/oauth-protected-resource/",
    "/.well-known/oauth-protected-resource/mcp",
    "/.well-known/oauth-authorization-server",
  ];

  for (const path of paths) {
    for (const method of ["GET", "OPTIONS"]) {
      const hostile = await worker.fetch(
        new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
          method,
          headers: { Origin: "https://attacker.example" },
        }),
        env,
        executionContext,
      );
      assert.equal(hostile.status, 403, `${method} ${path}`);
      assert.equal(
        hostile.headers.get("Access-Control-Allow-Origin"),
        null,
        `${method} ${path}`,
      );

      const allowed = await worker.fetch(
        new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
          method,
          headers: { Origin: "https://trusted.example" },
        }),
        env,
        executionContext,
      );
      assert.notEqual(allowed.status, 403, `${method} ${path}`);
      assert.equal(
        allowed.headers.get("Access-Control-Allow-Origin"),
        "https://trusted.example",
        `${method} ${path}`,
      );
      assert.match(allowed.headers.get("Vary") ?? "", /(?:^|,\s*)Origin(?:,|$)/i);
    }
  }
});

test("authorize and connect forms load from allowlisted browser origins but POST stays same-origin", async () => {
  const worker = createWorker();
  const env = {
    OAUTH_KV: {} as KVNamespace,
    MCP_ALLOWED_ORIGINS: "https://claude.ai,https://chatgpt.com,https://os-mcp.topline.com",
  };

  // GET: Claude/ChatGPT connector UIs must be able to load the consent and
  // connect forms from their own origin (regression: previously 403).
  for (const path of ["/authorize", "/connect"]) {
    const claudeGet = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
        headers: { Origin: "https://claude.ai" },
      }),
      env,
      executionContext,
    );
    assert.notEqual(claudeGet.status, 403, `GET ${path} from claude.ai`);
    assert.equal(claudeGet.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");

    const chatgptGet = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
        headers: { Origin: "https://chatgpt.com" },
      }),
      env,
      executionContext,
    );
    assert.notEqual(chatgptGet.status, 403, `GET ${path} from chatgpt.com`);

    // OPTIONS preflight from a client origin passes the gate too.
    const preflight = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
        method: "OPTIONS",
        headers: { Origin: "https://claude.ai", "Access-Control-Request-Method": "GET" },
      }),
      env,
      executionContext,
    );
    assert.equal(preflight.status, 204, `OPTIONS ${path} from claude.ai`);
    assert.equal(preflight.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");

    // Hostile origins stay blocked on GET.
    const hostileGet = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
        headers: { Origin: "https://attacker.example" },
      }),
      env,
      executionContext,
    );
    assert.equal(hostileGet.status, 403, `GET ${path} from attacker`);

    // POST: credential submission remains same-origin-only (CSRF boundary).
    const claudePost = await worker.fetch(
      new Request(`${AUTHORIZATION_SERVER_ORIGIN}${path}`, {
        method: "POST",
        headers: { Origin: "https://claude.ai", "Content-Type": "application/x-www-form-urlencoded" },
        body: "pit=pit-test&locationId=loc",
      }),
      env,
      executionContext,
    );
    assert.equal(claudePost.status, 403, `POST ${path} from claude.ai must stay forbidden`);
  }
});