import assert from "node:assert/strict";
import test from "node:test";

import { handleMcpHttpRequest } from "./mcp-http.js";

const ALLOWED_ORIGINS = [
  "https://claude.ai",
  "https://chatgpt.com",
  "https://os-mcp.topline.com",
  "http://localhost:6274",
  "http://127.0.0.1:6274",
  "http://[::1]:6274",
  "http://localhost:8787",
  "http://127.0.0.1:8787",
  "http://[::1]:8787",
] as const;

const REJECTED_ORIGINS = [
  "",
  "   ",
  "null",
  "not an origin",
  "https://claude.ai, https://evil.example",
  "https://claude.ai https://evil.example",
  "http://claude.ai",
  "https://claude.ai:443",
  "https://claude.ai:8443",
  "https://claude.ai:not-a-port",
  "https://claude.ai:65536",
  "https://claude.ai/",
  "https://claude.ai/path",
  "https://claude.ai?query=1",
  "https://claude.ai#fragment",
  "https://user@claude.ai",
  "HTTPS://CLAUDE.AI",
  "https://claude.ai.",
  "https://claudé.ai",
  "https://xn--claud-epa.ai",
  "https://%63laude.ai",
  "https://claude%2Eai",
  "https:claude.ai",
  "https:/claude.ai",
  "https:///claude.ai",
  "https://claude.ai\\evil",
  "https://subdomain.claude.ai",
  "wss://claude.ai",
  "file://claude.ai",
  "data:text/plain,hello",
  "http://localhost",
  "https://localhost:6274",
  "http://localhost:80",
  "http://localhost:6275",
  "http://127.1:8787",
  "http://2130706433:8787",
  "http://0x7f000001:8787",
  "http://0177.0.0.1:8787",
  "http://[0:0:0:0:0:0:0:1]:8787",
] as const;

const REQUIRED_HEADERS = [
  "authorization",
  "content-type",
  "mcp-method",
  "mcp-name",
  "mcp-protocol-version",
  "x-topline-location-id",
];

const EXPOSED_HEADERS = ["mcp-method", "mcp-name", "mcp-protocol-version"];

function tokens(value: string | null): string[] {
  return (value ?? "")
    .split(",")
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);
}

function request(method: string, origin?: string, headers: Record<string, string> = {}): Request {
  const requestHeaders = new Headers(headers);
  if (origin !== undefined) requestHeaders.set("Origin", origin);
  return new Request("https://os-mcp.topline.com/mcp?probe=1", {
    method,
    headers: requestHeaders,
  });
}

async function handled(
  method: string,
  origin?: string,
  downstream: Response = new Response("ok"),
): Promise<{ response: Response; dispatches: number }> {
  let dispatches = 0;
  const response = await handleMcpHttpRequest(request(method, origin), async () => {
    dispatches += 1;
    return downstream;
  });
  return { response, dispatches };
}

function assertSingleVaryOrigin(response: Response, preserved: string[] = []): void {
  const vary = tokens(response.headers.get("Vary"));
  assert.deepEqual(vary, [...preserved, "origin"]);
}

for (const origin of ALLOWED_ORIGINS) {
  test(`MCP allows exact serialized Origin ${origin}`, async () => {
    const { response, dispatches } = await handled("POST", origin);

    assert.equal(response.status, 200);
    assert.equal(dispatches, 1);
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), origin);
    assertSingleVaryOrigin(response);
  });
}

for (const origin of REJECTED_ORIGINS) {
  test(`MCP rejects non-allowlisted serialized Origin ${JSON.stringify(origin)}`, async () => {
    const { response, dispatches } = await handled("POST", origin);
    const body = await response.json();

    assert.equal(response.status, 403);
    assert.equal(dispatches, 0);
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
    assert.deepEqual(body, {
      jsonrpc: "2.0",
      id: null,
      error: { code: -32000, message: "Invalid Origin header" },
    });
    assertSingleVaryOrigin(response);
  });
}

test("MCP preserves absent-Origin non-browser clients", async () => {
  const { response, dispatches } = await handled("POST");

  assert.equal(response.status, 200);
  assert.equal(dispatches, 1);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
  assertSingleVaryOrigin(response);
});

test("MCP removes downstream ACAO when Origin is absent", async () => {
  const { response } = await handled(
    "POST",
    undefined,
    new Response("ok", { headers: { "Access-Control-Allow-Origin": "*" } }),
  );

  assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
  assertSingleVaryOrigin(response);
});

for (const method of ["OPTIONS", "POST", "GET", "DELETE"]) {
  test(`MCP rejects invalid Origin before ${method} dispatch`, async () => {
    const { response, dispatches } = await handled(method, "https://evil.example");

    assert.equal(response.status, 403);
    assert.equal(dispatches, 0);
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
    assertSingleVaryOrigin(response);
  });
}

test("MCP handles an allowed preflight without dispatch", async () => {
  const { response, dispatches } = await handled("OPTIONS", "https://claude.ai");

  assert.equal(response.status, 204);
  assert.equal(dispatches, 0);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");
  assert.equal(response.headers.get("Access-Control-Allow-Methods"), "POST, OPTIONS");
  assertSingleVaryOrigin(response);
});

for (const method of ["POST", "GET", "DELETE"]) {
  test(`MCP dispatches allowed ${method} requests`, async () => {
    const { response, dispatches } = await handled(
      method,
      "https://claude.ai",
      new Response(null, { status: method === "POST" ? 200 : 405 }),
    );

    assert.equal(response.status, method === "POST" ? 200 : 405);
    assert.equal(dispatches, 1);
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");
    assertSingleVaryOrigin(response);
  });
}

for (const status of [400, 401, 405, 500]) {
  test(`MCP applies validated CORS to downstream ${status} errors`, async () => {
    const { response } = await handled(
      "POST",
      "https://chatgpt.com",
      new Response("downstream error", { status }),
    );

    assert.equal(response.status, status);
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://chatgpt.com");
    assertSingleVaryOrigin(response);
  });
}

test("MCP preserves and deduplicates downstream Vary values", async () => {
  const { response } = await handled(
    "POST",
    "https://claude.ai",
    new Response("ok", { headers: { Vary: "Accept-Encoding, origin, Origin" } }),
  );

  assertSingleVaryOrigin(response, ["accept-encoding"]);
});

test("MCP preserves a downstream Vary wildcard", async () => {
  const { response } = await handled(
    "POST",
    "https://claude.ai",
    new Response("ok", { headers: { Vary: "*" } }),
  );

  assert.equal(response.headers.get("Vary"), "*");
});

test("MCP preflight allows standard and requested Mcp-Param headers only", async () => {
  const preflight = request("OPTIONS", "https://claude.ai", {
    "Access-Control-Request-Method": "POST",
    "Access-Control-Request-Headers":
      "Authorization, Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Param-Account-Id, X-Injected",
  });
  let dispatches = 0;
  const response = await handleMcpHttpRequest(preflight, async () => {
    dispatches += 1;
    return new Response("should not dispatch");
  });
  const allowed = tokens(response.headers.get("Access-Control-Allow-Headers"));

  assert.equal(response.status, 204);
  assert.equal(dispatches, 0);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");
  assert.equal(response.headers.get("Access-Control-Allow-Methods"), "POST, OPTIONS");
  for (const header of REQUIRED_HEADERS) assert.ok(allowed.includes(header), header);
  assert.ok(allowed.includes("mcp-param-account-id"));
  assert.equal(allowed.includes("x-injected"), false);
  assert.equal(allowed.includes("mcp-session-id"), false);
  assertSingleVaryOrigin(response);
});

test("MCP responses expose modern protocol and generated result headers without sessions", async () => {
  const response = await handleMcpHttpRequest(
    request("POST", "https://claude.ai"),
    async () => new Response("ok", {
      headers: {
        "Mcp-Param-Next-Cursor": "next",
        "Mcp-Result-Cache-Scope": "private",
        "Mcp-Result-Ttl-Ms": "30000",
      },
    }),
  );
  const exposed = tokens(response.headers.get("Access-Control-Expose-Headers"));

  for (const header of EXPOSED_HEADERS) assert.ok(exposed.includes(header), header);
  assert.ok(exposed.includes("mcp-param-next-cursor"));
  assert.ok(exposed.includes("mcp-result-cache-scope"));
  assert.ok(exposed.includes("mcp-result-ttl-ms"));
  assert.equal(exposed.includes("mcp-session-id"), false);
});
