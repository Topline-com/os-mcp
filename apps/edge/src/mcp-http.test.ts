import assert from "node:assert/strict";
import test from "node:test";

import { handleMcpHttpRequest } from "./mcp-http.js";

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

test("MCP rejects a hostile Origin", async () => {
  const request = new Request("https://os-mcp.topline.com/mcp", {
    method: "POST",
    headers: { Origin: "https://evil.example" },
  });
  let dispatched = false;

  const response = await handleMcpHttpRequest(request, async () => {
    dispatched = true;
    return new Response("should not dispatch");
  });

  assert.equal(response.status, 403);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
  assert.match(await response.text(), /Invalid Origin/);
  assert.equal(dispatched, false);
});

test("MCP dispatches an explicitly allowed browser Origin with exact CORS", async () => {
  const request = new Request("https://os-mcp.topline.com/mcp", {
    method: "POST",
    headers: { Origin: "https://claude.ai" },
  });

  const response = await handleMcpHttpRequest(request, async () => new Response("ok"));

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");
  assert.match(response.headers.get("Vary") ?? "", /(?:^|,\s*)Origin(?:,|$)/);
});

test("MCP dispatches requests without Origin for non-browser clients", async () => {
  const request = new Request("https://os-mcp.topline.com/mcp", { method: "POST" });

  const response = await handleMcpHttpRequest(request, async () => new Response("ok"));

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
});

for (const origin of ["not an origin", "null"]) {
  test(`MCP rejects malformed Origin ${JSON.stringify(origin)}`, async () => {
    const request = new Request("https://os-mcp.topline.com/mcp", {
      method: "POST",
      headers: { Origin: origin },
    });
    let dispatched = false;

    const response = await handleMcpHttpRequest(request, async () => {
      dispatched = true;
      return new Response("should not dispatch");
    });

    assert.equal(response.status, 403);
    assert.equal(dispatched, false);
  });
}

test("MCP rejects hostile preflight before CORS or dispatch", async () => {
  const request = new Request("https://os-mcp.topline.com/mcp", {
    method: "OPTIONS",
    headers: { Origin: "https://evil.example" },
  });
  let dispatched = false;

  const response = await handleMcpHttpRequest(request, async () => {
    dispatched = true;
    return new Response("should not dispatch");
  });

  assert.equal(response.status, 403);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
  assert.equal(dispatched, false);
});

test("MCP preflight allows standard and requested Mcp-Param headers", async () => {
  const request = new Request("https://test.local/mcp", {
    method: "OPTIONS",
    headers: {
      Origin: "https://claude.ai",
      "Access-Control-Request-Headers":
        "Authorization, Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Param-Account-Id",
    },
  });

  const response = await handleMcpHttpRequest(request, async () => new Response("should not dispatch"));
  const allowed = tokens(response.headers.get("Access-Control-Allow-Headers"));

  assert.equal(response.status, 204);
  assert.equal(response.headers.get("Access-Control-Allow-Origin"), "https://claude.ai");
  for (const header of REQUIRED_HEADERS) assert.ok(allowed.includes(header), header);
  assert.ok(allowed.includes("mcp-param-account-id"));
  assert.equal(allowed.includes("mcp-session-id"), false);
});

test("MCP responses expose modern protocol and generated result headers without sessions", async () => {
  const request = new Request("https://test.local/mcp", {
    method: "POST",
    headers: { Origin: "https://claude.ai" },
  });
  const response = await handleMcpHttpRequest(
    request,
    async () => new Response("ok", {
      headers: {
        "Mcp-Result-Cache-Scope": "private",
        "Mcp-Result-Ttl-Ms": "30000",
      },
    }),
  );
  const exposed = tokens(response.headers.get("Access-Control-Expose-Headers"));

  for (const header of EXPOSED_HEADERS) assert.ok(exposed.includes(header), header);
  assert.ok(exposed.includes("mcp-result-cache-scope"));
  assert.ok(exposed.includes("mcp-result-ttl-ms"));
  assert.equal(exposed.includes("mcp-session-id"), false);
});

test("non-MCP custom request headers are not reflected into preflight", async () => {
  const request = new Request("https://test.local/mcp", {
    method: "OPTIONS",
    headers: { "Access-Control-Request-Headers": "X-Injected, Mcp-Param-Region" },
  });
  const response = await handleMcpHttpRequest(request, async () => new Response("should not dispatch"));
  const allowed = tokens(response.headers.get("Access-Control-Allow-Headers"));

  assert.equal(allowed.includes("x-injected"), false);
  assert.ok(allowed.includes("mcp-param-region"));
});
