import assert from "node:assert/strict";
import test from "node:test";

import { applyMcpCors, mcpPreflightResponse } from "./mcp-http.js";

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

test("MCP preflight allows standard and requested Mcp-Param headers", () => {
  const request = new Request("https://test.local/mcp", {
    method: "OPTIONS",
    headers: {
      Origin: "https://client.example",
      "Access-Control-Request-Headers":
        "Authorization, Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Param-Account-Id",
    },
  });

  const response = mcpPreflightResponse(request);
  const allowed = tokens(response.headers.get("Access-Control-Allow-Headers"));

  assert.equal(response.status, 204);
  for (const header of REQUIRED_HEADERS) assert.ok(allowed.includes(header), header);
  assert.ok(allowed.includes("mcp-param-account-id"));
  assert.equal(allowed.includes("mcp-session-id"), false);
});

test("MCP responses expose modern protocol and generated result headers without sessions", () => {
  const request = new Request("https://test.local/mcp", {
    method: "POST",
    headers: { Origin: "https://client.example" },
  });
  const response = applyMcpCors(
    request,
    new Response("ok", {
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

test("non-MCP custom request headers are not reflected into preflight", () => {
  const request = new Request("https://test.local/mcp", {
    method: "OPTIONS",
    headers: { "Access-Control-Request-Headers": "X-Injected, Mcp-Param-Region" },
  });
  const response = mcpPreflightResponse(request);
  const allowed = tokens(response.headers.get("Access-Control-Allow-Headers"));

  assert.equal(allowed.includes("x-injected"), false);
  assert.ok(allowed.includes("mcp-param-region"));
});
