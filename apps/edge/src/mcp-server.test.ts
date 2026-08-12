import assert from "node:assert/strict";
import test from "node:test";

import type { ToolDef } from "./tools/types.js";
import { createRemoteMcpHandler } from "./mcp-server.js";

const ECHO_TOOL: ToolDef = {
  name: "topline_echo",
  description: "Echo a value.",
  inputSchema: {
    type: "object",
    properties: { value: {} },
    required: ["value"],
    additionalProperties: false,
  },
  handler: async ({ value }) => ({ value }),
};

const FAILING_TOOL: ToolDef = {
  name: "topline_failing",
  description: "Fail predictably.",
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
  handler: async () => {
    throw new Error("expected failure");
  },
};

const ALPHA_TOOL: ToolDef = {
  name: "topline_alpha",
  description: "Sort first.",
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
  handler: async () => "alpha",
};

const handler = createRemoteMcpHandler({
  tools: [ECHO_TOOL, FAILING_TOOL, ALPHA_TOOL],
});

const ENVELOPE = {
  "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  "io.modelcontextprotocol/clientInfo": { name: "transport-test", version: "1.0.0" },
  "io.modelcontextprotocol/clientCapabilities": {},
};

function requestBody(method: string, params: Record<string, unknown> = {}, id = 1) {
  return { jsonrpc: "2.0", id, method, params };
}

function modernRequest(
  method: string,
  params: Record<string, unknown> = {},
  headerOverrides: Record<string, string> = {},
): Request {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
    "MCP-Protocol-Version": "2026-07-28",
    "Mcp-Method": method,
    ...headerOverrides,
  };
  if (method === "tools/call" && typeof params.name === "string") {
    headers["Mcp-Name"] = params.name;
  }
  return new Request("https://test.local/mcp", {
    method: "POST",
    headers,
    body: JSON.stringify(requestBody(method, { ...params, _meta: ENVELOPE })),
  });
}

async function postModern(
  method: string,
  params: Record<string, unknown> = {},
  headerOverrides: Record<string, string> = {},
): Promise<{ response: Response; body: Record<string, any> }> {
  const response = await handler.fetch(modernRequest(method, params, headerOverrides));
  return { response, body: (await response.json()) as Record<string, any> };
}

async function postLegacy(method: string, params: Record<string, unknown> = {}, id = 1) {
  const response = await handler.fetch(
    new Request("https://test.local/mcp", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
      },
      body: JSON.stringify(requestBody(method, params, id)),
    }),
  );
  const contentType = response.headers.get("Content-Type") ?? "";
  if (contentType.includes("application/json")) {
    return { response, body: (await response.json()) as Record<string, any> };
  }
  const event = (await response.text())
    .split("\n")
    .find((line) => line.startsWith("data: "));
  assert.ok(event, "legacy SSE response includes a data event");
  return {
    response,
    body: JSON.parse(event.slice("data: ".length)) as Record<string, any>,
  };
}

test.after(async () => {
  await handler.close();
});

test("modern server/discover works without initialize or a session id", async () => {
  const { response, body } = await postModern("server/discover");

  assert.equal(response.status, 200);
  assert.equal(response.headers.has("Mcp-Session-Id"), false);
  assert.equal(body.result.resultType, "complete");
  assert.deepEqual(body.result.supportedVersions, ["2026-07-28"]);
  assert.equal(body.result.cacheScope, "private");
  assert.equal(typeof body.result.ttlMs, "number");
});

test("modern tools/list is deterministic and carries private cache hints", async () => {
  const { response, body } = await postModern("tools/list");

  assert.equal(response.status, 200);
  assert.equal(body.result.resultType, "complete");
  assert.deepEqual(
    body.result.tools.map((tool: { name: string }) => tool.name),
    ["topline_alpha", "topline_echo", "topline_failing"],
  );
  assert.equal(body.result.cacheScope, "private");
  assert.equal(body.result.ttlMs, 30_000);
});

test("remote registry preserves all tools and raw-PIT action-only compatibility", async () => {
  const productionHandler = createRemoteMcpHandler();
  try {
    const connected = await productionHandler.fetch(modernRequest("tools/list"), {
      authInfo: { token: "connected", clientId: "cid-test", scopes: ["mcp"] },
    });
    const connectedBody = (await connected.json()) as Record<string, any>;
    const connectedNames = connectedBody.result.tools.map((tool: { name: string }) => tool.name);

    const raw = await productionHandler.fetch(modernRequest("tools/list"), {
      authInfo: {
        token: "pit-test",
        clientId: "legacy-raw-pit",
        scopes: ["mcp"],
        extra: { rawPitBearer: true },
      },
    });
    const rawBody = (await raw.json()) as Record<string, any>;
    const rawNames = rawBody.result.tools.map((tool: { name: string }) => tool.name);

    const hiddenCall = await productionHandler.fetch(
      modernRequest("tools/call", {
        name: "topline_execute_query",
        arguments: { sql: "SELECT 1" },
      }),
      {
        authInfo: {
          token: "pit-test",
          clientId: "legacy-raw-pit",
          scopes: ["mcp"],
          extra: { rawPitBearer: true },
        },
      },
    );
    const hiddenCallBody = (await hiddenCall.json()) as Record<string, any>;

    assert.equal(connectedNames.length, 127);
    assert.equal(rawNames.length, 115);
    assert.deepEqual(connectedNames, [...connectedNames].sort());
    assert.deepEqual(rawNames, [...rawNames].sort());
    assert.ok(connectedNames.includes("topline_execute_query"));
    assert.equal(rawNames.includes("topline_execute_query"), false);
    assert.equal(hiddenCallBody.error.code, -32602);
  } finally {
    await productionHandler.close();
  }
});

test("modern tools/call discriminates complete structured results", async () => {
  const { body } = await postModern("tools/call", {
    name: "topline_echo",
    arguments: { value: "hello" },
  });

  assert.equal(body.result.resultType, "complete");
  assert.deepEqual(body.result.structuredContent, { value: "hello" });
  assert.match(body.result.content[0].text, /hello/);
});

test("text-only tool results remain complete text results", async () => {
  const { body } = await postModern("tools/call", {
    name: "topline_alpha",
    arguments: {},
  });

  assert.equal(body.result.resultType, "complete");
  assert.equal(body.result.content[0].text, "alpha");
  assert.equal("structuredContent" in body.result, false);
});

test("tool execution failures remain isError results", async () => {
  const { response, body } = await postModern("tools/call", {
    name: "topline_failing",
    arguments: {},
  });

  assert.equal(response.status, 200);
  assert.equal(body.result.resultType, "complete");
  assert.equal(body.result.isError, true);
  assert.equal(body.result.content[0].text, "expected failure");
});

test("unknown tools are protocol invalid-params errors", async () => {
  const { body } = await postModern("tools/call", {
    name: "topline_missing",
    arguments: {},
  });

  assert.equal(body.error.code, -32602);
});

test("legacy stateless list and call work without initialize or session state", async () => {
  const listed = await postLegacy("tools/list");
  assert.equal(listed.response.status, 200);
  assert.equal(listed.body.result.resultType, undefined);
  assert.deepEqual(
    listed.body.result.tools.map((tool: { name: string }) => tool.name),
    ["topline_alpha", "topline_echo", "topline_failing"],
  );

  const called = await postLegacy("tools/call", {
    name: "topline_echo",
    arguments: { value: "legacy" },
  });
  assert.equal(called.response.status, 200);
  assert.equal(called.body.result.resultType, undefined);
  assert.deepEqual(called.body.result.structuredContent, { value: "legacy" });
});

test("legacy initialize remains explicitly supported", async () => {
  const { response, body } = await postLegacy("initialize", {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "legacy-test", version: "1.0.0" },
  });

  assert.equal(response.status, 200);
  assert.equal(body.result.protocolVersion, "2025-06-18");
  assert.equal(body.result.serverInfo.version, "0.2.0");
});

test("malformed modern metadata cannot downgrade into the legacy path", async () => {
  const malformedEnvelope = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  };
  const response = await handler.fetch(
    new Request("https://test.local/mcp", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": "tools/list",
      },
      body: JSON.stringify(requestBody("tools/list", { _meta: malformedEnvelope })),
    }),
  );
  const body = (await response.json()) as Record<string, any>;

  assert.equal(body.result, undefined);
  assert.ok(body.error);
});

test("modern header and body mismatches are rejected", async () => {
  const { response, body } = await postModern("tools/list", {}, { "Mcp-Method": "ping" });

  assert.equal(response.status, 400);
  assert.equal(body.error.code, -32020);
});
