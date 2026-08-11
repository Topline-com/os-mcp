import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";

const tsxCli = fileURLToPath(new URL("../../../node_modules/tsx/dist/cli.mjs", import.meta.url));
const serverSource = fileURLToPath(new URL("./index.ts", import.meta.url));

async function listFromStdio(versionNegotiation?: ConstructorParameters<typeof Client>[1]) {
  const client = new Client(
    { name: "stdio-test", version: "1.0.0" },
    versionNegotiation,
  );
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [tsxCli, serverSource],
    stderr: "pipe",
  });
  try {
    await client.connect(transport);
    const result = await client.listTools();
    return {
      era: client.getProtocolEra(),
      names: result.tools.map((tool) => tool.name),
    };
  } finally {
    await client.close();
  }
}

test("stdio serves the modern protocol era", async () => {
  const result = await listFromStdio({
    versionNegotiation: { mode: { pin: "2026-07-28" } },
  });

  assert.equal(result.era, "modern");
  assert.equal(result.names.length, 115);
  assert.deepEqual(result.names, [...result.names].sort());
});

test("stdio explicitly serves legacy initialize-era clients", async () => {
  const result = await listFromStdio();

  assert.equal(result.era, "legacy");
  assert.equal(result.names.length, 115);
  assert.deepEqual(result.names, [...result.names].sort());
});
