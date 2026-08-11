import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const edgeConfigUrl = new URL("../../edge/wrangler.toml", import.meta.url);
const syncConfigUrl = new URL("../wrangler.toml", import.meta.url);
const syncSourceUrl = new URL("./index.ts", import.meta.url);

function bindingId(config: string, binding: string): string {
  const blocks = config.split("[[kv_namespaces]]").slice(1);
  const block = blocks.find((candidate) =>
    new RegExp(`\\bbinding\\s*=\\s*"${binding}"`).test(candidate),
  );
  assert.ok(block, `missing ${binding} KV binding`);
  const id = block.match(/\bid\s*=\s*"([^"]+)"/)?.[1];
  assert.ok(id, `missing ${binding} KV namespace id`);
  return id;
}

test("OAuth storage is distinct from the connection directory scanned by sync", async () => {
  const [edgeConfig, syncConfig, syncSource] = await Promise.all([
    readFile(edgeConfigUrl, "utf8"),
    readFile(syncConfigUrl, "utf8"),
    readFile(syncSourceUrl, "utf8"),
  ]);

  const edgeConnections = bindingId(edgeConfig, "CONNECTIONS");
  const syncConnections = bindingId(syncConfig, "CONNECTIONS");
  const oauth = bindingId(edgeConfig, "OAUTH_KV");

  assert.equal(edgeConnections, syncConnections);
  assert.notEqual(oauth, edgeConnections);
  assert.equal(oauth, "00000000000000000000000000000000");
  assert.match(syncSource, /env\.CONNECTIONS\.list\(/);
  assert.doesNotMatch(syncSource, /OAUTH_KV/);
});