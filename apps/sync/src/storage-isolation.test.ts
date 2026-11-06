import { test } from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";

const edgeConfigUrl = new URL("../../edge/wrangler.toml", import.meta.url);
const syncConfigUrl = new URL("../wrangler.toml", import.meta.url);
const syncSourceDirUrl = new URL("./", import.meta.url);
const syncCronUrl = new URL("./cron.ts", import.meta.url);

async function syncSources(): Promise<string[]> {
  const names = (await readdir(syncSourceDirUrl))
    .filter((name) => name.endsWith(".ts") && !name.endsWith(".test.ts"));
  return Promise.all(names.map((name) => readFile(new URL(name, syncSourceDirUrl), "utf8")));
}

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

function migrationBlock(config: string, tag: string): string {
  const blocks = config.split("[[migrations]]").slice(1);
  const block = blocks.find((candidate) =>
    new RegExp(`\\btag\\s*=\\s*"${tag}"`).test(candidate),
  );
  assert.ok(block, `missing ${tag} Durable Object migration`);
  return block;
}

test("OAuth storage is distinct from the connection directory scanned by sync", async () => {
  const [edgeConfig, syncConfig, syncCron, sources] = await Promise.all([
    readFile(edgeConfigUrl, "utf8"),
    readFile(syncConfigUrl, "utf8"),
    readFile(syncCronUrl, "utf8"),
    syncSources(),
  ]);

  const edgeConnections = bindingId(edgeConfig, "CONNECTIONS");
  const syncConnections = bindingId(syncConfig, "CONNECTIONS");
  const oauth = bindingId(edgeConfig, "OAUTH_KV");

  assert.equal(edgeConnections, syncConnections);
  assert.notEqual(oauth, edgeConnections);
  assert.match(oauth, /^[0-9a-f]{32}$/);
  assert.notEqual(oauth, "00000000000000000000000000000000");
  assert.match(syncCron, /env\.CONNECTIONS\.list\(/);
  assert.ok(sources.length > 0);
  for (const source of sources) assert.doesNotMatch(source, /OAUTH_KV/);
});

test("Durable Object migration history provisions every new class with SQLite", async () => {
  const edgeConfig = await readFile(edgeConfigUrl, "utf8");

  assert.match(migrationBlock(edgeConfig, "v1"), /new_sqlite_classes\s*=\s*\["LocationDO"\]/);
  assert.match(migrationBlock(edgeConfig, "v2"), /new_sqlite_classes\s*=\s*\["OAuthFlowDO"\]/);
  assert.match(
    migrationBlock(edgeConfig, "v3"),
    /new_sqlite_classes\s*=\s*\["ConnectionAuthDO"\]/,
  );
  assert.doesNotMatch(edgeConfig, /^new_classes\s*=/m);
});
