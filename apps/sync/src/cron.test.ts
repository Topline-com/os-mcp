import { test } from "node:test";
import assert from "node:assert/strict";
import { encryptPit } from "@topline/shared-auth";

import type { SyncEnv } from "./backfill.js";
import { runIncrementalForAllConnections } from "./cron.js";

// A made-up token, never a real one: "а" is the Cyrillic letter that looks like a Latin "a".
const CYRILLIC_PIT = "pit-00000000-0000-0000-0000-00000000000а";
const GOOD_PIT = "pit-00000000-0000-0000-0000-000000000000";
const SECRET = "test-secret";
const GOOD_CID = "6f1c2a9e-4b7d-4e2a-9c1f-0a1b2c3d4e5f";
const BAD_CID = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d";

async function storedConnection(locationId: string, pit: string): Promise<string> {
  const encrypted = await encryptPit(pit, SECRET);
  return JSON.stringify({
    location_id: locationId,
    pit_ct: encrypted.ct,
    pit_iv: encrypted.iv,
    brand_name: "Test",
    created_at: "2026-08-11T20:00:00.000Z",
    last_verified_at: "2026-08-11T20:00:00.000Z",
    source: "self-serve",
  });
}

async function envWithConnections(): Promise<SyncEnv> {
  const records = new Map<string, string>([
    [GOOD_CID, await storedConnection("locGood123", GOOD_PIT)],
    // Saved before the save paths checked characters.
    [BAD_CID, await storedConnection("locBad456", CYRILLIC_PIT)],
  ]);
  return {
    TOKEN_SIGNING_SECRET: SECRET,
    CONNECTIONS: {
      get: async (key: string) => records.get(key) ?? null,
      list: async () => ({
        keys: [...records.keys()].map((name) => ({ name })),
        list_complete: true,
        cacheStatus: null,
      }),
    } as unknown as KVNamespace,
    CONNECTION_AUTH_DO: {
      idFromName: (name: string) => name as unknown as DurableObjectId,
      get: () => ({
        getOrBootstrap: async (input: { location_id: string }) => ({
          status: "active",
          location_id: input.location_id,
        }),
      }),
    } as unknown as DurableObjectNamespace,
    LOCATION_DO: {} as SyncEnv["LOCATION_DO"],
  };
}

test("the cron skips a connection whose PIT has a non-ASCII character and logs one line without it", async () => {
  const env = await envWithConnections();
  const lines: string[] = [];
  const original = { log: console.log, warn: console.warn, error: console.error };
  for (const level of ["log", "warn", "error"] as const) {
    console[level] = (...args: unknown[]) => {
      lines.push(args.map(String).join(" "));
    };
  }
  const originalFetch = globalThis.fetch;
  let fetched = false;
  globalThis.fetch = async () => {
    fetched = true;
    return new Response("{}", { status: 200 });
  };
  const synced: string[] = [];
  try {
    await runIncrementalForAllConnections(env, {
      async incrementalAll(_env, connectionId) {
        synced.push(connectionId);
        return {};
      },
    });
  } finally {
    Object.assign(console, original);
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(synced, [GOOD_CID]);
  assert.equal(fetched, false);
  const skips = lines
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .filter((line) => line.event === "sync_connection_skipped_invalid_pit");
  assert.deepEqual(skips, [{
    event: "sync_connection_skipped_invalid_pit",
    connection_id: BAD_CID,
    location_id: "locBad456",
  }]);
  const completed = lines
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .find((line) => line.event === "sync_cron_completed");
  assert.deepEqual(completed, {
    event: "sync_cron_completed",
    connection_count: 2,
    skipped_connection_count: 1,
  });
  for (const line of lines) {
    assert.doesNotMatch(line, /pit-|а/);
  }
});
