import { describe, it } from "node:test";
import { rejects, strictEqual } from "node:assert";

import {
  loadAuthorizedSyncConnection,
  type SyncConnectionAuthorizationEnv,
} from "./connection-auth.js";

function envWithAuthorization(
  getOrBootstrap: (input: { location_id: string; credential_exists: boolean }) => Promise<unknown>,
): SyncConnectionAuthorizationEnv {
  const stored = JSON.stringify({
    location_id: "loc-a",
    pit_ct: "not-decryptable",
    pit_iv: "not-decryptable",
    brand_name: "Test",
    created_at: "2026-08-11T20:00:00.000Z",
    last_verified_at: "2026-08-11T20:00:00.000Z",
    source: "test",
  });
  return {
    TOKEN_SIGNING_SECRET: "test-secret",
    CONNECTIONS: {
      get: async () => stored,
    } as unknown as KVNamespace,
    CONNECTION_AUTH_DO: {
      idFromName: () => ({}) as DurableObjectId,
      get: () => ({ getOrBootstrap }),
    } as unknown as DurableObjectNamespace,
  };
}

describe("sync connection authorization", () => {
  it("denies a revoked connection before credential decryption", async () => {
    let checks = 0;
    const env = envWithAuthorization(async () => {
      checks += 1;
      throw new Error("revoked");
    });

    await rejects(
      () => loadAuthorizedSyncConnection(env, "cid-a"),
      /Connection is unavailable\./,
    );
    strictEqual(checks, 1);
  });

  it("denies a cross-tenant authorization snapshot", async () => {
    const env = envWithAuthorization(async () => ({
      status: "active",
      location_id: "loc-b",
    }));

    await rejects(
      () => loadAuthorizedSyncConnection(env, "cid-a"),
      /Connection is unavailable\./,
    );
  });
});
