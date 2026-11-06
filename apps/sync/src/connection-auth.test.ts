import { describe, it } from "node:test";
import { ok, rejects, strictEqual } from "node:assert";
import { encryptPit, type EncryptedPayload } from "@topline/shared-auth";

import {
  loadAuthorizedSyncConnection,
  UnsendablePitError,
  type SyncConnectionAuthorizationEnv,
} from "./connection-auth.js";

// A made-up token, never a real one: "\u0430" is the Cyrillic letter that looks like a Latin "a".
const CYRILLIC_PIT = "pit-00000000-0000-0000-0000-00000000000\u0430";
const SECRET = "test-secret";

function envWithAuthorization(
  getOrBootstrap: (input: { location_id: string; credential_exists: boolean }) => Promise<unknown>,
  pit: EncryptedPayload = { ct: "not-decryptable", iv: "not-decryptable" },
): SyncConnectionAuthorizationEnv {
  const stored = JSON.stringify({
    location_id: "loc-a",
    pit_ct: pit.ct,
    pit_iv: pit.iv,
    brand_name: "Test",
    created_at: "2026-08-11T20:00:00.000Z",
    last_verified_at: "2026-08-11T20:00:00.000Z",
    source: "test",
  });
  return {
    TOKEN_SIGNING_SECRET: SECRET,
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

  it("refuses a connection whose PIT has a non-ASCII character, naming only its Location ID", async () => {
    const env = envWithAuthorization(
      async () => ({ status: "active", location_id: "loc-a" }),
      await encryptPit(CYRILLIC_PIT, SECRET),
    );

    await rejects(
      () => loadAuthorizedSyncConnection(env, "cid-a"),
      (error: unknown) => {
        ok(error instanceof UnsendablePitError);
        strictEqual(error.locationId, "loc-a");
        strictEqual(/pit-0|\u0430/.test(error.message), false);
        return true;
      },
    );
  });

  it("returns a connection whose PIT is printable ASCII", async () => {
    const env = envWithAuthorization(
      async () => ({ status: "active", location_id: "loc-a" }),
      await encryptPit("pit-00000000-0000-0000-0000-000000000000", SECRET),
    );

    const connection = await loadAuthorizedSyncConnection(env, "cid-a");
    strictEqual(connection.location_id, "loc-a");
  });
});
