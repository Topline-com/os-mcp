import { test } from "node:test";
import assert from "node:assert/strict";
import type { AuthRequest } from "@cloudflare/workers-oauth-provider";
import { OAuthFlowDO, OAuthFlowState } from "./flow-do.js";

const authRequest: AuthRequest = {
  responseType: "code",
  clientId: "client-1",
  redirectUri: "https://client.example/callback",
  scope: ["mcp"],
  state: "state-1",
  codeChallenge: "A".repeat(43),
  codeChallengeMethod: "S256",
  resource: "https://os-mcp.topline.com/mcp",
  issuer: "https://os-mcp.topline.com",
};

test("consent continuation is idempotent for an identical submission", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = 1_000;

  assert.equal(
    await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000),
    true,
  );
  assert.deepEqual(await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now), {
    status: "reserved",
    request: authRequest,
  });
  assert.deepEqual(await flow.reserveConsent("csrf-hash", "submission-a", "lease-b", "connection-1", now), {
    status: "processing",
  });
  assert.deepEqual(
    await flow.reserveConsent("csrf-hash", "submission-a", "lease-b", "connection-1", now + 30_001),
    { status: "processing" },
  );
  assert.deepEqual(
    await flow.reserveConsent("csrf-hash", "submission-b", "lease-c", "connection-1", now + 30_001),
    { status: "invalid" },
  );
  assert.equal(
    await flow.completeConsent("submission-a", "lease-a", "https://client.example/callback?code=1"),
    true,
  );
  assert.deepEqual(await flow.reserveConsent("csrf-hash", "submission-a", "lease-c", "connection-1", now), {
    status: "completed",
    redirectTo: "https://client.example/callback?code=1",
  });
  assert.deepEqual(await flow.reserveConsent("wrong-hash", "submission-a", "lease-c", "connection-1", now), {
    status: "invalid",
  });
});

test("failed consent processing releases the same submission for retry", async () => {
  const flow = new OAuthFlowState(new MemoryStorage());
  const now = 1_000;
  assert.equal(
    await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000),
    true,
  );
  assert.equal(
    (await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now)).status,
    "reserved",
  );
  assert.equal(await flow.releaseConsent("submission-b", "lease-a"), false);
  assert.equal(await flow.releaseConsent("submission-a", "lease-b"), false);
  assert.equal(await flow.releaseConsent("submission-a", "lease-a"), true);
  assert.equal(
    (await flow.reserveConsent("csrf-hash", "submission-b", "lease-b", "connection-1", now)).status,
    "invalid",
  );
  assert.equal(
    (await flow.reserveConsent("csrf-hash", "submission-a", "lease-b", "connection-1", now)).status,
    "reserved",
  );
});

test("backfill kickoff is leased, retryable after failure, and completed once", async () => {
  const flow = new OAuthFlowState(new MemoryStorage());
  const now = 1_000;
  await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000);
  await flow.reserveConsent("csrf-hash", "submission-a", "consent-lease", "connection-1", now);
  await flow.completeConsent(
    "submission-a",
    "consent-lease",
    "https://client.example/callback?code=1",
  );

  assert.deepEqual(await flow.reserveBackfill("backfill-a", now), {
    status: "reserved",
    connectionId: "connection-1",
  });
  assert.deepEqual(await flow.reserveBackfill("backfill-b", now), { status: "processing" });
  assert.equal(await flow.releaseBackfill("backfill-b"), false);
  assert.equal(await flow.releaseBackfill("backfill-a"), true);
  assert.deepEqual(await flow.reserveBackfill("backfill-b", now), {
    status: "reserved",
    connectionId: "connection-1",
  });
  assert.equal(await flow.completeBackfill("backfill-b"), true);
  assert.deepEqual(await flow.reserveBackfill("backfill-c", now + 60_000), {
    status: "completed",
  });
});

test("successful consent schedules durable backfill kickoff", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = 1_000;
  await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000);
  await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now);
  const before = storage.alarms.length;
  const completedAt = Date.now();
  assert.equal(
    await flow.completeConsent(
      "submission-a",
      "lease-a",
      "https://client.example/callback?code=1",
    ),
    true,
  );
  assert.equal(storage.alarms.length, before + 1);
  assert.ok(storage.alarms.at(-1)! >= completedAt + 1_000);
});

test("terminal consent abort cannot be reopened by retry or stale owner", async () => {
  const flow = new OAuthFlowState(new MemoryStorage());
  const now = 1_000;
  await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000);
  await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now);
  assert.equal(await flow.abortConsent("submission-a", "lease-a"), true);
  assert.equal(await flow.releaseConsent("submission-a", "lease-a"), false);
  assert.equal(
    await flow.completeConsent(
      "submission-a",
      "lease-a",
      "https://client.example/callback?code=late",
    ),
    false,
  );
  assert.deepEqual(
    await flow.reserveConsent("csrf-hash", "submission-a", "lease-b", "connection-1", now),
    { status: "invalid" },
  );
});

test("failed durable backfill kickoff releases its lease and schedules another alarm", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = Date.now();
  await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000);
  await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now);
  await flow.completeConsent(
    "submission-a",
    "lease-a",
    "https://client.example/callback?code=1",
  );
  const before = storage.alarms.length;
  const durableObject = new OAuthFlowDO(
    { storage } as unknown as DurableObjectState,
    {
      CONNECTIONS: { async delete() {} } as unknown as KVNamespace,
      ADMIN_TOKEN: "test-admin",
      SYNC_WORKER: {
        async fetch() { return new Response("failed", { status: 503 }); },
      } as unknown as Fetcher,
    },
  );

  await durableObject.alarm();

  const record = storage.read() as { backfillStatus?: string };
  assert.equal(record.backfillStatus, "pending");
  assert.ok(storage.alarms.length > before);
});

test("failed credential cleanup stays expiring and schedules another alarm without throwing", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = Date.now();
  await flow.createConsent(authRequest, "connection-1", "csrf-hash", now + 60_000);
  await flow.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-1", now);
  await flow.abortConsent("submission-a", "lease-a");
  const before = storage.alarms.length;
  const durableObject = new OAuthFlowDO(
    { storage } as unknown as DurableObjectState,
    {
      CONNECTIONS: {
        async delete() { throw new Error("temporary KV failure"); },
      } as unknown as KVNamespace,
    },
  );

  await durableObject.alarm();

  const record = storage.read() as { status?: string };
  assert.equal(record.status, "expiring");
  assert.ok(storage.alarms.length > before);
});

test("expiry identifies only an unfinished deterministic connection for cleanup", async () => {
  const unfinished = new OAuthFlowState(new MemoryStorage());
  await unfinished.createConsent(authRequest, "connection-unfinished", "csrf-hash", 2_000);
  await unfinished.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-unfinished", 1_000);
  assert.equal(await unfinished.expire(), "connection-unfinished");
  assert.equal(
    await unfinished.completeConsent(
      "submission-a",
      "lease-a",
      "https://client.example/callback?code=late",
    ),
    false,
  );
  assert.equal(await unfinished.expire(), "connection-unfinished");
  assert.equal(await unfinished.completeExpiry("wrong-connection"), false);
  assert.equal(await unfinished.completeExpiry("connection-unfinished"), true);
  assert.equal(await unfinished.expire(), null);

  const completed = new OAuthFlowState(new MemoryStorage());
  await completed.createConsent(authRequest, "connection-completed", "csrf-hash", 2_000);
  await completed.reserveConsent("csrf-hash", "submission-a", "lease-a", "connection-completed", 1_000);
  await completed.completeConsent(
    "submission-a",
    "lease-a",
    "https://client.example/callback?code=1",
  );
  assert.equal(await completed.expire(), null);
});

test("legacy pending consent binds its continuation while legacy spent stays invalid", async () => {
  const pendingStorage = new MemoryStorage();
  pendingStorage.write({
    kind: "consent",
    status: "pending",
    request: authRequest,
    csrfHash: "csrf-hash",
    expiresAt: 60_000,
  });
  const pending = new OAuthFlowState(pendingStorage);
  assert.equal(
    (await pending.reserveConsent(
      "csrf-hash",
      "submission-a",
      "lease-a",
      "legacy-continuation",
      1_000,
    )).status,
    "reserved",
  );
  assert.equal(await pending.expire(), "legacy-continuation");

  const spentStorage = new MemoryStorage();
  spentStorage.write({
    kind: "consent",
    status: "spent",
    request: authRequest,
    csrfHash: "csrf-hash",
    expiresAt: 60_000,
  });
  const spent = new OAuthFlowState(spentStorage);
  assert.equal(
    (await spent.reserveConsent(
      "csrf-hash",
      "submission-a",
      "lease-a",
      "legacy-continuation",
      1_000,
    )).status,
    "invalid",
  );
});

test("authorization-code redemption has a single atomic winner", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = 1_000;

  const results = await Promise.all([
    flow.reserveCode("lease-a", now + 600_000, now),
    flow.reserveCode("lease-b", now + 600_000, now),
  ]);
  assert.equal(results.filter((result) => result === "reserved").length, 1);
  assert.equal(results.filter((result) => result === "pending").length, 1);

  const winningLease = results[0] === "reserved" ? "lease-a" : "lease-b";
  const losingLease = winningLease === "lease-a" ? "lease-b" : "lease-a";
  assert.equal(await flow.releaseCode(losingLease), false);
  assert.equal(await flow.commitCode(winningLease), true);
  assert.equal(await flow.reserveCode("lease-c", now + 600_000, now), "spent");
});

test("only the matching lease can release a deterministic failed redemption", async () => {
  const flow = new OAuthFlowState(new MemoryStorage());
  const now = 1_000;

  assert.equal(await flow.reserveCode("lease-a", now + 600_000, now), "reserved");
  assert.equal(await flow.releaseCode("lease-b"), false);
  assert.equal(await flow.releaseCode("lease-a"), true);
  assert.equal(await flow.reserveCode("lease-c", now + 600_000, now), "reserved");
});

class MemoryStorage {
  private value: unknown;
  private queue = Promise.resolve();
  readonly alarms: number[] = [];

  async transaction<T>(callback: (txn: MemoryTransaction) => Promise<T>): Promise<T> {
    const previous = this.queue;
    let release = () => {};
    this.queue = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      return await callback(new MemoryTransaction(this));
    } finally {
      release();
    }
  }

  read(): unknown {
    return this.value;
  }

  write(value: unknown): void {
    this.value = value;
  }

  clear(): void {
    this.value = undefined;
  }

  async setAlarm(scheduledTime: number): Promise<void> {
    this.alarms.push(scheduledTime);
  }
}

class MemoryTransaction {
  constructor(private readonly storage: MemoryStorage) {}

  async get<T>(): Promise<T | undefined> {
    return this.storage.read() as T | undefined;
  }

  async put(_key: string, value: unknown): Promise<void> {
    this.storage.write(value);
  }

  async delete(): Promise<boolean> {
    const existed = this.storage.read() !== undefined;
    this.storage.clear();
    return existed;
  }
}
