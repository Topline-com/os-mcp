import { test } from "node:test";
import assert from "node:assert/strict";
import type { AuthRequest } from "@cloudflare/workers-oauth-provider";
import { OAuthFlowState } from "./flow-do.js";

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

test("consent continuation is atomically consumed once", async () => {
  const storage = new MemoryStorage();
  const flow = new OAuthFlowState(storage);
  const now = 1_000;

  assert.equal(await flow.createConsent(authRequest, "csrf-hash", now + 60_000), true);
  const results = await Promise.all([
    flow.consumeConsent("csrf-hash", now),
    flow.consumeConsent("csrf-hash", now),
  ]);

  assert.equal(results.filter(Boolean).length, 1);
  assert.deepEqual(results.find(Boolean), authRequest);
  assert.equal(await flow.consumeConsent("wrong-hash", now), null);
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
