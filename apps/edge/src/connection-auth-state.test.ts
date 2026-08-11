import { describe, it } from "node:test";
import { deepStrictEqual, strictEqual, throws } from "node:assert";

import {
  ConnectionAuthorizationService,
  ConnectionAuthorizationStateError,
  type ConnectionAuthorizationRepository,
} from "./connection-auth-state.js";
import type {
  ConnectionAuthorizationSnapshot,
  ConnectionClientTarget,
  PersistedToolPolicy,
} from "./tool-policy.js";

class MemoryRepository implements ConnectionAuthorizationRepository {
  snapshot: ConnectionAuthorizationSnapshot | null = null;

  read(): ConnectionAuthorizationSnapshot | null {
    return this.snapshot ? structuredClone(this.snapshot) : null;
  }

  write(snapshot: ConnectionAuthorizationSnapshot): void {
    this.snapshot = structuredClone(snapshot);
  }
}

const NOW = "2026-08-11T20:00:00.000Z";
const CANONICAL_TOOL_IDS = ["topline_ping", "topline_hidden"];

describe("connection authorization state", () => {
  it("rejects bearer policy broadening without changing the snapshot", () => {
    const repository = new MemoryRepository();
    const service = new ConnectionAuthorizationService(repository, CANONICAL_TOOL_IDS);
    const initial = service.initialize(
      "loc-a",
      { version: 1, mode: "allow", tool_ids: ["topline_ping"] },
      NOW,
      "copilot_studio",
    );
    const before = structuredClone(repository.snapshot);

    const broadeningUpdates: Array<{
      policy: PersistedToolPolicy;
      target?: ConnectionClientTarget;
    }> = [
      { policy: { version: 1, mode: "all" } },
      {
        policy: {
          version: 1,
          mode: "allow",
          tool_ids: ["topline_ping", "topline_hidden"],
        },
      },
      {
        policy: { version: 1, mode: "allow", tool_ids: ["topline_ping"] },
        target: "generic",
      },
    ];
    for (const update of broadeningUpdates) {
      throws(
        () =>
          service.updatePolicy(
            "loc-a",
            initial.policy_version,
            update.policy,
            undefined,
            update.target,
          ),
        (error: unknown) =>
          error instanceof ConnectionAuthorizationStateError &&
          error.reason === "reauthorization_required",
      );
      deepStrictEqual(repository.snapshot, before);
    }
  });

  it("bootstraps an existing credential to active/all exactly once", () => {
    const repository = new MemoryRepository();
    const service = new ConnectionAuthorizationService(repository, CANONICAL_TOOL_IDS);

    throws(
      () => service.getOrBootstrap("loc-a", false, NOW),
      (error: unknown) =>
        error instanceof ConnectionAuthorizationStateError && error.reason === "missing_connection",
    );

    const first = service.getOrBootstrap("loc-a", true, NOW);
    deepStrictEqual(first.policy, { version: 1, mode: "all" });
    strictEqual(first.client_target, "generic");
    strictEqual(first.status, "active");
    strictEqual(first.policy_version, 1);

    const second = service.getOrBootstrap("loc-a", true, "2026-08-12T00:00:00.000Z");
    deepStrictEqual(second, first);
  });

  it("serializes policy updates, tenant checks, touches, and revocation", () => {
    const repository = new MemoryRepository();
    const service = new ConnectionAuthorizationService(repository, CANONICAL_TOOL_IDS);
    const initial = service.initialize(
      "loc-a",
      { version: 1, mode: "all" },
      NOW,
      "copilot_studio",
    );
    strictEqual(initial.client_target, "copilot_studio");

    throws(
      () => service.updatePolicy("loc-b", 1, { version: 1, mode: "allow", tool_ids: [] }),
      (error: unknown) =>
        error instanceof ConnectionAuthorizationStateError && error.reason === "tenant_mismatch",
    );

    const narrowed = service.updatePolicy(
      "loc-a",
      initial.policy_version,
      { version: 1, mode: "allow", tool_ids: ["topline_ping", "topline_hidden"] },
      "2026-08-11T21:00:00.000Z",
    );
    strictEqual(narrowed.policy_version, 2);
    strictEqual(narrowed.client_target, "copilot_studio");

    throws(
      () => service.updatePolicy("loc-a", 1, { version: 1, mode: "all" }),
      (error: unknown) =>
        error instanceof ConnectionAuthorizationStateError && error.reason === "version_conflict",
    );

    const reordered = service.updatePolicy(
      "loc-a",
      narrowed.policy_version,
      { version: 1, mode: "allow", tool_ids: ["topline_hidden", "topline_ping"] },
      "2026-08-11T21:15:00.000Z",
    );
    const strictSubset = service.updatePolicy(
      "loc-a",
      reordered.policy_version,
      { version: 1, mode: "allow", tool_ids: ["topline_ping"] },
      "2026-08-11T21:30:00.000Z",
    );
    strictEqual(strictSubset.policy_version, 4);

    const touched = service.touch("loc-a", "2026-08-11T22:00:00.000Z");
    deepStrictEqual(touched.policy, strictSubset.policy);
    strictEqual(touched.policy_version, strictSubset.policy_version);

    const revoked = service.revoke("loc-a", touched.policy_version, "2026-08-11T23:00:00.000Z");
    strictEqual(revoked.status, "revoked");
    throws(
      () => service.getOrBootstrap("loc-a", true),
      (error: unknown) =>
        error instanceof ConnectionAuthorizationStateError && error.reason === "revoked_connection",
    );
    const retryable = service.getForManagement("loc-a", true);
    strictEqual(retryable.status, "revoked");
    deepStrictEqual(service.revoke("loc-a", retryable.policy_version), retryable);
  });

  it("fails closed on corrupt persisted state", () => {
    const repository = new MemoryRepository();
    repository.snapshot = {
      schema_version: 1,
      status: "active",
      location_id: "loc-a",
      client_target: "generic",
      policy: { version: 1, mode: "allow", tool_ids: [""] },
      policy_version: 1,
      created_at: NOW,
      updated_at: NOW,
    };
    const service = new ConnectionAuthorizationService(repository, CANONICAL_TOOL_IDS);

    throws(
      () => service.getForManagement("loc-a", true),
      (error: unknown) =>
        error instanceof ConnectionAuthorizationStateError && error.reason === "corrupt_state",
    );
  });
});
