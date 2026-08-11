import { describe, it } from "node:test";
import { deepStrictEqual, ok, strictEqual, throws } from "node:assert";

import { ALL_TOOLS } from "./registry.js";
import {
  ToolPolicyError,
  resolveToolPolicy,
  type ConnectionAuthorizationSnapshot,
} from "./tool-policy.js";
import {
  PRESET_IDS,
  assessClientCompatibility,
  compileToolSelection,
} from "./tool-presets.js";

describe("connection-scoped tool policy", () => {
  it("treats a missing policy as the explicit legacy all-tools default", () => {
    const resolved = resolveToolPolicy(null, ALL_TOOLS);

    deepStrictEqual(
      resolved.tools.map((tool) => tool.name),
      ALL_TOOLS.map((tool) => tool.name),
    );
    deepStrictEqual(resolved.stale_tool_ids, []);
  });

  it("compiles every named preset to stable IDs in canonical registry order", () => {
    const canonicalIds = ALL_TOOLS.map((tool) => tool.name);

    for (const presetId of PRESET_IDS) {
      const policy = compileToolSelection({ kind: "preset", preset_id: presetId }, ALL_TOOLS);
      if (presetId === "all") {
        deepStrictEqual(policy, { version: 1, mode: "all" });
        continue;
      }

      strictEqual(policy.mode, "allow");
      ok(policy.tool_ids.length > 0);
      ok(policy.tool_ids.length <= 30);
      strictEqual(new Set(policy.tool_ids).size, policy.tool_ids.length);
      deepStrictEqual(
        policy.tool_ids,
        canonicalIds.filter((id) => policy.tool_ids.includes(id)),
      );
    }
  });

  it("warns above 30 and gates only Copilot Studio selections above 128", () => {
    deepStrictEqual(assessClientCompatibility(25, "copilot_studio"), {
      compatible: true,
      warnings: [],
    });
    deepStrictEqual(assessClientCompatibility(30, "copilot_studio"), {
      compatible: true,
      warnings: [],
    });

    for (const count of [127, 128]) {
      const assessment = assessClientCompatibility(count, "copilot_studio");
      strictEqual(assessment.compatible, true);
      strictEqual(assessment.warnings.length, 1);
    }

    const blocked = assessClientCompatibility(129, "copilot_studio");
    strictEqual(blocked.compatible, false);
    strictEqual(blocked.warnings.length, 1);
    strictEqual(assessClientCompatibility(129, "generic").compatible, true);
  });

  it("fails closed if a Copilot all-tools policy grows beyond 128", () => {
    const registry = Array.from({ length: 129 }, (_, index) => ({
      ...ALL_TOOLS[0],
      name: `topline_fixture_${index}`,
    }));
    const snapshot: ConnectionAuthorizationSnapshot = {
      schema_version: 1,
      status: "active",
      location_id: "loc-a",
      client_target: "copilot_studio",
      policy: { version: 1, mode: "all" },
      policy_version: 1,
      created_at: "2026-08-11T20:00:00.000Z",
      updated_at: "2026-08-11T20:00:00.000Z",
    };

    throws(
      () => resolveToolPolicy(snapshot, registry),
      (error: unknown) => error instanceof ToolPolicyError && error.reason === "client_tool_limit",
    );
    deepStrictEqual(
      resolveToolPolicy({ ...snapshot, client_target: "generic" }, registry).tools,
      registry,
    );
  });

  it("supports custom selections and reports removed IDs without granting them", () => {
    const policy = compileToolSelection(
      { kind: "custom", tool_ids: ["topline_execute_query", "topline_ping", "topline_ping"] },
      ALL_TOOLS,
    );
    strictEqual(policy.mode, "allow");
    deepStrictEqual(policy.tool_ids, ["topline_ping", "topline_execute_query"]);

    const snapshot: ConnectionAuthorizationSnapshot = {
      schema_version: 1,
      status: "active",
      location_id: "loc-a",
      policy: {
        version: 1,
        mode: "allow",
        tool_ids: ["topline_ping", "topline_removed_tool"],
      },
      policy_version: 2,
      created_at: "2026-08-11T20:00:00.000Z",
      updated_at: "2026-08-11T21:00:00.000Z",
    };
    const resolved = resolveToolPolicy(snapshot, ALL_TOOLS);
    deepStrictEqual(resolved.tools.map((tool) => tool.name), ["topline_ping"]);
    deepStrictEqual(resolved.stale_tool_ids, ["topline_removed_tool"]);
  });

  it("fails closed for revoked, corrupt, or unsupported policy state", () => {
    const base = {
      schema_version: 1,
      status: "active",
      location_id: "loc-a",
      policy: { version: 1, mode: "all" },
      policy_version: 1,
      created_at: "2026-08-11T20:00:00.000Z",
      updated_at: "2026-08-11T20:00:00.000Z",
    } as ConnectionAuthorizationSnapshot;

    for (const snapshot of [
      { ...base, status: "revoked" },
      { ...base, schema_version: 99 },
      { ...base, policy: { version: 99, mode: "all" } },
      { ...base, policy: { version: 1, mode: "unknown" } },
    ]) {
      throws(
        () => resolveToolPolicy(snapshot as ConnectionAuthorizationSnapshot, ALL_TOOLS),
        (error: unknown) => error instanceof ToolPolicyError,
      );
    }
  });

  it("keeps every stable tool ID portable and unique", () => {
    const ids = ALL_TOOLS.map((tool) => tool.name);
    strictEqual(new Set(ids).size, ids.length);
    ok(ids.every((id) => /^topline_[a-z0-9_]{1,56}$/.test(id)));
  });
});
