import { describe, it } from "node:test";
import { deepStrictEqual, strictEqual, throws } from "node:assert";

import { ALL_TOOLS } from "./registry.js";
import { buildToolAccess } from "./tool-access.js";
import { ToolPolicyError, type ConnectionAuthorizationSnapshot } from "./tool-policy.js";

const SNAPSHOT: ConnectionAuthorizationSnapshot = {
  schema_version: 1,
  status: "active",
  location_id: "loc-a",
  policy: { version: 1, mode: "allow", tool_ids: ["topline_ping"] },
  policy_version: 4,
  created_at: "2026-08-11T20:00:00.000Z",
  updated_at: "2026-08-11T21:00:00.000Z",
};

describe("tool list/call authorization parity", () => {
  it("advertises only allowed tools and rejects hidden direct calls before lookup", () => {
    const access = buildToolAccess(SNAPSHOT, ALL_TOOLS);

    deepStrictEqual(access.advertised.map((tool) => tool.name), ["topline_ping"]);
    strictEqual(access.requireCallable("topline_ping").name, "topline_ping");

    for (const name of ["topline_execute_query", "topline_not_a_real_tool"]) {
      throws(
        () => access.requireCallable(name),
        (error: unknown) =>
          error instanceof ToolPolicyError &&
          error.reason === "tool_unavailable" &&
          error.message === "Tool is not available for this connection.",
      );
    }
  });

  it("keeps the exact advertised/callable set in canonical order", () => {
    const access = buildToolAccess(null, ALL_TOOLS);
    const advertised = access.advertised.map((tool) => tool.name);
    const callable = ALL_TOOLS.filter((tool) => {
      try {
        access.requireCallable(tool.name);
        return true;
      } catch {
        return false;
      }
    }).map((tool) => tool.name);

    deepStrictEqual(callable, advertised);
    deepStrictEqual(advertised, ALL_TOOLS.map((tool) => tool.name));
  });

  it("never treats a removed persisted tool ID as callable", () => {
    const access = buildToolAccess(
      {
        ...SNAPSHOT,
        policy: { version: 1, mode: "allow", tool_ids: ["removed_tool_id"] },
      },
      ALL_TOOLS,
    );

    deepStrictEqual(access.advertised, []);
    throws(
      () => access.requireCallable("removed_tool_id"),
      (error: unknown) => error instanceof ToolPolicyError && error.reason === "tool_unavailable",
    );
  });
});
