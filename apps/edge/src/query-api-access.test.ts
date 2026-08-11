import { describe, it } from "node:test";
import { strictEqual, throws } from "node:assert";

import { ALL_TOOLS } from "./registry.js";
import {
  QUERY_API_TOOL_IDS,
  requireQueryApiOperation,
  type QueryApiOperation,
} from "./query-api-access.js";
import { ToolPolicyError, type ConnectionAuthorizationSnapshot } from "./tool-policy.js";
import { compileToolSelection } from "./tool-presets.js";

function snapshot(toolIds: string[]): ConnectionAuthorizationSnapshot {
  return {
    schema_version: 1,
    status: "active",
    location_id: "loc-a",
    client_target: "generic",
    policy: { version: 1, mode: "allow", tool_ids: toolIds },
    policy_version: 1,
    created_at: "2026-08-11T20:00:00.000Z",
    updated_at: "2026-08-11T20:00:00.000Z",
  };
}

describe("query API policy boundary", () => {
  it("maps every HTTP operation to its corresponding MCP analytics tool", () => {
    strictEqual(QUERY_API_TOOL_IDS.overview, "topline_describe_schema");
    strictEqual(QUERY_API_TOOL_IDS.catalog, "topline_describe_data_catalog");
    strictEqual(QUERY_API_TOOL_IDS.explain_tables, "topline_explain_tables");
    strictEqual(QUERY_API_TOOL_IDS.execute_sql, "topline_execute_query");
  });

  it("denies HTTP query operations omitted from the connection policy", () => {
    const policy = compileToolSelection(
      { kind: "preset", preset_id: "read_only_crm" },
      ALL_TOOLS,
    );
    if (policy.mode !== "allow") throw new Error("Expected allow policy fixture.");

    for (const operation of Object.keys(QUERY_API_TOOL_IDS) as QueryApiOperation[]) {
      throws(
        () => requireQueryApiOperation(snapshot(policy.tool_ids), operation, ALL_TOOLS),
        (error: unknown) =>
          error instanceof ToolPolicyError && error.reason === "tool_unavailable",
      );
    }
  });

  it("allows only explicitly selected HTTP query operations", () => {
    strictEqual(
      requireQueryApiOperation(
        snapshot(["topline_execute_query"]),
        "execute_sql",
        ALL_TOOLS,
      ).name,
      "topline_execute_query",
    );
    throws(() =>
      requireQueryApiOperation(snapshot(["topline_execute_query"]), "overview", ALL_TOOLS),
    );
  });
});
