import { buildToolAccess } from "./tool-access.js";
import type { ConnectionAuthorizationSnapshot } from "./tool-policy.js";
import type { ToolDef } from "./tools/types.js";

export const QUERY_API_TOOL_IDS = {
  overview: "topline_describe_schema",
  catalog: "topline_describe_data_catalog",
  explain_tables: "topline_explain_tables",
  execute_sql: "topline_execute_query",
} as const;

export type QueryApiOperation = keyof typeof QUERY_API_TOOL_IDS;

export function requireQueryApiOperation(
  snapshot: ConnectionAuthorizationSnapshot | null,
  operation: QueryApiOperation,
  registry: readonly ToolDef[],
): ToolDef {
  return buildToolAccess(snapshot, registry).requireCallable(QUERY_API_TOOL_IDS[operation]);
}
