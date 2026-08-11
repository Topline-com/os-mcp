import {
  assertToolAllowed,
  resolveToolPolicy,
  type ConnectionAuthorizationSnapshot,
  type ResolvedToolPolicy,
} from "./tool-policy.js";
import type { ToolDef } from "./tools/types.js";

export interface ToolAccess {
  advertised: readonly ToolDef[];
  policy: ResolvedToolPolicy;
  requireCallable(toolId: string): ToolDef;
}

export function buildToolAccess(
  snapshot: ConnectionAuthorizationSnapshot | null,
  registry: readonly ToolDef[],
): ToolAccess {
  const policy = resolveToolPolicy(snapshot, registry);
  const catalog = new Map(registry.map((tool) => [tool.name, tool]));

  return {
    advertised: policy.tools,
    policy,
    requireCallable(toolId: string): ToolDef {
      assertToolAllowed(policy, toolId);
      return catalog.get(toolId)!;
    },
  };
}
