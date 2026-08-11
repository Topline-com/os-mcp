import type { ToolDef } from "./tools/types.js";

export interface AllToolsPolicy {
  version: 1;
  mode: "all";
}

export interface AllowToolsPolicy {
  version: 1;
  mode: "allow";
  tool_ids: string[];
}

export type PersistedToolPolicy = AllToolsPolicy | AllowToolsPolicy;
export type ConnectionClientTarget = "generic" | "copilot_studio";

export interface ConnectionAuthorizationSnapshot {
  schema_version: 1;
  status: "active" | "revoked";
  location_id: string;
  client_target?: ConnectionClientTarget;
  policy: PersistedToolPolicy;
  policy_version: number;
  created_at: string;
  updated_at: string;
  last_verified_at?: string;
}

export interface ResolvedToolPolicy {
  tools: ToolDef[];
  allowed_tool_ids: ReadonlySet<string>;
  stale_tool_ids: string[];
  policy_version: number;
  legacy_default: boolean;
}

export class ToolPolicyError extends Error {
  readonly reason:
    | "corrupt_policy"
    | "revoked_connection"
    | "client_tool_limit"
    | "tool_unavailable";

  constructor(reason: ToolPolicyError["reason"]) {
    super("Tool is not available for this connection.");
    this.name = "ToolPolicyError";
    this.reason = reason;
  }
}

export function resolveToolPolicy(
  snapshot: ConnectionAuthorizationSnapshot | null,
  registry: readonly ToolDef[],
): ResolvedToolPolicy {
  if (snapshot === null) {
    return resolved(registry, registry.map((tool) => tool.name), [], 0, true);
  }

  assertSnapshot(snapshot);
  if (snapshot.status === "revoked") {
    throw new ToolPolicyError("revoked_connection");
  }

  if (snapshot.policy.mode === "all") {
    enforceClientLimit(snapshot, registry.length);
    return resolved(registry, registry.map((tool) => tool.name), [], snapshot.policy_version, false);
  }

  const catalogIds = new Set(registry.map((tool) => tool.name));
  const stale = snapshot.policy.tool_ids.filter((id) => !catalogIds.has(id));
  const allowed = snapshot.policy.tool_ids.filter((id) => catalogIds.has(id));
  enforceClientLimit(snapshot, allowed.length);
  return resolved(registry, allowed, stale, snapshot.policy_version, false);
}

export function assertToolAllowed(policy: ResolvedToolPolicy, toolId: string): void {
  if (!policy.allowed_tool_ids.has(toolId)) {
    throw new ToolPolicyError("tool_unavailable");
  }
}

function resolved(
  registry: readonly ToolDef[],
  allowed: Iterable<string>,
  stale_tool_ids: string[],
  policy_version: number,
  legacy_default: boolean,
): ResolvedToolPolicy {
  const allowed_tool_ids = new Set(allowed);
  return {
    tools: registry.filter((tool) => allowed_tool_ids.has(tool.name)),
    allowed_tool_ids,
    stale_tool_ids,
    policy_version,
    legacy_default,
  };
}

function assertSnapshot(snapshot: ConnectionAuthorizationSnapshot): void {
  if (
    snapshot.schema_version !== 1 ||
    (snapshot.status !== "active" && snapshot.status !== "revoked") ||
    typeof snapshot.location_id !== "string" ||
    snapshot.location_id.length === 0 ||
    (snapshot.client_target !== undefined &&
      snapshot.client_target !== "generic" &&
      snapshot.client_target !== "copilot_studio") ||
    !Number.isSafeInteger(snapshot.policy_version) ||
    snapshot.policy_version < 1 ||
    !snapshot.policy ||
    snapshot.policy.version !== 1 ||
    (snapshot.policy.mode !== "all" && snapshot.policy.mode !== "allow") ||
    (snapshot.policy.mode === "allow" &&
      (!Array.isArray(snapshot.policy.tool_ids) ||
        snapshot.policy.tool_ids.some((id) => typeof id !== "string" || id.length === 0)))
  ) {
    throw new ToolPolicyError("corrupt_policy");
  }
}

function enforceClientLimit(
  snapshot: ConnectionAuthorizationSnapshot,
  toolCount: number,
): void {
  if (snapshot.client_target === "copilot_studio" && toolCount > 128) {
    throw new ToolPolicyError("client_tool_limit");
  }
}
