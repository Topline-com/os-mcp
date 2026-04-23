// Source-audit framework.
//
// Scope today: TYPES only + a runner STUB. The actual live-audit
// implementation (hit GHL endpoints, verify IDs, confirm cursor semantics
// end-to-end, etc.) ships when we have the sync worker to run it in.
//
// Why land types now? Because `EntityManifest.audit: AuditReport` is the
// exposure gate. Without a declared shape, LocationDO + describe_schema
// have nothing to check.

import type { AuditReport, EntityManifest } from "./types.js";
import { auditPasses, requiredAuditChecks } from "./types.js";
import { ALL_ENTITIES, ENTITY_BY_TABLE } from "./entities.js";

export type AuditStatus = "not_run" | "passed" | "failed";

export interface AuditSummary {
  table: string;
  phase: 1 | 2 | 3;
  exposed: boolean;
  status: AuditStatus;
  report: AuditReport;
  /** Required checks (varies by entity) that are not yet true. */
  failing_checks: string[];
}

/** Classify the current audit state for an entity without running anything. */
export function summarizeAudit(entity: EntityManifest): AuditSummary {
  const r = entity.audit;
  const required = requiredAuditChecks(entity);
  const failing = required.filter((k) => r[k] !== true).map(String);
  const anyClaimed =
    r.live_tested ||
    r.stable_pk ||
    r.backfill_path ||
    r.incremental_path ||
    r.update_cursor ||
    r.webhook_coverage !== undefined;
  const status: AuditStatus = !anyClaimed
    ? "not_run"
    : auditPasses(entity)
    ? "passed"
    : "failed";
  return {
    table: entity.table,
    phase: entity.phase,
    exposed: entity.exposed,
    status,
    report: r,
    failing_checks: status === "passed" ? [] : failing,
  };
}

/** One-call snapshot for every entity. Feeds ops dashboards + runbooks. */
export function summarizeAllAudits(): AuditSummary[] {
  return ALL_ENTITIES.map(summarizeAudit);
}

/** Lookup a single entity's audit state by table name. Returns null if unknown. */
export function summarizeAuditByTable(table: string): AuditSummary | null {
  const e = ENTITY_BY_TABLE.get(table);
  return e ? summarizeAudit(e) : null;
}

// --- Runner contract (implementation lives in apps/sync later) ---

/**
 * The live audit runner signature. Implementations call GHL with a real PIT
 * + Location ID and verify each of the six checks end-to-end against a
 * non-empty sub-account. Returns a fresh AuditReport that the maintainer
 * pastes back into entities.ts.
 */
export type RunAuditFn = (args: {
  entity: EntityManifest;
  pit: string;
  location_id: string;
}) => Promise<AuditReport>;

/** Default stub — throws so the missing wiring is loud. */
export const runAudit: RunAuditFn = async () => {
  throw new Error(
    "runAudit is not implemented in @topline/shared-schema. The live audit " +
      "runner will ship with apps/sync. Until then, audit entries in " +
      "entities.ts are maintained by hand.",
  );
};
