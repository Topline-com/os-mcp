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
import { ALL_ENTITIES, ENTITY_BY_TABLE } from "./entities.js";
import { auditPasses } from "./types.js";

export type AuditStatus = "not_run" | "passed" | "failed";

export interface AuditSummary {
  table: string;
  phase: 1 | 2 | 3;
  exposed: boolean;
  status: AuditStatus;
  report: AuditReport;
  failing_checks: string[];
}

/** Classify the current audit state for an entity without running anything. */
export function summarizeAudit(entity: EntityManifest): AuditSummary {
  const r = entity.audit;
  const noneRun =
    !r.live_tested &&
    !r.stable_pk &&
    !r.backfill_path &&
    !r.incremental_path &&
    !r.update_cursor &&
    r.webhook_coverage === undefined;
  const status: AuditStatus = noneRun
    ? "not_run"
    : auditPasses(r)
    ? "passed"
    : "failed";
  const failing: string[] = [];
  if (!r.stable_pk) failing.push("stable_pk");
  if (!r.backfill_path) failing.push("backfill_path");
  if (!r.incremental_path) failing.push("incremental_path");
  if (!r.update_cursor) failing.push("update_cursor");
  if (r.webhook_coverage === false) failing.push("webhook_coverage");
  if (!r.live_tested) failing.push("live_tested");
  return {
    table: entity.table,
    phase: entity.phase,
    exposed: entity.exposed,
    status: noneRun ? "not_run" : status,
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
