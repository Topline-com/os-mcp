// Authoritative types shared by every downstream consumer:
//
//   - LocationDO uses EntityManifest to generate CREATE TABLE migrations
//   - Sync worker uses the backfill/incremental descriptors to know what
//     endpoint to paginate and what cursor field to advance
//   - apps/edge SQL tools (describe_schema, explain_tables) read these
//     directly and render them to the LLM
//
// A single source of truth. If you want to add a table, add an EntityManifest
// entry and the rest of the stack picks it up on the next deploy.

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

/** SQLite storage classes. We use this narrow set intentionally. */
export type SqliteType = "TEXT" | "INTEGER" | "REAL";

export interface ColumnDef {
  /** Column name as it appears in SQL. snake_case by convention. */
  name: string;

  /** SQLite storage class. Dates, JSON, enums all live in TEXT. */
  sqlite_type: SqliteType;

  /** Can this column be NULL? Primary keys must be non-nullable. */
  nullable: boolean;

  /**
   * Human description. Surfaces in `explain_tables` so the LLM writes
   * queries that match the real semantics, not just the column name.
   */
  description: string;

  /**
   * For TEXT columns holding a closed set of values, list them explicitly.
   * The LLM uses this to write valid WHERE clauses.
   */
  enum?: readonly string[];

  /**
   * Foreign-key hint as "table.column". Not enforced by SQLite (we don't
   * turn on FK constraints — the sync worker's eventual consistency makes
   * referential integrity best-effort), but surfaces in explain_tables
   * so the LLM writes correct joins.
   */
  references?: string;

  /**
   * If true, TEXT column contains serialized JSON. Encourages the LLM to
   * use SQLite's json_extract / json_each when querying.
   */
  json?: boolean;

  /**
   * If true, wrap in CREATE INDEX. Use for columns that show up in WHERE
   * and JOIN clauses often (location_id, contact_id, updated_at, etc.).
   */
  indexed?: boolean;

  /**
   * Path inside the upstream GHL JSON payload. Dot-notation for nested.
   * Sync worker uses this to map API responses into rows. If omitted,
   * defaults to the column name.
   */
  source_path?: string;
}

// ---------------------------------------------------------------------------
// Sync descriptors
// ---------------------------------------------------------------------------

export interface BackfillDescriptor {
  /** API path relative to services.leadconnectorhq.com. */
  endpoint: string;
  method: "GET" | "POST";
  /**
   * Parameter name used to scope the request to a location.
   * Defaults to "locationId" (camelCase, what most GHL v2 endpoints use).
   * Override to "location_id" for the snake_case stragglers like
   * /opportunities/search.
   */
  location_param_name?: string;
  /**
   * Pagination strategy.
   *   cursor   — keyset pagination via a cursor field
   *   page     — numeric page number (rare in GHL)
   *   none     — single-response endpoint, no pagination needed
   *   unknown  — contract not yet probed against live GHL. Consumers
   *              (sync worker, audit runner) MUST refuse to operate on
   *              entities in this state. Used for entities we've declared
   *              but haven't verified a real list endpoint for.
   */
  pagination: "cursor" | "page" | "none" | "unknown";
  /** Response field holding the array of records. Defaults vary per endpoint. */
  items_field?: string;
  /**
   * For cursor pagination: response field containing the next cursor,
   * and the query-param name to send it back on the next request.
   */
  cursor_response_field?: string;
  cursor_request_param?: string;
  /** Fixed query-string params always sent with this backfill. */
  query_extras?: Record<string, string | number | boolean>;
  /**
   * For endpoints that iterate over a parent (e.g. contact tasks require
   * a contactId), name the parent entity and the child column holding
   * the parent ID. Sync iterates all parent rows and paginates per parent.
   */
  per_parent?: { parent_entity: string; parent_fk_column: string };
}

export interface IncrementalDescriptor {
  /**
   * How updates are detected:
   *   updated_after — re-pull records where updated_at > last_cursor
   *   poll_full     — re-pull everything every interval (low-volume tables)
   *   per_parent    — rely on parent refresh + per_parent backfill
   */
  type: "updated_after" | "poll_full" | "per_parent";
  /** Column in our table holding the cursor timestamp. */
  cursor_column?: string;
  /** Query param / filter field name for "fetch records updated after X". */
  cursor_query_param?: string;
  /** Polling interval hint in minutes (sync worker reads this). */
  poll_interval_minutes: number;
  /**
   * Explicit flag that the filter contract has been verified against
   * live GHL. Only when true will the sync worker run an incremental
   * poll against this entity. When false, the entity stays synced via
   * periodic full backfills (triggered manually today, cron later).
   *
   * GHL's filter grammar varies per-endpoint: /contacts/search uses
   * a filters[] array with short operator codes (eq/gt/lte/contains/
   * range/etc.), while other endpoints take top-level query params.
   * This flag gates incremental sync until a maintainer has confirmed
   * the exact shape for the endpoint works. For `poll_full` this is
   * always true — no filter is sent.
   */
  filter_ready: boolean;
}

export interface WebhookEvent {
  /** GHL event name as emitted by the platform, e.g. "ContactCreate". */
  ghl_event: string;
  /** What this event implies for our stored row. */
  kind: "upsert" | "delete";
}

// ---------------------------------------------------------------------------
// Source audit
// ---------------------------------------------------------------------------

/**
 * Six-check audit per entity. Which checks are required depends on the
 * entity — see `requiredAuditChecks()` below. An entity passes audit when
 * every required check is true.
 */
export interface AuditReport {
  /** Has the audit been run end-to-end against a live Topline sub-account? */
  live_tested: boolean;
  /** GHL's `id` field is immutable across updates. */
  stable_pk: boolean;
  /** A paginated endpoint exists that returns every record. */
  backfill_path: boolean;
  /** updated_after filter OR webhook-based incremental confirmed. */
  incremental_path: boolean;
  /**
   * We know which timestamp field advances on every mutation.
   * N/A for `incremental.type === "poll_full"` tables — the gate skips
   * this check for those.
   */
  update_cursor: boolean;
  /**
   * Webhooks confirmed firing within ~10s.
   * Required for phase-1 hot tables. Optional (undefined = N/A) for warm/cool.
   */
  webhook_coverage?: boolean;
  /** Free-form notes: known issues, GHL quirks, caveats. */
  notes?: string;
}

/**
 * Which AuditReport fields must be true for this entity to pass the gate.
 * Keyed so failures can report which check blocked exposure.
 */
export function requiredAuditChecks(
  entity: EntityManifest,
): readonly (keyof AuditReport)[] {
  const checks: (keyof AuditReport)[] = [
    "live_tested",
    "stable_pk",
    "backfill_path",
    "incremental_path",
  ];
  // update_cursor is only meaningful when this entity has its own
  // updated_after-style cursor. Two incremental modes explicitly don't:
  //   poll_full  — re-fetches everything every interval; no cursor at all
  //   per_parent — inherits freshness from its parent's refresh cadence,
  //                doesn't track its own cursor
  const ownsCursor =
    entity.incremental.type !== "poll_full" &&
    entity.incremental.type !== "per_parent";
  if (ownsCursor) {
    checks.push("update_cursor");
  }
  // Phase-1 tables need *some* reliable freshness mechanism. Originally
  // that meant webhook_coverage was required. In practice a verified
  // cron+filter path is equivalent for analytics workloads — poll every
  // 15 min with a server-side date filter (filter_ready: true) gets the
  // same result as near-real-time webhooks for the class of queries
  // LLMs care about (GROUP BY, COUNT, JOIN, analytical aggregates).
  //
  // Gate: webhook_coverage is required UNLESS one of these is true:
  //   - incremental.type === "poll_full" (re-fetches everything)
  //   - incremental.type === "updated_after" AND filter_ready === true
  //
  // Entities that need true real-time (future: live chat dashboards,
  // alerting) should flip webhook_coverage true anyway; this gate is
  // about the minimum acceptable freshness floor for analytics exposure.
  const hasCronFreshness =
    entity.incremental.type === "poll_full" ||
    (entity.incremental.type === "updated_after" &&
      entity.incremental.filter_ready === true);
  if (entity.phase === 1 && !hasCronFreshness) {
    checks.push("webhook_coverage");
  }
  return checks;
}

/** An entity passes audit when every required check is explicitly true. */
export function auditPasses(entity: EntityManifest): boolean {
  const report = entity.audit;
  for (const check of requiredAuditChecks(entity)) {
    if (report[check] !== true) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Entity manifest
// ---------------------------------------------------------------------------

/**
 * One entry = one table in each tenant's LocationDO SQLite database.
 */
export interface EntityManifest {
  /** Table name in SQL (and the word users reference in SELECT). */
  table: string;

  /** One-sentence description shown to the LLM via describe_schema. */
  description: string;

  /**
   * Phase 1 (hot)    — webhook + 15-min poll. Highest-volume, highest-value.
   * Phase 2 (warm)   — 15-min poll only.
   * Phase 3 (cool)   — daily poll only.
   */
  phase: 1 | 2 | 3;

  /** Column defs. */
  columns: readonly ColumnDef[];

  /** The column holding the primary key (must match a columns[].name). */
  primary_key: string;

  /** Backfill config. */
  backfill: BackfillDescriptor;

  /** Incremental sync config. */
  incremental: IncrementalDescriptor;

  /** Webhook events (phase-1 tables only). */
  webhooks?: readonly WebhookEvent[];

  /** Source audit — exposure gate. */
  audit: AuditReport;

  /**
   * If false, the table exists in the schema but is hidden from
   * `describe_schema`. Set manually to true after audit passes AND
   * we've verified rows look right in a live sub-account.
   */
  exposed: boolean;
}
