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
  /** Pagination strategy. */
  pagination: "cursor" | "page" | "none";
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
  /** Query param name for "fetch records updated after X". */
  cursor_query_param?: string;
  /** Polling interval hint in minutes (sync worker reads this). */
  poll_interval_minutes: number;
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
 * Six-check audit per entity. Until all applicable checks pass, the entity
 * stays behind `exposed: false` and the SQL tools hide it from LLMs.
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
  /** We know which timestamp field advances on every mutation. */
  update_cursor: boolean;
  /** Webhooks confirmed firing within ~10s (hot tables only — optional). */
  webhook_coverage?: boolean;
  /** Free-form notes: known issues, GHL quirks, caveats. */
  notes?: string;
}

/** An entity passes audit when every applicable check is true. */
export function auditPasses(audit: AuditReport): boolean {
  const required = [
    audit.live_tested,
    audit.stable_pk,
    audit.backfill_path,
    audit.incremental_path,
    audit.update_cursor,
  ];
  // webhook_coverage is optional: undefined means "not claimed", false
  // would mean "claimed but failed". Only fail the audit if explicitly false.
  if (audit.webhook_coverage === false) return false;
  return required.every((v) => v === true);
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
