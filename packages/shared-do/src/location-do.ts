// LocationDO — one Durable Object instance per `location_id` (tenant).
// Owns that tenant's entire synced data warehouse in an embedded SQLite
// database. Other workers (edge, sync) interact via native RPC:
//
//   const id = env.LOCATION_DO.idFromName(locationId);
//   const stub = env.LOCATION_DO.get(id);
//   await stub.upsertRows("contacts", rows);
//
// Tenant isolation is physical — one DO instance per location means there
// is no shared query engine that could leak cross-tenant rows. The code in
// this file has no concept of "the other tenant's data" because there is
// no such data in scope.
//
// Migration strategy: schema-diff, not statement-replay.
//
// On first RPC call, LocationDO introspects its current SQLite state via
// PRAGMA table_info and reconciles against the @topline/shared-schema
// manifest:
//   - Missing tables → CREATE TABLE
//   - Missing (nullable) columns → ALTER TABLE ADD COLUMN
//   - Missing indexes → CREATE INDEX IF NOT EXISTS
//
// Schema changes we DON'T auto-apply (and will throw or log instead):
//   - Adding a NOT NULL column to an existing table (SQLite requires
//     a DEFAULT; manifest doesn't model defaults yet)
//   - Column renames, drops, type changes
//
// When one of those lands we'll introduce explicit numbered migration
// files. The `_schema_log` table is an append-only audit log of every
// DDL operation this DO has applied — useful for post-hoc debugging of
// schema drift across tenants. (Earlier versions of this code used a
// table called `_migrations` with a different shape; the new name
// avoids a collision on DOs that were created before the rewrite.
// Dead `_migrations` tables on older DOs are harmless.)

import { DurableObject } from "cloudflare:workers";
import {
  ALL_ENTITIES,
  ENTITY_BY_TABLE,
  ANALYTICS_VIEWS,
  renderCreateTable,
  renderColumn,
  renderIndexes,
  auditPasses,
  type EntityManifest,
  type ColumnDef,
} from "@topline/shared-schema";

// ---------------------------------------------------------------------------
// RPC response shapes — kept serializable (no class instances, no functions)
// ---------------------------------------------------------------------------

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  elapsed_ms: number;
  truncated: boolean;
}

export interface SyncState {
  [entity: string]: {
    cursor: string | null;
    /** Max cursor_column value seen; drives incremental polling. */
    watermark: string | null;
    /** 1 once backfill has walked the last page; 0 during partial backfill. */
    backfill_complete: boolean;
    last_sync_at: string | null;
    row_count: number;
    /**
     * ISO 8601 stamp when the current drain started. NULL once the
     * drain completes. Used by the snapshot-and-swap reconciliation
     * to distinguish rows refreshed during this drain from stale ones
     * that disappeared upstream.
     */
    drain_started_at: string | null;
    /**
     * Most recent upstream `total` GHL returned on a response that
     * carried one. NULL if we've never seen one. Compared against
     * row_count to detect stale backfill_complete states from older
     * broken-cursor runs and trigger a self-heal re-drain.
     */
    upstream_total: number | null;
  };
}

export interface UpsertResult {
  upserted: number;
  row_count_after: number;
}

export interface SchemaOverview {
  tables: Array<{
    name: string;
    description: string;
    row_count: number;
  }>;
  dialect_notes: string[];
  query_rules: string[];
}

export interface TableDetails {
  table: string;
  description: string;
  /** Null for views. */
  primary_key: string | null;
  row_count: number;
  columns: Array<{
    name: string;
    type: "TEXT" | "INTEGER" | "REAL";
    nullable: boolean;
    description: string;
    enum?: readonly string[];
    references?: string;
    json?: boolean;
  }>;
  relations: Array<{ column: string; references: string }>;
  notes?: string;
}

// ---------------------------------------------------------------------------
// Env interface the DO expects. Empty today; leaves room for future bindings
// (e.g., R2 for raw payload archive).
// ---------------------------------------------------------------------------

export interface LocationDOEnv {}

// ---------------------------------------------------------------------------
// Query cap — same default as Streamlined's docs. Edge will validate/cap
// user input separately; this is a defensive inner limit.
// ---------------------------------------------------------------------------

const DEFAULT_ROW_CAP = 5000;

// ---------------------------------------------------------------------------
// Dedicated error type for migrations so callers can distinguish schema
// problems from runtime query failures.
// ---------------------------------------------------------------------------

export class LocationDOMigrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LocationDOMigrationError";
  }
}

// ---------------------------------------------------------------------------
// The DO class
// ---------------------------------------------------------------------------

export class LocationDO extends DurableObject<LocationDOEnv> {
  private initialized = false;

  /**
   * Lazy init. Every RPC method calls this first. Migrations are idempotent
   * and cheap after the first call (tracked in `_schema_log`).
   */
  private ensureInitialized(): void {
    if (this.initialized) return;
    this.runMigrations();
    this.initialized = true;
  }

  /**
   * Reconciles this DO's SQLite schema with the current manifest.
   *
   * Strategy: schema diff, not statement-replay.
   *
   *   1. For each manifest entity, introspect the actual columns via
   *      PRAGMA table_info().
   *   2. If the table doesn't exist → CREATE TABLE from the manifest.
   *   3. If the table exists → ALTER TABLE ADD COLUMN for every manifest
   *      column missing from the live table.
   *   4. Unsupported diffs (rename, drop, type change, adding a NOT NULL
   *      column without a DEFAULT) throw — they require a hand-written
   *      migration, not auto-apply.
   *   5. CREATE INDEX IF NOT EXISTS for every indexed column (name-keyed
   *      idempotent — safe to rerun).
   *
   * Why not the old hash-keyed CREATE TABLE IF NOT EXISTS approach?
   * Because SQLite treats CREATE TABLE IF NOT EXISTS as a no-op when the
   * table already exists, so manifest edits that added a column would be
   * recorded as "applied" without actually altering anything, leaving
   * upserts to fail at runtime with "no such column". Schema-diff sees
   * the real table and emits the right ALTER.
   *
   * Every schema change is logged to `_schema_log` as an audit trail so
   * ops can reconstruct what happened to a given tenant's DO without
   * re-running anything.
   */
  private runMigrations(): void {
    const sql = this.ctx.storage.sql;

    // Bookkeeping tables. These aren't in the manifest.
    sql.exec(`
      CREATE TABLE IF NOT EXISTS _schema_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation TEXT NOT NULL,
        target TEXT NOT NULL,
        statement TEXT NOT NULL,
        applied_at TEXT NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS _sync_state (
        entity TEXT PRIMARY KEY,
        cursor TEXT,
        watermark TEXT,
        backfill_complete INTEGER NOT NULL DEFAULT 0,
        last_sync_at TEXT,
        row_count INTEGER NOT NULL DEFAULT 0
      )
    `);
    // Per-parent sync state. Replaces the naive "ORDER BY parent.id ASC
    // with a global resume cursor" approach in backfillPerParent: each
    // parent tracks its own cursor + completion flag, so a mid-parent
    // subrequest-cap error doesn't lose progress and a freshly-bumped
    // parent (conversation with a new message, contact with a new task)
    // gets picked back up on the next tick without re-walking the whole
    // parent table.
    sql.exec(`
      CREATE TABLE IF NOT EXISTS _parent_sync_state (
        entity TEXT NOT NULL,
        parent_id TEXT NOT NULL,
        cursor TEXT,
        backfill_complete INTEGER NOT NULL DEFAULT 0,
        last_sync_at TEXT,
        PRIMARY KEY (entity, parent_id)
      )
    `);
    // drain_started_at on _parent_sync_state: stamped when a parent
    // first starts syncing (pre-existing rows: NULL via additive
    // migration). Used by the per-parent snapshot-and-swap at
    // markParentBackfillComplete to prune children that existed in
    // this DO but weren't re-observed during the current drain —
    // i.e. the upstream record deleted them. Parent-scoped prune
    // mirrors the entity-level pruneStaleRows but bounded to one
    // parent's FK.
    this.ensurePerParentStateColumn("drain_started_at", "TEXT");
    // Additive migration for DOs created before watermark / backfill_complete
    // were introduced. Both are nullable/defaulted so ADD COLUMN is safe.
    this.ensureSyncStateColumn("watermark", "TEXT");
    this.ensureSyncStateColumn("backfill_complete", "INTEGER NOT NULL DEFAULT 0");
    // ISO 8601 stamp of when THIS drain started. Set on the first page of
    // a fresh backfill (when cursor was null AND backfill_complete was 0)
    // and cleared when the drain completes. Used by the snapshot-and-swap
    // reconciliation at drain end: any row whose _synced_at predates
    // drain_started_at was not re-observed, so the upstream deleted it.
    this.ensureSyncStateColumn("drain_started_at", "TEXT");
    // Upstream row total captured on the most recent response that
    // carried one (GHL puts `total` at the top level on some endpoints).
    // The cron's drift-detection compares this against row_count to catch
    // stale `backfill_complete=1` states left by older broken cursor
    // code; when the gap exceeds ~10% the cron resets the complete flag
    // and re-drains from scratch.
    this.ensureSyncStateColumn("upstream_total", "INTEGER");

    // Drop any existing view whose name collides with a manifested
    // entity. This happens during the call_events view → real-table
    // migration: a DO initialized before this change had call_events
    // as a VIEW, which reconcileTable's PRAGMA table_info check sees
    // as a "table with no primary key" and fails on. Query
    // sqlite_master so we only DROP VIEW on names that are actually
    // views (DROP VIEW on a table throws "use DROP TABLE").
    const entityNameSet = new Set(ALL_ENTITIES.map((e) => e.table));
    const existingViews = sql
      .exec<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type = 'view'`,
      )
      .toArray();
    for (const row of existingViews) {
      if (entityNameSet.has(row.name)) {
        sql.exec(`DROP VIEW IF EXISTS ${row.name}`);
      }
    }

    for (const entity of ALL_ENTITIES) {
      this.reconcileTable(entity);
    }

    // Indexes are name-keyed; CREATE INDEX IF NOT EXISTS is the correct
    // idempotent primitive. Reapply every time — no-op if already present,
    // picks up new indexed columns if added.
    for (const entity of ALL_ENTITIES) {
      for (const idxStmt of renderIndexes(entity)) {
        sql.exec(idxStmt);
      }
    }

    // Derived analytics views. CREATE VIEW IF NOT EXISTS is idempotent
    // but does NOT update the view body when the definition changes —
    // we DROP first so reshapes in code take effect on the next DO
    // restart. Views are read-only aliases over the base tables; no
    // storage cost, always consistent with the current data.
    for (const v of ANALYTICS_VIEWS) {
      sql.exec(`DROP VIEW IF EXISTS ${v.name}`);
      sql.exec(v.ddl);
    }
  }

  /**
   * Bring one entity's table into alignment with its manifest.
   *
   * Two-phase reconciliation:
   *
   *   Phase 1 — detect unsupported drift. Throws LocationDOMigrationError
   *     for any of:
   *       1a. PRIMARY KEY mismatch (live PK column != manifest primary_key,
   *           or composite live PK vs single-column manifest PK)
   *       1b. A live column missing from the manifest (rename or drop)
   *       1c. Type mismatch (manifest TEXT, live INTEGER, etc.)
   *       1d. Nullability flip
   *
   *   Phase 2 — apply additive changes. For every manifest column
   *     missing from the live table, ALTER TABLE ADD COLUMN. NOT NULL
   *     additions on an existing table throw (SQLite requires DEFAULT,
   *     which the manifest doesn't model yet).
   *
   * What's explicitly out of scope and therefore blocked at init:
   * column renames, drops, type changes, nullability flips, and PK
   * changes. Each requires a hand-written migration. Failing loudly at
   * init is strictly safer than silent drift — a DO that can't
   * initialize also cannot serve queries that return the wrong shape,
   * accept upserts against mismatched types, or fail runtime upserts
   * because ON CONFLICT no longer matches a real PK.
   */
  private reconcileTable(entity: EntityManifest): void {
    const sql = this.ctx.storage.sql;
    const live = sql
      .exec<{
        cid: number;
        name: string;
        type: string;
        notnull: number;
        dflt_value: string | null;
        pk: number;
      }>(`PRAGMA table_info(${quoteIdent(entity.table)})`)
      .toArray();

    if (live.length === 0) {
      // Fresh table.
      const stmt = renderCreateTable(entity);
      sql.exec(stmt);
      this.logMigration("CREATE TABLE", entity.table, stmt);
      return;
    }

    const manifestByName = new Map<string, ColumnDef>(
      entity.columns.map((c) => [c.name, c]),
    );

    // Phase 1a: detect PRIMARY KEY drift. PRAGMA table_info marks PK
    // columns with pk > 0 (position within a composite PK). We only
    // support single-column PKs in the manifest, so any composite live
    // PK is also a mismatch.
    //
    // This matters because upsertRows builds `ON CONFLICT(<pk>) DO
    // UPDATE` — if the manifest's primary_key no longer matches the
    // live table's PK, upserts fail at runtime with "ON CONFLICT
    // clause does not match any PRIMARY KEY or UNIQUE constraint".
    // Detecting this at init means the DO refuses to serve rather
    // than serving a broken write path.
    const livePkCols = live
      .filter((r) => r.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map((r) => r.name);
    if (livePkCols.length !== 1 || livePkCols[0] !== entity.primary_key) {
      throw new LocationDOMigrationError(
        `Primary key mismatch on ${entity.table}: ` +
          `live=[${livePkCols.join(", ") || "<none>"}], manifest=${entity.primary_key}. ` +
          `Primary key changes require an explicit migration.`,
      );
    }

    // Phase 1b: detect unsupported drift in every live column.
    for (const liveCol of live) {
      const manifestCol = manifestByName.get(liveCol.name);
      if (!manifestCol) {
        throw new LocationDOMigrationError(
          `Column ${entity.table}.${liveCol.name} exists in SQLite but not in the manifest. ` +
            `Dropping or renaming columns requires an explicit migration; this DO is out of sync with the current schema.`,
        );
      }
      if (liveCol.type.toUpperCase() !== manifestCol.sqlite_type) {
        throw new LocationDOMigrationError(
          `Column ${entity.table}.${liveCol.name} type mismatch: ` +
            `live=${liveCol.type}, manifest=${manifestCol.sqlite_type}. ` +
            `SQLite column types cannot be changed in place; this requires an explicit migration.`,
        );
      }
      const liveNullable = liveCol.notnull === 0;
      if (liveNullable !== manifestCol.nullable) {
        throw new LocationDOMigrationError(
          `Column ${entity.table}.${liveCol.name} nullability mismatch: ` +
            `live=${liveNullable ? "nullable" : "NOT NULL"}, manifest=${manifestCol.nullable ? "nullable" : "NOT NULL"}. ` +
            `Nullability changes require an explicit migration.`,
        );
      }
    }

    // Phase 2: add missing columns (additive, nullable-only).
    const liveNames = new Set(live.map((r) => r.name));
    for (const col of entity.columns) {
      if (liveNames.has(col.name)) continue;

      if (!col.nullable) {
        throw new LocationDOMigrationError(
          `Cannot auto-add NOT NULL column ${entity.table}.${col.name} to an existing table without a DEFAULT. ` +
            `Either make it nullable in the manifest or write a dedicated migration.`,
        );
      }

      const stmt = `ALTER TABLE ${quoteIdent(entity.table)} ADD COLUMN ${renderColumn(col)};`;
      sql.exec(stmt);
      this.logMigration("ADD COLUMN", `${entity.table}.${col.name}`, stmt);
    }
  }

  private logMigration(operation: string, target: string, statement: string): void {
    this.ctx.storage.sql.exec(
      "INSERT INTO _schema_log(operation, target, statement, applied_at) VALUES (?, ?, ?, ?)",
      operation,
      target,
      statement,
      new Date().toISOString(),
    );
  }

  /**
   * Ensure a column exists on _sync_state. Used for additive migrations of
   * the bookkeeping table itself (the entity tables go through the
   * manifest-driven reconcileTable path instead).
   */
  private ensureSyncStateColumn(column: string, decl: string): void {
    const sql = this.ctx.storage.sql;
    const cols = sql
      .exec<{ name: string }>("PRAGMA table_info(_sync_state)")
      .toArray();
    if (cols.some((c) => c.name === column)) return;
    const stmt = `ALTER TABLE _sync_state ADD COLUMN ${column} ${decl}`;
    sql.exec(stmt);
    this.logMigration("ADD COLUMN", `_sync_state.${column}`, stmt);
  }

  /** Mirror of ensureSyncStateColumn for the _parent_sync_state table. */
  private ensurePerParentStateColumn(column: string, decl: string): void {
    const sql = this.ctx.storage.sql;
    const cols = sql
      .exec<{ name: string }>("PRAGMA table_info(_parent_sync_state)")
      .toArray();
    if (cols.some((c) => c.name === column)) return;
    const stmt = `ALTER TABLE _parent_sync_state ADD COLUMN ${column} ${decl}`;
    sql.exec(stmt);
    this.logMigration("ADD COLUMN", `_parent_sync_state.${column}`, stmt);
  }

  // -------------------------------------------------------------------------
  // RPC: sync-side writes
  // -------------------------------------------------------------------------

  /**
   * Upsert rows into an entity's table.
   *
   * `rows` MUST already be keyed by column name (not the upstream GHL path).
   * The sync worker handles the source_path → column_name mapping before
   * calling this — it's not the DO's job to know GHL's JSON shape.
   *
   * Columns not present on a row are written as NULL. Extra keys on a row
   * (not in the column schema) are silently ignored.
   */
  async upsertRows(table: string, rows: ReadonlyArray<Record<string, unknown>>): Promise<UpsertResult> {
    this.ensureInitialized();
    const entity = ENTITY_BY_TABLE.get(table);
    if (!entity) throw new Error(`Unknown table: ${table}`);
    if (rows.length === 0) {
      const n = countRows(this.ctx.storage.sql, entity.table);
      return { upserted: 0, row_count_after: n };
    }

    const columnNames = entity.columns.map((c) => c.name);
    const pk = entity.primary_key;
    const placeholders = columnNames.map(() => "?").join(", ");
    const columnList = columnNames.map(quoteIdent).join(", ");
    const updateSet = columnNames
      .filter((c) => c !== pk)
      .map((c) => `${quoteIdent(c)} = excluded.${quoteIdent(c)}`)
      .join(", ");

    const now = new Date().toISOString();
    const sql = this.ctx.storage.sql;
    let upserted = 0;

    // Stamp _synced_at on every write.
    const stmt = `INSERT INTO ${quoteIdent(entity.table)} (${columnList}) VALUES (${placeholders}) ON CONFLICT(${quoteIdent(pk)}) DO UPDATE SET ${updateSet}`;

    for (const row of rows) {
      const values: SqlStorageValue[] = entity.columns.map((col) => {
        if (col.name === "_synced_at") return now;
        return coerceForSqlite(col, row[col.name]);
      });
      sql.exec(stmt, ...values);
      upserted++;
    }

    const rowCount = countRows(sql, entity.table);
    sql.exec(
      `INSERT INTO _sync_state(entity, last_sync_at, row_count) VALUES (?, ?, ?)
       ON CONFLICT(entity) DO UPDATE SET last_sync_at = excluded.last_sync_at, row_count = excluded.row_count`,
      entity.table,
      now,
      rowCount,
    );

    return { upserted, row_count_after: rowCount };
  }

  /** Advance the incremental-sync cursor for an entity. */
  async setSyncCursor(entity: string, cursor: string): Promise<void> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _sync_state(entity, cursor, last_sync_at) VALUES (?, ?, ?)
       ON CONFLICT(entity) DO UPDATE SET cursor = excluded.cursor, last_sync_at = excluded.last_sync_at`,
      entity,
      cursor,
      now,
    );
  }

  /**
   * Clear the sync cursor for an entity so the next backfill starts
   * from the beginning. Does not touch the table's data — only the
   * resume pointer in _sync_state.
   */
  async clearSyncCursor(entity: string): Promise<void> {
    this.ensureInitialized();
    this.ctx.storage.sql.exec(
      `UPDATE _sync_state SET cursor = NULL WHERE entity = ?`,
      entity,
    );
  }

  /** Snapshot of per-entity sync state. */
  async getSyncState(): Promise<SyncState> {
    this.ensureInitialized();
    const rows = this.ctx.storage.sql
      .exec<{
        entity: string;
        cursor: string | null;
        watermark: string | null;
        backfill_complete: number;
        last_sync_at: string | null;
        row_count: number;
        drain_started_at: string | null;
        upstream_total: number | null;
      }>(
        "SELECT entity, cursor, watermark, backfill_complete, last_sync_at, row_count, drain_started_at, upstream_total FROM _sync_state",
      )
      .toArray();
    const out: SyncState = {};
    for (const r of rows) {
      out[r.entity] = {
        cursor: r.cursor,
        watermark: r.watermark,
        backfill_complete: r.backfill_complete === 1,
        last_sync_at: r.last_sync_at,
        row_count: r.row_count,
        drain_started_at: r.drain_started_at,
        upstream_total: r.upstream_total,
      };
    }
    return out;
  }

  /**
   * Advance the incremental watermark for an entity. The watermark is the
   * max cursor_column value (typically updated_at) the worker has seen for
   * this entity — the next incremental poll uses it to filter GHL to only
   * newly-updated rows.
   */
  async setSyncWatermark(entity: string, watermark: string): Promise<void> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _sync_state(entity, watermark, last_sync_at) VALUES (?, ?, ?)
       ON CONFLICT(entity) DO UPDATE SET watermark = excluded.watermark, last_sync_at = excluded.last_sync_at`,
      entity,
      watermark,
      now,
    );
  }

  /**
   * Mark an entity's initial backfill as complete. This is the gate that
   * allows incremental polling to run — without it, the sync worker
   * refuses to incremental-sync (we'd miss rows with updated_at values
   * earlier than the partial-backfill watermark).
   */
  async markBackfillComplete(entity: string): Promise<void> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _sync_state(entity, backfill_complete, last_sync_at) VALUES (?, 1, ?)
       ON CONFLICT(entity) DO UPDATE SET backfill_complete = 1, last_sync_at = excluded.last_sync_at`,
      entity,
      now,
    );
  }

  /**
   * Reset an entity's backfill state so the next invocation drains from
   * scratch. Used by the self-heal path when we detect row_count drift
   * vs. upstream total — typically a sign an older broken-cursor run
   * left us with `backfill_complete = 1` alongside partial data.
   * Does NOT delete existing rows; the re-drain will refresh them in
   * place and the post-drain snapshot-and-swap will prune any that
   * disappeared upstream.
   */
  async resetBackfill(entity: string): Promise<void> {
    this.ensureInitialized();
    this.ctx.storage.sql.exec(
      `UPDATE _sync_state
         SET backfill_complete = 0, cursor = NULL, drain_started_at = NULL
       WHERE entity = ?`,
      entity,
    );
  }

  /**
   * Stamp the start of a fresh drain on an entity's sync state. Called
   * by the backfill loops the first time they run with cursor=null AND
   * backfill_complete=0. Every row upserted from this point forward
   * carries a `_synced_at >= drain_started_at`; rows that were never
   * touched keep their stale `_synced_at` from a previous drain.
   * Idempotent — only sets when currently NULL, so a multi-tick drain
   * doesn't keep bumping the marker forward.
   */
  async startDrainIfUnset(entity: string): Promise<string> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _sync_state(entity, drain_started_at, last_sync_at) VALUES (?, ?, ?)
       ON CONFLICT(entity) DO UPDATE
         SET drain_started_at = COALESCE(_sync_state.drain_started_at, excluded.drain_started_at),
             last_sync_at = excluded.last_sync_at`,
      entity,
      now,
      now,
    );
    const row = this.ctx.storage.sql
      .exec<{ drain_started_at: string | null }>(
        `SELECT drain_started_at FROM _sync_state WHERE entity = ?`,
        entity,
      )
      .one();
    return row.drain_started_at ?? now;
  }

  /**
   * Persist the most recent upstream `total` seen in a response. Used
   * by the self-heal path to compare against row_count and detect stale
   * backfill_complete flags left over from older broken-cursor runs.
   */
  async setUpstreamTotal(entity: string, total: number): Promise<void> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _sync_state(entity, upstream_total, last_sync_at) VALUES (?, ?, ?)
       ON CONFLICT(entity) DO UPDATE SET upstream_total = excluded.upstream_total, last_sync_at = excluded.last_sync_at`,
      entity,
      total,
      now,
    );
  }

  /**
   * Snapshot-and-swap: delete rows whose _synced_at predates the
   * provided drain_started_at. Called at the end of a full drain to
   * prune rows that were not re-observed — i.e. upstream deleted them.
   * Returns the number of rows removed so the caller can log / meter.
   *
   * Safe because:
   *   - upsertRows stamps _synced_at = now() on every write;
   *   - drain_started_at is captured once at drain start and doesn't
   *     advance across the multi-tick drain (see startDrainIfUnset);
   *   - any row not touched during the drain keeps its old _synced_at.
   *
   * Caller is responsible for only invoking this when a drain has
   * actually walked the entire upstream (stopped_reason = empty_page
   * or cursor_stalled) — calling it on a page_cap_hit / error run
   * would delete legitimately-pending rows.
   */
  async pruneStaleRows(table: string, drainStartedAt: string): Promise<number> {
    this.ensureInitialized();
    // Validate the table name before interpolating it into SQL. Every
    // caller today is the sync worker passing entity.table from the
    // manifest, but the identifier still gets a defense-in-depth check.
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) {
      throw new Error(`Invalid table identifier: ${table}`);
    }
    const cursor = this.ctx.storage.sql.exec(
      `DELETE FROM ${table} WHERE _synced_at < ?`,
      drainStartedAt,
    );
    for (const _ of cursor) {
      // discarded — DELETE returns no rows, but iterating forces execution
    }
    const removed = cursor.rowsWritten ?? 0;
    if (removed > 0) {
      // Row count cache in _sync_state drifts after a prune; refresh it
      // so getSyncState and describe_schema reflect reality immediately
      // instead of waiting for the next upsert.
      const fresh = this.ctx.storage.sql
        .exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`)
        .one().n;
      this.ctx.storage.sql.exec(
        `UPDATE _sync_state SET row_count = ? WHERE entity = ?`,
        fresh,
        table,
      );
    }
    return removed;
  }

  /**
   * Clear the drain marker. Called by the backfill loops after
   * markBackfillComplete + pruneStaleRows so the NEXT fresh drain
   * (if resetBackfill is ever called) starts its own generation.
   */
  async clearDrainMarker(entity: string): Promise<void> {
    this.ensureInitialized();
    this.ctx.storage.sql.exec(
      `UPDATE _sync_state SET drain_started_at = NULL WHERE entity = ?`,
      entity,
    );
  }

  // -------------------------------------------------------------------------
  // Per-parent sync state (for per_parent entities: messages, tasks,
  // notes, form_submissions, survey_submissions where fanned out by
  // parent). Each parent tracks its own cursor + completion flag so
  // mid-parent errors don't lose progress and re-activated parents
  // get revisited.
  // -------------------------------------------------------------------------

  /**
   * Pick the next batch of parent IDs to process for a per-parent sync.
   *
   * Priority order:
   *   1. Parents that haven't completed their child sync (backfill_complete = 0)
   *   2. Parents that DID complete, but whose `freshnessColumns` have
   *      advanced past their last_sync_at (new activity → re-visit)
   *
   * Within each bucket, oldest-last-sync-first so the work spreads
   * across ticks without starving any one parent. Caller is expected
   * to pass parent table columns (e.g. conversations.last_message_date
   * or contacts.updated_at) that bump when something the child entity
   * cares about has changed.
   */
  async getNextParentsForChild(
    childEntity: string,
    parentTable: string,
    limit: number,
    freshnessColumns: readonly string[] = [],
  ): Promise<string[]> {
    this.ensureInitialized();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(parentTable)) {
      throw new Error(`Invalid parent table identifier: ${parentTable}`);
    }
    for (const col of freshnessColumns) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(col)) {
        throw new Error(`Invalid freshness column identifier: ${col}`);
      }
    }
    const freshnessSql = freshnessColumns.length > 0
      ? " OR " + freshnessColumns
          .map((col) => `p.${col} > COALESCE(s.last_sync_at, '')`)
          .join(" OR ")
      : "";
    const capped = Math.max(1, Math.min(500, Math.floor(limit)));
    const rows = this.ctx.storage.sql
      .exec<{ id: string }>(
        `SELECT p.id
           FROM ${parentTable} p
           LEFT JOIN _parent_sync_state s
             ON s.entity = ? AND s.parent_id = p.id
          WHERE COALESCE(s.backfill_complete, 0) = 0${freshnessSql}
          ORDER BY COALESCE(s.last_sync_at, ''), p.id
          LIMIT ?`,
        childEntity,
        capped,
      )
      .toArray();
    return rows.map((r) => String(r.id));
  }

  /** Read the persisted cursor for one parent's child sync. */
  async getParentSyncCursor(
    entity: string,
    parentId: string,
  ): Promise<string | null> {
    this.ensureInitialized();
    const rows = this.ctx.storage.sql
      .exec<{ cursor: string | null }>(
        `SELECT cursor FROM _parent_sync_state WHERE entity = ? AND parent_id = ?`,
        entity,
        parentId,
      )
      .toArray();
    return rows[0]?.cursor ?? null;
  }

  /**
   * Advance the cursor after successfully upserting a page of children
   * for one parent. Resets backfill_complete to 0 — we're mid-parent
   * until the caller explicitly flips it via markParentBackfillComplete.
   * Stamps drain_started_at on first observation (COALESCE preserves
   * the existing value across multi-tick resumes so prune-on-complete
   * sees the true drain start).
   */
  async setParentSyncCursor(
    entity: string,
    parentId: string,
    cursor: string,
  ): Promise<void> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
       VALUES (?, ?, ?, 0, ?, ?)
       ON CONFLICT(entity, parent_id) DO UPDATE
         SET cursor = excluded.cursor,
             backfill_complete = 0,
             last_sync_at = excluded.last_sync_at,
             drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)`,
      entity,
      parentId,
      cursor,
      now,
      now,
    );
  }

  /**
   * Ensure a drain marker exists for (entity, parent_id). Called by the
   * per-parent loop on the first page of a parent that has no cursor
   * (e.g. single-page endpoints like tasks/notes that never call
   * setParentSyncCursor). Idempotent via COALESCE.
   */
  async ensureParentDrainMarker(
    entity: string,
    parentId: string,
  ): Promise<string> {
    this.ensureInitialized();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
       VALUES (?, ?, NULL, 0, ?, ?)
       ON CONFLICT(entity, parent_id) DO UPDATE
         SET last_sync_at = excluded.last_sync_at,
             drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)`,
      entity,
      parentId,
      now,
      now,
    );
    const row = this.ctx.storage.sql
      .exec<{ drain_started_at: string | null }>(
        `SELECT drain_started_at FROM _parent_sync_state WHERE entity = ? AND parent_id = ?`,
        entity,
        parentId,
      )
      .one();
    return row.drain_started_at ?? now;
  }

  /**
   * Flip the parent's child sync to complete AND snapshot-and-swap
   * prune any children whose _synced_at predates the drain marker
   * (i.e. rows present in this DO from a previous drain that GHL did
   * not return this time — upstream deleted them).
   *
   * For entities with a derived dual-write (messages → call_events),
   * the caller passes `derivedTables` so the same parent-scoped prune
   * runs on the derived rows too. Otherwise a deleted TYPE_CALL
   * message would leave its call_events row affecting lead_response_
   * metrics forever.
   *
   * Returns the pruned count for observability logging.
   */
  async markParentBackfillComplete(
    entity: string,
    parentId: string,
    childTable: string,
    fkColumn: string,
    derivedTables: ReadonlyArray<{ table: string; fk: string }> = [],
  ): Promise<number> {
    this.ensureInitialized();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(childTable)) {
      throw new Error(`Invalid child table identifier: ${childTable}`);
    }
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(fkColumn)) {
      throw new Error(`Invalid fk column identifier: ${fkColumn}`);
    }
    for (const { table, fk } of derivedTables) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table) || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(fk)) {
        throw new Error(`Invalid derived table descriptor: ${table}.${fk}`);
      }
    }

    const now = new Date().toISOString();
    const drainStart = this.ctx.storage.sql
      .exec<{ drain_started_at: string | null }>(
        `SELECT drain_started_at FROM _parent_sync_state WHERE entity = ? AND parent_id = ?`,
        entity,
        parentId,
      )
      .toArray()[0]?.drain_started_at ?? null;

    let totalPruned = 0;
    if (drainStart) {
      // Prune stale children for the primary entity.
      const pruneCursor = this.ctx.storage.sql.exec(
        `DELETE FROM ${childTable} WHERE ${fkColumn} = ? AND _synced_at < ?`,
        parentId,
        drainStart,
      );
      for (const _ of pruneCursor) {
        // iterate to force execution
      }
      totalPruned += pruneCursor.rowsWritten ?? 0;

      // Prune stale derived rows for the same parent (e.g. call_events
      // where conversation_id = parentId, for a messages parent).
      for (const { table, fk } of derivedTables) {
        const c = this.ctx.storage.sql.exec(
          `DELETE FROM ${table} WHERE ${fk} = ? AND _synced_at < ?`,
          parentId,
          drainStart,
        );
        for (const _ of c) {
          // force execution
        }
        totalPruned += c.rowsWritten ?? 0;
      }
    }

    // Now mark complete + clear drain marker + cursor.
    this.ctx.storage.sql.exec(
      `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
       VALUES (?, ?, NULL, 1, ?, NULL)
       ON CONFLICT(entity, parent_id) DO UPDATE
         SET cursor = NULL,
             backfill_complete = 1,
             last_sync_at = excluded.last_sync_at,
             drain_started_at = NULL`,
      entity,
      parentId,
      now,
    );

    return totalPruned;
  }

  // -------------------------------------------------------------------------
  // RPC: query-side reads
  // -------------------------------------------------------------------------

  /**
   * Execute a SQL query against this tenant's SQLite. Trusts its caller to
   * have validated the SQL is SELECT/WITH-only. The edge's SQL tool will
   * do that parse+guard; direct callers (ops scripts, tests) must be careful.
   */
  async executeQuery(
    sql: string,
    params: readonly SqlStorageValue[] = [],
    rowCap = DEFAULT_ROW_CAP,
  ): Promise<QueryResult> {
    this.ensureInitialized();
    const started = Date.now();
    const cursor = this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(
      sql,
      ...params,
    );
    const columns = cursor.columnNames;
    const rows: Record<string, unknown>[] = [];
    let truncated = false;
    for (const row of cursor) {
      if (rows.length >= rowCap) {
        truncated = true;
        break;
      }
      rows.push(row);
    }
    return {
      columns,
      rows,
      elapsed_ms: Date.now() - started,
      truncated,
    };
  }

  /**
   * Escape hatch for admin-gated maintenance ops (schema fix-ups, one-off
   * data wipes when a source-path change leaves stale rows in a table).
   *
   * Deliberately NOT exposed through any customer-facing surface. Only the
   * edge worker's ADMIN_TOKEN-gated `/admin/do-exec` route reaches here.
   * There's no SQL-safety gate — the caller is trusted.
   *
   * Returns the same shape as executeQuery so a DELETE or DDL statement
   * can still report rows_written via SQLite's rowsWritten counter, which
   * is surfaced by the cursor itself.
   */
  async adminExecute(
    sql: string,
    params: readonly SqlStorageValue[] = [],
  ): Promise<{ rows_written: number; elapsed_ms: number }> {
    this.ensureInitialized();
    const started = Date.now();
    const cursor = this.ctx.storage.sql.exec(sql, ...params);
    // Drain so the statement actually runs to completion.
    for (const _ of cursor) {
      // discarded — callers use this for writes, not reads
    }
    return {
      rows_written: cursor.rowsWritten ?? 0,
      elapsed_ms: Date.now() - started,
    };
  }

  /** High-level schema overview for describe_schema. */
  async describeSchema(): Promise<SchemaOverview> {
    this.ensureInitialized();
    const state = await this.getSyncState();
    const tables = ALL_ENTITIES
      .filter((e) => e.exposed && auditPasses(e))
      .map((e) => ({
        name: e.table,
        description: e.description,
        row_count: state[e.table]?.row_count ?? 0,
      }));
    // Derived analytics views show up alongside base tables. Row counts
    // are best-effort — computing them requires a full scan, so we
    // return undefined and let the LLM use COUNT(*) if it cares.
    for (const v of ANALYTICS_VIEWS) {
      tables.push({
        name: v.name,
        description: `[view] ${v.description}`,
        row_count: 0,
      });
    }
    return {
      tables,
      dialect_notes: [
        "SQLite dialect. No DATE_TRUNC — use strftime('%Y-%m-%d', col) for date truncation.",
        "Timestamps are ISO 8601 strings. Compare lexicographically or parse with strftime.",
        "JSON columns (e.g. contacts.tags, contacts.custom_fields, every table's raw_payload) — query with json_extract() and json_each().",
        "Every table has a `raw_payload` TEXT/JSON column holding the full upstream GHL object — use json_extract(raw_payload, '$.meta.call.duration') etc. for fields that aren't surfaced as their own columns.",
        "Derived views (call_events, email_events, sms_events, lead_response_metrics, contact_timeline, opportunity_funnel) are READ-ONLY virtual tables — cheaper to query than re-deriving from messages yourself.",
        "Tenant isolation is physical; every row you see is already scoped to your sub-account.",
        "Counts are eventually consistent: the sync worker polls upstream every 15 min, and a small sampling gap (~0.03% on conversations) exists because the upstream cursor uses a single timestamp field with no tie-breaker. For questions sensitive to single-row exactness, prefer SUM/COUNT over ranges to smooth the variance.",
        "When a question asks about an object that doesn't appear here (calls, transcripts, forms, invoices, etc.), call topline_describe_data_catalog to see whether it's synced, pending, or requires OAuth — don't assume it doesn't exist.",
      ],
      query_rules: [
        "SELECT and WITH only. DDL, DML, PRAGMA, ATTACH, and script-loading are rejected.",
        "One statement per request.",
        `Results are capped at ${DEFAULT_ROW_CAP} rows; use LIMIT + OFFSET for larger traversals.`,
        "Queries have a 30-second timeout.",
      ],
    };
  }

  /**
   * Detailed column info for explain_tables.
   *
   * Applies the same exposure gate as describeSchema: a table that hasn't
   * passed audit or has `exposed: false` is treated as if it doesn't
   * exist, so callers can't probe hidden tables through this method even
   * though their rows physically live in the DO.
   */
  async explainTables(tables: readonly string[]): Promise<TableDetails[]> {
    this.ensureInitialized();
    const state = await this.getSyncState();
    return tables.map((t) => {
      const e = ENTITY_BY_TABLE.get(t);
      if (e && e.exposed && auditPasses(e)) {
        return explainOne(e, state[t]?.row_count ?? 0);
      }
      // Analytics views are exposed-by-default; they live off base
      // tables that already passed the same exposure gate.
      const view = ANALYTICS_VIEWS.find((v) => v.name === t);
      if (view) {
        const detail: TableDetails = {
          table: view.name,
          description: `[view] ${view.description}`,
          row_count: 0,
          columns: this.inferViewColumns(view.name),
          primary_key: null,
          relations: [],
          notes: `Derived view over: ${view.base_tables.join(", ")}. Read-only.`,
        };
        return detail;
      }
      throw new Error(`Unknown or unavailable table: ${t}`);
    });
  }

  /**
   * Pull column names out of the live view by doing a zero-row SELECT.
   * The cursor's columnNames carries the schema even when no rows match.
   * Types come back as "TEXT" in SQLite's type-affinity model unless we
   * want to parse the view DDL — for now we just surface names, which
   * is all the LLM needs to write a query.
   */
  private inferViewColumns(
    viewName: string,
  ): TableDetails["columns"] {
    try {
      const cursor = this.ctx.storage.sql.exec(`SELECT * FROM ${viewName} LIMIT 0`);
      return cursor.columnNames.map((n) => ({
        name: n,
        type: "TEXT" as const,
        nullable: true,
        description: "",
      }));
    } catch {
      return [];
    }
  }

  /** Cheap health probe used by diagnostic endpoints. */
  async ping(): Promise<{ ok: true; initialized: boolean; migration_ops: number }> {
    this.ensureInitialized();
    const n = this.ctx.storage.sql
      .exec<{ n: number }>("SELECT COUNT(*) AS n FROM _schema_log")
      .one().n;
    return { ok: true, initialized: this.initialized, migration_ops: n };
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function countRows(sql: SqlStorage, table: string): number {
  return sql
    .exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${quoteIdent(table)}`)
    .one().n;
}

function explainOne(entity: EntityManifest, rowCount: number): TableDetails {
  return {
    table: entity.table,
    description: entity.description,
    primary_key: entity.primary_key,
    row_count: rowCount,
    columns: entity.columns.map((c: ColumnDef) => ({
      name: c.name,
      type: c.sqlite_type,
      nullable: c.nullable,
      description: c.description,
      enum: c.enum,
      references: c.references,
      json: c.json,
    })),
    relations: entity.columns
      .filter((c) => c.references)
      .map((c) => ({ column: c.name, references: c.references as string })),
  };
}

function quoteIdent(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`Invalid SQL identifier: ${name}`);
  }
  return `"${name}"`;
}

/**
 * Coerce an upstream value to a SqlStorageValue.
 *   - JSON columns: stringify non-strings (pass strings through unchanged)
 *   - booleans: 0 or 1 (SQLite has no native bool)
 *   - null/undefined: null
 *   - numbers into TEXT columns: explicitly String()-ify to avoid CF's
 *     SQL binding layer formatting the JS float as "28.0" instead of "28"
 *     (JavaScript has one Number type; the binding can't tell the
 *     caller's intent without the column type as context)
 *   - anything else non-primitive: String()
 * The sync worker SHOULD pass clean values; this is defensive coercion so
 * a GHL payload shape change can't crash the upsert.
 */
function coerceForSqlite(col: ColumnDef, value: unknown): SqlStorageValue {
  if (value === null || value === undefined) return null;
  if (col.json) {
    return typeof value === "string" ? value : JSON.stringify(value);
  }
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") {
    // Number → TEXT: stringify explicitly so integers stay integers
    // (String(28) === "28", not "28.0"). For INTEGER / REAL columns
    // let the binding layer handle it natively.
    if (col.sqlite_type === "TEXT") return String(value);
    return value;
  }
  if (typeof value === "string") return value;
  if (value instanceof ArrayBuffer) return value;
  return String(value);
}

