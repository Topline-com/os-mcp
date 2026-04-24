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
  primary_key: string;
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
    // Additive migration for DOs created before watermark / backfill_complete
    // were introduced. Both are nullable/defaulted so ADD COLUMN is safe.
    this.ensureSyncStateColumn("watermark", "TEXT");
    this.ensureSyncStateColumn("backfill_complete", "INTEGER NOT NULL DEFAULT 0");

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
      }>(
        "SELECT entity, cursor, watermark, backfill_complete, last_sync_at, row_count FROM _sync_state",
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
    return {
      tables,
      dialect_notes: [
        "SQLite dialect. No DATE_TRUNC — use strftime('%Y-%m-%d', col) for date truncation.",
        "Timestamps are ISO 8601 strings. Compare lexicographically or parse with strftime.",
        "JSON columns (e.g. contacts.tags, contacts.custom_fields) — query with json_extract() and json_each().",
        "Tenant isolation is physical; every row you see is already scoped to your sub-account.",
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
      if (!e || !e.exposed || !auditPasses(e)) {
        throw new Error(`Unknown or unavailable table: ${t}`);
      }
      return explainOne(e, state[t]?.row_count ?? 0);
    });
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
  if (typeof value === "string" || typeof value === "number") return value;
  if (value instanceof ArrayBuffer) return value;
  return String(value);
}

