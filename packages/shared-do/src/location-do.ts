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
// Migration strategy: the DO runs schema migrations lazily on first use.
// Applied statement hashes are tracked in `_migrations` so reruns are safe
// and new manifest entries land as incremental CREATE TABLE IF NOT EXISTS
// statements. SQLite is forgiving; this is sufficient for phase 1. When
// we need real column drops / renames, we'll introduce numbered migrations
// with an explicit direction.

import { DurableObject } from "cloudflare:workers";
import {
  ALL_ENTITIES,
  ENTITY_BY_TABLE,
  migrationStatements,
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
// The DO class
// ---------------------------------------------------------------------------

export class LocationDO extends DurableObject<LocationDOEnv> {
  private initialized = false;

  /**
   * Lazy init. Every RPC method calls this first. Migrations are idempotent
   * and cheap after the first call (tracked in `_migrations`).
   */
  private ensureInitialized(): void {
    if (this.initialized) return;
    this.runMigrations();
    this.initialized = true;
  }

  /**
   * Runs every migration statement that hasn't been applied to this DO yet.
   * Idempotent: statements are keyed by SHA-256 of their SQL text.
   */
  private runMigrations(): void {
    const sql = this.ctx.storage.sql;

    // Bookkeeping tables first. These aren't in the manifest — they're DO
    // infrastructure, not user-queryable data.
    sql.exec(`
      CREATE TABLE IF NOT EXISTS _migrations (
        statement_hash TEXT PRIMARY KEY,
        statement TEXT NOT NULL,
        applied_at TEXT NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS _sync_state (
        entity TEXT PRIMARY KEY,
        cursor TEXT,
        last_sync_at TEXT,
        row_count INTEGER NOT NULL DEFAULT 0
      )
    `);

    // Apply every manifest statement that hasn't landed yet. Hash-keyed so
    // manifest edits that change a statement cause a fresh apply.
    for (const stmt of migrationStatements()) {
      const hash = hashStatement(stmt);
      const existing = sql
        .exec<{ statement_hash: string }>(
          "SELECT statement_hash FROM _migrations WHERE statement_hash = ?",
          hash,
        )
        .toArray();
      if (existing.length > 0) continue;
      sql.exec(stmt);
      sql.exec(
        "INSERT INTO _migrations(statement_hash, statement, applied_at) VALUES (?, ?, ?)",
        hash,
        stmt,
        new Date().toISOString(),
      );
    }
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

  /** Snapshot of per-entity sync state. */
  async getSyncState(): Promise<SyncState> {
    this.ensureInitialized();
    const rows = this.ctx.storage.sql
      .exec<{ entity: string; cursor: string | null; last_sync_at: string | null; row_count: number }>(
        "SELECT entity, cursor, last_sync_at, row_count FROM _sync_state",
      )
      .toArray();
    const out: SyncState = {};
    for (const r of rows) {
      out[r.entity] = {
        cursor: r.cursor,
        last_sync_at: r.last_sync_at,
        row_count: r.row_count,
      };
    }
    return out;
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

  /** Detailed column info for explain_tables. */
  async explainTables(tables: readonly string[]): Promise<TableDetails[]> {
    this.ensureInitialized();
    const state = await this.getSyncState();
    return tables.map((t) => {
      const e = ENTITY_BY_TABLE.get(t);
      if (!e) throw new Error(`Unknown table: ${t}`);
      return explainOne(e, state[t]?.row_count ?? 0);
    });
  }

  /** Cheap health probe used by diagnostic endpoints. */
  async ping(): Promise<{ ok: true; initialized: boolean; tables_applied: number }> {
    this.ensureInitialized();
    const n = this.ctx.storage.sql
      .exec<{ n: number }>("SELECT COUNT(*) AS n FROM _migrations")
      .one().n;
    return { ok: true, initialized: this.initialized, tables_applied: n };
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

function hashStatement(stmt: string): string {
  // djb2-style — we don't need cryptographic strength here, just a stable
  // fingerprint so a re-run detects identical statements. crypto.subtle is
  // async; keeping the migration runner synchronous is worth avoiding.
  let h = 5381;
  for (let i = 0; i < stmt.length; i++) {
    h = ((h << 5) + h + stmt.charCodeAt(i)) | 0;
  }
  return `djb2-${(h >>> 0).toString(16)}`;
}
