import { ALL_ENTITIES, ENTITY_BY_TABLE, type ColumnDef, type EntityManifest } from "@topline/shared-schema";
import { PARENT_MARK_COMPLETE, PARENT_GET_DRAIN_START, parentPruneChildrenSql, recordParentFailure as persistParentFailure } from "./parent-sync-sql.js";

/** The runtime and node:sqlite tests run these same synchronous operations. */
export type SyncSql = Pick<SqlStorage, "exec">;
export interface SyncStorage {
  sql: SyncSql;
  transactionSync<T>(callback: () => T): T;
}

export interface UpsertResult {
  /** Accepted input rows, retained for existing callers. */
  upserted: number;
  row_count_after: number;
  inserted: number;
  updated: number;
  unchanged: number;
  /** Data statements' actual cursor.rowsWritten, including index writes. */
  rows_written: number;
  /** Explicit alias for callers that aggregate data and metadata separately. */
  data_rows_written: number;
  /** Bookkeeping statements' actual cursor.rowsWritten, kept separate. */
  metadata_rows_written: number;
}

export interface AppointmentSnapshotResult extends UpsertResult {
  missing_retained: number;
}

export const SYNC_LEASE_TTL_MS = 120_000;
export const SYNC_LEASE_DDL = `
  CREATE TABLE IF NOT EXISTS _sync_lease (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    location_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    token TEXT,
    entity TEXT,
    expires_at INTEGER NOT NULL DEFAULT 0
  )
`;

type Lease = {
  location_id: string;
  object_id: string;
  token: string | null;
  entity: string | null;
  expires_at: number;
};

export class SyncLeaseError extends Error {
  constructor() {
    super("Sync lease is missing, expired, or no longer owned");
    this.name = "SyncLeaseError";
  }
}

function readLease(sql: SyncSql): Lease | undefined {
  return sql.exec<Lease>("SELECT location_id, object_id, token, entity, expires_at FROM _sync_lease WHERE singleton = 1").toArray()[0];
}

/** An existing row is the permanent opt-in, even after release or expiry. */
export function assertSyncFence(sql: SyncSql, token: string | undefined, nowMs = Date.now()): Lease | undefined {
  const lease = readLease(sql);
  if (!lease && token === undefined) return undefined;
  if (!lease || !token || lease.token !== token || lease.expires_at <= nowMs) throw new SyncLeaseError();
  return lease;
}

function requireEntity(table: string): EntityManifest {
  const entity = ENTITY_BY_TABLE.get(table);
  if (!entity) throw new Error("Unknown sync entity");
  return entity;
}

export function acquireSyncLease(
  storage: SyncStorage,
  objectId: string,
  locationId: string,
  entity: string,
  nowMs = Date.now(),
): string | null {
  requireEntity(entity);
  if (!locationId.trim() || !objectId) throw new Error("Invalid sync tenant identity");
  return storage.transactionSync(() => {
    const { sql } = storage;
    const current = readLease(sql);
    if (current) {
      if (current.location_id !== locationId || current.object_id !== objectId) throw new Error("Sync tenant identity mismatch");
      if (current.token && current.expires_at > nowMs) return null;
    } else {
      // idFromName belongs to the caller's namespace, not DurableObjectState.
      // Physical routing supplies objectId; existing rows prevent claiming an
      // established store for another tenant. Empty stores bind on first claim.
      for (const manifest of ALL_ENTITIES) {
        if (!manifest.columns.some((column) => column.name === "location_id")) continue;
        if (sql.exec(`SELECT 1 AS mismatch FROM ${quoteIdent(manifest.table)} WHERE location_id IS NOT NULL AND location_id != ? LIMIT 1`, locationId).toArray().length) {
          throw new Error("Sync tenant identity mismatch");
        }
      }
    }
    const token = crypto.randomUUID();
    sql.exec(`INSERT INTO _sync_lease(singleton, location_id, object_id, token, entity, expires_at)
      VALUES (1, ?, ?, ?, ?, ?)
      ON CONFLICT(singleton) DO UPDATE SET token = excluded.token, entity = excluded.entity, expires_at = excluded.expires_at`,
      locationId, objectId, token, entity, nowMs + SYNC_LEASE_TTL_MS);
    return token;
  });
}

export function renewSyncLease(storage: SyncStorage, token: string, nowMs = Date.now()): void {
  storage.transactionSync(() => {
    if (!assertSyncFence(storage.sql, token, nowMs)) throw new SyncLeaseError();
    storage.sql.exec("UPDATE _sync_lease SET expires_at = ? WHERE singleton = 1", nowMs + SYNC_LEASE_TTL_MS);
  });
}

export function finishSyncLease(storage: SyncStorage, token: string, entity: string, success: boolean, nowMs = Date.now()): void {
  storage.transactionSync(() => {
    const lease = assertSyncFence(storage.sql, token, nowMs);
    if (!lease || lease.entity !== entity) throw new SyncLeaseError();
    if (success) {
      storage.sql.exec(`INSERT INTO _sync_state(entity, last_success_at) VALUES (?, ?)
        ON CONFLICT(entity) DO UPDATE SET last_success_at = excluded.last_success_at`, entity, new Date(nowMs).toISOString());
    }
    storage.sql.exec("UPDATE _sync_lease SET token = NULL, entity = NULL, expires_at = 0 WHERE singleton = 1");
  });
}

/** Manifest association validation also prevents callers selecting arbitrary tables. */
export function assertParentAssociation(entity: string, parentTable: string): EntityManifest {
  const manifest = requireEntity(entity);
  if (manifest.backfill.per_parent?.parent_entity !== parentTable || !ENTITY_BY_TABLE.has(parentTable)) {
    throw new Error("Invalid sync parent association");
  }
  return manifest;
}

function cachedRowCount(sql: SyncSql, table: string): { count: number; missing: boolean } {
  const row = sql.exec<{ row_count: number }>("SELECT row_count FROM _sync_state WHERE entity = ?", table).toArray()[0];
  return {
    count: row?.row_count ?? sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${quoteIdent(table)}`).one().n,
    missing: row === undefined,
  };
}

function write(sql: SyncSql, statement: string, ...values: SqlStorageValue[]): number {
  const cursor = sql.exec(statement, ...values);
  cursor.toArray();
  return cursor.rowsWritten;
}

/** Caller owns the transaction and has asserted the fence immediately before calling. */
function upsertContent(sql: SyncSql, entity: EntityManifest, rows: ReadonlyArray<Record<string, unknown>>, now: string): UpsertResult {
  const table = quoteIdent(entity.table);
  const pk = quoteIdent(entity.primary_key);
  const business = entity.columns.filter((column) => column.name !== "_synced_at");
  const jsonColumns = business.filter((column) => column.json);
  const mutable = entity.columns.filter((column) => column.name !== entity.primary_key);
  // Normalize old JSON in memory too: pre-upgrade rows may have arbitrary key
  // order, and comparing them must not cause a one-off indexed rewrite. Every
  // business value still uses SQLite's null-safe IS comparison. Scalar columns
  // retain their column affinity; JSON TEXT is compared in canonical form.
  const select = `SELECT (${business.map((column) => `${column.json ? "?" : quoteIdent(column.name)} IS ?`).join(" AND ")}) AS unchanged FROM ${table} WHERE ${pk} = ?`;
  const insert = `INSERT INTO ${table} (${entity.columns.map((column) => quoteIdent(column.name)).join(", ")}) VALUES (${entity.columns.map(() => "?").join(", ")})`;
  const update = `UPDATE ${table} SET ${mutable.map((column) => `${quoteIdent(column.name)} = ?`).join(", ")} WHERE ${pk} = ?`;
  const cached = cachedRowCount(sql, entity.table);
  const result: UpsertResult = { upserted: rows.length, row_count_after: cached.count, inserted: 0, updated: 0, unchanged: 0, rows_written: 0, data_rows_written: 0, metadata_rows_written: 0 };
  for (const row of rows) {
    const key = coerceForSqlite(entity.columns.find((column) => column.name === entity.primary_key)!, row[entity.primary_key]);
    const previousJson = jsonColumns.length
      ? sql.exec(`SELECT ${jsonColumns.map((column) => quoteIdent(column.name)).join(", ")} FROM ${table} WHERE ${pk} = ?`, key).toArray()[0]
      : undefined;
    const compareValues = business.flatMap((column) => column.json
      ? [coerceForSqlite(column, previousJson?.[column.name]), coerceForSqlite(column, row[column.name])]
      : [coerceForSqlite(column, row[column.name])]);
    const existing = sql.exec<{ unchanged: number }>(select, ...compareValues, key).toArray()[0];
    const value = (column: ColumnDef): SqlStorageValue => column.name === "_synced_at" ? now : coerceForSqlite(column, row[column.name]);
    if (!existing) {
      result.rows_written += write(sql, insert, ...entity.columns.map(value));
      result.inserted++;
    } else if (!existing.unchanged) {
      result.rows_written += write(sql, update, ...mutable.map(value), key);
      result.updated++;
    } else {
      result.unchanged++;
      // Other entities still participate in drain pruning. Refresh only the
      // unindexed observation column; appointment omissions are never pruned.
      if (entity.table !== "appointments") {
        result.rows_written += write(sql, `UPDATE ${table} SET _synced_at = ? WHERE ${pk} = ? AND _synced_at IS NOT ?`, now, key, now);
      }
    }
  }
  result.row_count_after += result.inserted;
  result.data_rows_written = result.rows_written;
  if (rows.length > 0 || cached.missing) {
    result.metadata_rows_written += write(sql, `INSERT INTO _sync_state(entity, last_sync_at, row_count) VALUES (?, ?, ?)
      ON CONFLICT(entity) DO UPDATE SET last_sync_at = excluded.last_sync_at, row_count = excluded.row_count
      WHERE _sync_state.last_sync_at IS NOT excluded.last_sync_at OR _sync_state.row_count IS NOT excluded.row_count`, entity.table, now, result.row_count_after);
  }
  return result;
}

export function upsertRows(storage: SyncStorage, table: string, rows: ReadonlyArray<Record<string, unknown>>, token?: string, nowMs = Date.now()): UpsertResult {
  const entity = requireEntity(table);
  return storage.transactionSync(() => {
    const lease = assertSyncFence(storage.sql, token, nowMs);
    if (lease && entity.columns.some((column) => column.name === "location_id")) {
      for (const row of rows) {
        if (row.location_id !== lease.location_id) throw new Error("Sync row tenant mismatch");
      }
    }
    return upsertContent(storage.sql, entity, rows, new Date(nowMs).toISOString());
  });
}

export function reconcileAppointmentSnapshot(
  storage: SyncStorage,
  locationId: string,
  parentId: string,
  rows: ReadonlyArray<Record<string, unknown>>,
  token: string,
  nowMs = Date.now(),
): AppointmentSnapshotResult {
  return storage.transactionSync(() => {
    const { sql } = storage;
    const lease = assertSyncFence(sql, token, nowMs);
    if (lease?.location_id !== locationId || lease.entity !== "appointments") throw new Error("Snapshot tenant or entity mismatch");
    const parent = sql.exec<{ location_id: string }>("SELECT location_id FROM calendars WHERE id = ?", parentId).toArray()[0];
    if (!parentId.trim() || parent?.location_id !== locationId) throw new Error("Snapshot calendar tenant mismatch");
    const ids = new Set<string>();
    for (const row of rows) {
      if (typeof row.id !== "string" || !row.id.trim() || ids.has(row.id)) throw new Error("Snapshot IDs must be nonempty and unique");
      if (row.location_id !== locationId || row.calendar_id !== parentId) throw new Error("Snapshot row tenant or calendar mismatch");
      ids.add(row.id);
      // A move between calendars within this tenant is a legitimate update.
      const old = sql.exec<{ location_id: string; calendar_id: string }>("SELECT location_id, calendar_id FROM appointments WHERE id = ?", row.id).toArray()[0];
      if (old && old.location_id !== locationId) throw new Error("Stored appointment tenant mismatch");
    }
    // Count omitted IDs within this calendar, including historical rows. This
    // uses the calendar index and never scans the entire appointment table.
    const previousIds = sql.exec<{ id: string }>("SELECT id FROM appointments WHERE calendar_id = ?", parentId).toArray();
    assertSyncFence(sql, token, nowMs);
    const now = new Date(nowMs).toISOString();
    const result = upsertContent(sql, requireEntity("appointments"), rows, now);
    result.metadata_rows_written += write(sql, PARENT_MARK_COMPLETE, "appointments", parentId, now);
    return { ...result, missing_retained: previousIds.filter((row) => !ids.has(row.id)).length };
  });
}

/** Shared by the RPC and SQLite tests. Appointment history is never pruned. */
export function pruneStaleRows(storage: SyncStorage, table: string, drainStartedAt: string, token?: string, nowMs = Date.now()): number {
  requireEntity(table);
  return storage.transactionSync(() => {
    const { sql } = storage;
    assertSyncFence(sql, token, nowMs);
    if (table === "appointments") return 0;
    write(sql, `DELETE FROM ${quoteIdent(table)} WHERE _synced_at < ?`, drainStartedAt);
    const removed = sql.exec<{ n: number }>("SELECT changes() AS n").one().n;
    if (removed > 0) sql.exec("UPDATE _sync_state SET row_count = MAX(0, row_count - ?) WHERE entity = ?", removed, table);
    return removed;
  });
}

export function markParentBackfillComplete(
  storage: SyncStorage,
  entity: string,
  parentId: string,
  childTable: string,
  fkColumn: string,
  derivedTables: ReadonlyArray<{ table: string; fk: string }> = [],
  token?: string,
  nowMs = Date.now(),
): number {
  const manifest = requireEntity(entity);
  const association = manifest.backfill.per_parent;
  if (entity !== childTable || !association || association.parent_fk_column !== fkColumn) throw new Error("Invalid child sync association");
  for (const { table, fk } of derivedTables) {
    const derived = requireEntity(table);
    if (!derived.columns.some((column) => column.name === fk && column.references === `${association.parent_entity}.id`)) throw new Error("Invalid derived sync association");
  }
  return storage.transactionSync(() => {
    const { sql } = storage;
    assertSyncFence(sql, token, nowMs);
    const drainStart = sql.exec<{ drain_started_at: string | null }>(PARENT_GET_DRAIN_START, entity, parentId).toArray()[0]?.drain_started_at;
    let removed = 0;
    if (drainStart && entity !== "appointments") {
      for (const { table, fk } of [{ table: childTable, fk: fkColumn }, ...derivedTables]) {
        if (table === "appointments") continue;
        write(sql, parentPruneChildrenSql(table, fk), parentId, drainStart);
        const changes = sql.exec<{ n: number }>("SELECT changes() AS n").one().n;
        removed += changes;
        if (changes > 0) sql.exec("UPDATE _sync_state SET row_count = MAX(0, row_count - ?) WHERE entity = ?", changes, table);
      }
    }
    sql.exec(PARENT_MARK_COMPLETE, entity, parentId, new Date(nowMs).toISOString());
    return removed;
  });
}

export function recordParentFailure(
  storage: SyncStorage,
  entity: string,
  parentId: string,
  retryAfterMs: number | undefined,
  token: string,
  nowMs = Date.now(),
): void {
  const association = requireEntity(entity).backfill.per_parent;
  if (!association) throw new Error("Invalid child sync association");
  storage.transactionSync(() => {
    const { sql } = storage;
    const lease = assertSyncFence(sql, token, nowMs);
    if (!lease || lease.entity !== entity) throw new SyncLeaseError();
    const parent = sql.exec<{ location_id: string }>(`SELECT location_id FROM ${quoteIdent(association.parent_entity)} WHERE id = ?`, parentId).toArray()[0];
    if (parent?.location_id !== lease.location_id) throw new Error("Parent sync tenant mismatch");
    persistParentFailure(sql, entity, parentId, retryAfterMs, nowMs);
  });
}

export function quoteIdent(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error("Invalid SQL identifier");
  return `"${name}"`;
}

function coerceForSqlite(col: ColumnDef, value: unknown): SqlStorageValue {
  if (value === null || value === undefined) return null;
  if (col.json) return canonicalJson(value);
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") return col.sqlite_type === "TEXT" ? String(value) : value;
  if (typeof value === "string" || value instanceof ArrayBuffer) return value;
  return String(value);
}

function canonicalJson(value: unknown): string {
  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch {
      // Preserve previously accepted non-JSON strings as opaque text.
      return value as string;
    }
  }
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item !== null && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(Object.entries(item).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0));
    }
    return item;
  });
}
