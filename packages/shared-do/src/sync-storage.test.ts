import { afterEach, beforeEach, describe, it } from "node:test";
import { deepStrictEqual, equal, ok, throws } from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { ALL_ENTITIES, ENTITY_BY_TABLE, renderCreateTable, renderIndexes } from "@topline/shared-schema";
import {
  SYNC_LEASE_DDL,
  SYNC_LEASE_TTL_MS,
  SyncLeaseError,
  acquireSyncLease,
  renewSyncLease,
  finishSyncLease,
  assertSyncFence,
  assertParentAssociation,
  upsertRows,
  reconcileAppointmentSnapshot,
  pruneStaleRows,
  markParentBackfillComplete,
  recordParentFailure,
  type SyncStorage,
} from "./sync-storage.js";
import { PARENT_SYNC_STATE_DDL, PARENT_ENSURE_DRAIN_MARKER, pendingParentsCountSql, sweepParentsForChildSql } from "./parent-sync-sql.js";

const START = Date.parse("2026-10-21T10:00:00.000Z");
const iso = (time: number) => new Date(time).toISOString();

/** Only the driver is adapted; all mutations below are production helpers. */
function sqliteStorage(db: DatabaseSync) {
  const statements: string[] = [];
  let reportedWriteMultiplier = 1;
  const sql = {
    exec(query: string, ...bindings: unknown[]) {
      statements.push(query);
      const stmt = db.prepare(query);
      const columnNames = stmt.columns().map((column) => column.name);
      const values = bindings.map((value) => value instanceof ArrayBuffer ? new Uint8Array(value) : value);
      let rows: Record<string, unknown>[] = [];
      let rowsWritten = 0;
      if (columnNames.length) rows = stmt.all(...values as never[]);
      else rowsWritten = Number(stmt.run(...values as never[]).changes) * reportedWriteMultiplier;
      return {
        columnNames,
        rowsWritten,
        toArray: () => rows,
        one: () => {
          equal(rows.length, 1);
          return rows[0];
        },
        [Symbol.iterator]: () => rows[Symbol.iterator](),
      };
    },
  };
  const storage = {
    sql,
    transactionSync<T>(callback: () => T): T {
      db.exec("BEGIN");
      try {
        const result = callback();
        db.exec("COMMIT");
        return result;
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  } as unknown as SyncStorage;
  return {
    storage,
    statements,
    // node:sqlite reports logical changes, unlike Cloudflare's cursor. Inject
    // an explicit cursor value to prove production metrics propagate it,
    // rather than silently substituting changes() or logical row counts.
    reportWritesMultipliedBy(multiplier: number) { reportedWriteMultiplier = multiplier; },
  };
}

let db: DatabaseSync;
let harness: ReturnType<typeof sqliteStorage>;
let storage: SyncStorage;

function appointment(id: string, extra: Record<string, unknown> = {}) {
  return { id, location_id: "tenant-a", calendar_id: "calendar-a", title: "Appointment", status: "confirmed", start_time: "2026-11-01T10:00:00.000Z", raw_payload: { id, metadata: { a: 1, b: 2 } }, ...extra };
}

function calendar(id = "calendar-a", locationId = "tenant-a") {
  return { id, location_id: locationId, name: "Calendar", raw_payload: { id } };
}

function seedCalendar(id = "calendar-a", locationId = "tenant-a") {
  upsertRows(storage, "calendars", [calendar(id, locationId)], undefined, START - 1_000);
}

function lease(entity = "appointments", time = START) {
  const token = acquireSyncLease(storage, "physical-object-a", "tenant-a", entity, time);
  ok(token);
  return token;
}

function state(entity = "appointments", parentId = "calendar-a") {
  return db.prepare("SELECT * FROM _parent_sync_state WHERE entity = ? AND parent_id = ?").get(entity, parentId);
}

beforeEach(() => {
  db = new DatabaseSync(":memory:");
  for (const entity of ALL_ENTITIES) {
    db.exec(renderCreateTable(entity));
    for (const index of renderIndexes(entity)) db.exec(index);
  }
  db.exec(`CREATE TABLE _sync_state (
    entity TEXT PRIMARY KEY, cursor TEXT, watermark TEXT,
    backfill_complete INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT, last_success_at TEXT, row_count INTEGER NOT NULL DEFAULT 0,
    drain_started_at TEXT, upstream_total INTEGER, row_count_verified INTEGER NOT NULL DEFAULT 0
  )`);
  db.exec(SYNC_LEASE_DDL);
  db.exec(PARENT_SYNC_STATE_DDL);
  harness = sqliteStorage(db);
  storage = harness.storage;
});

afterEach(() => db.close());

describe("production appointment reconciliation", () => {
  it("writes no appointment data for an unchanged snapshot and advances parent completion independently", () => {
    seedCalendar();
    const token = lease();
    const first = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START);
    equal(first.inserted, 1);
    const stored = db.prepare("SELECT * FROM appointments WHERE id = 'a1'").get();
    harness.statements.length = 0;
    const next = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START + 1_000);
    equal(next.rows_written, 0);
    equal(next.data_rows_written, 0);
    equal(next.unchanged, 1);
    equal(next.updated, 0);
    equal(next.inserted, 0);
    equal(next.row_count_after, 1);
    ok(next.metadata_rows_written > 0);
    deepStrictEqual(db.prepare("SELECT * FROM appointments WHERE id = 'a1'").get(), stored);
    equal(state()?.last_sync_at, iso(START + 1_000));
    equal(state()?.backfill_complete, 1);
    ok(!harness.statements.some((statement) => /^(INSERT INTO|UPDATE|DELETE FROM) "?appointments\b/.test(statement)));
    ok(!harness.statements.some((statement) => /SELECT COUNT\(\*\).*FROM "?appointments\b/.test(statement)));
  });

  it("updates only changed rows, compares nullable columns, and reports cursor writes separately from logical counts", () => {
    seedCalendar();
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1"), appointment("a2")], token, START);
    harness.reportWritesMultipliedBy(7);
    const changed = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1", { notes: "Changed" }), appointment("a2"), appointment("a3")], token, START + 100);
    equal(changed.upserted, 3);
    equal(changed.inserted, 1);
    equal(changed.updated, 1);
    equal(changed.unchanged, 1);
    equal(changed.rows_written, 14);
    equal(changed.data_rows_written, 14);
    equal(changed.row_count_after, 3);
    equal(db.prepare("SELECT _synced_at FROM appointments WHERE id = 'a2'").get()?._synced_at, iso(START));
    const clear = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START + 200);
    equal(clear.updated, 1);
    equal(db.prepare("SELECT notes FROM appointments WHERE id = 'a1'").get()?.notes, null);
  });

  it("compares every writable manifest business column, including raw payload", () => {
    seedCalendar();
    const token = lease();
    const original = appointment("a1");
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [original], token, START);
    const manifest = ENTITY_BY_TABLE.get("appointments")!;
    for (const column of manifest.columns) {
      if (["id", "location_id", "calendar_id", "_synced_at"].includes(column.name)) continue;
      const changed = { ...original, [column.name]: column.json ? { changed: column.name } : `changed-${column.name}` };
      const result = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [changed], token, START + 1);
      equal(result.updated, 1, column.name);
      equal(reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [changed], token, START + 2).rows_written, 0, column.name);
      reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [original], token, START + 3);
    }
  });

  it("canonicalizes nested JSON objects and JSON strings without reordering arrays, including pre-upgrade stored JSON", () => {
    seedCalendar();
    const token = lease();
    const raw = { b: { z: 2, a: 1 }, a: [{ y: 2, x: 1 }, 2] };
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1", { raw_payload: raw })], token, START);
    // Seed the old format independently: prior code did not sort keys.
    db.prepare("UPDATE appointments SET raw_payload = ? WHERE id = 'a1'").run(JSON.stringify(raw));
    const reordered = JSON.stringify({ a: [{ x: 1, y: 2 }, 2], b: { a: 1, z: 2 } });
    equal(reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1", { raw_payload: reordered })], token, START + 1).rows_written, 0);
    const reorderedArray = { a: [2, { x: 1, y: 2 }], b: { a: 1, z: 2 } };
    equal(reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1", { raw_payload: reorderedArray })], token, START + 2).updated, 1);
  });

  it("retains omitted rows and historical data on an empty or partial snapshot", () => {
    seedCalendar();
    const token = lease();
    const old = appointment("history", { start_time: "2019-01-01T00:00:00.000Z" });
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [old, appointment("current")], token, START);
    equal(reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("current")], token, START + 1).missing_retained, 1);
    const empty = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [], token, START + 2);
    equal(empty.missing_retained, 2);
    equal(empty.row_count_after, 2);
    equal(empty.rows_written, 0);
    equal(db.prepare("SELECT COUNT(*) AS n FROM appointments WHERE calendar_id = 'calendar-a'").get()?.n, 2);
    equal(db.prepare("SELECT _synced_at FROM appointments WHERE id = 'history'").get()?._synced_at, iso(START));
  });

  it("allows a same-tenant calendar move and counts omitted IDs by original parent membership", () => {
    seedCalendar();
    seedCalendar("calendar-b");
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("moving"), appointment("staying")], token, START);
    const move = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-b", [appointment("moving", { calendar_id: "calendar-b" })], token, START + 1);
    equal(move.updated, 1);
    equal(move.inserted, 0);
    equal(move.missing_retained, 0);
    ok(move.missing_retained >= 0);
    equal(move.row_count_after, 2);
    equal(db.prepare("SELECT calendar_id FROM appointments WHERE id = 'moving'").get()?.calendar_id, "calendar-b");
    equal(reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [], token, START + 2).missing_retained, 1);
  });

  for (const [name, locationId, parentId, rows] of [
    ["different tenant", "tenant-b", "calendar-a", [appointment("a1")]],
    ["missing parent", "tenant-a", "missing", [appointment("a1", { calendar_id: "missing" })]],
    ["wrong row tenant", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("a2", { location_id: "tenant-b" })]],
    ["wrong row calendar", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("a2", { calendar_id: "calendar-b" })]],
    ["duplicate ID", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("a1")]],
    ["empty ID", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("")]],
    ["blank ID", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment(" ")]],
    ["non-string ID", "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("a2", { id: 2 })]],
  ] as const) {
    it(`rejects ${name} atomically`, () => {
      seedCalendar();
      const token = lease();
      reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START);
      const before = db.prepare("SELECT * FROM appointments").all();
      const parentBefore = state();
      throws(() => reconcileAppointmentSnapshot(storage, locationId, parentId, rows, token, START + 1));
      deepStrictEqual(db.prepare("SELECT * FROM appointments").all(), before);
      deepStrictEqual(state(), parentBefore);
    });
  }

  it("rejects a parent or stored appointment from another tenant", () => {
    seedCalendar();
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START);
    db.prepare("UPDATE appointments SET location_id = 'tenant-b' WHERE id = 'a1'").run();
    throws(() => reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START + 1), /Stored appointment tenant mismatch/);
    db.prepare("UPDATE calendars SET location_id = 'tenant-b' WHERE id = 'calendar-a'").run();
    throws(() => reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [], token, START + 2), /calendar tenant mismatch/);
  });

  it("rolls back data, counts, and completion if a later row fails in SQLite", () => {
    seedCalendar();
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START);
    db.exec(`CREATE TRIGGER fail_snapshot BEFORE INSERT ON appointments WHEN NEW.id = 'bad' BEGIN SELECT RAISE(ABORT, 'test failure'); END`);
    const before = db.prepare("SELECT * FROM appointments").all();
    const parentBefore = state();
    const countBefore = db.prepare("SELECT * FROM _sync_state WHERE entity = 'appointments'").get();
    throws(() => reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1", { title: "Changed" }), appointment("new"), appointment("bad")], token, START + 1), /test failure/);
    deepStrictEqual(db.prepare("SELECT * FROM appointments").all(), before);
    deepStrictEqual(state(), parentBefore);
    deepStrictEqual(db.prepare("SELECT * FROM _sync_state WHERE entity = 'appointments'").get(), countBefore);
  });
});

describe("production upserts and pruning", () => {
  it("refreshes only the unindexed observation column and unchanged other rows survive pruning", () => {
    const contact = { id: "contact-a", location_id: "tenant-a", first_name: "Alex", raw_payload: { b: 2, a: 1 } };
    upsertRows(storage, "contacts", [contact], undefined, START);
    upsertRows(storage, "contacts", [{ ...contact, id: "stale" }], undefined, START);
    db.exec(`CREATE TABLE indexed_update_audit (id TEXT);
      CREATE TRIGGER indexed_update_probe AFTER UPDATE OF location_id, updated_at ON contacts BEGIN INSERT INTO indexed_update_audit VALUES (NEW.id); END;`);
    harness.statements.length = 0;
    harness.reportWritesMultipliedBy(7);
    const result = upsertRows(storage, "contacts", [{ ...contact, raw_payload: '{"a":1,"b":2}' }], undefined, START + 10);
    equal(result.unchanged, 1);
    equal(result.updated, 0);
    equal(result.rows_written, 7);
    equal(result.row_count_after, 2);
    equal(db.prepare("SELECT COUNT(*) AS n FROM indexed_update_audit").get()?.n, 0);
    ok(!harness.statements.some((statement) => /COUNT\(\*\)/.test(statement)));
    equal(upsertRows(storage, "contacts", [], undefined, START + 11).row_count_after, 2);
    ok(!harness.statements.some((statement) => /COUNT\(\*\)/.test(statement)));
    equal(pruneStaleRows(storage, "contacts", iso(START + 5), undefined, START + 12), 1, "pruning returns logical removed rows, not index writes");
    deepStrictEqual(db.prepare("SELECT id FROM contacts").all().map((row) => row.id), ["contact-a"]);
    equal(db.prepare("SELECT row_count FROM _sync_state WHERE entity = 'contacts'").get()?.row_count, 1);
  });

  it("initializes the count once from an existing table, then increments only inserted rows", () => {
    seedCalendar();
    db.prepare("DELETE FROM _sync_state WHERE entity = 'calendars'").run();
    harness.statements.length = 0;
    equal(upsertRows(storage, "calendars", [calendar(), calendar("calendar-b")], undefined, START).row_count_after, 2);
    equal(harness.statements.filter((statement) => /COUNT\(\*\)/.test(statement)).length, 1);
    harness.statements.length = 0;
    equal(upsertRows(storage, "calendars", [{ ...calendar(), name: "Updated" }], undefined, START + 1).row_count_after, 2);
    ok(!harness.statements.some((statement) => /COUNT\(\*\)/.test(statement)));
  });

  it("caches the initial empty-table count so repeated empty batches do not recount", () => {
    const first = upsertRows(storage, "contacts", [], undefined, START);
    equal(first.row_count_after, 0);
    equal(first.rows_written, 0);
    equal(first.metadata_rows_written, 1);
    harness.statements.length = 0;
    const second = upsertRows(storage, "contacts", [], undefined, START + 1);
    equal(second.row_count_after, 0);
    equal(second.metadata_rows_written, 0);
    ok(!harness.statements.some((statement) => /COUNT\(\*\)/.test(statement)));
  });

  it("protects appointment omissions through both legacy prune entry points", () => {
    seedCalendar();
    upsertRows(storage, "appointments", [appointment("history")], undefined, START);
    storage.sql.exec(PARENT_ENSURE_DRAIN_MARKER, "appointments", "calendar-a", iso(START + 5));
    equal(pruneStaleRows(storage, "appointments", iso(START + 5), undefined, START + 10), 0);
    equal(markParentBackfillComplete(storage, "appointments", "calendar-a", "appointments", "calendar_id", [], undefined, START + 10), 0);
    equal(db.prepare("SELECT COUNT(*) AS n FROM appointments").get()?.n, 1);
    equal(upsertRows(storage, "appointments", [appointment("history")], undefined, START + 20).rows_written, 0);
    equal(db.prepare("SELECT _synced_at FROM appointments").get()?._synced_at, iso(START));
  });

  it("prunes normal parent rows and caches logical counts independently of cursor write costs", () => {
    const rows = ["seen", "missing"].map((id) => ({ id, location_id: "tenant-a", contact_id: "contact-a", title: id }));
    upsertRows(storage, "tasks", rows, undefined, START);
    storage.sql.exec(PARENT_ENSURE_DRAIN_MARKER, "tasks", "contact-a", iso(START + 5));
    upsertRows(storage, "tasks", rows.slice(0, 1), undefined, START + 10);
    harness.reportWritesMultipliedBy(7);
    equal(markParentBackfillComplete(storage, "tasks", "contact-a", "tasks", "contact_id", [], undefined, START + 20), 1);
    equal(db.prepare("SELECT row_count FROM _sync_state WHERE entity = 'tasks'").get()?.row_count, 1);
    equal(db.prepare("SELECT id FROM tasks").get()?.id, "seen");
  });
});

describe("production tenant leases and fences", () => {
  it("rejects lease operations before acquisition, including omitted runtime tokens", () => {
    throws(() => renewSyncLease(storage, "missing", START), SyncLeaseError);
    throws(() => finishSyncLease(storage, "missing", "appointments", false, START), SyncLeaseError);
    throws(() => renewSyncLease(storage, undefined as unknown as string, START), SyncLeaseError);
    throws(() => finishSyncLease(storage, undefined as unknown as string, "appointments", false, START), SyncLeaseError);
  });

  it("keeps the global tenant lock exclusive across entities and binds established tenant rows", () => {
    seedCalendar();
    throws(() => acquireSyncLease(storage, "physical-object-a", "tenant-b", "appointments", START), /identity mismatch/);
    equal(db.prepare("SELECT COUNT(*) AS n FROM _sync_lease").get()?.n, 0);
    const token = lease();
    equal(acquireSyncLease(storage, "physical-object-a", "tenant-a", "contacts", START + 1), null);
    throws(() => acquireSyncLease(storage, "other-object", "tenant-a", "appointments", START + 1), /identity mismatch/);
    throws(() => acquireSyncLease(storage, "physical-object-a", "tenant-b", "appointments", START + 1), /identity mismatch/);
    equal(assertSyncFence(storage.sql, token, START)?.location_id, "tenant-a");
  });

  it("binds an empty store permanently and rejects wrong-tenant rows on managed upserts", () => {
    const token = lease("contacts");
    throws(() => upsertRows(storage, "contacts", [{ id: "c", location_id: "tenant-b" }], token, START), /tenant mismatch/);
    throws(() => upsertRows(storage, "contacts", [{ id: "c" }], token, START), /tenant mismatch/);
    finishSyncLease(storage, token, "contacts", false, START + 1);
    throws(() => acquireSyncLease(storage, "physical-object-a", "tenant-b", "contacts", START + 2), /identity mismatch/);
  });

  it("permits legacy callers only before the first lease and never after release", () => {
    seedCalendar();
    const token = lease();
    throws(() => upsertRows(storage, "appointments", [appointment("a1")], undefined, START + 1), SyncLeaseError);
    throws(() => pruneStaleRows(storage, "appointments", iso(START + 1), undefined, START + 1), SyncLeaseError);
    finishSyncLease(storage, token, "appointments", false, START + 2);
    throws(() => upsertRows(storage, "appointments", [appointment("a1")], undefined, START + 3), SyncLeaseError);
    throws(() => assertSyncFence(storage.sql, token, START + 3), SyncLeaseError);
    throws(() => finishSyncLease(storage, token, "appointments", true, START + 3), SyncLeaseError);
    throws(() => renewSyncLease(storage, token, START + 3), SyncLeaseError);
  });

  it("expires exactly at its deadline, renews only owned live leases, and rejects stale mutations after takeover", () => {
    seedCalendar();
    const token = lease();
    renewSyncLease(storage, token, START + SYNC_LEASE_TTL_MS - 1);
    const renewedExpiry = START + 2 * SYNC_LEASE_TTL_MS - 1;
    equal(acquireSyncLease(storage, "physical-object-a", "tenant-a", "contacts", renewedExpiry - 1), null);
    throws(() => renewSyncLease(storage, token, renewedExpiry), SyncLeaseError);
    throws(() => upsertRows(storage, "appointments", [appointment("a1")], token, renewedExpiry), SyncLeaseError);
    const replacement = lease("appointments", renewedExpiry);
    ok(replacement !== token);
    throws(() => reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, renewedExpiry + 1), SyncLeaseError);
    throws(() => markParentBackfillComplete(storage, "appointments", "calendar-a", "appointments", "calendar_id", [], token, renewedExpiry + 1), SyncLeaseError);
    throws(() => recordParentFailure(storage, "appointments", "calendar-a", undefined, token, renewedExpiry + 1), SyncLeaseError);
    throws(() => finishSyncLease(storage, token, "appointments", false, renewedExpiry + 1), SyncLeaseError);
    equal(assertSyncFence(storage.sql, replacement, renewedExpiry + 1)?.token, replacement);
  });

  it("advances last_success_at only for successful owned finishes and always releases a valid owned lease", () => {
    const failed = lease();
    finishSyncLease(storage, failed, "appointments", false, START + 1);
    equal(db.prepare("SELECT last_success_at FROM _sync_state WHERE entity = 'appointments'").get(), undefined);
    const succeeded = lease("appointments", START + 2);
    throws(() => finishSyncLease(storage, succeeded, "contacts", true, START + 3), SyncLeaseError);
    equal(assertSyncFence(storage.sql, succeeded, START + 3)?.token, succeeded);
    finishSyncLease(storage, succeeded, "appointments", true, START + 4);
    equal(db.prepare("SELECT last_success_at FROM _sync_state WHERE entity = 'appointments'").get()?.last_success_at, iso(START + 4));
    const another = lease("appointments", START + 5);
    finishSyncLease(storage, another, "appointments", false, START + 6);
    equal(db.prepare("SELECT last_success_at FROM _sync_state WHERE entity = 'appointments'").get()?.last_success_at, iso(START + 4));
    equal(db.prepare("SELECT token FROM _sync_lease").get()?.token, null);
  });
});

describe("production parent retry integration", () => {
  it("refreshes healthy parents on each cadence bucket while a failed parent keeps the cycle pending", () => {
    seedCalendar("calendar-a");
    seedCalendar("calendar-b");
    seedCalendar("calendar-failed");
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [], token, START + 30_000);
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-b", [], token, START + 60_000);
    recordParentFailure(storage, "appointments", "calendar-failed", 6 * 60 * 60_000, token, START + 60_000);
    finishSyncLease(storage, token, "appointments", false, START + 60_001);
    const select = sweepParentsForChildSql("calendars", true);
    for (const offset of [15 * 60_000, 30 * 60_000]) {
      const now = START + offset;
      const ids = storage.sql.exec<{ id: string }>(select, "appointments", iso(now), iso(now), 5).toArray().map((row) => row.id);
      deepStrictEqual(ids, ["calendar-a", "calendar-b"]);
      // Completing all healthy parents since the original cycle start does
      // not hide the delayed failure or declare the entity complete.
      equal(storage.sql.exec(select, "appointments", iso(now), iso(START), 5).toArray().length, 0);
      equal(storage.sql.exec<{ n: number }>(pendingParentsCountSql("calendars"), "appointments").one().n, 1);
      const next = lease("appointments", now);
      reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [], next, now + 10_000);
      reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-b", [], next, now + 20_000);
      finishSyncLease(storage, next, "appointments", false, now + 20_001);
      equal(storage.sql.exec(select, "appointments", iso(now + 20_002), iso(now), 5).toArray().length, 0);
    }
  });

  it("retains delayed failures in pending counts, selects when due, and clears retries after a successful snapshot", () => {
    seedCalendar();
    const token = lease();
    reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START);
    recordParentFailure(storage, "appointments", "calendar-a", 60_000, token, START + 1);
    equal(state()?.status, "failed");
    equal(state()?.backfill_complete, 0);
    equal(state()?.failure_count, 1);
    equal(state()?.last_sync_at, iso(START));
    equal(storage.sql.exec<{ n: number }>(pendingParentsCountSql("calendars"), "appointments").one().n, 1);
    const select = sweepParentsForChildSql("calendars", true);
    equal(storage.sql.exec(select, "appointments", iso(START + 60_000), iso(START - 1), 5).toArray().length, 0);
    equal(storage.sql.exec(select, "appointments", iso(START + 60_001), iso(START - 1), 5).toArray().length, 1);
    const recovered = reconcileAppointmentSnapshot(storage, "tenant-a", "calendar-a", [appointment("a1")], token, START + 60_001);
    equal(recovered.rows_written, 0);
    equal(state()?.failure_count, 0);
    equal(state()?.retry_due_at, null);
    equal(state()?.status, "complete");
    equal(storage.sql.exec<{ n: number }>(pendingParentsCountSql("calendars"), "appointments").one().n, 0);
    equal(storage.sql.exec(select, "appointments", iso(START + 60_002), iso(START - 1), 5).toArray().length, 0);
  });

  it("validates manifest parent associations and rejects unrelated failure state", () => {
    equal(assertParentAssociation("appointments", "calendars").table, "appointments");
    throws(() => assertParentAssociation("appointments", "contacts"), /association/);
    throws(() => assertParentAssociation("contacts", "contacts"), /association/);
    throws(() => assertParentAssociation("missing", "calendars"), /Unknown sync entity/);
    const token = lease();
    throws(() => recordParentFailure(storage, "appointments", "missing", undefined, token, START), /tenant mismatch/);
    equal(db.prepare("SELECT COUNT(*) AS n FROM _parent_sync_state").get()?.n, 0);
  });
});
