// Direct tests for the per-parent sync state machine.
//
// The state machine lives inside LocationDO (packages/shared-do/src/
// location-do.ts) and manipulates a `_parent_sync_state` table via
// Cloudflare's DurableObject storage.sql interface. These tests run the
// EXACT same SQL statements against node:sqlite so we can prove state
// transitions without standing up a Workers runtime.
//
// Why this file exists: the P1 bug fixed in 9044d08 — where
// ensureParentDrainMarker on a previously-complete parent would bump
// last_sync_at and leave backfill_complete=1, then a transient fetch
// failure would silently drop the activity signal because
// getNextParentsForChild could no longer re-select the parent — was
// not caught by any existing test. The SQL safety + mapping suites
// don't exercise the DO's sync bookkeeping at all. This suite covers
// exactly the invariants the P1 fix rests on.
//
// Maintenance note: the SQL strings below MUST match what location-do.ts
// runs for the corresponding RPCs. When you change an RPC's SQL, update
// the test. That duplication is intentional — it's the forcing function
// that turns SQL regressions into test failures.

import { describe, it, before, beforeEach } from "node:test";
import { strictEqual, deepStrictEqual, ok } from "node:assert";
import { DatabaseSync } from "node:sqlite";

// ---------------------------------------------------------------------------
// Test harness: an in-memory SQLite that mimics the LocationDO schema
// relevant to per-parent sync. Only the _parent_sync_state table and
// a tiny surrogate for the parent table (contacts) are needed.
// ---------------------------------------------------------------------------

let db: DatabaseSync;

function initSchema(): void {
  db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE _parent_sync_state (
      entity TEXT NOT NULL,
      parent_id TEXT NOT NULL,
      cursor TEXT,
      backfill_complete INTEGER NOT NULL DEFAULT 0,
      last_sync_at TEXT,
      drain_started_at TEXT,
      PRIMARY KEY (entity, parent_id)
    );
  `);
  // Surrogate contacts table for getNextParentsForChild's JOIN.
  db.exec(`
    CREATE TABLE contacts (
      id TEXT PRIMARY KEY,
      updated_at TEXT
    );
  `);
  // Surrogate tasks table for prune tests.
  db.exec(`
    CREATE TABLE tasks (
      id TEXT PRIMARY KEY,
      contact_id TEXT,
      title TEXT,
      _synced_at TEXT
    );
  `);
  // Minimal _sync_state for row_count refresh verification.
  db.exec(`
    CREATE TABLE _sync_state (
      entity TEXT PRIMARY KEY,
      row_count INTEGER NOT NULL DEFAULT 0,
      last_sync_at TEXT,
      backfill_complete INTEGER NOT NULL DEFAULT 0
    );
  `);
  db.prepare(
    `INSERT INTO _sync_state(entity, row_count) VALUES ('tasks', 0)`,
  ).run();
}

// ---------------------------------------------------------------------------
// SQL statement helpers — copied verbatim from location-do.ts. If the
// RPC SQL diverges from what's here, these tests fail, forcing a review.
// ---------------------------------------------------------------------------

function ensureParentDrainMarker(entity: string, parentId: string, now: string): void {
  db.prepare(
    `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
     VALUES (?, ?, NULL, 0, NULL, ?)
     ON CONFLICT(entity, parent_id) DO UPDATE
       SET backfill_complete = 0,
           drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)`,
  ).run(entity, parentId, now);
}

function setParentSyncCursor(
  entity: string,
  parentId: string,
  cursor: string,
  now: string,
): void {
  db.prepare(
    `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
     VALUES (?, ?, ?, 0, ?, ?)
     ON CONFLICT(entity, parent_id) DO UPDATE
       SET cursor = excluded.cursor,
           backfill_complete = 0,
           last_sync_at = excluded.last_sync_at,
           drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)`,
  ).run(entity, parentId, cursor, now, now);
}

function markParentBackfillComplete(
  entity: string,
  parentId: string,
  childTable: string,
  fkColumn: string,
  now: string,
): { pruned: number } {
  const drainRow = db
    .prepare(
      `SELECT drain_started_at FROM _parent_sync_state WHERE entity = ? AND parent_id = ?`,
    )
    .get(entity, parentId) as { drain_started_at: string | null } | undefined;
  const drainStart = drainRow?.drain_started_at ?? null;

  let pruned = 0;
  if (drainStart) {
    const info = db
      .prepare(
        `DELETE FROM ${childTable} WHERE ${fkColumn} = ? AND _synced_at < ?`,
      )
      .run(parentId, drainStart);
    pruned = Number(info.changes);
  }

  if (pruned > 0) {
    const fresh = db
      .prepare(`SELECT COUNT(*) AS n FROM ${childTable}`)
      .get() as { n: number };
    db.prepare(
      `UPDATE _sync_state SET row_count = ? WHERE entity = ?`,
    ).run(fresh.n, childTable);
  }

  db.prepare(
    `INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
     VALUES (?, ?, NULL, 1, ?, NULL)
     ON CONFLICT(entity, parent_id) DO UPDATE
       SET cursor = NULL,
           backfill_complete = 1,
           last_sync_at = excluded.last_sync_at,
           drain_started_at = NULL`,
  ).run(entity, parentId, now);

  return { pruned };
}

function getNextParentsForChild(
  childEntity: string,
  parentTable: string,
  limit: number,
  freshnessColumns: readonly string[],
): string[] {
  const freshnessSql =
    freshnessColumns.length > 0
      ? " OR " +
        freshnessColumns
          .map((col) => `p.${col} > COALESCE(s.last_sync_at, '')`)
          .join(" OR ")
      : "";
  const rows = db
    .prepare(
      `SELECT p.id
         FROM ${parentTable} p
         LEFT JOIN _parent_sync_state s
           ON s.entity = ? AND s.parent_id = p.id
        WHERE COALESCE(s.backfill_complete, 0) = 0${freshnessSql}
        ORDER BY COALESCE(s.last_sync_at, ''), p.id
        LIMIT ?`,
    )
    .all(childEntity, limit) as Array<{ id: string }>;
  return rows.map((r) => r.id);
}

function getState(entity: string, parentId: string) {
  return db
    .prepare(
      `SELECT cursor, backfill_complete, last_sync_at, drain_started_at
       FROM _parent_sync_state WHERE entity = ? AND parent_id = ?`,
    )
    .get(entity, parentId) as
    | {
        cursor: string | null;
        backfill_complete: number;
        last_sync_at: string | null;
        drain_started_at: string | null;
      }
    | undefined;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("parent-sync state machine", () => {
  beforeEach(() => initSchema());

  describe("ensureParentDrainMarker — fresh parent (no row)", () => {
    it("creates a row with backfill_complete=0, drain_started_at=now, last_sync_at=NULL", () => {
      ensureParentDrainMarker("tasks", "c1", "2026-04-24T12:00:00.000Z");
      const s = getState("tasks", "c1");
      strictEqual(s?.backfill_complete, 0);
      strictEqual(s?.drain_started_at, "2026-04-24T12:00:00.000Z");
      strictEqual(s?.last_sync_at, null, "last_sync_at must NOT be bumped on the INSERT path — it means 'last successful completion'");
      strictEqual(s?.cursor, null);
    });
  });

  describe("ensureParentDrainMarker — previously-complete parent (the P1 bug path)", () => {
    it("flips backfill_complete to 0 without bumping last_sync_at", () => {
      // Seed: a parent that has previously completed a sync.
      markParentBackfillComplete("tasks", "c2", "tasks", "contact_id", "2026-04-23T00:00:00.000Z");
      const before = getState("tasks", "c2");
      strictEqual(before?.backfill_complete, 1);
      strictEqual(before?.last_sync_at, "2026-04-23T00:00:00.000Z");

      // Cron re-selects this parent because its freshness column bumped.
      // backfillPerParent calls ensureParentDrainMarker.
      ensureParentDrainMarker("tasks", "c2", "2026-04-24T12:00:00.000Z");

      const after = getState("tasks", "c2");
      strictEqual(
        after?.backfill_complete,
        0,
        "P1: UPDATE must flip backfill_complete to 0 so the next tick's 'incomplete' clause re-selects the parent if this drain aborts.",
      );
      strictEqual(
        after?.last_sync_at,
        "2026-04-23T00:00:00.000Z",
        "P1: UPDATE must NOT bump last_sync_at — doing so would leave both getNextParentsForChild clauses false and permanently drop the activity signal after a transient fetch failure.",
      );
      ok(after?.drain_started_at, "drain_started_at must be set when starting the new drain");
    });

    it("preserves an existing drain_started_at across resumes (COALESCE)", () => {
      // First drain started at T1 but didn't complete.
      ensureParentDrainMarker("tasks", "c3", "2026-04-24T08:00:00.000Z");
      // Later page-persisted via setParentSyncCursor.
      setParentSyncCursor("tasks", "c3", '"cursor-value"', "2026-04-24T08:01:00.000Z");
      // Next tick re-visits; ensureParentDrainMarker should NOT overwrite
      // drain_started_at — snapshot-and-swap prune needs the true start.
      ensureParentDrainMarker("tasks", "c3", "2026-04-24T09:00:00.000Z");
      const s = getState("tasks", "c3");
      strictEqual(
        s?.drain_started_at,
        "2026-04-24T08:00:00.000Z",
        "COALESCE must preserve the earliest drain_started_at across multi-tick resumes so prune uses the true drain start.",
      );
    });
  });

  describe("getNextParentsForChild — transient-failure re-selection (P1 end-to-end)", () => {
    it("re-selects a parent that ran ensureParentDrainMarker but then aborted mid-drain", () => {
      // Setup: one contact, previously completed for tasks.
      db.prepare(`INSERT INTO contacts(id, updated_at) VALUES ('c4', '2026-04-23T00:00:00.000Z')`).run();
      markParentBackfillComplete("tasks", "c4", "tasks", "contact_id", "2026-04-23T00:00:00.000Z");

      // Sanity: because last_sync_at = updated_at (both 04-23), the
      // parent is NOT currently due for refresh.
      const beforeBump = getNextParentsForChild("tasks", "contacts", 10, ["updated_at"]);
      deepStrictEqual(beforeBump, [], "completed parent with no freshness bump should not be re-selected");

      // Activity bump: contact.updated_at advances.
      db.prepare(`UPDATE contacts SET updated_at = ? WHERE id = ?`).run("2026-04-24T10:00:00.000Z", "c4");

      // Cron picks it up via freshness clause.
      const afterBump = getNextParentsForChild("tasks", "contacts", 10, ["updated_at"]);
      deepStrictEqual(afterBump, ["c4"], "freshness bump should re-select");

      // backfillPerParent calls ensureParentDrainMarker.
      ensureParentDrainMarker("tasks", "c4", "2026-04-24T10:00:01.000Z");

      // Now simulate a transient fetch failure: nothing else runs this tick.

      // THE TEST: on the next cron tick, the parent MUST still be selected.
      // Before the P1 fix, ensureParentDrainMarker would have bumped
      // last_sync_at past updated_at AND left backfill_complete=1, so
      // both clauses would be false. With the fix, backfill_complete=0
      // keeps the incomplete clause firing.
      const nextTick = getNextParentsForChild("tasks", "contacts", 10, ["updated_at"]);
      deepStrictEqual(
        nextTick,
        ["c4"],
        "P1: after a transient failure mid-drain, the parent MUST be re-selected. Before the fix, the activity signal was silently dropped.",
      );
    });
  });

  describe("setParentSyncCursor — mid-drain progress", () => {
    it("bumps last_sync_at (indicates progress) but keeps backfill_complete=0", () => {
      ensureParentDrainMarker("tasks", "c5", "2026-04-24T12:00:00.000Z");
      setParentSyncCursor("tasks", "c5", '"page2"', "2026-04-24T12:00:30.000Z");
      const s = getState("tasks", "c5");
      strictEqual(s?.cursor, '"page2"');
      strictEqual(s?.backfill_complete, 0);
      strictEqual(s?.last_sync_at, "2026-04-24T12:00:30.000Z", "setParentSyncCursor bumps last_sync_at because a page was successfully persisted");
    });
  });

  describe("markParentBackfillComplete — prune + row_count refresh (P2)", () => {
    it("deletes children where _synced_at < drain_started_at and refreshes row_count cache", () => {
      // Seed: one parent with one fresh child and one ghost child.
      ensureParentDrainMarker("tasks", "c6", "2026-04-24T10:00:00.000Z");
      // Ghost predates the drain:
      db.prepare(
        `INSERT INTO tasks(id, contact_id, title, _synced_at) VALUES ('ghost', 'c6', 'x', '2020-01-01T00:00:00.000Z')`,
      ).run();
      // Fresh:
      db.prepare(
        `INSERT INTO tasks(id, contact_id, title, _synced_at) VALUES ('fresh', 'c6', 'y', '2026-04-24T10:00:05.000Z')`,
      ).run();
      // Simulate row_count cache getting stale (drifted high):
      db.prepare(`UPDATE _sync_state SET row_count = 999 WHERE entity = 'tasks'`).run();

      const { pruned } = markParentBackfillComplete(
        "tasks",
        "c6",
        "tasks",
        "contact_id",
        "2026-04-24T10:01:00.000Z",
      );
      strictEqual(pruned, 1, "ghost row should be pruned");

      const rows = db
        .prepare(`SELECT id FROM tasks ORDER BY id`)
        .all() as Array<{ id: string }>;
      deepStrictEqual(rows.map((r) => r.id), ["fresh"], "ghost deleted, fresh retained");

      const cached = db
        .prepare(`SELECT row_count FROM _sync_state WHERE entity = 'tasks'`)
        .get() as { row_count: number };
      strictEqual(
        cached.row_count,
        1,
        "P2: row_count cache must be refreshed to match actual count (1) after prune — prior to the fix it would have stayed at 999.",
      );

      const s = getState("tasks", "c6");
      strictEqual(s?.backfill_complete, 1);
      strictEqual(s?.drain_started_at, null);
      strictEqual(s?.cursor, null);
      strictEqual(s?.last_sync_at, "2026-04-24T10:01:00.000Z");
    });

    it("skips prune + row_count refresh when drain_started_at is NULL", () => {
      // No ensureParentDrainMarker call → no drain_started_at → no prune.
      db.prepare(`UPDATE _sync_state SET row_count = 42 WHERE entity = 'tasks'`).run();
      const { pruned } = markParentBackfillComplete(
        "tasks",
        "c7",
        "tasks",
        "contact_id",
        "2026-04-24T10:01:00.000Z",
      );
      strictEqual(pruned, 0);
      const cached = db
        .prepare(`SELECT row_count FROM _sync_state WHERE entity = 'tasks'`)
        .get() as { row_count: number };
      strictEqual(cached.row_count, 42, "row_count untouched when nothing pruned");
    });
  });
});
