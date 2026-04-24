// Canonical SQL for the per-parent sync state machine.
//
// SINGLE SOURCE OF TRUTH. Both the LocationDO runtime
// (location-do.ts) and the regression test suite
// (parent-sync-state.test.ts) import these strings. That means a
// regression in the SQL here shows up as a failing test against a
// real node:sqlite database, and a divergence between runtime and
// test is structurally impossible.
//
// Why not just have LocationDO's methods and call them from the test
// with a fake storage.sql adapter? The DurableObject class pulls in
// cloudflare:workers at import time, which can't be loaded outside
// a Workers runtime. Extracting the SQL as plain strings lets Node's
// test runner import them without dragging in the DO framework.

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/** DDL for the per-parent sync state table. Used by the DO migration. */
export const PARENT_SYNC_STATE_DDL = `
  CREATE TABLE IF NOT EXISTS _parent_sync_state (
    entity TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    cursor TEXT,
    backfill_complete INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT,
    drain_started_at TEXT,
    PRIMARY KEY (entity, parent_id)
  )
`;

// ---------------------------------------------------------------------------
// Statements — each is a parameterized string matching the signature the
// RPC expects. Param order matches the comments on each.
// ---------------------------------------------------------------------------

/**
 * ensureParentDrainMarker — stamp that a drain is in progress for this
 * parent WITHOUT advancing last_sync_at.
 *
 *   last_sync_at = "most recent successful drain COMPLETION" — only
 *   markParentBackfillComplete advances it. Bumping it here (as the
 *   pre-P1-fix UPDATE did) meant a transient fetch failure left the
 *   parent looking "just synced" while backfill_complete stayed 1,
 *   and getNextParentsForChild could no longer re-select it.
 *
 *   backfill_complete = 0 on the UPDATE clause so the next tick's
 *   "incomplete" selector picks the parent up even if this drain
 *   aborts entirely.
 *
 *   drain_started_at uses COALESCE to preserve the earliest start
 *   across multi-tick resumes — snapshot-and-swap prune at completion
 *   needs the true drain boundary.
 *
 * Params: (entity, parent_id, drain_started_at_if_fresh)
 */
export const PARENT_ENSURE_DRAIN_MARKER = `
  INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
  VALUES (?, ?, NULL, 0, NULL, ?)
  ON CONFLICT(entity, parent_id) DO UPDATE
    SET backfill_complete = 0,
        drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)
`;

/**
 * setParentSyncCursor — advance the cursor after successfully upserting
 * a page of children for one parent. last_sync_at IS bumped here
 * because a page completed successfully (so "last successful progress"
 * legitimately advances). backfill_complete stays 0 — we're mid-drain.
 *
 * Params: (entity, parent_id, cursor, last_sync_at, drain_started_at_if_fresh)
 */
export const PARENT_SET_SYNC_CURSOR = `
  INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
  VALUES (?, ?, ?, 0, ?, ?)
  ON CONFLICT(entity, parent_id) DO UPDATE
    SET cursor = excluded.cursor,
        backfill_complete = 0,
        last_sync_at = excluded.last_sync_at,
        drain_started_at = COALESCE(_parent_sync_state.drain_started_at, excluded.drain_started_at)
`;

/**
 * markParentBackfillComplete — flip the parent to complete, clear the
 * cursor and drain marker, advance last_sync_at to NOW. Called after
 * the parent page loop reaches empty_page / null cursor.
 *
 * The prune statements (PARENT_PRUNE_CHILDREN) run SEPARATELY before
 * this statement — check drain_started_at first, DELETE stale
 * children, then call this to finalize state.
 *
 * Params: (entity, parent_id, now)
 */
export const PARENT_MARK_COMPLETE = `
  INSERT INTO _parent_sync_state(entity, parent_id, cursor, backfill_complete, last_sync_at, drain_started_at)
  VALUES (?, ?, NULL, 1, ?, NULL)
  ON CONFLICT(entity, parent_id) DO UPDATE
    SET cursor = NULL,
        backfill_complete = 1,
        last_sync_at = excluded.last_sync_at,
        drain_started_at = NULL
`;

/** Read the cursor for one parent's child sync. Params: (entity, parent_id). */
export const PARENT_GET_SYNC_CURSOR = `
  SELECT cursor FROM _parent_sync_state WHERE entity = ? AND parent_id = ?
`;

/** Read drain_started_at. Params: (entity, parent_id). */
export const PARENT_GET_DRAIN_START = `
  SELECT drain_started_at FROM _parent_sync_state WHERE entity = ? AND parent_id = ?
`;

// ---------------------------------------------------------------------------
// Dynamic SQL (identifiers interpolated) — caller validates idents first.
// ---------------------------------------------------------------------------

/**
 * Snapshot-and-swap prune for one parent's children. The caller MUST
 * validate childTable / fkColumn as safe identifiers before
 * interpolating. Params at runtime: (parent_id, drain_start).
 */
export function parentPruneChildrenSql(childTable: string, fkColumn: string): string {
  return `DELETE FROM ${childTable} WHERE ${fkColumn} = ? AND _synced_at < ?`;
}

/**
 * getNextParentsForChild — pick the next batch of parent IDs needing
 * sync. Priority: not-yet-complete first, then parents whose freshness
 * columns advanced past last_sync_at. Within each bucket, oldest
 * last-sync first so work spreads evenly.
 *
 * freshnessColumns are interpolated (caller validates); params at
 * runtime are (child_entity, limit).
 */
export function nextParentsForChildSql(
  parentTable: string,
  freshnessColumns: readonly string[],
): string {
  const freshnessSql =
    freshnessColumns.length > 0
      ? " OR " +
        freshnessColumns
          .map((col) => `p.${col} > COALESCE(s.last_sync_at, '')`)
          .join(" OR ")
      : "";
  return `
    SELECT p.id
      FROM ${parentTable} p
      LEFT JOIN _parent_sync_state s
        ON s.entity = ? AND s.parent_id = p.id
     WHERE COALESCE(s.backfill_complete, 0) = 0${freshnessSql}
     ORDER BY COALESCE(s.last_sync_at, ''), p.id
     LIMIT ?
  `;
}

/**
 * After a parent-scoped prune, refresh the cached _sync_state.row_count
 * for the affected table. Params: (entity). The SELECT count uses a
 * dynamic table identifier, hence the builder.
 */
export function refreshRowCountSql(table: string): { select: string; update: string } {
  return {
    select: `SELECT COUNT(*) AS n FROM ${table}`,
    update: `UPDATE _sync_state SET row_count = ? WHERE entity = ?`,
  };
}
