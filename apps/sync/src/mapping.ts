// GHL payload → SQLite row mapper.
//
// Reads each column's source_path from the manifest and extracts the
// corresponding value from the upstream JSON. Dot-notation is supported
// for nested fields ("meta.searchAfter"). Arrays / objects are passed
// through unchanged — the LocationDO's coerceForSqlite is responsible
// for final JSON.stringify on columns flagged `json: true`.

import type { EntityManifest } from "@topline/shared-schema";

/**
 * Convert one GHL record into a row keyed by the manifest's column names.
 * Missing source fields become null. The caller passes the resulting rows
 * to `LocationDO.upsertRows(table, rows)`.
 */
export function mapRow(
  entity: EntityManifest,
  raw: Record<string, unknown>,
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (const col of entity.columns) {
    // `_synced_at` is stamped server-side by the DO; leave it absent here.
    if (col.name === "_synced_at") continue;

    const path = col.source_path ?? col.name;
    const value = getByPath(raw, path);
    row[col.name] = value ?? null;
  }
  return row;
}

/**
 * Walk a JSON object by dot-separated path. Returns undefined for any
 * missing intermediate — never throws.
 */
export function getByPath(obj: unknown, path: string): unknown {
  const parts = path.split(".");
  let cur: unknown = obj;
  for (const p of parts) {
    if (cur === null || cur === undefined || typeof cur !== "object") {
      return undefined;
    }
    cur = (cur as Record<string, unknown>)[p];
  }
  return cur;
}
