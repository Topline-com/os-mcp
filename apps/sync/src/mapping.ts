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
 * Missing source fields become null. Timestamp columns normalized to ISO
 * 8601 per the manifest's `timestamp_format` flag. The caller passes the
 * resulting rows to `LocationDO.upsertRows(table, rows)`.
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

    if (value === null || value === undefined) {
      row[col.name] = null;
      continue;
    }

    // Normalize timestamps. GHL is inconsistent across endpoints — some
    // return ISO strings, some return ms-epoch numbers. We pick a single
    // on-disk representation (ISO 8601 string) so queries don't have to
    // care which upstream shape produced the row.
    if (col.timestamp_format === "ms_epoch" && typeof value === "number") {
      row[col.name] = isoFromEpochMs(value);
      continue;
    }
    if (col.timestamp_format === "ms_epoch" && typeof value === "string") {
      // Some endpoints occasionally return the epoch as a string —
      // be defensive.
      const n = Number(value);
      if (Number.isFinite(n) && n > 0) {
        row[col.name] = isoFromEpochMs(n);
        continue;
      }
    }

    row[col.name] = value;
  }
  return row;
}

/**
 * Convert Unix ms epoch to ISO 8601. Returns null for unreasonable
 * values so a bad upstream doesn't write garbage timestamps.
 */
function isoFromEpochMs(ms: number): string | null {
  if (!Number.isFinite(ms)) return null;
  // Accept values from 1970-01-01 to year 2100. Anything outside is
  // almost certainly a bug (negative, NaN-coerced, or e.g. seconds
  // mistakenly labelled as ms).
  if (ms < 0 || ms > 4_102_444_800_000) return null;
  try {
    return new Date(ms).toISOString();
  } catch {
    return null;
  }
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
