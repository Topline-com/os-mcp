import type { EntityManifest } from "@topline/shared-schema";
import { getByPath, mapRow } from "./mapping.js";

/** Explicit continuation contradicts end-of-stream, even on an empty page. */
export function validateTerminalPage(response: unknown): void {
  for (const prefix of ["", "meta.", "messages.", "messages.meta."]) {
    for (const field of ["hasMore", "hasNextPage", "nextPage", "nextPageUrl", "nextCursor"]) {
      const value = getByPath(response, `${prefix}${field}`);
      if (value !== undefined && value !== null && value !== false && value !== "" && value !== 0) {
        throw new Error("Incomplete upstream response");
      }
    }
  }
}

/** Reject malformed or explicitly incomplete envelopes before any row is written. */
export function responseItems(response: unknown, path: string): Record<string, unknown>[] {
  const items = getByPath(response, path);
  if (!Array.isArray(items) || items.some((item) => !item || typeof item !== "object" || Array.isArray(item))) {
    throw new Error("Invalid upstream item array");
  }
  for (const key of ["truncated", "isTruncated", "meta.truncated", "meta.isTruncated", "messages.truncated", "messages.isTruncated"]) {
    const flag = getByPath(response, key);
    if (flag !== undefined && flag !== null && flag !== false) throw new Error("Truncated upstream response");
  }
  if (items.length === 0) validateTerminalPage(response);
  return items;
}

/** Unpaginated list contracts cannot safely ignore an advertised next page. */
export function validateSinglePage(response: unknown, count: number): void {
  validateTerminalPage(response);
  for (const key of ["total", "meta.total"]) {
    const total = getByPath(response, key);
    if (total !== undefined && (typeof total !== "number" || !Number.isSafeInteger(total) || total !== count)) {
      throw new Error("Upstream snapshot count mismatch");
    }
  }
}

/** Validate the whole page before writing, including tenant and parent scope. */
export function mappedRows(
  entity: EntityManifest,
  items: readonly Record<string, unknown>[],
  locationId: string,
  parent?: { column: string; id: string },
): Record<string, unknown>[] {
  const ids = new Set<string>();
  return items.map((raw) => {
    const row = mapRow(entity, raw);
    const id = row[entity.primary_key];
    if (typeof id !== "string" || !id.trim() || ids.has(id)) throw new Error("Invalid or duplicate upstream ID");
    ids.add(id);
    if (row.location_id == null) row.location_id = locationId;
    if (row.location_id !== locationId) throw new Error("Upstream tenant mismatch");
    if (parent) {
      if (row[parent.column] == null) row[parent.column] = parent.id;
      if (row[parent.column] !== parent.id) throw new Error("Upstream parent mismatch");
    }
    return row;
  });
}
