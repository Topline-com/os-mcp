import type { EntityManifest } from "@topline/shared-schema";
import { getByPath } from "./mapping.js";
import { validateTerminalPage } from "./response-validation.js";

const DEFAULT_PAGE_LIMIT = 100;

function extractNextCursor(
  response: unknown,
  items: ReadonlyArray<Record<string, unknown>>,
  cfg: NonNullable<EntityManifest["backfill"]["cursor"]>,
): Record<string, unknown> | null {
  let source: unknown;
  if (cfg.source === "meta") {
    source = (response as { meta?: unknown } | null | undefined)?.meta;
  } else {
    source = items[items.length - 1];
  }
  if (source === null || source === undefined || typeof source !== "object" || Array.isArray(source)) {
    throw new Error("Missing upstream cursor metadata");
  }
  const values = cfg.fields.map((field) => getByPath(source, field.response_path));
  if (values.every((value) => value === null)) return null;
  if (values.every((value) => value === undefined) && items.length < DEFAULT_PAGE_LIMIT) return null;
  const next: Record<string, unknown> = {};
  for (const f of cfg.fields) {
    const v = getByPath(source, f.response_path);
    if (v === null || v === undefined) {
      throw new Error("Incomplete upstream cursor");
    }
    const validScalar = (value: unknown) => (typeof value === "string" && value.length > 0) || (typeof value === "number" && Number.isFinite(value));
    if (f.encoding === "array" ? !Array.isArray(v) || !v.length || !v.every(validScalar) : !validScalar(v)) {
      throw new Error("Invalid upstream cursor");
    }
    next[f.request_param] = v;
  }
  return next;
}

/**
 * Resolve the next cursor value for any of the 3 backfill-loop call
 * sites, dispatching on manifest shape:
 *
 *   backfill.cursor (structured, multi-field) → extractNextCursor
 *   backfill.cursor_response_field (legacy scalar) → getByPath
 *   neither                                         → null (single-page)
 *
 * Returned value is shaped for the cursor storage + `cursorValue`
 * threading each loop already uses — a compound cursor comes back as
 * a Record<string, unknown>, a legacy scalar comes back as the raw
 * value. Both round-trip through serializeCursor / parseCursor cleanly.
 */
export function nextCursorFrom(
  response: unknown,
  items: ReadonlyArray<Record<string, unknown>>,
  backfill: EntityManifest["backfill"],
): unknown | null {
  let cursor: unknown = null;
  if (backfill.cursor) {
    cursor = extractNextCursor(response, items, backfill.cursor);
  } else if (backfill.cursor_response_field) {
    const value = getByPath(response, backfill.cursor_response_field);
    if (value === null || (value === undefined && items.length < DEFAULT_PAGE_LIMIT)) cursor = null;
    else if (typeof value !== "string" || !value.length) throw new Error("Invalid upstream cursor");
    else cursor = value;
  }
  if (cursor === null) validateTerminalPage(response);
  return cursor;
}
