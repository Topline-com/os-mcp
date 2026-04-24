// Backfill orchestrator — dispatches per entity-manifest incremental type:
//
//   updated_after → backfillStandardCursor  (contacts, opportunities,
//                                             conversations, appointments)
//   per_parent    → not yet (messages) — stubbed with clear error
//   poll_full     → not yet (pipelines, pipeline_stages) — stubbed
//
// Appointments is additionally blocked because its manifest has
// `pagination: "unknown"` — we haven't verified GHL's list contract
// yet. The dispatcher refuses to touch it.
//
// The sync worker holds no persistent state. Every call loads the
// connection record from KV, decrypts the PIT, runs against GHL.
// Cursor progress is persisted in the LocationDO's `_sync_state`
// table so retries and scheduled re-runs resume from the right
// place.

import {
  loadAndDecryptConnection,
  type DecryptedConnection,
} from "@topline/shared-auth";
import {
  toplineFetch,
  credentialsContext,
  ToplineApiError,
  type ToplineFetchOptions,
} from "@topline/shared";
import { ENTITY_BY_TABLE, type EntityManifest } from "@topline/shared-schema";
import type { LocationDO, UpsertResult, QueryResult, SyncState } from "@topline/shared-do";
import { mapRow, getByPath } from "./mapping.js";

/**
 * Minimal RPC surface of LocationDO that sync actually uses. Declaring it
 * here (rather than relying on DurableObjectNamespace<LocationDO> to
 * project method types through the RPC boundary) sidesteps an issue where
 * workers-types' generic projection collapses the method return types to
 * `never` in downstream consumers.
 */
type LocationDOStub = {
  executeQuery(sql: string, params?: readonly unknown[], rowCap?: number): Promise<QueryResult>;
  upsertRows(
    table: string,
    rows: ReadonlyArray<Record<string, unknown>>,
  ): Promise<UpsertResult>;
  getSyncState(): Promise<SyncState>;
  setSyncCursor(entity: string, cursor: string): Promise<void>;
  clearSyncCursor(entity: string): Promise<void>;
  setSyncWatermark(entity: string, watermark: string): Promise<void>;
  markBackfillComplete(entity: string): Promise<void>;
  resetBackfill(entity: string): Promise<void>;
  startDrainIfUnset(entity: string): Promise<string>;
  setUpstreamTotal(entity: string, total: number): Promise<void>;
  pruneStaleRows(table: string, drainStartedAt: string): Promise<number>;
  clearDrainMarker(entity: string): Promise<void>;
  // Per-parent sync state — durable cursor + completion flag per parent
  // for per_parent entities (messages, call_events, tasks, notes).
  getNextParentsForChild(
    childEntity: string,
    parentTable: string,
    limit: number,
    freshnessColumns?: readonly string[],
  ): Promise<string[]>;
  getParentSyncCursor(entity: string, parentId: string): Promise<string | null>;
  setParentSyncCursor(entity: string, parentId: string, cursor: string): Promise<void>;
  markParentBackfillComplete(entity: string, parentId: string): Promise<void>;
};

function locationStub(
  ns: DurableObjectNamespace<LocationDO>,
  locationId: string,
): LocationDOStub {
  return ns.get(ns.idFromName(locationId)) as unknown as LocationDOStub;
}

export interface SyncEnv {
  TOKEN_SIGNING_SECRET: string;
  CONNECTIONS: KVNamespace;
  LOCATION_DO: DurableObjectNamespace<LocationDO>;
}

export interface BackfillResult {
  entity: string;
  connection_id: string;
  location_id: string;
  pages: number;
  rows_upserted: number;
  row_count_after: number;
  final_cursor: string | null;
  duration_ms: number;
  stopped_reason:
    | "empty_page"
    | "cursor_stalled"
    | "page_cap_hit"
    | "error"
    | "unsupported";
  error?: string;
  /**
   * Rows deleted by the end-of-drain snapshot-and-swap (i.e. rows that
   * existed in the DO but weren't re-observed this drain — upstream
   * deleted them). Omitted from the response when 0.
   */
  pruned_stale_rows?: number;
  /**
   * Denormalized stage rows when this BackfillResult represents a
   * pipelines → pipeline_stages expansion. Omitted otherwise.
   */
  denormalized_stage_rows?: number;
}

// Upper bound on pages for a single invocation. Sync workers have a
// ~30s CPU budget per request; 200 × 100 = 20k rows is well within
// that. Large accounts resume on the next invocation.
const MAX_PAGES_PER_INVOCATION = 200;
const DEFAULT_PAGE_LIMIT = 100;

// per_parent fan-out cap. Cloudflare Workers limit subrequests to 1000
// per invocation (Paid tier) — conservatively budget for ~20 parents
// per run. Each parent costs one GHL fetch + one DO upsert = 2
// subrequests, plus per-page follow-ups. 20 × (say 3 pages + 3 upserts)
// = ~120 subrequests leaves plenty of headroom. Re-declared as the
// single source of truth in the per_parent backfill section below;
// this legacy comment is retained so the rationale survives the
// constant's move.

// ---------------------------------------------------------------------------
// Public: dispatcher
// ---------------------------------------------------------------------------

/**
 * Back-fill one entity into the connection's LocationDO.
 *
 * Dispatches by the entity's incremental.type + backfill.pagination.
 * Unsupported modes return a result with `stopped_reason: "unsupported"`
 * and a descriptive error — they don't throw, so callers doing a
 * batch over many entities keep going.
 */
export async function backfillEntity(
  env: SyncEnv,
  connectionId: string,
  entityTable: string,
): Promise<BackfillResult> {
  const started = Date.now();
  const entity = ENTITY_BY_TABLE.get(entityTable);
  if (!entity) {
    return unsupportedResult(started, entityTable, connectionId, "", `Unknown entity: ${entityTable}`);
  }

  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) {
    throw new Error(`Unknown or revoked connection: ${connectionId}`);
  }

  // Refuse entities with unverified contracts. The manifest flags these
  // as pagination: "unknown" precisely so the sync worker can bail
  // instead of inventing a backfill pattern.
  if (entity.backfill.pagination === "unknown") {
    return unsupportedResult(
      started,
      entityTable,
      connectionId,
      connection.location_id,
      `Backfill contract for ${entityTable} is marked "unknown" in the manifest. Verify against live GHL before syncing.`,
    );
  }

  // call_events is a derived table: rows are written as a side effect
  // of backfillPerParent when entity.table === "messages". Don't offer
  // a direct backfill path for it — redirect the caller to messages.
  if (entityTable === "call_events") {
    return unsupportedResult(
      started,
      entityTable,
      connectionId,
      connection.location_id,
      "call_events is derived automatically while syncing messages; run /sync/backfill?entity=messages instead.",
    );
  }

  switch (entity.incremental.type) {
    case "updated_after":
      return backfillStandardCursor(env, connection, entity, started, connectionId);
    case "per_parent":
      return backfillPerParent(env, connection, entity, started, connectionId);
    case "poll_full":
      return backfillPollFull(env, connection, entity, started, connectionId);
  }
}

// ---------------------------------------------------------------------------
// Standard cursor backfill — GET or POST, cursor in query or body
// ---------------------------------------------------------------------------

/**
 * Paginate an entity whose backfill is a single endpoint with a cursor.
 *
 * Method matters: GET puts the cursor in the query string, POST puts it in
 * the body. Cursor shape is whatever GHL returns — we round-trip via JSON
 * serialization so arrays, objects, and strings all survive unchanged.
 */
async function backfillStandardCursor(
  env: SyncEnv,
  connection: DecryptedConnection,
  entity: EntityManifest,
  started: number,
  connectionId: string,
): Promise<BackfillResult> {
  const { backfill } = entity;
  const stub = locationStub(env.LOCATION_DO, connection.location_id);

  const priorState = await stub.getSyncState();
  const priorCursor = priorState[entity.table]?.cursor ?? null;
  let cursorValue: unknown = priorCursor ? parseCursor(priorCursor) : null;
  let cursorSerialized: string | null = priorCursor;

  let pages = 0;
  let rowsUpserted = 0;
  let prunedStale = 0;
  let lastRowCount = priorState[entity.table]?.row_count ?? 0;
  // Widening cast — inner assignments live inside an async closure that
  // TS flow analysis doesn't peek into, so without the cast the post-try
  // narrowed type collapses to just "empty_page" | "error" and the
  // "cursor_stalled" check below becomes "unintentional".
  let stoppedReason = "empty_page" as BackfillResult["stopped_reason"];
  let errorMsg: string | undefined;

  // Drain marker: stamped on first page of a multi-tick drain and
  // cleared when we reach empty_page/cursor_stalled. Rows that weren't
  // re-observed during this drain get snapshot-and-swap-pruned at the
  // end. Survives across ticks via _sync_state.drain_started_at.
  let drainStartedAt: string | null = priorState[entity.table]?.drain_started_at ?? null;

  try {
    await runInContext(connection, async () => {
      while (pages < MAX_PAGES_PER_INVOCATION) {
        const { query, body } = buildRequest(entity, connection.location_id, cursorValue);
        const response = await toplineFetch<Record<string, unknown>>(
          resolveEndpoint(backfill.endpoint, connection.location_id),
          buildFetchOptions(entity, query, body),
        );

        // Capture upstream `total` whenever it shows up. Drives the
        // self-heal drift detection on subsequent runs.
        const upstreamTotal = extractUpstreamTotal(response);
        if (upstreamTotal !== null) {
          try {
            await stub.setUpstreamTotal(entity.table, upstreamTotal);
          } catch {
            // non-fatal
          }
        }

        const itemsField = backfill.items_field ?? entity.table;
        const items = (getByPath(response, itemsField) ?? []) as Array<Record<string, unknown>>;
        if (!Array.isArray(items) || items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        // First page of a fresh drain → stamp drain marker so the
        // end-of-drain prune can delete rows that disappeared upstream.
        if (pages === 0 && drainStartedAt === null) {
          try {
            drainStartedAt = await stub.startDrainIfUnset(entity.table);
          } catch {
            // non-fatal — no prune this drain; rows stay until next drain
          }
        }

        const rows = items.map((raw) => mapRow(entity, raw));
        const result: UpsertResult = await stub.upsertRows(entity.table, rows);
        rowsUpserted += result.upserted;
        lastRowCount = result.row_count_after;
        pages += 1;

        const nextCursor = nextCursorFrom(response, items, backfill);
        if (nextCursor === null || nextCursor === undefined) {
          stoppedReason = "empty_page";
          break;
        }
        const nextSerialized = serializeCursor(nextCursor);
        if (nextSerialized === cursorSerialized) {
          stoppedReason = "cursor_stalled";
          break;
        }
        cursorValue = nextCursor;
        cursorSerialized = nextSerialized;
        await stub.setSyncCursor(entity.table, nextSerialized);
      }
      if (pages >= MAX_PAGES_PER_INVOCATION) {
        stoppedReason = "page_cap_hit";
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError
      ? `GHL ${err.statusCode}: ${err.message}`
      : err instanceof Error
      ? err.message
      : String(err);
  }

  // Post-run bookkeeping: if we walked to end-of-stream (empty_page OR
  // cursor_stalled), we've seen every row upstream. Prune any that
  // didn't get touched in this drain (deleted upstream), then capture
  // the max cursor_column value as the watermark and flip
  // backfill_complete so incremental polling can start. cursor_stalled
  // is a legitimate EOF signal when GHL echoes the last-row-id back on
  // the final page.
  if (stoppedReason === "empty_page" || stoppedReason === "cursor_stalled") {
    if (drainStartedAt !== null) {
      try {
        prunedStale = await stub.pruneStaleRows(entity.table, drainStartedAt);
        if (prunedStale > 0) {
          console.log(
            `[backfill:${entity.table}] pruned ${prunedStale} stale rows (deleted upstream)`,
          );
          lastRowCount = Math.max(0, lastRowCount - prunedStale);
        }
      } catch {
        // non-fatal
      }
    }
    try {
      await stub.clearDrainMarker(entity.table);
    } catch {
      // non-fatal
    }
    if (entity.incremental.cursor_column) {
      await setWatermarkAndComplete(stub, entity);
    }
  }

  return {
    entity: entity.table,
    connection_id: connectionId,
    location_id: connection.location_id,
    pages,
    rows_upserted: rowsUpserted,
    row_count_after: lastRowCount,
    final_cursor: cursorSerialized,
    duration_ms: Date.now() - started,
    stopped_reason: stoppedReason,
    ...(errorMsg ? { error: errorMsg } : {}),
    ...(prunedStale > 0 ? ({ pruned_stale_rows: prunedStale } as Record<string, number>) : {}),
  };
}

/**
 * After a complete backfill (empty_page stop), record:
 *   1. Clear the pagination cursor — a completed backfill is NOT a
 *      partial one to resume. Leaving a stale cursor means a later
 *      /sync/backfill resumes from the last page of the previous
 *      complete run instead of doing a true full refresh.
 *   2. Set the incremental watermark from MAX(cursor_column) in the DO.
 *   3. Flip backfill_complete so the cron-driven incremental path
 *      unlocks.
 *
 * Skipped cursor_column → skip the watermark step but still clear
 * cursor + mark complete.
 */
async function setWatermarkAndComplete(
  stub: LocationDOStub,
  entity: EntityManifest,
): Promise<void> {
  try {
    await stub.clearSyncCursor(entity.table);
  } catch {
    // Non-fatal — worst case, a future resume reads a stale cursor
    // and GHL returns an empty page. We'd rather proceed than throw.
  }

  const col = entity.incremental.cursor_column;
  if (col) {
    try {
      const result = await stub.executeQuery(
        `SELECT MAX(${col}) AS w FROM ${entity.table}`,
      );
      const row = result.rows[0] as { w: string | null } | undefined;
      if (row && row.w !== null && row.w !== undefined && String(row.w).length > 0) {
        await stub.setSyncWatermark(entity.table, String(row.w));
      }
    } catch {
      // Non-fatal — watermark will be retried on the next empty_page.
    }
  }

  try {
    await stub.markBackfillComplete(entity.table);
  } catch {
    // Non-fatal
  }
}

/**
 * Assemble the { query, body } pair for a given entity + cursor value,
 * honoring GET vs POST semantics and the manifest's query_extras.
 */
function buildRequest(
  entity: EntityManifest,
  locationId: string,
  cursorValue: unknown,
): { query: Record<string, string | number | boolean | undefined>; body: Record<string, unknown> | undefined } {
  const { backfill } = entity;
  const query: Record<string, string | number | boolean | undefined> = {};
  const body: Record<string, unknown> = {};

  // Fixed query_extras always go into the query for GET and into the body
  // for POST (GHL's POST search endpoints expect filter params in body).
  if (backfill.query_extras) {
    for (const [k, v] of Object.entries(backfill.query_extras)) {
      if (backfill.method === "GET") {
        query[k] = v;
      } else {
        body[k] = v;
      }
    }
  }

  // Location scopes the request. Two shapes:
  //   (a) as a query param or body key (most endpoints):
  //         /opportunities/search?location_id=X
  //         POST /contacts/search  { locationId: "X", ... }
  //   (b) as a path segment with a {locationId} template in the manifest:
  //         /locations/{locationId}/tags
  //         /locations/{locationId}/customFields
  //
  // When the endpoint template carries {locationId}, don't ALSO add it
  // as a query/body key — GHL's shape-(b) endpoints reject extra
  // locationId params from some SDKs. resolveEndpoint() handles the
  // path substitution; buildRequest just needs to skip the param.
  const endpointHasLocationToken = backfill.endpoint.includes("{locationId}");
  if (!endpointHasLocationToken) {
    const locParamName = backfill.location_param_name ?? "locationId";
    if (backfill.method === "GET") {
      query[locParamName] = locationId;
    } else {
      body[locParamName] = locationId;
    }
  }

  // Cursor placement. Two paths:
  //
  //   Structured cursor (backfill.cursor)  — multi-field, supports
  //     compound cursors (opportunities' startAfter+startAfterId) and
  //     array-encoded bodies (contacts' searchAfter). The cursorValue
  //     is a Record<string, unknown> mapping request_param → value as
  //     produced by extractNextCursor.
  //
  //   Legacy scalar (cursor_request_param)  — single-field string, used
  //     by messages (per_parent child pagination).
  if (cursorValue !== null && cursorValue !== undefined) {
    if (backfill.cursor) {
      applyCursor(query, body, backfill.method, cursorValue as Record<string, unknown>, backfill.cursor);
    } else if (backfill.cursor_request_param) {
      if (backfill.method === "GET") {
        query[backfill.cursor_request_param] = String(cursorValue);
      } else {
        body[backfill.cursor_request_param] = cursorValue;
      }
    }
  }

  return {
    query,
    body: backfill.method === "POST" ? body : undefined,
  };
}

function buildFetchOptions(
  entity: EntityManifest,
  query: Record<string, string | number | boolean | undefined>,
  body: Record<string, unknown> | undefined,
): ToplineFetchOptions {
  return {
    method: entity.backfill.method,
    query,
    ...(body !== undefined ? { body } : {}),
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function runInContext<T>(
  connection: DecryptedConnection,
  fn: () => Promise<T>,
): Promise<T> {
  return credentialsContext.run(
    { pit: connection.pit, locationId: connection.location_id },
    fn,
  );
}

function serializeCursor(value: unknown): string {
  return JSON.stringify(value);
}

/**
 * Resolve `{locationId}` (and any future path-template variables) in
 * the manifest's endpoint string. Some GHL endpoints take location as
 * a path segment (`/locations/{locationId}/tags`) rather than a query
 * param; this keeps the manifest declarative for both shapes.
 */
function resolveEndpoint(template: string, locationId: string): string {
  return template.replace("{locationId}", locationId);
}

/**
 * Extract the upstream row-count estimate from a GHL list response.
 * Endpoints are inconsistent about where they put this:
 *   contacts / conversations (POST search) → top-level `total`
 *   opportunities / appointments (GET search) → `meta.total`
 * Returns null when neither path has a non-negative number; the caller
 * treats that as "no signal" and skips the setUpstreamTotal call.
 */
function extractUpstreamTotal(response: unknown): number | null {
  if (response === null || response === undefined || typeof response !== "object") {
    return null;
  }
  const top = (response as { total?: unknown }).total;
  if (typeof top === "number" && top >= 0) return top;
  const meta = (response as { meta?: { total?: unknown } }).meta;
  if (meta && typeof meta === "object") {
    const metaTotal = (meta as { total?: unknown }).total;
    if (typeof metaTotal === "number" && metaTotal >= 0) return metaTotal;
  }
  return null;
}

function parseCursor(stored: string): unknown {
  try {
    return JSON.parse(stored);
  } catch {
    return stored;
  }
}

// ---------------------------------------------------------------------------
// callEventFromMessage — normalize a raw GHL message into a call_events row.
//
// GHL's call fields are inconsistently placed across endpoints:
//   - duration: sometimes `duration`, sometimes `call.duration`,
//     `meta.duration`, `meta.call.duration`, `metadata.duration`
//   - voicemail/missed: sometimes booleans, sometimes strings, sometimes
//     inferred from status = "voicemail"
//   - recording: `recordingUrl`, `call.recordingUrl`, `meta.call.recordingUrl`
//
// We try every candidate path once per field. When upstream doesn't
// supply a value we leave NULL and the LLM can fall back to checking
// status / duration heuristics.
// ---------------------------------------------------------------------------

const CALL_MESSAGE_TYPES = new Set([
  "TYPE_CALL",
  "TYPE_IVR_CALL",
  "TYPE_CUSTOM_CALL",
  "TYPE_CAMPAIGN_CALL",
]);

function callEventFromMessage(
  raw: Record<string, unknown>,
  locationId: string,
  parentConversationId: string,
): Record<string, unknown> | null {
  const messageType = firstString(raw, ["messageType", "type"]);
  if (!messageType || !CALL_MESSAGE_TYPES.has(messageType)) return null;

  const messageId = firstString(raw, ["id", "_id", "messageId"]);
  if (!messageId) return null;

  const duration = firstNumber(raw, [
    "duration",
    "durationSeconds",
    "call.duration",
    "meta.call.duration",
    "meta.duration",
    "metadata.duration",
  ]);
  const callStatus = firstString(raw, [
    "callStatus",
    "call.status",
    "meta.call.status",
    "meta.callStatus",
    "metadata.callStatus",
  ]);
  const recordingUrl = firstString(raw, [
    "recordingUrl",
    "call.recordingUrl",
    "meta.call.recordingUrl",
    "meta.recordingUrl",
    "metadata.recordingUrl",
  ]);
  const transcriptionUrl = firstString(raw, [
    "transcriptionUrl",
    "call.transcriptionUrl",
    "meta.call.transcriptionUrl",
  ]);
  const voicemail = firstBooleanish(raw, [
    "voicemail",
    "isVoicemail",
    "call.voicemail",
    "meta.call.voicemail",
  ]);
  const missed = firstBooleanish(raw, [
    "missed",
    "isMissed",
    "call.missed",
    "meta.call.missed",
  ]);

  return {
    id: messageId,
    location_id: locationId,
    message_id: messageId,
    conversation_id:
      firstString(raw, ["conversationId", "conversation_id"]) ?? parentConversationId,
    contact_id: firstString(raw, ["contactId", "contact_id"]),
    direction: firstString(raw, ["direction"]),
    call_type: messageType,
    event_at: normalizeTimestamp(firstValue(raw, ["dateAdded", "createdAt", "timestamp"])),
    status: firstString(raw, ["status"]),
    call_status: callStatus,
    duration_seconds: duration,
    recording_url: recordingUrl,
    transcription_url: transcriptionUrl,
    voicemail,
    missed,
    user_id: firstString(raw, ["userId", "user_id", "assignedTo", "assigned_to"]),
    from_number: firstString(raw, ["from", "fromNumber", "from_number"]),
    to_number: firstString(raw, ["to", "toNumber", "to_number"]),
    raw_payload: raw,
  };
}

function firstValue(raw: Record<string, unknown>, paths: readonly string[]): unknown {
  for (const path of paths) {
    const value = getByPath(raw, path);
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function firstString(raw: Record<string, unknown>, paths: readonly string[]): string | null {
  const v = firstValue(raw, paths);
  return v === null || v === undefined ? null : String(v);
}

function firstNumber(raw: Record<string, unknown>, paths: readonly string[]): number | null {
  const v = firstValue(raw, paths);
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function firstBooleanish(raw: Record<string, unknown>, paths: readonly string[]): number | null {
  const v = firstValue(raw, paths);
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "number") return v ? 1 : 0;
  if (typeof v === "string") {
    const n = v.toLowerCase();
    if (["true", "yes", "1", "voicemail", "missed"].includes(n)) return 1;
    if (["false", "no", "0"].includes(n)) return 0;
  }
  return null;
}

function normalizeTimestamp(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value).toISOString();
  }
  if (typeof value === "string" && value.length > 0) {
    const n = Number(value);
    if (Number.isFinite(n) && value.length >= 10) return new Date(n).toISOString();
    return value;
  }
  return null;
}

/**
 * Pick the set of parent-table columns whose advance indicates "this
 * parent has new children we should re-visit." For messages/call_events
 * it's conversations.last_message_date; for tasks/notes on contacts
 * it's contacts.updated_at.
 */
function parentFreshnessColumns(parent: EntityManifest): readonly string[] {
  const available = new Set(parent.columns.map((c) => c.name));
  const candidates = [
    parent.incremental.cursor_column,
    "updated_at",
    "last_message_date",
  ].filter((c): c is string => typeof c === "string" && available.has(c));
  return Array.from(new Set(candidates));
}

// ---------------------------------------------------------------------------
// Structured cursor helpers (BackfillDescriptor.cursor).
//
// GHL's cursor shapes vary per endpoint; these helpers normalize the mess:
//
//   extractNextCursor  — pull the next-cursor record out of a response
//                         (from `meta` or from the last item of items[]).
//                         Returns a map of { request_param → value } that
//                         buildRequest will inject into the next request,
//                         or null if any required field is missing (treat
//                         as end-of-stream).
//
//   applyCursor        — inject that map into the outbound query / body,
//                         respecting each field's encoding (scalar string
//                         for GETs, arrays-as-is for POST bodies like
//                         contacts' searchAfter: [ts, id]).
//
// Legacy single-field manifests (messages uses cursor_request_param +
// cursor_response_field) bypass these helpers and use the simpler scalar
// code path in each loop.
// ---------------------------------------------------------------------------

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
  if (source === null || source === undefined || typeof source !== "object") {
    return null;
  }
  const next: Record<string, unknown> = {};
  for (const f of cfg.fields) {
    const v = getByPath(source, f.response_path);
    if (v === null || v === undefined) {
      // Incomplete cursor → treat as end-of-stream. GHL leaves the
      // trailing cursor field off on the last page.
      return null;
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
function nextCursorFrom(
  response: unknown,
  items: ReadonlyArray<Record<string, unknown>>,
  backfill: EntityManifest["backfill"],
): unknown | null {
  if (backfill.cursor) {
    return extractNextCursor(response, items, backfill.cursor);
  }
  if (backfill.cursor_response_field) {
    const v = getByPath(response, backfill.cursor_response_field);
    return v === undefined ? null : v;
  }
  return null;
}

function applyCursor(
  query: Record<string, string | number | boolean | undefined>,
  body: Record<string, unknown>,
  method: "GET" | "POST",
  cursor: Record<string, unknown>,
  cfg: NonNullable<EntityManifest["backfill"]["cursor"]>,
): void {
  for (const f of cfg.fields) {
    const v = cursor[f.request_param];
    if (v === null || v === undefined) continue;
    const encoding = f.encoding ?? "scalar";
    if (method === "GET") {
      // GETs can't carry arrays meaningfully; if an entity ever needed
      // array-valued query params we'd JSON-stringify here. Today only
      // POST body shapes use encoding: "array" (contacts).
      query[f.request_param] =
        encoding === "array"
          ? JSON.stringify(v)
          : typeof v === "number" || typeof v === "boolean"
          ? v
          : String(v);
    } else {
      body[f.request_param] = encoding === "array" ? v : v;
    }
  }
}

function unsupportedResult(
  started: number,
  table: string,
  connectionId: string,
  locationId: string,
  message: string,
): BackfillResult {
  return {
    entity: table,
    connection_id: connectionId,
    location_id: locationId,
    pages: 0,
    rows_upserted: 0,
    row_count_after: 0,
    final_cursor: null,
    duration_ms: Date.now() - started,
    stopped_reason: "unsupported",
    error: message,
  };
}

// ---------------------------------------------------------------------------
// per_parent — messages, call_events (derived), tasks, notes, and any
// future entity whose upstream shape is "one endpoint per parent id".
//
// Progress lives in _parent_sync_state (entity, parent_id) — one row
// per parent, not one row per entity. Each parent tracks its own
// cursor + backfill_complete flag. That means:
//
//   - Mid-parent subrequest-cap errors don't lose progress; the next
//     invocation resumes the same parent from the persisted cursor.
//   - A re-activated parent (conversation with a new message, contact
//     with a new task) gets revisited automatically because the
//     `getNextParentsForChild` RPC prioritizes parents whose freshness
//     column has advanced past last_sync_at.
//   - Empty parents flip complete on their first pass instead of being
//     re-probed forever.
//
// Supports two endpoint shapes via the manifest:
//   (a) path template: endpoint "/contacts/{parent}/tasks"
//   (b) query param:   endpoint "/forms/submissions",
//                      per_parent.request_param "formId"
//
// Dual-write: when entity.table === "messages" we also extract
// call-event rows via callEventFromMessage and upsert them into
// call_events in the same cycle. This keeps the typed call_events
// table always consistent with its source messages.
// ---------------------------------------------------------------------------

// Paid-plan-friendly sizing. Each parent typically costs 1–3 GHL
// subrequests + 1 DO upsert RPC, so 200 parents × ~4 calls = ~800
// under the 1000-subrequest Cloudflare cap.
const MAX_PARENTS_PER_INVOCATION = 200;

async function backfillPerParent(
  env: SyncEnv,
  connection: DecryptedConnection,
  entity: EntityManifest,
  started: number,
  connectionId: string,
): Promise<BackfillResult> {
  const { backfill } = entity;
  if (!backfill.per_parent) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent entity ${entity.table} has no per_parent descriptor.`,
    );
  }
  // Cursor metadata is only required for paginated children. Tasks /
  // notes use pagination: "none" (one page per parent is all GHL
  // gives you), so those don't need cursor_request_param.
  if (
    backfill.pagination === "cursor" &&
    (!backfill.cursor_request_param || !backfill.cursor_response_field)
  ) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent entity ${entity.table} with pagination=cursor is missing cursor metadata.`,
    );
  }

  const parentEntity = ENTITY_BY_TABLE.get(backfill.per_parent.parent_entity);
  if (!parentEntity) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent entity ${entity.table} references unknown parent ${backfill.per_parent.parent_entity}.`,
    );
  }

  const endpointTemplate = backfill.endpoint;
  const usesPathTemplate = endpointTemplate.includes("{parent}");
  if (!usesPathTemplate && !backfill.per_parent.request_param) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent endpoint ${endpointTemplate} must contain {parent} or set per_parent.request_param.`,
    );
  }

  const stub = locationStub(env.LOCATION_DO, connection.location_id);
  const fkColumn = backfill.per_parent.parent_fk_column;

  // Pull the next batch of parents to process. The DO RPC prioritizes
  // parents that aren't complete yet, then parents whose freshness
  // column has advanced since their last sync.
  const parentIds = await stub.getNextParentsForChild(
    entity.table,
    parentEntity.table,
    MAX_PARENTS_PER_INVOCATION,
    parentFreshnessColumns(parentEntity),
  );

  if (parentIds.length === 0) {
    // No remaining parents needing work this tick. Flip the entity-
    // level backfill_complete so the cron's freshness signals know
    // every parent has been visited at least once.
    try {
      await stub.markBackfillComplete(entity.table);
    } catch {
      // non-fatal
    }
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      pages: 0,
      rows_upserted: 0,
      row_count_after: 0,
      final_cursor: null,
      duration_ms: Date.now() - started,
      stopped_reason: "empty_page",
    };
  }

  let totalPages = 0;
  let rowsUpserted = 0;
  let callEventsUpserted = 0; // diagnostic only; logged on completion
  let lastRowCount = 0;
  let stoppedReason = "empty_page" as BackfillResult["stopped_reason"];
  let errorMsg: string | undefined;

  try {
    await runInContext(connection, async () => {
      parentLoop: for (const parentId of parentIds) {
        const priorParentCursor = await stub.getParentSyncCursor(entity.table, parentId);
        let cursorValue: unknown = priorParentCursor
          ? parseCursor(priorParentCursor)
          : null;
        let cursorSerialized: string | null = priorParentCursor;
        let pagesForParent = 0;
        let parentCompleted = false;

        while (pagesForParent < MAX_PAGES_PER_INVOCATION) {
          if (totalPages >= MAX_PAGES_PER_INVOCATION) {
            stoppedReason = "page_cap_hit";
            break parentLoop;
          }

          const query: Record<string, string | number | boolean | undefined> = {};
          // Only set `limit` on cursor-paginated endpoints. Some
          // single-page per-parent endpoints (/contacts/{id}/notes)
          // 422 on any `limit` param at all.
          if (backfill.pagination === "cursor") {
            query.limit = DEFAULT_PAGE_LIMIT;
          }
          if (backfill.query_extras) {
            for (const [k, v] of Object.entries(backfill.query_extras)) {
              query[k] = v;
            }
          }
          if (
            backfill.cursor_request_param &&
            cursorValue !== null &&
            cursorValue !== undefined
          ) {
            query[backfill.cursor_request_param] = String(cursorValue);
          }
          if (backfill.per_parent!.request_param) {
            query[backfill.per_parent!.request_param!] = parentId;
          }
          const endpoint = resolveEndpoint(
            usesPathTemplate ? endpointTemplate.replace("{parent}", parentId) : endpointTemplate,
            connection.location_id,
          );

          const response = await toplineFetch<Record<string, unknown>>(endpoint, {
            method: backfill.method,
            query,
          });

          const itemsField = backfill.items_field ?? entity.table;
          const items = (getByPath(response, itemsField) ?? []) as Array<Record<string, unknown>>;
          if (!Array.isArray(items) || items.length === 0) {
            parentCompleted = true;
            break;
          }

          const rows = items.map((raw) => {
            const row = mapRow(entity, raw);
            if (row[fkColumn] === null || row[fkColumn] === undefined) {
              row[fkColumn] = parentId;
            }
            if (row.location_id === null || row.location_id === undefined) {
              row.location_id = connection.location_id;
            }
            return row;
          });
          const result: UpsertResult = await stub.upsertRows(entity.table, rows);
          rowsUpserted += result.upserted;
          lastRowCount = result.row_count_after;

          // Dual-write call_events from messages. Each TYPE_CALL /
          // TYPE_IVR_CALL / TYPE_CUSTOM_CALL / TYPE_CAMPAIGN_CALL
          // message becomes a call_events row with duration + status
          // + voicemail/missed flags normalized across GHL's varied
          // shapes.
          if (entity.table === "messages") {
            const callRows: Record<string, unknown>[] = [];
            for (const raw of items) {
              const ce = callEventFromMessage(raw, connection.location_id, parentId);
              if (ce) callRows.push(ce);
            }
            if (callRows.length > 0) {
              const ceResult = await stub.upsertRows("call_events", callRows);
              callEventsUpserted += ceResult.upserted;
            }
          }

          totalPages += 1;
          pagesForParent += 1;

          // Non-cursor pagination (tasks, notes): first page is all we
          // get. Mark parent complete.
          if (backfill.pagination !== "cursor") {
            parentCompleted = true;
            break;
          }
          const nextCursor = getByPath(response, backfill.cursor_response_field!);
          if (nextCursor === null || nextCursor === undefined) {
            parentCompleted = true;
            break;
          }
          const nextSerialized = serializeCursor(nextCursor);
          if (nextSerialized === cursorSerialized) {
            parentCompleted = true;
            break;
          }
          cursorValue = nextCursor;
          cursorSerialized = nextSerialized;
          // Persist mid-parent cursor so a subrequest-cap failure on
          // the next page can resume from here.
          await stub.setParentSyncCursor(entity.table, parentId, nextSerialized);
        }

        if (parentCompleted) {
          await stub.markParentBackfillComplete(entity.table, parentId);
        }
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError
      ? `GHL ${err.statusCode}: ${err.message}`
      : err instanceof Error
      ? err.message
      : String(err);
  }

  if (callEventsUpserted > 0) {
    console.log(
      `[per_parent:${entity.table}] dual-wrote ${callEventsUpserted} call_events rows`,
    );
  }

  return {
    entity: entity.table,
    connection_id: connectionId,
    location_id: connection.location_id,
    pages: totalPages,
    rows_upserted: rowsUpserted,
    row_count_after: lastRowCount,
    final_cursor: null,
    duration_ms: Date.now() - started,
    stopped_reason: stoppedReason,
    ...(errorMsg ? { error: errorMsg } : {}),
  };
}

// ---------------------------------------------------------------------------
// poll_full — single-shot fetch, no pagination. For pipelines the
// response carries nested stages[] that get denormalized into the
// pipeline_stages table in the same pass.
// ---------------------------------------------------------------------------

async function backfillPollFull(
  env: SyncEnv,
  connection: DecryptedConnection,
  entity: EntityManifest,
  started: number,
  connectionId: string,
): Promise<BackfillResult> {
  const { backfill } = entity;
  const stub = locationStub(env.LOCATION_DO, connection.location_id);

  let rowsUpserted = 0;
  let rowCountAfter = 0;
  let pagesProcessed = 0;
  let prunedStale = 0;
  // Initializer uses a widening annotation cast — the inner assignments
  // happen inside an async closure (runInContext callback) that TS's
  // flow analysis doesn't peek into, so without the cast the post-try
  // narrowed type would be just "empty_page" | "error".
  let stoppedReason = "empty_page" as BackfillResult["stopped_reason"];
  let errorMsg: string | undefined;
  let denormalizedStageRows = 0;

  // Three-mode poll_full:
  //
  //   backfill_complete = false   → FRESH DRAIN. Resume from persisted
  //     cursor. Walk every page up to the invocation cap. Stamp a
  //     drain_started_at marker on first page so snapshot-and-swap at
  //     the end can delete rows that disappeared upstream.
  //
  //   complete + drift detected   → SELF-HEAL. Our row_count is
  //     materially smaller than the last-seen upstream total (symptom
  //     of an older broken-cursor run that marked us complete with
  //     partial data). Reset the flag and fall through to fresh-drain
  //     mode in the same invocation.
  //
  //   complete + no drift         → STEADY STATE. Walk newest-first
  //     pages until we hit a row whose cursor_column value is older
  //     than the DO's recorded watermark — everything past that point
  //     is already in our table. This replaces the old single-page
  //     freshness mode, which could miss rows when more than one page
  //     of updates happened between cron ticks.
  //
  // Pipelines / pipeline_stages are single-page endpoints (pagination
  // != "cursor"); both the fresh and steady-state paths collapse to
  // one fetch for them.
  const DRIFT_THRESHOLD = 0.9; // row_count < 90% of upstream → reassume broken
  const priorState = await stub.getSyncState();
  const priorStateRow = priorState[entity.table];
  let isFresh = !priorStateRow?.backfill_complete;

  // Self-heal: detect a stale `backfill_complete=1` alongside data that's
  // materially smaller than upstream. Reset to fresh-drain mode.
  if (
    !isFresh &&
    priorStateRow?.upstream_total != null &&
    priorStateRow.upstream_total > 0 &&
    priorStateRow.row_count / priorStateRow.upstream_total < DRIFT_THRESHOLD
  ) {
    console.log(
      `[poll_full:${entity.table}] drift detected row_count=${priorStateRow.row_count} ` +
        `< ${DRIFT_THRESHOLD * 100}% of upstream_total=${priorStateRow.upstream_total}; ` +
        `resetting backfill_complete`,
    );
    try {
      await stub.resetBackfill(entity.table);
    } catch {
      // non-fatal — if the reset fails we'll stay in steady-state but
      // correctness is eventually consistent once upstream_total re-compares
    }
    isFresh = true;
  }

  const resumeCursor = isFresh && priorStateRow?.cursor ? parseCursor(priorStateRow.cursor) : null;
  const maxPagesThisRun = isFresh
    ? MAX_PAGES_PER_INVOCATION
    : backfill.pagination === "cursor"
    ? MAX_PAGES_PER_INVOCATION
    : 1;

  // Steady-state watermark: lets the page loop stop the moment it's
  // caught up to what we already have. Resolved once per invocation so
  // concurrent upserts during the drain don't move the goalpost.
  let steadyWatermark: string | null = null;
  if (!isFresh && entity.incremental.cursor_column && backfill.pagination === "cursor") {
    try {
      const wm = await stub.executeQuery(
        `SELECT MAX(${entity.incremental.cursor_column}) AS w FROM ${entity.table}`,
        [],
      );
      const w = (wm.rows[0] as { w: string | null } | undefined)?.w;
      steadyWatermark = w == null ? null : String(w);
    } catch {
      steadyWatermark = null;
    }
  }

  // Stamp a drain marker on the first page of a fresh drain. The return
  // value survives across multi-tick drains so snapshot-and-swap at the
  // end knows exactly which rows were refreshed during THIS drain.
  let drainStartedAt: string | null =
    isFresh ? priorStateRow?.drain_started_at ?? null : null;

  try {
    await runInContext(connection, async () => {
      let cursorValue: unknown = resumeCursor;
      let cursorSerialized: string | null = resumeCursor ? serializeCursor(resumeCursor) : null;
      let caughtUpToWatermark = false;

      while (pagesProcessed < maxPagesThisRun) {
        const { query, body } = buildRequest(entity, connection.location_id, cursorValue);
        const response = await toplineFetch<Record<string, unknown>>(
          resolveEndpoint(backfill.endpoint, connection.location_id),
          buildFetchOptions(entity, query, body),
        );

        // GHL returns `total` on the top-level of many list responses.
        // Persist whichever one we see so the next run's drift check has
        // something to compare against.
        const upstreamTotal = extractUpstreamTotal(response);
        if (upstreamTotal !== null) {
          try {
            await stub.setUpstreamTotal(entity.table, upstreamTotal);
          } catch {
            // non-fatal
          }
        }

        // Primary table: resolve items_field (or default to entity.table).
        // Pipeline's primary items live at "pipelines"; pipeline_stages
        // items are denormalized from pipelines[].stages[] (see below).
        const itemsField =
          entity.table === "pipeline_stages" ? "pipelines" : backfill.items_field ?? entity.table;
        const items = (getByPath(response, itemsField) ?? []) as Array<Record<string, unknown>>;
        if (!Array.isArray(items) || items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        // First page of a fresh drain → stamp the drain marker so
        // snapshot-and-swap at the end can identify stale rows.
        if (isFresh && pagesProcessed === 0 && drainStartedAt === null) {
          try {
            drainStartedAt = await stub.startDrainIfUnset(entity.table);
          } catch {
            // non-fatal — missing marker means we can't prune, but the
            // drain itself still completes correctly
          }
        }

        if (entity.table === "pipeline_stages") {
          // Denormalize: expand every pipeline's stages[] into stage rows.
          const stageRows: Array<Record<string, unknown>> = [];
          for (const pipeline of items) {
            const pipelineId = pipeline.id;
            const stages = (pipeline.stages ?? []) as Array<Record<string, unknown>>;
            for (const stage of stages) {
              const row = mapRow(entity, stage);
              if (row.pipeline_id === null || row.pipeline_id === undefined) {
                row.pipeline_id = pipelineId ?? null;
              }
              if (row.location_id === null || row.location_id === undefined) {
                row.location_id = connection.location_id;
              }
              stageRows.push(row);
            }
          }
          if (stageRows.length > 0) {
            const result = await stub.upsertRows("pipeline_stages", stageRows);
            rowsUpserted += result.upserted;
            rowCountAfter = result.row_count_after;
            denormalizedStageRows += stageRows.length;
          }
        } else {
          const rows = items.map((raw) => {
            const row = mapRow(entity, raw);
            if (row.location_id === null || row.location_id === undefined) {
              row.location_id = connection.location_id;
            }
            return row;
          });
          const result = await stub.upsertRows(entity.table, rows);
          rowsUpserted += result.upserted;
          rowCountAfter = result.row_count_after;

          // Steady-state watermark check: if ANY row on this page is
          // older than our watermark, we've reached previously-synced
          // territory. Subsequent pages are by construction older, so
          // stop. The current page's older rows are harmlessly
          // re-upserted — cheaper than splitting the page.
          if (
            !isFresh &&
            steadyWatermark !== null &&
            entity.incremental.cursor_column
          ) {
            const col = entity.incremental.cursor_column;
            for (const r of rows) {
              const v = r[col];
              if (v !== null && v !== undefined && String(v) <= steadyWatermark) {
                caughtUpToWatermark = true;
                break;
              }
            }
          }
        }
        pagesProcessed += 1;

        // Steady-state short-circuit: we saw a row at-or-older-than the
        // watermark, so we're caught up.
        if (caughtUpToWatermark) {
          stoppedReason = "empty_page";
          break;
        }

        // Single-page endpoints (pagination: "none") → we're done.
        // Cursor-paginated endpoints → route through nextCursorFrom.
        if (backfill.pagination !== "cursor") {
          stoppedReason = "empty_page";
          break;
        }
        const nextCursor = nextCursorFrom(response, items, backfill);
        if (nextCursor === null || nextCursor === undefined) {
          stoppedReason = "empty_page";
          break;
        }
        const nextSerialized = serializeCursor(nextCursor);
        if (nextSerialized === cursorSerialized) {
          // GHL echoed the same cursor back — common on the last page
          // (returns final row's id as startAfterId). Treat as done.
          stoppedReason = "cursor_stalled";
          break;
        }
        cursorValue = nextCursor;
        cursorSerialized = nextSerialized;
        // Persist resume cursor on every page so a subrequest-cap error
        // leaves the next invocation in a usable resume position.
        if (isFresh) {
          await stub.setSyncCursor(entity.table, nextSerialized);
        }
      }
      if (isFresh && pagesProcessed >= maxPagesThisRun) {
        stoppedReason = "page_cap_hit";
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError
      ? `GHL ${err.statusCode}: ${err.message}`
      : err instanceof Error
      ? err.message
      : String(err);
  }

  // End-of-stream bookkeeping. A fresh drain that reached empty_page /
  // cursor_stalled has observed every upstream row — this is where we:
  //   1. snapshot-and-swap prune stale rows (deleted upstream)
  //   2. clear the resume cursor
  //   3. mark backfill_complete = 1
  //   4. clear the drain marker
  // Steady-state runs skip all of this — they don't change the
  // complete flag and their row-level refresh is idempotent.
  if (isFresh && (stoppedReason === "empty_page" || stoppedReason === "cursor_stalled")) {
    if (drainStartedAt !== null) {
      try {
        prunedStale = await stub.pruneStaleRows(entity.table, drainStartedAt);
        if (prunedStale > 0) {
          console.log(
            `[poll_full:${entity.table}] pruned ${prunedStale} stale rows (deleted upstream)`,
          );
          // Post-prune: the reported row_count_after should match the
          // real table, not the pre-prune upsert high-water mark.
          rowCountAfter = Math.max(0, rowCountAfter - prunedStale);
        }
      } catch {
        // non-fatal — leaving stale rows is better than failing the
        // whole drain. Next drain will retry the prune.
      }
    }
    try {
      await stub.clearSyncCursor(entity.table);
    } catch {
      // non-fatal
    }
    try {
      await stub.markBackfillComplete(entity.table);
    } catch {
      // non-fatal
    }
    try {
      await stub.clearDrainMarker(entity.table);
    } catch {
      // non-fatal
    }
  }

  return {
    entity: entity.table,
    connection_id: connectionId,
    location_id: connection.location_id,
    pages: pagesProcessed,
    rows_upserted: rowsUpserted,
    row_count_after: rowCountAfter,
    final_cursor: null,
    duration_ms: Date.now() - started,
    stopped_reason: stoppedReason,
    ...(errorMsg ? { error: errorMsg } : {}),
    ...(denormalizedStageRows > 0 ? ({ denormalized_stage_rows: denormalizedStageRows } as Record<string, number>) : {}),
    ...(prunedStale > 0 ? ({ pruned_stale_rows: prunedStale } as Record<string, number>) : {}),
  };
}

// ---------------------------------------------------------------------------
// Incremental sync — called by cron every 15 min per entity.
//
// For updated_after entities:
//   Require backfill_complete = 1 (else the watermark would miss rows).
//   Inject cursor_query_param=<watermark> into the request. Paginate any
//   matching rows. At end, advance the watermark to max(cursor_column)
//   in the DO.
//
// For poll_full entities:
//   Just re-run the poll_full backfill. It refreshes everything in one
//   shot; idempotent on unchanged rows.
//
// For per_parent entities:
//   Not supported in this commit. Webhooks (phase 1 step 5) are the
//   primary freshness mechanism for messages.
// ---------------------------------------------------------------------------

export interface IncrementalResult {
  entity: string;
  connection_id: string;
  location_id: string;
  skipped?: "not_complete" | "unsupported_mode" | "no_watermark_source";
  pages: number;
  rows_upserted: number;
  row_count_after: number;
  watermark_before: string | null;
  watermark_after: string | null;
  duration_ms: number;
  stopped_reason:
    | "empty_page"
    | "cursor_stalled"
    | "page_cap_hit"
    | "error"
    | "skipped";
  error?: string;
  /** Forwarded from the underlying BackfillResult when > 0. */
  pruned_stale_rows?: number;
}

export async function incrementalEntity(
  env: SyncEnv,
  connectionId: string,
  entityTable: string,
): Promise<IncrementalResult> {
  const started = Date.now();
  const entity = ENTITY_BY_TABLE.get(entityTable);
  if (!entity) {
    return {
      entity: entityTable,
      connection_id: connectionId,
      location_id: "",
      pages: 0,
      rows_upserted: 0,
      row_count_after: 0,
      watermark_before: null,
      watermark_after: null,
      duration_ms: Date.now() - started,
      stopped_reason: "error",
      error: `Unknown entity: ${entityTable}`,
    };
  }

  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) {
    throw new Error(`Unknown or revoked connection: ${connectionId}`);
  }

  const stub = locationStub(env.LOCATION_DO, connection.location_id);
  const state = await stub.getSyncState();
  const stateRow = state[entity.table];

  // call_events rides along with messages. Trying to run its incremental
  // independently would double-fetch or error confusingly — short-circuit.
  if (entityTable === "call_events") {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "unsupported_mode",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow?.row_count ?? 0,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: stateRow?.watermark ?? null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: "call_events is derived alongside messages; run incremental/backfill for messages.",
    };
  }

  // Poll-full entities: re-run the poll_full backfill. Cheap, idempotent.
  if (entity.incremental.type === "poll_full") {
    const res = await backfillPollFull(env, connection, entity, started, connectionId);
    const stopped: IncrementalResult["stopped_reason"] =
      res.stopped_reason === "unsupported" ? "error" : res.stopped_reason;
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      pages: res.pages,
      rows_upserted: res.rows_upserted,
      row_count_after: res.row_count_after,
      watermark_before: null,
      watermark_after: null,
      duration_ms: res.duration_ms,
      stopped_reason: stopped,
      ...(res.error ? { error: res.error } : {}),
      ...(res.pruned_stale_rows != null && res.pruned_stale_rows > 0
        ? { pruned_stale_rows: res.pruned_stale_rows }
        : {}),
    };
  }

  // per_parent entities: delegate to backfillPerParent. That function
  // is bi-modal off backfill_complete — fresh drain while the initial
  // history is being loaded, then activity-driven refresh of parents
  // whose cursor_column has advanced. Same function handles both,
  // which keeps the state-machine transitions in one place.
  if (entity.incremental.type === "per_parent") {
    const res = await backfillEntity(env, connectionId, entityTable);
    const stopped: IncrementalResult["stopped_reason"] =
      res.stopped_reason === "unsupported" ? "error" : res.stopped_reason;
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      pages: res.pages,
      rows_upserted: res.rows_upserted,
      row_count_after: res.row_count_after,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: null,
      duration_ms: res.duration_ms,
      stopped_reason: stopped,
      ...(res.error ? { error: res.error } : {}),
    };
  }

  if (entity.incremental.type !== "updated_after") {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "unsupported_mode",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow?.row_count ?? 0,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: stateRow?.watermark ?? null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: `Incremental sync for ${entity.incremental.type} entities not implemented.`,
    };
  }

  // If the initial backfill isn't complete yet, use this cron tick to
  // advance it instead of bailing. This is the auto-seed path: on a
  // fresh connection the DO has zero rows for an updated_after entity,
  // and without this delegation the cron would refuse forever and
  // contacts / any future updated_after entity would stay empty until
  // a human ran /sync/backfill manually.
  //
  // backfillEntity resumes from the prior persisted cursor, so each
  // successive cron tick advances more pages. Once it reaches
  // empty_page the post-run hook flips backfill_complete and the NEXT
  // tick naturally switches to the real incremental watermark filter.
  if (!stateRow?.backfill_complete) {
    const res = await backfillEntity(env, connectionId, entityTable);
    const stopped: IncrementalResult["stopped_reason"] =
      res.stopped_reason === "unsupported" ? "error" : res.stopped_reason;
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      pages: res.pages,
      rows_upserted: res.rows_upserted,
      row_count_after: res.row_count_after,
      watermark_before: null,
      watermark_after: null,
      duration_ms: res.duration_ms,
      stopped_reason: stopped,
      ...(res.error ? { error: res.error } : {}),
      ...(res.pruned_stale_rows != null && res.pruned_stale_rows > 0
        ? { pruned_stale_rows: res.pruned_stale_rows }
        : {}),
    };
  }

  // Require an entity cursor_query_param and current watermark. If either
  // is missing, we have no way to filter — skip with a clear reason.
  if (!entity.incremental.cursor_query_param || !entity.incremental.cursor_column) {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "no_watermark_source",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow?.row_count ?? 0,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: stateRow?.watermark ?? null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: `Entity ${entity.table} has no cursor_query_param / cursor_column configured for incremental sync.`,
    };
  }

  // Filter contract not yet verified for this entity. GHL's filter grammar
  // varies per-endpoint (see notes on IncrementalDescriptor.filter_ready);
  // running an unverified filter burns API budget for likely 422s. When a
  // maintainer confirms the shape, they flip this flag in the manifest.
  if (!entity.incremental.filter_ready) {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "no_watermark_source",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow?.row_count ?? 0,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: stateRow?.watermark ?? null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: `Entity ${entity.table} has filter_ready: false. Its incremental filter contract against GHL has not been verified; stick to manual /sync/backfill until the manifest entry is updated.`,
    };
  }

  const watermarkBefore = stateRow.watermark;
  if (!watermarkBefore) {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "no_watermark_source",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow.row_count,
      watermark_before: null,
      watermark_after: null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: `No watermark recorded yet (likely because MAX(${entity.incremental.cursor_column}) is NULL — check that the column is populated).`,
    };
  }

  // Run a cursor-paginated fetch with cursor_query_param=<watermark>
  // injected. Mirror backfillStandardCursor's pagination loop.
  let pages = 0;
  let rowsUpserted = 0;
  let lastRowCount = stateRow.row_count;
  let stoppedReason: IncrementalResult["stopped_reason"] = "empty_page";
  let errorMsg: string | undefined;
  let paginationCursor: unknown = null;
  let paginationCursorSerialized: string | null = null;

  try {
    await runInContext(connection, async () => {
      while (pages < MAX_PAGES_PER_INVOCATION) {
        const { query, body } = buildRequest(entity, connection.location_id, paginationCursor);
        // Inject the incremental filter. Shape depends on method:
        //   GET   → query[<cursor_query_param>] = <watermark>
        //   POST  → body.filters = [...existing, { field, operator: "greater_than", value: watermark }]
        // GHL's POST search endpoints use a structured filter array; a
        // top-level body.dateUpdated returns 422 "property should not exist".
        if (entity.backfill.method === "GET") {
          query[entity.incremental.cursor_query_param!] = watermarkBefore;
        } else if (body) {
          const existingFilters = Array.isArray(body.filters) ? body.filters : [];
          // GHL's date fields in POST filters reject `gt` / `gte` as
          // top-level operators and only accept `range` with a nested
          // `{gte, lte}` object. Verified live against /contacts/search:
          //   {field:"dateUpdated", operator:"range", value:{gte:X}}
          // Using `gte: watermarkBefore` is equivalent to a strict-after
          // bound for practical purposes — duplicates (updated exactly
          // at the watermark) dedupe on PK during upsert.
          body.filters = [
            ...existingFilters,
            {
              field: entity.incremental.cursor_query_param,
              operator: "range",
              value: { gte: watermarkBefore },
            },
          ];
        }

        const response = await toplineFetch<Record<string, unknown>>(
          resolveEndpoint(entity.backfill.endpoint, connection.location_id),
          buildFetchOptions(entity, query, body),
        );

        const itemsField = entity.backfill.items_field ?? entity.table;
        const items = (getByPath(response, itemsField) ?? []) as Array<Record<string, unknown>>;
        if (!Array.isArray(items) || items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        const rows = items.map((raw) => mapRow(entity, raw));
        const result: UpsertResult = await stub.upsertRows(entity.table, rows);
        rowsUpserted += result.upserted;
        lastRowCount = result.row_count_after;
        pages += 1;

        const nextCursor = nextCursorFrom(response, items, entity.backfill);
        if (nextCursor === null || nextCursor === undefined) {
          stoppedReason = "empty_page";
          break;
        }
        const nextSerialized = serializeCursor(nextCursor);
        if (nextSerialized === paginationCursorSerialized) {
          stoppedReason = "cursor_stalled";
          break;
        }
        paginationCursor = nextCursor;
        paginationCursorSerialized = nextSerialized;
      }
      if (pages >= MAX_PAGES_PER_INVOCATION) {
        stoppedReason = "page_cap_hit";
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError
      ? `GHL ${err.statusCode}: ${err.message}`
      : err instanceof Error
      ? err.message
      : String(err);
  }

  // Advance the watermark from whatever the DO now reports as MAX.
  let watermarkAfter: string | null = watermarkBefore;
  if (rowsUpserted > 0) {
    try {
      const result = await stub.executeQuery(
        `SELECT MAX(${entity.incremental.cursor_column}) AS w FROM ${entity.table}`,
      );
      const row = result.rows[0] as { w: string | null } | undefined;
      if (row && row.w !== null && row.w !== undefined && String(row.w).length > 0) {
        watermarkAfter = String(row.w);
        if (watermarkAfter !== watermarkBefore) {
          await stub.setSyncWatermark(entity.table, watermarkAfter);
        }
      }
    } catch {
      // non-fatal
    }
  }

  return {
    entity: entity.table,
    connection_id: connectionId,
    location_id: connection.location_id,
    pages,
    rows_upserted: rowsUpserted,
    row_count_after: lastRowCount,
    watermark_before: watermarkBefore,
    watermark_after: watermarkAfter,
    duration_ms: Date.now() - started,
    stopped_reason: stoppedReason,
    ...(errorMsg ? { error: errorMsg } : {}),
  };
}

// ---------------------------------------------------------------------------
// Batch incremental — for the scheduled handler. Runs every entity in
// dependency order, failures isolated per-entity.
// ---------------------------------------------------------------------------

export const DEFAULT_INCREMENTAL_ORDER: readonly string[] = [
  // Metadata + join targets first so FK hints stay valid during the
  // entity pulls that follow.
  "calendar_groups",
  "calendars",
  "tags",
  "custom_fields",
  "custom_values",
  "forms",
  "surveys",
  "pipelines",
  "pipeline_stages",
  "contacts",
  "opportunities",
  // conversations must run BEFORE messages so the active-parent
  // selector in backfillPerParent sees the freshest last_message_date
  // values and fans out to the right conversations.
  "conversations",
  "messages", // dual-writes to call_events
  "form_submissions",
  "survey_submissions",
  "tasks", // per-parent over contacts
  "notes", // per-parent over contacts
];

export async function incrementalAll(
  env: SyncEnv,
  connectionId: string,
  order: readonly string[] = DEFAULT_INCREMENTAL_ORDER,
): Promise<Record<string, IncrementalResult>> {
  const out: Record<string, IncrementalResult> = {};
  for (const table of order) {
    out[table] = await incrementalEntity(env, connectionId, table);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Back-compat: keep the contacts-specific entry point so /sync/backfill
// calls today don't break. Just delegates to the dispatcher.
// ---------------------------------------------------------------------------
export async function backfillContacts(
  env: SyncEnv,
  connectionId: string,
): Promise<BackfillResult> {
  return backfillEntity(env, connectionId, "contacts");
}

// ---------------------------------------------------------------------------
// Batch helper — runs every entity in dependency order for a single
// connection. Failures in one entity don't stop the others.
// ---------------------------------------------------------------------------

/**
 * Back-fill every (currently supportable) entity in the dependency order
 * required by FK hints: pipelines first, then pipeline_stages (both are
 * poll_full off the same response), then contacts, then the opportunity
 * / conversation / message / appointment fan-out.
 */
export const DEFAULT_BACKFILL_ORDER: readonly string[] = [
  "calendar_groups",
  "calendars",
  "tags",
  "custom_fields",
  "custom_values",
  "forms",
  "surveys",
  "pipelines",
  "pipeline_stages",
  "contacts",
  "opportunities",
  "conversations",
  "messages",
  "form_submissions",
  "survey_submissions",
  "tasks",
  "notes",
  "appointments",
];

export async function backfillAll(
  env: SyncEnv,
  connectionId: string,
  order: readonly string[] = DEFAULT_BACKFILL_ORDER,
): Promise<Record<string, BackfillResult>> {
  const out: Record<string, BackfillResult> = {};
  for (const table of order) {
    out[table] = await backfillEntity(env, connectionId, table);
  }
  return out;
}
