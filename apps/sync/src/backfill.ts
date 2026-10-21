// Manifest-driven sync. Every public entry point claims a durable tenant lease;
// data, cursor and reconciliation writes carry its fence through completion.

import { type DecryptedConnection } from "@topline/shared-auth";
import {
  toplineFetch as upstreamFetch,
  credentialsContext,
  safeLog,
  ToplineApiError,
  type ToplineFetchOptions,
} from "@topline/shared";
import { ENTITY_BY_TABLE, type EntityManifest } from "@topline/shared-schema";
import type { LocationDO, UpsertResult, QueryResult, SyncState } from "@topline/shared-do";
import { mapRow, getByPath } from "./mapping.js";
import { loadAuthorizedSyncConnection } from "./connection-auth.js";
import { coordinateSync, fencedStub, sweepDueCutoff, renewCurrentLease, type LeaseStore } from "./coordination.js";
import { responseItems, validateSinglePage, validateTerminalPage, mappedRows } from "./response-validation.js";
import { nextCursorFrom } from "./cursor.js";

async function toplineFetch<T>(path: string, options: ToplineFetchOptions): Promise<T> {
  await renewCurrentLease();
  return upstreamFetch<T>(path, { ...options, retryRateLimits: false });
}

/**
 * Minimal RPC surface of LocationDO that sync actually uses. Declaring it
 * here (rather than relying on DurableObjectNamespace<LocationDO> to
 * project method types through the RPC boundary) sidesteps an issue where
 * workers-types' generic projection collapses the method return types to
 * `never` in downstream consumers.
 */
type LocationDOStub = Omit<LeaseStore, "getSyncState"> & {
  executeQuery(sql: string, params?: readonly unknown[], rowCap?: number): Promise<QueryResult>;
  upsertRows(
    table: string,
    rows: ReadonlyArray<Record<string, unknown>>,
  ): Promise<UpsertResult>;
  getSyncState(): Promise<SyncState>;
  setSyncCursor(entity: string, cursor: string): Promise<void>;
  clearSyncCursor(entity: string): Promise<void>;
  completeSyncEntity(entity: string, watermark?: string): Promise<void>;
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
  getSweepParentsForChild(
    childEntity: string,
    parentTable: string,
    limit: number,
    checkedBefore?: string,
  ): Promise<string[]>;
  getPendingParentCount(entity: string, parentTable: string): Promise<number>;
  recordParentFailure(entity: string, parentId: string, retryAfterMs?: number): Promise<void>;
  reconcileAppointmentSnapshot(locationId: string, parentId: string, rows: ReadonlyArray<Record<string, unknown>>): Promise<UpsertResult & { unchanged: number; missing_retained: number }>;
  getParentSyncCursor(entity: string, parentId: string): Promise<string | null>;
  setParentSyncCursor(entity: string, parentId: string, cursor: string): Promise<void>;
  ensureParentDrainMarker(entity: string, parentId: string): Promise<string>;
  markParentBackfillComplete(
    entity: string,
    parentId: string,
    childTable: string,
    fkColumn: string,
    derivedTables?: ReadonlyArray<{ table: string; fk: string }>,
  ): Promise<number>;
};

function locationStub(
  ns: DurableObjectNamespace<LocationDO>,
  locationId: string,
): LocationDOStub {
  return fencedStub(ns.get(ns.idFromName(locationId)) as unknown as LocationDOStub, locationId);
}

export interface SyncEnv {
  TOKEN_SIGNING_SECRET: string;
  CONNECTIONS: KVNamespace;
  CONNECTION_AUTH_DO: DurableObjectNamespace;
  LOCATION_DO: DurableObjectNamespace<LocationDO>;
}

export interface BackfillResult {
  entity: string;
  connection_id: string;
  location_id: string;
  pages: number;
  rows_upserted: number;
  /** Actual data/index writes from upsertRows and appointment snapshots; excludes pruning and bookkeeping. */
  data_rows_written?: number;
  unchanged_rows?: number;
  missing_retained?: number;
  pending_parents?: number;
  row_count_after: number;
  final_cursor: string | null;
  duration_ms: number;
  stopped_reason:
    | "empty_page"
    | "cursor_stalled"
    | "page_cap_hit"
    | "error"
    | "unsupported"
    | "skipped";
  skipped?: "not_complete" | "unsupported_mode" | "no_watermark_source" | "busy" | "not_due";
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
  /**
   * Per-parent failures observed during a per_parent backfill. Only
   * present when at least one parent threw — populated even when
   * `stopped_reason` is not "error" (i.e. some parents succeeded and
   * some failed). This is what surfaces a stuck calendar in cron logs
   * instead of silently halting the whole tick on the first failure.
   *
   * Omitted from the response when empty.
   */
  failed_parents?: Array<{ parent_id: string; error: string }>;
}

// Leave room for lease renewals, parent state, derived writes and completion
// under the upstream + DO subrequest budget. Large drains resume next tick.
const MAX_PAGES_PER_INVOCATION = 100;
const DEFAULT_PAGE_LIMIT = 100;

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
async function backfillEntityUnleased(
  env: SyncEnv,
  connectionId: string,
  entityTable: string,
): Promise<BackfillResult> {
  const started = Date.now();
  const entity = ENTITY_BY_TABLE.get(entityTable);
  if (!entity) {
    return unsupportedResult(started, entityTable, connectionId, "", `Unknown entity: ${entityTable}`);
  }

  const connection = await loadAuthorizedSyncConnection(env, connectionId);

  // Refuse entities with unverified contracts. The manifest flags these
  // as pagination: "unknown" precisely so the sync worker can bail
  // instead of inventing a backfill pattern.
  if (entity.backfill.pagination === "unknown") {
    return unsupportedResult(
      started,
      entityTable,
      connectionId,
      connection.location_id,
      `Backfill contract for ${entityTable} is marked "unknown" in the manifest. Verify against the live CRM before syncing.`,
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

/** Both admin and cron entry points claim the same tenant-wide fenced lease. */
async function runEntity(
  env: SyncEnv,
  connectionId: string,
  entityTable: string,
  mode: "backfill" | "incremental",
): Promise<BackfillResult & { watermark_before?: string | null; watermark_after?: string | null }> {
  const started = Date.now();
  const entity = ENTITY_BY_TABLE.get(entityTable);
  if (!entity) return unsupportedResult(started, entityTable, connectionId, "", "Unknown entity");
  const connection = await loadAuthorizedSyncConnection(env, connectionId);
  const stub = locationStub(env.LOCATION_DO, connection.location_id);
  const result = await coordinateSync<BackfillResult | IncrementalResult>(stub, connection.location_id, entityTable, entity.incremental.poll_interval_minutes,
    async () => {
      if (mode === "backfill" && entity.incremental.type === "updated_after") {
        const state = (await stub.getSyncState())[entityTable];
        if (state?.backfill_complete) {
          // Keep a partial incremental cursor paired with its original filter.
          if (state.cursor || state.drain_started_at) return incrementalEntityUnleased(env, connectionId, entityTable);
          // A new manual full drain must remain a full drain on the next cron.
          await stub.resetBackfill(entityTable);
        }
      }
      return mode === "backfill"
        ? backfillEntityUnleased(env, connectionId, entityTable)
        : incrementalEntityUnleased(env, connectionId, entityTable);
    });
  if (result.value) return {
    ...result.value, final_cursor: "final_cursor" in result.value ? result.value.final_cursor : null,
    data_rows_written: result.rows_written, unchanged_rows: result.unchanged_rows, missing_retained: result.missing_retained,
  };
  const state = (await stub.getSyncState())[entityTable];
  return {
    entity: entityTable, connection_id: connectionId, location_id: connection.location_id,
    pages: 0, rows_upserted: 0, data_rows_written: 0, row_count_after: state?.row_count ?? 0,
    final_cursor: null, duration_ms: Date.now() - started,
    stopped_reason: "skipped", skipped: result.skipped,
  };
}

export async function backfillEntity(env: SyncEnv, connectionId: string, entity: string): Promise<BackfillResult> {
  return runEntity(env, connectionId, entity, "backfill");
}

export async function incrementalEntity(env: SyncEnv, connectionId: string, entity: string): Promise<IncrementalResult> {
  const result = await runEntity(env, connectionId, entity, "incremental");
  return { watermark_before: null, watermark_after: null, ...result,
    stopped_reason: result.stopped_reason === "unsupported" ? "error" : result.stopped_reason };
}

/** Resetting a cursor is also a sync mutation and cannot race an active drain. */
export async function clearEntityCursor(env: SyncEnv, connectionId: string, entity: string): Promise<boolean> {
  if (!ENTITY_BY_TABLE.has(entity)) throw new Error("Unknown entity");
  const connection = await loadAuthorizedSyncConnection(env, connectionId);
  const stub = locationStub(env.LOCATION_DO, connection.location_id);
  const result = await coordinateSync(stub, connection.location_id, entity, 0, async () => {
    await stub.resetBackfill(entity);
    return { stopped_reason: "skipped" };
  }, true);
  return !result.skipped;
}

// ---------------------------------------------------------------------------
// Standard cursor backfill — GET or POST, cursor in query or body
// ---------------------------------------------------------------------------

/**
 * Paginate an entity whose backfill is a single endpoint with a cursor.
 *
 * Method matters: GET puts the cursor in the query string, POST puts it in
 * the body. Cursor shape is whatever the CRM returns — we round-trip via JSON
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
  const seenCursors = new Set<string>(priorCursor ? [priorCursor] : []);

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
  // cleared only when we reach a valid end-of-stream. Rows that weren't
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
          await stub.setUpstreamTotal(entity.table, upstreamTotal);
        }

        const itemsField = backfill.items_field ?? entity.table;
        const items = responseItems(response, itemsField);
        if (entity.backfill.pagination === "none") validateSinglePage(response, items.length);
        if (items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        // First page of a fresh drain → stamp drain marker so the
        // end-of-drain prune can delete rows that disappeared upstream.
        if (pages === 0 && drainStartedAt === null) {
          drainStartedAt = await stub.startDrainIfUnset(entity.table);
        }

        const rows = mappedRows(entity, items, connection.location_id);
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
        if (seenCursors.has(nextSerialized)) {
          stoppedReason = "cursor_stalled";
          break;
        }
        seenCursors.add(nextSerialized);
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
    errorMsg = err instanceof ToplineApiError ? `Upstream request failed (${err.statusCode})` : "Sync failed; progress retained for retry.";
  }

  // Only verified end-of-stream completes a drain. A stalled cursor retains
  // the drain and its rows for recovery; it must never authorize pruning.
  if (stoppedReason === "empty_page") {
    if (drainStartedAt !== null) {
      prunedStale = await stub.pruneStaleRows(entity.table, drainStartedAt);
      if (prunedStale > 0) {
        console.log(`[backfill:${entity.table}] pruned ${prunedStale} stale rows (deleted upstream)`);
        lastRowCount = Math.max(0, lastRowCount - prunedStale);
      }
    }
    await setWatermarkAndComplete(stub, entity);
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
 * Complete a full drain with one fenced storage transaction: clear the
 * cursor and drain marker, advance the watermark, and mark backfill complete.
 * A cursor must never survive with a different incremental filter watermark.
 * Without a cursor_column, preserve the prior watermark while completing.
 */
async function setWatermarkAndComplete(stub: LocationDOStub, entity: EntityManifest): Promise<void> {
  let watermark: string | undefined;
  if (entity.incremental.cursor_column) {
    const result = await stub.executeQuery(`SELECT MAX(${entity.incremental.cursor_column}) AS w FROM ${entity.table}`);
    if (result.truncated) throw new Error("Watermark query truncated");
    const value = result.rows[0]?.w;
    if (value != null && String(value)) watermark = String(value);
  }
  await stub.completeSyncEntity(entity.table, watermark);
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
  // for POST (the CRM's POST search endpoints expect filter params in body).
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
  // as a query/body key — the CRM's shape-(b) endpoints reject extra
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
 * the manifest's endpoint string. Some CRM endpoints take location as
 * a path segment (`/locations/{locationId}/tags`) rather than a query
 * param; this keeps the manifest declarative for both shapes.
 */
function resolveEndpoint(template: string, locationId: string): string {
  return template.replace("{locationId}", locationId);
}

/**
 * Extract the upstream row-count estimate from a CRM list response.
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
// callEventFromMessage — normalize a raw CRM message into a call_events row.
//
// the CRM's call fields are inconsistently placed across endpoints:
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
// the CRM's cursor shapes vary per endpoint; these helpers normalize the mess:
//
//   extractNextCursor  — pull the next-cursor record out of a response
//                         (from `meta` or from the last item of items[]).
//                         Returns a map of { request_param → value } that
//                         buildRequest will inject into the next request,
//                         or null at a valid terminal page. Incomplete
//                         cursor metadata throws and retains progress.
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
// per_parent — durable retry state and finite checked-parent sweeps.
const MAX_PARENTS_PER_INVOCATION = 100;

async function backfillPerParent(
  env: SyncEnv,
  connection: DecryptedConnection,
  entity: EntityManifest,
  started: number,
  connectionId: string,
): Promise<BackfillResult> {
  const { backfill } = entity;
  const fanout = backfill.per_parent;
  const parent = fanout && ENTITY_BY_TABLE.get(fanout.parent_entity);
  if (!fanout || !parent) return unsupportedResult(started, entity.table, connectionId, connection.location_id, "Missing parent descriptor");
  if (backfill.pagination === "cursor" && (!backfill.cursor_request_param || !backfill.cursor_response_field)) {
    return unsupportedResult(started, entity.table, connectionId, connection.location_id, "Missing parent cursor descriptor");
  }
  const usesPathTemplate = backfill.endpoint.includes("{parent}");
  if (!usesPathTemplate && !fanout.request_param) {
    return unsupportedResult(started, entity.table, connectionId, connection.location_id, "Missing parent request parameter");
  }
  const stub = locationStub(env.LOCATION_DO, connection.location_id);
  const prior = (await stub.getSyncState())[entity.table];
  // A durable cycle boundary lets a sweep larger than the invocation budget
  // resume without revisiting already checked parents or starting its interval.
  const cycleStart = await stub.startDrainIfUnset(entity.table);
  const sweep = fanout.steady_state_sweep === true;
  // A delayed failure keeps cycleStart alive, but must not freeze parents
  // already checked in an earlier cadence bucket. Oldest-first selection
  // still prioritizes never-checked parents when a sweep spans invocations.
  const dueCutoff = sweepDueCutoff(cycleStart, entity.incremental.poll_interval_minutes);
  const select = (limit: number, cutoff = dueCutoff) => sweep
    ? stub.getSweepParentsForChild(entity.table, parent.table, limit, cutoff)
    : stub.getNextParentsForChild(entity.table, parent.table, limit, parentFreshnessColumns(parent));
  const parentIds = await select(MAX_PARENTS_PER_INVOCATION);
  let pages = 0;
  let rowsUpserted = 0;
  let pruned = 0;
  let rowCount = prior?.row_count ?? 0;
  let stop: BackfillResult["stopped_reason"] = "empty_page";
  const failedParents: NonNullable<BackfillResult["failed_parents"]> = [];

  await runInContext(connection, async () => {
    for (const parentId of parentIds) {
      if (pages >= MAX_PAGES_PER_INVOCATION) { stop = "page_cap_hit"; break; }
      try {
        const priorCursor = backfill.pagination === "cursor" ? await stub.getParentSyncCursor(entity.table, parentId) : null;
        let cursor: unknown = priorCursor ? parseCursor(priorCursor) : null;
        const seenCursors = new Set<string>(priorCursor ? [priorCursor] : []);
        if (entity.table !== "appointments") await stub.ensureParentDrainMarker(entity.table, parentId);
        let complete = false;
        while (pages < MAX_PAGES_PER_INVOCATION) {
          const query: NonNullable<ToplineFetchOptions["query"]> = { ...backfill.query_extras };
          if (backfill.pagination === "cursor") query.limit = DEFAULT_PAGE_LIMIT;
          if (!usesPathTemplate && !backfill.endpoint.includes("{locationId}")) {
            query[backfill.location_param_name ?? "locationId"] = connection.location_id;
          }
          if (cursor != null && backfill.cursor_request_param) query[backfill.cursor_request_param] = String(cursor);
          if (fanout.request_param) query[fanout.request_param] = parentId;
          const endpoint = resolveEndpoint(backfill.endpoint.replace("{parent}", encodeURIComponent(parentId)), connection.location_id);
          const response = await toplineFetch<Record<string, unknown>>(endpoint, { method: backfill.method, query });
          const items = responseItems(response, backfill.items_field ?? entity.table);
          if (backfill.pagination === "none") validateSinglePage(response, items.length);
          const rows = mappedRows(entity, items, connection.location_id, { column: fanout.parent_fk_column, id: parentId });
          pages += 1;
          if (entity.table === "appointments") {
            // List omission is not proof of deletion. This atomic RPC retains
            // missing history and leaves identical appointment rows untouched.
            const result = await stub.reconcileAppointmentSnapshot(connection.location_id, parentId, rows);
            rowsUpserted += result.upserted;
            rowCount = result.row_count_after;
            complete = true;
            break;
          }
          if (rows.length) {
            const result = await stub.upsertRows(entity.table, rows);
            rowsUpserted += result.upserted;
            rowCount = result.row_count_after;
            if (entity.table === "messages") {
              const calls = items.map((raw) => callEventFromMessage(raw, connection.location_id, parentId)).filter((row): row is Record<string, unknown> => row !== null);
              if (calls.length) await stub.upsertRows("call_events", calls);
            }
          }
          if (!items.length || backfill.pagination !== "cursor") { complete = true; break; }
          const next = nextCursorFrom(response, items, backfill);
          if (next == null) { complete = true; break; }
          const serialized = serializeCursor(next);
          if (seenCursors.has(serialized)) throw new Error("Upstream cursor stalled");
          seenCursors.add(serialized);
          cursor = next;
          await stub.setParentSyncCursor(entity.table, parentId, serialized);
        }
        if (complete && entity.table !== "appointments") {
          pruned += await stub.markParentBackfillComplete(entity.table, parentId, entity.table, fanout.parent_fk_column,
            entity.table === "messages" ? [{ table: "call_events", fk: "conversation_id" }] : []);
        }
        if (!complete) { stop = "page_cap_hit"; break; }
      } catch (error) {
        // Invalid credentials belong to this connection, not to shared parent
        // state. Stop immediately so another authorized connection can retry.
        if (error instanceof ToplineApiError && error.statusCode === 401) throw error;
        // Store only retry metadata. Payloads, credentials and upstream error
        // bodies never enter logs or durable state. A 403 can be parent scoped;
        // it follows the same bounded backoff as other parent request failures.
        await stub.recordParentFailure(entity.table, parentId, error instanceof ToplineApiError ? error.retryAfterMs : undefined);
        failedParents.push({ parent_id: parentId, error: error instanceof ToplineApiError ? `Upstream request failed (${error.statusCode})` : "Parent sync failed; retained for retry." });
      }
    }
  });

  const pending = await stub.getPendingParentCount(entity.table, parent.table);
  // Completion is about the original cycle, independently of additional
  // healthy-parent refreshes that became due while a retry was delayed.
  const remaining = (await select(1, cycleStart)).length;
  if (failedParents.length) stop = "error";
  else if (pending || remaining) stop = "page_cap_hit";
  else {
    stop = "empty_page";
    await stub.markBackfillComplete(entity.table);
    await stub.clearDrainMarker(entity.table);
  }
  rowCount = (await stub.getSyncState())[entity.table]?.row_count ?? rowCount;
  if (failedParents.length) safeLog("warn", "per_parent_sync_failures", { failed_parent_count: failedParents.length, parents_attempted: parentIds.length });
  return {
    entity: entity.table, connection_id: connectionId, location_id: connection.location_id,
    pages, rows_upserted: rowsUpserted, row_count_after: rowCount,
    final_cursor: null, duration_ms: Date.now() - started, stopped_reason: stop,
    pending_parents: pending,
    ...(pruned ? { pruned_stale_rows: pruned } : {}),
    ...(failedParents.length ? { failed_parents: failedParents, error: "Parent retries pending." } : {}),
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
  rowCountAfter = priorStateRow?.row_count ?? 0;
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
    await stub.resetBackfill(entity.table);
    priorStateRow.cursor = null;
    isFresh = true;
  }

  const resumeCursor = priorStateRow?.cursor ? parseCursor(priorStateRow.cursor) : null;
  const maxPagesThisRun = isFresh
    ? MAX_PAGES_PER_INVOCATION
    : backfill.pagination === "cursor"
    ? MAX_PAGES_PER_INVOCATION
    : 1;

  // Steady-state watermark: lets the page loop stop the moment it's
  // caught up to what we already have. Resolved once per invocation so
  // concurrent upserts during the drain don't move the goalpost.
  let steadyWatermark: string | null = priorStateRow?.watermark ?? null;
  if (!isFresh && steadyWatermark === null && entity.incremental.cursor_column && backfill.pagination === "cursor") {
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
    if (steadyWatermark !== null) await stub.setSyncWatermark(entity.table, steadyWatermark);
  }

  // Stamp a drain marker on the first page of a fresh drain. The return
  // value survives across multi-tick drains so snapshot-and-swap at the
  // end knows exactly which rows were refreshed during THIS drain.
  const drainStartedAt = await stub.startDrainIfUnset(entity.table);
  try {
    await runInContext(connection, async () => {
      let cursorValue: unknown = resumeCursor;
      let cursorSerialized: string | null = resumeCursor ? serializeCursor(resumeCursor) : null;
      const seenCursors = new Set<string>(cursorSerialized ? [cursorSerialized] : []);
      let caughtUpToWatermark = false;

      while (pagesProcessed < maxPagesThisRun) {
        const { query, body } = buildRequest(entity, connection.location_id, cursorValue);
        const response = await toplineFetch<Record<string, unknown>>(
          resolveEndpoint(backfill.endpoint, connection.location_id),
          buildFetchOptions(entity, query, body),
        );

        // the CRM returns `total` on the top-level of many list responses.
        // Persist whichever one we see so the next run's drift check has
        // something to compare against.
        const upstreamTotal = extractUpstreamTotal(response);
        if (upstreamTotal !== null) {
          await stub.setUpstreamTotal(entity.table, upstreamTotal);
        }

        // Primary table: resolve items_field (or default to entity.table).
        // Pipeline's primary items live at "pipelines"; pipeline_stages
        // items are denormalized from pipelines[].stages[] (see below).
        const itemsField =
          entity.table === "pipeline_stages" ? "pipelines" : backfill.items_field ?? entity.table;
        const items = responseItems(response, itemsField);
        if (entity.backfill.pagination === "none") validateSinglePage(response, items.length);
        if (items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        if (entity.table === "pipeline_stages") {
          // Denormalize: expand every pipeline's stages[] into stage rows.
          const stageRows: Array<Record<string, unknown>> = [];
          for (const pipeline of items) {
            const pipelineId = pipeline.id;
            const stages = responseItems(pipeline, "stages");
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
          const rows = mappedRows(entity, items, connection.location_id);
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
        if (seenCursors.has(nextSerialized)) {
          // This endpoint's live-tested EOF echoes the current compound cursor.
          // An earlier cursor in a cycle, or an advertised next page, is not EOF.
          if (entity.table === "opportunities" && backfill.endpoint === "/opportunities/search" && nextSerialized === cursorSerialized) {
            validateTerminalPage(response);
            stoppedReason = "empty_page";
          } else {
            stoppedReason = "cursor_stalled";
          }
          break;
        }
        seenCursors.add(nextSerialized);
        cursorValue = nextCursor;
        cursorSerialized = nextSerialized;
        // Persist resume cursor on every page so a subrequest-cap error
        // leaves the next invocation in a usable resume position.
        await stub.setSyncCursor(entity.table, nextSerialized);
      }
      if (pagesProcessed >= maxPagesThisRun && backfill.pagination === "cursor" && !caughtUpToWatermark) {
        stoppedReason = "page_cap_hit";
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError ? `Upstream request failed (${err.statusCode})` : "Sync failed; progress retained for retry.";
  }

  // A steady refresh uses the same durable cursor but never prunes history.
  // Advance its watermark only after the entire refresh completes.
  if (stoppedReason === "empty_page") {
    if (isFresh && drainStartedAt !== null) {
      prunedStale = await stub.pruneStaleRows(entity.table, drainStartedAt);
      rowCountAfter = Math.max(0, rowCountAfter - prunedStale);
    }
    await setWatermarkAndComplete(stub, entity);
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
// Per-parent entities use the same bounded, resumable drain as initial sync.
// ---------------------------------------------------------------------------

export interface IncrementalResult {
  entity: string;
  connection_id: string;
  location_id: string;
  skipped?: "not_complete" | "unsupported_mode" | "no_watermark_source" | "busy" | "not_due";
  pages: number;
  rows_upserted: number;
  /** Actual data/index writes from upsertRows and appointment snapshots; excludes pruning and bookkeeping. */
  data_rows_written?: number;
  unchanged_rows?: number;
  missing_retained?: number;
  pending_parents?: number;
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
  /**
   * Forwarded from the underlying per_parent BackfillResult. Present
   * only when at least one parent threw. Omitted otherwise.
   */
  failed_parents?: Array<{ parent_id: string; error: string }>;
}

async function incrementalEntityUnleased(
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

  const connection = await loadAuthorizedSyncConnection(env, connectionId);

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
      pending_parents: res.pending_parents,
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
    const res = await backfillEntityUnleased(env, connectionId, entityTable);
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
      pending_parents: res.pending_parents,
      stopped_reason: stopped,
      ...(res.error ? { error: res.error } : {}),
      ...(res.failed_parents && res.failed_parents.length > 0
        ? { failed_parents: res.failed_parents }
        : {}),
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
    const res = await backfillEntityUnleased(env, connectionId, entityTable);
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
      pending_parents: res.pending_parents,
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

  // Filter contract not yet verified for this entity. the CRM's filter grammar
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
      error: `Entity ${entity.table} has filter_ready: false. Its incremental filter contract against the CRM has not been verified; stick to manual /sync/backfill until the manifest entry is updated.`,
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
  let paginationCursor: unknown = stateRow.cursor ? parseCursor(stateRow.cursor) : null;
  let paginationCursorSerialized: string | null = stateRow.cursor;
  await stub.startDrainIfUnset(entity.table);
  const seenCursors = new Set<string>(paginationCursorSerialized ? [paginationCursorSerialized] : []);

  try {
    await runInContext(connection, async () => {
      while (pages < MAX_PAGES_PER_INVOCATION) {
        const { query, body } = buildRequest(entity, connection.location_id, paginationCursor);
        // Inject the incremental filter. Shape depends on method:
        //   GET   → query[<cursor_query_param>] = <watermark>
        //   POST  → body.filters = [...existing, { field, operator: "greater_than", value: watermark }]
        // the CRM's POST search endpoints use a structured filter array; a
        // top-level body.dateUpdated returns 422 "property should not exist".
        if (entity.backfill.method === "GET") {
          query[entity.incremental.cursor_query_param!] = watermarkBefore;
        } else if (body) {
          const existingFilters = Array.isArray(body.filters) ? body.filters : [];
          // the CRM's date fields in POST filters reject `gt` / `gte` as
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
        const items = responseItems(response, itemsField);
        if (entity.backfill.pagination === "none") validateSinglePage(response, items.length);
        if (items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        const rows = mappedRows(entity, items, connection.location_id);
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
        if (seenCursors.has(nextSerialized)) {
          stoppedReason = "cursor_stalled";
          break;
        }
        seenCursors.add(nextSerialized);
        paginationCursor = nextCursor;
        paginationCursorSerialized = nextSerialized;
        await stub.setSyncCursor(entity.table, nextSerialized);
      }
      if (pages >= MAX_PAGES_PER_INVOCATION) {
        stoppedReason = "page_cap_hit";
      }
    });
  } catch (err) {
    stoppedReason = "error";
    errorMsg = err instanceof ToplineApiError ? `Upstream request failed (${err.statusCode})` : "Sync failed; progress retained for retry.";
  }

  let watermarkAfter = watermarkBefore;
  if (stoppedReason === "empty_page") {
    const result = await stub.executeQuery(`SELECT MAX(${entity.incremental.cursor_column}) AS w FROM ${entity.table}`);
    if (result.truncated) throw new Error("Watermark query truncated");
    const watermark = result.rows[0]?.w;
    if (watermark != null && String(watermark)) {
      watermarkAfter = String(watermark);
    }
    await stub.completeSyncEntity(entity.table, watermarkAfter);
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
  "workflows",
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
  "appointments", // per-parent over calendars, sweep mode
];

export async function incrementalAll(
  env: SyncEnv,
  connectionId: string,
  order: readonly string[] = DEFAULT_INCREMENTAL_ORDER,
): Promise<Record<string, IncrementalResult>> {
  const out: Record<string, IncrementalResult> = {};
  for (const table of order) {
    try {
      out[table] = await incrementalEntity(env, connectionId, table);
    } catch {
      out[table] = { ...failedEntity(table, connectionId), watermark_before: null, watermark_after: null };
    }
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
  "workflows",
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
    try {
      out[table] = await backfillEntity(env, connectionId, table);
    } catch {
      out[table] = failedEntity(table, connectionId);
    }
  }
  return out;
}

function failedEntity(entity: string, connectionId: string): BackfillResult & { stopped_reason: "error" } {
  return { entity, connection_id: connectionId, location_id: "", pages: 0, rows_upserted: 0,
    row_count_after: 0, final_cursor: null, duration_ms: 0,
    stopped_reason: "error", error: "Entity sync failed; progress retained for retry." };
}
