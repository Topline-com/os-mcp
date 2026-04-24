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
// = ~120 subrequests leaves plenty of headroom.
//
// For initial backfill of a tenant with hundreds of parents, callers
// repeatedly hit /sync/backfill?entity=messages; a future resume-state
// implementation (track "last parent processed") will make this
// automatic. Until then each invocation processes the next batch based
// on which children are missing.
const MAX_PARENTS_PER_INVOCATION = 20;

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
  let lastRowCount = priorState[entity.table]?.row_count ?? 0;
  // Widening cast — inner assignments live inside an async closure that
  // TS flow analysis doesn't peek into, so without the cast the post-try
  // narrowed type collapses to just "empty_page" | "error" and the
  // "cursor_stalled" check below becomes "unintentional".
  let stoppedReason = "empty_page" as BackfillResult["stopped_reason"];
  let errorMsg: string | undefined;

  try {
    await runInContext(connection, async () => {
      while (pages < MAX_PAGES_PER_INVOCATION) {
        const { query, body } = buildRequest(entity, connection.location_id, cursorValue);
        const response = await toplineFetch<Record<string, unknown>>(
          backfill.endpoint,
          buildFetchOptions(entity, query, body),
        );

        const itemsField = backfill.items_field ?? entity.table;
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
  // cursor_stalled), we've seen every row upstream. Capture the max
  // cursor_column value as the watermark and flip backfill_complete so
  // incremental polling can start. cursor_stalled is a legitimate EOF
  // signal when GHL echoes the last-row-id back on the final page.
  if (
    (stoppedReason === "empty_page" || stoppedReason === "cursor_stalled") &&
    entity.incremental.cursor_column
  ) {
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

  // Location always scopes the request. The parameter name defaults to
  // "locationId" (camelCase) but opportunities etc. use snake_case — the
  // manifest overrides per-entity via location_param_name.
  const locParamName = backfill.location_param_name ?? "locationId";
  if (backfill.method === "GET") {
    query[locParamName] = locationId;
  } else {
    body[locParamName] = locationId;
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

function parseCursor(stored: string): unknown {
  try {
    return JSON.parse(stored);
  } catch {
    return stored;
  }
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
// per_parent — messages. Iterates every parent row (conversations) and
// paginates its children.
// ---------------------------------------------------------------------------

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
  if (!backfill.cursor_request_param || !backfill.cursor_response_field) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent entity ${entity.table} is missing cursor metadata.`,
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

  const stub = locationStub(env.LOCATION_DO, connection.location_id);

  // Pull parent IDs that DON'T yet have any children in this DO. This
  // lets repeated invocations make progress without re-walking already-
  // processed parents. Ordered so iteration is deterministic across runs.
  const fkColumn = backfill.per_parent.parent_fk_column;
  const parentQuery = await stub.executeQuery(
    `SELECT p.id
     FROM ${parentEntity.table} p
     LEFT JOIN ${entity.table} c ON c.${fkColumn} = p.id
     WHERE c.id IS NULL
     GROUP BY p.id
     ORDER BY p.id
     LIMIT ${MAX_PARENTS_PER_INVOCATION}`,
    [],
  );
  if (parentQuery.rows.length === 0) {
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
      error: `No unprocessed parents remaining in ${parentEntity.table}. Either all are synced or the parent table is empty — check ${parentEntity.table} first.`,
    };
  }

  // Endpoint template uses {parent} as placeholder.
  const endpointTemplate = backfill.endpoint;
  if (!endpointTemplate.includes("{parent}")) {
    return unsupportedResult(
      started,
      entity.table,
      connectionId,
      connection.location_id,
      `per_parent endpoint ${endpointTemplate} must contain {parent}.`,
    );
  }

  let totalPages = 0;
  let rowsUpserted = 0;
  let lastRowCount = 0;
  let stoppedReason: BackfillResult["stopped_reason"] = "empty_page";
  let errorMsg: string | undefined;

  try {
    await runInContext(connection, async () => {
      parentLoop: for (const parentRow of parentQuery.rows) {
        const parentId = String(parentRow.id);
        let cursorValue: unknown = null;
        let cursorSerialized: string | null = null;
        let pagesForParent = 0;

        while (pagesForParent < MAX_PAGES_PER_INVOCATION) {
          if (totalPages >= MAX_PAGES_PER_INVOCATION) {
            stoppedReason = "page_cap_hit";
            break parentLoop;
          }

          const query: Record<string, string | number | boolean | undefined> = {
            limit: DEFAULT_PAGE_LIMIT,
          };
          if (backfill.query_extras) {
            for (const [k, v] of Object.entries(backfill.query_extras)) {
              query[k] = v;
            }
          }
          if (cursorValue !== null && cursorValue !== undefined) {
            query[backfill.cursor_request_param!] = String(cursorValue);
          }
          const endpoint = endpointTemplate.replace("{parent}", parentId);

          const response = await toplineFetch<Record<string, unknown>>(endpoint, {
            method: backfill.method,
            query,
          });

          const itemsField = backfill.items_field ?? entity.table;
          const items = (getByPath(response, itemsField) ?? []) as Array<Record<string, unknown>>;
          if (!Array.isArray(items) || items.length === 0) break;

          // Denormalize: stamp the parent FK onto each row before mapping.
          const rows = items.map((raw) => {
            const row = mapRow(entity, raw);
            if (row[fkColumn] === null || row[fkColumn] === undefined) {
              row[fkColumn] = parentId;
            }
            return row;
          });
          const result: UpsertResult = await stub.upsertRows(entity.table, rows);
          rowsUpserted += result.upserted;
          lastRowCount = result.row_count_after;
          totalPages += 1;
          pagesForParent += 1;

          const nextCursor = getByPath(response, backfill.cursor_response_field!);
          if (nextCursor === null || nextCursor === undefined) break;
          const nextSerialized = serializeCursor(nextCursor);
          if (nextSerialized === cursorSerialized) break;
          cursorValue = nextCursor;
          cursorSerialized = nextSerialized;
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
  // Initializer uses a widening annotation cast — the inner assignments
  // happen inside an async closure (runInContext callback) that TS's
  // flow analysis doesn't peek into, so without the cast the post-try
  // narrowed type would be just "empty_page" | "error".
  let stoppedReason = "empty_page" as BackfillResult["stopped_reason"];
  let errorMsg: string | undefined;
  let denormalizedStageRows = 0;

  // Bi-modal poll_full:
  //
  //   backfill_complete = false  — initial drain. Resume from the
  //     persisted cursor so a 14k-row table can work through the
  //     Cloudflare 1000-subrequest-per-invocation cap across many
  //     cron ticks. Without this, every tick re-fetches page 1 and
  //     the tail of the table is never reached.
  //
  //   backfill_complete = true   — steady-state freshness. Fetch only
  //     page 1 (newest rows) and upsert. GHL's /conversations/search
  //     and /opportunities/search both return newest-first, so page 1
  //     naturally carries every freshly-updated row. Cheaper than
  //     re-walking the entire history every 15 min.
  //
  // Pipelines / pipeline_stages hit the backfill_complete branch after
  // their first (single-page) run and stay fresh off page 1, same as
  // conversations. Their small row counts make either mode equivalent.
  const priorState = await stub.getSyncState();
  const priorStateRow = priorState[entity.table];
  const isFresh = !priorStateRow?.backfill_complete;
  const resumeCursor = isFresh && priorStateRow?.cursor ? parseCursor(priorStateRow.cursor) : null;
  const maxPagesThisRun = isFresh ? MAX_PAGES_PER_INVOCATION : 1;

  try {
    await runInContext(connection, async () => {
      let cursorValue: unknown = resumeCursor;
      let cursorSerialized: string | null = resumeCursor ? serializeCursor(resumeCursor) : null;

      while (pagesProcessed < maxPagesThisRun) {
        const { query, body } = buildRequest(entity, connection.location_id, cursorValue);
        const response = await toplineFetch<Record<string, unknown>>(
          backfill.endpoint,
          buildFetchOptions(entity, query, body),
        );

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
        }
        pagesProcessed += 1;

        // Single-page endpoints (pagination: "none") → we're done.
        // Cursor-paginated endpoints → route through nextCursorFrom,
        // which handles both structured multi-field cursors and legacy
        // scalar cursor_response_field configs.
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

  // End-of-stream markers. Only mark the backfill complete when we've
  // actually walked every page (empty_page or cursor_stalled). page_cap_hit
  // and error leave backfill_complete untouched so the NEXT run resumes.
  // Once complete, steady-state freshness runs (maxPagesThisRun=1) don't
  // need to re-flip this.
  if (isFresh && (stoppedReason === "empty_page" || stoppedReason === "cursor_stalled")) {
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
      error: `Incremental sync for ${entity.incremental.type} entities not implemented; use webhooks (phase 1 step 5).`,
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
          entity.backfill.endpoint,
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
  "pipelines",
  "pipeline_stages",
  "contacts",
  "opportunities",
  "conversations",
  // messages: webhooks handle freshness; incremental stub returns skipped
  "messages",
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
  "pipelines",
  "pipeline_stages",
  "contacts",
  "opportunities",
  "conversations",
  "messages",
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
