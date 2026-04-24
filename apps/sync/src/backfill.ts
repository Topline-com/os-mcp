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
  let stoppedReason: BackfillResult["stopped_reason"] = "empty_page";
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

        const cursorPath = backfill.cursor_response_field;
        const nextCursor = cursorPath ? getByPath(response, cursorPath) : undefined;
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

  // Post-run bookkeeping: if we walked to an empty page, we've seen every
  // row upstream. Capture the max cursor_column value as the watermark and
  // flip backfill_complete so incremental polling can start.
  if (stoppedReason === "empty_page" && entity.incremental.cursor_column) {
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
 * After a complete backfill, compute the max cursor_column value from
 * actual rows in the DO and persist it as the incremental watermark.
 * Then flip backfill_complete. Skipped if cursor_column is missing or
 * MAX returns null (table is empty).
 */
async function setWatermarkAndComplete(
  stub: LocationDOStub,
  entity: EntityManifest,
): Promise<void> {
  const col = entity.incremental.cursor_column;
  if (!col) return;
  try {
    const result = await stub.executeQuery(
      `SELECT MAX(${col}) AS w FROM ${entity.table}`,
    );
    const row = result.rows[0] as { w: string | null } | undefined;
    if (row && row.w !== null && row.w !== undefined && String(row.w).length > 0) {
      await stub.setSyncWatermark(entity.table, String(row.w));
    }
    await stub.markBackfillComplete(entity.table);
  } catch {
    // Non-fatal — watermark will be retried on the next empty_page.
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

  // Cursor placement follows method conventions.
  if (cursorValue !== null && cursorValue !== undefined && backfill.cursor_request_param) {
    if (backfill.method === "GET") {
      // GHL's GET cursor params are scalars. If GHL ever returned an array
      // here, we'd need to JSON.stringify — but for the entities we
      // support (startAfterId for opportunities/conversations), it's a
      // plain string.
      query[backfill.cursor_request_param] = String(cursorValue);
    } else {
      body[backfill.cursor_request_param] = cursorValue;
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
  let stoppedReason: BackfillResult["stopped_reason"] = "empty_page";
  let errorMsg: string | undefined;
  let denormalizedStageRows = 0;

  try {
    await runInContext(connection, async () => {
      const { query, body } = buildRequest(entity, connection.location_id, null);
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
        return;
      }

      if (entity.table === "pipeline_stages") {
        // Denormalize: expand every pipeline's stages[] into stage rows.
        const stagesEntity = entity;
        const stageRows: Array<Record<string, unknown>> = [];
        for (const pipeline of items) {
          const pipelineId = pipeline.id;
          const stages = (pipeline.stages ?? []) as Array<Record<string, unknown>>;
          for (const stage of stages) {
            const row = mapRow(stagesEntity, stage);
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
          rowsUpserted = result.upserted;
          rowCountAfter = result.row_count_after;
          denormalizedStageRows = stageRows.length;
        }
      } else {
        // Straightforward: map each item, upsert.
        const rows = items.map((raw) => {
          const row = mapRow(entity, raw);
          if (row.location_id === null || row.location_id === undefined) {
            row.location_id = connection.location_id;
          }
          return row;
        });
        const result = await stub.upsertRows(entity.table, rows);
        rowsUpserted = result.upserted;
        rowCountAfter = result.row_count_after;
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

  // poll_full tables are always "complete" after a successful run — the
  // whole table's been refreshed in one shot. Mark so the cron knows to
  // just re-invoke (no watermark filter for poll_full).
  if (stoppedReason === "empty_page") {
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
    pages: rowsUpserted > 0 ? 1 : 0,
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

  // Gate: refuse to incremental-sync if backfill isn't complete. The
  // watermark would miss records upstream from the partial-sync point.
  if (!stateRow?.backfill_complete) {
    return {
      entity: entity.table,
      connection_id: connectionId,
      location_id: connection.location_id,
      skipped: "not_complete",
      pages: 0,
      rows_upserted: 0,
      row_count_after: stateRow?.row_count ?? 0,
      watermark_before: stateRow?.watermark ?? null,
      watermark_after: stateRow?.watermark ?? null,
      duration_ms: Date.now() - started,
      stopped_reason: "skipped",
      error: `Initial backfill not complete for ${entity.table}. Run /sync/backfill?entity=${entity.table} until stopped_reason=empty_page.`,
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
          body.filters = [
            ...existingFilters,
            {
              field: entity.incremental.cursor_query_param,
              // GHL's search operators are short codes: gt, gte, lt, lte,
              // eq, contains, etc. Use gt (strict >) since the watermark
              // itself was already captured in the previous run's data.
              operator: "gt",
              value: watermarkBefore,
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

        const cursorPath = entity.backfill.cursor_response_field;
        const nextCursor = cursorPath ? getByPath(response, cursorPath) : undefined;
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
