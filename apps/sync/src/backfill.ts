// Backfill orchestrator — phase 1 MVP scope: contacts only.
//
// Pulls every contact from a given location via GHL's paginated
// /contacts/search (POST with body.searchAfter cursor), maps each page
// into column-named rows, upserts into the tenant's LocationDO via RPC.
// Stops when the cursor stops advancing or the response returns zero
// rows.
//
// The sync worker holds no permanent state of its own — every call
// loads the connection record from KV, decrypts the PIT, and runs
// against GHL. Cursor progress is persisted in the LocationDO's
// `_sync_state` table so a retry or scheduled re-run resumes from the
// right place.

import {
  loadAndDecryptConnection,
  type DecryptedConnection,
} from "@topline/shared-auth";
import { toplineFetch, credentialsContext, ToplineApiError } from "@topline/shared";
import { ENTITY_BY_TABLE } from "@topline/shared-schema";
import type { LocationDO, UpsertResult } from "@topline/shared-do";
import { mapRow } from "./mapping.js";

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
  stopped_reason: "empty_page" | "cursor_stalled" | "error";
  error?: string;
}

// Upper bound on pages for a single invocation. Sync workers have a
// ~30s CPU budget per request; 200 pages × ~100 rows = 20k rows is well
// within that for the MVP. Real backfills for huge accounts will
// resume-on-cursor across multiple invocations.
const MAX_PAGES_PER_INVOCATION = 200;
const PAGE_LIMIT = 100;

export async function backfillContacts(
  env: SyncEnv,
  connectionId: string,
): Promise<BackfillResult> {
  const started = Date.now();
  const entity = ENTITY_BY_TABLE.get("contacts");
  if (!entity) throw new Error("contacts entity missing from manifest — impossible");

  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) {
    throw new Error(`Unknown or revoked connection: ${connectionId}`);
  }

  const doId = env.LOCATION_DO.idFromName(connection.location_id);
  const stub = env.LOCATION_DO.get(doId);

  // Resume from last recorded cursor if present.
  const priorState = await stub.getSyncState();
  let cursor: string | undefined = priorState.contacts?.cursor ?? undefined;

  let pages = 0;
  let rowsUpserted = 0;
  let lastRowCount = priorState.contacts?.row_count ?? 0;
  let stoppedReason: BackfillResult["stopped_reason"] = "empty_page";
  let errorMsg: string | undefined;

  // Run every toplineFetch inside the credentialsContext so the shared
  // client picks up this connection's PIT + location without touching
  // process.env (we're in a Worker — there is no process.env to set).
  try {
    await runInContext(connection, async () => {
      while (pages < MAX_PAGES_PER_INVOCATION) {
        const body: Record<string, unknown> = {
          locationId: connection.location_id,
          pageLimit: PAGE_LIMIT,
        };
        if (cursor) body.searchAfter = [cursor];

        const response = await toplineFetch<{
          contacts?: Array<Record<string, unknown>>;
          meta?: { searchAfter?: string | string[] };
        }>("/contacts/search", { method: "POST", body });

        const items = response.contacts ?? [];
        if (items.length === 0) {
          stoppedReason = "empty_page";
          break;
        }

        const rows = items.map((raw) => mapRow(entity, raw));
        const result: UpsertResult = await stub.upsertRows("contacts", rows);
        rowsUpserted += result.upserted;
        lastRowCount = result.row_count_after;
        pages += 1;

        // Advance the cursor. GHL's /contacts/search returns searchAfter
        // in meta; the shape can vary (string or array), defensively
        // handle both. If the cursor doesn't advance, we're done.
        const nextCursor = extractCursor(response.meta?.searchAfter, items);
        if (!nextCursor || nextCursor === cursor) {
          stoppedReason = "cursor_stalled";
          break;
        }
        cursor = nextCursor;
        await stub.setSyncCursor("contacts", nextCursor);
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
    entity: "contacts",
    connection_id: connectionId,
    location_id: connection.location_id,
    pages,
    rows_upserted: rowsUpserted,
    row_count_after: lastRowCount,
    final_cursor: cursor ?? null,
    duration_ms: Date.now() - started,
    stopped_reason: stoppedReason,
    ...(errorMsg ? { error: errorMsg } : {}),
  };
}

function runInContext<T>(
  connection: DecryptedConnection,
  fn: () => Promise<T>,
): Promise<T> {
  return credentialsContext.run(
    { pit: connection.pit, locationId: connection.location_id },
    fn,
  );
}

/**
 * Extract the next searchAfter cursor. GHL returns either a single
 * string or an array depending on the endpoint version. Fall back to
 * the last item's `id` — /contacts/search pagination is keyset, and
 * searchAfter echoes the previous page's last row.
 */
function extractCursor(
  metaCursor: string | string[] | undefined,
  items: Array<Record<string, unknown>>,
): string | null {
  if (Array.isArray(metaCursor)) {
    return metaCursor[0] ?? null;
  }
  if (typeof metaCursor === "string" && metaCursor.length > 0) {
    return metaCursor;
  }
  const last = items[items.length - 1];
  if (last && typeof last.id === "string") return last.id;
  return null;
}
