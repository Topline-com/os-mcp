// Scheduled entry — enumerates CONNECTIONS KV and runs incrementalAll for
// each one. Sequential today because our customer count is small; when
// it grows we'll fan out via Cloudflare Queues.

import { safeErrorFields, safeId, safeLog } from "@topline/shared";
import { incrementalAll, type IncrementalResult, type SyncEnv } from "./backfill.js";
import { loadAuthorizedSyncConnection, UnsendablePitError } from "./connection-auth.js";

export interface CronDependencies {
  incrementalAll(env: SyncEnv, connectionId: string): Promise<Record<string, IncrementalResult>>;
}

export async function runIncrementalForAllConnections(
  env: SyncEnv,
  dependencies: CronDependencies = { incrementalAll },
): Promise<void> {
  let cursor: string | undefined = undefined;
  let totalConnections = 0;
  let skippedConnections = 0;

  while (true) {
    const list: KVNamespaceListResult<unknown, string> = await env.CONNECTIONS.list({ cursor });
    for (const entry of list.keys) {
      totalConnections += 1;
      if (await skipsUnsendablePit(env, entry.name)) {
        skippedConnections += 1;
        continue;
      }
      try {
        const results = await dependencies.incrementalAll(env, entry.name);
        // Log a compact one-line summary per entity for observability.
        for (const [table, r] of Object.entries(results)) {
          safeLog("log", `sync_entity_${table}`, {
            rows_upserted: r.rows_upserted,
            data_rows_written: r.data_rows_written ?? null,
            unchanged_rows: r.unchanged_rows ?? null,
            missing_retained: r.missing_retained ?? null,
            pending_parents: r.pending_parents ?? 0,
            skipped: Boolean(r.skipped),
            row_count: r.row_count_after,
            failed_parent_count: r.failed_parents?.length ?? 0,
            has_error: Boolean(r.error),
          });
        }
      } catch (err) {
        safeLog("error", "sync_connection_failed", { error: safeErrorFields(err) });
      }
    }
    if (list.list_complete) break;
    cursor = list.cursor;
  }

  safeLog("log", "sync_cron_completed", {
    connection_count: totalConnections,
    skipped_connection_count: skippedConnections,
  });
}

/**
 * Skip a connection whose stored PIT can't become an Authorization header,
 * before incrementalAll builds one. One line per skipped connection per run,
 * with its connection ID and Location ID so it can be found and reconnected,
 * and never any part of the PIT. Every other load failure (a revoked or
 * missing connection) goes on to incrementalAll exactly as before.
 */
async function skipsUnsendablePit(env: SyncEnv, connectionId: string): Promise<boolean> {
  try {
    await loadAuthorizedSyncConnection(env, connectionId);
    return false;
  } catch (error) {
    if (!(error instanceof UnsendablePitError)) return false;
    safeLog("warn", "sync_connection_skipped_invalid_pit", {
      connection_id: safeId(connectionId),
      location_id: safeId(error.locationId),
    });
    return true;
  }
}
