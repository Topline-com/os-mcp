/// <reference types="@cloudflare/workers-types" />

// Topline OS Sync Worker.
//
// Separate from the edge worker so backfill / polling traffic never shares
// the same isolate as customer-facing MCP requests. Reads encrypted
// connection records from the shared CONNECTIONS KV namespace, calls GHL
// via the shared toplineFetch client, and writes rows into each tenant's
// LocationDO via cross-worker DO binding.
//
// v1 surface: admin-triggered HTTP endpoints only. Cron schedules +
// webhook queue consumer come in phase 1 step 3b. Every route is gated
// by Authorization: Bearer <ADMIN_TOKEN>.

import {
  backfillEntity,
  backfillAll,
  DEFAULT_BACKFILL_ORDER,
  incrementalEntity,
  incrementalAll,
  type SyncEnv,
  type BackfillResult,
  type IncrementalResult,
} from "./backfill.js";

interface Env extends SyncEnv {
  ADMIN_TOKEN?: string;
  TOPLINE_BRAND_NAME?: string;
}

export default {
  /**
   * Cron trigger — fires on the schedule in wrangler.toml (every 15 min).
   * Enumerates every connection in the CONNECTIONS KV and runs
   * incrementalAll on each. Per-entity failures are isolated; one
   * tenant's bad day doesn't stop the rest.
   *
   * Logging only — no response is visible. Tail with `wrangler tail
   * topline-os-sync` to watch a run.
   */
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(runIncrementalForAllConnections(env));
  },

  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.ADMIN_TOKEN) {
      return plain(503, "ADMIN_TOKEN not configured. Run: wrangler secret put ADMIN_TOKEN");
    }
    if (!env.TOKEN_SIGNING_SECRET) {
      return plain(503, "TOKEN_SIGNING_SECRET not configured. Must match the edge worker's.");
    }

    const authHeader = request.headers.get("Authorization") ?? "";
    const bearer = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!bearer || bearer !== env.ADMIN_TOKEN) {
      return plain(401, "Invalid or missing Authorization: Bearer <ADMIN_TOKEN>");
    }

    const url = new URL(request.url);
    switch (url.pathname) {
      case "/":
        return plain(200, `Topline OS sync worker. Admin-gated endpoints only.`);
      case "/sync/backfill":
        return handleBackfill(request, env);
      case "/sync/backfill-all":
        return handleBackfillAll(request, env);
      case "/sync/incremental":
        return handleIncremental(request, env);
      case "/sync/incremental-all":
        return handleIncrementalAll(request, env);
      case "/sync/clear-cursor":
        return handleClearCursor(request, env);
      default:
        return plain(404, "Not found");
    }
  },
};

// ---------------------------------------------------------------------------
// POST /sync/incremental?connection_id=<uuid>&entity=<table>
//
// Fetches only records with cursor_column > watermark from GHL and upserts
// them. Refuses with `skipped: "not_complete"` if the entity's initial
// backfill hasn't finished — see index.ts handleBackfill for that flow.
// ---------------------------------------------------------------------------
async function handleIncremental(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  const entity = url.searchParams.get("entity") ?? "";
  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");
  if (!entity) return plain(400, "Missing ?entity=<table>");

  let result: IncrementalResult;
  try {
    result = await incrementalEntity(env, connectionId, entity);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json(500, { error: message });
  }
  return json(200, result);
}

// ---------------------------------------------------------------------------
// POST /sync/incremental-all?connection_id=<uuid>
// Same as /sync/backfill-all but runs the incremental path for each entity.
// Failures per-entity don't stop the others.
// ---------------------------------------------------------------------------
async function handleIncrementalAll(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");

  try {
    const results = await incrementalAll(env, connectionId);
    return json(200, { results });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json(500, { error: message });
  }
}

// ---------------------------------------------------------------------------
// POST /sync/backfill-all?connection_id=<uuid>
//
// Runs every entity in dependency order (pipelines first, then
// pipeline_stages, then contacts / opportunities / conversations /
// messages / appointments). One entity's failure does not stop the
// others — each result is reported independently. The caller
// inspects the per-entity payload to see what succeeded.
// ---------------------------------------------------------------------------
async function handleBackfillAll(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");

  try {
    const results = await backfillAll(env, connectionId);
    return json(200, { order: DEFAULT_BACKFILL_ORDER, results });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json(500, { error: message });
  }
}

// ---------------------------------------------------------------------------
// POST /sync/clear-cursor?connection_id=<uuid>&entity=contacts
// Resets the resume pointer so the next backfill starts from page 0.
// Leaves the DO's actual rows untouched — upserts will no-op on rows that
// already match.
// ---------------------------------------------------------------------------
async function handleClearCursor(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  const entity = url.searchParams.get("entity") ?? "";
  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");
  if (!entity) return plain(400, "Missing ?entity=<table>");

  const { loadAndDecryptConnection } = await import("@topline/shared-auth");
  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) return json(404, { error: `Unknown connection: ${connectionId}` });

  const doId = env.LOCATION_DO.idFromName(connection.location_id);
  const stub = env.LOCATION_DO.get(doId);
  await stub.clearSyncCursor(entity);
  return json(200, { cleared: entity, location_id: connection.location_id });
}

// ---------------------------------------------------------------------------
// POST /sync/backfill?connection_id=<uuid>&entity=<table>
//
// Accepts any entity declared in @topline/shared-schema. Unsupported sync
// modes (per_parent, poll_full, pagination=unknown) return a 200 with
// stopped_reason: "unsupported" rather than a 4xx — callers batching over
// many entities keep going, and the result payload explains why.
// ---------------------------------------------------------------------------
async function handleBackfill(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  const entity = url.searchParams.get("entity") ?? "contacts";

  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");

  let result: BackfillResult;
  try {
    result = await backfillEntity(env, connectionId, entity);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json(500, { error: message });
  }

  // 200 even if the backfill hit GHL auth / rate-limit errors — the result
  // payload carries the error detail. A 500 is reserved for infra failures
  // (missing connection, DO binding broken, etc.) where retrying won't help.
  return json(200, result);
}

// ---------------------------------------------------------------------------
// Scheduled entry — enumerates CONNECTIONS KV and runs incrementalAll for
// each one. Sequential today because our customer count is small; when
// it grows we'll fan out via Cloudflare Queues.
// ---------------------------------------------------------------------------
async function runIncrementalForAllConnections(env: Env): Promise<void> {
  let cursor: string | undefined = undefined;
  let totalConnections = 0;
  const runStartedAt = new Date().toISOString();

  while (true) {
    const list: KVNamespaceListResult<unknown, string> = await env.CONNECTIONS.list({ cursor });
    for (const entry of list.keys) {
      totalConnections += 1;
      try {
        const results = await incrementalAll(env, entry.name);
        // Log a compact one-line summary per entity for observability.
        for (const [table, r] of Object.entries(results)) {
          console.log(
            `[sync/cron ${runStartedAt}] connection=${entry.name} entity=${table} ` +
              `stopped=${r.stopped_reason} rows_upserted=${r.rows_upserted} ` +
              `row_count=${r.row_count_after}` +
              (r.error ? ` error=${r.error.slice(0, 120)}` : ""),
          );
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.log(
          `[sync/cron ${runStartedAt}] connection=${entry.name} FATAL ${msg.slice(0, 200)}`,
        );
      }
    }
    if (list.list_complete) break;
    cursor = list.cursor;
  }

  console.log(
    `[sync/cron ${runStartedAt}] completed, processed ${totalConnections} connections`,
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function plain(status: number, body: string): Response {
  return new Response(body, { status, headers: { "Content-Type": "text/plain" } });
}
function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
