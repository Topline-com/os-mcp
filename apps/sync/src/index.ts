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

import { backfillContacts, type SyncEnv, type BackfillResult } from "./backfill.js";

interface Env extends SyncEnv {
  ADMIN_TOKEN?: string;
  TOPLINE_BRAND_NAME?: string;
}

export default {
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
      default:
        return plain(404, "Not found");
    }
  },
};

// ---------------------------------------------------------------------------
// POST /sync/backfill?connection_id=<uuid>&entity=contacts
// ---------------------------------------------------------------------------
async function handleBackfill(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return plain(405, "Method not allowed");
  const url = new URL(request.url);
  const connectionId = url.searchParams.get("connection_id") ?? "";
  const entity = url.searchParams.get("entity") ?? "contacts";

  if (!connectionId) return plain(400, "Missing ?connection_id=<uuid>");
  if (entity !== "contacts") {
    return plain(400, `Entity "${entity}" not yet supported. MVP covers: contacts.`);
  }

  let result: BackfillResult;
  try {
    result = await backfillContacts(env, connectionId);
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
