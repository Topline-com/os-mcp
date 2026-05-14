// Composite SQL-backed MCP tools — single-call equivalents of the
// `topline query doctor | snapshot | audit | freshness` CLI commands.
//
// Goal: an MCP-driven agent should never need to compose its own SQL,
// resolve pipeline names by hand, or chain three reads to get a standard
// pipeline audit. Every command here returns the same JSON shape as the
// Go CLI's `topline --agent query ...` so existing skills and prompts
// port over by renaming the call site.
//
// Naming follows the rest of the server's `topline_` prefix.

import { peekLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { str, obj, num } from "@topline/shared";
import { edgeContext } from "../request-context.js";
import { locationClient } from "../location-do-client.js";

function getClient() {
  const locId = peekLocationId();
  if (!locId) {
    throw new Error(
      "No location in the current request context. This tool requires an authenticated MCP connection with a resolved location_id.",
    );
  }
  const ctx = edgeContext.getStore();
  if (!ctx) {
    throw new Error(
      "Edge request context is not set. This indicates a bug in the worker's request wiring.",
    );
  }
  return locationClient(ctx.location_do, locId);
}

// ---------------------------------------------------------------------------
// Helpers — pipeline resolution, time window parsing, status clause.
// Mirror the Go CLI semantics in internal/commands/query.go so a port from
// `topline --agent query audit` to mcp_topline_topline_pipeline_audit is
// a pure rename, not a behavior change.
// ---------------------------------------------------------------------------

const PIPELINE_ID_PATTERN = /^[A-Za-z0-9]{20}$/;

export interface PipelineResolution {
  input: string;
  matchedId: string;
  matchedName?: string;
  matchedBy: "id" | "name";
}

type SqlClient = ReturnType<typeof getClient>;

async function resolvePipelineID(
  client: SqlClient,
  input: string,
): Promise<PipelineResolution> {
  const trimmed = input.trim();
  if (!trimmed) {
    throw new Error("pipeline is required (id or name)");
  }
  if (PIPELINE_ID_PATTERN.test(trimmed)) {
    return { input: trimmed, matchedId: trimmed, matchedBy: "id" };
  }
  const tokens = trimmed.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    throw new Error("pipeline value is empty after whitespace trim");
  }
  const clauses = tokens.map(() => "LOWER(name) LIKE ?").join(" AND ");
  const params = tokens.map((t) => `%${t}%`);
  const matches = await client.executeQuery(
    `SELECT id, name FROM pipelines WHERE ${clauses} ORDER BY name`,
    params,
  );
  if (matches.rows.length === 1) {
    const row = matches.rows[0] as { id?: string; name?: string };
    return {
      input: trimmed,
      matchedId: String(row.id ?? ""),
      matchedName: String(row.name ?? ""),
      matchedBy: "name",
    };
  }
  if (matches.rows.length === 0) {
    const all = await client.executeQuery("SELECT id, name FROM pipelines ORDER BY name");
    throw new Error(
      `no pipeline matched ${JSON.stringify(trimmed)}. Available pipelines: ${formatPipelineList(all.rows)}`,
    );
  }
  throw new Error(
    `pipeline ${JSON.stringify(trimmed)} is ambiguous (${matches.rows.length} matches). Pass the opaque id or a more specific name. Candidates: ${formatPipelineList(matches.rows)}`,
  );
}

function formatPipelineList(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(none found)";
  return rows
    .map((r) => `${String(r.name ?? "")} (${String(r.id ?? "")})`)
    .join(", ");
}

// Time-window parsing. Mirrors the Go CLI's parseAuditTimeWithNow:
//   - empty/undefined → fallback
//   - "now"           → now
//   - "this-week-et"  → Monday 00:00 America/New_York of the current week
//   - "Nd" / "Nh"     → N days/hours before now (convenience addition)
//   - RFC3339 / YYYY-MM-DD → parsed
export function parseAuditTime(
  value: string | undefined,
  fallback: Date,
  now: Date = new Date(),
): Date {
  const trimmed = (value ?? "").trim();
  if (!trimmed) return fallback;
  const lower = trimmed.toLowerCase();
  if (lower === "now") return new Date(now);
  if (lower === "this-week" || lower === "this-week-et" || lower === "week" || lower === "week-et") {
    return mondayStartInTimezone(now, "America/New_York");
  }
  const relMatch = /^(\d+)\s*([dh])$/.exec(lower);
  if (relMatch) {
    const n = Number(relMatch[1]);
    const unit = relMatch[2];
    const ms = unit === "d" ? n * 86_400_000 : n * 3_600_000;
    return new Date(now.getTime() - ms);
  }
  const rfc = Date.parse(trimmed);
  if (!Number.isNaN(rfc)) return new Date(rfc);
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const [y, m, d] = trimmed.split("-").map(Number);
    return new Date(Date.UTC(y, m - 1, d));
  }
  throw new Error(
    `invalid time ${JSON.stringify(value)}; use YYYY-MM-DD, RFC3339, now, this-week-et, or Nd/Nh`,
  );
}

function mondayStartInTimezone(now: Date, timeZone: string): Date {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    weekday: "short",
    hour12: false,
  }).formatToParts(now);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const weekdayMap: Record<string, number> = {
    Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 7,
  };
  const weekday = weekdayMap[get("weekday")] ?? 1;
  const daysSinceMonday = weekday - 1;
  const year = Number(get("year"));
  const month = Number(get("month"));
  const day = Number(get("day"));
  // Construct the target wall-time (Monday 00:00 in the zone) as if it were
  // UTC, then back out the zone's offset from UTC at that instant. tzOffsetMs
  // returns (zoneWall − utc); to convert a wall instant to true UTC we
  // subtract that offset.
  const wallAsUtc = Date.UTC(year, month - 1, day - daysSinceMonday, 0, 0, 0);
  const offsetMs = tzOffsetMs(new Date(wallAsUtc), timeZone);
  return new Date(wallAsUtc - offsetMs);
}

function tzOffsetMs(at: Date, timeZone: string): number {
  // Compute the offset between UTC and the named zone at the given instant.
  // Intl returns the "local" wall time for the zone; the gap between that
  // wall time interpreted as UTC and the actual UTC instant is the offset.
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = dtf.formatToParts(at);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const asUtc = Date.UTC(
    Number(get("year")),
    Number(get("month")) - 1,
    Number(get("day")),
    Number(get("hour")) === 24 ? 0 : Number(get("hour")),
    Number(get("minute")),
    Number(get("second")),
  );
  return asUtc - at.getTime();
}

function statusClauseSql(column: string, status: string | undefined): { clause: string; params: string[] } {
  const trimmed = (status ?? "").trim();
  if (!trimmed || trimmed.toLowerCase() === "all" || trimmed.toLowerCase() === "any") {
    return { clause: "", params: [] };
  }
  return { clause: ` AND ${column} = ?`, params: [trimmed] };
}

// ---------------------------------------------------------------------------
// Tool: topline_warehouse_freshness
// ---------------------------------------------------------------------------

const freshnessTool: ToolDef = {
  name: "topline_warehouse_freshness",
  description:
    "Per-table sync freshness probe for the warehouse tables that drive pipeline audits. Returns row_count, last_synced_at, and lag_seconds for each tracked table. Call this when the user asks whether warehouse data is current, or when a pipeline audit shows stale numbers and you need to attribute it to sync lag versus coverage gaps. Cheap, no arguments.",
  inputSchema: obj({}, []),
  handler: async () => {
    const client = getClient();
    const result = await client.executeQuery(
      "SELECT table_name, row_count, last_synced_at, lag_seconds FROM warehouse_freshness ORDER BY table_name",
    );
    return {
      columns: result.columns,
      rows: result.rows,
      elapsed_ms: result.elapsed_ms,
    };
  },
};

// ---------------------------------------------------------------------------
// Tool: topline_query_doctor
// ---------------------------------------------------------------------------

const EXPECTED_TABLES = [
  "contacts",
  "opportunities",
  "messages",
  "pipelines",
  "pipeline_stages",
  "call_events",
  "appointments",
  "conversations",
];

const doctorTool: ToolDef = {
  name: "topline_query_doctor",
  description:
    "Deterministic readiness probe for the SQL warehouse surface. Confirms the schema endpoint is reachable, lists which expected tables are present, and reports any coverage gaps as actionable bugs (not as a reason to fall back to REST). Run this once at the start of any standard pipeline audit; if the report is green, proceed to topline_pipeline_audit. Returns { schemaReachable, tableCount, expectedTables, missingTables, recommendation }.",
  inputSchema: obj({}, []),
  handler: async () => {
    const client = getClient();
    const expected: Record<string, boolean> = Object.fromEntries(
      EXPECTED_TABLES.map((t) => [t, false]),
    );
    let schemaReachable = false;
    let schemaError: string | undefined;
    let tableCount = 0;
    try {
      const overview = await client.describeSchema();
      schemaReachable = true;
      const names = extractTableNames(overview);
      tableCount = names.length;
      for (const name of names) {
        if (name in expected) expected[name] = true;
      }
    } catch (err) {
      schemaError = err instanceof Error ? err.message : String(err);
    }
    const missingTables = EXPECTED_TABLES.filter((t) => !expected[t]);
    let recommendation: string;
    if (!schemaReachable) {
      recommendation =
        "SQL endpoint unreachable from this MCP server. Surface the schemaError to the operator; the warehouse may be unprovisioned for this location.";
    } else if (missingTables.length === 0) {
      recommendation =
        "SQL analytics ready. Use topline_pipeline_audit / topline_pipeline_snapshot for standard pipeline questions; topline_execute_query only for non-standard analytics.";
    } else {
      recommendation = `SQL reachable but missing expected tables (${missingTables.join(", ")}). Treat the gap as an os-mcp coverage bug rather than silently falling back to REST.`;
    }
    return {
      schemaReachable,
      schemaError,
      tableCount,
      expectedTables: expected,
      missingTables,
      recommendation,
    };
  },
};

function extractTableNames(overview: unknown): string[] {
  const root = overview as Record<string, unknown> | null;
  if (!root) return [];
  const tables = (root as { tables?: unknown }).tables;
  if (!Array.isArray(tables)) return [];
  const out: string[] = [];
  for (const entry of tables) {
    if (typeof entry === "string") {
      const v = entry.trim();
      if (v) out.push(v);
    } else if (entry && typeof entry === "object") {
      const name = (entry as { name?: unknown }).name;
      if (typeof name === "string" && name.trim()) out.push(name.trim());
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Tool: topline_pipeline_snapshot
// ---------------------------------------------------------------------------

const snapshotTool: ToolDef = {
  name: "topline_pipeline_snapshot",
  description:
    "Open opportunity count, pipeline value, and stage distribution for one pipeline. Single-call equivalent of `topline --agent query snapshot --pipeline ... --status open`. Accepts either an opaque 20-char pipeline ID or a fuzzy pipeline name (e.g. 'flex triage', 'qualified'); on 0 or >1 matches it errors with the candidate list. Use this when the user wants a snapshot without activity/movement detail. Returns { pipelineId, pipelineResolution, status, snapshot }.",
  inputSchema: obj(
    {
      pipeline: str(
        "Pipeline ID (opaque 20-char) or fuzzy name (e.g. 'flex triage', 'qualified').",
      ),
      status: str(
        "Opportunity status filter: 'open' (default), 'won', 'lost', 'abandoned', or 'all'/'any' to skip filtering.",
      ),
    },
    ["pipeline"],
  ),
  handler: async (args) => {
    const client = getClient();
    const pipelineInput = String(args.pipeline ?? "");
    const status = String(args.status ?? "open");
    const resolution = await resolvePipelineID(client, pipelineInput);
    const { clause, params: statusParams } = statusClauseSql("opportunity_status", status);
    const snapshot = await client.executeQuery(
      `SELECT pipeline_id, pipeline_name, pipeline_stage_id, stage_name, stage_position, opportunity_status, ` +
        `opportunity_count, pipeline_value, CAST(avg_days_in_stage AS INTEGER) AS avg_days_in_stage ` +
        `FROM pipeline_snapshot WHERE pipeline_id = ?${clause} ORDER BY stage_position`,
      [resolution.matchedId, ...statusParams],
    );
    return {
      pipelineId: resolution.matchedId,
      pipelineResolution: resolution,
      status,
      snapshot: {
        columns: snapshot.columns,
        rows: snapshot.rows,
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Tool: topline_pipeline_audit  (the marquee tool — replaces 3+ raw SQL calls)
// ---------------------------------------------------------------------------

const auditTool: ToolDef = {
  name: "topline_pipeline_audit",
  description:
    "Standard pipeline activity audit in a single call. Replaces the legacy 3-step flow (describe_schema → resolve pipeline name → hand-write SQL across pipeline_snapshot / pipeline_activity_window / pipeline_movement_window / warehouse_freshness). Accepts an opaque pipeline ID or a fuzzy name ('flex triage', 'qualified'); resolves the name internally and echoes how it matched. Accepts since/until as 'this-week-et' (default since = 7d), 'now', RFC3339, YYYY-MM-DD, or relative shorthand (e.g. '7d', '24h'). Returns { pipelineId, pipelineResolution, window, status, freshness, snapshot, activity, deals, movement } where activity uses unique_touches (deduped against source_id), deals carries per-deal touch breakdowns, and movement is stage/status changes inside the window. PREFER THIS over topline_execute_query for any 'what happened in pipeline X over window W' question — it is faster, cheaper, and prevents the agent from inventing SQL the audit views already cover.",
  inputSchema: obj(
    {
      pipeline: str("Pipeline ID (opaque 20-char) or fuzzy name."),
      since: str(
        "Window start. Default: 7 days ago. Accepts 'this-week-et', 'now', RFC3339, YYYY-MM-DD, or Nd/Nh shorthand.",
      ),
      until: str("Window end. Default: now. Same format as `since`."),
      status: str(
        "Opportunity status filter: 'open' (default), 'won', 'lost', 'abandoned', or 'all'/'any' to skip filtering.",
      ),
      limit: num("Max deals returned in the `deals` section. Default 25, max 100."),
    },
    ["pipeline"],
  ),
  handler: async (args) => {
    const client = getClient();
    const pipelineInput = String(args.pipeline ?? "");
    const status = String(args.status ?? "open");
    const limitRaw = Number(args.limit ?? 25);
    const dealLimit = Math.max(1, Math.min(100, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 25));
    const now = new Date();
    const start = parseAuditTime(
      args.since == null ? undefined : String(args.since),
      new Date(now.getTime() - 7 * 86_400_000),
      now,
    );
    const end = parseAuditTime(
      args.until == null ? undefined : String(args.until),
      now,
      now,
    );
    const since = start.toISOString();
    const until = end.toISOString();
    const resolution = await resolvePipelineID(client, pipelineInput);
    const pipelineId = resolution.matchedId;
    const { clause, params: statusParams } = statusClauseSql("opportunity_status", status);

    const [freshness, snapshot, activity, deals, movement] = await Promise.all([
      client.executeQuery(
        "SELECT table_name, row_count, last_synced_at, lag_seconds FROM warehouse_freshness ORDER BY table_name",
      ),
      client.executeQuery(
        `SELECT pipeline_id, pipeline_name, pipeline_stage_id, stage_name, stage_position, opportunity_status, ` +
          `opportunity_count, pipeline_value, CAST(avg_days_in_stage AS INTEGER) AS avg_days_in_stage ` +
          `FROM pipeline_snapshot WHERE pipeline_id = ?${clause} ORDER BY stage_position`,
        [pipelineId, ...statusParams],
      ),
      client.executeQuery(
        `SELECT activity_class, direction, COUNT(DISTINCT source_id) AS unique_touches, ` +
          `COUNT(DISTINCT opportunity_id) AS opportunities_touched, COUNT(DISTINCT contact_id) AS contacts_touched, ` +
          `MIN(event_at) AS first_touch, MAX(event_at) AS last_touch ` +
          `FROM pipeline_activity_window WHERE pipeline_id = ?${clause} AND event_at >= ? AND event_at <= ? ` +
          `GROUP BY activity_class, direction ORDER BY unique_touches DESC`,
        [pipelineId, ...statusParams, since, until],
      ),
      client.executeQuery(
        `SELECT opportunity_id, opportunity_name, contact_id, pipeline_stage_id, owner_user_id, ROUND(monetary_value, 2) AS monetary_value, ` +
          `COUNT(DISTINCT source_id) AS unique_touches, ` +
          `COUNT(DISTINCT CASE WHEN activity_class = 'message' THEN source_id END) AS message_touches, ` +
          `COUNT(DISTINCT CASE WHEN activity_class = 'call' THEN source_id END) AS call_touches, ` +
          `COUNT(DISTINCT CASE WHEN activity_class = 'appointment' THEN source_id END) AS appointment_touches, ` +
          `COUNT(DISTINCT CASE WHEN direction = 'inbound' THEN source_id END) AS inbound_touches, ` +
          `COUNT(DISTINCT CASE WHEN direction = 'outbound' THEN source_id END) AS outbound_touches, ` +
          `MIN(event_at) AS first_touch, MAX(event_at) AS last_touch ` +
          `FROM pipeline_activity_window WHERE pipeline_id = ?${clause} AND event_at >= ? AND event_at <= ? ` +
          `GROUP BY opportunity_id, opportunity_name, contact_id, pipeline_stage_id, owner_user_id, monetary_value ` +
          `ORDER BY unique_touches DESC, monetary_value DESC LIMIT ?`,
        [pipelineId, ...statusParams, since, until, dealLimit],
      ),
      client.executeQuery(
        `SELECT opportunity_id, opportunity_name, contact_id, pipeline_stage_id, stage_name, opportunity_status, monetary_value, ` +
          `last_movement_at, last_movement_kind ` +
          `FROM pipeline_movement_window WHERE pipeline_id = ?${clause} AND last_movement_at >= ? AND last_movement_at <= ? ` +
          `ORDER BY last_movement_at DESC`,
        [pipelineId, ...statusParams, since, until],
      ),
    ]);

    return {
      pipelineId,
      pipelineResolution: resolution,
      window: { since, until },
      status,
      freshness: { columns: freshness.columns, rows: freshness.rows },
      snapshot: { columns: snapshot.columns, rows: snapshot.rows },
      activity: { columns: activity.columns, rows: activity.rows },
      deals: { columns: deals.columns, rows: deals.rows },
      movement: { columns: movement.columns, rows: movement.rows },
    };
  },
};

export const tools: ToolDef[] = [doctorTool, freshnessTool, snapshotTool, auditTool];
