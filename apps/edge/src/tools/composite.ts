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

import { peekLocationId, toplineFetch, getLocationId } from "@topline/shared";
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

// ---------------------------------------------------------------------------
// Tool: topline_contact_audit
// ---------------------------------------------------------------------------
//
// Single-call replacement for the legacy "search contacts → get contact → list
// opportunities → search conversations → get messages → list tasks → list notes"
// REST fan-out. Resolves a fuzzy contact (name/email/phone) or accepts an opaque
// contact_id, then returns the full warehouse picture in one shot.
//
// Tasks and notes are REST-only (not in the warehouse). Agents that need those
// drill down with the typed REST tools after this audit; we deliberately avoid
// fan-out inside this composite so the SQL surface stays the answer surface.

export interface ContactResolution {
  input: string;
  matchedId: string;
  matchedBy: "id" | "email" | "phone" | "name";
  candidates?: Array<{ id: string; name: string; email?: string; phone?: string }>;
}

const CONTACT_ID_PATTERN = /^[A-Za-z0-9]{20,32}$/;
const EMAIL_PATTERN = /@/;
const PHONE_PATTERN = /^[+\d][\d\s().\-]{5,}$/;

function normalizePhone(input: string): string {
  return input.replace(/[^\d+]/g, "");
}

async function resolveContactID(
  client: SqlClient,
  input: string,
): Promise<ContactResolution> {
  const trimmed = input.trim();
  if (!trimmed) {
    throw new Error("contact is required (id, email, phone, or name)");
  }
  // Path 1: looks like an opaque contact id.
  if (CONTACT_ID_PATTERN.test(trimmed) && !EMAIL_PATTERN.test(trimmed)) {
    const exists = await client.executeQuery(
      "SELECT id FROM contacts WHERE id = ? LIMIT 1",
      [trimmed],
    );
    if (exists.rows.length === 1) {
      return { input: trimmed, matchedId: trimmed, matchedBy: "id" };
    }
    // Fall through — id-shaped but not found, treat as a search string.
  }
  // Path 2: email.
  if (EMAIL_PATTERN.test(trimmed)) {
    const matches = await client.executeQuery(
      "SELECT id, full_name, first_name, last_name, email, phone FROM contacts WHERE LOWER(email) = LOWER(?) ORDER BY updated_at DESC LIMIT 25",
      [trimmed],
    );
    if (matches.rows.length === 1) {
      const row = matches.rows[0] as Record<string, unknown>;
      return { input: trimmed, matchedId: String(row.id ?? ""), matchedBy: "email" };
    }
    if (matches.rows.length === 0) {
      throw new Error(`no contact matched email ${JSON.stringify(trimmed)}`);
    }
    throw new Error(
      `email ${JSON.stringify(trimmed)} is ambiguous (${matches.rows.length} contacts). Pass the opaque id. Candidates: ${formatContactCandidates(matches.rows)}`,
    );
  }
  // Path 3: phone.
  if (PHONE_PATTERN.test(trimmed)) {
    const normalized = normalizePhone(trimmed);
    if (normalized.length >= 7) {
      const matches = await client.executeQuery(
        // Match against the suffix to handle E.164 vs. local-format mismatches.
        "SELECT id, full_name, first_name, last_name, email, phone FROM contacts WHERE phone IS NOT NULL AND REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '(', ''), ')', ''), '.', '') LIKE ? ORDER BY updated_at DESC LIMIT 25",
        [`%${normalized}%`],
      );
      if (matches.rows.length === 1) {
        const row = matches.rows[0] as Record<string, unknown>;
        return { input: trimmed, matchedId: String(row.id ?? ""), matchedBy: "phone" };
      }
      if (matches.rows.length === 0) {
        throw new Error(`no contact matched phone ${JSON.stringify(trimmed)}`);
      }
      throw new Error(
        `phone ${JSON.stringify(trimmed)} is ambiguous (${matches.rows.length} contacts). Pass the opaque id. Candidates: ${formatContactCandidates(matches.rows)}`,
      );
    }
  }
  // Path 4: fuzzy name. Tokenize and AND-match across first/last/full/company.
  const tokens = trimmed.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    throw new Error("contact value is empty after whitespace trim");
  }
  const clauses = tokens
    .map(
      () =>
        "(LOWER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'') || ' ' || COALESCE(full_name,'') || ' ' || COALESCE(company_name,'') || ' ' || COALESCE(email,'')) LIKE ?)",
    )
    .join(" AND ");
  const params = tokens.map((t) => `%${t}%`);
  const matches = await client.executeQuery(
    `SELECT id, full_name, first_name, last_name, email, phone, company_name, updated_at FROM contacts WHERE ${clauses} ORDER BY updated_at DESC LIMIT 25`,
    params,
  );
  if (matches.rows.length === 1) {
    const row = matches.rows[0] as Record<string, unknown>;
    return { input: trimmed, matchedId: String(row.id ?? ""), matchedBy: "name" };
  }
  if (matches.rows.length === 0) {
    throw new Error(
      `no contact matched ${JSON.stringify(trimmed)}. Try the contact's email, phone, or full name.`,
    );
  }
  throw new Error(
    `contact ${JSON.stringify(trimmed)} is ambiguous (${matches.rows.length} matches). Pass the opaque id or a more specific name/email. Candidates: ${formatContactCandidates(matches.rows)}`,
  );
}

function formatContactCandidates(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(none found)";
  return rows
    .slice(0, 10)
    .map((r) => {
      const id = String(r.id ?? "");
      const name = String(
        r.full_name ??
          (`${r.first_name ?? ""} ${r.last_name ?? ""}`.trim() ||
            r.company_name ||
            "(no name)"),
      );
      const email = r.email ? ` <${r.email}>` : "";
      return `${name}${email} (${id})`;
    })
    .join(", ");
}

const contactAuditTool: ToolDef = {
  name: "topline_contact_audit",
  description:
    "Standard contact activity audit in a single call. Replaces the legacy fan-out of search_contacts → get_contact → search_opportunities → search_conversations → get_messages. Accepts an opaque contact ID OR a fuzzy identifier (email like 'jane@acme.com', phone like '+15551234567', or name like 'Jane Smith'); resolves it internally and echoes how it matched. Accepts since/until as 'this-week-et', 'now', RFC3339, YYYY-MM-DD, or relative shorthand (e.g. '90d', '24h'). Default since = 90d, until = now. Returns { contact, contactResolution, window, freshness, opportunities, activity, timeline, messages } where opportunities lists all deals for the contact with pipeline_name/stage_name/value/status (via opportunity_funnel), activity is a counts rollup by event_kind+direction over the window, timeline is per-event rows from contact_timeline within the window (messages + opportunities-created + appointments), and messages is the last N raw message bodies for context. PREFER THIS over topline_execute_query, topline_search_contacts, topline_search_conversations, or topline_get_messages for any 'what's going on with contact X' / 'what did Y say this month' / 'give me a brief on Z' question — it is faster, cheaper, and prevents the agent from inventing SQL the warehouse views already cover. Tasks and notes are NOT included (they are REST-only); drill down with topline_list_contact_tasks / topline_list_contact_notes if the user asks.",
  inputSchema: obj(
    {
      contact: str(
        "Contact identifier. Accepts opaque contact ID, email (jane@acme.com), phone (+15551234567 or local-format), or fuzzy name (e.g. 'Jane Smith', 'Acme Corp').",
      ),
      since: str(
        "Window start. Default: 90 days ago. Accepts 'this-week-et', 'now', RFC3339, YYYY-MM-DD, or Nd/Nh shorthand.",
      ),
      until: str("Window end. Default: now. Same format as `since`."),
      message_limit: num(
        "Max raw messages returned in the `messages` section. Default 20, max 100.",
      ),
      timeline_limit: num(
        "Max timeline events returned. Default 50, max 200.",
      ),
    },
    ["contact"],
  ),
  handler: async (args) => {
    const client = getClient();
    const contactInput = String(args.contact ?? "");
    const messageLimitRaw = Number(args.message_limit ?? 20);
    const messageLimit = Math.max(
      1,
      Math.min(100, Number.isFinite(messageLimitRaw) ? Math.trunc(messageLimitRaw) : 20),
    );
    const timelineLimitRaw = Number(args.timeline_limit ?? 50);
    const timelineLimit = Math.max(
      1,
      Math.min(200, Number.isFinite(timelineLimitRaw) ? Math.trunc(timelineLimitRaw) : 50),
    );
    const now = new Date();
    const start = parseAuditTime(
      args.since == null ? undefined : String(args.since),
      new Date(now.getTime() - 90 * 86_400_000),
      now,
    );
    const end = parseAuditTime(
      args.until == null ? undefined : String(args.until),
      now,
      now,
    );
    const since = start.toISOString();
    const until = end.toISOString();
    const resolution = await resolveContactID(client, contactInput);
    const contactId = resolution.matchedId;

    const [contact, freshness, opportunities, activity, timeline, messages] = await Promise.all([
      client.executeQuery(
        `SELECT id, first_name, last_name, full_name, email, phone, company_name, source, ` +
          `assigned_to, type, dnd, timezone, tags, city, state, country, ` +
          `created_at, updated_at ` +
          `FROM contacts WHERE id = ? LIMIT 1`,
        [contactId],
      ),
      client.executeQuery(
        "SELECT table_name, row_count, last_synced_at, lag_seconds FROM warehouse_freshness ORDER BY table_name",
      ),
      client.executeQuery(
        `SELECT id AS opportunity_id, name AS opportunity_name, pipeline_id, pipeline_name, ` +
          `pipeline_stage_id, stage_name, status, ROUND(COALESCE(monetary_value, 0), 2) AS monetary_value, ` +
          `assigned_user_id, source, created_at, updated_at, last_status_change_at, last_stage_change_at, ` +
          `CAST(days_since_created AS INTEGER) AS days_since_created, ` +
          `CAST(days_in_current_stage AS INTEGER) AS days_in_current_stage ` +
          `FROM opportunity_funnel WHERE contact_id = ? ORDER BY updated_at DESC LIMIT 100`,
        [contactId],
      ),
      client.executeQuery(
        `SELECT event_kind, direction, COUNT(*) AS event_count, ` +
          `COUNT(DISTINCT source_id) AS unique_events, ` +
          `MIN(event_at) AS first_event, MAX(event_at) AS last_event ` +
          `FROM contact_timeline WHERE contact_id = ? AND event_at >= ? AND event_at <= ? ` +
          `GROUP BY event_kind, direction ORDER BY event_count DESC`,
        [contactId, since, until],
      ),
      client.executeQuery(
        `SELECT event_kind, event_subtype, direction, event_at, ` +
          `SUBSTR(COALESCE(summary, ''), 1, 240) AS summary, source_id, user_id ` +
          `FROM contact_timeline WHERE contact_id = ? AND event_at >= ? AND event_at <= ? ` +
          `ORDER BY event_at DESC LIMIT ?`,
        [contactId, since, until, timelineLimit],
      ),
      client.executeQuery(
        `SELECT id AS message_id, type, direction, date_added AS event_at, body, user_id ` +
          `FROM messages WHERE contact_id = ? AND date_added >= ? AND date_added <= ? ` +
          `ORDER BY date_added DESC LIMIT ?`,
        [contactId, since, until, messageLimit],
      ),
    ]);

    const contactRow =
      contact.rows.length === 1 ? (contact.rows[0] as Record<string, unknown>) : null;

    return {
      contactId,
      contactResolution: resolution,
      window: { since, until },
      contact: contactRow,
      freshness: { columns: freshness.columns, rows: freshness.rows },
      opportunities: { columns: opportunities.columns, rows: opportunities.rows },
      activity: { columns: activity.columns, rows: activity.rows },
      timeline: { columns: timeline.columns, rows: timeline.rows },
      messages: { columns: messages.columns, rows: messages.rows },
    };
  },
};

// ---------------------------------------------------------------------------
// Tool: topline_owner_audit
// ---------------------------------------------------------------------------
//
// Owner-scoped activity audit in a single call. Replaces the fan-out of
// list_users → search_opportunities(assignedTo=...) → execute_query on
// pipeline_activity_window for THEIR touches. Two ownership lenses exist
// and they diverge in real life — surface both:
//
//   1. Book  = opportunities.assigned_to    (their deals, regardless of toucher)
//   2. Touch = messages.user_id / call_events.user_id (touches they logged)
//
// The skill should treat these as distinct questions. "Is Joey caught up on
// his book?" = lens 1. "How many calls did Joey make this week?" = lens 2.

export interface OwnerResolution {
  input: string;
  matchedId: string;
  matchedBy: "id" | "email" | "name";
  user?: { id: string; firstName?: string; lastName?: string; email?: string };
  candidates?: Array<{ id: string; name: string; email?: string }>;
}

const USER_ID_PATTERN = /^[A-Za-z0-9]{20,32}$/;

interface UserApiRecord {
  id: string;
  firstName?: string;
  lastName?: string;
  email?: string;
  name?: string;
}

function userDisplayName(u: UserApiRecord): string {
  const explicit = (u.name ?? "").trim();
  if (explicit) return explicit;
  const composed = `${u.firstName ?? ""} ${u.lastName ?? ""}`.trim();
  return composed || u.email || "(no name)";
}

async function fetchAllUsers(): Promise<UserApiRecord[]> {
  const locationId = getLocationId(undefined);
  const res = await toplineFetch<{ users?: UserApiRecord[] }>("/users/", {
    query: { locationId },
  });
  const users = Array.isArray(res?.users) ? res.users : [];
  return users.filter((u): u is UserApiRecord => Boolean(u?.id));
}

async function resolveOwner(input: string): Promise<OwnerResolution> {
  const trimmed = input.trim();
  if (!trimmed) {
    throw new Error("owner is required (user_id, email, or name)");
  }
  // Path 1: opaque user_id — fetch directly to validate.
  if (USER_ID_PATTERN.test(trimmed) && !trimmed.includes("@") && !trimmed.includes(" ")) {
    try {
      const res = await toplineFetch<{ user?: UserApiRecord }>(`/users/${trimmed}`);
      const user = res?.user;
      if (user?.id) {
        return {
          input: trimmed,
          matchedId: user.id,
          matchedBy: "id",
          user: {
            id: user.id,
            firstName: user.firstName,
            lastName: user.lastName,
            email: user.email,
          },
        };
      }
    } catch {
      // fall through to name/email lookup
    }
  }
  const all = await fetchAllUsers();
  // Path 2: email exact match.
  if (trimmed.includes("@")) {
    const target = trimmed.toLowerCase();
    const matches = all.filter((u) => (u.email ?? "").toLowerCase() === target);
    if (matches.length === 1) {
      const u = matches[0];
      return {
        input: trimmed,
        matchedId: u.id,
        matchedBy: "email",
        user: { id: u.id, firstName: u.firstName, lastName: u.lastName, email: u.email },
      };
    }
    if (matches.length === 0) {
      throw new Error(`no user matched email ${JSON.stringify(trimmed)}`);
    }
    throw new Error(
      `email ${JSON.stringify(trimmed)} is ambiguous (${matches.length} users). Pass the opaque user id.`,
    );
  }
  // Path 3: fuzzy name — AND-match all tokens against firstName+lastName+email.
  const tokens = trimmed.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    throw new Error("owner value is empty after whitespace trim");
  }
  const matches = all.filter((u) => {
    const hay = `${u.firstName ?? ""} ${u.lastName ?? ""} ${u.email ?? ""}`.toLowerCase();
    return tokens.every((t) => hay.includes(t));
  });
  if (matches.length === 1) {
    const u = matches[0];
    return {
      input: trimmed,
      matchedId: u.id,
      matchedBy: "name",
      user: { id: u.id, firstName: u.firstName, lastName: u.lastName, email: u.email },
    };
  }
  if (matches.length === 0) {
    throw new Error(
      `no user matched ${JSON.stringify(trimmed)}. Available users: ${all
        .slice(0, 15)
        .map((u) => `${userDisplayName(u)} (${u.id})`)
        .join(", ")}${all.length > 15 ? ` ... +${all.length - 15} more` : ""}`,
    );
  }
  const candidates = matches.slice(0, 10).map((u) => ({
    id: u.id,
    name: userDisplayName(u),
    email: u.email,
  }));
  throw new Error(
    `owner ${JSON.stringify(trimmed)} is ambiguous (${matches.length} users). Pass the opaque user id or a more specific name. Candidates: ${candidates
      .map((c) => `${c.name}${c.email ? ` <${c.email}>` : ""} (${c.id})`)
      .join(", ")}`,
  );
}

const ownerAuditTool: ToolDef = {
  name: "topline_owner_audit",
  description:
    "Owner-scoped activity audit in a single call. THE tool for any 'how is rep X doing' / 'rep snapshot for X' / 'what did X work on this week' / 'X's book' / 'who's most active' / 'is X caught up on their book' question. Replaces the legacy fan-out of topline_list_users → topline_search_opportunities(assignedTo=...) → topline_execute_query on pipeline_activity_window. Accepts an opaque user_id, email, or fuzzy name; resolves via REST /users/ once and echoes how it matched. Accepts since/until as 'this-week-et' (default since = 7d), 'now', RFC3339, YYYY-MM-DD, or relative shorthand. Optional `pipeline` arg (id or fuzzy name) scopes everything to one pipeline. Optional `stale_days` (default 14) defines the stale-ownership threshold. Returns { userId, ownerResolution, user, window, pipelineFilter, status, staleDays, freshness, book, bookByStage, activity, deals, staleOwnedDeals, crossAssignedTouches, movement }. TWO OWNERSHIP LENSES: `book` / `bookByStage` / `staleOwnedDeals` / `movement` use opportunities.assigned_to (deals owned by this user). `activity` / `deals` / `crossAssignedTouches` use messages.user_id / call_events.user_id (touches THIS USER personally logged). AUTOMATION FILTER: activity/deals/staleOwnedDeals/crossAssignedTouches all exclude messages where raw_payload.$.source IN ('campaign','workflow') — those are workflow blasts and bulk campaigns, not personal rep touches. Calls and appointments are always counted as rep activity (no automation noise there). staleOwnedDeals = opps in book with no rep-attributed touch in the last `stale_days` days, ordered by monetary_value DESC — this is the 'what's rotting in the rep's book' surface. crossAssignedTouches = deals THIS USER personally touched in window where assigned_to is someone else, ordered by unique_touches DESC — this is the 'helping someone else's book' surface. PREFER THIS over topline_execute_query for any owner-scoped question. NEVER hand-write CTEs for owner book + touches + stale + cross-assignment when this tool returns all four in one call.",
  inputSchema: obj(
    {
      owner: str(
        "Owner identifier. Accepts opaque user_id, email (jane@topline.com), or fuzzy name (e.g. 'Joey Skatell', 'paul').",
      ),
      pipeline: str(
        "Optional pipeline filter (id or fuzzy name). When set, scopes everything to one pipeline.",
      ),
      since: str(
        "Window start. Default: 7 days ago. Accepts 'this-week-et', 'now', RFC3339, YYYY-MM-DD, or Nd/Nh shorthand.",
      ),
      until: str("Window end. Default: now. Same format as `since`."),
      status: str(
        "Opportunity status filter for book/deals/movement/staleOwnedDeals: 'open' (default), 'won', 'lost', 'abandoned', or 'all'/'any' to skip filtering.",
      ),
      limit: num("Max deals returned in `deals` / `staleOwnedDeals` / `crossAssignedTouches` sections. Default 25, max 100. (`book` is capped at 200, `bookByStage` is unbounded.)"),
      stale_days: num(
        "Stale-ownership threshold in days. Default 14. A deal is 'stale' when no rep-attributed touch (excluding campaign/workflow automation) has landed within this many days. Set to 7 for a stricter cadence, 30 for low-velocity pipelines.",
      ),
    },
    ["owner"],
  ),
  handler: async (args) => {
    const client = getClient();
    const ownerInput = String(args.owner ?? "");
    const status = String(args.status ?? "open");
    const limitRaw = Number(args.limit ?? 25);
    const dealLimit = Math.max(
      1,
      Math.min(100, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 25),
    );
    const staleDaysRaw = Number(args.stale_days ?? 14);
    const staleDays = Math.max(
      1,
      Math.min(365, Number.isFinite(staleDaysRaw) ? Math.trunc(staleDaysRaw) : 14),
    );
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

    const ownerResolution = await resolveOwner(ownerInput);
    const userId = ownerResolution.matchedId;

    let pipelineFilter: PipelineResolution | null = null;
    const pipelineInputRaw = args.pipeline == null ? "" : String(args.pipeline).trim();
    if (pipelineInputRaw) {
      pipelineFilter = await resolvePipelineID(client, pipelineInputRaw);
    }
    const pipelineClause = pipelineFilter ? " AND pipeline_id = ?" : "";
    const pipelineParams: string[] = pipelineFilter ? [pipelineFilter.matchedId] : [];
    // For queries that join messages alias `m` for the automation filter,
    // pipeline_id lives on pipeline_activity_window's columns — alias as `paw`.
    const pipelineClausePaw = pipelineFilter ? " AND paw.pipeline_id = ?" : "";

    const bookStatus = statusClauseSql("status", status);
    const activityStatus = statusClauseSql("opportunity_status", status);
    const movementStatus = statusClauseSql("opportunity_status", status);
    // Variant where opportunity_status lives on the aliased `paw` view.
    const activityStatusPaw = statusClauseSql("paw.opportunity_status", status);

    // Automation filter: exclude messages where raw_payload.$.source is one
    // of GHL's bulk/automation sources. Calls and appointments always count
    // as rep activity. NULL source is treated as rep (catches old inbound).
    const REP_TOUCH_FILTER = `(paw.activity_class != 'message' OR json_extract(m.raw_payload, '$.source') IS NULL OR json_extract(m.raw_payload, '$.source') NOT IN ('campaign', 'workflow'))`;

    const [freshness, book, bookByStage, activity, deals, staleOwnedDeals, crossAssignedTouches, movement] = await Promise.all([
      client.executeQuery(
        "SELECT table_name, row_count, last_synced_at, lag_seconds FROM warehouse_freshness ORDER BY table_name",
      ),
      // Book: opportunities assigned to this user, status-filtered, optionally pipeline-filtered.
      client.executeQuery(
        `SELECT id AS opportunity_id, name AS opportunity_name, contact_id, pipeline_id, pipeline_name, ` +
          `pipeline_stage_id, stage_name, status, ROUND(COALESCE(monetary_value, 0), 2) AS monetary_value, ` +
          `created_at, updated_at, last_status_change_at, last_stage_change_at, ` +
          `CAST(days_since_created AS INTEGER) AS days_since_created, ` +
          `CAST(days_in_current_stage AS INTEGER) AS days_in_current_stage ` +
          `FROM opportunity_funnel WHERE assigned_user_id = ?${bookStatus.clause}${pipelineClause} ` +
          `ORDER BY updated_at DESC LIMIT 200`,
        [userId, ...bookStatus.params, ...pipelineParams],
      ),
      // Book rolled up by pipeline+stage for at-a-glance shape.
      client.executeQuery(
        `SELECT pipeline_id, pipeline_name, pipeline_stage_id, stage_name, status, ` +
          `COUNT(*) AS opportunity_count, ROUND(COALESCE(SUM(monetary_value), 0), 2) AS pipeline_value ` +
          `FROM opportunity_funnel WHERE assigned_user_id = ?${bookStatus.clause}${pipelineClause} ` +
          `GROUP BY pipeline_id, pipeline_name, pipeline_stage_id, stage_name, status ` +
          `ORDER BY pipeline_name, opportunity_count DESC`,
        [userId, ...bookStatus.params, ...pipelineParams],
      ),
      // Activity: rep-attributed touches THIS USER authored in window
      // (lens 2 — user_id, not owner_user_id). Excludes campaign/workflow
      // automation via the message-source filter; calls and appointments
      // always count as rep activity.
      client.executeQuery(
        `SELECT paw.activity_class, paw.direction, COUNT(DISTINCT paw.source_id) AS unique_touches, ` +
          `COUNT(DISTINCT paw.opportunity_id) AS opportunities_touched, COUNT(DISTINCT paw.contact_id) AS contacts_touched, ` +
          `MIN(paw.event_at) AS first_touch, MAX(paw.event_at) AS last_touch ` +
          `FROM pipeline_activity_window paw ` +
          `LEFT JOIN messages m ON (paw.activity_class = 'message' AND m.id = paw.source_id) ` +
          `WHERE paw.user_id = ?${activityStatusPaw.clause}${pipelineClausePaw} ` +
          `AND paw.event_at >= ? AND paw.event_at <= ? ` +
          `AND ${REP_TOUCH_FILTER} ` +
          `GROUP BY paw.activity_class, paw.direction ORDER BY unique_touches DESC`,
        [userId, ...activityStatusPaw.params, ...pipelineParams, since, until],
      ),
      // Per-deal breakdown of deals THIS USER touched in window (lens 2).
      // Same automation filter as `activity` so counts are consistent.
      client.executeQuery(
        `SELECT paw.opportunity_id, paw.opportunity_name, paw.contact_id, paw.pipeline_id, paw.pipeline_stage_id, paw.owner_user_id, ` +
          `ROUND(paw.monetary_value, 2) AS monetary_value, ` +
          `COUNT(DISTINCT paw.source_id) AS unique_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'message' THEN paw.source_id END) AS message_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'call' THEN paw.source_id END) AS call_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'appointment' THEN paw.source_id END) AS appointment_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.direction = 'inbound' THEN paw.source_id END) AS inbound_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.direction = 'outbound' THEN paw.source_id END) AS outbound_touches, ` +
          `MIN(paw.event_at) AS first_touch, MAX(paw.event_at) AS last_touch ` +
          `FROM pipeline_activity_window paw ` +
          `LEFT JOIN messages m ON (paw.activity_class = 'message' AND m.id = paw.source_id) ` +
          `WHERE paw.user_id = ?${activityStatusPaw.clause}${pipelineClausePaw} ` +
          `AND paw.event_at >= ? AND paw.event_at <= ? ` +
          `AND ${REP_TOUCH_FILTER} ` +
          `GROUP BY paw.opportunity_id, paw.opportunity_name, paw.contact_id, paw.pipeline_id, paw.pipeline_stage_id, paw.owner_user_id, paw.monetary_value ` +
          `ORDER BY unique_touches DESC, monetary_value DESC LIMIT ?`,
        [userId, ...activityStatusPaw.params, ...pipelineParams, since, until, dealLimit],
      ),
      // staleOwnedDeals: opps in this user's BOOK (assigned_to) with NO
      // rep-attributed touch by anyone in the last `stale_days` days.
      // Independent of the audit window — staleness is "as of right now".
      // `last_rep_touch_at` is the most recent rep touch by anyone (not
      // just this user); a deal owned by Alex that Joey touched yesterday
      // is NOT stale. `days_since_last_rep_touch` is NULL when the deal
      // has never received a rep touch, ordered last.
      client.executeQuery(
        `WITH rep_touches AS ( ` +
          `SELECT paw.opportunity_id, MAX(paw.event_at) AS last_rep_touch_at ` +
          `FROM pipeline_activity_window paw ` +
          `LEFT JOIN messages m ON (paw.activity_class = 'message' AND m.id = paw.source_id) ` +
          `WHERE ${REP_TOUCH_FILTER} ` +
          `GROUP BY paw.opportunity_id` +
          `) ` +
          `SELECT o.id AS opportunity_id, o.name AS opportunity_name, o.contact_id, ` +
          `o.pipeline_id, o.pipeline_name, o.pipeline_stage_id, o.stage_name, ` +
          `o.status, ROUND(COALESCE(o.monetary_value, 0), 2) AS monetary_value, ` +
          `o.assigned_user_id, o.created_at, o.updated_at, ` +
          `rt.last_rep_touch_at, ` +
          `CASE WHEN rt.last_rep_touch_at IS NULL THEN NULL ` +
          `     ELSE CAST(julianday('now') - julianday(rt.last_rep_touch_at) AS INTEGER) END AS days_since_last_rep_touch ` +
          `FROM opportunity_funnel o ` +
          `LEFT JOIN rep_touches rt ON rt.opportunity_id = o.id ` +
          `WHERE o.assigned_user_id = ?${bookStatus.clause}${pipelineClause} ` +
          `AND (rt.last_rep_touch_at IS NULL OR rt.last_rep_touch_at < datetime('now', ?)) ` +
          `ORDER BY o.monetary_value DESC, rt.last_rep_touch_at ASC LIMIT ?`,
        [userId, ...bookStatus.params, ...pipelineParams, `-${staleDays} days`, dealLimit],
      ),
      // crossAssignedTouches: deals THIS USER personally touched (lens 2)
      // in the window where assigned_to is someone ELSE. The "helping
      // someone else's book" surface. NULL owner_user_id is excluded —
      // unassigned deals aren't "someone else's book", they're nobody's.
      client.executeQuery(
        `SELECT paw.opportunity_id, paw.opportunity_name, paw.contact_id, paw.pipeline_id, paw.pipeline_stage_id, paw.owner_user_id, ` +
          `ROUND(paw.monetary_value, 2) AS monetary_value, ` +
          `COUNT(DISTINCT paw.source_id) AS unique_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'message' THEN paw.source_id END) AS message_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'call' THEN paw.source_id END) AS call_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.activity_class = 'appointment' THEN paw.source_id END) AS appointment_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.direction = 'inbound' THEN paw.source_id END) AS inbound_touches, ` +
          `COUNT(DISTINCT CASE WHEN paw.direction = 'outbound' THEN paw.source_id END) AS outbound_touches, ` +
          `MIN(paw.event_at) AS first_touch, MAX(paw.event_at) AS last_touch ` +
          `FROM pipeline_activity_window paw ` +
          `LEFT JOIN messages m ON (paw.activity_class = 'message' AND m.id = paw.source_id) ` +
          `WHERE paw.user_id = ? ` +
          `AND paw.owner_user_id IS NOT NULL AND paw.owner_user_id != ? ` +
          `${activityStatusPaw.clause}${pipelineClausePaw} ` +
          `AND paw.event_at >= ? AND paw.event_at <= ? ` +
          `AND ${REP_TOUCH_FILTER} ` +
          `GROUP BY paw.opportunity_id, paw.opportunity_name, paw.contact_id, paw.pipeline_id, paw.pipeline_stage_id, paw.owner_user_id, paw.monetary_value ` +
          `ORDER BY unique_touches DESC, monetary_value DESC LIMIT ?`,
        [userId, userId, ...activityStatusPaw.params, ...pipelineParams, since, until, dealLimit],
      ),
      // Movement: deals owned by THIS USER (lens 1) that moved in the window.
      client.executeQuery(
        `SELECT opportunity_id, opportunity_name, contact_id, pipeline_id, pipeline_name, ` +
          `pipeline_stage_id, stage_name, opportunity_status, monetary_value, ` +
          `last_movement_at, last_movement_kind ` +
          `FROM pipeline_movement_window WHERE owner_user_id = ?${movementStatus.clause}${pipelineClause} ` +
          `AND last_movement_at >= ? AND last_movement_at <= ? ` +
          `ORDER BY last_movement_at DESC`,
        [userId, ...movementStatus.params, ...pipelineParams, since, until],
      ),
    ]);

    return {
      userId,
      ownerResolution,
      user: ownerResolution.user ?? null,
      window: { since, until },
      pipelineFilter,
      status,
      staleDays,
      freshness: { columns: freshness.columns, rows: freshness.rows },
      book: { columns: book.columns, rows: book.rows },
      bookByStage: { columns: bookByStage.columns, rows: bookByStage.rows },
      activity: { columns: activity.columns, rows: activity.rows },
      deals: { columns: deals.columns, rows: deals.rows },
      staleOwnedDeals: { columns: staleOwnedDeals.columns, rows: staleOwnedDeals.rows },
      crossAssignedTouches: { columns: crossAssignedTouches.columns, rows: crossAssignedTouches.rows },
      movement: { columns: movement.columns, rows: movement.rows },
    };
  },
};

export const tools: ToolDef[] = [
  doctorTool,
  freshnessTool,
  snapshotTool,
  auditTool,
  contactAuditTool,
  ownerAuditTool,
];
