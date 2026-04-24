// Shared tool registry — imported by both the stdio entry point (src/index.ts)
// and the Cloudflare Worker entry point (src/remote.ts).
//
// Tools are split into two categories:
//
//   ACTION_TOOLS   — proxy individual GHL API endpoints (contacts,
//                    opportunities, calendars, messages, etc). Available
//                    in every runtime, including stdio. They depend only
//                    on the shared toplineFetch client + credentialsContext.
//
//   ANALYTICS_TOOLS — the SQL surface (describe_schema, explain_tables,
//                    execute_query, utilize_api). They require the
//                    edge worker's request context (LOCATION_DO binding
//                    + the edgeContext AsyncLocalStorage) and would
//                    fail at runtime from stdio. Hidden from stdio.
//
// Worker consumers use ALL_TOOLS (union). Stdio uses ACTION_TOOLS only.

import type { ToolDef } from "./tools/types.js";

import { tools as pingTools } from "./tools/ping.js";
import { tools as setupCheckTools } from "./tools/setup_check.js";
import { tools as passthroughTools } from "./tools/passthrough.js";
import { tools as sqlTools } from "./tools/sql.js";
import { tools as referenceTools } from "./tools/references.js";
import { tools as contactTools } from "./tools/contacts.js";
import { tools as conversationTools } from "./tools/conversations.js";
import { tools as opportunityTools } from "./tools/opportunities.js";
import { tools as calendarTools } from "./tools/calendars.js";
import { tools as taskTools } from "./tools/tasks.js";
import { tools as noteTools } from "./tools/notes.js";
import { tools as customFieldTools } from "./tools/custom_fields.js";
import { tools as customValueTools } from "./tools/custom_values.js";
import { tools as workflowTools } from "./tools/workflows.js";
import { tools as tagTools } from "./tools/tags.js";
import { tools as locationTools } from "./tools/locations.js";
import { tools as userTools } from "./tools/users.js";
import { tools as formTools } from "./tools/forms.js";
import { tools as surveyTools } from "./tools/surveys.js";

/**
 * Runtime-independent GHL action tools. Safe to run from stdio (local
 * Claude Desktop / Code install) because they only need the shared
 * HTTP client + credentialsContext.
 */
export const ACTION_TOOLS: ToolDef[] = [
  ...pingTools,
  ...setupCheckTools,
  ...passthroughTools,
  ...contactTools,
  ...conversationTools,
  ...opportunityTools,
  ...calendarTools,
  ...taskTools,
  ...noteTools,
  ...customFieldTools,
  ...customValueTools,
  ...workflowTools,
  ...tagTools,
  ...locationTools,
  ...userTools,
  ...formTools,
  ...surveyTools,
];

/**
 * Worker-only analytics tools (SQL surface). Require apps/edge's
 * request context + LOCATION_DO binding; not usable from stdio.
 */
export const ANALYTICS_TOOLS: ToolDef[] = [
  ...sqlTools,
  ...referenceTools,
];

/**
 * Name set for category checks at dispatch time (e.g., rejecting
 * raw-PIT bearers for analytics tools).
 */
export const ANALYTICS_TOOL_NAMES: ReadonlySet<string> = new Set(
  ANALYTICS_TOOLS.map((t) => t.name),
);

/** Every tool available to the Worker (action + analytics). */
export const ALL_TOOLS: ToolDef[] = [...ACTION_TOOLS, ...ANALYTICS_TOOLS];

// Sanity check: reject duplicate tool names at startup.
const seen = new Set<string>();
for (const t of ALL_TOOLS) {
  if (seen.has(t.name)) {
    throw new Error(`Duplicate tool name: ${t.name}`);
  }
  seen.add(t.name);
}

export const toolsByName = new Map<string, ToolDef>(ALL_TOOLS.map((t) => [t.name, t]));
