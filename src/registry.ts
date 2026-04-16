// Shared tool registry — imported by both the stdio entry point (src/index.ts)
// and the Cloudflare Worker entry point (src/remote.ts).

import type { ToolDef } from "./tools/types.js";

import { tools as pingTools } from "./tools/ping.js";
import { tools as setupCheckTools } from "./tools/setup_check.js";
import { tools as passthroughTools } from "./tools/passthrough.js";
import { tools as contactTools } from "./tools/contacts.js";
import { tools as conversationTools } from "./tools/conversations.js";
import { tools as opportunityTools } from "./tools/opportunities.js";
import { tools as calendarTools } from "./tools/calendars.js";
import { tools as taskTools } from "./tools/tasks.js";
import { tools as noteTools } from "./tools/notes.js";
import { tools as customFieldTools } from "./tools/custom_fields.js";
import { tools as workflowTools } from "./tools/workflows.js";
import { tools as tagTools } from "./tools/tags.js";
import { tools as locationTools } from "./tools/locations.js";
import { tools as userTools } from "./tools/users.js";
import { tools as formTools } from "./tools/forms.js";
import { tools as surveyTools } from "./tools/surveys.js";

export const ALL_TOOLS: ToolDef[] = [
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
  ...workflowTools,
  ...tagTools,
  ...locationTools,
  ...userTools,
  ...formTools,
  ...surveyTools,
];

// Sanity check: reject duplicate tool names at startup.
const seen = new Set<string>();
for (const t of ALL_TOOLS) {
  if (seen.has(t.name)) {
    throw new Error(`Duplicate tool name: ${t.name}`);
  }
  seen.add(t.name);
}

export const toolsByName = new Map<string, ToolDef>(ALL_TOOLS.map((t) => [t.name, t]));
