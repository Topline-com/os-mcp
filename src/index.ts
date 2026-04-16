#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { BRAND_NAME, SERVER_INFO } from "./branding.js";
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

const ALL_TOOLS: ToolDef[] = [
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

const toolsByName = new Map<string, ToolDef>(ALL_TOOLS.map((t) => [t.name, t]));

const server = new Server(
  { name: SERVER_INFO.name, version: SERVER_INFO.version },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: ALL_TOOLS.map(({ name, description, inputSchema }) => ({
    name,
    description,
    inputSchema,
  })),
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: rawArgs } = req.params;
  const tool = toolsByName.get(name);
  if (!tool) {
    return {
      isError: true,
      content: [{ type: "text", text: `Unknown tool: ${name}` }],
    };
  }
  try {
    const result = await tool.handler((rawArgs ?? {}) as Record<string, unknown>);
    return {
      content: [
        {
          type: "text",
          text: typeof result === "string" ? result : JSON.stringify(result, null, 2),
        },
      ],
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      isError: true,
      content: [{ type: "text", text: message }],
    };
  }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  // Log to stderr so we don't corrupt the stdio JSON-RPC stream.
  console.error(
    `${BRAND_NAME} MCP v${SERVER_INFO.version} ready — ${ALL_TOOLS.length} tools registered.`,
  );
}

main().catch((err) => {
  console.error(`${BRAND_NAME} MCP failed to start:`, err);
  process.exit(1);
});
