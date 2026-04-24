import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { BRAND_NAME, SERVER_INFO } from "@topline/shared";
import { ACTION_TOOLS } from "./registry.js";

// Stdio only exposes ACTION_TOOLS (GHL REST proxies). The analytics SQL
// surface needs the Cloudflare Worker's request context (LOCATION_DO
// binding + edgeContext); it would register successfully in stdio but
// every call would throw at runtime. Better to not advertise it at all.
const toolsForStdio = ACTION_TOOLS;
const toolsByName = new Map(toolsForStdio.map((t) => [t.name, t]));

const server = new Server(
  { name: SERVER_INFO.name, version: SERVER_INFO.version },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: toolsForStdio.map(({ name, description, inputSchema }) => ({
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
    `${BRAND_NAME} MCP v${SERVER_INFO.version} ready — ${toolsForStdio.length} tools registered.`,
  );
}

main().catch((err) => {
  console.error(`${BRAND_NAME} MCP failed to start:`, err);
  process.exit(1);
});
