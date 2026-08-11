import {
  ProtocolError,
  ProtocolErrorCode,
  Server,
  createMcpHandler,
  type CallToolResult,
  type Tool,
} from "@modelcontextprotocol/server";
import { SERVER_INFO } from "@topline/shared";

import { ACTION_TOOLS, ALL_TOOLS, ANALYTICS_TOOL_NAMES } from "./registry.js";
import type { ToolDef } from "./tools/types.js";

export type McpTransport = "remote" | "stdio";

export interface BuildMcpServerOptions {
  transport: McpTransport;
  tools?: readonly ToolDef[];
}

export interface CreateRemoteMcpHandlerOptions {
  tools?: readonly ToolDef[];
}

function canonicalTools(tools: readonly ToolDef[]): ToolDef[] {
  return [...tools].sort((left, right) => left.name.localeCompare(right.name));
}

function toolResult(value: unknown): CallToolResult {
  if (typeof value === "string") {
    return { content: [{ type: "text", text: value }] };
  }
  const structuredContent = value === undefined ? null : JSON.parse(JSON.stringify(value));
  return {
    content: [{ type: "text", text: JSON.stringify(structuredContent, null, 2) }],
    structuredContent: structuredContent as CallToolResult["structuredContent"],
  };
}

export function buildMcpServer(options: BuildMcpServerOptions): Server {
  const tools = canonicalTools(
    options.tools ?? (options.transport === "stdio" ? ACTION_TOOLS : ALL_TOOLS),
  );
  const toolsByName = new Map(tools.map((tool) => [tool.name, tool]));
  const server = new Server(
    { name: SERVER_INFO.name, version: SERVER_INFO.version },
    {
      capabilities: { tools: {} },
      cacheHints: {
        "server/discover": { ttlMs: 300_000, cacheScope: "private" },
        "tools/list": { ttlMs: 30_000, cacheScope: "private" },
      },
    },
  );

  server.setRequestHandler("tools/list", async () => ({
    tools: tools.map(({ name, description, inputSchema }) => ({
      name,
      description,
      inputSchema: JSON.parse(JSON.stringify(inputSchema)) as Tool["inputSchema"],
    })),
  }));

  server.setRequestHandler("tools/call", async (request) => {
    const tool = toolsByName.get(request.params.name);
    if (!tool) {
      throw new ProtocolError(
        ProtocolErrorCode.InvalidParams,
        `Unknown tool: ${request.params.name}`,
      );
    }

    try {
      const value = await tool.handler(
        (request.params.arguments ?? {}) as Record<string, unknown>,
      );
      return server.projectCallToolResult(toolResult(value), undefined);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return server.projectCallToolResult(
        {
          isError: true,
          content: [{ type: "text", text: message }],
        },
        undefined,
      );
    }
  });

  return server;
}

export function createRemoteMcpHandler(options: CreateRemoteMcpHandlerOptions = {}) {
  return createMcpHandler(
    ({ authInfo }) => {
      const rawPitBearer = authInfo?.extra?.rawPitBearer === true;
      const tools = options.tools ?? (
        rawPitBearer
          ? ALL_TOOLS.filter((tool) => !ANALYTICS_TOOL_NAMES.has(tool.name))
          : ALL_TOOLS
      );
      return buildMcpServer({ transport: "remote", tools });
    },
    { legacy: "stateless" },
  );
}

export const remoteMcpHandler = createRemoteMcpHandler();
