import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { BRAND_NAME, SERVER_INFO } from "@topline/shared";
import { ACTION_TOOLS } from "./registry.js";
import { buildMcpServer } from "./mcp-server.js";

// Stdio only exposes ACTION_TOOLS (the CRM REST proxies). The analytics SQL
// surface needs the Cloudflare Worker's request context (LOCATION_DO
// binding + edgeContext); it would register successfully in stdio but
// every call would throw at runtime. Better to not advertise it at all.
serveStdio(
  () => buildMcpServer({ transport: "stdio" }),
  {
    legacy: "serve",
    onerror(error) {
      console.error(`${BRAND_NAME} MCP transport error:`, error);
    },
  },
);

// Log to stderr so we don't corrupt the stdio JSON-RPC stream.
console.error(
  `${BRAND_NAME} MCP v${SERVER_INFO.version} ready — ${ACTION_TOOLS.length} tools registered.`,
);
