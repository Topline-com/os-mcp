// Dump tool metadata (name, description, inputSchema) from the registry to JSON.
// Run: tsx scripts/dump-tools.ts > docs/api/os-mcp/tools.json
import { ALL_TOOLS, ACTION_TOOLS, ANALYTICS_TOOL_NAMES } from "../apps/edge/src/registry.js";

const out = ALL_TOOLS.map((t) => ({
  name: t.name,
  description: t.description,
  inputSchema: t.inputSchema,
  category: ANALYTICS_TOOL_NAMES.has(t.name) ? "analytics" : "action",
}));

process.stdout.write(JSON.stringify({ tools: out, count: out.length }, null, 2));
