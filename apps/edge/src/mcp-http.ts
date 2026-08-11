import { originValidationResponse } from "@modelcontextprotocol/server";

const MCP_ALLOWED_ORIGIN_HOSTNAMES = [
  "claude.ai",
  "chatgpt.com",
  "os-mcp.topline.com",
  "localhost",
  "127.0.0.1",
  "[::1]",
];

const MCP_ALLOW_HEADERS = [
  "Authorization",
  "Content-Type",
  "MCP-Protocol-Version",
  "Mcp-Method",
  "Mcp-Name",
  "X-Topline-Location-Id",
] as const;

const MCP_EXPOSE_HEADERS = [
  "MCP-Protocol-Version",
  "Mcp-Method",
  "Mcp-Name",
] as const;

function validateMcpOrigin(request: Request): Response | undefined {
  return originValidationResponse(request, MCP_ALLOWED_ORIGIN_HOSTNAMES);
}

function requestedMcpParamHeaders(request: Request): string[] {
  const requested = request.headers.get("Access-Control-Request-Headers") ?? "";
  return requested
    .split(",")
    .map((header) => header.trim())
    .filter((header) => /^mcp-param-[a-z0-9-]+$/i.test(header));
}

function applyMcpCors(request: Request, response: Response): Response {
  const generatedHeaders = [...response.headers.keys()].filter((header) =>
    /^mcp-(?:param|result)-[a-z0-9-]+$/i.test(header)
  );
  const origin = request.headers.get("Origin");
  if (origin) {
    response.headers.set("Access-Control-Allow-Origin", origin);
    response.headers.append("Vary", "Origin");
  }
  response.headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  response.headers.set(
    "Access-Control-Allow-Headers",
    [...MCP_ALLOW_HEADERS, ...requestedMcpParamHeaders(request)].join(", "),
  );
  response.headers.set(
    "Access-Control-Expose-Headers",
    [...MCP_EXPOSE_HEADERS, ...generatedHeaders].join(", "),
  );
  return response;
}

function mcpPreflightResponse(request: Request): Response {
  return applyMcpCors(request, new Response(null, { status: 204 }));
}

export async function handleMcpHttpRequest(
  request: Request,
  dispatch: () => Promise<Response>,
): Promise<Response> {
  const rejected = validateMcpOrigin(request);
  if (rejected) return rejected;
  if (request.method === "OPTIONS") return mcpPreflightResponse(request);
  return applyMcpCors(request, await dispatch());
}
