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

function requestedMcpParamHeaders(request: Request): string[] {
  const requested = request.headers.get("Access-Control-Request-Headers") ?? "";
  return requested
    .split(",")
    .map((header) => header.trim())
    .filter((header) => /^mcp-param-[a-z0-9-]+$/i.test(header));
}

export function applyMcpCors(request: Request, response: Response): Response {
  const generatedHeaders = [...response.headers.keys()].filter((header) =>
    /^mcp-(?:param|result)-[a-z0-9-]+$/i.test(header)
  );
  response.headers.set("Access-Control-Allow-Origin", "*");
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

export function mcpPreflightResponse(request: Request): Response {
  return applyMcpCors(request, new Response(null, { status: 204 }));
}
