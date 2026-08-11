const MCP_ALLOWED_ORIGINS = new Set([
  "https://claude.ai",
  "https://chatgpt.com",
  "https://os-mcp.topline.com",
  "http://localhost:6274",
  "http://127.0.0.1:6274",
  "http://[::1]:6274",
  "http://localhost:8787",
  "http://127.0.0.1:8787",
  "http://[::1]:8787",
]);

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

function rejectedOriginResponse(): Response {
  return Response.json(
    {
      jsonrpc: "2.0",
      id: null,
      error: { code: -32000, message: "Invalid Origin header" },
    },
    { status: 403 },
  );
}

function requestedMcpParamHeaders(request: Request): string[] {
  const requested = request.headers.get("Access-Control-Request-Headers") ?? "";
  return requested
    .split(",")
    .map((header) => header.trim())
    .filter((header) => /^mcp-param-[a-z0-9-]+$/i.test(header));
}

function varyByOrigin(response: Response): Response {
  const values = (response.headers.get("Vary") ?? "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (values.includes("*")) {
    response.headers.set("Vary", "*");
    return response;
  }

  const seen = new Set<string>();
  const unique = values.filter((value) => {
    const key = value.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  const originIndex = unique.findIndex((value) => value.toLowerCase() === "origin");
  if (originIndex === -1) unique.push("Origin");
  else unique[originIndex] = "Origin";
  response.headers.set("Vary", unique.join(", "));
  return response;
}

function applyMcpCors(request: Request, response: Response, origin?: string): Response {
  const generatedHeaders = [...response.headers.keys()].filter((header) =>
    /^mcp-(?:param|result)-[a-z0-9-]+$/i.test(header)
  );
  response.headers.delete("Access-Control-Allow-Origin");
  if (origin !== undefined) response.headers.set("Access-Control-Allow-Origin", origin);
  response.headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  response.headers.set(
    "Access-Control-Allow-Headers",
    [...MCP_ALLOW_HEADERS, ...requestedMcpParamHeaders(request)].join(", "),
  );
  response.headers.set(
    "Access-Control-Expose-Headers",
    [...MCP_EXPOSE_HEADERS, ...generatedHeaders].join(", "),
  );
  return varyByOrigin(response);
}

function mcpPreflightResponse(request: Request, origin?: string): Response {
  return applyMcpCors(request, new Response(null, { status: 204 }), origin);
}

export async function handleMcpHttpRequest(
  request: Request,
  dispatch: () => Promise<Response>,
): Promise<Response> {
  // Compare the normalized header value to exact serialized origins. Parsing
  // as a URL would accept paths, userinfo, alternate IP forms, and other
  // values that are not valid serialized Origin tuples.
  const hasOrigin = request.headers.has("Origin");
  const origin = request.headers.get("Origin") ?? "";
  if (hasOrigin && !MCP_ALLOWED_ORIGINS.has(origin)) {
    return varyByOrigin(rejectedOriginResponse());
  }
  const validatedOrigin = hasOrigin ? origin : undefined;
  if (request.method === "OPTIONS") return mcpPreflightResponse(request, validatedOrigin);
  return applyMcpCors(request, await dispatch(), validatedOrigin);
}
