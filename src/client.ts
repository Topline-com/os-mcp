import { BRAND_NAME } from "./branding.js";

const BASE_URL = "https://services.leadconnectorhq.com";
const API_VERSION = "2021-07-28";

export class ToplineApiError extends Error {
  statusCode: number;
  body: unknown;
  constructor(statusCode: number, body: unknown, message: string) {
    super(message);
    this.statusCode = statusCode;
    this.body = body;
  }
}

export interface FetchOptions {
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
}

function requirePit(): string {
  const pit = process.env.TOPLINE_PIT?.trim();
  if (!pit) {
    throw new Error(
      `${BRAND_NAME} MCP is missing the TOPLINE_PIT environment variable. ` +
        `Add your Private Integration Token to your Claude config.`
    );
  }
  return pit;
}

export function getLocationId(override?: string): string {
  const id = override?.trim() || process.env.TOPLINE_LOCATION_ID?.trim();
  if (!id) {
    throw new Error(
      `${BRAND_NAME} MCP is missing the TOPLINE_LOCATION_ID environment variable. ` +
        `Add your sub-account Location ID to your Claude config, or pass locationId as a tool argument.`
    );
  }
  return id;
}

function buildUrl(path: string, query?: FetchOptions["query"]): string {
  const trimmed = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(`${BASE_URL}${trimmed}`);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v === undefined || v === null) continue;
      url.searchParams.set(k, String(v));
    }
  }
  return url.toString();
}

function shapeError(status: number, body: unknown): ToplineApiError {
  let message = `${BRAND_NAME} API error ${status}`;
  if (body && typeof body === "object") {
    const b = body as Record<string, unknown>;
    if (typeof b.message === "string") message = b.message;
    else if (Array.isArray(b.message)) message = b.message.join("; ");
  }
  if (status === 401) {
    message = `Authentication failed. Your ${BRAND_NAME} Private Integration Token is invalid or expired. Regenerate it in ${BRAND_NAME} → Settings → Private Integrations.`;
  } else if (status === 403) {
    message = `Forbidden (${message}). Your ${BRAND_NAME} Private Integration is missing the required scope. Edit the integration and tick the missing scope.`;
  } else if (status === 429) {
    message = `Rate limited by ${BRAND_NAME}. Try again in a few seconds.`;
  }
  return new ToplineApiError(status, body, message);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export interface ToplineFetchOptions extends FetchOptions {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
}

export async function toplineFetch<T = unknown>(
  path: string,
  opts: ToplineFetchOptions = {}
): Promise<T> {
  const pit = requirePit();
  const method = opts.method ?? "GET";
  const url = buildUrl(path, opts.query);

  const headers: Record<string, string> = {
    Authorization: `Bearer ${pit}`,
    Version: API_VERSION,
    Accept: "application/json",
  };
  let body: string | undefined;
  if (opts.body !== undefined && method !== "GET") {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(opts.body);
  }

  let attempt = 0;
  while (true) {
    attempt += 1;
    const res = await fetch(url, { method, headers, body });
    const raw = await res.text();
    let parsed: unknown = raw;
    if (raw) {
      try {
        parsed = JSON.parse(raw);
      } catch {
        // leave as raw string
      }
    }

    if (res.ok) return parsed as T;

    // Retry 429 with backoff up to 3 attempts
    if (res.status === 429 && attempt < 3) {
      const retryAfter = Number(res.headers.get("retry-after") ?? "0");
      const delay = retryAfter > 0 ? retryAfter * 1000 : 500 * 2 ** (attempt - 1);
      await sleep(delay);
      continue;
    }

    throw shapeError(res.status, parsed);
  }
}
