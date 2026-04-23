import { AsyncLocalStorage } from "node:async_hooks";
import { BRAND_NAME } from "./branding.js";

const BASE_URL = "https://services.leadconnectorhq.com";
const API_VERSION = "2021-07-28";

export interface RequestCredentials {
  pit: string;
  locationId?: string;
}

/**
 * Per-request credentials store. The stdio entry point never sets this —
 * credentials flow from process.env. The remote (Worker) entry point sets
 * this once per incoming HTTP request so tool handlers read the right user's
 * PIT / Location ID without any changes to the handlers themselves.
 */
export const credentialsContext = new AsyncLocalStorage<RequestCredentials>();

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

/** Returns the current PIT (from per-request context OR env), or null if absent. */
export function peekPit(): string | null {
  const ctxPit = credentialsContext.getStore()?.pit?.trim();
  if (ctxPit) return ctxPit;
  const envPit = process.env.TOPLINE_PIT?.trim();
  return envPit || null;
}

/** Returns the current Location ID (from per-request context OR env), or null if absent. */
export function peekLocationId(): string | null {
  const ctxLoc = credentialsContext.getStore()?.locationId?.trim();
  if (ctxLoc) return ctxLoc;
  const envLoc = process.env.TOPLINE_LOCATION_ID?.trim();
  return envLoc || null;
}

function requirePit(): string {
  const pit = peekPit();
  if (pit) return pit;
  throw new Error(
    `${BRAND_NAME} MCP is missing the Private Integration Token. ` +
      `For local setup add TOPLINE_PIT to your Claude config env block. ` +
      `For remote setup send it as an Authorization: Bearer header.`,
  );
}

export function getLocationId(override?: string): string {
  const overrideId = override?.trim();
  if (overrideId) return overrideId;
  const id = peekLocationId();
  if (id) return id;
  throw new Error(
    `${BRAND_NAME} MCP is missing the Location ID. ` +
      `For local setup add TOPLINE_LOCATION_ID to your Claude config env block. ` +
      `For remote setup send it as an X-Topline-Location-Id header. ` +
      `Or pass locationId as a tool argument.`,
  );
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
