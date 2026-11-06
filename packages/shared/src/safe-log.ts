export type SafeErrorCategory = "upstream" | "timeout" | "network" | "internal";

export interface SafeErrorFields {
  category: SafeErrorCategory;
  status?: number;
}

declare const safeIdBrand: unique symbol;
/** An ID that `safeId` admitted: the only kind of string a safe log line takes. */
export type SafeId = string & { readonly [safeIdBrand]: true };

type SafeLogValue = number | boolean | null | undefined | SafeErrorFields | SafeId;

const SAFE_ID = /^[A-Za-z0-9-]{1,64}$/;

/**
 * Admit a connection ID or a Location ID to a log line: letters, digits and
 * hyphens only, at most 64 characters, and never anything that starts like a
 * Private Integration Token, so a token pasted into the wrong field can't reach
 * a log. Anything else comes back as null.
 */
export function safeId(value: string | null | undefined): SafeId | null {
  if (!value || !SAFE_ID.test(value) || /^pit-/i.test(value)) return null;
  return value as SafeId;
}
export type SafeLogFields = Record<string, SafeLogValue>;

export function safeErrorFields(error: unknown): SafeErrorFields {
  if (isObject(error)) {
    const status = numericStatus(error.status);
    if (status !== undefined) return { category: "upstream", status };
    if (error.name === "AbortError") return { category: "timeout" };
    if (typeof error.code === "string" && /^(?:E|UND_ERR_)/.test(error.code)) {
      return { category: "network" };
    }
  }
  return { category: "internal" };
}

export function safeLog(
  level: "log" | "warn" | "error",
  event: string,
  fields: SafeLogFields = {},
): void {
  console[level](JSON.stringify({ event, ...compact(fields) }));
}

function compact(fields: SafeLogFields): SafeLogFields {
  return Object.fromEntries(
    Object.entries(fields).filter(([, value]) => value !== undefined),
  );
}

function numericStatus(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value >= 400 && value <= 599
    ? value
    : undefined;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
