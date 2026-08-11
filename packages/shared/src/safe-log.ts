export type SafeErrorCategory = "upstream" | "timeout" | "network" | "internal";

export interface SafeErrorFields {
  category: SafeErrorCategory;
  status?: number;
}

type SafeLogValue = number | boolean | null | undefined | SafeErrorFields;
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
