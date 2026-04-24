// SQL-safety layer for the customer-facing execute_query tool.
//
// Design:
//   1. Parse the query with node-sql-parser using the SQLite dialect.
//   2. Reject anything that isn't a single SELECT or WITH ... SELECT.
//   3. Inject LIMIT <cap> on the outermost SELECT if none is present, or
//      cap it at <cap> if a larger one is present.
//   4. Return the rewritten query string ready to execute.
//
// Defense-in-depth: even if the parser misses something, the LocationDO
// runs under the tenant's isolated SQLite DB, so the blast radius is
// one customer's own data. But fail loud at parse time for anything we
// can catch — "PRAGMA", "ATTACH", "DETACH", "INSERT", "DROP", etc.
// should never reach SQLite.

import { Parser } from "node-sql-parser";

export const DEFAULT_ROW_CAP = 5000;

export interface SafeQuery {
  /** The rewritten SQL, safe to execute. Equal to the input when no rewrite was needed. */
  sql: string;
  /** True when the parser had to cap or inject a LIMIT. */
  limited: boolean;
  /** The effective row cap applied (either the injected cap or the user's lower value). */
  effective_limit: number;
}

export class SqlSafetyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SqlSafetyError";
  }
}

const parser = new Parser();

/**
 * Validate + rewrite. Throws SqlSafetyError on anything unsafe.
 *
 * What's accepted:
 *   - Single SELECT statement
 *   - Single WITH ... SELECT (CTE)
 *
 * What's rejected:
 *   - Multi-statement queries ("SELECT 1; SELECT 2")
 *   - DDL (CREATE, DROP, ALTER)
 *   - DML (INSERT, UPDATE, DELETE, REPLACE, MERGE)
 *   - Meta commands (PRAGMA, ATTACH, DETACH, VACUUM, ANALYZE)
 *   - Anything the parser can't parse at all
 */
export function sanitizeQuery(
  query: string,
  rowCap: number = DEFAULT_ROW_CAP,
): SafeQuery {
  if (typeof query !== "string" || query.trim().length === 0) {
    throw new SqlSafetyError("Query is empty.");
  }

  // Strip a trailing semicolon (common copy-paste), but reject multiple
  // statements. node-sql-parser returns an array when the input contains
  // multiple statements, so we'll catch that via the array-check below.
  const cleaned = query.trim().replace(/;\s*$/, "");

  let ast: unknown;
  try {
    ast = parser.astify(cleaned, { database: "sqlite" });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new SqlSafetyError(`Could not parse SQL: ${message}`);
  }

  // Multi-statement input comes back as an array.
  if (Array.isArray(ast)) {
    if (ast.length !== 1) {
      throw new SqlSafetyError(
        `Only one SQL statement per request is allowed (got ${ast.length}).`,
      );
    }
    ast = ast[0];
  }

  const stmt = ast as { type?: string };
  if (!stmt || typeof stmt !== "object" || typeof stmt.type !== "string") {
    throw new SqlSafetyError("Unrecognized SQL shape.");
  }

  if (stmt.type !== "select") {
    throw new SqlSafetyError(
      `Only SELECT / WITH queries are allowed. Got "${stmt.type.toUpperCase()}". DDL, DML, PRAGMA, ATTACH, and admin statements are blocked.`,
    );
  }

  // Inject / cap LIMIT.
  const selectStmt = stmt as {
    limit?: { seperator?: string; value?: Array<{ type: string; value: number }> } | null;
  };
  let limited = false;
  let effectiveLimit = rowCap;

  const existing = selectStmt.limit?.value;
  if (!existing || existing.length === 0) {
    // Inject fresh limit.
    selectStmt.limit = {
      seperator: "",
      value: [{ type: "number", value: rowCap }],
    };
    limited = true;
    effectiveLimit = rowCap;
  } else {
    // LIMIT exists. The value is either [n] for "LIMIT n" or [offset, count]
    // for "LIMIT offset, count" — we cap the row count in both shapes.
    const lastIdx = existing.length - 1;
    const current = existing[lastIdx]?.value;
    if (typeof current === "number" && current > rowCap) {
      existing[lastIdx] = { type: "number", value: rowCap };
      limited = true;
      effectiveLimit = rowCap;
    } else if (typeof current === "number") {
      effectiveLimit = current;
    }
  }

  let rewritten: string;
  try {
    rewritten = parser.sqlify(stmt as Parameters<typeof parser.sqlify>[0], {
      database: "sqlite",
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new SqlSafetyError(`Failed to re-serialize SQL after safety pass: ${message}`);
  }

  return {
    sql: rewritten,
    limited,
    effective_limit: effectiveLimit,
  };
}
