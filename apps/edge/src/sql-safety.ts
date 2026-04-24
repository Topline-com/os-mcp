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

// node-sql-parser ships as CommonJS. Native ESM (tsx test runner) can't
// destructure named exports from CJS, so we import the default and pull
// Parser off it. esbuild / wrangler bundling also handles this path
// correctly via their CJS/ESM interop.
import nodeSqlParser from "node-sql-parser";
import { getExposedEntities, ANALYTICS_VIEWS } from "@topline/shared-schema";

const { Parser } = nodeSqlParser as unknown as {
  Parser: new () => {
    astify(sql: string, opts: { database: string }): unknown;
    sqlify(ast: unknown, opts: { database: string }): string;
    tableList(sql: string, opts: { database: string }): string[];
  };
};

export const DEFAULT_ROW_CAP = 5000;

/**
 * Bookkeeping / internal-SQLite tables that must never be readable through
 * the customer-facing SQL surface, even if an allowlist check misfires.
 * Keeps SELECT name FROM sqlite_master, SELECT * FROM _sync_state, etc.
 * from leaking schema internals or sync state.
 */
const INTERNAL_TABLE_PREFIXES: readonly string[] = ["sqlite_", "_"];

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

  // Block SQLite's table-valued PRAGMA helpers. These appear
  // syntactically as function calls (SELECT ... FROM pragma_table_info(
  // 'x')) not table references, so parser.tableList() returns nothing
  // for them and the exposed-table allowlist in enforceExposedTables
  // has nothing to reject. They expose schema metadata for every table
  // in the DO — including hidden entities and bookkeeping tables —
  // which is exactly what blocking PRAGMA is supposed to prevent.
  //
  // Detected by substring on the sanitized (sqlify) output so quoting
  // is normalized. Does not affect json_each, json_tree, or other
  // legitimate table-valued helpers — those don't start with pragma_.
  if (/\bpragma_\w+\s*\(/i.test(rewritten)) {
    throw new SqlSafetyError(
      "Table-valued PRAGMA functions (pragma_table_info, pragma_foreign_key_list, pragma_index_list, etc.) are blocked. Call topline_explain_tables for schema introspection instead.",
    );
  }

  return {
    sql: rewritten,
    limited,
    effective_limit: effectiveLimit,
  };
}

/**
 * Second-pass safety check for the CUSTOMER-facing SQL surface (MCP
 * topline_execute_query + HTTP /query/api/execute-sql).
 *
 * sanitizeQuery proves the statement is SELECT/WITH-only; this function
 * proves it only references tables the tenant is allowed to see. Every
 * DO physically contains:
 *   - all manifest entity tables (whether exposed: true or not)
 *   - the internal _sync_state, _schema_log bookkeeping tables
 *   - SQLite's sqlite_* meta tables
 *
 * Without this check, an authenticated user could read hidden entities
 * (opportunities/conversations/messages today), bookkeeping state, and
 * SQLite metadata. We gate the customer surface to ONLY the
 * `exposed: true && auditPasses()` subset returned by
 * getExposedEntities().
 *
 * CTE aliases (WITH foo AS (...) SELECT * FROM foo) ARE returned by
 * node-sql-parser's tableList alongside real tables. We parse the
 * WITH clause from the AST, collect the alias names, and exempt them
 * from the allowlist check.
 *
 * Admin surfaces (e.g. /admin/do-query) deliberately SKIP this check.
 */
export function enforceExposedTables(query: string): void {
  // Exposed set = every entity past the audit gate + every analytics
  // view whose base_tables are ALL themselves exposed. The base-table
  // check is defense-in-depth: if someone reshapes a view to UNION in
  // a hidden table (the way contact_timeline used to include
  // appointments), the view drops out of the allowlist automatically
  // rather than silently leaking the hidden data through a side door.
  const exposedEntityNames = new Set(getExposedEntities().map((e) => e.table));
  const safeViewNames = ANALYTICS_VIEWS
    .filter((v) => v.base_tables.every((bt) => exposedEntityNames.has(bt)))
    .map((v) => v.name);
  const exposed = new Set<string>([
    ...exposedEntityNames,
    ...safeViewNames,
  ]);

  // Extract CTE alias names from the AST so "WITH n AS (...) SELECT * FROM n"
  // doesn't get rejected because `n` isn't in the exposed set.
  const cteNames = extractCteNames(query);

  let tableRefs: readonly string[];
  try {
    // tableList returns entries like "select::<db>::<table>". We only
    // care about the final segment.
    tableRefs = parser.tableList(query, { database: "sqlite" });
  } catch (err) {
    // Shouldn't happen — sanitizeQuery already parsed successfully — but
    // fail closed if the parser disagrees on the second pass.
    const message = err instanceof Error ? err.message : String(err);
    throw new SqlSafetyError(`Could not introspect table references: ${message}`);
  }

  for (const ref of tableRefs) {
    const parts = ref.split("::");
    const tableName = (parts[parts.length - 1] ?? ref).toLowerCase();

    if (cteNames.has(tableName)) continue;

    if (INTERNAL_TABLE_PREFIXES.some((p) => tableName.startsWith(p))) {
      throw new SqlSafetyError(
        `Access to internal table '${tableName}' is blocked. ` +
          `This is a bookkeeping / SQLite-metadata table and is never exposed.`,
      );
    }

    if (!exposed.has(tableName)) {
      throw new SqlSafetyError(
        `Table '${tableName}' is not currently exposed. ` +
          `Call topline_describe_schema to see what's queryable.`,
      );
    }
  }
}

/** AST helper: collect the aliases from `WITH x AS (...), y AS (...) SELECT ...`. */
function extractCteNames(query: string): ReadonlySet<string> {
  const names = new Set<string>();
  let ast: unknown;
  try {
    ast = parser.astify(query, { database: "sqlite" });
  } catch {
    // sanitizeQuery already validated the query; if astify fails now
    // let the main tableList path handle the follow-up error.
    return names;
  }
  const single = Array.isArray(ast) ? ast[0] : ast;
  if (!single || typeof single !== "object") return names;
  const withClauses =
    (single as { with?: Array<{ name?: { value?: string } | string }> }).with ?? [];
  for (const clause of withClauses) {
    const rawName =
      clause && typeof clause === "object"
        ? typeof clause.name === "string"
          ? clause.name
          : clause.name?.value
        : undefined;
    if (typeof rawName === "string" && rawName.length > 0) {
      names.add(rawName.toLowerCase());
    }
  }
  return names;
}
