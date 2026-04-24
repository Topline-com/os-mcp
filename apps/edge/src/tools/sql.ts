// The four SQL-surface MCP tools. All four operate on the caller's
// LocationDO and respect the manifest's `exposed` gate — hidden tables
// never show up, no matter how you ask.
//
// Tool naming follows the rest of the server's `topline_` prefix.

import { peekLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { str, obj, arr } from "@topline/shared";
import { edgeContext } from "../request-context.js";
import { locationClient } from "../location-do-client.js";
import { sanitizeQuery, SqlSafetyError } from "../sql-safety.js";

function getClient() {
  const locId = peekLocationId();
  if (!locId) {
    throw new Error(
      "No location in the current request context. This tool requires an authenticated MCP connection with a resolved location_id.",
    );
  }
  const ctx = edgeContext.getStore();
  if (!ctx) {
    throw new Error(
      "Edge request context is not set. This indicates a bug in the worker's request wiring.",
    );
  }
  return locationClient(ctx.location_do, locId);
}

export const tools: ToolDef[] = [
  {
    name: "topline_describe_schema",
    description:
      "Overview of every table currently exposed to SQL queries in this sub-account's data warehouse. Returns table names, one-line descriptions, row counts, and a short SQLite-dialect cheat sheet. Call this FIRST when the user asks anything analytics-flavored ('how many', 'group by', 'trend', 'compare', 'duplicate'). It's cheap and tells you what's queryable. Follow up with topline_explain_tables on the ones you want to use, then topline_execute_query with the SQL.",
    inputSchema: obj({}, []),
    handler: async () => {
      const client = getClient();
      return await client.describeSchema();
    },
  },

  {
    name: "topline_explain_tables",
    description:
      "Per-column detail for one or more tables returned by topline_describe_schema. Gives column names, SQLite types, nullability, JSON-column flags, enum values for closed-set text columns, foreign-key hints for joins, and approximate row counts. Call this before writing a SELECT so your WHERE / JOIN clauses use real column names and valid enum values. Rejects tables that aren't in the exposed set.",
    inputSchema: obj(
      {
        tables: arr(str("Table name, e.g. 'contacts'"), "One or more table names to explain"),
      },
      ["tables"],
    ),
    handler: async (args) => {
      const client = getClient();
      const tables = args.tables as string[];
      if (!Array.isArray(tables) || tables.length === 0) {
        throw new Error("tables must be a non-empty array of table names");
      }
      return await client.explainTables(tables);
    },
  },

  {
    name: "topline_execute_query",
    description:
      "Run a read-only SQL query against this sub-account's SQLite warehouse. ONE SELECT or WITH...SELECT statement at a time. DDL, DML, PRAGMA, ATTACH, and admin commands are rejected by the parser before they reach the database. Results are capped at 5000 rows; larger result sets come back with truncated: true. Returns { columns, rows, elapsed_ms, truncated, effective_limit, rewritten_sql }. SQLite dialect — no DATE_TRUNC (use strftime), JSON columns need json_extract / json_each, timestamps are ISO 8601 strings that compare lexicographically.",
    inputSchema: obj(
      {
        query: str(
          "A single SELECT or WITH ... SELECT statement. Example: \"SELECT status, COUNT(*) AS n FROM opportunities GROUP BY status ORDER BY n DESC\"",
        ),
      },
      ["query"],
    ),
    handler: async (args) => {
      const client = getClient();
      const queryInput = String(args.query ?? "");
      let safe;
      try {
        safe = sanitizeQuery(queryInput);
      } catch (err) {
        if (err instanceof SqlSafetyError) {
          // Return as structured error so the LLM can self-correct.
          return {
            error: err.message,
            rejected_query: queryInput,
          };
        }
        throw err;
      }
      const result = await client.executeQuery(safe.sql, []);
      // Truncation reflects whether the result hit the cap, not whether
      // we injected one. If rows.length === effective_limit, there may be
      // more rows upstream (LLM should paginate with OFFSET or narrow the
      // query). If rows.length < effective_limit, the result is complete.
      const hitCap = result.rows.length >= safe.effective_limit;
      return {
        columns: result.columns,
        rows: result.rows,
        elapsed_ms: result.elapsed_ms,
        truncated: result.truncated || hitCap,
        effective_limit: safe.effective_limit,
        rewritten_sql: safe.sql,
      };
    },
  },

  {
    name: "topline_utilize_api",
    description:
      "Describes the HTTP query API you can point Looker Studio, Retool, Lovable, Claude Code, n8n, or curl at for live-data dashboards. Returns URL shapes, auth format, example curl commands, and guidance for wiring up a dashboard. Use this when the user asks about building dashboards, embedding the data in another tool, or when a query is too complex for chat and they'd rather save it as a saved view.",
    inputSchema: obj({}, []),
    handler: async () => {
      return {
        overview:
          "The same data warehouse you queried with topline_execute_query is also exposed over plain HTTP. Any dashboard tool that can make an authenticated POST with a JSON body can hit it.",
        base_url: "https://os-mcp.topline.com",
        auth: {
          type: "Bearer token (same token that authorizes /mcp)",
          header: "Authorization: Bearer <access_token>",
          how_to_get:
            "For interactive use: OAuth through Claude / ChatGPT. For programmatic / dashboard use: generate a long-lived token at https://os-mcp.topline.com/connect by pasting your Private Integration Token and Location ID.",
        },
        endpoints: [
          {
            path: "GET /query/api/get-overview",
            description: "Same payload as topline_describe_schema.",
            example:
              'curl -H "Authorization: Bearer $TOKEN" https://os-mcp.topline.com/query/api/get-overview',
          },
          {
            path: "GET /query/api/explain-tables?table=<name>&table=<name>",
            description:
              "Same payload as topline_explain_tables. Repeat the `table` query param for each table.",
            example:
              'curl -H "Authorization: Bearer $TOKEN" "https://os-mcp.topline.com/query/api/explain-tables?table=contacts&table=opportunities"',
          },
          {
            path: "POST /query/api/execute-sql",
            description:
              "Same SELECT/WITH gate as topline_execute_query. Body: { sql: string }. Returns the same result shape: { columns, rows, elapsed_ms, truncated, effective_limit, rewritten_sql }.",
            example:
              'curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d \'{"sql":"SELECT COUNT(*) FROM contacts"}\' https://os-mcp.topline.com/query/api/execute-sql',
          },
        ],
        dashboard_integration_hints: [
          "Looker Studio: add a Community Connector that POSTs to /query/api/execute-sql with the user's token. Each chart = one saved SQL string.",
          "Retool: use a Resource of type 'REST API', base URL https://os-mcp.topline.com, auth Bearer. Queries become 'POST /query/api/execute-sql' resource queries with {{sql}} bindings.",
          "n8n: HTTP Request node, same pattern.",
          "Claude Code / Cursor: any script can shell out to curl against these endpoints.",
        ],
      };
    },
  },
];
