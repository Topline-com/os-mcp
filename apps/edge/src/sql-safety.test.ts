// SQL safety regression matrix.
//
// Run with: npm test
//
// This guard prevents authenticated SQL callers from reaching tables the
// tenant isn't meant to see. It's security-critical, so every accept /
// reject decision belongs in a committed test.
//
// Tests depend implicitly on the current manifest's exposure state:
//   exposed:  every EntityManifest with exposed=true && auditPasses()
//   hidden:   auth/sync internals (_sync_state, _schema_log), SQLite
//             meta tables, AND anything not present in the exposed
//             allowlist (typo'd names, cataloged-only objects, etc.)
// When a manifest flips an entity's exposure, the corresponding test
// cases flip — intended forcing function for review. Every entity in
// entities.ts currently has exposed=true, so "rejects hidden entity"
// coverage uses a synthetic unknown-to-allowlist table name to prove
// the gate still rejects anything not explicitly exposed.

import { describe, it } from "node:test";
import { strictEqual, throws, doesNotThrow, ok } from "node:assert";
import {
  sanitizeQuery,
  enforceExposedTables,
  SqlSafetyError,
  DEFAULT_ROW_CAP,
} from "./sql-safety.js";

// ---------------------------------------------------------------------------
// sanitizeQuery — statement-level gate (SELECT/WITH, LIMIT, pragma funcs)
// ---------------------------------------------------------------------------

describe("sanitizeQuery — accepts", () => {
  const ok_cases = [
    "SELECT * FROM contacts",
    "SELECT id, email FROM contacts WHERE email IS NOT NULL",
    "SELECT type, COUNT(*) AS n FROM contacts GROUP BY type HAVING COUNT(*) > 1",
    "SELECT * FROM contacts ORDER BY updated_at DESC LIMIT 10",
    "WITH n AS (SELECT COUNT(*) AS c FROM contacts) SELECT c FROM n",
    "WITH a AS (SELECT * FROM contacts), b AS (SELECT * FROM pipelines) SELECT * FROM a, b",
    // Legitimate table-valued helpers that superficially look like functions
    "SELECT c.id FROM contacts c, json_each(c.tags) je",
    "SELECT c.id FROM contacts c, json_tree(c.custom_fields) jt",
    // Trailing semicolon is tolerated
    "SELECT 1;",
  ];
  for (const q of ok_cases) {
    it(q, () => doesNotThrow(() => sanitizeQuery(q)));
  }
});

describe("sanitizeQuery — rejects DDL", () => {
  const ddl = [
    "DROP TABLE contacts",
    "CREATE TABLE x (id TEXT)",
    "CREATE INDEX ix ON contacts(email)",
    "ALTER TABLE contacts ADD COLUMN evil TEXT",
    "CREATE VIEW v AS SELECT * FROM contacts",
  ];
  for (const q of ddl) {
    it(q, () => throws(() => sanitizeQuery(q), SqlSafetyError));
  }
});

describe("sanitizeQuery — rejects DML", () => {
  const dml = [
    "INSERT INTO contacts(id) VALUES ('x')",
    "UPDATE contacts SET email = 'z' WHERE id = 'x'",
    "DELETE FROM contacts",
    "REPLACE INTO contacts(id) VALUES ('x')",
  ];
  for (const q of dml) {
    it(q, () => throws(() => sanitizeQuery(q), SqlSafetyError));
  }
});

describe("sanitizeQuery — rejects meta / admin statements", () => {
  const meta = [
    "PRAGMA table_info(contacts)",
    "ATTACH DATABASE '/tmp/evil' AS x",
    "DETACH DATABASE x",
    "VACUUM",
  ];
  for (const q of meta) {
    it(q, () => throws(() => sanitizeQuery(q), SqlSafetyError));
  }
});

describe("sanitizeQuery — rejects pragma_* table-valued FUNCTIONS (parens)", () => {
  // The pragma regex in sanitizeQuery catches function-call forms. The
  // parenless form (e.g. `FROM pragma_database_list`) is a table ref,
  // not a function — tableList sees it and enforceExposedTables rejects
  // it as not-exposed. See the "full customer gate" group below for
  // the full-pipeline verification.
  const pragmas = [
    "SELECT * FROM pragma_table_info('opportunities')",
    "SELECT * FROM pragma_foreign_key_list('contacts')",
    "SELECT * FROM pragma_index_list('pipelines')",
    // Quoted identifier variant — word boundary still matches
    `SELECT * FROM "pragma_table_info"('x')`,
    // Mixed case
    "SELECT * FROM PRAGMA_TABLE_INFO('x')",
    // Via a CTE
    "WITH p AS (SELECT * FROM pragma_table_info('x')) SELECT * FROM p",
  ];
  for (const q of pragmas) {
    it(q, () => {
      try {
        sanitizeQuery(q);
        ok(false, `${q} should have been rejected`);
      } catch (err) {
        ok(err instanceof SqlSafetyError, `expected SqlSafetyError, got ${err}`);
      }
    });
  }
});

describe("sanitizeQuery — pragma_ false-positive guards", () => {
  it("column named pragma_info (no function call) is allowed", () => {
    // Not a function call — no paren after. Should not trigger the regex.
    // Note: this doesn't pass enforceExposedTables because there's no such
    // column, but we're testing the sanitize layer in isolation.
    doesNotThrow(() =>
      sanitizeQuery("SELECT pragma_info FROM contacts"),
    );
  });
});

describe("sanitizeQuery — rejects multi-statement and empty", () => {
  it("two-statement input", () => {
    throws(() => sanitizeQuery("SELECT 1; SELECT 2"), SqlSafetyError);
  });
  it("empty string", () => {
    throws(() => sanitizeQuery(""), SqlSafetyError);
  });
  it("whitespace only", () => {
    throws(() => sanitizeQuery("   \n  "), SqlSafetyError);
  });
});

describe("sanitizeQuery — LIMIT injection and capping", () => {
  it("injects LIMIT when absent", () => {
    const result = sanitizeQuery("SELECT * FROM contacts");
    strictEqual(result.limited, true);
    strictEqual(result.effective_limit, DEFAULT_ROW_CAP);
    ok(/LIMIT\s+5000/i.test(result.sql), `expected LIMIT in: ${result.sql}`);
  });
  it("caps LIMIT > 5000 to 5000", () => {
    const result = sanitizeQuery("SELECT * FROM contacts LIMIT 9999");
    strictEqual(result.effective_limit, DEFAULT_ROW_CAP);
    strictEqual(result.limited, true);
  });
  it("preserves LIMIT <= 5000", () => {
    const result = sanitizeQuery("SELECT * FROM contacts LIMIT 10");
    strictEqual(result.effective_limit, 10);
    strictEqual(result.limited, false);
  });
});

// ---------------------------------------------------------------------------
// enforceExposedTables — tenant-visibility gate
// ---------------------------------------------------------------------------

describe("enforceExposedTables — accepts exposed tables", () => {
  // Exposed per the current manifest (contacts / pipelines / pipeline_stages).
  const queries = [
    "SELECT * FROM contacts",
    "SELECT * FROM pipelines",
    "SELECT * FROM pipeline_stages",
    "SELECT p.name, ps.name FROM pipelines p JOIN pipeline_stages ps ON ps.pipeline_id = p.id",
    "SELECT COUNT(*) FROM contacts WHERE type = 'lead'",
  ];
  for (const q of queries) {
    it(q, () => doesNotThrow(() => enforceExposedTables(q)));
  }
});

describe("enforceExposedTables — rejects unknown / non-allowlisted tables", () => {
  // Every current EntityManifest has exposed=true, so this section
  // proves the gate still rejects anything NOT explicitly in the
  // allowlist — future hidden tables, typos, or attempts to probe
  // marketplace-OAuth-only objects surfaced in catalog.ts
  // (workflow_internals, pipeline_writes, form_writes, etc.).
  const queries = [
    "SELECT * FROM workflow_internals",
    "SELECT * FROM pipeline_writes",
    "SELECT * FROM some_future_table",
    "SELECT * FROM contact_typo",
  ];
  for (const q of queries) {
    it(q, () => throws(() => enforceExposedTables(q), SqlSafetyError));
  }
});

describe("enforceExposedTables — rejects bookkeeping / SQLite metadata", () => {
  const queries = [
    "SELECT * FROM _sync_state",
    "SELECT * FROM _schema_log",
    "SELECT name FROM sqlite_master",
    "SELECT * FROM sqlite_schema",
    "SELECT * FROM sqlite_sequence",
  ];
  for (const q of queries) {
    it(q, () => throws(() => enforceExposedTables(q), SqlSafetyError));
  }
});

describe("enforceExposedTables — CTE aliases exempted", () => {
  it("WITH aliasing an exposed table", () => {
    doesNotThrow(() =>
      enforceExposedTables(
        "WITH n AS (SELECT COUNT(*) AS c FROM contacts) SELECT c FROM n",
      ),
    );
  });
  it("multi-CTE across exposed tables", () => {
    doesNotThrow(() =>
      enforceExposedTables(
        "WITH a AS (SELECT * FROM contacts), b AS (SELECT * FROM pipelines) SELECT * FROM a, b",
      ),
    );
  });
  it("CTE referencing a HIDDEN table still blocks via inner ref", () => {
    // The CTE alias `x` is exempt from allowlist, but an internal
    // bookkeeping table like _sync_state is not — the inner table
    // ref fails regardless of the CTE wrapping.
    throws(
      () =>
        enforceExposedTables(
          "WITH x AS (SELECT * FROM _sync_state) SELECT * FROM x",
        ),
      SqlSafetyError,
    );
  });
});

describe("enforceExposedTables — join including hidden table", () => {
  it("contacts JOIN _sync_state is rejected because internals stay hidden", () => {
    throws(
      () =>
        enforceExposedTables(
          "SELECT c.id FROM contacts c JOIN _sync_state s ON s.entity = c.id",
        ),
      SqlSafetyError,
    );
  });
});

// ---------------------------------------------------------------------------
// Full customer SQL gate — sanitize + enforce chained, exactly the way
// topline_execute_query and POST /query/api/execute-sql run them.
// Every attacker-interesting case must fail somewhere in the chain.
// ---------------------------------------------------------------------------

function runFullCustomerGate(query: string): void {
  const safe = sanitizeQuery(query);
  enforceExposedTables(safe.sql);
}

describe("full customer gate — every pragma variant is blocked", () => {
  const forms = [
    // Function-call forms (caught by sanitizeQuery regex)
    "SELECT * FROM pragma_table_info('opportunities')",
    "SELECT * FROM pragma_foreign_key_list('contacts')",
    "SELECT * FROM pragma_index_list('pipelines')",
    "SELECT * FROM PRAGMA_TABLE_INFO('x')",
    // Parenless forms (caught by enforceExposedTables as unknown table)
    "SELECT name FROM pragma_database_list",
    "SELECT * FROM pragma_database_list",
  ];
  for (const q of forms) {
    it(q, () => throws(() => runFullCustomerGate(q), SqlSafetyError));
  }
});

describe("full customer gate — bookkeeping / sqlite_ / unknown tables all blocked", () => {
  const forms = [
    "SELECT * FROM _sync_state",
    "SELECT * FROM _schema_log",
    "SELECT * FROM _parent_sync_state",
    "SELECT name FROM sqlite_master",
    // Unknown-to-allowlist (typos, future/cataloged-only objects).
    "SELECT * FROM some_future_table",
    "SELECT c.id FROM contacts c JOIN _sync_state s ON c.id = s.entity",
  ];
  for (const q of forms) {
    it(q, () => throws(() => runFullCustomerGate(q), SqlSafetyError));
  }
});

describe("full customer gate — legitimate analytics queries pass", () => {
  const forms = [
    "SELECT COUNT(*) AS n FROM contacts",
    "SELECT type, COUNT(*) FROM contacts GROUP BY type",
    "SELECT * FROM contacts ORDER BY updated_at DESC LIMIT 20",
    "WITH n AS (SELECT COUNT(*) AS c FROM contacts) SELECT c FROM n",
    "SELECT p.name, COUNT(ps.id) AS stage_count FROM pipelines p LEFT JOIN pipeline_stages ps ON ps.pipeline_id = p.id GROUP BY p.id, p.name",
    "SELECT c.id, je.value AS tag FROM contacts c, json_each(c.tags) je",
    // Newly exposed — opportunities + conversations. These are the
    // Streamlined-style analytics queries the MCP ships to unlock.
    "SELECT COUNT(*) FROM opportunities WHERE status = 'open'",
    "SELECT c.id, COUNT(o.id) AS n FROM contacts c LEFT JOIN opportunities o ON o.contact_id = c.id GROUP BY c.id",
    "SELECT type, COUNT(*) FROM conversations GROUP BY type",
    // Messages now exposed — the Streamlined-demo query shape:
    // first inbound call → first outbound callback per contact.
    "SELECT m.contact_id, MIN(CASE WHEN direction='inbound' THEN date_added END) AS first_in, MIN(CASE WHEN direction='outbound' THEN date_added END) AS first_out FROM messages m WHERE type IN ('TYPE_CALL','TYPE_IVR_CALL') GROUP BY m.contact_id",
    "SELECT COUNT(*) FROM messages WHERE type='TYPE_CALL' AND direction='inbound' AND call_duration_seconds <= 5",
    // Appointments now exposed (per-parent backfill over calendars).
    // Closes the find_references P1 where kind=calendar returned
    // zero refs even when upstream appointments existed.
    "SELECT COUNT(*) FROM appointments WHERE status = 'confirmed'",
    "SELECT cal.name, COUNT(a.id) FROM calendars cal LEFT JOIN appointments a ON a.calendar_id = cal.id GROUP BY cal.id, cal.name",
    // Workflows shallow sync.
    "SELECT status, COUNT(*) FROM workflows GROUP BY status",
  ];
  for (const q of forms) {
    it(q, () => doesNotThrow(() => runFullCustomerGate(q)));
  }
});
