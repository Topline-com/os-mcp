// SQL migration generator.
//
// Given an EntityManifest, produces the CREATE TABLE + CREATE INDEX
// statements LocationDO needs to run on first initialization. The DO
// records the manifest version it was initialized with; future schema
// changes go through numbered migrations so existing tenants get
// incremental updates rather than table drops.

import type { EntityManifest, ColumnDef } from "./types.js";
import { ALL_ENTITIES } from "./entities.js";

/**
 * Render a single column's definition as it appears inside CREATE TABLE.
 *   id TEXT NOT NULL
 *   email TEXT
 *   dnd INTEGER
 */
export function renderColumn(col: ColumnDef): string {
  const parts = [quoteIdent(col.name), col.sqlite_type];
  if (!col.nullable) parts.push("NOT NULL");
  return parts.join(" ");
}

/** Render a single CREATE TABLE for an entity. */
export function renderCreateTable(entity: EntityManifest): string {
  const lines = entity.columns.map((c) => `  ${renderColumn(c)}`);
  lines.push(`  PRIMARY KEY (${quoteIdent(entity.primary_key)})`);
  return `CREATE TABLE IF NOT EXISTS ${quoteIdent(entity.table)} (\n${lines.join(",\n")}\n);`;
}

/** Render all CREATE INDEX statements for an entity's `indexed: true` columns. */
export function renderIndexes(entity: EntityManifest): string[] {
  return entity.columns
    .filter((c) => c.indexed && c.name !== entity.primary_key)
    .map((c) =>
      `CREATE INDEX IF NOT EXISTS ${quoteIdent(
        `idx_${entity.table}_${c.name}`,
      )} ON ${quoteIdent(entity.table)} (${quoteIdent(c.name)});`,
    );
}

/** Render the full boot-time SQL script: all tables, then all indexes. */
export function renderAllMigrations(
  entities: readonly EntityManifest[] = ALL_ENTITIES,
): string {
  const tables = entities.map(renderCreateTable);
  const indexes = entities.flatMap(renderIndexes);
  return [...tables, "", ...indexes].join("\n\n");
}

/** Get the boot-time SQL as a list of individual statements (for batch exec). */
export function migrationStatements(
  entities: readonly EntityManifest[] = ALL_ENTITIES,
): string[] {
  const out: string[] = [];
  for (const e of entities) {
    out.push(renderCreateTable(e));
    for (const idx of renderIndexes(e)) out.push(idx);
  }
  return out;
}

/** Paranoid quoting for identifiers. SQLite accepts double-quoted idents. */
function quoteIdent(name: string): string {
  // Reject anything weird — manifests are authored by us, so bad names are
  // authoring bugs, not attacker input, but fail loudly if someone slips.
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`Invalid SQL identifier: ${name}`);
  }
  return `"${name}"`;
}
