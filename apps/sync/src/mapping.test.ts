// Tests for the sync worker's row mapper. The timestamp normalization
// logic is the most important thing to lock down — it's how we paper
// over GHL's per-endpoint inconsistency (some return ISO, some return
// ms epoch, some return epoch-as-string) so the downstream SQL layer
// only ever sees ISO 8601.

import { describe, it } from "node:test";
import { strictEqual, deepStrictEqual } from "node:assert";
import { mapRow, getByPath } from "./mapping.js";
import type { EntityManifest, ColumnDef } from "@topline/shared-schema";

// ---------------------------------------------------------------------------
// Small helper: build a synthetic single-column entity so tests don't need
// to import the real manifest (and couple to its current state).
// ---------------------------------------------------------------------------

function entity(col: ColumnDef): EntityManifest {
  return {
    table: "synthetic",
    description: "",
    phase: 2,
    primary_key: col.name,
    columns: [col],
    backfill: { endpoint: "/x", method: "GET", pagination: "none" },
    incremental: { type: "poll_full", poll_interval_minutes: 15, filter_ready: true },
    audit: {
      live_tested: false,
      stable_pk: false,
      backfill_path: false,
      incremental_path: false,
      update_cursor: false,
    },
    exposed: false,
  };
}

// ---------------------------------------------------------------------------
// getByPath — dot-notation walk used by mapRow + backfill cursors
// ---------------------------------------------------------------------------

describe("getByPath", () => {
  it("shallow key", () => {
    strictEqual(getByPath({ a: 1 }, "a"), 1);
  });
  it("nested key", () => {
    strictEqual(getByPath({ a: { b: { c: 42 } } }, "a.b.c"), 42);
  });
  it("missing key returns undefined", () => {
    strictEqual(getByPath({ a: 1 }, "a.b.c"), undefined);
  });
  it("null intermediate returns undefined", () => {
    strictEqual(getByPath({ a: null }, "a.b"), undefined);
  });
  it("array index via numeric segment works (arrays are objects keyed by string digits)", () => {
    // JS arrays behave like Records when keyed by string digits, so
    // getByPath walks them for free. Not a feature manifests rely on
    // (items_field handles array-of-records separately), but this
    // locks the incidental behavior so it doesn't silently change.
    strictEqual(getByPath({ xs: [{ id: 1 }] }, "xs.0.id"), 1);
  });
});

// ---------------------------------------------------------------------------
// mapRow — column mapping, timestamp normalization, null handling
// ---------------------------------------------------------------------------

describe("mapRow — basic column mapping", () => {
  it("source_path override pulls from a different upstream key", () => {
    const e = entity({
      name: "first_name",
      sqlite_type: "TEXT",
      nullable: true,
      description: "",
      source_path: "firstName",
    });
    const row = mapRow(e, { firstName: "Jane" });
    strictEqual(row.first_name, "Jane");
  });

  it("defaults source_path to column name", () => {
    const e = entity({ name: "email", sqlite_type: "TEXT", nullable: true, description: "" });
    const row = mapRow(e, { email: "x@y.com" });
    strictEqual(row.email, "x@y.com");
  });

  it("missing upstream value becomes null", () => {
    const e = entity({ name: "phone", sqlite_type: "TEXT", nullable: true, description: "" });
    const row = mapRow(e, {});
    strictEqual(row.phone, null);
  });

  it("_synced_at is omitted (DO stamps it server-side)", () => {
    const e = entity({
      name: "_synced_at",
      sqlite_type: "TEXT",
      nullable: false,
      description: "",
    });
    const row = mapRow(e, { _synced_at: "2026-04-23T00:00:00Z" });
    strictEqual("_synced_at" in row, false);
  });
});

describe("mapRow — timestamp_format: ms_epoch", () => {
  const e = entity({
    name: "last_message_date",
    sqlite_type: "TEXT",
    nullable: true,
    description: "",
    source_path: "lastMessageDate",
    timestamp_format: "ms_epoch",
  });

  it("converts ms epoch Number to ISO 8601", () => {
    const row = mapRow(e, { lastMessageDate: 1776983510911 });
    strictEqual(row.last_message_date, "2026-04-23T22:31:50.911Z");
  });

  it("converts ms epoch as numeric STRING to ISO 8601 (defensive)", () => {
    const row = mapRow(e, { lastMessageDate: "1776983510911" });
    strictEqual(row.last_message_date, "2026-04-23T22:31:50.911Z");
  });

  it("leaves null unchanged", () => {
    const row = mapRow(e, { lastMessageDate: null });
    strictEqual(row.last_message_date, null);
  });

  it("leaves missing field as null", () => {
    const row = mapRow(e, {});
    strictEqual(row.last_message_date, null);
  });

  it("rejects implausible epoch values (returns null)", () => {
    // Negative / absurdly-large values are almost always a bug upstream.
    // Better to write null than a garbage timestamp.
    const negative = mapRow(e, { lastMessageDate: -1 });
    strictEqual(negative.last_message_date, null);
    const way_future = mapRow(e, { lastMessageDate: 99_999_999_999_999 });
    strictEqual(way_future.last_message_date, null);
  });
});

describe("mapRow — timestamp_format: iso8601", () => {
  const e = entity({
    name: "created_at",
    sqlite_type: "TEXT",
    nullable: true,
    description: "",
    source_path: "createdAt",
    timestamp_format: "iso8601",
  });

  it("passes ISO strings through unchanged", () => {
    const iso = "2026-04-23T21:20:44.149Z";
    const row = mapRow(e, { createdAt: iso });
    strictEqual(row.created_at, iso);
  });
});

describe("mapRow — no timestamp_format flag", () => {
  it("number values pass through for the DO's coerceForSqlite to handle", () => {
    // This is the default path — mapRow doesn't guess the intent,
    // coerceForSqlite on the DO side handles final storage conversion.
    const e = entity({
      name: "score",
      sqlite_type: "INTEGER",
      nullable: true,
      description: "",
    });
    const row = mapRow(e, { score: 42 });
    strictEqual(row.score, 42);
  });
});

describe("mapRow — multi-column entity", () => {
  // Sanity check: mapRow handles a realistic multi-column definition
  // with mixed timestamp formats.
  const e: EntityManifest = {
    table: "conversations",
    description: "",
    phase: 1,
    primary_key: "id",
    columns: [
      { name: "id", sqlite_type: "TEXT", nullable: false, description: "" },
      {
        name: "last_message_date",
        sqlite_type: "TEXT",
        nullable: true,
        description: "",
        source_path: "lastMessageDate",
        timestamp_format: "ms_epoch",
      },
      { name: "type", sqlite_type: "TEXT", nullable: true, description: "" },
    ],
    backfill: { endpoint: "/x", method: "GET", pagination: "none" },
    incremental: { type: "poll_full", poll_interval_minutes: 15, filter_ready: true },
    audit: {
      live_tested: false,
      stable_pk: false,
      backfill_path: false,
      incremental_path: false,
      update_cursor: false,
    },
    exposed: false,
  };

  it("normalizes ms_epoch fields while passing others through", () => {
    const row = mapRow(e, {
      id: "abc",
      lastMessageDate: 1776983510911,
      type: "TYPE_PHONE",
    });
    deepStrictEqual(row, {
      id: "abc",
      last_message_date: "2026-04-23T22:31:50.911Z",
      type: "TYPE_PHONE",
    });
  });
});
