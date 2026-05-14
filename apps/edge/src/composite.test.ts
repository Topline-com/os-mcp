import { test } from "node:test";
import assert from "node:assert/strict";
import { parseAuditTime } from "./tools/composite.js";

test("parseAuditTime — empty returns fallback", () => {
  const fallback = new Date("2026-01-01T00:00:00Z");
  const got = parseAuditTime(undefined, fallback);
  assert.equal(got.toISOString(), fallback.toISOString());
});

test("parseAuditTime — 'now' returns now", () => {
  const now = new Date("2026-05-14T18:00:00Z");
  const got = parseAuditTime("now", new Date(0), now);
  assert.equal(got.toISOString(), now.toISOString());
});

test("parseAuditTime — 'this-week-et' returns Monday 00:00 ET of the current week", () => {
  // Thursday 2026-05-14 18:00 UTC is Thursday in ET → week start is Monday 2026-05-11 00:00 ET.
  // ET is EDT (UTC-4) on that date.
  const now = new Date("2026-05-14T18:00:00Z");
  const got = parseAuditTime("this-week-et", new Date(0), now);
  // Monday 2026-05-11 00:00 EDT == 2026-05-11 04:00 UTC.
  assert.equal(got.toISOString(), "2026-05-11T04:00:00.000Z");
});

test("parseAuditTime — '7d' subtracts 7 days from now", () => {
  const now = new Date("2026-05-14T18:00:00Z");
  const got = parseAuditTime("7d", new Date(0), now);
  assert.equal(got.toISOString(), "2026-05-07T18:00:00.000Z");
});

test("parseAuditTime — '24h' subtracts 24 hours", () => {
  const now = new Date("2026-05-14T18:00:00Z");
  const got = parseAuditTime("24h", new Date(0), now);
  assert.equal(got.toISOString(), "2026-05-13T18:00:00.000Z");
});

test("parseAuditTime — YYYY-MM-DD parses as UTC midnight", () => {
  const got = parseAuditTime("2026-05-11", new Date(0));
  assert.equal(got.toISOString(), "2026-05-11T00:00:00.000Z");
});

test("parseAuditTime — RFC3339 parses verbatim", () => {
  const got = parseAuditTime("2026-05-11T12:34:56Z", new Date(0));
  assert.equal(got.toISOString(), "2026-05-11T12:34:56.000Z");
});

test("parseAuditTime — invalid throws", () => {
  assert.throws(() => parseAuditTime("not-a-date", new Date(0)), /invalid time/);
});
