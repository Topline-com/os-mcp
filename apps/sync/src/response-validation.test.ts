import { test } from "node:test";
import assert from "node:assert/strict";
import { ENTITY_BY_TABLE } from "@topline/shared-schema";
import { credentialsContext, toplineFetch, ToplineApiError, parseRetryAfter } from "@topline/shared";
import { responseItems, validateSinglePage, mappedRows } from "./response-validation.js";

test("missing, malformed, truncated and explicitly partial snapshots are rejected", () => {
  for (const response of [{}, { events: null }, { events: {} }, { events: [null] }, { events: [[]] }, { events: [], truncated: true }, { events: [], meta: { truncated: true } }, { events: [], hasMore: true }]) {
    assert.throws(() => responseItems(response, "events"));
  }
  for (const response of [{ total: 2 }, { total: "1" }, { meta: { total: 2 } }, { nextCursor: "next" }, { meta: { hasMore: true } }]) {
    assert.throws(() => validateSinglePage(response, 1));
  }
  assert.deepEqual(responseItems({ events: [] }, "events"), []);
  assert.doesNotThrow(() => validateSinglePage({ events: [], total: 0 }, 0));
});

test("mapping validates the entire page, tenant, parent, IDs and cancellation updates", () => {
  const entity = ENTITY_BY_TABLE.get("appointments")!;
  const scope = { column: "calendar_id", id: "calendar" };
  const raw = { id: "appointment", locationId: "loc", calendarId: "calendar", appointmentStatus: "cancelled" };
  const [row] = mappedRows(entity, [raw], "loc", scope);
  assert.equal(row!.status, "cancelled");
  assert.equal(row!.location_id, "loc");
  for (const items of [[{ ...raw, id: "" }], [raw, raw], [{ ...raw, locationId: "other" }], [{ ...raw, calendarId: "other" }]]) {
    assert.throws(() => mappedRows(entity, items, "loc", scope));
  }
  const [injected] = mappedRows(entity, [{ id: "appointment" }], "loc", scope);
  assert.equal(injected!.calendar_id, "calendar");
  assert.equal(injected!.location_id, "loc");
});

test("Retry-After supports seconds and HTTP dates and sync can persist instead of sleeping", async (context) => {
  const now = Date.parse("2026-10-21T12:00:00Z");
  assert.equal(parseRetryAfter("120", now), 120_000);
  assert.equal(parseRetryAfter("Wed, 21 Oct 2026 12:02:00 GMT", now), 120_000);
  assert.equal(parseRetryAfter("Wed, 21 Oct 2026 11:00:00 GMT", now), 0);
  assert.equal(parseRetryAfter("invalid", now), undefined);
  assert.equal(parseRetryAfter(null, now), undefined);
  let calls = 0;
  context.mock.method(globalThis, "fetch", async () => {
    calls++;
    return new Response("{}", { status: 429, headers: { "retry-after": "120" } });
  });
  await credentialsContext.run({ pit: "synthetic-test-credential", locationId: "loc" }, async () => {
    await assert.rejects(toplineFetch("/calendars/events", { retryRateLimits: false }), (error: unknown) => {
      assert.ok(error instanceof ToplineApiError);
      assert.equal(error.retryAfterMs, 120_000);
      return true;
    });
  });
  assert.equal(calls, 1);
});
