import assert from "node:assert/strict";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import test, { type TestContext } from "node:test";

import { encryptPit, type StoredConnection } from "@topline/shared-auth";
import { ENTITY_BY_TABLE, renderCreateTable } from "@topline/shared-schema";
import {
  backfillEntity,
  incrementalEntity,
  type SyncEnv,
} from "../apps/sync/src/backfill.js";
import { mapRow } from "../apps/sync/src/mapping.js";
import { LocationDO } from "../packages/shared-do/src/location-do.js";
import { SYNC_LEASE_TTL_MS } from "../packages/shared-do/src/sync-storage.js";

const LOCATION = "sync-test-location";
const CONNECTION = "sync-test-connection";
const CALENDAR = "calendar-a";
const START = Date.parse("2026-10-21T00:00:00.000Z");
const POLL = 15 * 60_000;
const OPTIONS = { concurrency: false, timeout: 10_000 };

function appointment(id = "appointment-a", calendarId = CALENDAR) {
  return {
    id,
    locationId: LOCATION,
    calendarId,
    title: "Existing appointment",
    appointmentStatus: "confirmed",
    startTime: "2026-10-21T12:00:00.000Z",
    endTime: "2026-10-21T12:30:00.000Z",
    dateUpdated: "2026-10-20T12:00:00.000Z",
  };
}

/** Adapt storage only: migrations, RPC methods and orchestration are production code. */
function sqliteStorage(db: DatabaseSync) {
  let appointmentRowsWritten = 0;
  return {
    get appointmentRowsWritten() { return appointmentRowsWritten; },
    sql: {
      exec(query: string, ...bindings: SQLInputValue[]) {
        const statement = db.prepare(query);
        const columnNames = statement.columns().map((column) => column.name);
        const rows = columnNames.length ? statement.all(...bindings) : [];
        // Node reports changed rows, not production index-write billing.
        const rowsWritten = columnNames.length ? 0 : Number(statement.run(...bindings).changes);
        if (/^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"?appointments\b/i.test(query)) {
          appointmentRowsWritten += rowsWritten;
        }
        const iterator = rows[Symbol.iterator]();
        return Object.assign(iterator, {
          columnNames,
          rowsWritten,
          toArray: () => Array.from(iterator),
          one() {
            const remaining = Array.from(iterator);
            assert.equal(remaining.length, 1, "one() requires exactly one row");
            return remaining[0];
          },
        });
      },
    },
    transactionSync<T>(callback: () => T): T {
      db.exec("BEGIN");
      try {
        const result = callback();
        db.exec("COMMIT");
        return result;
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
}

async function fixture(t: TestContext, appointments = [appointment()]) {
  t.mock.timers.enable({ apis: ["Date"], now: START - POLL });
  const db = new DatabaseSync(":memory:");
  t.after(() => db.close());
  const storage = sqliteStorage(db);
  const objectId = { toString: () => "sync-test-tenant-object" };
  const location = new LocationDO(
    { id: objectId, storage } as unknown as ConstructorParameters<typeof LocationDO>[0],
    {},
  );

  // Legacy seeding must precede the first lease, which permanently fences mutations.
  await location.upsertRows("calendars", [...new Set([CALENDAR, ...appointments.map((row) => row.calendarId)])]
    .map((id) => mapRow(ENTITY_BY_TABLE.get("calendars")!, { id, locationId: LOCATION })));
  await location.upsertRows("appointments", appointments
    .map((row) => mapRow(ENTITY_BY_TABLE.get("appointments")!, row)));

  const signingSecret = "synthetic-sync-test-signing-secret";
  const pit = "synthetic-sync-test-token";
  const encrypted = await encryptPit(pit, signingSecret);
  const stored: StoredConnection = {
    location_id: LOCATION,
    pit_ct: encrypted.ct,
    pit_iv: encrypted.iv,
    brand_name: "Test",
    created_at: new Date().toISOString(),
    last_verified_at: new Date().toISOString(),
    source: "test",
  };
  const connections = new Map([[CONNECTION, JSON.stringify(stored)]]);
  const env = {
    TOKEN_SIGNING_SECRET: signingSecret,
    CONNECTIONS: {
      async get(key: string, format: string) {
        assert.equal(format, "text");
        return connections.get(key) ?? null;
      },
    },
    CONNECTION_AUTH_DO: {
      idFromName(name: string) { assert.equal(name, CONNECTION); return name; },
      get(id: string) {
        assert.equal(id, CONNECTION);
        return {
          async getOrBootstrap(input: unknown) {
            assert.deepEqual(input, { location_id: LOCATION, credential_exists: true });
            return { status: "active", location_id: LOCATION };
          },
        };
      },
    },
    LOCATION_DO: {
      idFromName(name: string) { assert.equal(name, LOCATION); return objectId; },
      get(id: unknown) { assert.equal(id, objectId); return location; },
    },
  } as unknown as SyncEnv;

  const previousBaseUrl = process.env.TOPLINE_API_BASE_URL;
  process.env.TOPLINE_API_BASE_URL = "https://crm.example.test";
  t.after(() => {
    if (previousBaseUrl === undefined) delete process.env.TOPLINE_API_BASE_URL;
    else process.env.TOPLINE_API_BASE_URL = previousBaseUrl;
  });
  const requests: URL[] = [];
  let respond: (url: URL, init?: RequestInit) => Response | Promise<Response> = () => {
    throw new Error("Unexpected request; network access is disabled in this fixture");
  };
  t.mock.method(globalThis, "fetch", async (input: string | URL | Request, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    requests.push(url);
    assert.equal(url.origin, "https://crm.example.test");
    assert.equal(init?.method, url.pathname === "/contacts/search" ? "POST" : "GET");
    assert.ok(new Headers(init?.headers).get("Authorization") === `Bearer ${pit}`);
    return respond(url, init);
  });
  t.mock.timers.setTime(START);
  return {
    env, location, storage, requests, db,
    at: (now: number) => t.mock.timers.setTime(now),
    respond: (handler: typeof respond) => { respond = handler; },
    rows: () => db.prepare("SELECT * FROM appointments ORDER BY id").all(),
    parent: (id = CALENDAR) => db.prepare(
      "SELECT * FROM _parent_sync_state WHERE entity = 'appointments' AND parent_id = ?",
    ).get(id),
    backfill: (entity = "appointments") => backfillEntity(env, CONNECTION, entity),
    incremental: (entity = "appointments") => incrementalEntity(env, CONNECTION, entity),
  };
}

test("identical appointment refreshes write no appointment rows at the next cron boundary", OPTIONS, async (t) => {
  const h = await fixture(t);
  const original = h.rows();
  const writesBefore = h.storage.appointmentRowsWritten;
  h.respond((url) => {
    assert.equal(url.pathname, "/calendars/events");
    assert.equal(url.searchParams.get("calendarId"), CALENDAR);
    assert.equal(url.searchParams.get("locationId"), LOCATION);
    h.at(Date.now() + 30_000);
    return Response.json({ events: [appointment()] });
  });

  const initial = await h.backfill();
  assert.equal(initial.stopped_reason, "empty_page");
  assert.equal(initial.data_rows_written, 0);
  assert.equal(initial.unchanged_rows, 1);
  h.at(START + POLL);
  const refresh = await h.incremental();
  assert.equal(h.requests.length, 2, "completion latency must not skip the next cron bucket");
  assert.equal(refresh.stopped_reason, "empty_page");
  assert.equal(refresh.data_rows_written, 0);
  assert.equal(refresh.unchanged_rows, 1);
  assert.equal(refresh.row_count_after, 1);
  assert.equal(refresh.pending_parents, 0);
  assert.equal(h.storage.appointmentRowsWritten, writesBefore);
  assert.deepEqual(h.rows(), original, "including the original observation timestamp");
});

const changed = { ...appointment(), title: "Must not be persisted" };
const invalidEnvelopes = [
  ["missing item field", {}],
  ["malformed item collection", { events: {} }],
  ["malformed row after a valid row", { events: [changed, null] }],
  ["explicit truncation", { events: [changed], truncated: true }],
  ["advertised next page", { events: [changed], hasMore: true }],
  ["inconsistent total", { events: [changed], meta: { total: 2 } }],
] as const;

for (const mode of ["backfill", "incremental"] as const) {
  for (const [label, envelope] of invalidEnvelopes) {
    test(`${mode}: ${label} retains data and leaves the sweep incomplete`, OPTIONS, async (t) => {
      const h = await fixture(t);
      if (mode === "incremental") {
        h.respond(() => Response.json({ events: [appointment()] }));
        assert.equal((await h.backfill()).stopped_reason, "empty_page");
        h.at(START + POLL);
      }
      const before = h.rows();
      const writesBefore = h.storage.appointmentRowsWritten;
      const lastSuccess = (await h.location.getSyncState()).appointments.last_success_at;
      const requestsBefore = h.requests.length;
      h.respond(() => Response.json(envelope));

      const result = await h[mode]();
      assert.equal(h.requests.length, requestsBefore + 1);
      assert.equal(result.stopped_reason, "error");
      assert.equal(result.failed_parents?.length, 1);
      assert.equal(result.failed_parents?.[0].parent_id, CALENDAR);
      assert.equal(result.data_rows_written, 0);
      assert.equal(result.pending_parents, 1);
      assert.equal(result.row_count_after, 1);
      assert.deepEqual(h.rows(), before);
      assert.equal(h.storage.appointmentRowsWritten, writesBefore);
      assert.equal(h.parent()?.backfill_complete, 0);
      assert.equal(h.parent()?.status, "failed");
      const state = (await h.location.getSyncState()).appointments;
      assert.equal(state.last_success_at, lastSuccess);
      assert.ok(state.drain_started_at, "an invalid response cannot finish the sweep");
      if (mode === "backfill") assert.equal(state.backfill_complete, false);
    });
  }
}

for (const returnedCount of [1, 0]) {
  test(`appointment snapshot returning ${returnedCount} rows retains omitted history`, OPTIONS, async (t) => {
    const appointments = [appointment(), appointment("appointment-b")];
    const h = await fixture(t, appointments);
    h.respond(() => Response.json({ events: appointments }));
    assert.equal((await h.backfill()).stopped_reason, "empty_page");
    const before = h.rows();
    const writesBefore = h.storage.appointmentRowsWritten;
    h.at(START + POLL);
    h.respond(() => Response.json({ events: appointments.slice(0, returnedCount) }));

    const result = await h.incremental();
    assert.equal(h.requests.length, 2);
    assert.equal(result.stopped_reason, "empty_page");
    assert.equal(result.missing_retained, 2 - returnedCount);
    assert.equal(result.unchanged_rows, returnedCount);
    assert.equal(result.data_rows_written, 0);
    assert.equal(result.row_count_after, 2);
    assert.equal(result.pending_parents, 0);
    assert.deepEqual(h.rows(), before);
    assert.equal(h.storage.appointmentRowsWritten, writesBefore);
    assert.equal(h.parent()?.status, "complete");
  });
}

test("rate-limited parents remain pending without an early request and recover when due", OPTIONS, async (t) => {
  const h = await fixture(t);
  // A regression to in-request sleeps should fail immediately, not wait for Retry-After.
  t.mock.method(globalThis, "setTimeout", () => { throw new Error("Sync must persist retry delays"); });
  h.respond(() => Response.json({}, { status: 429, headers: { "Retry-After": "1800" } }));
  const failed = await h.backfill();
  assert.equal(failed.stopped_reason, "error");
  assert.equal(h.requests.length, 1);
  assert.equal(failed.pending_parents, 1);
  assert.equal(h.parent()?.failure_count, 1);
  assert.equal(h.parent()?.retry_due_at, new Date(START + 2 * POLL).toISOString());
  const drainStart = (await h.location.getSyncState()).appointments.drain_started_at;

  h.at(START + POLL);
  const waiting = await h.incremental();
  assert.equal(h.requests.length, 1, "a pending retry must not make another upstream request early");
  assert.equal(waiting.stopped_reason, "page_cap_hit");
  assert.equal(waiting.pending_parents, 1);
  assert.equal(waiting.data_rows_written, 0);
  assert.equal(h.parent()?.failure_count, 1);
  const waitingState = (await h.location.getSyncState()).appointments;
  assert.equal(waitingState.last_success_at, null);
  assert.equal(waitingState.backfill_complete, false);
  assert.equal(waitingState.drain_started_at, drainStart);

  h.at(START + 2 * POLL);
  h.respond(() => Response.json({ events: [appointment()] }));
  const recovered = await h.incremental();
  assert.equal(h.requests.length, 2);
  assert.equal(recovered.stopped_reason, "empty_page");
  assert.equal(recovered.pending_parents, 0);
  assert.equal(h.parent()?.failure_count, 0);
  assert.equal(h.parent()?.retry_due_at, null);
  assert.equal(h.parent()?.status, "complete");
  const state = (await h.location.getSyncState()).appointments;
  assert.equal(state.backfill_complete, true);
  assert.equal(state.drain_started_at, null);
  assert.ok(state.last_success_at);
});

test("a calendar in backoff does not freeze healthy calendars on the next cron", OPTIONS, async (t) => {
  const healthy = appointment();
  const delayed = appointment("appointment-b", "calendar-b");
  const h = await fixture(t, [healthy, delayed]);
  t.mock.method(globalThis, "setTimeout", () => { throw new Error("Sync must persist retry delays"); });
  let healthyTitle = healthy.title;
  h.respond((url) => {
    const calendar = url.searchParams.get("calendarId");
    h.at(Date.now() + 15_000);
    if (calendar === delayed.calendarId) {
      return Response.json({}, { status: 429, headers: { "Retry-After": "3600" } });
    }
    assert.equal(calendar, healthy.calendarId);
    return Response.json({ events: [{ ...healthy, title: healthyTitle }] });
  });
  const initial = await h.backfill();
  assert.equal(initial.stopped_reason, "error");
  assert.equal(initial.pending_parents, 1);
  assert.equal(h.requests.length, 2);
  const delayedBefore = h.rows().find((row) => row.id === delayed.id);
  const retryDue = h.parent(delayed.calendarId)?.retry_due_at;

  h.at(START + POLL);
  healthyTitle = "Updated during the other calendar's backoff";
  const nextCron = await h.incremental();
  assert.equal(h.requests.filter((url) => url.searchParams.get("calendarId") === CALENDAR).length, 2);
  assert.equal(h.requests.filter((url) => url.searchParams.get("calendarId") === delayed.calendarId).length, 1);
  assert.equal(nextCron.pending_parents, 1);
  assert.equal(nextCron.data_rows_written, 1);
  assert.notEqual(nextCron.stopped_reason, "empty_page");
  assert.equal(h.rows().find((row) => row.id === healthy.id)?.title, healthyTitle);
  assert.deepEqual(h.rows().find((row) => row.id === delayed.id), delayedBefore);
  assert.equal(h.parent(delayed.calendarId)?.retry_due_at, retryDue);
  assert.equal(h.parent(delayed.calendarId)?.failure_count, 1);
  assert.equal((await h.location.getSyncState()).appointments.last_success_at, null);
});

for (const [entity, intervalMinutes] of [["tags", 60], ["pipelines", 1440]] as const) {
  test(`${entity}: cadence skips repeat requests and resumes at the UTC bucket despite fetch latency`, OPTIONS, async (t) => {
    const h = await fixture(t);
    h.respond(() => {
      h.at(Date.now() + 30_000);
      return Response.json({ [entity]: [{ id: `${entity}-a`, locationId: LOCATION }] });
    });
    assert.equal((await h.backfill(entity)).stopped_reason, "empty_page");
    const due = START + intervalMinutes * 60_000;
    for (const now of [START + POLL, due - 1]) {
      h.at(now);
      const repeat = await h.incremental(entity);
      assert.equal(repeat.stopped_reason, "skipped");
      assert.equal(repeat.skipped, "not_due");
      assert.equal(repeat.data_rows_written, 0);
      assert.equal(h.requests.length, 1);
    }
    assert.equal((await h.backfill(entity)).skipped, "not_due");
    h.at(due);
    assert.equal((await h.incremental(entity)).stopped_reason, "empty_page");
    assert.equal(h.requests.length, 2);
  });
}

test("overlapping backfill and incremental invocations share the tenant lease", OPTIONS, async (t) => {
  const h = await fixture(t);
  let enter!: () => void;
  let release!: () => void;
  const entered = new Promise<void>((resolve) => { enter = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  t.after(() => release());
  h.respond(async () => {
    enter();
    await gate;
    return Response.json({ events: [appointment()] });
  });
  const first = h.backfill();
  try {
    await Promise.race([entered, first.then(() => { throw new Error("First sync never reached fetch"); })]);
    const overlaps = await Promise.all([h.backfill(), h.incremental(), h.incremental("tags")]);
    for (const result of overlaps) {
      assert.equal(result.stopped_reason, "skipped");
      assert.equal(result.skipped, "busy");
      assert.equal(result.data_rows_written, 0);
    }
    assert.equal(h.requests.length, 1);
  } finally {
    release();
    await first;
  }
  assert.equal((await first).stopped_reason, "empty_page");
  assert.equal(h.requests.length, 1);
  assert.equal((await h.incremental()).skipped, "not_due");
});

function contact(id: string, updatedAt: string) {
  return { id, locationId: LOCATION, firstName: id, dateUpdated: updatedAt };
}

function opportunity(id: string, updatedAt = "2026-10-20T12:00:00.000Z") {
  return { id, locationId: LOCATION, name: id, status: "open", updatedAt };
}

async function opportunityFixture(t: TestContext) {
  const h = await fixture(t, []);
  h.at(START - POLL);
  await h.location.upsertRows("opportunities", [mapRow(
    ENTITY_BY_TABLE.get("opportunities")!, opportunity("opportunity-stale"),
  )]);
  h.at(START);
  return {
    ...h,
    rows: () => h.db.prepare("SELECT * FROM opportunities ORDER BY id").all(),
  };
}

for (const resumed of [false, true]) {
  test(`opportunities ${resumed ? "resumed" : "fresh"} drain completes on its current compound cursor echo`, OPTIONS, async (t) => {
    const h = await opportunityFixture(t);
    const first = opportunity("opportunity-a");
    const last = opportunity("opportunity-b", "2026-10-20T11:00:00.000Z");
    const cursor = { startAfter: Date.parse(first.updatedAt), startAfterId: first.id };
    h.respond((url) => {
      assert.equal(url.pathname, "/opportunities/search");
      assert.equal(url.searchParams.get("location_id"), LOCATION);
      assert.ok(h.requests.length <= (resumed ? 3 : 2), "the EOF echo must stop fetching");
      if (h.requests.length === 1) {
        assert.equal(url.searchParams.get("startAfter"), null);
        assert.equal(url.searchParams.get("startAfterId"), null);
        return Response.json({ opportunities: [first], meta: cursor });
      }
      assert.equal(url.searchParams.get("startAfter"), String(cursor.startAfter));
      assert.equal(url.searchParams.get("startAfterId"), cursor.startAfterId);
      if (resumed && h.requests.length === 2) {
        return Response.json({ message: "Retry this page" }, { status: 429 });
      }
      return Response.json({ opportunities: [last], meta: cursor });
    });

    let result = await h.backfill("opportunities");
    if (resumed) {
      assert.equal(result.stopped_reason, "error");
      assert.equal(result.pruned_stale_rows ?? 0, 0);
      const interrupted = (await h.location.getSyncState()).opportunities;
      assert.equal(interrupted.cursor, JSON.stringify(cursor));
      assert.equal(interrupted.drain_started_at, new Date(START).toISOString());
      assert.equal(interrupted.backfill_complete, false);
      assert.equal(interrupted.last_success_at, null);
      h.at(START + POLL);
      result = await h.incremental("opportunities");
    }

    assert.equal(result.stopped_reason, "empty_page");
    assert.equal(result.pages, resumed ? 1 : 2);
    assert.equal(result.row_count_after, 2);
    assert.equal(result.pruned_stale_rows, 1, "a completed full drain prunes only omitted old rows");
    assert.deepEqual(h.rows().map((row) => JSON.parse(String(row.raw_payload))), [first, last],
      "retain both the earlier page and the nonempty terminal page across cleanup");
    const completed = (await h.location.getSyncState()).opportunities;
    assert.equal(completed.cursor, null);
    assert.equal(completed.drain_started_at, null);
    assert.equal(completed.backfill_complete, true);
    assert.equal(completed.row_count, 2);
    assert.equal(completed.watermark, first.updatedAt);
    assert.equal(completed.last_success_at, new Date(resumed ? START + POLL : START).toISOString());
    assert.equal((await h.incremental("opportunities")).skipped, "not_due");
    assert.equal(h.requests.length, resumed ? 3 : 2);
  });
}

for (const [label, continuation] of [
  ["hasMore", { hasMore: true }],
  ["nextPage", { meta: { nextPage: 3 } }],
  ["truncation", { truncated: true }],
] as const) {
  test(`opportunities current cursor echo with ${label} cannot complete or prune`, OPTIONS, async (t) => {
    const h = await opportunityFixture(t);
    const fetched = opportunity("opportunity-fetched");
    const cursor = { startAfter: Date.parse(fetched.updatedAt), startAfterId: fetched.id };
    h.respond((url) => {
      assert.equal(url.pathname, "/opportunities/search");
      assert.ok(h.requests.length <= 2);
      if (h.requests.length === 1) return Response.json({ opportunities: [fetched], meta: cursor });
      assert.equal(url.searchParams.get("startAfter"), String(cursor.startAfter));
      assert.equal(url.searchParams.get("startAfterId"), cursor.startAfterId);
      return Response.json({
        opportunities: [fetched], ...continuation,
        meta: { ...cursor, ...("meta" in continuation ? continuation.meta : {}) },
      });
    });

    const result = await h.backfill("opportunities");
    assert.equal(result.stopped_reason, "error");
    assert.equal(h.requests.length, 2);
    assert.equal(result.pruned_stale_rows ?? 0, 0);
    assert.deepEqual(h.rows().map((row) => row.id), [fetched.id, "opportunity-stale"]);
    const state = (await h.location.getSyncState()).opportunities;
    assert.equal(state.cursor, JSON.stringify(cursor));
    assert.equal(state.drain_started_at, new Date(START).toISOString());
    assert.equal(state.backfill_complete, false);
    assert.equal(state.last_success_at, null);
    assert.equal(state.watermark, null);
  });
}

test("opportunities non-immediate compound cursor cycle stays incomplete", OPTIONS, async (t) => {
  const h = await opportunityFixture(t);
  const first = opportunity("opportunity-a");
  const second = opportunity("opportunity-b");
  const cursorA = { startAfter: Date.parse(first.updatedAt), startAfterId: first.id };
  const cursorB = { startAfter: Date.parse(second.updatedAt), startAfterId: second.id };
  h.respond((url) => {
    assert.equal(url.pathname, "/opportunities/search");
    assert.ok(h.requests.length <= 3);
    const current = h.requests.length === 1 ? null : h.requests.length === 2 ? cursorA : cursorB;
    assert.equal(url.searchParams.get("startAfter"), current ? String(current.startAfter) : null);
    assert.equal(url.searchParams.get("startAfterId"), current?.startAfterId ?? null);
    return Response.json(h.requests.length === 2
      ? { opportunities: [second], meta: cursorB }
      : { opportunities: [first], meta: cursorA });
  });

  const result = await h.backfill("opportunities");
  assert.equal(result.stopped_reason, "cursor_stalled");
  assert.equal(h.requests.length, 3);
  assert.equal(result.pruned_stale_rows ?? 0, 0);
  assert.deepEqual(h.rows().map((row) => row.id), [first.id, second.id, "opportunity-stale"]);
  const state = (await h.location.getSyncState()).opportunities;
  assert.equal(state.cursor, JSON.stringify(cursorB));
  assert.equal(state.drain_started_at, new Date(START).toISOString());
  assert.equal(state.backfill_complete, false);
  assert.equal(state.last_success_at, null);
  assert.equal(state.watermark, null);
});

test("contacts current cursor repetition remains incomplete", OPTIONS, async (t) => {
  const h = await fixture(t, []);
  const old = contact("contact-stale", "2026-10-20T10:00:00.000Z");
  h.at(START - POLL);
  await h.location.upsertRows("contacts", [mapRow(ENTITY_BY_TABLE.get("contacts")!, old)]);
  h.at(START);
  const fetched = contact("contact-fetched", "2026-10-20T12:00:00.000Z");
  const searchAfter = [Date.parse(fetched.dateUpdated), fetched.id];
  h.respond((url, init) => {
    assert.equal(url.pathname, "/contacts/search");
    assert.ok(h.requests.length <= 2);
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
    assert.deepEqual(body.searchAfter, h.requests.length === 1 ? undefined : searchAfter);
    return Response.json({ contacts: [{ ...fetched, searchAfter }] });
  });

  const result = await h.backfill("contacts");
  assert.equal(result.stopped_reason, "cursor_stalled");
  assert.equal(h.requests.length, 2);
  assert.equal(result.pruned_stale_rows ?? 0, 0);
  assert.deepEqual(h.db.prepare("SELECT id FROM contacts ORDER BY id").all().map((row) => row.id),
    [fetched.id, old.id]);
  const state = (await h.location.getSyncState()).contacts;
  assert.equal(state.cursor, JSON.stringify({ searchAfter }));
  assert.equal(state.drain_started_at, new Date(START).toISOString());
  assert.equal(state.backfill_complete, false);
  assert.equal(state.last_success_at, null);
  assert.equal(state.watermark, null);
});

test("contacts completion rolls back watermark and cursor together after a post-SQL failure", OPTIONS, async (t) => {
  const h = await fixture(t);
  const watermark = "2026-10-20T10:00:00.000Z";
  const intermediate = contact("contact-intermediate", "2026-10-20T11:00:00.000Z");
  const latest = contact("contact-latest", "2026-10-20T12:00:00.000Z");
  const originalCursor = { searchAfter: [Date.parse(watermark), "contact-before-crash"] };
  await h.location.upsertRows("contacts", [mapRow(ENTITY_BY_TABLE.get("contacts")!, contact("contact-existing", watermark))]);
  await h.location.markBackfillComplete("contacts");
  await h.location.setSyncWatermark("contacts", watermark);
  await h.location.setSyncCursor("contacts", JSON.stringify(originalCursor));
  const drain = await h.location.startDrainIfUnset("contacts");

  const bodies: Array<Record<string, unknown>> = [];
  h.respond((url, init) => {
    assert.equal(url.pathname, "/contacts/search");
    bodies.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
    return Response.json({ contacts: bodies.length === 1 ? [latest] : [intermediate, latest] });
  });
  const fault = new Error("Injected failure after completion SQL executed");
  const exec = h.storage.sql.exec.bind(h.storage.sql);
  let injected = false;
  t.mock.method(h.storage.sql, "exec", (query: string, ...bindings: SQLInputValue[]) => {
    const cursor = exec(query, ...bindings);
    if (!injected && /^\s*(?:INSERT|UPDATE)\b/i.test(query)
      && /\b_sync_state\b/i.test(query) && /\bwatermark\b/i.test(query)) {
      assert.equal(h.db.prepare("SELECT watermark FROM _sync_state WHERE entity = 'contacts'").get()?.watermark, latest.dateUpdated);
      injected = true;
      throw fault;
    }
    return cursor;
  });

  await assert.rejects(h.incremental("contacts"), (error) => error === fault);
  assert.equal(injected, true, "the fault must occur after the real SQL changed the watermark");
  const interrupted = (await h.location.getSyncState()).contacts;
  assert.equal(interrupted.watermark, watermark, "failed completion must roll back the new watermark");
  assert.equal(interrupted.cursor, JSON.stringify(originalCursor));
  assert.equal(interrupted.drain_started_at, drain);
  assert.equal(interrupted.last_success_at, null);

  const retried = await h.incremental("contacts");
  assert.equal(retried.stopped_reason, "empty_page");
  assert.equal(bodies.length, 2);
  for (const body of bodies) {
    assert.deepEqual(body.searchAfter, originalCursor.searchAfter);
    assert.deepEqual(body.filters, [{ field: "dateUpdated", operator: "range", value: { gte: watermark } }]);
  }
  assert.deepEqual(h.db.prepare("SELECT id FROM contacts ORDER BY id").all().map((row) => row.id),
    ["contact-existing", "contact-intermediate", "contact-latest"]);
  const completed = (await h.location.getSyncState()).contacts;
  assert.equal(completed.watermark, latest.dateUpdated);
  assert.equal(completed.cursor, null);
  assert.equal(completed.drain_started_at, null);
  assert.equal(completed.row_count, 3);
  assert.equal(completed.backfill_complete, true);
});

test("a lost contacts completion reply retries with the new filter and no stale cursor", OPTIONS, async (t) => {
  const h = await fixture(t);
  const watermark = "2026-10-20T10:00:00.000Z";
  const latest = contact("contact-latest", "2026-10-20T12:00:00.000Z");
  const next = contact("contact-next", "2026-10-20T13:00:00.000Z");
  const originalCursor = { searchAfter: [Date.parse(watermark), "contact-before-crash"] };
  await h.location.upsertRows("contacts", [mapRow(ENTITY_BY_TABLE.get("contacts")!, contact("contact-existing", watermark))]);
  await h.location.markBackfillComplete("contacts");
  await h.location.setSyncWatermark("contacts", watermark);
  await h.location.setSyncCursor("contacts", JSON.stringify(originalCursor));
  await h.location.startDrainIfUnset("contacts");

  const bodies: Array<Record<string, unknown>> = [];
  h.respond((url, init) => {
    assert.equal(url.pathname, "/contacts/search");
    bodies.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
    return Response.json({ contacts: bodies.length === 1 ? [latest] : [latest, next] });
  });
  const fault = new Error("Injected lost completion reply");
  const complete = h.location.completeSyncEntity.bind(h.location);
  let lostReply = false;
  t.mock.method(h.location, "completeSyncEntity", async (...args: Parameters<LocationDO["completeSyncEntity"]>) => {
    await complete(...args);
    if (!lostReply) {
      lostReply = true;
      throw fault;
    }
  });

  await assert.rejects(h.incremental("contacts"), (error) => error === fault);
  assert.equal(lostReply, true);
  const committed = (await h.location.getSyncState()).contacts;
  assert.equal(committed.watermark, latest.dateUpdated);
  assert.equal(committed.cursor, null);
  assert.equal(committed.drain_started_at, null);
  assert.equal(committed.last_success_at, null);

  assert.equal((await h.incremental("contacts")).stopped_reason, "empty_page");
  assert.equal(bodies.length, 2);
  assert.deepEqual(bodies[0].searchAfter, originalCursor.searchAfter);
  assert.deepEqual(bodies[0].filters, [{ field: "dateUpdated", operator: "range", value: { gte: watermark } }]);
  assert.equal(bodies[1].searchAfter, undefined);
  assert.deepEqual(bodies[1].filters, [{ field: "dateUpdated", operator: "range", value: { gte: latest.dateUpdated } }]);
  assert.deepEqual(h.db.prepare("SELECT id FROM contacts ORDER BY id").all().map((row) => row.id),
    ["contact-existing", "contact-latest", "contact-next"]);
  const state = (await h.location.getSyncState()).contacts;
  assert.equal(state.watermark, next.dateUpdated);
  assert.equal(state.cursor, null);
  assert.equal(state.row_count, 3);
});

test("completion rejects foreign, expired and superseded leases without mutating progress", OPTIONS, async (t) => {
  const h = await fixture(t);
  const watermark = "2026-10-20T10:00:00.000Z";
  const nextWatermark = "2026-10-20T12:00:00.000Z";
  await h.location.setSyncWatermark("contacts", watermark);
  await h.location.setSyncCursor("contacts", "unfinished-page");
  await h.location.startDrainIfUnset("contacts");
  const before = await h.location.getSyncState();
  const token = await h.location.acquireSyncLease(LOCATION, "contacts");
  assert.ok(token);

  const otherDb = new DatabaseSync(":memory:");
  t.after(() => otherDb.close());
  const other = new LocationDO({
    id: { toString: () => "other-tenant-object" }, storage: sqliteStorage(otherDb),
  } as unknown as ConstructorParameters<typeof LocationDO>[0], {});
  const foreignToken = await other.acquireSyncLease("other-location", "contacts");
  assert.ok(foreignToken);
  await assert.rejects(h.location.completeSyncEntity("contacts", nextWatermark, foreignToken), { name: "SyncLeaseError" });
  await assert.rejects(h.location.completeSyncEntity("tags", nextWatermark, token), /entity mismatch/);
  assert.deepEqual(await h.location.getSyncState(), before);

  h.at(START + SYNC_LEASE_TTL_MS);
  await assert.rejects(h.location.completeSyncEntity("contacts", nextWatermark, token), { name: "SyncLeaseError" });
  const replacement = await h.location.acquireSyncLease(LOCATION, "contacts");
  assert.ok(replacement);
  assert.notEqual(replacement, token);
  await assert.rejects(h.location.completeSyncEntity("contacts", nextWatermark, token), { name: "SyncLeaseError" });
  assert.deepEqual(await h.location.getSyncState(), before);

  await h.location.completeSyncEntity("contacts", undefined, replacement);
  const completed = (await h.location.getSyncState()).contacts;
  assert.equal(completed.watermark, watermark, "omitting a watermark preserves the existing filter");
  assert.equal(completed.cursor, null);
  assert.equal(completed.drain_started_at, null);
  assert.equal(completed.backfill_complete, true);
  assert.equal(h.requests.length, 0);
});

for (const firstCall of ["getSyncState", "upsertRows"] as const) {
  test(`legacy row counts are repaired once when ${firstCall} initializes the real object`, OPTIONS, async (t) => {
    t.mock.timers.enable({ apis: ["Date"], now: START });
    const db = new DatabaseSync(":memory:");
    t.after(() => db.close());
    // The pre-upgrade cache has no verification column. Simulate a partial
    // legacy batch whose committed data outgrew its cached bookkeeping.
    db.exec(`CREATE TABLE _sync_state (
      entity TEXT PRIMARY KEY, cursor TEXT, watermark TEXT,
      backfill_complete INTEGER NOT NULL DEFAULT 0, last_sync_at TEXT,
      row_count INTEGER NOT NULL DEFAULT 0
    )`);
    for (const [entity, staleCount] of [["contacts", 1], ["tags", 0]] as const) {
      db.exec(renderCreateTable(ENTITY_BY_TABLE.get(entity)!));
      const insert = db.prepare(`INSERT INTO ${entity}(id, location_id, _synced_at) VALUES (?, ?, ?)`);
      insert.run(`${entity}-a`, LOCATION, new Date(START - POLL).toISOString());
      insert.run(`${entity}-b`, LOCATION, new Date(START - POLL).toISOString());
      db.prepare("INSERT INTO _sync_state(entity, row_count, backfill_complete, cursor) VALUES (?, ?, 1, 'legacy-cursor')").run(entity, staleCount);
    }
    const storage = sqliteStorage(db);
    const countQueries: string[] = [];
    const exec = storage.sql.exec.bind(storage.sql);
    t.mock.method(storage.sql, "exec", (query: string, ...bindings: SQLInputValue[]) => {
      if (/^\s*SELECT\b/i.test(query) && /\bCOUNT\s*\(/i.test(query)) countQueries.push(query);
      return exec(query, ...bindings);
    });
    const ctx = { id: { toString: () => "legacy-count-test-object" }, storage } as unknown as ConstructorParameters<typeof LocationDO>[0];
    let location = new LocationDO(ctx, {});
    if (firstCall === "upsertRows") {
      assert.equal((await location.upsertRows("contacts", [{ id: "contacts-a", location_id: LOCATION }])).row_count_after, 2);
    }
    const repaired = await location.getSyncState();
    assert.equal(repaired.contacts.row_count, 2);
    assert.equal(repaired.tags.row_count, 2);
    assert.equal(repaired.contacts.cursor, "legacy-cursor");
    assert.equal(repaired.tags.cursor, "legacy-cursor");
    assert.equal(db.prepare("SELECT row_count_verified FROM _sync_state WHERE entity = 'contacts'").get()?.row_count_verified, 1);
    assert.equal(db.prepare("SELECT row_count_verified FROM _sync_state WHERE entity = 'tags'").get()?.row_count_verified, 1);
    for (const entity of ["contacts", "tags"]) {
      assert.equal(countQueries.filter((query) => new RegExp(`\\bFROM\\s+"?${entity}\\b`, "i").test(query)).length, 1);
    }
    const initialCountQueries = countQueries.length;

    assert.equal((await location.upsertRows("contacts", [{ id: "contacts-c", location_id: LOCATION }])).row_count_after, 3);
    assert.equal((await location.getSyncState()).contacts.row_count, 3);
    location = new LocationDO(ctx, {});
    const restarted = await location.getSyncState();
    assert.equal(restarted.contacts.row_count, 3);
    assert.equal(restarted.tags.row_count, 2);
    assert.equal((await location.upsertRows("contacts", [{ id: "contacts-c", location_id: LOCATION }])).row_count_after, 3);
    assert.equal(countQueries.length, initialCountQueries, "verified counts must survive upserts and a cold restart without recounting tables");
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM contacts").get()?.n, 3);
  });
}
