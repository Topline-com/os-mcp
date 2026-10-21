import assert from "node:assert/strict";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import test, { type TestContext } from "node:test";

import { encryptPit, type StoredConnection } from "@topline/shared-auth";
import { ENTITY_BY_TABLE } from "@topline/shared-schema";
import {
  backfillEntity,
  incrementalEntity,
  type SyncEnv,
} from "../apps/sync/src/backfill.js";
import { mapRow } from "../apps/sync/src/mapping.js";
import { LocationDO } from "../packages/shared-do/src/location-do.js";

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
  let respond: (url: URL) => Response | Promise<Response> = () => {
    throw new Error("Unexpected request; network access is disabled in this fixture");
  };
  t.mock.method(globalThis, "fetch", async (input: string | URL | Request, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    requests.push(url);
    assert.equal(url.origin, "https://crm.example.test");
    assert.equal(init?.method, "GET");
    assert.ok(new Headers(init?.headers).get("Authorization") === `Bearer ${pit}`);
    return respond(url);
  });
  t.mock.timers.setTime(START);
  return {
    env, location, storage, requests,
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
