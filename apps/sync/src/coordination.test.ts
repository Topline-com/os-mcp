import { test } from "node:test";
import assert from "node:assert/strict";
import { coordinateSync, fencedStub, isComplete, pollBucketStart, pollIsDue, sweepDueCutoff, renewCurrentLease, type LeaseStore, type ScheduleState } from "./coordination.js";

const completeState: ScheduleState = {
  backfill_complete: true, cursor: null, drain_started_at: null,
  last_success_at: "2026-10-21T12:00:00.000Z",
};

test("poll intervals use successful completion, while every partial state remains due", () => {
  const now = Date.parse("2026-10-21T12:15:00.000Z");
  assert.equal(pollIsDue(completeState, 60, now), false);
  assert.equal(pollIsDue(completeState, 15, now), true);
  assert.equal(pollIsDue({ ...completeState, backfill_complete: false }, 60, now), true);
  assert.equal(pollIsDue({ ...completeState, cursor: "next" }, 60, now), true);
  assert.equal(pollIsDue({ ...completeState, drain_started_at: "in-progress" }, 60, now), true);
  assert.equal(pollIsDue({ ...completeState, last_success_at: null }, 60, now), true);
});

test("stalls, errors, partial success and delayed parents never count as completion", () => {
  assert.equal(isComplete({ stopped_reason: "empty_page" }), true);
  for (const stopped_reason of ["cursor_stalled", "page_cap_hit", "error", "skipped", "unsupported"]) {
    assert.equal(isComplete({ stopped_reason }), false);
  }
  assert.equal(isComplete({ stopped_reason: "empty_page", pending_parents: 1 }), false);
  assert.equal(isComplete({ stopped_reason: "empty_page", failed_parents: [{}] }), false);
});

test("a nonzero run duration consumes one UTC cadence bucket without skipping the next cron", () => {
  const finished = { ...completeState, last_success_at: "2026-10-21T14:00:30.000Z" };
  assert.equal(pollIsDue(finished, 15, Date.parse("2026-10-21T14:14:59.999Z")), false);
  assert.equal(pollIsDue(finished, 15, Date.parse("2026-10-21T14:15:00.000Z")), true);
  assert.equal(pollIsDue(finished, 60, Date.parse("2026-10-21T15:00:00.000Z")), true);
  assert.equal(new Date(pollBucketStart(15, Date.parse("2026-10-21T14:15:05.000Z"))).toISOString(), "2026-10-21T14:15:00.000Z");
  assert.equal(sweepDueCutoff("2026-10-21T14:00:05.000Z", 15, Date.parse("2026-10-21T14:15:00.000Z")), "2026-10-21T14:15:00.000Z");
  assert.equal(sweepDueCutoff("2026-10-21T14:00:05.000Z", 15, Date.parse("2026-10-21T14:00:10.000Z")), "2026-10-21T14:00:05.000Z");
});

function store(): LeaseStore & { finishes: boolean[]; active: boolean; renewals: number } {
  return {
    finishes: [], active: false, renewals: 0,
    async getSyncState() { return {}; },
    async acquireSyncLease() {
      if (this.active) return null;
      this.active = true;
      return "fence";
    },
    async renewSyncLease(token) { assert.equal(token, "fence"); this.renewals++; },
    async finishSyncLease(token, _entity, success) {
      assert.equal(token, "fence"); this.active = false; this.finishes.push(success);
    },
  };
}

test("overlapping entry points do not enter work and failure releases without success", async () => {
  const storage = store();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let entered!: () => void;
  const started = new Promise<void>((resolve) => { entered = resolve; });
  const first = coordinateSync(storage, "loc", "appointments", 15, async () => {
    entered(); await gate;
    return { stopped_reason: "error" };
  });
  await started;
  const second = await coordinateSync(storage, "loc", "contacts", 15, async () => {
    assert.fail("overlap entered work");
  });
  assert.equal(second.skipped, "busy");
  release(); await first;
  assert.deepEqual(storage.finishes, [false]);
  await assert.rejects(coordinateSync(storage, "loc", "contacts", 15, async () => { throw new Error("broken transport"); }));
  assert.deepEqual(storage.finishes, [false, false]);
});

test("a completion racing lease acquisition is rechecked under the lease", async () => {
  const storage = store();
  let reads = 0;
  storage.getSyncState = async (): Promise<Record<string, ScheduleState>> => (++reads === 1 ? {} : { contacts: { ...completeState, last_success_at: new Date().toISOString() } });
  const result = await coordinateSync(storage, "loc", "contacts", 15, async () => {
    assert.fail("not-due work must not execute");
  });
  assert.equal(result.skipped, "not_due");
  assert.deepEqual(storage.finishes, [false]);
});

test("fenced writes pad optional arguments, preserve tenant and aggregate actual returned counters", async () => {
  const storage = store();
  const calls: unknown[][] = [];
  const rpc = {
    async markParentBackfillComplete(...args: unknown[]) { calls.push(args); return 0; },
    async reconcileAppointmentSnapshot(...args: unknown[]) {
      calls.push(args);
      return { rows_written: 6, unchanged: 999, missing_retained: 2 };
    },
    async recordParentFailure(...args: unknown[]) { calls.push(args); },
  };
  const bound = fencedStub(rpc, "loc");
  await assert.rejects(bound.markParentBackfillComplete("messages", "p", "messages", "conversation_id"), /tenant mismatch/);
  const result = await coordinateSync(storage, "loc", "appointments", 15, async () => {
    await renewCurrentLease();
    await bound.markParentBackfillComplete("messages", "p", "messages", "conversation_id");
    await bound.reconcileAppointmentSnapshot("loc", "calendar", []);
    await bound.recordParentFailure("appointments", "calendar");
    await assert.rejects(fencedStub(rpc, "other").recordParentFailure("appointments", "calendar"), /tenant mismatch/);
    return { stopped_reason: "empty_page" };
  });
  assert.deepEqual(calls, [
    ["messages", "p", "messages", "conversation_id", undefined, "fence"],
    ["loc", "calendar", [], "fence"],
    ["appointments", "calendar", undefined, "fence"],
  ]);
  assert.equal(result.rows_written, 6);
  assert.equal(result.unchanged_rows, 999);
  assert.equal(result.missing_retained, 2);
  assert.equal(storage.renewals, 1);
  assert.deepEqual(storage.finishes, [true]);
});
