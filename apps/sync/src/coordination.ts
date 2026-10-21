import { AsyncLocalStorage } from "node:async_hooks";

export interface ScheduleState {
  backfill_complete: boolean;
  last_success_at?: string | null;
  drain_started_at: string | null;
  cursor: string | null;
}

export interface LeaseStore {
  acquireSyncLease(locationId: string, entity: string): Promise<string | null>;
  renewSyncLease(token: string): Promise<void>;
  finishSyncLease(token: string, entity: string, success: boolean): Promise<void>;
  getSyncState(): Promise<Record<string, ScheduleState>>;
}

interface RunContext {
  token: string;
  locationId: string;
  store: LeaseStore;
  rows_written: number;
  unchanged_rows: number;
  missing_retained: number;
}

const syncContext = new AsyncLocalStorage<RunContext>();

/** UTC interval buckets avoid delaying a cron tick by the prior run's duration. */
export function pollBucketStart(intervalMinutes: number, now = Date.now()): number {
  const interval = intervalMinutes * 60_000;
  return interval > 0 ? Math.floor(now / interval) * interval : now;
}

export function sweepDueCutoff(cycleStart: string, intervalMinutes: number, now = Date.now()): string {
  return new Date(Math.max(Date.parse(cycleStart), pollBucketStart(intervalMinutes, now))).toISOString();
}

/** Only a completed run consumes its bucket. Partial drains resume next tick. */
export function pollIsDue(state: ScheduleState | undefined, intervalMinutes: number, now = Date.now()): boolean {
  if (!state?.backfill_complete || state.cursor || state.drain_started_at || !state.last_success_at) return true;
  const last = Date.parse(state.last_success_at);
  return !Number.isFinite(last) || intervalMinutes <= 0 || pollBucketStart(intervalMinutes, now) > pollBucketStart(intervalMinutes, last);
}

export interface CompletionResult {
  stopped_reason: string;
  failed_parents?: readonly unknown[];
  pending_parents?: number;
}

export function isComplete(result: CompletionResult): boolean {
  return result.stopped_reason === "empty_page" && !result.failed_parents?.length && !result.pending_parents;
}

export async function coordinateSync<T extends CompletionResult>(
  store: LeaseStore,
  locationId: string,
  entity: string,
  intervalMinutes: number,
  run: () => Promise<T>,
  force = false,
): Promise<{ value?: T; skipped?: "busy" | "not_due"; rows_written: number; unchanged_rows: number; missing_retained: number }> {
  const skipped = (reason: "busy" | "not_due") => ({ skipped: reason, rows_written: 0, unchanged_rows: 0, missing_retained: 0 });
  if (!force && !pollIsDue((await store.getSyncState())[entity], intervalMinutes)) return skipped("not_due");
  const token = await store.acquireSyncLease(locationId, entity);
  if (token === null) return skipped("busy");
  let released = false;
  try {
    // Another invocation may have completed between the first read and claim.
    if (!force && !pollIsDue((await store.getSyncState())[entity], intervalMinutes)) {
      await store.finishSyncLease(token, entity, false);
      released = true;
      return skipped("not_due");
    }
    const context: RunContext = { token, locationId, store, rows_written: 0, unchanged_rows: 0, missing_retained: 0 };
    const value = await syncContext.run(context, run);
    await store.finishSyncLease(token, entity, isComplete(value));
    released = true;
    return { value, rows_written: context.rows_written, unchanged_rows: context.unchanged_rows, missing_retained: context.missing_retained };
  } finally {
    if (!released) {
      // A stale invocation must never release a newer lease. The DO checks
      // the token and expiry; losing that race is intentionally harmless here.
      await store.finishSyncLease(token, entity, false).catch(() => undefined);
    }
  }
}

/** Renew before an upstream request. A fetch returning after expiry cannot write. */
export async function renewCurrentLease(): Promise<void> {
  const context = syncContext.getStore();
  if (!context) throw new Error("Sync requires a lease");
  await context.store.renewSyncLease(context.token);
}

// Positional arity preserves optional legacy arguments before the new fence.
const mutationArity: Readonly<Record<string, number>> = {
  upsertRows: 2, setSyncCursor: 2, clearSyncCursor: 1, completeSyncEntity: 2,
  setSyncWatermark: 2, markBackfillComplete: 1, resetBackfill: 1,
  startDrainIfUnset: 1, setUpstreamTotal: 2, pruneStaleRows: 2,
  clearDrainMarker: 1, setParentSyncCursor: 3, ensureParentDrainMarker: 2,
  markParentBackfillComplete: 5, recordParentFailure: 3,
  reconcileAppointmentSnapshot: 3,
};

/** All sync mutators, including derived-table writes, use the same tenant fence. */
export function fencedStub<T extends object>(stub: T, locationId: string): T {
  return new Proxy(stub, {
    get(target, property) {
      const method = Reflect.get(target, property);
      if (typeof method !== "function") return method;
      const arity = typeof property === "string" ? mutationArity[property] : undefined;
      if (arity === undefined) return method.bind(target);
      return async (...args: unknown[]) => {
        const context = syncContext.getStore();
        if (!context || context.locationId !== locationId) throw new Error("Sync lease tenant mismatch");
        while (args.length < arity) args.push(undefined);
        args.push(context.token);
        const result = await Reflect.apply(method, target, args);
        if (result && typeof result === "object" && "rows_written" in result) {
          context.rows_written += Number(result.rows_written);
          context.unchanged_rows += Number(result.unchanged ?? 0);
          context.missing_retained += Number(result.missing_retained ?? 0);
        }
        return result;
      };
    },
  });
}
