// Regression for RPC methods being remote property paths (method.bind is RPC,
// not Function.prototype.bind). Exercise production code across real workerd
// RPC, including the entity entry points. Everything is synthetic/disposable;
// outboundService intercepts every fetch and never forwards to the network.
// Run: node scripts/probe-sync-rpc.mjs
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const edgeRequire = createRequire(resolve(root, "apps/edge/package.json"));
const { Miniflare, convertV4MiniflareOptions } = await import(pathToFileURL(edgeRequire.resolve("miniflare")).href);
const { outputFiles } = await build({
  stdin: {
    resolveDir: root,
    contents: `
      import { LocationDO } from './packages/shared-do/src/location-do.ts';
      import { fencedStub, coordinateSync, renewCurrentLease } from './apps/sync/src/coordination.ts';
      import { backfillEntity, incrementalEntity } from './apps/sync/src/backfill.ts';
      import { encryptPit } from '@topline/shared-auth';
      import { ENTITY_BY_TABLE } from '@topline/shared-schema';
      import { mapRow } from './apps/sync/src/mapping.ts';
      export { LocationDO };
      const check = (condition, message) => { if (!condition) throw new Error(message); };
      async function proveReleased(stub, locationId) {
        const token = await stub.acquireSyncLease(locationId, 'appointments');
        check(typeof token === 'string', 'Previous lease was not released');
        await stub.finishSyncLease(token, 'appointments', false);
        return true;
      }
      export default {
        async fetch(request, env) {
          const results = {};
          const raw = (name) => env.LOCATION_DO.get(env.LOCATION_DO.idFromName(name));
          async function probe(name, run) {
            try { results[name] = { ok: true, value: await run() }; }
            catch (error) { results[name] = { ok: false, name: error.name, error: error.message, stack: error.stack }; }
          }
          await probe('direct_getSyncState', () => raw('direct').getSyncState());
          await probe('direct_acquireSyncLease', async () => {
            const stub = raw('direct');
            const token = await stub.acquireSyncLease('direct', 'appointments');
            if (!token) throw new Error('Fixture lease unavailable');
            await stub.finishSyncLease(token, 'appointments', false);
            return { acquired: true, released: true };
          });
          await probe('fenced_getSyncState', () => fencedStub(raw('fenced-read'), 'fenced-read').getSyncState());
          await probe('fenced_acquireSyncLease', async () => {
            const stub = raw('fenced-lease');
            const token = await fencedStub(stub, 'fenced-lease').acquireSyncLease('fenced-lease', 'appointments');
            if (!token) throw new Error('Fixture lease unavailable');
            const fenced = fencedStub(stub, 'fenced-lease');
            await fenced.renewSyncLease(token);
            await fenced.finishSyncLease(token, 'appointments', false);
            return { acquired: true, renewed: true, released: await proveReleased(stub, 'fenced-lease') };
          });
          await probe('fenced_mutation_raw_coordinator', async () => {
            const stub = raw('fenced-mutation');
            const fenced = fencedStub(stub, 'fenced-mutation');
            const result = await coordinateSync(stub, 'fenced-mutation', 'appointments', 0, async () => {
              await fenced.setSyncCursor('appointments', 'synthetic-cursor');
              return { stopped_reason: 'page_cap_hit' };
            });
            const state = await stub.getSyncState();
            if (state.appointments?.cursor !== 'synthetic-cursor') throw new Error('Fenced mutation was not persisted');
            return { result, cursor: state.appointments.cursor, released: await proveReleased(stub, 'fenced-mutation') };
          });
          await probe('fenced_coordinateSync', async () => {
            const stub = raw('fenced-coordinator');
            const fenced = fencedStub(stub, 'fenced-coordinator');
            const result = await coordinateSync(fenced, 'fenced-coordinator', 'appointments', 0, async () => {
              await renewCurrentLease();
              await fenced.setSyncCursor('appointments', 'synthetic-cursor');
              await fenced.completeSyncEntity('appointments');
              return { stopped_reason: 'empty_page' };
            });
            const state = (await stub.getSyncState()).appointments;
            check(state.cursor === null && state.backfill_complete && !!state.last_success_at,
              'Successful coordination did not persist completion');
            return { result, completed: true, released: await proveReleased(stub, 'fenced-coordinator') };
          });
          await probe('fenced_coordinateSync_failure_cleanup', async () => {
            const locationId = 'fenced-failure';
            const stub = raw(locationId);
            let errorMessage;
            try {
              await coordinateSync(fencedStub(stub, locationId), locationId, 'appointments', 0, async () => {
                throw new Error('Synthetic run failure');
              });
            } catch (error) { errorMessage = error.message; }
            check(errorMessage === 'Synthetic run failure', 'Callback failure was not preserved');
            check(!(await stub.getSyncState()).appointments?.last_success_at,
              'Failed callback must not stamp success');
            const released = await proveReleased(stub, locationId);
            let unfencedRejected = false;
            try { await stub.setSyncCursor('appointments', 'unfenced'); }
            catch (error) { unfencedRejected = error.message.includes('Sync lease is missing'); }
            check(unfencedRejected, 'Releasing a lease must preserve mandatory fencing');
            return { released, unfencedRejected };
          });
          for (const mode of ['backfill', 'incremental']) {
            await probe('real_' + mode + 'Entity', async () => {
              const locationId = 'entity-' + mode;
              const connectionId = 'connection-' + mode;
              const stub = raw(locationId);
              const upstreamRow = {
                id: 'appointment-a', locationId, calendarId: 'calendar-a',
                title: 'Synthetic appointment', appointmentStatus: 'confirmed',
                startTime: '2026-10-21T12:00:00.000Z', endTime: '2026-10-21T12:30:00.000Z',
                dateUpdated: '2026-10-20T12:00:00.000Z',
              };
              // Seed before the first lease, just like a pre-existing tenant.
              await stub.upsertRows('calendars', [{ id: 'calendar-a', location_id: locationId }]);
              if (mode === 'incremental') {
                await stub.upsertRows('appointments', [mapRow(ENTITY_BY_TABLE.get('appointments'), upstreamRow)]);
                await stub.markBackfillComplete('appointments');
              }
              const secret = 'synthetic-rpc-probe-signing-secret';
              const encrypted = await encryptPit('synthetic-rpc-probe-token', secret);
              const stored = JSON.stringify({ location_id: locationId, pit_ct: encrypted.ct, pit_iv: encrypted.iv,
                brand_name: 'Test', source: 'test', created_at: '2026-01-01T00:00:00.000Z',
                last_verified_at: '2026-01-01T00:00:00.000Z' });
              const syncEnv = {
                LOCATION_DO: env.LOCATION_DO,
                TOKEN_SIGNING_SECRET: secret,
                CONNECTIONS: { async get(key, format) {
                  check(key === connectionId && format === 'text', 'Unexpected connection lookup');
                  return stored;
                } },
                CONNECTION_AUTH_DO: {
                  idFromName(name) { check(name === connectionId, 'Unexpected auth identity'); return name; },
                  get() { return { async getOrBootstrap(input) {
                    check(input.location_id === locationId && input.credential_exists, 'Unexpected auth input');
                    return { status: 'active', location_id: locationId };
                  } }; },
                },
              };
              const run = mode === 'backfill' ? backfillEntity : incrementalEntity;
              const result = await run(syncEnv, connectionId, 'appointments');
              check(result.stopped_reason === 'empty_page' && result.pages === 1 && result.rows_upserted === 1 &&
                !result.failed_parents?.length && !result.pending_parents && !result.error,
                'Real entity sync did not complete exactly one parent/page');
              const state = (await stub.getSyncState()).appointments;
              check(state.row_count === 1 && state.backfill_complete && !!state.last_success_at &&
                state.cursor === null && state.drain_started_at === null, 'Entity completion was not persisted');
              const rows = (await stub.executeQuery('SELECT id, location_id, title FROM appointments')).rows;
              check(rows.length === 1 && rows[0].id === 'appointment-a' && rows[0].location_id === locationId &&
                rows[0].title === 'Synthetic appointment', 'Appointment readback mismatch');
              if (mode === 'incremental') check(result.data_rows_written === 0 && result.unchanged_rows === 1,
                'Unchanged incremental refresh must not rewrite the appointment');
              else check(result.data_rows_written > 0, 'Backfill did not report its insertion');
              return { stopped_reason: result.stopped_reason, pages: result.pages, row_count: state.row_count,
                data_rows_written: result.data_rows_written, unchanged_rows: result.unchanged_rows,
                completed: true, released: await proveReleased(stub, locationId) };
            });
          }
          return Response.json(results);
        }
      };
    `,
  },
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  external: ["cloudflare:workers", "node:*"],
  // Read current source, not a potentially stale local package build.
  alias: {
    "@topline/shared": resolve(root, "packages/shared/src/index.ts"),
    "@topline/shared-auth": resolve(root, "packages/shared-auth/src/index.ts"),
    "@topline/shared-schema": resolve(root, "packages/shared-schema/src/index.ts"),
  },
  define: { "process.env.TOPLINE_API_BASE_URL": JSON.stringify("https://crm.example.test") },
  nodePaths: [resolve(root, "node_modules")],
});
const requests = [];
const mf = new Miniflare(convertV4MiniflareOptions({
  name: "sync-rpc-fixture",
  modules: true,
  script: outputFiles[0].text,
  compatibilityDate: "2025-04-01",
  compatibilityFlags: ["nodejs_compat"],
  durableObjects: { LOCATION_DO: { className: "LocationDO", useSQLite: true } },
  outboundService(request) {
    const url = new URL(request.url);
    // Never log headers or forward requests, including unexpected ones.
    assert.equal(url.origin, "https://crm.example.test");
    assert.equal(url.pathname, "/calendars/events");
    assert.equal(request.method, "GET");
    assert.equal(url.searchParams.get("calendarId"), "calendar-a");
    const locationId = url.searchParams.get("locationId");
    assert.ok(["entity-backfill", "entity-incremental"].includes(locationId));
    requests.push(locationId);
    return new Response(JSON.stringify({ events: [{
      id: "appointment-a", locationId, calendarId: "calendar-a",
      title: "Synthetic appointment", appointmentStatus: "confirmed",
      startTime: "2026-10-21T12:00:00.000Z", endTime: "2026-10-21T12:30:00.000Z",
      dateUpdated: "2026-10-20T12:00:00.000Z",
    }] }), { headers: { "Content-Type": "application/json" } });
  },
}));
try {
  const response = await mf.dispatchFetch("http://localhost/");
  assert.equal(response.status, 200, "Local RPC fixture must return JSON");
  const results = await response.json();
  console.log(JSON.stringify(results, null, 2));
  const expected = ["direct_getSyncState", "direct_acquireSyncLease", "fenced_getSyncState", "fenced_acquireSyncLease",
    "fenced_mutation_raw_coordinator", "fenced_coordinateSync", "fenced_coordinateSync_failure_cleanup",
    "real_backfillEntity", "real_incrementalEntity"];
  assert.deepEqual(Object.keys(results), expected);
  for (const name of expected) assert.equal(results[name].ok, true, name + ": " + (results[name].error ?? "failed"));
  assert.deepEqual(requests, ["entity-backfill", "entity-incremental"], "Each real entry point must fetch exactly once");
} finally {
  await mf.dispose();
}
