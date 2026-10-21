// Exercise the real LocationDO in local workerd. No credentials or network
// calls to the CRM/Cloudflare API; all records and storage are disposable.
import { build } from "esbuild";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve, dirname } from "node:path";
import assert from "node:assert/strict";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const edgeRequire = createRequire(resolve(root, "apps/edge/package.json"));
const { Miniflare, convertV4MiniflareOptions } = await import(
  pathToFileURL(edgeRequire.resolve("miniflare")).href
);
const source = resolve(root, process.argv[2] ?? "packages/shared-do/src/location-do.ts");
const { outputFiles } = await build({
  stdin: {
    resolveDir: root,
    contents: `
      import { LocationDO } from ${JSON.stringify(source)};
      export class Probe extends LocationDO {
        async measure(snapshot) {
          if (snapshot && typeof this.reconcileAppointmentSnapshot !== 'function') return null;
          await this.getSyncState();
          if (snapshot) await this.upsertRows('calendars', [{ id: 'test-calendar', location_id: 'test-location' }]);
          const sql = this.ctx.storage.sql;
          const exec = sql.exec.bind(sql);
          let counts = { sql_rows_written: 0, appointment_rows_written: 0 };
          sql.exec = (statement, ...bindings) => {
            const cursor = exec(statement, ...bindings);
            counts.sql_rows_written += cursor.rowsWritten;
            if (/^(?:INSERT\\s+INTO|UPDATE|DELETE\\s+FROM)\\s+"?appointments"?(?:\\s|\\()/i.test(statement.trim())) {
              counts.appointment_rows_written += cursor.rowsWritten;
            }
            return cursor;
          };
          const rows = Array.from({ length: 1000 }, (_, i) => ({
            id: 'appointment-' + i, location_id: 'test-location',
            calendar_id: 'test-calendar', contact_id: 'contact-' + i,
            title: 'Fixture appointment', status: 'confirmed',
            start_time: '2026-10-21T12:00:00.000Z',
            end_time: '2026-10-21T12:30:00.000Z',
            updated_at: '2026-10-20T12:00:00.000Z',
            raw_payload: { id: 'appointment-' + i, title: 'Fixture appointment' }
          }));
          const results = {};
          for (const [name, batch] of [
            ['initial_insert', rows],
            ['identical_refresh', rows],
            ['one_changed_record', rows.map((row, i) => i === 0 ? { ...row, title: 'Changed title' } : row)],
          ]) {
            // workerd advances its clock at I/O boundaries. Ensure the
            // refresh timestamp differs, as it does between cron runs.
            await new Promise((done) => setTimeout(done, 10));
            counts = { sql_rows_written: 0, appointment_rows_written: 0 };
            let result;
            if (snapshot) {
              const token = await this.acquireSyncLease('test-location', 'appointments');
              if (!token) throw new Error('Fixture could not acquire its lease');
              result = await this.reconcileAppointmentSnapshot('test-location', 'test-calendar', batch, token);
              await this.finishSyncLease(token, 'appointments', true);
            } else {
              result = await this.upsertRows('appointments', batch);
            }
            results[name] = { ...counts, row_count: result.row_count_after };
          }
          return { fixture_records: rows.length, results };
        }
      }
      export default {
        async fetch(request, env) {
          const snapshot = new URL(request.url).searchParams.get('snapshot') === 'true';
          return Response.json(await env.PROBE.get(env.PROBE.idFromName('cost-fixture-' + snapshot)).measure(snapshot));
        }
      };
    `,
  },
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  external: ["cloudflare:workers"],
  nodePaths: [resolve(root, "node_modules")],
});
const mf = new Miniflare(convertV4MiniflareOptions({
  name: "sync-cost-fixture",
  modules: true,
  script: outputFiles[0].text,
  compatibilityDate: "2025-04-01",
  durableObjects: { PROBE: { className: "Probe", useSQLite: true } },
}));
try {
  const result = {};
  for (const [name, snapshot] of [["legacy_upsert", false], ["calendar_snapshot", true]]) {
    const response = await mf.dispatchFetch(`http://localhost/?snapshot=${snapshot}`);
    if (!response.ok) throw new Error(`Local fixture failed: ${await response.text()}`);
    result[name] = await response.json();
  }
  console.log(JSON.stringify(result, null, 2));
  // An explicit source path is a measurement-only comparison against an older
  // implementation. The current implementation must meet its cost contract.
  if (!process.argv[2]) {
    const snapshot = result.calendar_snapshot;
    assert.ok(snapshot, "Calendar snapshot RPC is required");
    assert.equal(snapshot.results.identical_refresh.appointment_rows_written, 0);
    assert.equal(snapshot.results.one_changed_record.row_count, 1000);
    assert.ok(snapshot.results.one_changed_record.appointment_rows_written > 0);
    assert.ok(snapshot.results.one_changed_record.appointment_rows_written < 20,
      "Changing one appointment must not rewrite the other 999");
  }
} finally {
  await mf.dispose();
}
