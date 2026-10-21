# Sync operations

The hosted connector reads each connection's authorized CRM data into a separate
`LocationDO` for its location. The scheduled Worker wakes every 15 minutes;
individual entities use the polling intervals in the schema manifest. Private
integration tokens use polling, so this release requires no new credentials or
webhook setup.

## Write and retry behavior

Appointments are compared with stored content. Unchanged appointments cause no
appointment-row or index writes. New and changed appointments are written in an
atomic calendar reconciliation, and parent metadata records the completed check.
Other entities refresh only the unindexed observation timestamp when their
content is unchanged, preserving their existing drain-cleanup behavior.
Legacy row-count caches are reconciled once during initialization, then maintained
from logical inserts and deletions. The durable verification flag avoids repeated
full-table counts on later refreshes or restarts. Completing a paginated drain
commits its cursor reset and watermark together, so an interruption cannot pair
an old pagination cursor with a new incremental filter.

Appointment `_synced_at` records the last material write. Use
`topline_warehouse_freshness` or the entity's `last_success_at` for check freshness.
The freshness view advances after a completed sweep even when no appointments
changed; partial or failed sweeps do not report themselves as completed.

The calendar-list contract does not establish that an omitted appointment was
deleted. Missing appointments are retained and counted as `missing_retained`.
This intentionally favors retaining history over silently deleting valid records.
Returned cancellation/status changes still update normally. Verified deletion
reconciliation requires a separate upstream contract; this release does not infer
deletion from a list omission. Malformed item arrays, duplicate IDs, scope
mismatches, advertised truncation, and unexpected cursor stalls cannot authorize
cleanup. The opportunities endpoint's documented echo of its current compound
cursor is an end-of-results signal only when no further page is advertised;
cycles through earlier cursors remain incomplete.

Manual triggers and scheduled runs share a durable location lease. Each mutation
checks the lease token, so a run that resumes after expiry cannot overwrite a
newer run. Failed parents keep durable retry state; a parent in cooldown still
counts as pending. Completed runs consume a UTC interval window (15 minutes,
30 minutes, hourly, or daily according to the manifest). This keeps a run's
duration from making it skip the next scheduled tick. Healthy calendars remain
eligible in the next window while a different calendar is in retry cooldown.

## Local validation

```sh
npm ci --ignore-scripts
npm run check
npm run worker:deploy -w @topline-ai/edge -- --dry-run
npm run worker:deploy -w @topline-ai/sync -- --dry-run
node scripts/measure-sync-writes.mjs
```

The measurement script runs the real `LocationDO` in disposable local workerd
storage. It uses 1,000 synthetic appointments, wraps the native SQL cursor, and
records `rowsWritten`. It needs no credentials and does not call the CRM or the
Cloudflare API. Schema creation is excluded. The appointment counter includes
index maintenance, so it is not a count of distinct appointments.

The baseline at commit `387a889` measured 6,000 appointment-storage writes plus
one metadata write for an identical refresh of 1,000 appointments. Changing only
one appointment also caused all 6,000 appointment writes. The script accepts an
alternate `location-do.ts` path as its first argument for comparing versions;
keep that source's relative imports alongside it.

The updated implementation measured zero appointment writes on the identical
refresh and six appointment writes when one of the 1,000 records changed. The
calendar-snapshot fixture also records lease and completion bookkeeping separately
from appointment writes. CI runs this fixture and fails if identical snapshots
start writing appointments again or a one-record change rewrites the batch.

Local fixtures establish the change's behavior, not a forecast of the account's
bill. Production volume, changed-record share, index definitions, and retries
determine the actual savings.

## Release checks

Both Workers must deploy from the same checked commit, with the edge Worker
first. The production account is pinned in the deployment configuration and CI.
Keep Durable Object class migrations append-only and preserve the existing
storage and credential bindings.

Once coordinated sync first runs for a location, its lease record permanently
requires fenced sync mutations, including after release or expiry. This prevents
an old in-flight Worker from pruning newer unchanged records. Roll forward with
both Workers if release recovery is needed; do not restore an unfenced sync
Worker or remove the lease record to bypass the check.

After deployment, compare an equal observation window with the baseline:

- LocationDO storage writes and the account's billable storage-write counter.
- Appointment freshness and successful calendar checks.
- Failed parents, retries, and incomplete drains.
- Row counts and calendar-reference queries for existing historical records.

The existing appointment coverage remains 2020–2030. Calendar-reference checks
use that history, so reducing the window is a separate behavior change.
