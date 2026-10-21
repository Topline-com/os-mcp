# Topline OS MCP v0.2.1

Released: 2026-10-21

v0.2.1 collects fixes since v0.2.0. The functional changes are already live through commit `2fb26d1`; this release records that deployed behavior.

## Fixes

- **OAuth connections:** fix browser-origin compatibility and callback redirects, keep consent forms valid for 30 minutes, prevent duplicate submissions, and make consent retries idempotent. Remove the blocking credential preflight and defer background backfill kickoff until after consent completes.
- **CRM requests:** read the configured API base URL at request time and send a User-Agent on CRM requests and credential probes.
- **Sync writes and retries ([#29](https://github.com/Topline-com/os-mcp/pull/29)):** skip appointment-row and index writes when content is unchanged, reconcile changed appointments atomically, persist retry cooldowns for failing parents, repair legacy row-count caches, and commit pagination completion with its watermark. Shared leases fence mutations against stale or overlapping runs.
- **RPC recovery ([#30](https://github.com/Topline-com/os-mcp/pull/30)):** forward Durable Object RPC calls through a local closure instead of `.bind`, fixing the runtime failure while preserving lease fencing.

## Sync behavior

Private integration tokens continue to use polling with the existing 2020–2030 appointment history. No new credentials or webhook setup are required. Appointments omitted from a calendar response are retained; returned cancellation and status changes still update normally.

Appointment `_synced_at` now means the last material row write. Use `topline_warehouse_freshness` or the entity's `last_success_at` for completed-check freshness. Partial or failed sweeps do not count as completed checks.

## Validation and remaining checks

382 standard tests and nine real-workerd regression cases passed. Local workerd measurements for 1,000 identical appointments fell from 6,000 appointment-storage writes to zero; changing one appointment produced six appointment-storage writes. Metadata and lease bookkeeping are separate.

Passive observation of the deployed version on October 21, 16:14:50–16:30 UTC returned four appointment summaries and three errors. Observed results included 4,647 unchanged appointments, `data_rows_written=0`, `row_count=5373`, and 40 failed/pending parents. No full cron completion returned.

`data_rows_written` is not an account-billing measurement. Account billing and complete sync freshness remain unverified; these observations do not establish full sync health. Follow-up is tracked in [#31](https://github.com/Topline-com/os-mcp/issues/31).

## Install and upgrade

Hosted clients using `https://os-mcp.topline.com/mcp` are already updated and need no local upgrade.

Local Claude Desktop, Claude Code, and Codex installations keep the existing command:

```text
npx -y github:topline-com/os-mcp
```

Fully quit and reopen the client to relaunch the local server. No new installation mechanism is introduced.

For deployment recovery, preserve lease fencing and roll forward with both Workers. Never restore the old unfenced sync Worker or remove its lease record to bypass fencing. See [Sync operations](https://github.com/Topline-com/os-mcp/blob/v0.2.1/docs/sync-operations.md).

[Full changelog](https://github.com/Topline-com/os-mcp/compare/v0.2.0...v0.2.1)
