# Topline Marketing OS — rollout runbook (P7)

This document describes how to land Pillars 1–6 cleanly, configure each
tenant, and verify end-to-end after every merge. It's the operational
counterpart to `/Users/alexskatell/.claude/plans/does-my-mcp-include-lively-lightning.md`.

## PR inventory

| PR | Pillar | Title | Deps |
|---|---|---|---|
| #8 | Pre-flight | White-label scrub (vendor names + env-driven base URL) | — |
| #9 | P1A | Social Planner tools (4 umbrella) | — |
| #10 | P1D | Agent Studio tools (3 umbrella) | — |
| #11 | P1C | Email Campaigns tools (4 umbrella, paths best-effort) | — |
| #12 | P1B | Ad Publishing — Facebook + Google + LinkedIn (24 umbrella) | — |
| #13 | P2 | UTM standardization (8 tools) | — |
| #14 | P3 | Spend ingestion scaffold (6 tools, Brex + QBO pluggable) | #13 |
| #15 | P4 | Attribution SQL views (contact + opportunity) | — |
| #16 | P6 | Form submission Slack notifier (3 tools) | #13 |
| #17 | P5 | Marketing dashboard data tool (1 composite) | #13, #14 |

## Suggested merge order

1. **#8 — White-label scrub** first. Sets the env-var pattern + scrubs vendor names.
2. **Parallel: #9, #10, #11, #12, #15** — independent Pillar-1 sub-PRs + attribution views.
3. **#13 — P2 UTM** — foundation for downstream.
4. **#14 — P3 Spend** — depends on #13.
5. **Parallel: #16 (P6 Forms), #17 (P5 Dashboard)** — depend on #13/#14.

Each merge step expects the previous batch to be in `main` first.

## ⚠️ Worker secrets — set these BEFORE merging #8

The white-label scrub (#8) replaced the hardcoded API base URL with the
`TOPLINE_API_BASE_URL` env var. The default in source is the placeholder
`https://api.example.com`. If you merge #8 without setting the secret on
each deployed Cloudflare Worker first, every API call 530s because the
Worker hits the placeholder host.

**On every Worker deployment** (`topline-os-mcp` edge worker AND
`topline-os-sync` sync worker) — set the secret before the deploy
triggered by #8 finishes:

Substitute `<CRM_API_URL>` below with the actual upstream API base URL
Topline provides during onboarding (not committed to this repo — see
white-label hygiene below).

```bash
# From apps/edge/
printf '<CRM_API_URL>' \
  | npx wrangler secret put TOPLINE_API_BASE_URL

# From apps/sync/
printf '<CRM_API_URL>' \
  | npx wrangler secret put TOPLINE_API_BASE_URL
```

Cloudflare Workers hot-load secrets on the next request — no redeploy
needed once they're set.

Symptom if you skip this: `topline_setup_check` returns "Auth: ❌ HTTP 530"
on the auth probe (NOT 401 — 401 would mean bad PIT; 530 means the
upstream host is unreachable, which is what happens when the Worker
fetches `https://api.example.com/...`).

`ADMIN_TOKEN` and `TOKEN_SIGNING_SECRET` are already managed via this
same `wrangler secret put` pattern. `TOPLINE_API_BASE_URL` is now the
third secret each Worker needs.

## Per-tenant configuration (one-time per location)

After the worker is deployed with these PRs landed:

### Required env vars (per stdio install — `claude_desktop_config.json` / `claude mcp add`)

```
TOPLINE_PIT=<<Private Integration Token>>
TOPLINE_LOCATION_ID=<<Location ID>>
TOPLINE_API_BASE_URL=<<API base URL from onboarding>>
```

The Cloudflare-hosted remote Worker already has `TOPLINE_API_BASE_URL`
baked in via the Wrangler secret (see "Worker secrets" above); only
stdio installs need to pass it through the install snippet.

### Optional (per pillar)

```
# P3 Spend
TOPLINE_BREX_API_KEY=
TOPLINE_QBO_REFRESH_TOKEN=
TOPLINE_QBO_CLIENT_ID=
TOPLINE_QBO_CLIENT_SECRET=
TOPLINE_QBO_REALM_ID=

# P6 Slack
TOPLINE_SLACK_WEBHOOK_URL=
```

Same as `TOPLINE_API_BASE_URL`: any of these the remote Worker needs to
provide should be set via `wrangler secret put`. Stdio installs set them
in the local install snippet's env block.

### Tenant config blob (`_topline_marketing_config`)

Run once per tenant via `topline_set_marketing_config`:

```jsonc
{
  "sources": ["google", "meta", "linkedin", "x", "software-advice", "marketplaces",
              "topline-connect", "topline-signals", "cold-email", "email-subscribers",
              "influencer", "referral", "organic"],
  "mediums": ["cpc", "cpm", "paid-social", "social-organic", "email",
              "marketplace", "influencer", "referral", "organic", "banner"],
  "attribution": {
    "qualified_pipeline_id_or_name": "Qualified",
    "closed_won_stage_names": ["Won", "Closed Won"],
    "stage_probabilities": {
      "Qualified": 0.5,
      "Proposal Sent": 0.7,
      "Verbal Yes": 0.9,
      "Won": 1.0
    },
    "attribution_models": ["first-touch", "last-touch"]
  },
  "spend_rules": [
    { "when": { "merchant_regex": "Meta|Facebook" }, "classify_as": "meta" },
    { "when": { "merchant_regex": "Google.*Ads" }, "classify_as": "google" },
    { "when": { "merchant_regex": "LinkedIn" }, "classify_as": "linkedin" },
    { "when": { "memo_regex": "Software Advice" }, "classify_as": "software-advice" }
  ],
  "slack": {
    "webhook_url": ""
  }
}
```

The example uses Topline's 13-channel taxonomy. Client tenants override with
their own sources/mediums/pipelines.

### Initialize attribution custom fields

Run once per tenant after `topline_set_marketing_config`:

```
topline_init_attribution_fields
```

Creates the six custom fields on contacts (`utm_source_first`, `utm_medium_first`,
`utm_campaign_first`, `utm_source_last`, `utm_medium_last`, `utm_campaign_last`)
needed for the P4 attribution view. Idempotent — safe to re-run.

## Post-merge smoke tests

Run these against the live PIT after each pillar lands.

### After #8 (white-label scrub)

- The `.github/workflows/white-label-check.yml` CI job (canonical vendor-name grep) returns zero matches on every PR.
- `npm run build` + `npm run test` + `npm run worker:typecheck` clean.
- `topline_setup_check` still green.

### After #9 (P1A Social Planner)

- `topline_social_account action=list` returns connected accounts.
- `topline_social_post action=list` (with optional date filters) returns recent posts.
- Create a draft post via `topline_social_post action=create` with a future schedule, verify it appears in the CRM social planner UI, then delete via `topline_social_post action=delete`.

### After #10 (P1D Agent Studio)

- `topline_agent action=list` returns agent inventory.
- `topline_agent action=get agentId=<one of them>` returns metadata + versions.

### After #11 (P1C Email Campaigns)

- `topline_email_template action=list` returns templates.
- `topline_email_campaign action=list` returns campaigns.
- If either returns 404, fix the endpoint paths in `apps/edge/src/tools/email_campaigns.ts` and ship a follow-up PR. Paths in this PR were best-effort.

### After #12 (P1B Ad Publishing)

For each of the three networks, confirm read-only calls succeed under PIT:

- `topline_fb_ad_account action=list`
- `topline_google_ad_account action=list`
- `topline_linkedin_ad_account action=list`

If any return 401/403, demote that network's catalog entry from `exposed` to
`requires_oauth` with a probe-note dated today.

### After #15 (P4 Attribution views)

- `topline_describe_schema` should list `contact_attribution` and `opportunity_attribution` as views.
- Query: `SELECT * FROM contact_attribution LIMIT 5` via `topline_execute_query`. Channels will fall back to `contacts.source` until the homepage form integration populates the UTM custom fields.

### After #13 (P2 UTM)

- `topline_get_marketing_config` returns the bundled defaults (or the tenant's overrides).
- `topline_set_marketing_config` with Topline's full taxonomy seeds the config.
- `topline_register_campaign_utm slug=test-campaign source=google medium=cpc campaign=test` succeeds.
- `topline_build_utm_url websiteUrl="https://www.topline.com" slug=test-campaign` returns a URL with `utm_source=google&utm_medium=cpc&utm_campaign=test`.
- `topline_lint_utm url="https://www.topline.com?utm_source=meta_ads"` returns a typo warning suggesting `meta`.

### After #14 (P3 Spend)

With Brex configured:

- `topline_list_spend_providers` shows `brex: configured`.
- `topline_list_spend_transactions since=2026-04-01 until=2026-05-01` returns Brex card transactions.
- `topline_add_spend_classification_rule when='{ "merchant_regex": "Meta" }' classify_as=meta` succeeds.
- `topline_get_channel_spend since=2026-04-01 until=2026-05-01` returns per-channel rollup with `meta` populated from the rule above.

With QBO configured:

- `topline_reconcile_spend provider_a=brex provider_b=qbo since=... until=...` returns the delta.

### After #16 (P6 Form submissions)

- `topline_get_slack_config` shows the active source (env vs marketing_config) or `configured: false`.
- `topline_notify_slack text="P6 smoke test"` posts to the configured channel (or no-ops gracefully).
- `topline_dispatch_form_submission submission='{...}'` classifies channel + posts to Slack.

### After #17 (P5 Dashboard)

- `topline_get_marketing_dashboard` returns the composite payload.
- `channel_rollup` has real `spend` values per channel, with `opportunities`/`mrr_*` as `null`.
- `todo_sql_widgets` contains three SQL strings — run them via `topline_execute_query` against the P4 views to fill in the rest.

## Known follow-ups (not blocking these merges)

- **CI grep guard.** `.github/workflows/white-label-check.yml` is ready but the current `gh` OAuth token lacks `workflow` scope. After `gh auth refresh -s workflow`, force-push the file to PR #8 (or land as a follow-up PR).
- **Homepage form UTM capture.** P4 attribution falls back to `contacts.source` until web pages set the six `utm_*_first/last` custom fields on form submission. Requires a small browser-side JS snippet, out of scope for these PRs.
- **Worker-mode dashboard SQL execution.** P5's `topline_get_marketing_dashboard` returns SQL strings rather than running them, because the SQL surface needs the LocationDO context. A follow-up will fold the SQL execution into the dashboard tool when run in Worker mode.
- **D1 persistence for spend.** P3 ships live-fetch only. A follow-up will sync spend transactions into a partitioned D1 table so historical data is queryable via `topline_execute_query`.
- **Live PIT probe-confirm for ad publishing.** Tools were exposed optimistically. Demote per-network catalog entries to `requires_oauth` if PIT auth fails post-merge.
