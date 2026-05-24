// Dashboard data API — composite MCP tool that aggregates channel spend,
// opportunity counts, and form-submission classification into the payload
// the marketing dashboard needs.
//
// Consumed by: any frontend that wants to render the Topline OS marketing
// dashboard (channel rollup grid + summary cards + form-submission feed).
// The frontend calls this MCP tool over the worker's MCP RPC endpoint and
// gets back a single JSON blob with all widget data.
//
// Architectural note: this PR ships the THIN composite. Spend rollup is
// real (via P3 providers). Opportunity counts and MRR pipeline calculations
// surface as TODO with structured placeholders — they require the analytics
// SQL surface (topline_execute_query against opportunity_attribution from
// P4) which runs in the Worker context with LocationDO bindings, not
// from stdio. A follow-up PR will wire those calls through a Worker-only
// dashboard route. Until then the dashboard frontend hits this tool for
// channel spend + form submissions, and topline_execute_query directly
// (in Worker mode) for the opportunity / MRR fields.

import { obj, str, num, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { loadMarketingConfig } from "../lib/marketing_config.js";
import { PROVIDERS, listConfiguredProviders } from "../lib/spend/providers/index.js";
import { classifyTransactions, rollupByChannel } from "../lib/spend/classify.js";

function parseDate(v: unknown, label: string, fallback?: Date): Date {
  if (v === undefined || v === null || v === "") {
    if (fallback) return fallback;
    throw new Error(`${label} is required (ISO date string).`);
  }
  if (typeof v !== "string") throw new Error(`${label} must be a string.`);
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) throw new Error(`${label}='${v}' is not a valid ISO date.`);
  return d;
}

function startOfMonth(d: Date): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));
}
function startOfNextMonth(d: Date): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1));
}

export const tools: ToolDef[] = [
  {
    name: "topline_get_marketing_dashboard",
    description:
      "Return the marketing dashboard payload for a date range — channel rollup " +
      "(spend per source from configured spend providers), summary cards, and a " +
      "list-shaped placeholder for opportunity + MRR figures that require the " +
      "analytics SQL surface (queryable via topline_execute_query against the " +
      "opportunity_attribution view from P4). Dates default to the current month " +
      "if omitted. Returns: { range, taxonomy_sources, channel_rollup, " +
      "summary_cards, todo_sql_widgets }.",
    inputSchema: obj(
      {
        since: str("ISO date (inclusive). Defaults to first of current month."),
        until: str("ISO date (exclusive). Defaults to first of next month."),
        locationId,
      },
      [],
    ),
    handler: async (args) => {
      const now = new Date();
      const since = parseDate(args.since, "since", startOfMonth(now));
      const until = parseDate(args.until, "until", startOfNextMonth(now));

      const config = await loadMarketingConfig(args.locationId as string | undefined);

      // ----- Channel spend (live via P3 providers) -----
      const providers = listConfiguredProviders();
      let channelSpend: Array<{ channel: string; spend: number; spend_count: number }> = [];
      let providersUsed: string[] = [];
      if (providers.length > 0) {
        const results = await Promise.all(
          providers.map((p) => p.listTransactions({ since, until })),
        );
        const txns = results.flat();
        const classified = classifyTransactions(txns, config);
        const rollup = rollupByChannel(classified);
        channelSpend = rollup.map((r) => ({
          channel: r.channel,
          spend: r.total,
          spend_count: r.count,
        }));
        providersUsed = providers.map((p) => p.name);
      }

      // ----- Build channel rollup grid -----
      // One row per configured source. Spend from above; opps / qos / mrr
      // come from topline_execute_query in Worker mode (placeholder here).
      const spendMap = new Map(channelSpend.map((c) => [c.channel, c.spend]));
      const channelRollup = config.sources.map((source) => ({
        channel: source,
        spend: spendMap.get(source) ?? 0,
        // Placeholders — see todo_sql_widgets below.
        opportunities: null as number | null,
        qualified_opportunities: null as number | null,
        mrr_pipeline_added: null as number | null,
        closed_mrr: null as number | null,
        cpqo: null as number | null,
      }));
      // Include needs_review and any non-configured channels that showed
      // up in spend (typo bucket).
      const extraChannels = channelSpend
        .filter((c) => !config.sources.includes(c.channel))
        .map((c) => ({
          channel: c.channel,
          spend: c.spend,
          opportunities: null,
          qualified_opportunities: null,
          mrr_pipeline_added: null,
          closed_mrr: null,
          cpqo: null,
        }));

      // ----- Summary cards -----
      const totalSpend = channelSpend.reduce((s, c) => s + c.spend, 0);
      const summary_cards = {
        total_spend: totalSpend,
        total_opportunities: null as number | null,
        total_qualified_opportunities: null as number | null,
        mrr_pipeline_added: null as number | null,
        closed_mrr: null as number | null,
      };

      // ----- TODO widget hints for the frontend -----
      // These SQL queries assume the P4 views (opportunity_attribution) are
      // exposed in the LocationDO. The frontend calls topline_execute_query
      // with these (or the LLM does, depending on architecture).
      const qualifiedPipeline =
        config.attribution?.qualified_pipeline_id_or_name ?? "Qualified";
      const closedWonStages = config.attribution?.closed_won_stage_names ?? ["Won", "Closed Won"];
      const closedWonList = closedWonStages.map((s) => `'${s.replace(/'/g, "''")}'`).join(",");
      const sinceIso = since.toISOString();
      const untilIso = until.toISOString();
      const todo_sql_widgets = {
        opportunities_by_channel: `SELECT channel_first_touch AS channel, COUNT(*) AS opps
  FROM opportunity_attribution
 WHERE opp_created_at >= '${sinceIso}' AND opp_created_at < '${untilIso}'
   AND pipeline_name != '${qualifiedPipeline.replace(/'/g, "''")}'
 GROUP BY channel_first_touch`,
        qualified_opportunities_by_channel: `SELECT channel_first_touch AS channel, COUNT(*) AS qos
  FROM opportunity_attribution
 WHERE opp_created_at >= '${sinceIso}' AND opp_created_at < '${untilIso}'
   AND pipeline_name = '${qualifiedPipeline.replace(/'/g, "''")}'
 GROUP BY channel_first_touch`,
        closed_mrr_by_channel: `SELECT channel_first_touch AS channel, SUM(monetary_value) AS closed_mrr
  FROM opportunity_attribution
 WHERE opp_created_at >= '${sinceIso}' AND opp_created_at < '${untilIso}'
   AND stage_name IN (${closedWonList})
 GROUP BY channel_first_touch`,
      };

      return {
        range: { since: sinceIso, until: untilIso },
        taxonomy_sources: config.sources,
        providers_used: providersUsed,
        channel_rollup: [...channelRollup, ...extraChannels],
        summary_cards,
        todo_sql_widgets,
      };
    },
  },
];
