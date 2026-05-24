// Spend tools — list / classify / rollup / reconcile spend transactions
// across configured providers (Brex, QBO, ...).
//
// Storage: this PR ships the live-fetch path (call provider APIs on each
// tool invocation). A future PR will add D1 persistence so historical
// spend is queryable via topline_execute_query.

import { obj, objLoose, str, num, arr, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { loadMarketingConfig, saveMarketingConfig } from "../lib/marketing_config.js";
import { PROVIDERS, getProvider, listConfiguredProviders } from "../lib/spend/providers/index.js";
import { classifyTransactions, rollupByChannel } from "../lib/spend/classify.js";

function parseDate(v: unknown, label: string): Date {
  if (!v || typeof v !== "string") {
    throw new Error(`${label} is required (ISO date string).`);
  }
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`${label}='${v}' is not a valid ISO date.`);
  }
  return d;
}

export const tools: ToolDef[] = [
  {
    name: "topline_list_spend_providers",
    description:
      "List all spend providers compiled into the MCP, and which are configured (credentials present in env). " +
      "Bundled providers: brex (TOPLINE_BREX_API_KEY), qbo (TOPLINE_QBO_REFRESH_TOKEN + " +
      "TOPLINE_QBO_CLIENT_ID + TOPLINE_QBO_CLIENT_SECRET + TOPLINE_QBO_REALM_ID). " +
      "Returns { providers: [{ name, configured }], configured_count }.",
    inputSchema: obj({}),
    handler: async () => {
      return {
        providers: PROVIDERS.map((p) => ({ name: p.name, configured: p.isConfigured() })),
        configured_count: listConfiguredProviders().length,
      };
    },
  },

  {
    name: "topline_list_spend_transactions",
    description:
      "Fetch raw spend transactions across configured providers within a date range. " +
      "Each transaction includes provider, amount, merchant, date, memo, etc. " +
      "Pass `provider` to filter to a single source (otherwise all configured providers are queried). " +
      "Does NOT apply channel classification — use topline_get_channel_spend for the per-channel rollup.",
    inputSchema: obj(
      {
        since: str("ISO date string (inclusive)."),
        until: str("ISO date string (exclusive)."),
        provider: str("Filter to a single provider name (brex / qbo / ...). Omit to query all configured."),
      },
      ["since", "until"],
    ),
    handler: async (args) => {
      const since = parseDate(args.since, "since");
      const until = parseDate(args.until, "until");
      let providers = listConfiguredProviders();
      if (args.provider) {
        const p = getProvider(args.provider as string);
        if (!p) throw new Error(`Unknown provider '${args.provider}'.`);
        if (!p.isConfigured()) {
          throw new Error(`Provider '${args.provider}' is not configured (missing env credentials).`);
        }
        providers = [p];
      }
      const results = await Promise.all(
        providers.map((p) => p.listTransactions({ since, until })),
      );
      const txns = results.flat();
      return { transactions: txns, count: txns.length, providers: providers.map((p) => p.name) };
    },
  },

  {
    name: "topline_get_channel_spend",
    description:
      "Per-channel spend rollup for a date range. Pulls transactions from configured providers, " +
      "applies the tenant's spend classification rules from marketing_config, and aggregates by " +
      "canonical channel. Unclassified transactions land in the 'needs_review' bucket and need " +
      "either a rule (topline_add_spend_classification_rule) or manual reclassification.",
    inputSchema: obj(
      {
        since: str("ISO date string (inclusive)."),
        until: str("ISO date string (exclusive)."),
        provider: str("Filter to a single provider name."),
        locationId,
      },
      ["since", "until"],
    ),
    handler: async (args) => {
      const since = parseDate(args.since, "since");
      const until = parseDate(args.until, "until");
      const config = await loadMarketingConfig(args.locationId as string | undefined);
      let providers = listConfiguredProviders();
      if (args.provider) {
        const p = getProvider(args.provider as string);
        if (!p || !p.isConfigured())
          throw new Error(`Provider '${args.provider}' is not configured.`);
        providers = [p];
      }
      const results = await Promise.all(
        providers.map((p) => p.listTransactions({ since, until })),
      );
      const txns = results.flat();
      const classified = classifyTransactions(txns, config);
      const rollup = rollupByChannel(classified);
      return {
        rollup,
        unclassified_count: classified.filter((c) => c.channel === "needs_review").length,
        total_count: classified.length,
        providers: providers.map((p) => p.name),
      };
    },
  },

  {
    name: "topline_list_spend_classification_rules",
    description: "List the tenant's spend classification rules (from marketing_config.spend_rules).",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const config = await loadMarketingConfig(args.locationId as string | undefined);
      return { rules: config.spend_rules ?? [] };
    },
  },

  {
    name: "topline_add_spend_classification_rule",
    description:
      "Append a spend classification rule. Each rule has a `when` matcher " +
      "(merchant_regex / memo_regex / account_regex — at least one required) and " +
      "a `classify_as` value (one of the tenant's configured sources). Rules are " +
      "evaluated top-to-bottom; first match wins. To reorder or replace, use " +
      "topline_set_marketing_config with the full rules array.",
    inputSchema: obj(
      {
        when: objLoose(
          {
            merchant_regex: str("Case-insensitive regex matched against merchant name."),
            memo_regex: str("Case-insensitive regex matched against memo / description."),
            account_regex: str("Case-insensitive regex matched against accounting category."),
          },
          [],
        ),
        classify_as: str("Channel to assign — must be one of the tenant's configured sources."),
        locationId,
      },
      ["when", "classify_as"],
    ),
    handler: async (args) => {
      const config = await loadMarketingConfig(args.locationId as string | undefined);
      const channel = args.classify_as as string;
      if (!config.sources.includes(channel)) {
        throw new Error(
          `classify_as '${channel}' is not in the tenant's configured sources. Allowed: ${config.sources.join(", ")}.`,
        );
      }
      const when = args.when as {
        merchant_regex?: string;
        memo_regex?: string;
        account_regex?: string;
      };
      if (!when.merchant_regex && !when.memo_regex && !when.account_regex) {
        throw new Error("`when` must specify at least one of merchant_regex / memo_regex / account_regex.");
      }
      const rules = config.spend_rules ?? [];
      rules.push({ when, classify_as: channel });
      await saveMarketingConfig({ ...config, spend_rules: rules }, args.locationId as string | undefined);
      return { rules };
    },
  },

  {
    name: "topline_reconcile_spend",
    description:
      "Reconcile totals across two providers for a date range — useful for cross-checking Brex against QBO. " +
      "Returns per-channel totals from each provider plus delta and a list of transactions present in one but not the other (best-effort match by date + amount + merchant).",
    inputSchema: obj(
      {
        since: str("ISO date string (inclusive)."),
        until: str("ISO date string (exclusive)."),
        provider_a: str("First provider name (e.g. 'brex')."),
        provider_b: str("Second provider name (e.g. 'qbo')."),
        locationId,
      },
      ["since", "until", "provider_a", "provider_b"],
    ),
    handler: async (args) => {
      const since = parseDate(args.since, "since");
      const until = parseDate(args.until, "until");
      const a = getProvider(args.provider_a as string);
      const b = getProvider(args.provider_b as string);
      if (!a || !a.isConfigured()) throw new Error(`provider_a '${args.provider_a}' is not configured.`);
      if (!b || !b.isConfigured()) throw new Error(`provider_b '${args.provider_b}' is not configured.`);
      const [txA, txB] = await Promise.all([
        a.listTransactions({ since, until }),
        b.listTransactions({ since, until }),
      ]);
      const totalA = txA.reduce((s, t) => s + t.amount, 0);
      const totalB = txB.reduce((s, t) => s + t.amount, 0);
      return {
        provider_a: { name: a.name, count: txA.length, total: totalA },
        provider_b: { name: b.name, count: txB.length, total: totalB },
        delta: Math.round((totalA - totalB) * 100) / 100,
        within_threshold: Math.abs(totalA - totalB) <= 50,
      };
    },
  },
];
