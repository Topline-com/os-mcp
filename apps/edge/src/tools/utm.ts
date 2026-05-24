// UTM standardization tools — campaign registry + lint.
//
// The actual link-building helpers live in lib/utm.ts and are used by
// other tools (social planner, ad publishing, email campaigns) to
// auto-inject UTMs at emit time. These MCP tools are the user-facing
// surface for managing campaign slugs and validating link payloads.

import { obj, objLoose, str, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import {
  loadMarketingConfig,
} from "../lib/marketing_config.js";
import {
  loadCampaignUtmRegistry,
  saveCampaignUtmRegistry,
  type CampaignUtmEntry,
} from "../lib/marketing_config.js";
import { buildUtmUrl, lintUtm, parseUtmFromUrl } from "../lib/utm.js";

export const tools: ToolDef[] = [
  {
    name: "topline_register_campaign_utm",
    description:
      "Register or update a campaign's canonical UTM set. Same campaign slug " +
      "always emits the same utm_source / utm_medium / utm_campaign / utm_id / " +
      "utm_term / utm_content across every surface (social planner, email " +
      "campaign, ad publishing). Validates source + medium against the " +
      "tenant's marketing config. Stored in the `_topline_campaign_utms` " +
      "custom value on this location.",
    inputSchema: obj(
      {
        slug: str("Campaign slug — short stable identifier, e.g. '2026-q2-lp-funnel'."),
        source: str("utm_source. Must be in the tenant's configured sources."),
        medium: str("utm_medium. Must be in the tenant's configured mediums."),
        campaign: str("utm_campaign — human-readable name. One of campaign or campaignId required."),
        campaignId: str("utm_id — ad-network campaign id. One of campaign or campaignId required."),
        term: str("utm_term — paid keywords (Google Ads search)."),
        content: str("utm_content — A/B differentiator."),
        locationId,
      },
      ["slug", "source", "medium"],
    ),
    handler: async (args) => {
      const taxonomy = await loadMarketingConfig(args.locationId as string | undefined);
      const slug = args.slug as string;
      const source = args.source as string;
      const medium = args.medium as string;
      if (!taxonomy.sources.includes(source)) {
        throw new Error(
          `source '${source}' is not in the tenant's configured sources. Allowed: ${taxonomy.sources.join(", ")}.`,
        );
      }
      if (!taxonomy.mediums.includes(medium)) {
        throw new Error(
          `medium '${medium}' is not in the tenant's configured mediums. Allowed: ${taxonomy.mediums.join(", ")}.`,
        );
      }
      if (!args.campaign && !args.campaignId) {
        throw new Error("One of `campaign` or `campaignId` is required.");
      }
      const entry: CampaignUtmEntry = {
        slug,
        source,
        medium,
        campaign: args.campaign as string | undefined,
        campaignId: args.campaignId as string | undefined,
        term: args.term as string | undefined,
        content: args.content as string | undefined,
        created_at: new Date().toISOString(),
      };
      const registry = await loadCampaignUtmRegistry(args.locationId as string | undefined);
      registry[slug] = entry;
      await saveCampaignUtmRegistry(registry, args.locationId as string | undefined);
      return { entry };
    },
  },

  {
    name: "topline_get_campaign_utm",
    description:
      "Get a registered campaign's canonical UTM set by `slug`. Returns " +
      "null if not registered. Use this before emitting a link in any " +
      "surface to ensure consistency.",
    inputSchema: obj(
      {
        slug: str("Campaign slug."),
        locationId,
      },
      ["slug"],
    ),
    handler: async (args) => {
      const registry = await loadCampaignUtmRegistry(args.locationId as string | undefined);
      const entry = registry[args.slug as string] ?? null;
      return { entry };
    },
  },

  {
    name: "topline_list_campaign_utms",
    description:
      "List all registered campaign UTM entries for this location.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const registry = await loadCampaignUtmRegistry(args.locationId as string | undefined);
      return { entries: Object.values(registry) };
    },
  },

  {
    name: "topline_build_utm_url",
    description:
      "Build a fully-attributable URL by appending UTM params to a website " +
      "URL. Validates source + medium against the tenant's configured " +
      "taxonomy and throws on unknown values. Either pass explicit UTM " +
      "params, or pass a registered campaign `slug` to resolve from the " +
      "campaign UTM registry.",
    inputSchema: obj(
      {
        websiteUrl: str("Full destination URL (https://...)."),
        slug: str("Optional: resolve UTMs from a registered campaign slug."),
        source: str("utm_source. Required if slug not provided."),
        medium: str("utm_medium. Required if slug not provided."),
        campaign: str("utm_campaign."),
        campaignId: str("utm_id."),
        term: str("utm_term."),
        content: str("utm_content."),
        locationId,
      },
      ["websiteUrl"],
    ),
    handler: async (args) => {
      const taxonomy = await loadMarketingConfig(args.locationId as string | undefined);
      let params: Record<string, string | undefined> = {
        source: args.source as string | undefined,
        medium: args.medium as string | undefined,
        campaign: args.campaign as string | undefined,
        campaignId: args.campaignId as string | undefined,
        term: args.term as string | undefined,
        content: args.content as string | undefined,
      };
      if (args.slug) {
        const registry = await loadCampaignUtmRegistry(args.locationId as string | undefined);
        const entry = registry[args.slug as string];
        if (!entry) {
          throw new Error(
            `Campaign slug '${args.slug}' is not registered. Call topline_register_campaign_utm first.`,
          );
        }
        // Explicit params override the slug's defaults (allows per-call content variant).
        params = {
          source: params.source ?? entry.source,
          medium: params.medium ?? entry.medium,
          campaign: params.campaign ?? entry.campaign,
          campaignId: params.campaignId ?? entry.campaignId,
          term: params.term ?? entry.term,
          content: params.content ?? entry.content,
        };
      }
      const url = buildUtmUrl(
        args.websiteUrl as string,
        {
          source: params.source as string,
          medium: params.medium as string,
          campaign: params.campaign,
          campaignId: params.campaignId,
          term: params.term,
          content: params.content,
        },
        taxonomy,
      );
      return { url };
    },
  },

  {
    name: "topline_lint_utm",
    description:
      "Lint a URL (or explicit UTM params) against the tenant's marketing " +
      "config. Returns an array of warnings: missing required fields, " +
      "non-canonical source/medium values, common typos. Empty array if " +
      "everything looks good. Useful for sanity-checking links before they " +
      "ship in a post / email / ad.",
    inputSchema: obj(
      {
        url: str("URL to lint."),
        source: str("utm_source (if not parsing from URL)."),
        medium: str("utm_medium."),
        campaign: str("utm_campaign."),
        campaignId: str("utm_id."),
        term: str("utm_term."),
        content: str("utm_content."),
        locationId,
      },
      [],
    ),
    handler: async (args) => {
      const taxonomy = await loadMarketingConfig(args.locationId as string | undefined);
      const warnings = lintUtm(
        {
          url: args.url as string | undefined,
          params: args.url
            ? undefined
            : {
                source: args.source as string | undefined,
                medium: args.medium as string | undefined,
                campaign: args.campaign as string | undefined,
                campaignId: args.campaignId as string | undefined,
                term: args.term as string | undefined,
                content: args.content as string | undefined,
              },
        },
        taxonomy,
      );
      return {
        warnings,
        parsed: args.url ? parseUtmFromUrl(args.url as string) : null,
      };
    },
  },
];
