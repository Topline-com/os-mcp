// Marketing config tools — read, write, and inspect the tenant's
// _topline_marketing_config blob (channel taxonomy, attribution rules,
// spend classification rules, dashboard layout, Slack target).

import { toplineFetch, getLocationId } from "@topline/shared";
import { obj, objLoose, str, arr, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import {
  loadMarketingConfig,
  saveMarketingConfig,
} from "../lib/marketing_config.js";
import {
  DEFAULT_MARKETING_CONFIG,
  MARKETING_CONFIG_KEY,
  type MarketingConfig,
} from "../config/defaults.js";

const ATTRIBUTION_FIELDS = [
  "utm_source_first",
  "utm_medium_first",
  "utm_campaign_first",
  "utm_source_last",
  "utm_medium_last",
  "utm_campaign_last",
];

export const tools: ToolDef[] = [
  {
    name: "topline_get_marketing_config",
    description:
      "Returns this location's marketing config — the channel taxonomy " +
      "(allowed utm_source / utm_medium values), attribution rules " +
      "(qualified-pipeline name, closed-won stage names, stage probabilities " +
      "for forecast MRR), spend-classification rules, dashboard layout, " +
      "and Slack webhook target. Stored as a JSON-encoded custom value named " +
      "`_topline_marketing_config` in the location. Falls back to generic " +
      "defaults if the value doesn't exist yet.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const config = await loadMarketingConfig(args.locationId as string | undefined);
      return { config };
    },
  },

  {
    name: "topline_set_marketing_config",
    description:
      "Write the location's marketing config. WHOLE-BLOB REPLACE — pass " +
      "the complete config object you want stored. To do a partial update, " +
      "call topline_get_marketing_config first, merge your changes, then call " +
      "this. Validates that `sources` and `mediums` are non-empty arrays " +
      "of strings. Stored as JSON in the `_topline_marketing_config` " +
      "custom value on this location.",
    inputSchema: obj(
      {
        config: objLoose(
          {
            sources: arr({ type: "string" }, "Allowed utm_source values (channels)."),
            mediums: arr({ type: "string" }, "Allowed utm_medium values."),
            attribution: objLoose({}, []),
            spend_rules: arr(objLoose({}, [])),
            dashboard_layout: objLoose({}, []),
            slack: objLoose({}, []),
          },
          ["sources", "mediums"],
        ),
        locationId,
      },
      ["config"],
    ),
    handler: async (args) => {
      const raw = args.config as Partial<MarketingConfig>;
      if (!Array.isArray(raw?.sources) || raw.sources.length === 0) {
        throw new Error("config.sources must be a non-empty array of strings.");
      }
      if (!Array.isArray(raw?.mediums) || raw.mediums.length === 0) {
        throw new Error("config.mediums must be a non-empty array of strings.");
      }
      const merged: MarketingConfig = {
        ...DEFAULT_MARKETING_CONFIG,
        ...raw,
        sources: raw.sources,
        mediums: raw.mediums,
      };
      const saved = await saveMarketingConfig(merged, args.locationId as string | undefined);
      return { config: saved, stored_under: MARKETING_CONFIG_KEY };
    },
  },

  {
    name: "topline_init_attribution_fields",
    description:
      "Ensure the six attribution custom fields exist on contacts for " +
      "this location: utm_source_first, utm_medium_first, utm_campaign_first, " +
      "utm_source_last, utm_medium_last, utm_campaign_last. Idempotent — " +
      "creates each that's missing, skips ones that already exist. Run once " +
      "during marketing setup so the homepage form snippet can populate them.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      // Custom fields live on contacts; list and check for existence.
      const existing = await toplineFetch<{ customFields?: Array<{ fieldKey?: string; name?: string }> }>(
        `/locations/${loc}/customFields`,
        { query: { model: "contact" } },
      );
      const have = new Set(
        (existing.customFields ?? [])
          .flatMap((f) => [f.fieldKey, f.name])
          .filter((s): s is string => Boolean(s)),
      );
      const created: string[] = [];
      const skipped: string[] = [];
      for (const name of ATTRIBUTION_FIELDS) {
        if ([...have].some((h) => h.toLowerCase().includes(name.toLowerCase()))) {
          skipped.push(name);
          continue;
        }
        await toplineFetch(`/locations/${loc}/customFields`, {
          method: "POST",
          body: {
            name,
            dataType: "TEXT",
            model: "contact",
            placeholder: "",
          },
        });
        created.push(name);
      }
      return { created, skipped, total: ATTRIBUTION_FIELDS.length };
    },
  },
];
