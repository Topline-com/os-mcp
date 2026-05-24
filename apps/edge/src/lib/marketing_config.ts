// Marketing-config storage backed by CRM custom values.
//
// Each tenant's _topline_marketing_config and _topline_campaign_utms
// live as JSON-encoded values under custom-value names in their own
// location. No central storage; multi-tenant by construction.
//
// Custom values are listed/get/upserted via toplineFetch directly
// (same endpoints as apps/edge/src/tools/custom_values.ts).

import { toplineFetch, getLocationId } from "@topline/shared";
import {
  DEFAULT_MARKETING_CONFIG,
  MARKETING_CONFIG_KEY,
  CAMPAIGN_UTM_REGISTRY_KEY,
  type MarketingConfig,
} from "../config/defaults.js";

interface CustomValue {
  id: string;
  name: string;
  value: string;
}

interface CustomValuesResponse {
  customValues?: CustomValue[];
}

/**
 * Find a custom value by its `name` field within the location.
 * Returns null if not found.
 */
async function findCustomValueByName(
  locationId: string,
  name: string,
): Promise<CustomValue | null> {
  const res = await toplineFetch<CustomValuesResponse>(
    `/locations/${locationId}/customValues`,
  );
  const items = res.customValues ?? [];
  return items.find((v) => v.name === name) ?? null;
}

/**
 * Upsert a custom value by name. Creates if absent, updates if present.
 * Returns the resulting record.
 */
async function upsertCustomValueByName(
  locationId: string,
  name: string,
  value: string,
): Promise<CustomValue> {
  const existing = await findCustomValueByName(locationId, name);
  if (existing) {
    const res = await toplineFetch<{ customValue?: CustomValue }>(
      `/locations/${locationId}/customValues/${existing.id}`,
      { method: "PUT", body: { name, value } },
    );
    return res.customValue ?? { id: existing.id, name, value };
  }
  const res = await toplineFetch<{ customValue?: CustomValue }>(
    `/locations/${locationId}/customValues`,
    { method: "POST", body: { name, value } },
  );
  return res.customValue ?? { id: "", name, value };
}

/**
 * Load the tenant's marketing config, falling back to bundled defaults
 * for any missing top-level keys.
 */
export async function loadMarketingConfig(locationIdOverride?: string): Promise<MarketingConfig> {
  const locationId = getLocationId(locationIdOverride);
  const cv = await findCustomValueByName(locationId, MARKETING_CONFIG_KEY);
  if (!cv) return { ...DEFAULT_MARKETING_CONFIG };
  let parsed: Partial<MarketingConfig> = {};
  try {
    parsed = JSON.parse(cv.value) as Partial<MarketingConfig>;
  } catch (e) {
    throw new Error(
      `Marketing config in ${MARKETING_CONFIG_KEY} is not valid JSON. ` +
        `Either fix the custom value or delete it to fall back to defaults.`,
    );
  }
  return {
    ...DEFAULT_MARKETING_CONFIG,
    ...parsed,
    sources: parsed.sources ?? DEFAULT_MARKETING_CONFIG.sources,
    mediums: parsed.mediums ?? DEFAULT_MARKETING_CONFIG.mediums,
  };
}

/**
 * Persist a marketing config to the tenant's location. Whole-blob replace —
 * callers should merge with the existing config first if they want a
 * partial update.
 */
export async function saveMarketingConfig(
  config: MarketingConfig,
  locationIdOverride?: string,
): Promise<MarketingConfig> {
  const locationId = getLocationId(locationIdOverride);
  await upsertCustomValueByName(locationId, MARKETING_CONFIG_KEY, JSON.stringify(config));
  return config;
}

// ---------------------------------------------------------------------------
// Campaign UTM registry
// ---------------------------------------------------------------------------

export interface CampaignUtmEntry {
  slug: string;
  source: string;
  medium: string;
  campaign?: string;
  campaignId?: string;
  term?: string;
  content?: string;
  created_at?: string;
}

export type CampaignUtmRegistry = Record<string, CampaignUtmEntry>;

export async function loadCampaignUtmRegistry(
  locationIdOverride?: string,
): Promise<CampaignUtmRegistry> {
  const locationId = getLocationId(locationIdOverride);
  const cv = await findCustomValueByName(locationId, CAMPAIGN_UTM_REGISTRY_KEY);
  if (!cv) return {};
  try {
    return JSON.parse(cv.value) as CampaignUtmRegistry;
  } catch {
    throw new Error(
      `Campaign UTM registry in ${CAMPAIGN_UTM_REGISTRY_KEY} is not valid JSON.`,
    );
  }
}

export async function saveCampaignUtmRegistry(
  registry: CampaignUtmRegistry,
  locationIdOverride?: string,
): Promise<CampaignUtmRegistry> {
  const locationId = getLocationId(locationIdOverride);
  await upsertCustomValueByName(
    locationId,
    CAMPAIGN_UTM_REGISTRY_KEY,
    JSON.stringify(registry),
  );
  return registry;
}
