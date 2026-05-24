// UTM helper module — pure functions shared by all link-emitting tools.
//
// - buildUtmUrl: single source of truth for emitting attributable links
// - parseUtmFromUrl: extracts the 7 canonical UTM fields
// - classifyChannel: collapses a parsed UTM (or referrer) to one of the
//   tenant's configured sources
//
// Tenant taxonomy comes from MarketingConfig (loaded via marketing_config.ts).
// Bundled defaults provide a generic starter so a fresh install works
// out of the box.

import type { MarketingConfig } from "../config/defaults.js";

export interface UtmParams {
  /** Required: utm_source — one of the tenant's configured sources. */
  source: string;
  /** Required: utm_medium — one of the tenant's configured mediums. */
  medium: string;
  /** utm_campaign — required unless campaignId is provided. */
  campaign?: string;
  /** utm_id — required unless campaign is provided. */
  campaignId?: string;
  /** utm_term — paid keywords (Google Ads search). */
  term?: string;
  /** utm_content — A/B differentiator (ad creative id, post variant, etc.). */
  content?: string;
}

export interface ParsedUtm {
  source?: string;
  medium?: string;
  campaign?: string;
  campaignId?: string;
  term?: string;
  content?: string;
}

/**
 * Build a fully-attributable URL by appending UTM params. Validates
 * source + medium against the tenant's taxonomy; throws on unknown
 * values so callers can't silently emit unattributable links.
 *
 * Existing UTM params on `websiteUrl` are OVERWRITTEN by the provided
 * params. Other query params are preserved.
 */
export function buildUtmUrl(
  websiteUrl: string,
  params: UtmParams,
  taxonomy: MarketingConfig,
): string {
  if (!websiteUrl || typeof websiteUrl !== "string") {
    throw new Error("buildUtmUrl: websiteUrl is required and must be a string.");
  }
  let url: URL;
  try {
    url = new URL(websiteUrl);
  } catch (e) {
    throw new Error(`buildUtmUrl: websiteUrl '${websiteUrl}' is not a valid URL.`);
  }

  if (!params.source) {
    throw new Error("buildUtmUrl: source (utm_source) is required.");
  }
  if (!taxonomy.sources.includes(params.source)) {
    throw new Error(
      `buildUtmUrl: source '${params.source}' is not in the tenant's configured sources. ` +
        `Allowed: ${taxonomy.sources.join(", ")}. ` +
        `Add it via topline_set_marketing_config if it should be valid.`,
    );
  }
  if (!params.medium) {
    throw new Error("buildUtmUrl: medium (utm_medium) is required.");
  }
  if (!taxonomy.mediums.includes(params.medium)) {
    throw new Error(
      `buildUtmUrl: medium '${params.medium}' is not in the tenant's configured mediums. ` +
        `Allowed: ${taxonomy.mediums.join(", ")}. ` +
        `Add it via topline_set_marketing_config if it should be valid.`,
    );
  }
  if (!params.campaign && !params.campaignId) {
    throw new Error(
      "buildUtmUrl: one of campaign (utm_campaign) or campaignId (utm_id) is required.",
    );
  }

  url.searchParams.set("utm_source", params.source);
  url.searchParams.set("utm_medium", params.medium);
  if (params.campaign) url.searchParams.set("utm_campaign", params.campaign);
  if (params.campaignId) url.searchParams.set("utm_id", params.campaignId);
  if (params.term) url.searchParams.set("utm_term", params.term);
  if (params.content) url.searchParams.set("utm_content", params.content);

  return url.toString();
}

/** Extract the 7 canonical UTM fields from a URL. Returns whatever's present. */
export function parseUtmFromUrl(urlStr: string): ParsedUtm {
  let url: URL;
  try {
    url = new URL(urlStr);
  } catch {
    return {};
  }
  const get = (k: string) => url.searchParams.get(k) ?? undefined;
  return {
    source: get("utm_source"),
    medium: get("utm_medium"),
    campaign: get("utm_campaign"),
    campaignId: get("utm_id"),
    term: get("utm_term"),
    content: get("utm_content"),
  };
}

export interface ClassifyInput {
  utmSource?: string;
  utmMedium?: string;
  referrer?: string;
}

/**
 * Collapse a parsed UTM (or referrer) to one of the tenant's configured
 * sources. Order of precedence:
 *
 *   1. utm_source if it's a valid configured source → return it
 *   2. referrer host inference (google.com → google, meta hosts → meta, etc.)
 *      ONLY if the inferred channel is in the tenant's configured sources
 *   3. fallback to "organic" if configured, else the first configured source,
 *      else "unknown"
 */
export function classifyChannel(input: ClassifyInput, taxonomy: MarketingConfig): string {
  if (input.utmSource && taxonomy.sources.includes(input.utmSource)) {
    return input.utmSource;
  }
  if (input.referrer) {
    const inferred = inferChannelFromReferrer(input.referrer);
    if (inferred && taxonomy.sources.includes(inferred)) return inferred;
  }
  if (taxonomy.sources.includes("organic")) return "organic";
  return taxonomy.sources[0] ?? "unknown";
}

function inferChannelFromReferrer(referrer: string): string | undefined {
  let host: string;
  try {
    host = new URL(referrer).hostname.toLowerCase();
  } catch {
    return undefined;
  }
  if (/(^|\.)google\./.test(host)) return "google";
  if (/(^|\.)(facebook|instagram|fb)\./.test(host)) return "meta";
  if (/(^|\.)linkedin\./.test(host)) return "linkedin";
  if (/(^|\.)(x|twitter|t\.co)\b/.test(host)) return "x";
  return "referral";
}

/**
 * Lint a URL or set of UTM params. Returns an array of human-readable
 * warnings (empty if everything looks good).
 */
export function lintUtm(
  input: { url?: string; params?: ParsedUtm },
  taxonomy: MarketingConfig,
): string[] {
  const warnings: string[] = [];
  const parsed: ParsedUtm = input.params ?? (input.url ? parseUtmFromUrl(input.url) : {});

  if (!parsed.source) warnings.push("utm_source is missing (required).");
  else if (!taxonomy.sources.includes(parsed.source))
    warnings.push(
      `utm_source '${parsed.source}' is not in the tenant's configured sources. ` +
        `Allowed: ${taxonomy.sources.join(", ")}.`,
    );

  if (!parsed.medium) warnings.push("utm_medium is missing (required).");
  else if (!taxonomy.mediums.includes(parsed.medium))
    warnings.push(
      `utm_medium '${parsed.medium}' is not in the tenant's configured mediums. ` +
        `Allowed: ${taxonomy.mediums.join(", ")}.`,
    );

  if (!parsed.campaign && !parsed.campaignId)
    warnings.push("Both utm_campaign and utm_id are missing — at least one is required.");

  // Common typos
  if (parsed.source) {
    const typos: Record<string, string> = {
      meta_ads: "meta",
      "facebook-ads": "meta",
      facebook: "meta",
      instagram: "meta",
      fb: "meta",
      "google-ads": "google",
      googleads: "google",
      "linked-in": "linkedin",
      twitter: "x",
    };
    const suggestion = typos[parsed.source.toLowerCase()];
    if (suggestion && parsed.source !== suggestion)
      warnings.push(`utm_source '${parsed.source}' looks like a typo — did you mean '${suggestion}'?`);
  }

  return warnings;
}
