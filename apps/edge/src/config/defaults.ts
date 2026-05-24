// Generic out-of-the-box marketing config for a freshly installed tenant.
//
// IMPORTANT: this is a GENERIC starter — not Topline's specific taxonomy.
// Topline's own 13-channel taxonomy (software-advice, topline-connect,
// topline-signals, cold-email, email-subscribers, marketplaces,
// influencer, etc.) lives in Topline's own _topline_marketing_config
// custom value, seeded during Topline's onboarding. Every tenant
// installs the same MCP and configures their own list.

export interface MarketingConfig {
  /** Allowed utm_source values. Channels the dashboard rolls up to. */
  sources: string[];
  /** Allowed utm_medium values. */
  mediums: string[];
  /**
   * Pipeline / stage rules for the attribution model (P4).
   * Tenant must set these before attribution analytics work.
   */
  attribution?: {
    /** Pipeline id or name that marks an opportunity as "Qualified". */
    qualified_pipeline_id_or_name?: string;
    /** Stage names that count as Closed Won (terminal won). */
    closed_won_stage_names?: string[];
    /** Stage name → probability multiplier for forecast MRR. */
    stage_probabilities?: Record<string, number>;
    /** Which attribution models the dashboard exposes. */
    attribution_models?: Array<"first-touch" | "last-touch">;
  };
  /**
   * Channel-classification rules for spend transactions (P3).
   * Each rule: a merchant/memo/account regex → which canonical
   * source the transaction maps to.
   */
  spend_rules?: Array<{
    when: { merchant_regex?: string; memo_regex?: string; account_regex?: string };
    classify_as: string;
  }>;
  /** Optional dashboard layout config (P5). */
  dashboard_layout?: {
    widgets: Array<{ widget: string; options?: Record<string, unknown> }>;
  };
  /** Optional Slack target for form-submission notifications (P6). */
  slack?: { webhook_url?: string };
}

export const DEFAULT_MARKETING_CONFIG: MarketingConfig = {
  sources: [
    "google",
    "meta",
    "linkedin",
    "x",
    "email",
    "referral",
    "organic",
    "marketplace",
    "influencer",
  ],
  mediums: [
    "cpc",
    "cpm",
    "paid-social",
    "social-organic",
    "email",
    "marketplace",
    "influencer",
    "referral",
    "organic",
    "banner",
  ],
  attribution: {
    attribution_models: ["first-touch", "last-touch"],
  },
};

/** Well-known custom-value name where each tenant stores their config. */
export const MARKETING_CONFIG_KEY = "_topline_marketing_config";

/** Well-known custom-value name for the campaign UTM registry. */
export const CAMPAIGN_UTM_REGISTRY_KEY = "_topline_campaign_utms";
