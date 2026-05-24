// Spend provider interface — common contract for every spend source.
//
// Providers pull pre-aggregated spend from the CRM's ad-publishing
// reporting endpoints (Meta, Google, LinkedIn). Each provider knows
// which channel its data belongs to and self-classifies via the
// optional `channel` field on SpendTxn — the classification engine
// applies regex rules only for transactions that arrive unclassified
// (e.g. manual non-ad-network entries added later).

export interface SpendTxn {
  /** Provider-side stable id for the transaction (or synthetic id for aggregates). */
  id: string;
  /** Which provider this came from ("meta_ads", "google_ads", "linkedin_ads", etc.). */
  provider: string;
  /** Charge amount in dollars (positive for spend, negative for refunds). */
  amount: number;
  /** ISO 8601 date string of the transaction (or period end for aggregates). */
  date: string;
  /** Merchant / vendor name. */
  merchant: string;
  /** Free-form memo / description (e.g. campaign name). */
  memo?: string;
  /** Card or account identifier (ad-account id for ad-network providers). */
  card?: string;
  /** Accounting category if available. */
  account?: string;
  /**
   * Pre-set channel from the tenant's source taxonomy. Ad-network
   * providers always set this (they know which channel they serve);
   * manual-entry or generic-merchant providers leave it undefined and
   * the classification engine applies regex rules. When set, classify
   * passes the value through unchanged.
   */
  channel?: string;
  /** Raw provider payload, preserved for debugging / reclassification. */
  raw?: unknown;
}

export interface SpendProvider {
  /** Stable provider key — used in SpendTxn.provider. */
  name: string;
  /** True if the provider can fetch (PIT auth + connected integration). */
  isConfigured(): boolean;
  /** Fetch transactions within the date range. May be empty. */
  listTransactions(opts: { since: Date; until: Date }): Promise<SpendTxn[]>;
}

export interface ClassifiedSpendTxn extends SpendTxn {
  /** Which canonical channel (from the tenant's marketing_config.sources)
   *  this transaction maps to, or "needs_review" if no rule matched. */
  channel: string;
  /** Which rule matched (rule index in the tenant's spend_rules), or null. */
  matched_rule_index?: number;
}
