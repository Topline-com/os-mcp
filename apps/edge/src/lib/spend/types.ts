// Spend provider interface — common contract for every expense source
// (Brex, QBO, Ramp, Mercury, Xero, etc.). Adding a new provider means
// writing one file implementing this interface + registering it in
// providers/index.ts. No core changes needed.

export interface SpendTxn {
  /** Provider-side stable id for the transaction. */
  id: string;
  /** Which provider this came from ("brex", "qbo", "ramp", etc.). */
  provider: string;
  /** Charge amount in dollars (positive for spend, negative for refunds). */
  amount: number;
  /** ISO 8601 date string of the transaction. */
  date: string;
  /** Merchant / vendor name. */
  merchant: string;
  /** Free-form memo / description. */
  memo?: string;
  /** Card or account identifier (last-4, account name, etc.). */
  card?: string;
  /** Accounting category if available (QBO expense category, etc.). */
  account?: string;
  /** Raw provider payload, preserved for debugging / reclassification. */
  raw?: unknown;
}

export interface SpendProvider {
  /** Stable provider key — used in SpendTxn.provider. */
  name: string;
  /** True iff env credentials are present and the provider can fetch. */
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
