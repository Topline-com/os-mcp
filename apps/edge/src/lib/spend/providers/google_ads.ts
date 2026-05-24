// Google Ads spend provider — pulls per-campaign spend from the CRM's
// /ad-publishing/google/reporting endpoint.
//
// Activation: works automatically if the location has Google Ads
// connected in the CRM Integrations UI and the PIT has the Google
// ad-publishing scope (regenerate the PIT with Select-All if the scope
// was added after the original PIT was issued).
//
// Output: one SpendTxn per campaign with channel pre-set to "google".

import { toplineFetch, getLocationId } from "@topline/shared";
import type { SpendProvider, SpendTxn } from "../types.js";

interface ReportingRow {
  id?: string;
  campaign_id?: string;
  campaignId?: string;
  name?: string;
  campaign_name?: string;
  campaignName?: string;
  cost?: number | string;
  cost_micros?: number | string;
  spend?: number | string;
  date_start?: string;
  date_stop?: string;
  [key: string]: unknown;
}

interface ReportingResponse {
  data?: ReportingRow[];
  rows?: ReportingRow[];
  list?: ReportingRow[];
  results?: ReportingRow[];
  [key: string]: unknown;
}

function pickArray(body: ReportingResponse): ReportingRow[] {
  if (Array.isArray(body)) return body as unknown as ReportingRow[];
  return body.data ?? body.rows ?? body.list ?? body.results ?? [];
}

function toAmount(row: ReportingRow): number {
  // Google Ads reports cost in micros (1 USD = 1_000_000 micros). The
  // CRM may pass either field through. Prefer normal cost; fall back to
  // micros / 1e6; finally try the generic `spend`.
  if (typeof row.cost === "number") return row.cost;
  if (typeof row.cost === "string") {
    const n = Number(row.cost);
    if (!Number.isNaN(n)) return n;
  }
  if (typeof row.cost_micros === "number") return row.cost_micros / 1e6;
  if (typeof row.cost_micros === "string") {
    const n = Number(row.cost_micros);
    if (!Number.isNaN(n)) return n / 1e6;
  }
  if (typeof row.spend === "number") return row.spend;
  if (typeof row.spend === "string") {
    const n = Number(row.spend);
    if (!Number.isNaN(n)) return n;
  }
  return 0;
}

export const googleAdsProvider: SpendProvider = {
  name: "google_ads",

  isConfigured() {
    return true;
  },

  async listTransactions(opts) {
    const locationId = getLocationId();
    const dateFrom = opts.since.toISOString().slice(0, 10);
    const dateTo = opts.until.toISOString().slice(0, 10);

    let body: ReportingResponse;
    try {
      body = await toplineFetch<ReportingResponse>(
        `/ad-publishing/google/reporting`,
        { query: { locationId, level: "campaign", dateFrom, dateTo } },
      );
    } catch {
      return [];
    }

    const rows = pickArray(body);
    return rows
      .map<SpendTxn | null>((r) => {
        const amount = toAmount(r);
        if (!amount) return null;
        const id = String(r.id ?? r.campaign_id ?? r.campaignId ?? `google-${dateFrom}-${dateTo}`);
        const name = String(r.name ?? r.campaign_name ?? r.campaignName ?? "Google campaign");
        return {
          id: `google_ads:${id}`,
          provider: "google_ads",
          amount,
          date: String(r.date_stop ?? r.date_start ?? dateTo),
          merchant: "Google Ads",
          memo: name,
          card: id,
          channel: "google",
          raw: r,
        };
      })
      .filter((t): t is SpendTxn => t !== null);
  },
};
