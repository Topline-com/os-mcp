// Meta Ads spend provider — pulls per-campaign spend from the CRM's
// /ad-publishing/facebook/reporting endpoint.
//
// Activation: works automatically if the location has Meta connected in
// the CRM Integrations UI and the PIT has the Facebook ad-publishing
// scope. No env vars to set.
//
// Output: one SpendTxn per campaign with channel pre-set to "meta".

import { toplineFetch, getLocationId } from "@topline/shared";
import type { SpendProvider, SpendTxn } from "../types.js";

interface ReportingRow {
  id?: string;
  campaign_id?: string;
  campaignId?: string;
  name?: string;
  campaign_name?: string;
  campaignName?: string;
  spend?: number | string;
  amount_spent?: number | string;
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

function toAmount(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = Number(v);
    if (!Number.isNaN(n)) return n;
  }
  return 0;
}

export const metaAdsProvider: SpendProvider = {
  name: "meta_ads",

  isConfigured() {
    // PIT auth is gated upstream; the reporting endpoint will return an
    // error if Meta isn't connected for this location.
    return true;
  },

  async listTransactions(opts) {
    const locationId = getLocationId();
    const dateFrom = opts.since.toISOString().slice(0, 10);
    const dateTo = opts.until.toISOString().slice(0, 10);

    // The CRM's ad-publishing reporting endpoint contract is partially
    // probed: it requires `level` (account / campaign / adset / ad),
    // `dateFrom`, `dateTo`. We request `level=campaign` to get one row
    // per campaign — enough granularity for channel rollup without
    // pulling per-ad data. If the endpoint contract changes, this
    // provider returns [] (caller treats as "no spend data").
    let body: ReportingResponse;
    try {
      body = await toplineFetch<ReportingResponse>(
        `/ad-publishing/facebook/reporting`,
        { query: { locationId, level: "campaign", dateFrom, dateTo } },
      );
    } catch {
      return [];
    }

    const rows = pickArray(body);
    return rows
      .map<SpendTxn | null>((r) => {
        const amount = toAmount(r.spend ?? r.amount_spent);
        if (!amount) return null;
        const id = String(r.id ?? r.campaign_id ?? r.campaignId ?? `meta-${dateFrom}-${dateTo}`);
        const name = String(r.name ?? r.campaign_name ?? r.campaignName ?? "Meta campaign");
        return {
          id: `meta_ads:${id}`,
          provider: "meta_ads",
          amount,
          date: String(r.date_stop ?? r.date_start ?? dateTo),
          merchant: "Meta Ads",
          memo: name,
          card: id,
          channel: "meta",
          raw: r,
        };
      })
      .filter((t): t is SpendTxn => t !== null);
  },
};
