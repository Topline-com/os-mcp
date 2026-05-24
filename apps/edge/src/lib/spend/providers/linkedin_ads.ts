// LinkedIn Ads spend provider — pulls per-campaign-group spend from
// the CRM's /ad-publishing/linkedin/reporting endpoint.
//
// Activation: works automatically if the location has LinkedIn Ads
// connected in the CRM Integrations UI and the PIT has the LinkedIn
// ad-publishing scope.
//
// Output: one SpendTxn per campaign group with channel pre-set to "linkedin".

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
  cost?: number | string;
  costInUsd?: number | string;
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
  for (const key of ["spend", "cost", "costInUsd"] as const) {
    const v = row[key];
    if (typeof v === "number") return v;
    if (typeof v === "string") {
      const n = Number(v);
      if (!Number.isNaN(n)) return n;
    }
  }
  return 0;
}

export const linkedinAdsProvider: SpendProvider = {
  name: "linkedin_ads",

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
        `/ad-publishing/linkedin/reporting`,
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
        const id = String(r.id ?? r.campaign_id ?? r.campaignId ?? `linkedin-${dateFrom}-${dateTo}`);
        const name = String(r.name ?? r.campaign_name ?? r.campaignName ?? "LinkedIn campaign");
        return {
          id: `linkedin_ads:${id}`,
          provider: "linkedin_ads",
          amount,
          date: String(r.date_stop ?? r.date_start ?? dateTo),
          merchant: "LinkedIn Ads",
          memo: name,
          card: id,
          channel: "linkedin",
          raw: r,
        };
      })
      .filter((t): t is SpendTxn => t !== null);
  },
};
