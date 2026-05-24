// Brex provider — pulls transactions via Brex's Transactions API.
//
// Activation: set TOPLINE_BREX_API_KEY in the deployment env. If absent,
// isConfigured() returns false and listTransactions() returns []. The
// provider is safe to register unconditionally.
//
// API surface: Brex's v2 Transactions API. Card-level granularity per
// transaction. Pagination via cursor. We fetch all pages until the
// `until` boundary is reached.

import type { SpendProvider, SpendTxn } from "../types.js";

const BREX_BASE = "https://platform.brexapis.com";

interface BrexTxn {
  id: string;
  amount?: { amount?: number; currency?: string };
  posted_at_date?: string;
  initiated_at_date?: string;
  merchant?: { raw_descriptor?: string };
  description?: string;
  card_metadata?: { last_four?: string };
}

interface BrexListResponse {
  items?: BrexTxn[];
  next_cursor?: string | null;
}

export const brexProvider: SpendProvider = {
  name: "brex",

  isConfigured() {
    return Boolean(process.env.TOPLINE_BREX_API_KEY?.trim());
  },

  async listTransactions(opts) {
    const apiKey = process.env.TOPLINE_BREX_API_KEY?.trim();
    if (!apiKey) return [];

    const out: SpendTxn[] = [];
    const since = opts.since.toISOString().slice(0, 10);
    const until = opts.until.toISOString().slice(0, 10);
    let cursor: string | undefined;

    for (let page = 0; page < 50; page++) {
      const url = new URL(`${BREX_BASE}/v2/transactions/card/primary`);
      url.searchParams.set("posted_at_start", since);
      url.searchParams.set("posted_at_end", until);
      url.searchParams.set("limit", "100");
      if (cursor) url.searchParams.set("cursor", cursor);

      const res = await fetch(url.toString(), {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
        },
      });
      if (!res.ok) {
        throw new Error(
          `Brex listTransactions failed: ${res.status} ${res.statusText}`,
        );
      }
      const body = (await res.json()) as BrexListResponse;
      for (const t of body.items ?? []) {
        out.push({
          id: t.id,
          provider: "brex",
          amount: (t.amount?.amount ?? 0) / 100,
          date: t.posted_at_date ?? t.initiated_at_date ?? since,
          merchant: t.merchant?.raw_descriptor ?? "",
          memo: t.description,
          card: t.card_metadata?.last_four ? `****${t.card_metadata.last_four}` : undefined,
          raw: t,
        });
      }
      if (!body.next_cursor) break;
      cursor = body.next_cursor;
    }
    return out;
  },
};
