// QuickBooks Online (Intuit) provider — pulls monthly expense rollups
// via Intuit's Reports API for reconciliation against card-level Brex data.
//
// Activation: set TOPLINE_QBO_REFRESH_TOKEN, TOPLINE_QBO_CLIENT_ID,
// TOPLINE_QBO_CLIENT_SECRET, TOPLINE_QBO_REALM_ID in the deployment env.
// If any are missing, isConfigured() returns false and listTransactions()
// returns [].
//
// Intuit OAuth: refresh tokens are long-lived (~100 days), short-lived
// access tokens are minted on each call. Production deployments should
// cache the access token until it expires — this stub mints a fresh
// one per call for simplicity.

import type { SpendProvider, SpendTxn } from "../types.js";

const QBO_OAUTH_BASE = "https://oauth.platform.intuit.com";
const QBO_API_BASE = "https://quickbooks.api.intuit.com";

interface QboTokenResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
  refresh_token?: string;
}

interface QboQueryResponse {
  QueryResponse?: {
    Purchase?: Array<{
      Id?: string;
      TxnDate?: string;
      TotalAmt?: number;
      PrivateNote?: string;
      AccountRef?: { name?: string };
      EntityRef?: { name?: string };
    }>;
  };
}

async function mintAccessToken(): Promise<string | null> {
  const refresh = process.env.TOPLINE_QBO_REFRESH_TOKEN?.trim();
  const clientId = process.env.TOPLINE_QBO_CLIENT_ID?.trim();
  const clientSecret = process.env.TOPLINE_QBO_CLIENT_SECRET?.trim();
  if (!refresh || !clientId || !clientSecret) return null;

  const basic = btoa(`${clientId}:${clientSecret}`);
  const res = await fetch(`${QBO_OAUTH_BASE}/oauth2/v1/tokens/bearer`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refresh }).toString(),
  });
  if (!res.ok) {
    throw new Error(`QBO token mint failed: ${res.status} ${res.statusText}`);
  }
  const body = (await res.json()) as QboTokenResponse;
  return body.access_token;
}

export const qboProvider: SpendProvider = {
  name: "qbo",

  isConfigured() {
    return Boolean(
      process.env.TOPLINE_QBO_REFRESH_TOKEN?.trim() &&
        process.env.TOPLINE_QBO_CLIENT_ID?.trim() &&
        process.env.TOPLINE_QBO_CLIENT_SECRET?.trim() &&
        process.env.TOPLINE_QBO_REALM_ID?.trim(),
    );
  },

  async listTransactions(opts) {
    const realmId = process.env.TOPLINE_QBO_REALM_ID?.trim();
    if (!realmId) return [];
    const token = await mintAccessToken();
    if (!token) return [];

    const since = opts.since.toISOString().slice(0, 10);
    const until = opts.until.toISOString().slice(0, 10);
    const query = `SELECT * FROM Purchase WHERE TxnDate >= '${since}' AND TxnDate <= '${until}' MAXRESULTS 1000`;
    const url = new URL(`${QBO_API_BASE}/v3/company/${realmId}/query`);
    url.searchParams.set("query", query);

    const res = await fetch(url.toString(), {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
    });
    if (!res.ok) {
      throw new Error(`QBO query failed: ${res.status} ${res.statusText}`);
    }
    const body = (await res.json()) as QboQueryResponse;
    const rows = body.QueryResponse?.Purchase ?? [];
    return rows.map((p) => ({
      id: p.Id ?? "",
      provider: "qbo",
      amount: p.TotalAmt ?? 0,
      date: p.TxnDate ?? since,
      merchant: p.EntityRef?.name ?? "",
      memo: p.PrivateNote,
      account: p.AccountRef?.name,
      raw: p,
    }));
  },
};
