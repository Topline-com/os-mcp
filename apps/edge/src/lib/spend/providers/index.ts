// Spend provider registry.
//
// Providers source per-channel spend directly from the CRM's
// ad-publishing reporting endpoints. No third-party expense
// integration required. Each ad-network provider self-classifies its
// rows (channel pre-set), so the regex-based classification engine in
// classify.ts is only exercised for transactions that arrive
// unclassified (e.g. manual non-ad-network entries added later).
//
// Adding a new ad network: write a file in this dir implementing
// SpendProvider against /ad-publishing/<network>/reporting, register
// it in PROVIDERS below.

import type { SpendProvider } from "../types.js";
import { metaAdsProvider } from "./meta_ads.js";
import { googleAdsProvider } from "./google_ads.js";
import { linkedinAdsProvider } from "./linkedin_ads.js";

export const PROVIDERS: SpendProvider[] = [
  metaAdsProvider,
  googleAdsProvider,
  linkedinAdsProvider,
];

export function getProvider(name: string): SpendProvider | undefined {
  return PROVIDERS.find((p) => p.name === name);
}

export function listConfiguredProviders(): SpendProvider[] {
  return PROVIDERS.filter((p) => p.isConfigured());
}
