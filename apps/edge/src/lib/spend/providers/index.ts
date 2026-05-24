// Spend provider registry. Adding a new provider: import it here and
// add to the PROVIDERS array. Everything else (tools, classification,
// reconciliation) flows through the SpendProvider interface.

import type { SpendProvider } from "../types.js";
import { brexProvider } from "./brex.js";
import { qboProvider } from "./qbo.js";

export const PROVIDERS: SpendProvider[] = [brexProvider, qboProvider];

export function getProvider(name: string): SpendProvider | undefined {
  return PROVIDERS.find((p) => p.name === name);
}

export function listConfiguredProviders(): SpendProvider[] {
  return PROVIDERS.filter((p) => p.isConfigured());
}
