/// <reference types="@cloudflare/workers-types" />

// Edge-specific request context — per-request bindings tool handlers need.
//
// Why separate from packages/shared's credentialsContext? That one lives
// in the stdio + worker shared code and carries tenant creds (PIT + loc).
// The DO binding only exists in the Worker runtime, and adding it to the
// shared type would pull Cloudflare-types into the stdio path.
//
// handleMcp wraps every dispatch in edgeContext.run so tool handlers can
// reach the DurableObjectNamespace without threading env through every
// signature.

import { AsyncLocalStorage } from "node:async_hooks";
import type { LocationDO } from "@topline/shared-do";

export interface EdgeRequestContext {
  /** The LocationDO namespace binding; used by SQL tools to RPC the tenant's DO. */
  location_do: DurableObjectNamespace<LocationDO>;
}

export const edgeContext = new AsyncLocalStorage<EdgeRequestContext>();
