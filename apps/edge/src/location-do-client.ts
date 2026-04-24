/// <reference types="@cloudflare/workers-types" />

// Typed stub for LocationDO RPC calls from the edge worker.
//
// DurableObjectNamespace<T> collapses method return types to `never` in
// downstream consumers (same bug sync works around). Declaring a minimal
// RPC surface here and casting through unknown gives us correct types at
// the edge call sites without a framework upgrade.

import type {
  LocationDO,
  QueryResult,
  SchemaOverview,
  SyncState,
  TableDetails,
} from "@topline/shared-do";

export type LocationDOClient = {
  executeQuery(sql: string, params?: readonly unknown[], rowCap?: number): Promise<QueryResult>;
  describeSchema(): Promise<SchemaOverview>;
  explainTables(tables: readonly string[]): Promise<TableDetails[]>;
  getSyncState(): Promise<SyncState>;
  ping(): Promise<{ ok: true; initialized: boolean; migration_ops: number }>;
};

export function locationClient(
  ns: DurableObjectNamespace<LocationDO>,
  locationId: string,
): LocationDOClient {
  return ns.get(ns.idFromName(locationId)) as unknown as LocationDOClient;
}
