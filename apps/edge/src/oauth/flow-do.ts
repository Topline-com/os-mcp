import { DurableObject } from "cloudflare:workers";
import type { AuthRequest } from "@cloudflare/workers-oauth-provider";

const FLOW_KEY = "flow";

interface FlowTransaction {
  get<T>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
}

export interface FlowStorage {
  transaction<T>(callback: (txn: FlowTransaction) => Promise<T>): Promise<T>;
  setAlarm?(scheduledTime: number): Promise<void>;
  deleteAll?(): Promise<void>;
}

type ConsentRecord = {
  kind: "consent";
  status: "pending" | "spent";
  request: AuthRequest;
  csrfHash: string;
  expiresAt: number;
};

type CodeRecord = {
  kind: "code";
  status: "pending" | "spent";
  leaseHash: string;
  expiresAt: number;
};

type FlowRecord = ConsentRecord | CodeRecord;
export type CodeReservation = "reserved" | "pending" | "spent";

export class OAuthFlowState {
  constructor(private readonly storage: FlowStorage) {}

  async createConsent(
    request: AuthRequest,
    csrfHash: string,
    expiresAt: number,
  ): Promise<boolean> {
    const created = await this.storage.transaction(async (txn) => {
      const existing = await txn.get<FlowRecord>(FLOW_KEY);
      if (existing) return false;
      await txn.put(FLOW_KEY, {
        kind: "consent",
        status: "pending",
        request,
        csrfHash,
        expiresAt,
      } satisfies ConsentRecord);
      return true;
    });
    if (created) await this.storage.setAlarm?.(expiresAt);
    return created;
  }

  async consumeConsent(csrfHash: string, now: number): Promise<AuthRequest | null> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (!record || record.kind !== "consent" || record.status !== "pending") return null;
      if (record.expiresAt <= now) {
        await txn.delete(FLOW_KEY);
        return null;
      }
      if (record.csrfHash !== csrfHash) return null;
      await txn.put(FLOW_KEY, { ...record, status: "spent" } satisfies ConsentRecord);
      return record.request;
    });
  }

  async reserveCode(
    leaseHash: string,
    expiresAt: number,
    now: number,
  ): Promise<CodeReservation> {
    const result = await this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (record?.kind === "code") {
        if (record.expiresAt <= now) {
          await txn.delete(FLOW_KEY);
          return "spent" as const;
        }
        return record.status;
      }
      if (record) return "spent" as const;

      await txn.put(FLOW_KEY, {
        kind: "code",
        status: "pending",
        leaseHash,
        expiresAt,
      } satisfies CodeRecord);
      return "reserved" as const;
    });
    if (result === "reserved") await this.storage.setAlarm?.(expiresAt);
    return result;
  }

  releaseCode(leaseHash: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "code" ||
        record.status !== "pending" ||
        record.leaseHash !== leaseHash
      ) {
        return false;
      }
      await txn.delete(FLOW_KEY);
      return true;
    });
  }

  commitCode(leaseHash: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "code" ||
        record.status !== "pending" ||
        record.leaseHash !== leaseHash
      ) {
        return false;
      }
      await txn.put(FLOW_KEY, { ...record, status: "spent" } satisfies CodeRecord);
      return true;
    });
  }
}

interface OAuthFlowEnv {}

export class OAuthFlowDO extends DurableObject<OAuthFlowEnv> {
  private readonly flow: OAuthFlowState;

  constructor(ctx: DurableObjectState, env: OAuthFlowEnv) {
    super(ctx, env);
    this.flow = new OAuthFlowState(ctx.storage as unknown as FlowStorage);
  }

  createConsent(request: AuthRequest, csrfHash: string, expiresAt: number): Promise<boolean> {
    return this.flow.createConsent(request, csrfHash, expiresAt);
  }

  consumeConsent(csrfHash: string, now: number): Promise<AuthRequest | null> {
    return this.flow.consumeConsent(csrfHash, now);
  }

  reserveCode(leaseHash: string, expiresAt: number, now: number): Promise<CodeReservation> {
    return this.flow.reserveCode(leaseHash, expiresAt, now);
  }

  releaseCode(leaseHash: string): Promise<boolean> {
    return this.flow.releaseCode(leaseHash);
  }

  commitCode(leaseHash: string): Promise<boolean> {
    return this.flow.commitCode(leaseHash);
  }

  async alarm(): Promise<void> {
    await (this.ctx.storage as unknown as FlowStorage).deleteAll?.();
  }
}
