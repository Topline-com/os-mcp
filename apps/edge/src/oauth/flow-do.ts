import { DurableObject } from "cloudflare:workers";
import type { AuthRequest } from "@cloudflare/workers-oauth-provider";

const FLOW_KEY = "flow";
const BACKFILL_KICKOFF_LEASE_MS = 30_000;
const EXPIRY_RETRY_MS = 60_000;

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
  status: "pending" | "processing" | "completed" | "expiring";
  request: AuthRequest;
  connectionId?: string;
  csrfHash: string;
  expiresAt: number;
  submissionHash?: string;
  processingLeaseHash?: string;
  processingStartedAt?: number;
  redirectTo?: string;
  backfillStatus?: "pending" | "processing" | "completed";
  backfillLeaseHash?: string;
  backfillStartedAt?: number;
};

type CodeRecord = {
  kind: "code";
  status: "pending" | "spent";
  leaseHash: string;
  expiresAt: number;
};

type FlowRecord = ConsentRecord | CodeRecord;
export type CodeReservation = "reserved" | "pending" | "spent";
export type ConsentReservation =
  | { status: "reserved"; request: AuthRequest }
  | { status: "processing" }
  | { status: "completed"; redirectTo: string }
  | { status: "invalid" };
export type BackfillReservation =
  | { status: "reserved"; connectionId: string }
  | { status: "processing" | "completed" | "invalid" };

export class OAuthFlowState {
  constructor(private readonly storage: FlowStorage) {}

  async createConsent(
    request: AuthRequest,
    connectionId: string,
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
        connectionId,
        csrfHash,
        expiresAt,
      } satisfies ConsentRecord);
      return true;
    });
    if (created) await this.storage.setAlarm?.(expiresAt);
    return created;
  }

  async reserveConsent(
    csrfHash: string,
    submissionHash: string,
    processingLeaseHash: string,
    connectionId: string,
    now: number,
  ): Promise<ConsentReservation> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (!record || record.kind !== "consent") return { status: "invalid" } as const;
      if (record.expiresAt <= now) {
        return { status: "invalid" } as const;
      }
      if (record.csrfHash !== csrfHash) return { status: "invalid" } as const;
      if (!["pending", "processing", "completed", "expiring"].includes(record.status)) {
        return { status: "invalid" } as const;
      }
      if (record.submissionHash && record.submissionHash !== submissionHash) {
        return { status: "invalid" } as const;
      }
      if (record.status === "processing") {
        return { status: "processing" } as const;
      }
      if (record.status === "expiring") return { status: "invalid" } as const;
      if (record.status === "completed") {
        return record.redirectTo
          ? { status: "completed", redirectTo: record.redirectTo } as const
          : { status: "invalid" } as const;
      }
      await txn.put(FLOW_KEY, {
        ...record,
        status: "processing",
        submissionHash,
        processingLeaseHash,
        connectionId: record.connectionId ?? connectionId,
        processingStartedAt: now,
      } satisfies ConsentRecord);
      return { status: "reserved", request: record.request } as const;
    });
  }

  async completeConsent(
    submissionHash: string,
    processingLeaseHash: string,
    redirectTo: string,
  ): Promise<boolean> {
    const completed = await this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        record.status !== "processing" ||
        record.submissionHash !== submissionHash ||
        record.processingLeaseHash !== processingLeaseHash
      ) {
        return false;
      }
      await txn.put(FLOW_KEY, {
        ...record,
        status: "completed",
        redirectTo,
        backfillStatus: "pending",
      } satisfies ConsentRecord);
      return true;
    });
    if (completed) await this.storage.setAlarm?.(Date.now());
    return completed;
  }

  async abortConsent(
    submissionHash: string,
    processingLeaseHash: string,
  ): Promise<boolean> {
    const aborted = await this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        !["processing", "completed", "expiring"].includes(record.status) ||
        record.submissionHash !== submissionHash ||
        record.processingLeaseHash !== processingLeaseHash
      ) {
        return false;
      }
      if (record.status !== "expiring") {
        await txn.put(FLOW_KEY, { ...record, status: "expiring" } satisfies ConsentRecord);
      }
      return true;
    });
    if (aborted) await this.storage.setAlarm?.(Date.now());
    return aborted;
  }

  releaseConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        record.status !== "processing" ||
        record.submissionHash !== submissionHash ||
        record.processingLeaseHash !== processingLeaseHash
      ) {
        return false;
      }
      const {
        processingLeaseHash: _processingLeaseHash,
        processingStartedAt: _processingStartedAt,
        redirectTo: _redirectTo,
        ...pending
      } = record;
      await txn.put(FLOW_KEY, { ...pending, status: "pending" } satisfies ConsentRecord);
      return true;
    });
  }

  reserveBackfill(leaseHash: string, now: number): Promise<BackfillReservation> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (!record || record.kind !== "consent" || record.status !== "completed") {
        return { status: "invalid" };
      }
      if (record.backfillStatus === "completed") return { status: "completed" };
      if (
        record.backfillStatus === "processing" &&
        (record.backfillStartedAt ?? now) + BACKFILL_KICKOFF_LEASE_MS > now
      ) {
        return { status: "processing" };
      }
      if (!record.connectionId) return { status: "invalid" };
      await txn.put(FLOW_KEY, {
        ...record,
        backfillStatus: "processing",
        backfillLeaseHash: leaseHash,
        backfillStartedAt: now,
      } satisfies ConsentRecord);
      return { status: "reserved", connectionId: record.connectionId };
    });
  }

  getAlarmState(): Promise<{ status: ConsentRecord["status"]; expiresAt: number } | null> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      return record?.kind === "consent"
        ? { status: record.status as ConsentRecord["status"], expiresAt: record.expiresAt }
        : null;
    });
  }

  completeBackfill(leaseHash: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        record.status !== "completed" ||
        record.backfillStatus !== "processing" ||
        record.backfillLeaseHash !== leaseHash
      ) return false;
      await txn.put(FLOW_KEY, {
        ...record,
        backfillStatus: "completed",
      } satisfies ConsentRecord);
      return true;
    });
  }

  releaseBackfill(leaseHash: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        record.status !== "completed" ||
        record.backfillStatus !== "processing" ||
        record.backfillLeaseHash !== leaseHash
      ) return false;
      const {
        backfillLeaseHash: _backfillLeaseHash,
        backfillStartedAt: _backfillStartedAt,
        ...pending
      } = record;
      await txn.put(FLOW_KEY, {
        ...pending,
        backfillStatus: "pending",
      } satisfies ConsentRecord);
      return true;
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

  expire(): Promise<string | null> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (record?.kind === "consent" && record.status !== "completed") {
        if (!record.connectionId) {
          await txn.delete(FLOW_KEY);
          return null;
        }
        if (record.status !== "expiring") {
          await txn.put(FLOW_KEY, { ...record, status: "expiring" } satisfies ConsentRecord);
        }
        return record.connectionId;
      }
      await txn.delete(FLOW_KEY);
      return null;
    });
  }

  completeExpiry(connectionId: string): Promise<boolean> {
    return this.storage.transaction(async (txn) => {
      const record = await txn.get<FlowRecord>(FLOW_KEY);
      if (
        !record ||
        record.kind !== "consent" ||
        record.status !== "expiring" ||
        record.connectionId !== connectionId
      ) return false;
      await txn.delete(FLOW_KEY);
      return true;
    });
  }
}

interface OAuthFlowEnv {
  CONNECTIONS: KVNamespace;
  SYNC_WORKER?: Fetcher;
  ADMIN_TOKEN?: string;
}

export class OAuthFlowDO extends DurableObject<OAuthFlowEnv> {
  private readonly flow: OAuthFlowState;

  constructor(ctx: DurableObjectState, env: OAuthFlowEnv) {
    super(ctx, env);
    this.flow = new OAuthFlowState(ctx.storage as unknown as FlowStorage);
  }

  createConsent(
    request: AuthRequest,
    connectionId: string,
    csrfHash: string,
    expiresAt: number,
  ): Promise<boolean> {
    return this.flow.createConsent(request, connectionId, csrfHash, expiresAt);
  }

  reserveConsent(
    csrfHash: string,
    submissionHash: string,
    processingLeaseHash: string,
    connectionId: string,
    now: number,
  ): Promise<ConsentReservation> {
    return this.flow.reserveConsent(
      csrfHash,
      submissionHash,
      processingLeaseHash,
      connectionId,
      now,
    );
  }

  completeConsent(
    submissionHash: string,
    processingLeaseHash: string,
    redirectTo: string,
  ): Promise<boolean> {
    return this.flow.completeConsent(submissionHash, processingLeaseHash, redirectTo);
  }

  abortConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean> {
    return this.flow.abortConsent(submissionHash, processingLeaseHash);
  }

  releaseConsent(submissionHash: string, processingLeaseHash: string): Promise<boolean> {
    return this.flow.releaseConsent(submissionHash, processingLeaseHash);
  }

  reserveBackfill(leaseHash: string, now: number): Promise<BackfillReservation> {
    return this.flow.reserveBackfill(leaseHash, now);
  }

  completeBackfill(leaseHash: string): Promise<boolean> {
    return this.flow.completeBackfill(leaseHash);
  }

  releaseBackfill(leaseHash: string): Promise<boolean> {
    return this.flow.releaseBackfill(leaseHash);
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
    const now = Date.now();
    const leaseHash = crypto.randomUUID();
    const backfill = await this.flow.reserveBackfill(leaseHash, now);

    if (backfill.status === "reserved") {
      await this.ctx.storage.setAlarm(now + EXPIRY_RETRY_MS);
      try {
        if (!this.env.SYNC_WORKER || !this.env.ADMIN_TOKEN) {
          throw new Error("Backfill service binding unavailable");
        }
        const response = await this.env.SYNC_WORKER.fetch(
          `https://sync/sync/backfill-all?connection_id=${encodeURIComponent(backfill.connectionId)}`,
          {
            method: "POST",
            headers: { Authorization: `Bearer ${this.env.ADMIN_TOKEN}` },
          },
        );
        if (!response.ok) throw new Error(`Backfill kickoff failed with status ${response.status}`);
        if (!await this.flow.completeBackfill(leaseHash)) {
          throw new Error("Backfill kickoff lease could not be completed");
        }
      } catch {
        await this.flow.releaseBackfill(leaseHash);
        await this.ctx.storage.setAlarm(now + EXPIRY_RETRY_MS);
        return;
      }
    } else if (backfill.status === "processing") {
      await this.ctx.storage.setAlarm(now + EXPIRY_RETRY_MS);
      return;
    }

    const alarmState = await this.flow.getAlarmState();
    if (alarmState && alarmState.status !== "expiring" && alarmState.expiresAt > now) {
      await this.ctx.storage.setAlarm(alarmState.expiresAt);
      return;
    }

    const orphanedConnectionId = await this.flow.expire();
    if (orphanedConnectionId) {
      try {
        await this.env.CONNECTIONS.delete(orphanedConnectionId);
        await this.flow.completeExpiry(orphanedConnectionId);
      } catch {
        await this.ctx.storage.setAlarm(now + EXPIRY_RETRY_MS);
      }
    }
  }
}
