import {
  PolicyReauthorizationRequiredError,
  assertPolicyUpdateCanUseBearer,
  type ConnectionClientTarget,
  type ConnectionAuthorizationSnapshot,
  type PersistedToolPolicy,
} from "./tool-policy.js";

export interface ConnectionAuthorizationRepository {
  read(): ConnectionAuthorizationSnapshot | null;
  write(snapshot: ConnectionAuthorizationSnapshot): void;
}

export class ConnectionAuthorizationStateError extends Error {
  readonly reason:
    | "missing_connection"
    | "tenant_mismatch"
    | "version_conflict"
    | "revoked_connection"
    | "already_initialized"
    | "reauthorization_required"
    | "corrupt_state";

  constructor(reason: ConnectionAuthorizationStateError["reason"]) {
    super("Connection authorization is unavailable.");
    this.name = "ConnectionAuthorizationStateError";
    this.reason = reason;
  }
}

export class ConnectionAuthorizationService {
  constructor(
    private readonly repository: ConnectionAuthorizationRepository,
    private readonly canonicalToolIds: readonly string[],
  ) {}

  getOrBootstrap(
    locationId: string,
    credentialExists: boolean,
    now = new Date().toISOString(),
  ): ConnectionAuthorizationSnapshot {
    const current = this.readCurrent();
    if (current) {
      this.assertLocation(current, locationId);
      this.assertActive(current);
      return current;
    }
    if (!credentialExists) {
      throw new ConnectionAuthorizationStateError("missing_connection");
    }

    const snapshot: ConnectionAuthorizationSnapshot = {
      schema_version: 1,
      status: "active",
      location_id: requireLocation(locationId),
      client_target: "generic",
      policy: { version: 1, mode: "all" },
      policy_version: 1,
      created_at: now,
      updated_at: now,
    };
    this.repository.write(snapshot);
    return snapshot;
  }

  getForManagement(
    locationId: string,
    credentialExists: boolean,
    now = new Date().toISOString(),
  ): ConnectionAuthorizationSnapshot {
    const current = this.readCurrent();
    if (current) {
      this.assertLocation(current, locationId);
      return current;
    }
    return this.getOrBootstrap(locationId, credentialExists, now);
  }

  initialize(
    locationId: string,
    policy: PersistedToolPolicy,
    now = new Date().toISOString(),
    clientTarget: ConnectionClientTarget = "generic",
  ): ConnectionAuthorizationSnapshot {
    const current = this.readCurrent();
    if (current) {
      assertPolicy(policy);
      const sameInitialization =
        current.status === "active" &&
        current.location_id === requireLocation(locationId) &&
        (current.client_target ?? "generic") === requireClientTarget(clientTarget) &&
        JSON.stringify(current.policy) === JSON.stringify(policy);
      if (sameInitialization) return current;
      throw new ConnectionAuthorizationStateError("already_initialized");
    }
    const snapshot: ConnectionAuthorizationSnapshot = {
      schema_version: 1,
      status: "active",
      location_id: requireLocation(locationId),
      client_target: requireClientTarget(clientTarget),
      policy: structuredClone(policy),
      policy_version: 1,
      created_at: now,
      updated_at: now,
    };
    assertPolicy(policy);
    this.repository.write(snapshot);
    return snapshot;
  }

  updatePolicy(
    locationId: string,
    expectedPolicyVersion: number,
    policy: PersistedToolPolicy,
    now = new Date().toISOString(),
    clientTarget?: ConnectionClientTarget,
  ): ConnectionAuthorizationSnapshot {
    const current = this.requireCurrent(locationId);
    this.assertActive(current);
    if (current.policy_version !== expectedPolicyVersion) {
      throw new ConnectionAuthorizationStateError("version_conflict");
    }
    assertPolicy(policy);
    const nextTarget = requireClientTarget(clientTarget ?? current.client_target ?? "generic");
    try {
      assertPolicyUpdateCanUseBearer(
        current.policy,
        current.client_target ?? "generic",
        policy,
        nextTarget,
        this.canonicalToolIds,
      );
    } catch (error) {
      if (error instanceof PolicyReauthorizationRequiredError) {
        throw new ConnectionAuthorizationStateError("reauthorization_required");
      }
      throw error;
    }
    const updated: ConnectionAuthorizationSnapshot = {
      ...current,
      client_target: nextTarget,
      policy: structuredClone(policy),
      policy_version: current.policy_version + 1,
      updated_at: now,
    };
    this.repository.write(updated);
    return updated;
  }

  revoke(
    locationId: string,
    expectedPolicyVersion: number,
    now = new Date().toISOString(),
  ): ConnectionAuthorizationSnapshot {
    const current = this.requireCurrent(locationId);
    if (current.policy_version !== expectedPolicyVersion) {
      throw new ConnectionAuthorizationStateError("version_conflict");
    }
    if (current.status === "revoked") return current;
    const revoked: ConnectionAuthorizationSnapshot = {
      ...current,
      status: "revoked",
      policy_version: current.policy_version + 1,
      updated_at: now,
    };
    this.repository.write(revoked);
    return revoked;
  }

  touch(locationId: string, now = new Date().toISOString()): ConnectionAuthorizationSnapshot {
    const current = this.requireCurrent(locationId);
    this.assertActive(current);
    const touched: ConnectionAuthorizationSnapshot = {
      ...current,
      last_verified_at: now,
    };
    this.repository.write(touched);
    return touched;
  }

  private requireCurrent(locationId: string): ConnectionAuthorizationSnapshot {
    const current = this.readCurrent();
    if (!current) throw new ConnectionAuthorizationStateError("missing_connection");
    this.assertLocation(current, locationId);
    return current;
  }

  private readCurrent(): ConnectionAuthorizationSnapshot | null {
    const current = this.repository.read();
    if (!current) return null;
    if (
      current.schema_version !== 1 ||
      (current.status !== "active" && current.status !== "revoked") ||
      !Number.isSafeInteger(current.policy_version) ||
      current.policy_version < 1 ||
      typeof current.created_at !== "string" ||
      typeof current.updated_at !== "string" ||
      (current.client_target !== undefined &&
        current.client_target !== "generic" &&
        current.client_target !== "copilot_studio")
    ) {
      throw new ConnectionAuthorizationStateError("corrupt_state");
    }
    requireLocation(current.location_id);
    assertPolicy(current.policy);
    return current.client_target ? current : { ...current, client_target: "generic" };
  }

  private assertLocation(current: ConnectionAuthorizationSnapshot, locationId: string): void {
    if (current.location_id !== requireLocation(locationId)) {
      throw new ConnectionAuthorizationStateError("tenant_mismatch");
    }
  }

  private assertActive(current: ConnectionAuthorizationSnapshot): void {
    if (current.status !== "active") {
      throw new ConnectionAuthorizationStateError("revoked_connection");
    }
  }
}

function requireLocation(locationId: string): string {
  if (typeof locationId !== "string" || locationId.length === 0) {
    throw new ConnectionAuthorizationStateError("corrupt_state");
  }
  return locationId;
}

function requireClientTarget(clientTarget: ConnectionClientTarget): ConnectionClientTarget {
  if (clientTarget !== "generic" && clientTarget !== "copilot_studio") {
    throw new ConnectionAuthorizationStateError("corrupt_state");
  }
  return clientTarget;
}

function assertPolicy(policy: PersistedToolPolicy): void {
  if (
    !policy ||
    policy.version !== 1 ||
    (policy.mode !== "all" && policy.mode !== "allow") ||
    (policy.mode === "allow" &&
      (!Array.isArray(policy.tool_ids) ||
        policy.tool_ids.some((id) => typeof id !== "string" || id.length === 0)))
  ) {
    throw new ConnectionAuthorizationStateError("corrupt_state");
  }
}
