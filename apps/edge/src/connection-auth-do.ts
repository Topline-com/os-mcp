import { DurableObject } from "cloudflare:workers";

import {
  ConnectionAuthorizationService,
  ConnectionAuthorizationStateError,
  type ConnectionAuthorizationRepository,
} from "./connection-auth-state.js";
import type {
  ConnectionClientTarget,
  ConnectionAuthorizationSnapshot,
  PersistedToolPolicy,
} from "./tool-policy.js";

export interface ConnectionAuthDOEnv {}

export interface BootstrapAuthorizationInput {
  location_id: string;
  credential_exists: boolean;
}

export interface InitializeAuthorizationInput {
  location_id: string;
  policy: PersistedToolPolicy;
  client_target?: ConnectionClientTarget;
}

export interface UpdateAuthorizationInput extends InitializeAuthorizationInput {
  expected_policy_version: number;
}

export interface RevokeAuthorizationInput {
  location_id: string;
  expected_policy_version: number;
}

interface AuthorizationRow {
  [key: string]: SqlStorageValue;
  schema_version: number;
  status: string;
  location_id: string;
  client_target: string;
  policy_json: string;
  policy_version: number;
  created_at: string;
  updated_at: string;
  last_verified_at: string | null;
}

export class ConnectionAuthDO extends DurableObject<ConnectionAuthDOEnv> {
  private readonly service: ConnectionAuthorizationService;

  constructor(ctx: DurableObjectState, env: ConnectionAuthDOEnv) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS authz (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        schema_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        location_id TEXT NOT NULL,
        client_target TEXT NOT NULL DEFAULT 'generic',
        policy_json TEXT NOT NULL,
        policy_version INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_verified_at TEXT
      )
    `);
    this.service = new ConnectionAuthorizationService(new SqlAuthorizationRepository(ctx));
  }

  async getOrBootstrap(
    input: BootstrapAuthorizationInput,
  ): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.getOrBootstrap(input.location_id, input.credential_exists);
  }

  async getForManagement(
    input: BootstrapAuthorizationInput,
  ): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.getForManagement(input.location_id, input.credential_exists);
  }

  async initialize(input: InitializeAuthorizationInput): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.initialize(
      input.location_id,
      input.policy,
      undefined,
      input.client_target ?? "generic",
    );
  }

  async updatePolicy(input: UpdateAuthorizationInput): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.updatePolicy(
      input.location_id,
      input.expected_policy_version,
      input.policy,
      undefined,
      input.client_target,
    );
  }

  async revoke(input: RevokeAuthorizationInput): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.revoke(input.location_id, input.expected_policy_version);
  }

  async touch(locationId: string): Promise<ConnectionAuthorizationSnapshot> {
    return this.service.touch(locationId);
  }
}

class SqlAuthorizationRepository implements ConnectionAuthorizationRepository {
  constructor(private readonly ctx: DurableObjectState) {}

  read(): ConnectionAuthorizationSnapshot | null {
    const rows = [...this.ctx.storage.sql.exec<AuthorizationRow>(
      `SELECT schema_version, status, location_id, client_target, policy_json, policy_version,
              created_at, updated_at, last_verified_at
         FROM authz
        WHERE singleton = 1`,
    )];
    if (rows.length === 0) return null;
    const row = rows[0];
    try {
      return {
        schema_version: row.schema_version as 1,
        status: row.status as ConnectionAuthorizationSnapshot["status"],
        location_id: row.location_id,
        client_target: row.client_target as ConnectionClientTarget,
        policy: JSON.parse(row.policy_json) as PersistedToolPolicy,
        policy_version: row.policy_version,
        created_at: row.created_at,
        updated_at: row.updated_at,
        ...(row.last_verified_at ? { last_verified_at: row.last_verified_at } : {}),
      };
    } catch {
      throw new ConnectionAuthorizationStateError("corrupt_state");
    }
  }

  write(snapshot: ConnectionAuthorizationSnapshot): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO authz(
         singleton, schema_version, status, location_id, client_target, policy_json,
         policy_version, created_at, updated_at, last_verified_at
       ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(singleton) DO UPDATE SET
         schema_version = excluded.schema_version,
         status = excluded.status,
         location_id = excluded.location_id,
         client_target = excluded.client_target,
         policy_json = excluded.policy_json,
         policy_version = excluded.policy_version,
         created_at = excluded.created_at,
         updated_at = excluded.updated_at,
         last_verified_at = excluded.last_verified_at`,
      snapshot.schema_version,
      snapshot.status,
      snapshot.location_id,
      snapshot.client_target ?? "generic",
      JSON.stringify(snapshot.policy),
      snapshot.policy_version,
      snapshot.created_at,
      snapshot.updated_at,
      snapshot.last_verified_at ?? null,
    );
  }
}
