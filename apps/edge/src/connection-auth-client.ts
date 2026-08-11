import {
  loadAndDecryptConnection,
  loadConnection,
  type DecryptedConnection,
  type StoredConnection,
} from "@topline/shared-auth";

import type { ConnectionAuthDO } from "./connection-auth-do.js";
import type {
  ConnectionClientTarget,
  ConnectionAuthorizationSnapshot,
  PersistedToolPolicy,
} from "./tool-policy.js";

export interface ConnectionAuthorizationEnv {
  TOKEN_SIGNING_SECRET: string;
  CONNECTIONS: KVNamespace;
  CONNECTION_AUTH_DO: DurableObjectNamespace<ConnectionAuthDO>;
}

export interface ActiveConnectionAuthorization {
  connection: DecryptedConnection;
  authorization: ConnectionAuthorizationSnapshot;
}

export interface ManagedConnectionAuthorization {
  connection: StoredConnection;
  authorization: ConnectionAuthorizationSnapshot;
}

export class ConnectionAuthorizationError extends Error {
  constructor() {
    super("Access token references an unavailable connection.");
    this.name = "ConnectionAuthorizationError";
  }
}

export async function loadActiveConnectionAuthorization(
  env: ConnectionAuthorizationEnv,
  connectionId: string,
): Promise<ActiveConnectionAuthorization> {
  const stored = await loadConnection(env.CONNECTIONS, connectionId);
  if (!stored) throw new ConnectionAuthorizationError();

  const stub = authorizationStub(env, connectionId);
  let authorization: ConnectionAuthorizationSnapshot;
  try {
    authorization = await stub.getOrBootstrap({
      location_id: stored.location_id,
      credential_exists: true,
    });
  } catch {
    throw new ConnectionAuthorizationError();
  }

  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) throw new ConnectionAuthorizationError();
  return { connection, authorization };
}

export async function initializeConnectionAuthorization(
  env: ConnectionAuthorizationEnv,
  connectionId: string,
  locationId: string,
  policy: PersistedToolPolicy,
  clientTarget: ConnectionClientTarget,
): Promise<ConnectionAuthorizationSnapshot> {
  return authorizationStub(env, connectionId).initialize({
    location_id: locationId,
    policy,
    client_target: clientTarget,
  });
}

export async function loadConnectionAuthorizationForManagement(
  env: ConnectionAuthorizationEnv,
  connectionId: string,
): Promise<ManagedConnectionAuthorization> {
  const connection = await loadConnection(env.CONNECTIONS, connectionId);
  if (!connection) throw new ConnectionAuthorizationError();
  try {
    const authorization = await authorizationStub(env, connectionId).getForManagement({
      location_id: connection.location_id,
      credential_exists: true,
    });
    return { connection, authorization };
  } catch {
    throw new ConnectionAuthorizationError();
  }
}

export function authorizationStub(
  env: ConnectionAuthorizationEnv,
  connectionId: string,
): DurableObjectStub<ConnectionAuthDO> {
  return env.CONNECTION_AUTH_DO.get(env.CONNECTION_AUTH_DO.idFromName(connectionId));
}
