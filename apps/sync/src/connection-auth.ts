import { isPrintableAscii } from "@topline/shared";
import {
  loadAndDecryptConnection,
  loadConnection,
  type DecryptedConnection,
} from "@topline/shared-auth";

export interface SyncConnectionAuthorizationEnv {
  TOKEN_SIGNING_SECRET: string;
  CONNECTIONS: KVNamespace;
  CONNECTION_AUTH_DO: DurableObjectNamespace;
}

/**
 * A stored connection whose PIT has a character outside printable ASCII. It
 * can't become the CRM's Authorization header, and the runtime's warning about
 * such a header prints the value, so sync refuses the connection before any
 * header is built. The error carries the Location ID for the cron's skip line
 * and never any part of the PIT.
 */
export class UnsendablePitError extends Error {
  readonly locationId: string;

  constructor(locationId: string) {
    super("Connection is unavailable: its token has a character that can't be part of a token.");
    this.name = "UnsendablePitError";
    this.locationId = locationId;
  }
}

interface ConnectionAuthStub {
  getOrBootstrap(input: {
    location_id: string;
    credential_exists: boolean;
  }): Promise<{ status: "active" | "revoked"; location_id: string }>;
}

export async function loadAuthorizedSyncConnection(
  env: SyncConnectionAuthorizationEnv,
  connectionId: string,
): Promise<DecryptedConnection> {
  const stored = await loadConnection(env.CONNECTIONS, connectionId);
  if (!stored) throw new Error("Connection is unavailable.");

  const id = env.CONNECTION_AUTH_DO.idFromName(connectionId);
  const stub = env.CONNECTION_AUTH_DO.get(id) as unknown as ConnectionAuthStub;
  try {
    const authorization = await stub.getOrBootstrap({
      location_id: stored.location_id,
      credential_exists: true,
    });
    if (authorization.status !== "active" || authorization.location_id !== stored.location_id) {
      throw new Error("inactive");
    }
  } catch {
    throw new Error("Connection is unavailable.");
  }

  const connection = await loadAndDecryptConnection(
    env.CONNECTIONS,
    connectionId,
    env.TOKEN_SIGNING_SECRET,
  );
  if (!connection) throw new Error("Connection is unavailable.");
  if (!isPrintableAscii(connection.pit)) throw new UnsendablePitError(connection.location_id);
  return connection;
}
