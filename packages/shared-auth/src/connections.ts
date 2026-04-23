// ConnectionDirectory — KV-backed store for encrypted customer connections.
//
// Key:    connection_id (UUID v4, assigned at creation)
// Value:  stored JSON (see StoredConnection below)
//
// The stored record contains the *encrypted* PIT. To actually call GHL,
// callers must decrypt with the worker's TOKEN_SIGNING_SECRET via
// `loadAndDecryptConnection`.
//
// Writes are rare (new connection, metadata refresh). Reads happen on every
// MCP request. Cloudflare KV is eventually consistent globally; within a
// single edge location it's read-your-own-writes. For the rare case where a
// brand-new connection is hit from a different edge seconds after creation,
// we add a one-shot retry on `get` misses.

import { encryptPit, decryptPit, type EncryptedPayload } from "./encryption.js";

export interface StoredConnection {
  /** Sub-account ID this connection is scoped to. */
  location_id: string;
  /** Encrypted PIT (base64url ciphertext). */
  pit_ct: string;
  /** Encryption IV (base64url). */
  pit_iv: string;
  /** Branded name captured at creation (for audit / diagnostics). */
  brand_name: string;
  /** ISO 8601 timestamp of creation. */
  created_at: string;
  /** ISO 8601 timestamp of the most recent successful MCP request. */
  last_verified_at: string;
  /** Free-text label set by the creating flow, e.g. "oauth" or "self-serve". */
  source: string;
}

/** Creation input for `createConnection`. PIT is plaintext; we encrypt on write. */
export interface NewConnectionInput {
  location_id: string;
  pit: string;
  brand_name: string;
  source: string;
}

/** In-memory connection view after KV load + PIT decryption. */
export interface DecryptedConnection {
  connection_id: string;
  location_id: string;
  pit: string;
  brand_name: string;
  created_at: string;
  last_verified_at: string;
  source: string;
}

/** 60-second grace window for eventual consistency. */
const GET_RETRY_DELAY_MS = 250;
const GET_RETRY_ATTEMPTS = 3;

function randomId(): string {
  // UUID v4 via Web Crypto (available in Workers)
  return crypto.randomUUID();
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Create a connection record. PIT is encrypted before writing.
 * Returns the new connection_id.
 */
export async function createConnection(
  kv: KVNamespace,
  input: NewConnectionInput,
  signingSecret: string,
): Promise<string> {
  const enc = await encryptPit(input.pit, signingSecret);
  const now = new Date().toISOString();
  const record: StoredConnection = {
    location_id: input.location_id,
    pit_ct: enc.ct,
    pit_iv: enc.iv,
    brand_name: input.brand_name,
    created_at: now,
    last_verified_at: now,
    source: input.source,
  };
  const id = randomId();
  await kv.put(id, JSON.stringify(record));
  return id;
}

/**
 * Load a connection by id. Returns null if not found.
 * Retries briefly on miss to paper over KV's eventual consistency window
 * for just-created records.
 */
export async function loadConnection(
  kv: KVNamespace,
  connectionId: string,
): Promise<StoredConnection | null> {
  for (let attempt = 0; attempt < GET_RETRY_ATTEMPTS; attempt++) {
    const raw = await kv.get(connectionId, "text");
    if (raw) {
      try {
        return JSON.parse(raw) as StoredConnection;
      } catch {
        return null;
      }
    }
    if (attempt < GET_RETRY_ATTEMPTS - 1) await sleep(GET_RETRY_DELAY_MS);
  }
  return null;
}

/**
 * Load + decrypt in one call. Returns a DecryptedConnection or null if not
 * found / decryption fails (e.g., signing secret rotated).
 */
export async function loadAndDecryptConnection(
  kv: KVNamespace,
  connectionId: string,
  signingSecret: string,
): Promise<DecryptedConnection | null> {
  const stored = await loadConnection(kv, connectionId);
  if (!stored) return null;
  const payload: EncryptedPayload = { ct: stored.pit_ct, iv: stored.pit_iv };
  const pit = await decryptPit(payload, signingSecret);
  if (!pit) return null;
  return {
    connection_id: connectionId,
    location_id: stored.location_id,
    pit,
    brand_name: stored.brand_name,
    created_at: stored.created_at,
    last_verified_at: stored.last_verified_at,
    source: stored.source,
  };
}

/**
 * Update `last_verified_at` to now. Called opportunistically on successful
 * MCP requests. Failure is non-fatal — we don't block a request on a metadata
 * write error.
 */
export async function touchConnection(
  kv: KVNamespace,
  connectionId: string,
): Promise<void> {
  const stored = await loadConnection(kv, connectionId);
  if (!stored) return;
  stored.last_verified_at = new Date().toISOString();
  try {
    await kv.put(connectionId, JSON.stringify(stored));
  } catch {
    // swallow — this is best-effort
  }
}

/**
 * Delete a connection. All tokens referencing it will start returning 401.
 * Used for v2 revocation UI — not wired in v1 but available.
 */
export async function deleteConnection(
  kv: KVNamespace,
  connectionId: string,
): Promise<void> {
  await kv.delete(connectionId);
}
