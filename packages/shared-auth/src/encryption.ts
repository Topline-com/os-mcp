// AES-GCM encryption for Private Integration Tokens at rest.
// Key derived from the worker's TOKEN_SIGNING_SECRET via HKDF so no new
// secret provisioning is required — one secret covers token signing AND
// PIT encryption. Rotating TOKEN_SIGNING_SECRET invalidates both.
//
// Wire format (everything base64url-encoded in the ConnectionDirectory):
//   pit_iv:          12 random bytes per encryption
//   pit_encrypted:   AES-GCM(plaintext) — includes 16-byte auth tag at end

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// Fixed salt and info parameters so derivations are reproducible.
// Bumping "v1" rotates the derived key for a keyring migration (not needed
// yet, but makes future key rotation straightforward).
const HKDF_SALT = encoder.encode("topline-os-pit-kek-v1");
const HKDF_INFO = encoder.encode("aes-gcm-256");

async function deriveKey(secret: string): Promise<CryptoKey> {
  const seed = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    "HKDF",
    false,
    ["deriveKey"],
  );
  return crypto.subtle.deriveKey(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: HKDF_SALT,
      info: HKDF_INFO,
    },
    seed,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"],
  );
}

function b64urlEncode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function b64urlDecode(str: string): Uint8Array {
  const pad = str.length % 4 === 0 ? "" : "=".repeat(4 - (str.length % 4));
  const s = atob(str.replace(/-/g, "+").replace(/_/g, "/") + pad);
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) bytes[i] = s.charCodeAt(i);
  return bytes;
}

export interface EncryptedPayload {
  /** Ciphertext, base64url. Includes AES-GCM auth tag. */
  ct: string;
  /** 12-byte IV, base64url. */
  iv: string;
}

/** Encrypt a plaintext PIT. Returns ciphertext + random IV. */
export async function encryptPit(plaintext: string, secret: string): Promise<EncryptedPayload> {
  const key = await deriveKey(secret);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = new Uint8Array(
    await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, encoder.encode(plaintext)),
  );
  return { ct: b64urlEncode(ct), iv: b64urlEncode(iv) };
}

/** Decrypt a ciphertext + IV pair. Returns null if the auth tag fails. */
export async function decryptPit(payload: EncryptedPayload, secret: string): Promise<string | null> {
  const key = await deriveKey(secret);
  try {
    const pt = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv: b64urlDecode(payload.iv) },
      key,
      b64urlDecode(payload.ct),
    );
    return decoder.decode(pt);
  } catch {
    return null;
  }
}
