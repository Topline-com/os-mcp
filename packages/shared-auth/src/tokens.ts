// HMAC-signed tokens (JWT-like but compact).
// Payload format: base64url(JSON).base64url(HMAC-SHA256).
//
// Three token shapes exist in the wild:
//
//   Legacy access token (pre-connection-layer):
//     { pit, locationId, exp }
//     Still honored on inbound MCP requests so existing OAuth / /connect
//     users don't break on deploy.
//
//   New access token (post-connection-layer):
//     { cid, exp }
//     `cid` is a ConnectionDirectory KV key.
//
//   OAuth authorization code (10-min TTL):
//     { pit, locationId, redirect_uri, code_challenge, code_challenge_method, exp }
//     Still contains PIT because the code is single-use and short-lived —
//     moving this to a `cid`-based flow adds no security and costs a KV
//     write on an abandoned auth.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

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

async function hmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

export async function signToken(payload: object, secret: string): Promise<string> {
  const key = await hmacKey(secret);
  const payloadB64 = b64urlEncode(encoder.encode(JSON.stringify(payload)));
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(payloadB64)));
  return `${payloadB64}.${b64urlEncode(sig)}`;
}

export async function verifyToken<T = unknown>(token: string, secret: string): Promise<T | null> {
  // ALL parse + crypto operations are wrapped: any attacker-controlled input
  // must fail closed as `null`, never throw and escape to the caller as a
  // Worker exception. Bad inputs to watch:
  //   - b64urlDecode can throw via atob("…%%…") on non-base64 chars
  //   - crypto.subtle.verify can throw on malformed signature length
  //   - importKey on an empty/undefined secret can throw
  //   - JSON.parse on non-JSON payload can throw
  try {
    const parts = token.split(".");
    if (parts.length !== 2) return null;
    const [payloadB64, sigB64] = parts;
    if (!payloadB64 || !sigB64) return null;
    const key = await hmacKey(secret);
    const sig = b64urlDecode(sigB64);
    const ok = await crypto.subtle.verify("HMAC", key, sig, encoder.encode(payloadB64));
    if (!ok) return null;
    const payload = JSON.parse(decoder.decode(b64urlDecode(payloadB64))) as T & { exp?: number };
    if (payload.exp && Date.now() / 1000 > payload.exp) return null;
    return payload;
  } catch {
    return null;
  }
}

// PKCE verification (RFC 7636) — lives here because it's used alongside tokens
// in the OAuth token endpoint.
export async function verifyPkce(
  codeVerifier: string,
  codeChallenge: string,
  method: "S256" | "plain",
): Promise<boolean> {
  if (method === "plain") return codeVerifier === codeChallenge;
  const hash = new Uint8Array(
    await crypto.subtle.digest("SHA-256", encoder.encode(codeVerifier)),
  );
  return b64urlEncode(hash) === codeChallenge;
}

// ---- Payload types ----

/** Legacy access token payload (pre-connection-layer). Still accepted inbound. */
export interface LegacyAccessTokenPayload {
  pit: string;
  locationId: string;
  exp: number;
}

/** New access token payload — references a ConnectionDirectory entry. */
export interface AccessTokenPayload {
  cid: string;
  exp: number;
}

/** OAuth authorization code payload (short-lived, exchanged at /token). */
export interface AuthCodePayload {
  pit: string;
  locationId: string;
  redirect_uri: string;
  code_challenge: string;
  code_challenge_method: "S256" | "plain";
  exp: number;
}

/** Type guard: is this the legacy shape that embeds the PIT? */
export function isLegacyAccess(
  p: unknown,
): p is LegacyAccessTokenPayload {
  return typeof p === "object" && p !== null &&
    typeof (p as { pit?: unknown }).pit === "string" &&
    typeof (p as { locationId?: unknown }).locationId === "string";
}

/** Type guard: is this the new cid-referencing shape? */
export function isCidAccess(p: unknown): p is AccessTokenPayload {
  return typeof p === "object" && p !== null &&
    typeof (p as { cid?: unknown }).cid === "string";
}
