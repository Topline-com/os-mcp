// OAuth 2.1 primitives for the remote (Cloudflare Worker) MCP server.
//
// Design: no server-side storage. Everything the server needs to know about
// a user's session — their Topline PIT and Location ID — is encoded directly
// into signed tokens. Authorization codes and access tokens are HMAC-signed
// with a worker-level secret (TOKEN_SIGNING_SECRET) so they cannot be forged.
//
// Flow: Claude → /register (DCR) → /authorize (HTML form collects PIT+LocId)
//   → authorization code → /token (PKCE verify) → access token → /mcp

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
  const parts = token.split(".");
  if (parts.length !== 2) return null;
  const [payloadB64, sigB64] = parts;
  const key = await hmacKey(secret);
  const sig = b64urlDecode(sigB64);
  const ok = await crypto.subtle.verify("HMAC", key, sig, encoder.encode(payloadB64));
  if (!ok) return null;
  try {
    const payload = JSON.parse(decoder.decode(b64urlDecode(payloadB64))) as T & { exp?: number };
    if (payload.exp && Date.now() / 1000 > payload.exp) return null;
    return payload;
  } catch {
    return null;
  }
}

export interface AuthCodePayload {
  pit: string;
  locationId: string;
  redirect_uri: string;
  code_challenge: string;
  code_challenge_method: "S256" | "plain";
  exp: number;
}

export interface AccessTokenPayload {
  pit: string;
  locationId: string;
  exp: number;
}

// PKCE verification (RFC 7636)
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

// --- HTML / responses ---

export function authorizeFormHtml(params: {
  brand: string;
  error?: string;
  redirect_uri: string;
  code_challenge: string;
  code_challenge_method: string;
  state: string;
  client_id: string;
}): string {
  const { brand, error, redirect_uri, code_challenge, code_challenge_method, state, client_id } =
    params;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Connect ${escapeHtml(brand)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 480px; margin: 48px auto; padding: 0 20px; line-height: 1.5; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  p.sub { color: #666; margin-top: 0; font-size: 14px; }
  label { display: block; margin-top: 20px; font-weight: 600; font-size: 14px; }
  input { width: 100%; box-sizing: border-box; padding: 10px 12px; font-size: 14px; border: 1px solid #999; border-radius: 6px; margin-top: 6px; font-family: monospace; }
  button { margin-top: 24px; background: #000; color: #fff; border: 0; padding: 12px 20px; border-radius: 6px; font-size: 15px; cursor: pointer; width: 100%; }
  button:hover { background: #222; }
  .err { color: #b00; background: #fee; padding: 10px 14px; border-radius: 6px; margin-top: 16px; font-size: 14px; }
  .steps { background: #f6f6f6; padding: 16px; border-radius: 8px; margin-top: 20px; font-size: 13px; color: #444; }
  .steps ol { margin: 4px 0 0 0; padding-left: 20px; }
  @media (prefers-color-scheme: dark) {
    body { background: #111; color: #eee; }
    p.sub { color: #999; }
    input { background: #1c1c1c; color: #eee; border-color: #444; }
    .err { background: #3a0000; color: #ffaaaa; }
    .steps { background: #1c1c1c; color: #bbb; }
  }
</style>
</head>
<body>
<h1>Connect ${escapeHtml(brand)} to Claude</h1>
<p class="sub">Paste your Private Integration Token and Location ID. Both are stored only as a signed token in Claude — nothing is stored on this server.</p>

${error ? `<div class="err">${escapeHtml(error)}</div>` : ""}

<div class="steps">
  <strong>If you don't have these yet:</strong>
  <ol>
    <li>In ${escapeHtml(brand)} go to <b>Settings → Private Integrations</b> and create a new integration. Click <b>Select All</b> on the scopes screen. Copy the <code>pit-…</code> token.</li>
    <li>Go to <b>Settings → Business Info</b> and copy the <b>Location ID</b>.</li>
  </ol>
</div>

<form method="POST" action="/authorize">
  <input type="hidden" name="redirect_uri" value="${escapeHtml(redirect_uri)}">
  <input type="hidden" name="code_challenge" value="${escapeHtml(code_challenge)}">
  <input type="hidden" name="code_challenge_method" value="${escapeHtml(code_challenge_method)}">
  <input type="hidden" name="state" value="${escapeHtml(state)}">
  <input type="hidden" name="client_id" value="${escapeHtml(client_id)}">

  <label for="pit">Private Integration Token</label>
  <input id="pit" name="pit" placeholder="pit-xxxxxxxxxxxxxxxx" autocomplete="off" spellcheck="false" required>

  <label for="locationId">Location ID</label>
  <input id="locationId" name="locationId" placeholder="abcDEF1234567" autocomplete="off" spellcheck="false" required>

  <button type="submit">Connect</button>
</form>
</body>
</html>`;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
