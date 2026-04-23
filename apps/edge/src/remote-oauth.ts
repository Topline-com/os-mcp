// HTML rendering for the OAuth 2.1 and /connect flows in the remote worker.
//
// Token primitives (signToken/verifyToken/verifyPkce) and payload types
// (AuthCodePayload / AccessTokenPayload / LegacyAccessTokenPayload) live in
// @topline/shared-auth — they're shared with the sync worker and any future
// service that needs to validate inbound tokens.

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

// ---------------------------------------------------------------------------
// Self-serve token generator — for MCP clients (ChatGPT Apps, curl, any
// Bearer-only client) that can't complete the OAuth dance. The user pastes
// their PIT + Location ID, gets back a single long-lived signed access token.
// ---------------------------------------------------------------------------

export function connectFormHtml(params: { brand: string; origin: string; error?: string }): string {
  const { brand, origin, error } = params;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Generate ${escapeHtml(brand)} access token</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 560px; margin: 48px auto; padding: 0 20px; line-height: 1.5; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  p.sub { color: #666; margin-top: 0; font-size: 14px; }
  label { display: block; margin-top: 20px; font-weight: 600; font-size: 14px; }
  input { width: 100%; box-sizing: border-box; padding: 10px 12px; font-size: 14px; border: 1px solid #999; border-radius: 6px; margin-top: 6px; font-family: monospace; }
  button { margin-top: 24px; background: #000; color: #fff; border: 0; padding: 12px 20px; border-radius: 6px; font-size: 15px; cursor: pointer; width: 100%; }
  button:hover { background: #222; }
  .err { color: #b00; background: #fee; padding: 10px 14px; border-radius: 6px; margin-top: 16px; font-size: 14px; }
  .steps { background: #f6f6f6; padding: 16px; border-radius: 8px; margin-top: 20px; font-size: 13px; color: #444; }
  .steps ol { margin: 4px 0 0 0; padding-left: 20px; }
  code { background: rgba(0,0,0,0.07); padding: 2px 6px; border-radius: 4px; font-size: 13px; }
  @media (prefers-color-scheme: dark) {
    body { background: #111; color: #eee; }
    p.sub { color: #999; }
    input { background: #1c1c1c; color: #eee; border-color: #444; }
    .err { background: #3a0000; color: #ffaaaa; }
    .steps { background: #1c1c1c; color: #bbb; }
    code { background: rgba(255,255,255,0.1); }
  }
</style>
</head>
<body>
<h1>Generate an access token</h1>
<p class="sub">For ChatGPT Apps, curl, or any MCP client that needs a single Bearer token. Paste your PIT and Location ID — we'll sign a token you can copy into your MCP client.</p>

${error ? `<div class="err">${escapeHtml(error)}</div>` : ""}

<div class="steps">
  <strong>If you don't have these yet:</strong>
  <ol>
    <li>In ${escapeHtml(brand)} go to <b>Settings → Private Integrations</b> and create a new integration. Click <b>Select All</b> on the scopes screen. Copy the <code>pit-…</code> token.</li>
    <li>Go to <b>Settings → Business Info</b> and copy the <b>Location ID</b>.</li>
  </ol>
</div>

<form method="POST" action="/connect">
  <label for="pit">Private Integration Token</label>
  <input id="pit" name="pit" placeholder="pit-xxxxxxxxxxxxxxxx" autocomplete="off" spellcheck="false" required>

  <label for="locationId">Location ID</label>
  <input id="locationId" name="locationId" placeholder="abcDEF1234567" autocomplete="off" spellcheck="false" required>

  <button type="submit">Generate token</button>
</form>

<p class="sub" style="margin-top: 32px; font-size: 12px;">MCP Server URL: <code>${escapeHtml(origin)}/mcp</code></p>
</body>
</html>`;
}

export function connectResultHtml(params: { brand: string; origin: string; token: string }): string {
  const { brand, origin, token } = params;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${escapeHtml(brand)} access token</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 640px; margin: 48px auto; padding: 0 20px; line-height: 1.5; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  p.sub { color: #666; margin-top: 0; font-size: 14px; }
  .token-box { position: relative; margin-top: 20px; }
  .token { width: 100%; box-sizing: border-box; padding: 14px 52px 14px 14px; font-size: 12px; border: 1px solid #999; border-radius: 8px; font-family: monospace; word-break: break-all; background: #f6f6f6; color: #000; }
  .copy-btn { position: absolute; top: 10px; right: 10px; background: #000; color: #fff; border: 0; padding: 6px 10px; border-radius: 4px; font-size: 12px; cursor: pointer; }
  .copy-btn:hover { background: #333; }
  .copy-btn.copied { background: #2d6; }
  table { margin-top: 24px; width: 100%; border-collapse: collapse; font-size: 14px; }
  th, td { text-align: left; padding: 8px 0; border-bottom: 1px solid #ddd; vertical-align: top; }
  th { font-weight: 600; width: 160px; white-space: nowrap; padding-right: 16px; }
  code { background: rgba(0,0,0,0.07); padding: 2px 6px; border-radius: 4px; font-size: 13px; font-family: monospace; }
  .warn { background: #fff8e0; border: 1px solid #e6c200; padding: 12px 14px; border-radius: 8px; margin-top: 24px; font-size: 13px; color: #6a4a00; }
  @media (prefers-color-scheme: dark) {
    body { background: #111; color: #eee; }
    p.sub { color: #999; }
    .token { background: #1c1c1c; color: #eee; border-color: #444; }
    th, td { border-color: #333; }
    code { background: rgba(255,255,255,0.1); }
    .warn { background: #2a2000; color: #ffd97a; border-color: #806600; }
  }
</style>
</head>
<body>
<h1>Your ${escapeHtml(brand)} access token</h1>
<p class="sub">Copy this single token and paste it into your MCP client. It contains your signed PIT + Location ID. Valid for 1 year.</p>

<div class="token-box">
  <textarea class="token" id="token" rows="5" readonly>${escapeHtml(token)}</textarea>
  <button class="copy-btn" id="copy">Copy</button>
</div>

<h2 style="font-size: 16px; margin-top: 32px;">For ChatGPT (Apps → New App)</h2>
<table>
  <tr><th>Name</th><td>${escapeHtml(brand)}</td></tr>
  <tr><th>MCP Server URL</th><td><code>${escapeHtml(origin)}/mcp</code></td></tr>
  <tr><th>Authentication</th><td>Access token / API key</td></tr>
  <tr><th>Header scheme</th><td>Bearer</td></tr>
  <tr><th>Token</th><td>(the token above)</td></tr>
</table>

<h2 style="font-size: 16px; margin-top: 32px;">For curl / other clients</h2>
<pre style="background: rgba(0,0,0,0.05); padding: 12px; border-radius: 6px; overflow-x: auto; font-size: 12px;">curl -X POST ${escapeHtml(origin)}/mcp \\
  -H "Authorization: Bearer &lt;token&gt;" \\
  -H "Content-Type: application/json" \\
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'</pre>

<div class="warn">
  <strong>Keep this token private.</strong> Anyone with it can access your ${escapeHtml(brand)} sub-account with all scopes until it expires. Revoke by rotating the Private Integration Token in ${escapeHtml(brand)} — all tokens issued for the old PIT stop working immediately.
</div>

<script>
document.getElementById("copy").addEventListener("click", async () => {
  const btn = document.getElementById("copy");
  const ta = document.getElementById("token");
  try {
    await navigator.clipboard.writeText(ta.value);
    btn.textContent = "Copied";
    btn.classList.add("copied");
    setTimeout(() => { btn.textContent = "Copy"; btn.classList.remove("copied"); }, 1500);
  } catch { ta.select(); document.execCommand("copy"); btn.textContent = "Copied"; }
});
</script>
</body>
</html>`;
}
