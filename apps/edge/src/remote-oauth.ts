// HTML rendering for the OAuth 2.1 and /connect flows in the remote worker.
//
// Token primitives (signToken/verifyToken/verifyPkce) and payload types
// (AuthCodePayload / AccessTokenPayload / LegacyAccessTokenPayload) live in
// @topline/shared-auth — they're shared with the sync worker and any future
// service that needs to validate inbound tokens.

import type { ToolSelectionView } from "./tool-selection-view.js";

// --- HTML / responses ---

export function authorizeFormHtml(params: {
  brand: string;
  error?: string;
  redirect_uri: string;
  code_challenge: string;
  code_challenge_method: string;
  state: string;
  client_id: string;
  toolSelection: ToolSelectionView;
}): string {
  const {
    brand,
    error,
    redirect_uri,
    code_challenge,
    code_challenge_method,
    state,
    client_id,
    toolSelection,
  } = params;
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
<p class="sub">Paste your Private Integration Token and Location ID. They are encrypted at rest on this server so Claude can reach your sub-account; Claude itself receives only a signed reference token, never the raw credentials. Revoke at any time by rotating the PIT in ${escapeHtml(brand)}.</p>

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

  ${toolSelectionControlsHtml(toolSelection)}

  <button type="submit">Connect</button>
</form>
</body>
</html>`;
}

function toolSelectionControlsHtml(view: ToolSelectionView): string {
  const options = view.presets
    .map(
      (preset) =>
        `<option value="${escapeHtml(preset.id)}" data-count="${preset.count}" data-tools="${escapeHtml(JSON.stringify(preset.tool_ids))}" ${
          preset.id === view.default_preset ? "selected" : ""
        }>${escapeHtml(preset.label)} — ${preset.count} tools</option>`,
    )
    .join("");
  const customTools = view.custom_tools
    .map(
      (tool) =>
        `<label style="display:block;margin-top:6px;font-weight:400;font-family:monospace"><input type="checkbox" name="toolIds" value="${escapeHtml(tool.id)}" style="width:auto;margin:0 8px 0 0">${escapeHtml(tool.label)}</label>`,
    )
    .join("");

  return `<fieldset style="margin-top:24px;border:1px solid #999;border-radius:8px;padding:14px">
    <legend style="font-weight:600">Tools this connection can use</legend>
    <label for="toolPreset">Tool set</label>
    <select id="toolPreset" name="toolPreset" style="width:100%;box-sizing:border-box;padding:10px 12px;margin-top:6px">
      ${options}
      <option value="custom" data-count="0">Custom selection</option>
    </select>
    <p id="toolPolicyDescription" style="font-size:13px;margin:8px 0 0">The server advertises and accepts only the selected tools. Changing the policy affects the next request; cached hidden calls are still denied.</p>
    <div id="customTools" hidden style="max-height:220px;overflow:auto;border:1px solid #999;padding:8px 12px;margin-top:8px">${customTools}</div>
    <label for="targetClient">Client compatibility</label>
    <select id="targetClient" name="targetClient" style="width:100%;box-sizing:border-box;padding:10px 12px;margin-top:6px">
      <option value="generic">Generic MCP client</option>
      <option value="copilot_studio">Microsoft Copilot Studio</option>
    </select>
    <p style="font-size:12px;color:#555;margin:6px 0 0">The target is saved with this connection. A Copilot Studio all-tools policy fails closed if future catalog growth would exceed 128 tools.</p>
    <p id="toolCount" style="font-size:13px;margin:8px 0 0"></p>
    <p id="toolWarning" role="alert" style="font-size:13px;margin:8px 0 0;color:#8a5600;font-weight:600"></p>
    <details style="margin-top:8px"><summary>Review selected tool IDs</summary><pre id="selectedTools" style="white-space:pre-wrap;font-size:11px;max-height:180px;overflow:auto"></pre></details>
  </fieldset>
  <script>
  (() => {
    const form = document.currentScript.closest("form");
    const preset = form.querySelector("#toolPreset");
    const target = form.querySelector("#targetClient");
    const custom = form.querySelector("#customTools");
    const countEl = form.querySelector("#toolCount");
    const warning = form.querySelector("#toolWarning");
    const selectedTools = form.querySelector("#selectedTools");
    const submit = form.querySelector('button[type="submit"]');
    const update = () => {
      const isCustom = preset.value === "custom";
      custom.hidden = !isCustom;
      const count = isCustom
        ? custom.querySelectorAll('input[type="checkbox"]:checked').length
        : Number(preset.selectedOptions[0].dataset.count || 0);
      const selectedIds = isCustom
        ? Array.from(custom.querySelectorAll('input[type="checkbox"]:checked')).map((input) => input.value)
        : JSON.parse(preset.selectedOptions[0].dataset.tools || "[]");
      countEl.textContent = count + " tools selected.";
      selectedTools.textContent = selectedIds.join("\n") || "No tools selected.";
      if (target.value === "copilot_studio" && count > 128) {
        warning.textContent = "Copilot Studio supports at most 128 tools. Reduce this selection before connecting.";
        submit.disabled = true;
      } else {
        warning.textContent = count > 30
          ? "Microsoft recommends 25–30 tools for Copilot Studio performance and selection quality."
          : "";
        submit.disabled = false;
      }
    };
    preset.addEventListener("change", update);
    target.addEventListener("change", update);
    custom.addEventListener("change", update);
    update();
  })();
  </script>`;
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

export function connectFormHtml(params: {
  brand: string;
  origin: string;
  error?: string;
  toolSelection: ToolSelectionView;
}): string {
  const { brand, origin, error, toolSelection } = params;
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

  ${toolSelectionControlsHtml(toolSelection)}

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
<p class="sub">Copy this single token and paste it into your MCP client. The token is a signed reference to your encrypted PIT + Location ID stored on this server — the plaintext credentials never travel back to clients. Valid for 1 year.</p>

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
