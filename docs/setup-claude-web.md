# Setup — Claude web / Team / Enterprise (custom connector)

Use this path if you're connecting Claude on **claude.ai** (web or via Team/Enterprise workspaces). Desktop and Code users see [setup-claude-desktop.md](./setup-claude-desktop.md) / [setup-claude-code.md](./setup-claude-code.md).

Prerequisites:
- A Topline OS sub-account with admin access.
- The hosted MCP server URL: `https://os-mcp.topline.com/mcp` (already deployed and ready to use).

## 1. Create a Private Integration

In Topline OS → **Settings → Private Integrations → Create new integration**. Name it `Claude`. On the scopes screen click **Select All**. Click Create. Copy the `pit-…` token. Keep the tab open until setup is done.

## 2. Copy your Location ID

Settings → **Business Info** → copy the **Location ID**.

## 3. Add the custom connector in Claude

1. In Claude → **Settings → Connectors → Add custom connector**.
2. Name: `Topline OS`.
3. Remote MCP server URL: the Worker URL your admin gave you, e.g. `https://os-mcp.topline.com/mcp`.
4. Leave **Advanced settings** alone unless you have a reason to change it.
5. Click **Add**.

## 4. Connect

Claude will show a **Connect** button. Click it. A popup opens.

The popup is the MCP server's authorization page. It has two fields:
- **Private Integration Token** — paste the `pit-…` from Step 1.
- **Location ID** — paste the ID from Step 2.

Click **Connect**. The popup closes automatically.

## 5. Verify

In a new Claude conversation, send:

```
Run topline_setup_check
```

You'll get back a structured report. If `summary` says all scope areas OK, you're live. If any scope is `forbidden`, go back to **Settings → Private Integrations**, edit the integration, click **Select All** again, save, and reconnect the custom connector (the token may need to be regenerated and re-pasted).

## Troubleshooting

| Symptom | Fix |
|---|---|
| "Access token invalid or expired" | Reconnect the connector (Settings → Connectors → Topline OS → Reconnect). Tokens last 30 days. |
| Setup check returns `forbidden` on some scope | Edit the Private Integration, click Select All, save, regenerate token, reconnect. |
| "Private Integration Token should start with 'pit-'" in the popup | You copied the integration name instead of the token. Re-copy from the token field. |
| Connector shows but tools don't appear | Click Refresh on the connector. If still empty, remove and re-add. |

## Security

- The PIT and Location ID are encoded into a Claude-issued access token, HMAC-signed by the MCP Worker, and sent on each request.
- Nothing is stored server-side. Rotating the Worker's signing secret invalidates all sessions immediately.
- Revoke the PIT at any time from **Settings → Private Integrations** in Topline OS.
