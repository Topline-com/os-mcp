# Setup — ChatGPT (Apps)

Connect Topline OS to ChatGPT as a custom MCP App. ~2 minutes.

## 1. Create a Private Integration in Topline OS

- **Settings → Private Integrations → Create new integration**
- Name: `ChatGPT` (or whatever you want)
- On the scopes screen click **Select All**
- Click Create. Copy the `pit-…` token.

## 2. Copy your Location ID

- **Settings → Business Info** → copy the **Location ID**.

## 3. Create the App in ChatGPT

**ChatGPT → Apps → New App**. Fill in:

| Field | Value |
|---|---|
| **Icon** | optional |
| **Name** | `Topline OS` |
| **Description** | `Connect to Topline OS` |
| **MCP Server URL** | `https://os-mcp.topline.com/mcp` |
| **Authentication** | **`OAuth`** |
| **Client ID** | *leave blank* |
| **Client Secret** | *leave blank* |

> **Why OAuth?** Our server implements Dynamic Client Registration (RFC 7591), so ChatGPT registers itself. You don't create anything in a developer console. Selecting "Access token / API key" instead works but forces an extra step — see [setup-mcp-clients.md](./setup-mcp-clients.md).

Check the "I understand and want to continue" risk acknowledgment, then click **Create**.

## 4. Connect

ChatGPT shows a **Connect** button on the new app. Click it. A popup opens — this is our OAuth authorize page on `os-mcp.topline.com`.

The popup has two fields:
- **Private Integration Token** — paste the `pit-…` from Step 1
- **Location ID** — paste the ID from Step 2

Click **Connect**. Popup closes. ChatGPT stores the access token.

## 5. Verify

Start a new ChatGPT conversation with the `Topline OS` app enabled. Send:

```
Run topline_setup_check
```

You should get a structured report with all 10 scope areas listed as `ok`. If anything shows `forbidden`, go back to **Settings → Private Integrations** in Topline OS, edit the integration, click **Select All** again, save, then disconnect and reconnect the app in ChatGPT.

## Troubleshooting

| Symptom | Fix |
|---|---|
| OAuth popup shows an error | Check that the URL field exactly matches `https://os-mcp.topline.com/mcp` (no trailing slash, no `/oauth/authorize` appended). |
| "Access token invalid or expired" after a month | OAuth tokens last 30 days. Disconnect and reconnect the app — it'll re-issue. |
| `topline_setup_check` says `forbidden` on some scope | You missed Select All. Edit the integration in Topline OS, click Select All, regenerate the token, reconnect the ChatGPT app. |
| App shows but tools are empty | Click the refresh icon on the app, or disconnect and reconnect. |
| OAuth flow fails entirely | Fall back to API key mode — see [setup-mcp-clients.md](./setup-mcp-clients.md). Paste the token into ChatGPT using "Access token / API key" + "Bearer" scheme. |

## Security

- Your PIT and Location ID are encoded into an HMAC-signed access token stored by ChatGPT. The token is what ChatGPT sends with every request; the raw PIT never leaves the popup form.
- Nothing is stored on the MCP server. Rotating the worker's signing secret invalidates every issued token instantly.
- Revoke the PIT any time from Topline OS → Settings → Private Integrations. Any token ever issued against that PIT stops working on the next call.
