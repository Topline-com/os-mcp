# Setup — Claude Desktop

## Prerequisites

- Claude Desktop installed: https://claude.ai/download
- Node.js 20+ on your machine: https://nodejs.org (`node --version` should print v20 or newer)
- A Topline sub-account with admin access

## 1. Create a Private Integration Token

1. Open your Topline sub-account in a browser.
2. **Settings → Private Integrations → Create new integration**.
3. Name: `Claude`. Description: `Claude Desktop MCP`.
4. On the scopes screen, click **Select All**.
5. Click **Create**. Copy the token (starts with `pit-`). Store it somewhere safe — you won't see it again.

## 2. Copy your Location ID

**Settings → Business Info**. Scroll to the bottom, copy the **Location ID**.

## 3. Edit `claude_desktop_config.json`

**macOS**:
```
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Windows**:
```
%APPDATA%\Claude\claude_desktop_config.json
```

If the file doesn't exist, create it. Add (or merge with existing `mcpServers`):

```json
{
  "mcpServers": {
    "topline": {
      "command": "npx",
      "args": ["-y", "github:topline-com/os-mcp"],
      "env": {
        "TOPLINE_PIT": "pit-xxxxxxxxxxxxxxxxxxxxxxxx",
        "TOPLINE_LOCATION_ID": "abcDEF1234567"
      }
    }
  }
}
```

Save. **Fully quit Claude Desktop** (not just close the window — `Cmd+Q` on Mac) and reopen.

## 4. Verify

New chat. Ask: *"Run `topline_setup_check`."*

The tool returns a structured report confirming auth, location, and all scope areas. If anything shows `forbidden`, edit your Private Integration, click Select All again, save, and restart Claude.

> **First launch takes 10–30 seconds** while `npx` fetches and builds the package from GitHub. Subsequent launches are cached and fast.

## Common gotchas

- **Config file is invalid JSON** — paste it into https://jsonlint.com to check. Trailing commas break it.
- **`npx: command not found`** — Node isn't installed or not on PATH.
- **Claude restarts but no Topline tools appear** — check Claude Desktop's logs (Help → Open Log) for an error from the `topline` server.
- **"TOPLINE_PIT missing" error** — the env block didn't get picked up. Make sure it's under `"topline"`, not at the top level.
