# Setup — Claude Code (CLI)

## Prerequisites

- Claude Code installed: https://claude.com/claude-code
- Node.js 20+ (`node --version`)
- A Topline sub-account with admin access

## 1. Create a Private Integration Token

In your Topline sub-account: **Settings → Private Integrations → Create new integration**. Name it `Claude`, click **Select All** for scopes, click Create, copy the `pit-...` token.

## 2. Copy your Location ID

**Settings → Business Info → Location ID** (at the bottom).

## 3. Register the MCP with Claude Code

```bash
claude mcp add topline \
  -e TOPLINE_PIT=pit-xxxxxxxxxxxxxxxxxxxxxxxx \
  -e TOPLINE_LOCATION_ID=abcDEF1234567 \
  -- npx -y github:topline-com/os-mcp
```

Scope it to a single project instead with `--scope project`, or to the current user with `--scope user` (default).

## 4. Verify

```bash
claude mcp list
```

You should see `topline` listed. Start Claude Code (`claude`) and ask: *"Run `topline_setup_check`."*

The tool returns a pass/fail report for auth, location, and every scope area. Green across the board means you're live.

> **First launch takes 10–30 seconds** while `npx` fetches and builds the package from GitHub. Subsequent launches are cached.

## Removing

```bash
claude mcp remove topline
```

## Updating the PIT or Location ID

Remove and re-add:

```bash
claude mcp remove topline
claude mcp add topline \
  -e TOPLINE_PIT=pit-new-token \
  -e TOPLINE_LOCATION_ID=abcDEF1234567 \
  -- npx -y github:topline-com/os-mcp
```
