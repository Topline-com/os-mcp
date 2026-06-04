# Setup — ChatGPT Codex (CLI & IDE extension)

Codex is OpenAI's coding agent. It runs MCP servers locally over stdio, configured in `~/.codex/config.toml` — so the Topline OS MCP installs the same way it does for Claude Code: `npx` pulls the package from GitHub and Codex runs it as a subprocess.

> **Not the same as the ChatGPT app.** If you want Topline OS inside chatgpt.com (the consumer app, via the **Apps** connector / OAuth), use [setup-chatgpt.md](./setup-chatgpt.md) instead — that path talks to the hosted worker and needs no local install. This guide is for the **Codex CLI** and the **Codex IDE extension** (VS Code / JetBrains), which share the same `config.toml`.

## Prerequisites

- Codex installed: https://developers.openai.com/codex (CLI via `npm i -g @openai/codex`, or the IDE extension)
- Node.js 20+ on your machine: https://nodejs.org (`node --version` should print v20 or newer — Codex shells out to `npx` to launch the server)
- A Topline sub-account with admin access

## 1. Create a Private Integration Token

1. Open your Topline sub-account in a browser.
2. **Settings → Private Integrations → Create new integration**.
3. Name: `Codex`. Description: `ChatGPT Codex MCP`.
4. On the scopes screen, click **Select All**.
5. Click **Create**. Copy the token (starts with `pit-`). Store it somewhere safe — you won't see it again.

## 2. Copy your Location ID

**Settings → Business Info**. Scroll to the bottom, copy the **Location ID**.

## 3. Register the MCP with Codex

Two ways — pick one. Editing `config.toml` directly (A) is recommended because it lets you set the startup timeout, which matters for the first launch (see the note below).

### A. Edit `config.toml` (recommended)

Open `~/.codex/config.toml` (create it if it doesn't exist) and add:

```toml
[mcp_servers.topline]
command = "npx"
args = ["-y", "github:topline-com/os-mcp"]
startup_timeout_sec = 30

[mcp_servers.topline.env]
TOPLINE_PIT = "pit-xxxxxxxxxxxxxxxxxxxxxxxx"
TOPLINE_LOCATION_ID = "abcDEF1234567"
```

> **Order matters in TOML.** `command`, `args`, and `startup_timeout_sec` must stay **above** the `[mcp_servers.topline.env]` line. Everything after that line belongs to the `env` table until the next `[...]` header — so if you move `command` below it, Codex reads it as an env var and the server won't start.

> **Why `startup_timeout_sec = 30`?** Codex's default MCP startup timeout is **10 seconds**, but the first launch takes 10–30 seconds while `npx` fetches and builds the package from GitHub. Without the bump, the first launch times out. Subsequent launches are cached and fast.

The Codex IDE extension reads the same file — open it from the gear menu → **MCP settings → Open config.toml**.

### B. `codex mcp add` (CLI)

```bash
codex mcp add topline \
  --env TOPLINE_PIT=pit-xxxxxxxxxxxxxxxxxxxxxxxx \
  --env TOPLINE_LOCATION_ID=abcDEF1234567 \
  -- npx -y github:topline-com/os-mcp
```

This writes the same `[mcp_servers.topline]` entry into `~/.codex/config.toml`. The `add` command doesn't expose the startup timeout, so if the first launch times out (see the note above), open `~/.codex/config.toml` and add `startup_timeout_sec = 30` under the `[mcp_servers.topline]` table.

## 4. Verify

Start a new Codex session. In the TUI, run:

```
/mcp
```

`topline` should appear as a connected server with its tools listed. Then ask Codex:

> *"Run `topline_setup_check`."*

The tool returns a structured report confirming auth, location, and all scope areas. All-green means you're live. If anything shows `forbidden`, edit your Private Integration in Topline OS, click **Select All** again, save, regenerate the token if prompted, update the token in `config.toml`, and restart Codex.

## Common gotchas

- **`/mcp` shows nothing or `topline` is missing** — the `[mcp_servers.topline]` table has a typo, or you edited a `config.toml` other than `~/.codex/config.toml`. Confirm the path and that the table header is exactly `[mcp_servers.topline]`.
- **Server fails to start on first launch / "request timed out"** — `startup_timeout_sec` is too low (or absent) while `npx` builds the package. Set `startup_timeout_sec = 30` and retry.
- **`TOPLINE_PIT is missing` at startup** — the `[mcp_servers.topline.env]` table is missing, or `command`/`args` were placed below it (see the TOML ordering note in Step 3A).
- **`npx: command not found`** — Node isn't installed or isn't on Codex's PATH. Install Node 20+ from https://nodejs.org.
- **Edited `config.toml` but nothing changed** — Codex reads the config at session start. Quit and start a new session.

## Removing

Delete the `[mcp_servers.topline]` and `[mcp_servers.topline.env]` tables from `~/.codex/config.toml` (or run `codex mcp remove topline`), then restart Codex.

## Updating the PIT or Location ID

Edit the values under `[mcp_servers.topline.env]` in `~/.codex/config.toml` and restart Codex. (With the CLI: `codex mcp remove topline`, then re-run the `codex mcp add` command from Step 3B with the new values.)

## Security

- The PIT lives only in your local `~/.codex/config.toml`. It is never sent to any hosted intermediary — Codex runs the MCP as a local subprocess that talks to Topline directly.
- Revoke the token any time from Topline OS → **Settings → Private Integrations**. Rotating the PIT there invalidates it on the next request.
