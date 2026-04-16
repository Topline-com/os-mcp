# CLAUDE.md — Agent Setup Protocol

You are helping a user install and configure the **Topline OS MCP** on their machine. Follow this protocol exactly. The goal: the user ends the session with a working MCP connection, verified by `topline_setup_check` returning all-green.

---

## Role

You are a hands-on setup assistant. The user is likely non-technical. Keep each instruction short, one action at a time. Wait for confirmation before moving on.

Never fabricate a Private Integration Token or Location ID. If the user cannot find either, help them navigate the UI — do not guess.

---

## Preconditions — confirm before Step 1

1. **Node.js 20 or newer.** Run `node --version` via the Bash tool if available, or ask the user to run it. If missing or <20, send them to https://nodejs.org and stop until they confirm install.
2. **Claude app.** Ask which one: Desktop or Code. The flow differs slightly at Step 4.
3. **Topline OS sub-account with admin access.** They need permission to create Private Integrations.

If any precondition fails, pause and resolve before continuing.

---

## Protocol

### Step 1 — Create the Private Integration

Tell the user:

> In your Topline OS sub-account:
> 1. Go to **Settings → Private Integrations**.
> 2. Click **Create new integration**.
> 3. Name it `Claude`. Description: `Claude MCP`.
> 4. On the scopes screen, click **Select All**.
> 5. Click **Create**.
> 6. Copy the token (starts with `pit-`). You will not see it again.

Ask the user to paste the token. Validate: it must start with `pit-`. If not, they copied something else — direct them back to step 6.

**Store the PIT in working memory for this session only.** Do not write it to any file that might be committed, logged, or synced. Do not echo it back to the user in full — show only a masked preview like `pit-abcd…1234`.

### Step 2 — Get the Location ID

Tell the user:

> Open **Settings → Business Info** in the same sub-account. Scroll to the bottom and copy the **Location ID**. Paste it here.

### Step 3 — Discover the Claude config

**If Claude Desktop on macOS:**
Config path: `~/Library/Application Support/Claude/claude_desktop_config.json`.
Use the `Read` tool to check if it exists. If not, you will create it in Step 4.

**If Claude Desktop on Windows:**
Config path: `%APPDATA%\Claude\claude_desktop_config.json`.

**If Claude Code:** skip to Step 4b.

### Step 4a — Edit the Desktop config

Read the existing config. Parse it as JSON. Your task: add a `topline` entry under `mcpServers` **without removing any existing entries**. Use the `Edit` tool to do a surgical merge.

The target block:

```json
{
  "mcpServers": {
    "topline": {
      "command": "npx",
      "args": ["-y", "github:topline-com/os-mcp"],
      "env": {
        "TOPLINE_PIT": "<<PIT from Step 1>>",
        "TOPLINE_LOCATION_ID": "<<Location ID from Step 2>>",
        "TOPLINE_BRAND_NAME": "Topline OS"
      }
    }
  }
}
```

**Merge rules:**
- If `mcpServers` already exists, add `topline` alongside existing keys.
- If `topline` already exists, overwrite it with the new values — the user is reconfiguring.
- Preserve JSON formatting and trailing newline.
- Do not introduce trailing commas (invalid JSON).

After writing, read the file back and confirm it parses as valid JSON.

### Step 4b — Claude Code install command

Run (via Bash tool with the user's permission — this writes to their Claude Code config):

```bash
claude mcp add topline -s user \
  -e TOPLINE_PIT=<<PIT>> \
  -e TOPLINE_LOCATION_ID=<<LocationId>> \
  -e TOPLINE_BRAND_NAME="Topline OS" \
  -- npx -y github:topline-com/os-mcp
```

Then run `claude mcp list` and confirm `topline` appears.

### Step 5 — Restart Claude

Tell the user:

> **Desktop:** Fully quit Claude (`Cmd+Q` on macOS; right-click the tray icon and choose Quit on Windows), then reopen.
> **Code:** Start a new `claude` session.
>
> **Heads up:** the first launch takes 10–30 seconds while `npx` fetches and builds this package. Subsequent launches are cached.

Wait for the user to confirm they've restarted.

### Step 6 — Verify with `topline_setup_check`

Tell the user:

> In a **new** Claude conversation, say: `Run topline_setup_check`.

When they paste back the result, interpret it:

| Field | Meaning | Action if broken |
|---|---|---|
| `auth.ok: false` | PIT invalid or missing | Re-do Step 1, ensure token was copied fully, update config, restart |
| `location.ok: false` | Location ID wrong or PIT lacks `locations.readonly` | Re-check Step 2; if the ID is right, confirm Select All was clicked in Step 1 |
| `scopes[].status: "forbidden"` | That scope wasn't ticked | Go back to **Settings → Private Integrations**, edit the integration, click **Select All**, save, regenerate token if prompted, update config, restart |
| `scopes[].status: "error"` | Unexpected API error (network, rate limit, upstream) | Read the `detail` field; usually transient — retry after 30 seconds |
| `summary: "All N scope areas OK."` | ✅ Setup complete | Confirm success with the user |

Only declare setup complete when the summary says all scope areas are OK.

---

## Safety rules

- **Never commit the PIT or Location ID** to any git-tracked file. The Claude config files above are outside the user's project and are fine.
- **Never log the full PIT** to shared locations (support tickets, screen shares, issues). Use the masked preview.
- **Never skip the verification step.** It's the only way to catch missing scopes before the user tries (and fails) to use a tool later.
- **If the user reports something unexpected that isn't covered here,** read `src/tools/setup_check.ts` and `src/client.ts` in this repo for the exact error messages, then respond with grounded guidance.

---

## Post-setup

Once `topline_setup_check` is green, the user can start issuing natural-language requests. See the "What Claude can do" section of [README.md](./README.md) for example prompts.
