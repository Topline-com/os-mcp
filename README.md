# Topline OS MCP

Drive your Topline OS sub-account from Claude. Search contacts, send SMS and email, book appointments, create opportunities, enroll workflows — all under your sub-account's permissions.

> **Point Claude at this repo and it sets itself up.**
> Open a new Claude conversation and say:
> *"Set up this MCP for me: https://github.com/topline-com/os-mcp"*
> Claude will read [CLAUDE.md](./CLAUDE.md) and walk you through the whole thing.

---

## For Claude: Setup Protocol

If you are an AI assistant helping a user connect this MCP, follow these steps in order. Do not skip ahead. Ask one question at a time.

### Step 1 — Confirm which Claude the user is on

Ask: **"Are you using Claude Desktop or Claude Code?"** Branch on the answer.

### Step 2 — Guide the user to create a Private Integration

Tell the user, verbatim:

> 1. Log in to your Topline OS sub-account.
> 2. Go to **Settings → Private Integrations**.
> 3. Click **Create new integration**.
> 4. Name: `Claude`. Description: `Claude MCP`.
> 5. On the scopes screen, click **Select All**.
> 6. Click **Create**.
> 7. Copy the token — it starts with `pit-`. **You will not see it again.** Paste it here in the chat.

When the user pastes the token, confirm it starts with `pit-`. If it does not, ask them to re-copy from Step 6.

### Step 3 — Get the Location ID

Tell the user:

> Go to **Settings → Business Info** in the same sub-account. Scroll to the bottom and copy the **Location ID**. Paste it here.

### Step 4 — Edit the Claude config

**Claude Desktop — macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Claude Desktop — Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**Claude Code:** run the `claude mcp add` command in Step 5.

Read the existing config (if any). Merge this block into `mcpServers` — do not overwrite other servers.

```json
{
  "mcpServers": {
    "topline": {
      "command": "npx",
      "args": ["-y", "github:topline-com/os-mcp"],
      "env": {
        "TOPLINE_PIT": "pit-xxxxxxxx",
        "TOPLINE_LOCATION_ID": "xxxxxxxx",
        "TOPLINE_BRAND_NAME": "Topline OS"
      }
    }
  }
}
```

Replace `pit-xxxxxxxx` with the token from Step 2 and `xxxxxxxx` with the Location ID from Step 3.

### Step 5 — Claude Code alternative (skip if Desktop)

```bash
claude mcp add topline -s user \
  -e TOPLINE_PIT=pit-xxxxxxxx \
  -e TOPLINE_LOCATION_ID=xxxxxxxx \
  -e TOPLINE_BRAND_NAME="Topline OS" \
  -- npx -y github:topline-com/os-mcp
```

### Step 6 — Restart Claude

- **Desktop:** fully quit (`Cmd+Q` on macOS, right-click tray icon → Quit on Windows) and reopen.
- **Code:** start a new `claude` session.

First launch will take 10–30 seconds while `npx` fetches and builds the package. Subsequent launches are fast.

### Step 7 — Verify

In a **new** Claude conversation, say:

> Run `topline_setup_check`.

The tool returns a structured report: auth status, location details, and pass/fail for every scope area. If anything is `forbidden`, the user missed a scope — send them back to **Settings → Private Integrations**, have them edit the integration, click **Select All** again, save, and if prompted regenerate the token. Update the config with the new token and restart Claude.

If everything is green, setup is done.

---

## What Claude can do

Ask in plain English. Examples:

**Contacts**
- *"Find the contact Jane Doe and show me her recent messages."*
- *"Create a contact for john@acme.com with tag `inbound-lead`."*
- *"Tag every contact named `Acme` with `vip` and enroll them in the Onboarding workflow."*

**Messaging**
- *"Send Jane an SMS: 'Your proposal is ready for review.'"*
- *"Email the contact at ceo@acme.com with subject `Next steps` and body `Let's schedule the demo.`"*

**Pipelines & Opportunities**
- *"Show me all pipelines."*
- *"Create a $12,000 opportunity in the Sales pipeline, Discovery stage, for Acme Corp."*
- *"Move the Acme opportunity to Closed Won."*

**Calendars**
- *"Show me free slots on the Discovery Call calendar next Tuesday."*
- *"Book Acme for a Discovery Call next Tuesday at 2pm Eastern."*

**Ops**
- *"Which contacts haven't been messaged in 30 days and are tagged `warm-lead`?"*
- *"Add a task to call Jane back tomorrow at 10am."*

48 curated tools plus a generic `topline_request` escape hatch that can hit any Topline API endpoint.

---

## Tool categories

| Area | Tools |
|---|---|
| Health & verification | `topline_ping`, `topline_setup_check` |
| Contacts | search, get, create, update, delete, upsert, add/remove tags, enroll/remove workflow |
| Conversations | search, get, list messages, send message (SMS / Email / WhatsApp / IG / FB / Custom) |
| Opportunities | list pipelines, search, get, create, update, delete |
| Calendars | list calendars, get slots, book / update / cancel appointments |
| Tasks | list / create / update / delete |
| Notes | list / create / update / delete |
| Custom fields | list / get |
| Workflows | list |
| Tags | list |
| Users | list / get |
| Forms & surveys | list, list submissions |
| Location | `topline_get_location` |
| Escape hatch | `topline_request` — call any `services.leadconnectorhq.com` endpoint |

---

## Appendix A — Full scope reference

Click **Select All** when creating the Private Integration. If your Topline OS build does not have a Select All button, tick every scope below:

- `contacts.readonly`, `contacts.write`
- `conversations.readonly`, `conversations.write`
- `conversations/message.readonly`, `conversations/message.write`
- `opportunities.readonly`, `opportunities.write`
- `calendars.readonly`, `calendars.write`, `calendars/events.readonly`, `calendars/events.write`
- `workflows.readonly`
- `forms.readonly`, `forms.write`
- `surveys.readonly`
- `users.readonly`
- `locations.readonly`
- `locations/customFields.readonly`, `locations/customFields.write`
- `locations/tags.readonly`, `locations/tags.write`
- `locations/tasks.readonly`, `locations/tasks.write`
- `medias.readonly`, `medias.write`

`topline_setup_check` probes each of these and tells you which are missing.

---

## Appendix B — Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Authentication failed. Your Private Integration Token is invalid or expired.` | PIT is wrong, revoked, or you copied the name instead of the token | Regenerate in Settings → Private Integrations, update config, restart Claude |
| `Forbidden — missing scope` from a specific tool | Private Integration doesn't have that scope ticked | Edit the integration, click Select All, save, regenerate token if prompted |
| `Rate limited by Topline.` | Too many calls in a short window | MCP retries automatically with backoff; if persistent, pace requests |
| Claude doesn't see Topline tools | Config not reloaded, or JSON invalid | Fully quit Claude Desktop (`Cmd+Q`), validate config at jsonlint.com |
| `TOPLINE_PIT is missing` at startup | `env` block attached to the wrong server key | Ensure `env` is nested under `"topline"`, not at top level |
| `npx: command not found` | Node.js not installed or not on PATH | Install Node 20+ from https://nodejs.org |
| First launch hangs | `npx` is downloading and building the package | Wait 10–30s the first time; subsequent launches are cached |

---

## Appendix C — Further white-labeling

Agencies reselling this MCP can override the brand name end-users see. Add to the `env` block:

```json
"env": {
  "TOPLINE_PIT": "pit-...",
  "TOPLINE_LOCATION_ID": "...",
  "TOPLINE_BRAND_NAME": "Acme Growth"
}
```

All user-facing error messages, tool descriptions, and server identity switch to "Acme Growth" automatically.

---

## Security

- The PIT lives **only** in the user's local Claude config. It never leaves the machine except in outbound calls to `services.leadconnectorhq.com`.
- The MCP runs locally as a subprocess of Claude. No hosted intermediary.
- Revoke any token at any time from **Settings → Private Integrations**.

## License

MIT. See [LICENSE](./LICENSE).
