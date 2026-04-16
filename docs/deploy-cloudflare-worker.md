# Deploying the Remote MCP to Cloudflare Workers

This deploys `src/remote.ts` as a hosted MCP server that Claude web / Team / Enterprise users can connect to via the "Add custom connector" flow.

Prerequisites:
- A Cloudflare account (free tier is fine)
- Node.js 20+ locally
- This repo cloned

## One-time setup

```bash
# 1. Install deps (includes wrangler)
npm install

# 2. Authenticate wrangler with your Cloudflare account
npx wrangler login

# 3. Generate and store the token-signing secret.
#    This secret signs OAuth access tokens. Generate a fresh 32-byte secret
#    and paste it when prompted. Treat it like a database password.
openssl rand -hex 32
npx wrangler secret put TOKEN_SIGNING_SECRET
```

## Deploy

```bash
npm run worker:deploy
```

You'll get a URL like `https://topline-os-mcp.<your-subdomain>.workers.dev`. That URL + `/mcp` is what clients paste into Claude.

## Custom domain (recommended for client-facing use)

In the Cloudflare dashboard:

1. **Workers & Pages → topline-os-mcp → Settings → Domains & Routes → Add → Custom Domain**
2. Enter e.g. `os-mcp.topline.com`
3. Cloudflare provisions TLS automatically. Takes 1–5 minutes.

Clients then connect to `https://os-mcp.topline.com/mcp`.

## Local development

```bash
# Put the signing secret in .dev.vars (gitignored):
echo "TOKEN_SIGNING_SECRET = \"$(openssl rand -hex 32)\"" > .dev.vars

npm run worker:dev
```

The Worker runs at `http://127.0.0.1:8787`. Test with:

```bash
# OAuth metadata
curl http://127.0.0.1:8787/.well-known/oauth-authorization-server

# MCP initialize (using raw PIT bearer — bypasses the OAuth dance)
curl -X POST http://127.0.0.1:8787/mcp \
  -H "Authorization: Bearer pit-YOUR-REAL-TOKEN" \
  -H "X-Topline-Location-Id: YOUR-REAL-LOC-ID" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"topline_setup_check","arguments":{}}}'
```

## What the deployed Worker exposes

| Path | Purpose |
|---|---|
| `GET  /` | Landing page |
| `GET  /.well-known/oauth-authorization-server` | OAuth 2.1 metadata (RFC 8414) |
| `POST /register` | Dynamic Client Registration (RFC 7591) |
| `GET  /authorize` | HTML form for user to paste PIT + Location ID |
| `POST /authorize` | Form submission → issues signed auth code |
| `POST /token` | Auth code + PKCE verifier → signed access token |
| `POST /mcp` | MCP JSON-RPC endpoint (requires Bearer auth) |

## How auth works (short version)

- Claude web users → OAuth flow → signed access token (HMAC-SHA256) containing `{pit, locationId, exp}` → sent as Bearer on every `/mcp` call.
- Direct clients (mcp-inspector, curl, custom integrations) → `Authorization: Bearer pit-...` with a raw PIT + `X-Topline-Location-Id` header.
- No server-side storage. If you rotate `TOKEN_SIGNING_SECRET`, all existing OAuth sessions invalidate and users re-authorize on next call.

## Rotating the signing secret

```bash
openssl rand -hex 32
npx wrangler secret put TOKEN_SIGNING_SECRET
```

All previously-issued access tokens become invalid. Users will be prompted to reconnect.

## Logs

```bash
npx wrangler tail
```

Streams live Worker logs. Useful for debugging customer connection issues — but **never** log the raw PIT; the code is careful not to, and neither should you.
