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
#    Generate a fresh 32-byte secret and paste it when prompted.
openssl rand -hex 32
npx wrangler secret put TOKEN_SIGNING_SECRET
```

## Storage preflight

The edge Worker requires separate `CONNECTIONS` and `OAUTH_KV` namespaces. It also owns the SQLite-backed `LocationDO`, `OAuthFlowDO`, and `ConnectionAuthDO` classes. Keep all Wrangler migration entries append-only.

Run both dry-runs before deployment and inspect their binding tables:

```bash
npx wrangler deploy --dry-run --config apps/edge/wrangler.toml
npx wrangler deploy --dry-run --config apps/sync/wrangler.toml
```

Confirm that `OAUTH_KV` and `CONNECTIONS` are different, and that the sync Worker has no OAuth storage binding.

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
| `GET  /.well-known/oauth-authorization-server` | Authorization-server metadata (RFC 8414) |
| `GET  /.well-known/oauth-protected-resource/mcp` | Protected-resource metadata (RFC 9728) |
| `POST /register` | Dynamic Client Registration (RFC 7591) |
| `GET  /authorize` | Validated OAuth consent and credential form |
| `POST /authorize` | Form submission and connection creation |
| `POST /token` | Authorization-code and refresh-token exchange |
| `POST /mcp` | MCP JSON-RPC endpoint (requires Bearer auth) |

## How auth works (short version)

- OAuth clients use DCR or CIMD, exact redirect-URI validation, S256 PKCE, one-time authorization codes, and access tokens bound to the canonical `/mcp` resource.
- The PIT is encrypted in `CONNECTIONS`. OAuth clients, grants, and tokens live in the dedicated `OAUTH_KV` namespace.
- Each connection's active tool policy and revocation state live in `ConnectionAuthDO`. The same policy filters `tools/list` and gates `tools/call`.
- Direct clients can use a `/connect` bearer token. Raw PIT + `X-Topline-Location-Id` remains available for compatible clients and exposes action tools only.

## Rotating the signing secret

```bash
openssl rand -hex 32
npx wrangler secret put TOKEN_SIGNING_SECRET
```

Signed `/connect` and legacy tokens become invalid. OAuth-provider sessions may also require reconnection because connection credentials use the same secret for encryption.

## Rollback limit

Wrangler migrations `v2` and `v3` introduce `OAuthFlowDO` and `ConnectionAuthDO`. Cloudflare cannot roll back to a version from before those Durable Object lifecycle changes. Recover by deploying a forward fix that preserves all three class exports, bindings, and migration entries.

## Logs

```bash
npx wrangler tail
```

Streams live Worker logs. Useful for debugging customer connection issues — but **never** log the raw PIT; the code is careful not to, and neither should you.
