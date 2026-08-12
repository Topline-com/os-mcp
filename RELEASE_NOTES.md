# Topline OS MCP v0.2.0

Released: 2026-08-11

v0.2.0 is the first public release of Topline OS MCP. It ships the local stdio server, the hosted Cloudflare Worker, 127 registered Worker tools, per-tenant analytics storage, and connection-scoped tool authorization.

## Compatibility

The hosted endpoint supports both MCP protocol generations used by current clients:

- MCP `2026-07-28`, including stateless `server/discover`, modern request metadata, standard transport headers, and private cache hints.
- MCP `2025-06-18`, including `initialize`, `notifications/initialized`, and Streamable HTTP compatibility.

The stdio server supports the modern protocol and remains available to Claude Desktop, Claude Code, and Codex through the existing `npx` install. Raw PIT bearer clients keep the action-only compatibility path. OAuth and `/connect` clients use connection-bound bearer tokens.

## OAuth and authorization

OAuth now uses `@cloudflare/workers-oauth-provider` for client registration, CIMD lookup, authorization-server and protected-resource metadata, grants, access and refresh tokens, resource matching, and token validation.

The release adds:

- Dynamic Client Registration compatibility and CIMD support.
- Exact client and redirect-URI validation before the credential form renders.
- S256 PKCE for public clients; plain PKCE and the implicit flow are disabled.
- One-time consent continuations and one-time authorization-code redemption guarded by `OAuthFlowDO`.
- Access tokens bound to the canonical `https://os-mcp.topline.com/mcp` resource.
- Exact browser Origin checks, bounded token request bodies, and explicit MCP CORS headers.
- Encrypted PIT connection records. OAuth grant data contains the connection ID and OAuth client ID, not the PIT.

Each hosted connection now owns a server-side tool policy. The server filters `tools/list` and independently checks the same policy before `tools/call` dispatch. Presets cover read-only CRM, sales, marketing, analytics, and all tools; custom allowlists are also supported. Revoked, malformed, unsupported, and cross-tenant policy records fail closed.

## Storage and lifecycle

OAuth clients, grants, and tokens use a dedicated `OAUTH_KV` namespace. It must remain separate from the encrypted `CONNECTIONS` directory.

The edge Worker owns three SQLite-backed Durable Object classes:

- `LocationDO` stores each tenant's synced analytics data.
- `OAuthFlowDO` serializes consent and authorization-code redemption.
- `ConnectionAuthDO` stores each connection's active policy, revision, and revocation state.

Wrangler migrations `v2` and `v3` add `OAuthFlowDO` and `ConnectionAuthDO`. Cloudflare cannot roll back across those Durable Object lifecycle changes. Any recovery from a pre-v2/v3 behavior regression must be a forward fix that preserves all three class exports, bindings, and migration entries.

## Production verification

The functional code underlying v0.2.0 was deployed to production on 2026-08-11 before the release-only version and documentation commit. The verification covered:

- Edge-first, then sync Worker deployment.
- Applied `v2` and `v3` Durable Object migrations with SQLite storage.
- Separate `OAUTH_KV` and `CONNECTIONS` bindings.
- Hosted landing and OAuth metadata responses.
- Rejection of an unapproved browser Origin.
- MCP `2026-07-28` discovery and deterministic raw-PIT action-tool listing.
- MCP `2025-06-18` initialization and deterministic raw-PIT action-tool listing.
- Setup authentication and location checks.
- The sync Worker's unauthenticated authorization gate.

The production setup check passed 18 of 19 scope probes. The LinkedIn ad-publishing OAuth-data probe returned an upstream HTTP 422 response. This is a known LinkedIn integration limitation, not an MCP transport, OAuth, or deployment failure.

## Install and upgrade

Hosted clients using `https://os-mcp.topline.com/mcp` need no local upgrade. Reconnect only if the client reports an expired or invalid token.

Claude Desktop, Claude Code, and Codex installations that already run:

```text
npx -y github:topline-com/os-mcp
```

should fully quit and reopen the client. The next process launch installs the current repository version.

For a local development checkout:

```text
git pull
npm ci
npm run test
npm run build
npm run worker:typecheck
npm run worker:typecheck -w @topline-ai/sync
```

Do not remove or renumber existing Wrangler migrations during an upgrade. Keep `OAUTH_KV` separate from `CONNECTIONS`.
