# OAuth ownership

`@cloudflare/workers-oauth-provider` owns client registration, CIMD lookup, authorization-server and protected-resource metadata, grants, access and refresh tokens, resource matching, and token validation.

Topline keeps three narrow adapters:

- `authorization.ts` renders the PIT and Location ID form after the provider validates the client, redirect URI, scope, resource, and S256 PKCE request. The form carries only an opaque continuation and CSRF value.
- `flow-do.ts` stores one-time consent continuations and authorization-code redemption leases. This Durable Object exists because concurrent token requests need one atomic winner.
- `provider.ts` applies exact Origin checks, caps token request bodies, and reserves the authorization code before handing the request to the provider.
- `remote.ts` accepts provider tokens only after `unwrapToken` proves expiry, the exact `/mcp` audience, the `mcp` token and grant scopes, OAuth client binding, user-to-connection binding, a live encrypted connection record, and an active connection policy. Modern MCP and `/query/api/*` requests enforce the same policy snapshot.

The provider's grant props contain only a connection ID and OAuth client ID. PITs stay in the encrypted `CONNECTIONS` records. Existing signed connection tokens, legacy signed PIT tokens, and raw PIT bearer clients remain available through `resolveExternalToken` in `remote.ts`.

## Storage migration and rollback

`CONNECTIONS` and `OAUTH_KV` must remain different KV namespaces. The sync worker lists only `CONNECTIONS`, so OAuth clients, grants, tokens, and open DCR traffic cannot consume connection-scan pages or subrequests. Production has a dedicated `OAUTH_KV`; do not reuse `CONNECTIONS`, `AUTH_CACHE`, or another shared namespace.

Before any deployment, run edge and sync Wrangler dry-runs and confirm the binding tables keep `OAUTH_KV` separate from `CONNECTIONS`. Existing encrypted PIT records remain in `CONNECTIONS` and need no data migration. Never bulk-delete the connection directory.

The `v2` Wrangler migration introduces `OAuthFlowDO`; `v3` introduces the SQLite-backed `ConnectionAuthDO` (`v1` introduced `LocationDO`). Cloudflare cannot roll a deployment back across either Durable Object lifecycle change. After `v3` reaches production, restore older request behavior with a forward deployment that retains both class exports, both bindings, and the full migration history. Leave both KV namespaces intact. If storage configuration must be restored, rebind only to the previous dedicated OAuth namespace. Never point `OAUTH_KV` at `CONNECTIONS`. Provider-issued sessions may require reauthorization if their dedicated namespace is unavailable, while cid, legacy signed, and raw-PIT compatibility paths continue to use the encrypted connection directory.
