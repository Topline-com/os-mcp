# OAuth ownership

`@cloudflare/workers-oauth-provider` owns client registration, CIMD lookup, authorization-server and protected-resource metadata, grants, access and refresh tokens, resource matching, and token validation.

Topline keeps three narrow adapters:

- `authorization.ts` renders the PIT and Location ID form after the provider validates the client, redirect URI, scope, resource, and S256 PKCE request. The form carries only an opaque continuation and CSRF value.
- `flow-do.ts` stores one-time consent continuations and authorization-code redemption leases. This Durable Object exists because concurrent token requests need one atomic winner.
- `provider.ts` applies exact Origin checks, caps token request bodies, and reserves the authorization code before handing the request to the provider.
- `remote.ts` accepts provider tokens only after `unwrapToken` proves expiry, the exact `/mcp` audience, the `mcp` token and grant scopes, OAuth client binding, user-to-connection binding, a live encrypted connection record, and an active connection policy. Modern MCP and `/query/api/*` requests enforce the same policy snapshot.

The provider's grant props contain only a connection ID and OAuth client ID. PITs stay in the encrypted `CONNECTIONS` records. Existing signed connection tokens, legacy signed PIT tokens, and raw PIT bearer clients remain available through `resolveExternalToken` in `remote.ts`.

## Storage migration and rollback

`CONNECTIONS` and `OAUTH_KV` must be different KV namespaces. The sync worker lists only `CONNECTIONS`, so OAuth clients, grants, tokens, and open DCR traffic cannot consume connection-scan pages or subrequests. `apps/edge/wrangler.toml` intentionally contains an all-zero `OAUTH_KV` sentinel; a deploy must fail closed until an operator receives explicit infrastructure approval, creates a dedicated namespace, and replaces the sentinel with its ID. Do not reuse `AUTH_CACHE` or another shared namespace.

Before the first approved deploy:

1. Create a dedicated namespace with `wrangler kv namespace create topline-os-mcp-OAUTH`.
2. Replace only the `OAUTH_KV` sentinel in `apps/edge/wrangler.toml`; leave the `CONNECTIONS` ID unchanged.
3. Run the edge and sync Wrangler dry-runs and verify their binding tables show different IDs.
4. Deploy only after the separate production approval gate.

No production namespace was created or mutated by this change. Existing encrypted PIT records remain in `CONNECTIONS` and need no data migration. If any canary ever ran the provider with `OAUTH_KV` incorrectly bound to `CONNECTIONS`, stop before rollout: export and verify the provider-prefixed OAuth records into the dedicated namespace, then remove only those verified OAuth keys from `CONNECTIONS` under a separately approved cleanup. Never bulk-delete the connection directory.

The `v2` Wrangler migration introduces `OAuthFlowDO`; `v3` introduces the SQLite-backed `ConnectionAuthDO` (`v1` introduced `LocationDO`). Cloudflare cannot roll a deployment back across either Durable Object lifecycle change. After `v3` reaches production, restore older request behavior with a forward deployment that retains both class exports, both bindings, and the full migration history. Leave both KV namespaces intact. If storage configuration must be restored, rebind only to the previous dedicated OAuth namespace. Never point `OAUTH_KV` at `CONNECTIONS`. Provider-issued sessions may require reauthorization if their dedicated namespace is unavailable, while cid, legacy signed, and raw-PIT compatibility paths continue to use the encrypted connection directory.
