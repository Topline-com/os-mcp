# Dependency audit — MCP v2 transport migration

Recorded: 2026-08-11T21:02:18Z

## Result

`npm audit --json` reports 0 vulnerabilities after a clean MCP v2 and Wrangler 4 dependency install.

## Classification

All 14 baseline findings are fixed. There are no accepted, blocked-upstream, unreachable, or dev-only findings remaining.

| Baseline finding | Classification | Evidence |
| --- | --- | --- |
| `@modelcontextprotocol/sdk`, `@hono/node-server`, `body-parser`, `express-rate-limit`, `fast-uri`, `hono`, `ip-address`, `qs` | Fixed | Removed with the monolithic MCP v1 package. Runtime now uses `@modelcontextprotocol/server@2.0.0`; tests use `@modelcontextprotocol/client@2.0.0`. |
| `wrangler`, `miniflare`, `sharp`, `undici`, `ws` | Fixed | Both Worker workspaces use `wrangler@4.121.0`; its resolved transitives are outside the audited ranges. |
| `esbuild` | Fixed | Direct build dependency is `0.28.2`; `tsx@4.23.12` deduplicates to `0.28.2`; Wrangler resolves `0.28.1`. |

## Reproduction

```text
npm ci --no-audit
npm audit --json
npm ls @modelcontextprotocol/server @modelcontextprotocol/client wrangler esbuild tsx
```

Expected audit metadata:

```json
{"info":0,"low":0,"moderate":0,"high":0,"critical":0,"total":0}
```

No `npm audit fix --force` or unrelated broad upgrade was used.
