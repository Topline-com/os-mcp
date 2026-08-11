# Connection-scoped tool policies

Recorded: 2026-08-11

Topline binds each tool policy to the server-issued connection ID (`cid`). The client does not supply a tool filter on MCP requests. For each authenticated request, the server loads one authorization snapshot, filters `tools/list`, and checks the same resolved set again before handler lookup in `tools/call`.

A legitimate existing connection without an authorization record is the only legacy case that defaults to all tools. The server creates its active/all authorization record lazily after confirming that its encrypted `CONNECTIONS` record exists. A present but malformed, unsupported, cross-tenant, or revoked record fails closed.

## Policy choices

New OAuth and `/connect` flows show the selected tool count and the effect of the policy before confirmation. The default is `read_only_crm`. The built-in choices are `read_only_crm`, `sales`, `marketing`, `analytics`, and `all`; users can also select individual tools with `custom`.

Preset definitions use stable `topline_*` tool IDs. Presets other than `all` expand to an ordered ID snapshot when the connection is created. The `all` policy tracks the current catalog, so newly published tools become available to an all-tools connection. A removed ID in a saved custom or preset snapshot stays visible as a stale management entry but cannot be listed or called.

The current UI warns when a selection contains more than 30 tools. If the user identifies the target as Copilot Studio, the server saves that target and rejects selections above 128. If an all-tools Copilot connection would grow above 128 after a later catalog release, listing and calling fail closed until the policy is narrowed. The server never truncates a selection because truncation would make the saved policy and user confirmation disagree.

## Persistence and request enforcement

`ConnectionAuthDO`, addressed by `cid`, stores one SQLite authorization row. The row contains schema version, active/revoked status, location ID, client target, policy JSON, policy revision, and timestamps. It never stores a PIT, bearer token, authorization code, PKCE verifier, or customer payload.

The location ID binds the authorization object to the connection's tenant. Every read, update, touch, and revocation checks that location before changing state. Policy updates use `expected_policy_version`; stale writes return a conflict instead of overwriting a newer selection. A metadata touch does not change the policy revision or policy JSON.

Remote MCP requests with a `cid` bearer check `ConnectionAuthDO` before the encrypted PIT is decrypted. Query API routes use the same bearer path and map overview, catalog, table explanation, and SQL execution to their corresponding MCP analytics tool IDs. Manual sync, scheduled sync, backfill, and cursor-reset paths check the same object before decrypting credentials or opening `LocationDO`. Raw PIT and legacy embedded-credential tokens retain their earlier bounded behavior and have no per-connection policy.

`tools/list` and `tools/call` use `buildToolAccess()` over the same immutable request snapshot. Listing preserves canonical registry order. Calling checks the allowed ID before looking up a handler, so a hidden known name and an unknown name return the same client-safe error. Raw PIT requests intersect the policy-neutral legacy default with `ACTION_TOOLS`, which keeps analytics unavailable on that path.

## Update, cache, and revocation semantics

A connection-bound bearer can manage its policy at `/connection/policy`. `GET` returns the current policy, client target, revision, selected count, stale IDs, and private-cache revision. `PUT` accepts an expected revision plus an `all`, preset, or custom selection and can change the client target. Existing access credentials remain valid; the new policy applies to the next stateless request.

Tool-list caches are private to the authenticated connection and keyed operationally by `policy_version`. A cached list is never an authorization grant. If a client calls a tool from an old list after the policy narrows, `tools/call` resolves the current authorization snapshot and denies it. Clients that support list-change notifications can refresh sooner after the transport integration adds that signal, but enforcement does not depend on notification or cache behavior.

`DELETE /connection/policy` marks the authorization object revoked before deleting the encrypted connection record. This order blocks new MCP, query, and sync requests even while KV deletion or a client cache lags. Existing bearer tokens cannot reactivate the object and reauthorization is required. The current implementation has no refresh-token store; the OAuth-provider integration must delete its grants and refresh tokens after the DO revocation succeeds.

If credential cleanup fails after revocation, the endpoint returns `202` with `credential_cleanup: "pending"`. The connection remains denied because the DO state is authoritative; operators can retry cleanup without reopening access.

New connection creation writes the encrypted credential record, initializes authorization, and only then issues a bearer. If authorization initialization fails, the server deletes the new credential record and issues nothing. Existing customer records are not rewritten or migrated in bulk.

## Microsoft constraints

The following limits are verified from Microsoft documentation retrieved on 2026-08-11:

- Copilot Studio supports Streamable HTTP and no longer supports legacy SSE. Its server setup supports no authentication, API key, and OAuth configurations. Source: [Connect an existing MCP server to Copilot Studio](https://learn.microsoft.com/en-us/microsoft-copilot-studio/mcp-add-existing-server-to-agent).
- Copilot Studio enables all server tools by default. Makers can turn off `Allow all` and select individual tools; dynamically added server tools remain disabled when `Allow all` is off. Sources: [Add and select MCP tools](https://learn.microsoft.com/en-us/microsoft-copilot-studio/mcp-add-components-to-agent) and [Extend an agent with MCP](https://learn.microsoft.com/en-us/microsoft-copilot-studio/agent-extend-action-mcp).
- Copilot Studio generative orchestration supports at most 128 tools per agent and Microsoft recommends 25 to 30 for performance and selection quality. Source: [Add tools to Copilot Studio custom agents](https://learn.microsoft.com/en-us/microsoft-copilot-studio/add-tools-custom-agent).
- Microsoft 365 declarative-agent MCP plugins use dynamic discovery by default. The runtime fetches tools per session and validates additions or definition changes; developers can pin a fixed list in the plugin manifest instead. Source: [Dynamic tool discovery](https://learn.microsoft.com/en-us/microsoft-365/copilot/extensibility/plugin-dynamic-tool-discovery).
- Microsoft 365 MCP plugin authentication supports Entra SSO, dynamic client registration, OAuth authorization code, and anonymous access. API keys are not supported for that plugin type. Sources: [Build an MCP plugin](https://learn.microsoft.com/en-us/microsoft-365/copilot/extensibility/build-mcp-plugins) and [Plugin authentication](https://learn.microsoft.com/en-us/microsoft-365/copilot/extensibility/plugin-authentication).
- Microsoft agent-connector validation requires unique names and descriptions, valid object-root JSON Schema, and clear required and optional parameters. Source: [Register MCP servers as Microsoft 365 agent connectors](https://learn.microsoft.com/en-us/microsoftteams/platform/m365-apps/agent-connectors).
- Agent 365 BYO MCP registration is preview. Admins review a declared tool snapshot, governance is server-level, approval can take up to 30 minutes to reach Copilot Studio, and republishing a new version is not supported in the documented preview. Source: [Manage agent tools and BYO MCP servers](https://learn.microsoft.com/en-us/microsoft-365/admin/manage/manage-tools-for-agent?view=o365-worldwide).

Microsoft does not document the exact Copilot Studio error at 129 tools. It also does not publish a separate tool-count limit for Microsoft 365 declarative-agent plugins, Agent 365 BYO, or Copilot Cowork in the reviewed pages. A January 26, 2026 [VS Code issue](https://github.com/microsoft/vscode/issues/290356) reported `Tool limit exceeded (132/128)`; this is an observation about VS Code, not a Copilot Studio or Microsoft 365 contract.

The exact protocol revisions accepted by each Microsoft surface, list-change notification behavior, cache lifetime after policy updates, paginated `tools/list` behavior, JSON Schema keyword support, and user-visible revocation errors remain nonproduction smoke-test items. Topline does not claim compatibility for those unknowns without a test-tenant result.

## Verification

The policy suite covers legacy all-tools bootstrap, each preset, custom ordering and deduplication, stale IDs, tenant mismatch, optimistic updates, metadata touches, revocation, malformed and unsupported records, hidden direct calls, unknown calls, stable IDs, canonical ordering, and exact advertised/callable parity. Count fixtures cover 25, 30, 127, 128, and 129 tools. The full repository test, build, and Worker typecheck commands remain the release gates.

No production connection, credential, Durable Object, KV record, or customer data was changed while implementing this policy layer.
