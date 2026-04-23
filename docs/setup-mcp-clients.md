# Setup — Zapier, n8n, mcp-inspector, curl, and other Bearer-only clients

If your MCP client can't complete an OAuth 2.1 flow, use `/connect` to mint a single long-lived Bearer token. Paste that token into whatever client you're using.

## 1. Generate a token

1. Create a Private Integration in Topline OS (Settings → Private Integrations → **Select All** scopes). Copy the `pit-…` token.
2. Copy your Location ID from Settings → Business Info.
3. Visit **https://os-mcp.topline.com/connect** in a browser.
4. Paste the PIT and Location ID. Click **Generate token**. Click **Copy**.

Keep that token private — anyone with it can drive your Topline OS sub-account. It's valid for 1 year.

## 2. Configure your client

All clients use the same pattern:

- **URL:** `https://os-mcp.topline.com/mcp`
- **Method:** `POST`
- **Header:** `Authorization: Bearer <your token>`
- **Header:** `Content-Type: application/json`
- **Body:** JSON-RPC 2.0

### n8n (native MCP Client node)

1. Add an **MCP Client** node.
2. Credential → new MCP credential:
   - Transport: `HTTP (Streamable)`
   - Server URL: `https://os-mcp.topline.com/mcp`
   - Headers: add one — name `Authorization`, value `Bearer <your token>`
3. The node auto-discovers all 48 tools. Pick one per node.

### Zapier

Zapier doesn't natively speak MCP — use a **Webhooks by Zapier → Custom Request (POST)** step:

- URL: `https://os-mcp.topline.com/mcp`
- Headers:
  - `Authorization`: `Bearer <your token>`
  - `Content-Type`: `application/json`
- Data pass-through: JSON
- Data:
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "topline_search_contacts",
      "arguments": { "query": "jane@example.com" }
    }
  }
  ```

Replace the `method` / `params` per the tool you want. Full tool list: send a `tools/list` request first and inspect the response.

### mcp-inspector

```bash
npx @modelcontextprotocol/inspector
```

In the UI: Transport = `SSE` or `Streamable HTTP`, URL = `https://os-mcp.topline.com/mcp`, Headers → add `Authorization: Bearer <your token>`.

### curl (for testing / scripts)

```bash
TOKEN="<paste the token from /connect>"

# List all tools
curl -s -X POST https://os-mcp.topline.com/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | jq

# Run the setup check
curl -s -X POST https://os-mcp.topline.com/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"topline_setup_check","arguments":{}}}' | jq

# Search contacts
curl -s -X POST https://os-mcp.topline.com/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"topline_search_contacts","arguments":{"query":"acme"}}}' | jq
```

### ChatGPT (Apps — Access token mode)

If you selected "Access token / API key" in ChatGPT (rather than OAuth — see [setup-chatgpt.md](./setup-chatgpt.md) for the recommended path):

| Field | Value |
|---|---|
| MCP Server URL | `https://os-mcp.topline.com/mcp` |
| Authentication | `Access token / API key` |
| Header scheme | `Bearer` |
| Token | the token from `/connect` |

## 3. Verify

Every MCP client can invoke `topline_setup_check`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": { "name": "topline_setup_check", "arguments": {} }
}
```

If the `summary` field says all scope areas are OK, you're live.

## Rotating / revoking

- **Single client:** generate a fresh token from `/connect` and swap it in. The old one still works until it expires (or you rotate the PIT).
- **All tokens for one sub-account:** rotate the PIT in Topline OS → Settings → Private Integrations. Every token ever issued with the old PIT fails on the next request.
- **All tokens globally (across all users):** rotate the worker's `TOKEN_SIGNING_SECRET`. Every token ever signed becomes invalid immediately. See [deploy-cloudflare-worker.md](./deploy-cloudflare-worker.md#rotating-the-signing-secret).
