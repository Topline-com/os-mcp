// Generate a static HTML page documenting every MCP tool in the registry.
// Run: tsx scripts/gen-docs.ts > docs/api/os-mcp/index.html
import { ALL_TOOLS, ANALYTICS_TOOL_NAMES } from "../apps/edge/src/registry.js";
import type { ToolDef } from "../apps/edge/src/tools/types.js";

type Group = { id: string; title: string; blurb: string; match: (name: string) => boolean };

const GROUPS: Group[] = [
  { id: "setup", title: "Setup & diagnostics", blurb: "Verify your PIT and probe every scope.", match: (n) => ["topline_ping", "topline_setup_check", "topline_request"].includes(n) },
  { id: "contacts", title: "Contacts", blurb: "Create, search, and update contacts and their tags, notes, tasks, and workflow enrollments.", match: (n) => /^topline_(search_contacts|get_contact|create_contact|upsert_contact|update_contact|delete_contact|add_contact_tags|remove_contact_tags|add_contact_to_workflow|remove_contact_from_workflow|list_contact_)/.test(n) },
  { id: "conversations", title: "Conversations & messaging", blurb: "Send SMS, email, WhatsApp, and DMs; read conversation history.", match: (n) => /^topline_(search_conversations|get_conversation|create_conversation|get_messages|send_message)$/.test(n) },
  { id: "opportunities", title: "Opportunities & pipelines", blurb: "Move deals through pipeline stages, mark won/lost, set value.", match: (n) => /^topline_(list_pipelines|search_opportunities|get_opportunity|create_opportunity|update_opportunity|delete_opportunity)$/.test(n) },
  { id: "calendars", title: "Calendars & appointments", blurb: "Read calendar config, find slots, book appointments.", match: (n) => /^topline_(list_calendars|get_calendar|update_calendar|delete_calendar|get_calendar_slots|create_appointment|update_appointment|delete_appointment)$/.test(n) },
  { id: "tasks", title: "Tasks", blurb: "Create and complete tasks against contacts.", match: (n) => /_task$/.test(n) || /^topline_create_task$/.test(n) },
  { id: "notes", title: "Notes", blurb: "Add and update contact notes.", match: (n) => /^topline_(create_note|update_note|delete_note)$/.test(n) },
  { id: "custom_fields", title: "Custom fields", blurb: "Define and manage custom fields on contacts and opportunities.", match: (n) => /custom_field/.test(n) },
  { id: "custom_values", title: "Custom values", blurb: "Sub-account custom values used as merge tags in workflows and messages.", match: (n) => /custom_value/.test(n) },
  { id: "workflows", title: "Workflows", blurb: "Enroll and remove contacts from automation workflows.", match: (n) => /^topline_list_workflows$/.test(n) },
  { id: "tags", title: "Tags", blurb: "Create, rename, and delete tags on the sub-account.", match: (n) => /^topline_(list_tags|create_tag|update_tag|delete_tag)$/.test(n) },
  { id: "users", title: "Users", blurb: "List sub-account users and fetch user records.", match: (n) => /^topline_(list_users|get_user)$/.test(n) },
  { id: "location", title: "Location (sub-account)", blurb: "Sub-account metadata: name, address, timezone, business info.", match: (n) => /^topline_get_location$/.test(n) },
  { id: "forms", title: "Forms & surveys", blurb: "List forms and surveys and read their submissions.", match: (n) => /(form|survey)/.test(n) },
  { id: "analytics", title: "Analytics (SQL)", blurb: "Read-only SQL surface over the sub-account data warehouse. Worker-only.", match: (n) => ANALYTICS_TOOL_NAMES.has(n) },
];

function pickGroup(t: ToolDef): Group {
  for (const g of GROUPS) if (g.match(t.name)) return g;
  throw new Error(`No group for tool: ${t.name}`);
}

const grouped = new Map<string, ToolDef[]>();
for (const g of GROUPS) grouped.set(g.id, []);
for (const t of ALL_TOOLS) grouped.get(pickGroup(t).id)!.push(t);

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function renderType(schema: any): string {
  if (!schema) return "any";
  if (schema.enum) return schema.enum.map((v: any) => `<code>${escapeHtml(JSON.stringify(v))}</code>`).join(" | ");
  if (schema.type === "array") return `array&lt;${renderType(schema.items)}&gt;`;
  if (schema.type === "object") return "object";
  if (Array.isArray(schema.type)) return schema.type.join(" | ");
  return schema.type ?? "any";
}

function renderProps(schema: any, required: string[] = [], depth = 0): string {
  if (!schema?.properties) return "";
  const rows: string[] = [];
  for (const [key, raw] of Object.entries<any>(schema.properties)) {
    const isReq = required.includes(key);
    const desc = raw.description ? `<div class="desc">${escapeHtml(raw.description)}</div>` : "";
    let nested = "";
    if (raw.type === "object" && raw.properties) {
      nested = `<details class="nested"><summary>nested properties</summary>${renderProps(raw, raw.required ?? [], depth + 1)}</details>`;
    } else if (raw.type === "array" && raw.items?.type === "object" && raw.items.properties) {
      nested = `<details class="nested"><summary>item properties</summary>${renderProps(raw.items, raw.items.required ?? [], depth + 1)}</details>`;
    }
    rows.push(`
      <tr>
        <td><code>${escapeHtml(key)}</code>${isReq ? ' <span class="req">required</span>' : ""}</td>
        <td class="type">${renderType(raw)}</td>
        <td>${desc}${nested}</td>
      </tr>`);
  }
  return `<table class="params"><thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
}

function renderTool(t: ToolDef): string {
  const props = t.inputSchema?.properties ?? {};
  const hasParams = Object.keys(props).length > 0;
  const required = t.inputSchema?.required ?? [];
  const example = buildExample(t);
  return `
    <article class="tool" id="${t.name}">
      <header>
        <h3><code>${t.name}</code></h3>
        <a class="anchor" href="#${t.name}">#</a>
      </header>
      <p class="tool-desc">${escapeHtml(t.description)}</p>
      ${hasParams ? `<h4>Parameters</h4>${renderProps(t.inputSchema, required)}` : '<p class="no-params">No parameters.</p>'}
      <h4>Example call</h4>
      <pre><code>${escapeHtml(example)}</code></pre>
    </article>
  `;
}

function buildExample(t: ToolDef): string {
  const props = t.inputSchema?.properties ?? {};
  const required = t.inputSchema?.required ?? [];
  const args: Record<string, unknown> = {};
  for (const k of required) {
    args[k] = exampleValue((props as any)[k]);
  }
  // Show one or two optional fields too if no required ones.
  if (required.length === 0) {
    const optional = Object.keys(props).slice(0, 2);
    for (const k of optional) args[k] = exampleValue((props as any)[k]);
  }
  return JSON.stringify(
    { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: t.name, arguments: args } },
    null,
    2,
  );
}

function exampleValue(schema: any): unknown {
  if (!schema) return null;
  if (schema.enum) return schema.enum[0];
  if (schema.type === "string") return schema.format === "email" ? "alex@example.com" : "string";
  if (schema.type === "number" || schema.type === "integer") return 0;
  if (schema.type === "boolean") return false;
  if (schema.type === "array") return [exampleValue(schema.items)];
  if (schema.type === "object") {
    const o: Record<string, unknown> = {};
    for (const [k, v] of Object.entries<any>(schema.properties ?? {})) o[k] = exampleValue(v);
    return o;
  }
  return null;
}

const sidebar = GROUPS
  .filter((g) => grouped.get(g.id)!.length > 0)
  .map((g) => {
    const tools = grouped.get(g.id)!;
    const links = tools.map((t) => `<li><a href="#${t.name}">${t.name}</a></li>`).join("");
    return `<section><h4><a href="#group-${g.id}">${g.title}</a></h4><ul>${links}</ul></section>`;
  })
  .join("");

const main = GROUPS
  .filter((g) => grouped.get(g.id)!.length > 0)
  .map((g) => {
    const tools = grouped.get(g.id)!;
    return `
      <section class="group" id="group-${g.id}">
        <h2>${g.title}</h2>
        <p class="group-blurb">${escapeHtml(g.blurb)}</p>
        ${tools.map(renderTool).join("")}
      </section>`;
  })
  .join("");

const totalActions = ALL_TOOLS.filter((t) => !ANALYTICS_TOOL_NAMES.has(t.name)).length;
const totalAnalytics = ANALYTICS_TOOL_NAMES.size;

const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Topline OS MCP — Tool reference</title>
  <meta name="description" content="Native MCP tool reference for the Topline OS MCP server. ${ALL_TOOLS.length} tools across ${GROUPS.filter((g) => grouped.get(g.id)!.length > 0).length} categories." />
  <style>
    :root {
      --bg: #0b0d10;
      --panel: #14181d;
      --panel-2: #1a1f26;
      --border: #232a33;
      --text: #e6e9ee;
      --muted: #9aa3ad;
      --accent: #5dd6a8;
      --accent-2: #6fb1ff;
      --code-bg: #0f1418;
      --req: #f59e8a;
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; background: var(--bg); color: var(--text); font: 15px/1.55 -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Roboto, system-ui, sans-serif; }
    a { color: var(--accent-2); text-decoration: none; }
    a:hover { text-decoration: underline; }
    code { font-family: "SF Mono", ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace; font-size: 0.92em; background: var(--code-bg); padding: 0.1em 0.4em; border-radius: 4px; border: 1px solid var(--border); }
    pre { background: var(--code-bg); border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; overflow-x: auto; font-size: 13px; line-height: 1.5; }
    pre code { background: none; border: none; padding: 0; }
    .layout { display: grid; grid-template-columns: 280px 1fr; min-height: 100vh; }
    aside.sidebar { background: var(--panel); border-right: 1px solid var(--border); padding: 24px 20px; position: sticky; top: 0; height: 100vh; overflow-y: auto; }
    aside.sidebar h1 { font-size: 16px; margin: 0 0 4px; }
    aside.sidebar .tag { font-size: 11px; color: var(--muted); display: block; margin-bottom: 20px; }
    aside.sidebar h4 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin: 18px 0 6px; }
    aside.sidebar h4 a { color: var(--muted); }
    aside.sidebar ul { list-style: none; padding: 0; margin: 0; }
    aside.sidebar li { margin: 0; }
    aside.sidebar li a { display: block; padding: 3px 8px; border-radius: 4px; font-size: 12.5px; color: var(--text); }
    aside.sidebar li a:hover { background: var(--panel-2); text-decoration: none; }
    main { padding: 40px 56px; max-width: 920px; }
    main > header.hero { padding-bottom: 24px; border-bottom: 1px solid var(--border); margin-bottom: 32px; }
    main > header.hero h1 { font-size: 32px; margin: 0 0 8px; }
    main > header.hero p { color: var(--muted); margin: 0 0 12px; }
    .stats { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 12px; }
    .stat { background: var(--panel); border: 1px solid var(--border); padding: 8px 14px; border-radius: 8px; font-size: 13px; }
    .stat strong { color: var(--accent); }
    section.group { margin-bottom: 56px; }
    section.group h2 { font-size: 22px; margin: 0 0 4px; padding-top: 12px; border-top: 1px dashed var(--border); }
    section.group .group-blurb { color: var(--muted); margin: 0 0 24px; }
    article.tool { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 22px 24px; margin-bottom: 18px; }
    article.tool header { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 8px; }
    article.tool h3 { margin: 0; font-size: 17px; font-weight: 600; }
    article.tool h3 code { background: none; border: none; padding: 0; color: var(--accent); font-size: 1em; }
    article.tool h4 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin: 18px 0 8px; }
    article.tool a.anchor { color: var(--muted); font-size: 14px; opacity: 0.5; }
    article.tool a.anchor:hover { opacity: 1; text-decoration: none; }
    article.tool .tool-desc { margin: 0 0 6px; color: var(--text); }
    article.tool .no-params { color: var(--muted); font-style: italic; margin: 14px 0 0; }
    table.params { width: 100%; border-collapse: collapse; font-size: 13.5px; }
    table.params th { text-align: left; font-weight: 600; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; padding: 6px 12px 6px 0; border-bottom: 1px solid var(--border); }
    table.params td { padding: 10px 12px 10px 0; border-bottom: 1px solid var(--border); vertical-align: top; }
    table.params td.type { color: var(--accent); font-family: "SF Mono", ui-monospace, monospace; font-size: 12.5px; }
    table.params .desc { color: var(--muted); font-size: 13px; margin-top: 2px; }
    table.params .req { color: var(--req); font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.05em; margin-left: 6px; }
    details.nested { margin-top: 8px; background: var(--panel-2); border: 1px solid var(--border); border-radius: 6px; padding: 6px 12px; }
    details.nested summary { cursor: pointer; color: var(--muted); font-size: 12px; }
    details.nested table.params { margin-top: 8px; }
    @media (max-width: 880px) {
      .layout { grid-template-columns: 1fr; }
      aside.sidebar { position: static; height: auto; border-right: 0; border-bottom: 1px solid var(--border); }
      main { padding: 24px; }
    }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <h1>Topline OS MCP</h1>
      <span class="tag">Tool reference</span>
      ${sidebar}
    </aside>
    <main>
      <header class="hero">
        <h1>Topline OS MCP — Tool reference</h1>
        <p>Native reference for every tool exposed by the <a href="https://github.com/Topline-com/os-mcp">Topline-com/os-mcp</a> server. Each tool is callable over MCP via <code>tools/call</code> with the arguments listed below.</p>
        <div class="stats">
          <div class="stat"><strong>${ALL_TOOLS.length}</strong> tools total</div>
          <div class="stat"><strong>${totalActions}</strong> action tools</div>
          <div class="stat"><strong>${totalAnalytics}</strong> analytics (SQL) tools</div>
          <div class="stat">Endpoint: <code>https://os-mcp.topline.com/mcp</code></div>
        </div>
      </header>
      ${main}
      <footer style="margin-top: 60px; padding-top: 20px; border-top: 1px solid var(--border); color: var(--muted); font-size: 12px;">
        Generated from <code>apps/edge/src/registry.ts</code>. To regenerate: <code>npx tsx scripts/gen-docs.ts &gt; docs/api/os-mcp/index.html</code>.
      </footer>
    </main>
  </div>
</body>
</html>
`;

process.stdout.write(html);
