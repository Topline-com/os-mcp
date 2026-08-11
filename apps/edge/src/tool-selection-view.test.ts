import { describe, it } from "node:test";
import { deepStrictEqual, match, strictEqual, throws } from "node:assert";

import { ALL_TOOLS } from "./registry.js";
import { authorizeFormHtml } from "./remote-oauth.js";
import {
  buildToolSelectionView,
  compilePolicyUpdate,
  parseToolSelectionForm,
} from "./tool-selection-view.js";

describe("connection tool-selection UX", () => {
  it("shows every preset, exact selected counts, consequences, and custom selection", () => {
    const view = buildToolSelectionView(ALL_TOOLS);
    const html = authorizeFormHtml({
      brand: "Topline OS",
      redirect_uri: "https://client.example/callback",
      code_challenge: "challenge",
      code_challenge_method: "S256",
      state: "state",
      client_id: "client",
      toolSelection: view,
    });

    strictEqual(view.default_preset, "read_only_crm");
    strictEqual(view.presets.find((preset) => preset.id === "all")?.count, ALL_TOOLS.length);
    match(html, /Tools this connection can use/);
    match(html, /The server advertises and accepts only the selected tools/);
    match(html, /Custom selection/);
    match(html, /Review selected tool IDs/);
    match(html, /Copilot Studio supports at most 128 tools/);
    match(html, /Microsoft recommends 25–30 tools/);
  });

  it("parses a custom selection without trusting client-provided counts", () => {
    const form = new FormData();
    form.set("toolPreset", "custom");
    form.set("targetClient", "copilot_studio");
    form.append("toolIds", "topline_execute_query");
    form.append("toolIds", "topline_ping");

    const parsed = parseToolSelectionForm(form, ALL_TOOLS);
    strictEqual(parsed.target, "copilot_studio");
    strictEqual(parsed.selected_count, 2);
    deepStrictEqual(parsed.policy, {
      version: 1,
      mode: "allow",
      tool_ids: ["topline_ping", "topline_execute_query"],
    });
  });

  it("retains the saved Copilot target when an update omits it", () => {
    const registry = Array.from({ length: 129 }, (_, index) => ({
      ...ALL_TOOLS[0],
      name: `topline_fixture_${index}`,
    }));

    throws(
      () => compilePolicyUpdate({ kind: "all" }, undefined, "copilot_studio", registry),
      /at most 128 tools/,
    );
    strictEqual(
      compilePolicyUpdate({ kind: "all" }, undefined, "generic", registry).target,
      "generic",
    );
  });
});
