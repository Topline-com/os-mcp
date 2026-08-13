import { describe, it } from "node:test";
import { deepStrictEqual, match, strictEqual, throws } from "node:assert";

import { ALL_TOOLS } from "./registry.js";
import { authorizeFormHtml } from "./remote-oauth.js";
import { PolicyReauthorizationRequiredError } from "./tool-policy.js";
import {
  buildToolSelectionView,
  compilePolicyUpdate,
  parseToolSelectionForm,
} from "./tool-selection-view.js";

describe("connection tool-selection UX", () => {
  it("classifies bearer attempts to add a hidden tool as reauthorization-required", () => {
    throws(
      () =>
        compilePolicyUpdate(
          { kind: "custom", tool_ids: ["topline_ping", "topline_get_contact"] },
          "generic",
          "generic",
          ALL_TOOLS,
          { version: 1, mode: "allow", tool_ids: ["topline_ping"] },
        ),
      (error: unknown) => error instanceof PolicyReauthorizationRequiredError,
    );
  });

  it("shows every preset, exact selected counts, consequences, and custom selection", () => {
    const view = buildToolSelectionView(ALL_TOOLS);
    const html = authorizeFormHtml({
      brand: "Topline OS",
      continuation: "opaque-continuation",
      csrf: "opaque-csrf",
      scriptNonce: "test-nonce",
      toolSelection: view,
    });

    strictEqual(view.default_preset, "read_only_crm");
    strictEqual(view.presets.find((preset) => preset.id === "all")?.count, ALL_TOOLS.length);
    match(html, /Tools this connection can use/);
    match(html, /The server advertises and accepts only the selected tools/);
    match(html, /Adding tools later requires a new authorization/);
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
      tool_ids: ["topline_execute_query", "topline_ping"],
    });
  });

  it("retains the saved Copilot target when an update omits it", () => {
    const registry = Array.from({ length: 129 }, (_, index) => ({
      ...ALL_TOOLS[0],
      name: `topline_fixture_${index}`,
    }));

    throws(
      () =>
        compilePolicyUpdate(
          { kind: "all" },
          undefined,
          "copilot_studio",
          registry,
          { version: 1, mode: "all" },
        ),
      /at most 128 tools/,
    );
    strictEqual(
      compilePolicyUpdate(
        { kind: "all" },
        undefined,
        "generic",
        registry,
        { version: 1, mode: "all" },
      ).target,
      "generic",
    );
  });
});
