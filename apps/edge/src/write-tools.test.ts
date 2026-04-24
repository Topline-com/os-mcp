// Shape regression tests for the write-tool surface.
//
// These tests prove each write tool stays registered and carries the
// right request shape (method, path template, body construction).
// They monkey-patch global fetch to intercept the outbound call and
// assert what got sent — no live GHL needed.
//
// What they don't test: whether GHL accepts the body. That's the job
// of the live-probe step + the smoke test against the test sub-account.
// Here we only guard against refactors that accidentally change the
// path, method, or body shape.

import { describe, it, before, after, beforeEach } from "node:test";
import { strictEqual, ok, deepStrictEqual } from "node:assert";
import { credentialsContext } from "@topline/shared";

// Pin a fake PIT so requirePit() doesn't throw inside toplineFetch.
// credentialsContext is an AsyncLocalStorage — wrap each test in .run().
async function withFakeCreds<T>(fn: () => Promise<T>): Promise<T> {
  return credentialsContext.run(
    { pit: "pit-fake-for-tests", locationId: "loc-fake-for-tests" },
    fn,
  );
}

// Capture of the last fetch call. Each test clears then runs a handler.
interface FetchCall {
  url: string;
  method: string;
  body: unknown | null;
  headers: Record<string, string>;
}

let lastCall: FetchCall | null = null;
const realFetch = globalThis.fetch;

function installFetchStub(responseBody: unknown = { ok: true }, status = 200): void {
  globalThis.fetch = ((url: string | URL | Request, init?: RequestInit) => {
    const req = new Request(url as RequestInfo, init);
    lastCall = {
      url: req.url,
      method: req.method,
      body: init?.body ? JSON.parse(String(init.body)) : null,
      headers: Object.fromEntries(
        Array.from(
          new Headers(init?.headers as HeadersInit).entries(),
        ) as Array<[string, string]>,
      ),
    };
    return Promise.resolve(
      new Response(JSON.stringify(responseBody), {
        status,
        headers: { "Content-Type": "application/json" },
      }),
    );
  }) as typeof fetch;
}

before(() => installFetchStub());
after(() => {
  globalThis.fetch = realFetch;
});
beforeEach(() => {
  lastCall = null;
  installFetchStub();
});

// ---------------------------------------------------------------------------
// Custom fields
// ---------------------------------------------------------------------------

describe("custom_fields write tools", () => {
  it("topline_create_custom_field POSTs to /locations/{loc}/customFields with body shape", async () => {
    const { tools } = await import("./tools/custom_fields.js");
    const tool = tools.find((t) => t.name === "topline_create_custom_field");
    ok(tool, "tool must be registered");

    await withFakeCreds(() =>
      tool!.handler({
        name: "Deal Priority",
        dataType: "SINGLE_OPTIONS",
        model: "contact",
        options: [
          { label: "High", value: "high" },
          { label: "Low", value: "low" },
        ],
        locationId: "loc-xyz",
      }),
    );

    ok(lastCall, "fetch must have been called");
    strictEqual(lastCall!.method, "POST");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/customFields"));
    deepStrictEqual(lastCall!.body, {
      name: "Deal Priority",
      dataType: "SINGLE_OPTIONS",
      model: "contact",
      options: [
        { label: "High", value: "high" },
        { label: "Low", value: "low" },
      ],
    });
  });

  it("topline_update_custom_field PUTs to /locations/{loc}/customFields/{id}", async () => {
    const { tools } = await import("./tools/custom_fields.js");
    const tool = tools.find((t) => t.name === "topline_update_custom_field")!;
    await withFakeCreds(() =>
      tool.handler({
        customFieldId: "cf1",
        name: "Renamed",
        locationId: "loc-xyz",
      }),
    );
    strictEqual(lastCall!.method, "PUT");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/customFields/cf1"));
    deepStrictEqual(lastCall!.body, { name: "Renamed" });
  });

  it("topline_delete_custom_field DELETEs the right path", async () => {
    const { tools } = await import("./tools/custom_fields.js");
    const tool = tools.find((t) => t.name === "topline_delete_custom_field")!;
    await withFakeCreds(() =>
      tool.handler({ customFieldId: "cf1", locationId: "loc-xyz" }),
    );
    strictEqual(lastCall!.method, "DELETE");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/customFields/cf1"));
    strictEqual(lastCall!.body, null);
  });
});

// ---------------------------------------------------------------------------
// Custom values
// ---------------------------------------------------------------------------

describe("custom_values write tools", () => {
  it("topline_create_custom_value POSTs name+value, drops locationId from body", async () => {
    const { tools } = await import("./tools/custom_values.js");
    const tool = tools.find((t) => t.name === "topline_create_custom_value")!;
    await withFakeCreds(() =>
      tool.handler({
        name: "Review Link",
        value: "https://example.com/r",
        locationId: "loc-xyz",
      }),
    );
    strictEqual(lastCall!.method, "POST");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/customValues"));
    deepStrictEqual(lastCall!.body, {
      name: "Review Link",
      value: "https://example.com/r",
    });
    ok(!("locationId" in (lastCall!.body as Record<string, unknown>)));
  });

  it("topline_update_custom_value PUTs with body excluding customValueId + locationId", async () => {
    const { tools } = await import("./tools/custom_values.js");
    const tool = tools.find((t) => t.name === "topline_update_custom_value")!;
    await withFakeCreds(() =>
      tool.handler({
        customValueId: "cv1",
        value: "new",
        locationId: "loc-xyz",
      }),
    );
    strictEqual(lastCall!.method, "PUT");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/customValues/cv1"));
    deepStrictEqual(lastCall!.body, { value: "new" });
  });
});

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

describe("tags write tools", () => {
  it("topline_create_tag POSTs { name }", async () => {
    const { tools } = await import("./tools/tags.js");
    const tool = tools.find((t) => t.name === "topline_create_tag")!;
    await withFakeCreds(() =>
      tool.handler({ name: "VIP", locationId: "loc-xyz" }),
    );
    strictEqual(lastCall!.method, "POST");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/tags"));
    deepStrictEqual(lastCall!.body, { name: "VIP" });
  });

  it("topline_delete_tag DELETEs the right path", async () => {
    const { tools } = await import("./tools/tags.js");
    const tool = tools.find((t) => t.name === "topline_delete_tag")!;
    await withFakeCreds(() =>
      tool.handler({ tagId: "t1", locationId: "loc-xyz" }),
    );
    strictEqual(lastCall!.method, "DELETE");
    ok(lastCall!.url.endsWith("/locations/loc-xyz/tags/t1"));
  });
});

// ---------------------------------------------------------------------------
// Calendars
// ---------------------------------------------------------------------------

describe("calendars write tools", () => {
  it("topline_update_calendar PUTs without creating (POST is requires_oauth)", async () => {
    const { tools } = await import("./tools/calendars.js");
    const tool = tools.find((t) => t.name === "topline_update_calendar")!;
    await withFakeCreds(() =>
      tool.handler({
        calendarId: "cal1",
        name: "Renamed",
        slotDuration: 30,
      }),
    );
    strictEqual(lastCall!.method, "PUT");
    ok(lastCall!.url.endsWith("/calendars/cal1"));
    deepStrictEqual(lastCall!.body, { name: "Renamed", slotDuration: 30 });
  });

  it("topline_delete_calendar DELETEs", async () => {
    const { tools } = await import("./tools/calendars.js");
    const tool = tools.find((t) => t.name === "topline_delete_calendar")!;
    await withFakeCreds(() => tool.handler({ calendarId: "cal1" }));
    strictEqual(lastCall!.method, "DELETE");
    ok(lastCall!.url.endsWith("/calendars/cal1"));
  });

  it("does NOT register topline_create_calendar (POST requires_oauth)", async () => {
    const { tools } = await import("./tools/calendars.js");
    const creators = tools.filter((t) => t.name === "topline_create_calendar");
    strictEqual(creators.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Registry sanity — confirm the new tools are wired into ACTION_TOOLS
// ---------------------------------------------------------------------------

describe("registry wiring", () => {
  it("exposes all the new write tools via ACTION_TOOLS", async () => {
    const { ACTION_TOOLS } = await import("./registry.js");
    const names = new Set(ACTION_TOOLS.map((t) => t.name));
    const required = [
      "topline_create_custom_field",
      "topline_update_custom_field",
      "topline_delete_custom_field",
      "topline_list_custom_values",
      "topline_create_custom_value",
      "topline_update_custom_value",
      "topline_delete_custom_value",
      "topline_create_tag",
      "topline_update_tag",
      "topline_delete_tag",
      "topline_update_calendar",
      "topline_delete_calendar",
    ];
    for (const n of required) {
      ok(names.has(n), `ACTION_TOOLS must include ${n}`);
    }
  });

  it("exposes topline_find_references via ANALYTICS_TOOLS (needs edge context)", async () => {
    const { ANALYTICS_TOOLS } = await import("./registry.js");
    const names = new Set(ANALYTICS_TOOLS.map((t) => t.name));
    ok(names.has("topline_find_references"));
  });
});

// ---------------------------------------------------------------------------
// find_references — closed-enum rejection of bad kinds
// ---------------------------------------------------------------------------

describe("topline_find_references", () => {
  it("rejects unsupported kinds (e.g. 'workflow') with a clear error", async () => {
    const { tools } = await import("./tools/references.js");
    const tool = tools.find((t) => t.name === "topline_find_references")!;
    let err: unknown;
    try {
      await tool.handler({ kind: "workflow", id: "wf1" });
    } catch (e) {
      err = e;
    }
    ok(err instanceof Error);
    ok(
      (err as Error).message.includes("workflow"),
      "error message should explain the workflow gap",
    );
  });
});
