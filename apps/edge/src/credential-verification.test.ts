import { afterEach, test } from "node:test";
import assert from "node:assert/strict";

process.env.TOPLINE_API_BASE_URL = "https://crm.example.test";

const originalFetch = globalThis.fetch;
const { verifyCredentials } = await import("./credential-verification.js");

afterEach(() => {
  globalThis.fetch = originalFetch;
  process.env.TOPLINE_API_BASE_URL = "https://crm.example.test";
});

test("credential verification uses the shared configured API base and request contract", async () => {
  let request: { input: string | URL | Request; init?: RequestInit } | undefined;
  globalThis.fetch = async (input, init) => {
    request = { input, init };
    return new Response(null, { status: 200 });
  };

  await verifyCredentials("pit-test", "location/one");

  assert.equal(String(request?.input), "https://crm.example.test/locations/location%2Fone");
  assert.deepEqual(request?.init?.headers, {
    Authorization: ["Bearer", "pit-test"].join(" "),
    Version: "2021-07-28",
  });
  assert.ok(request?.init?.signal instanceof AbortSignal);
});

test("credential verification trims pasted PIT and Location ID", async () => {
  let request: { input: string | URL | Request; init?: RequestInit } | undefined;
  globalThis.fetch = async (input, init) => {
    request = { input, init };
    return new Response(null, { status: 200 });
  };

  await verifyCredentials(" pit-test \n", " location/one ");

  assert.equal(String(request?.input), "https://crm.example.test/locations/location%2Fone");
  assert.deepEqual(request?.init?.headers, {
    Authorization: ["Bearer", "pit-test"].join(" "),
    Version: "2021-07-28",
  });
});

test("credential verification preserves the non-OK failure contract", async () => {
  globalThis.fetch = async () => new Response(null, { status: 401 });

  await assert.rejects(
    verifyCredentials("pit-test", "location-one"),
    new Error("credential_verification_failed"),
  );
});

test("credential verification fails closed when the API base is the placeholder", async () => {
  process.env.TOPLINE_API_BASE_URL = "https://api.example.com";
  let fetched = false;
  globalThis.fetch = async () => {
    fetched = true;
    return new Response(null, { status: 200 });
  };

  await assert.rejects(
    verifyCredentials("pit-test", "location-one"),
    new Error("topline_api_base_url_missing"),
  );
  assert.equal(fetched, false);
});
