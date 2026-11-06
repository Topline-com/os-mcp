import { test } from "node:test";
import assert from "node:assert/strict";

process.env.TOPLINE_API_BASE_URL = "https://crm.example.test";

const { credentialsContext, toplineApiUrl, toplineFetch } = await import("@topline/shared");

// A made-up token, never a real one: "\u0430" is the Cyrillic letter that looks like a Latin "a".
const CYRILLIC_PIT = "pit-00000000-0000-0000-0000-00000000000\u0430";

test("toplineApiUrl uses the configured shared API base", () => {
  assert.equal(
    toplineApiUrl("/locations/location%2Fone"),
    "https://crm.example.test/locations/location%2Fone",
  );
});

test("toplineFetch refuses a PIT with a non-ASCII character before it builds a header", async () => {
  const originalFetch = globalThis.fetch;
  let fetched = false;
  globalThis.fetch = async () => {
    fetched = true;
    return new Response("{}", { status: 200 });
  };
  try {
    await assert.rejects(
      credentialsContext.run(
        { pit: CYRILLIC_PIT, locationId: "location-one" },
        () => toplineFetch("/contacts/"),
      ),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /has a character that can't be part of a token/);
        assert.doesNotMatch(error.message, /pit-0|\u0430/);
        return true;
      },
    );
    assert.equal(fetched, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
