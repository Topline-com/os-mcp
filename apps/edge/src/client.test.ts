import { test } from "node:test";
import assert from "node:assert/strict";

process.env.TOPLINE_API_BASE_URL = "https://crm.example.test";

const { toplineApiUrl } = await import("@topline/shared");

test("toplineApiUrl uses the configured shared API base", () => {
  assert.equal(
    toplineApiUrl("/locations/location%2Fone"),
    "https://crm.example.test/locations/location%2Fone",
  );
});
