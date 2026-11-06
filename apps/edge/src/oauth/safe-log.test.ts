import { test } from "node:test";
import assert from "node:assert/strict";
import { safeErrorFields, safeId } from "@topline/shared";

test("safe error fields never include arbitrary error messages", () => {
  const sensitive = "pit-secret bearer-secret customer@example.com";
  const internal = safeErrorFields(new Error(sensitive));
  assert.deepEqual(internal, { category: "internal" });
  assert.doesNotMatch(JSON.stringify(internal), /pit-|bearer|customer@/);

  const upstream = safeErrorFields({ status: 401, message: sensitive });
  assert.deepEqual(upstream, { category: "upstream", status: 401 });
  assert.doesNotMatch(JSON.stringify(upstream), /pit-|bearer|customer@/);

  const timeout = new Error(sensitive);
  timeout.name = "AbortError";
  assert.deepEqual(safeErrorFields(timeout), { category: "timeout" });
});

test("safe IDs admit connection and Location IDs and nothing token-shaped", () => {
  assert.equal(
    safeId("6f1c2a9e-4b7d-4e2a-9c1f-0a1b2c3d4e5f"),
    "6f1c2a9e-4b7d-4e2a-9c1f-0a1b2c3d4e5f",
  );
  assert.equal(safeId("abcDEF1234567"), "abcDEF1234567");
  for (const value of [
    "pit-00000000-0000-0000-0000-000000000000",
    "PIT-abc",
    "header.payload.signature",
    "location-12\u0430",
    "two words",
    "a".repeat(65),
    "",
    null,
    undefined,
  ]) {
    assert.equal(safeId(value), null, JSON.stringify(value));
  }
});
