import { test } from "node:test";
import assert from "node:assert/strict";
import { safeErrorFields } from "@topline/shared";

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