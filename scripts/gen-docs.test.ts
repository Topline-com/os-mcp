import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

import { ALL_TOOLS } from "../apps/edge/src/registry.js";

test("generated reference covers the full registry and identifies v0.2.0", () => {
  const generated = spawnSync(
    process.execPath,
    ["--import", "tsx", "scripts/gen-docs.ts"],
    { cwd: process.cwd(), encoding: "utf8" },
  );

  assert.equal(generated.status, 0, generated.stderr);
  assert.match(generated.stdout, /Release v0\.2\.0/);
  assert.match(generated.stdout, /datetime="2026-08-11"/);

  for (const tool of ALL_TOOLS) {
    const occurrences = generated.stdout.split(`id="${tool.name}"`).length - 1;
    assert.equal(occurrences, 1, `${tool.name} must appear exactly once`);
  }
});

test("generated tool JSON is newline-terminated", () => {
  const generated = spawnSync(
    process.execPath,
    ["--import", "tsx", "scripts/dump-tools.ts"],
    { cwd: process.cwd(), encoding: "utf8" },
  );

  assert.equal(generated.status, 0, generated.stderr);
  assert.equal(generated.stdout.endsWith("\n"), true);
});
