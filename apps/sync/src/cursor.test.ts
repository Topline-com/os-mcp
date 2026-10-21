import { test } from "node:test";
import assert from "node:assert/strict";
import { ENTITY_BY_TABLE } from "@topline/shared-schema";
import { nextCursorFrom } from "./cursor.js";
import { responseItems } from "./response-validation.js";

test("empty pages advertising continuation cannot complete or authorize pruning", () => {
  for (const continuation of [{ nextPage: 2 }, { nextCursor: "next" }, { meta: { nextPage: 2 } }, { messages: { nextPage: 2 } }, { messages: { meta: { hasMore: true } } }]) {
    assert.throws(() => responseItems({ items: [], ...continuation }, "items"), /Incomplete/);
  }
});

test("a short page with hasMore and a missing or null cursor is incomplete", () => {
  const messages = ENTITY_BY_TABLE.get("messages")!.backfill;
  const rows = [{ id: "one" }];
  for (const response of [{ messages: { messages: rows }, hasMore: true }, { messages: { messages: rows, lastMessageId: null, hasMore: true } }, { messages: { messages: rows, lastMessageId: null }, meta: { nextPage: 2 } }]) {
    assert.throws(() => nextCursorFrom(response, rows, messages), /Incomplete/);
  }
  const contacts = ENTITY_BY_TABLE.get("contacts")!.backfill;
  assert.throws(() => nextCursorFrom({ contacts: rows, hasMore: true }, rows, contacts), /Incomplete/);
});

test("ordinary message cursor echoes on empty pages are not explicit continuation", () => {
  assert.deepEqual(responseItems({ messages: { messages: [], lastMessageId: "previous" } }, "messages.messages"), []);
  const messages = ENTITY_BY_TABLE.get("messages")!.backfill;
  assert.equal(nextCursorFrom({ messages: { lastMessageId: "previous" } }, [{ id: "previous" }], messages), "previous");
  assert.equal(nextCursorFrom({ messages: { lastMessageId: null } }, [{ id: "previous" }], messages), null);
});

test("invalid and partially missing compound cursors fail instead of completing", () => {
  const opportunities = ENTITY_BY_TABLE.get("opportunities")!.backfill;
  for (const meta of [{ startAfter: 123 }, { startAfter: {}, startAfterId: "id" }, { startAfter: 123, startAfterId: "" }]) {
    assert.throws(() => nextCursorFrom({ meta }, [{ id: "id" }], opportunities));
  }
  assert.throws(() => nextCursorFrom({}, [{ id: "id" }], opportunities), /Missing/);
  assert.deepEqual(nextCursorFrom({ meta: { startAfter: 123, startAfterId: "id" } }, [{ id: "id" }], opportunities), { startAfter: 123, startAfterId: "id" });
});
