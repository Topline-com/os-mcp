import { describe, it, beforeEach } from "node:test";
import { equal, deepEqual, ok } from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { ANALYTICS_VIEWS } from "./views.js";

let db: DatabaseSync;

function viewDDL(name: string): string {
  const view = ANALYTICS_VIEWS.find((v) => v.name === name);
  if (!view) throw new Error(`missing view ${name}`);
  return view.ddl;
}

function initSchema(): void {
  db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE contacts (
      id TEXT PRIMARY KEY,
      location_id TEXT,
      first_name TEXT,
      last_name TEXT,
      email TEXT,
      phone TEXT,
      assigned_to TEXT
    );
    CREATE TABLE messages (
      id TEXT PRIMARY KEY,
      location_id TEXT,
      conversation_id TEXT,
      contact_id TEXT,
      user_id TEXT,
      type TEXT,
      direction TEXT,
      status TEXT,
      date_added TEXT,
      body TEXT,
      raw_payload TEXT,
      _synced_at TEXT
    );
    CREATE TABLE call_events (
      id TEXT PRIMARY KEY,
      location_id TEXT,
      message_id TEXT,
      conversation_id TEXT,
      contact_id TEXT,
      direction TEXT,
      call_type TEXT,
      event_at TEXT,
      status TEXT,
      call_status TEXT,
      duration_seconds REAL,
      recording_url TEXT,
      transcription_url TEXT,
      voicemail INTEGER,
      missed INTEGER,
      user_id TEXT,
      from_number TEXT,
      to_number TEXT,
      raw_payload TEXT,
      _synced_at TEXT
    );
    CREATE TABLE opportunities (
      id TEXT PRIMARY KEY,
      location_id TEXT,
      contact_id TEXT,
      name TEXT,
      status TEXT,
      monetary_value REAL,
      assigned_to TEXT,
      source TEXT,
      pipeline_id TEXT,
      pipeline_stage_id TEXT,
      created_at TEXT,
      updated_at TEXT,
      last_status_change_at TEXT,
      last_stage_change_at TEXT,
      _synced_at TEXT
    );
    CREATE TABLE pipelines (
      id TEXT PRIMARY KEY,
      name TEXT
    );
    CREATE TABLE pipeline_stages (
      id TEXT PRIMARY KEY,
      pipeline_id TEXT,
      name TEXT,
      position INTEGER
    );
    CREATE TABLE appointments (
      id TEXT PRIMARY KEY,
      location_id TEXT,
      calendar_id TEXT,
      contact_id TEXT,
      title TEXT,
      status TEXT,
      start_time TEXT,
      assigned_user_id TEXT,
      _synced_at TEXT
    );
  `);
}

beforeEach(initSchema);

describe("analytics view DDL", () => {
  it("creates every view against the expected base-table columns", () => {
    for (const view of ANALYTICS_VIEWS) {
      db.exec(view.ddl);
    }
  });

  it("pipeline_activity_window does not double-count call messages as both message and call", () => {
    db.exec(viewDDL("pipeline_activity_window"));
    db.prepare(`INSERT INTO opportunities(id, location_id, contact_id, name, status, monetary_value, pipeline_id, pipeline_stage_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "opp_1",
      "loc_1",
      "contact_1",
      "Test Deal",
      "open",
      100,
      "pipe_1",
      "stage_1",
    );
    db.prepare(`INSERT INTO messages(id, location_id, conversation_id, contact_id, user_id, type, direction, status, date_added, body, _synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "msg_sms",
      "loc_1",
      "conv_1",
      "contact_1",
      "user_1",
      "TYPE_SMS",
      "outbound",
      "sent",
      "2026-05-13T12:00:00Z",
      "hello",
      "2026-05-13T12:01:00Z",
    );
    db.prepare(`INSERT INTO messages(id, location_id, conversation_id, contact_id, user_id, type, direction, status, date_added, body, _synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "msg_call",
      "loc_1",
      "conv_1",
      "contact_1",
      "user_1",
      "TYPE_CALL",
      "outbound",
      "completed",
      "2026-05-13T12:05:00Z",
      "call",
      "2026-05-13T12:06:00Z",
    );
    db.prepare(`INSERT INTO call_events(id, location_id, message_id, conversation_id, contact_id, direction, call_type, event_at, user_id, _synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "msg_call",
      "loc_1",
      "msg_call",
      "conv_1",
      "contact_1",
      "outbound",
      "TYPE_CALL",
      "2026-05-13T12:05:00Z",
      "user_1",
      "2026-05-13T12:06:00Z",
    );
    db.prepare(`INSERT INTO appointments(id, location_id, contact_id, title, status, start_time, assigned_user_id, _synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "appt_1",
      "loc_1",
      "contact_1",
      "Discovery",
      "confirmed",
      "2026-05-13T13:00:00Z",
      "user_1",
      "2026-05-13T12:10:00Z",
    );

    const rawRows = db.prepare(`SELECT activity_class, COUNT(DISTINCT source_id) AS n FROM pipeline_activity_window GROUP BY activity_class ORDER BY activity_class`).all() as Array<{ activity_class: string; n: number }>;
    const rows = rawRows.map((r) => ({ activity_class: r.activity_class, n: r.n }));
    deepEqual(rows, [
      { activity_class: "appointment", n: 1 },
      { activity_class: "call", n: 1 },
      { activity_class: "message", n: 1 },
    ]);
  });

  it("pipeline_snapshot reports non-zero days in current stage", () => {
    db.exec(viewDDL("pipeline_snapshot"));
    db.prepare(`INSERT INTO pipelines(id, name) VALUES (?, ?)`).run("pipe_1", "Pipeline");
    db.prepare(`INSERT INTO pipeline_stages(id, pipeline_id, name, position) VALUES (?, ?, ?, ?)`).run("stage_1", "pipe_1", "Qualified", 1);
    db.prepare(`INSERT INTO opportunities(id, location_id, contact_id, name, status, monetary_value, pipeline_id, pipeline_stage_id, created_at, updated_at, last_stage_change_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "opp_1",
      "loc_1",
      "contact_1",
      "Test Deal",
      "open",
      100,
      "pipe_1",
      "stage_1",
      "2026-01-01T00:00:00Z",
      "2026-05-13T00:00:00Z",
      "2026-05-01T00:00:00Z",
    );

    const row = db.prepare(`SELECT avg_days_in_stage FROM pipeline_snapshot WHERE pipeline_id = 'pipe_1'`).get() as { avg_days_in_stage: number };
    ok(row.avg_days_in_stage > 0, `expected positive days in stage, got ${row.avg_days_in_stage}`);
  });

  it("pipeline_movement_window classifies the latest movement timestamp, not the first non-null field", () => {
    db.exec(viewDDL("pipeline_movement_window"));
    db.prepare(`INSERT INTO pipelines(id, name) VALUES (?, ?)`).run("pipe_1", "Pipeline");
    db.prepare(`INSERT INTO pipeline_stages(id, pipeline_id, name, position) VALUES (?, ?, ?, ?)`).run("stage_1", "pipe_1", "Qualified", 1);
    db.prepare(`INSERT INTO opportunities(id, location_id, contact_id, name, status, monetary_value, pipeline_id, pipeline_stage_id, created_at, updated_at, last_status_change_at, last_stage_change_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "opp_1",
      "loc_1",
      "contact_1",
      "Test Deal",
      "open",
      100,
      "pipe_1",
      "stage_1",
      "2026-04-01T00:00:00Z",
      "2026-05-13T12:00:00Z",
      "2026-05-12T12:00:00Z",
      "2026-05-11T12:00:00Z",
    );

    const row = db.prepare(`SELECT last_movement_at, last_movement_kind FROM pipeline_movement_window WHERE opportunity_id = 'opp_1'`).get() as { last_movement_at: string; last_movement_kind: string };
    equal(row.last_movement_at, "2026-05-13T12:00:00Z");
    equal(row.last_movement_kind, "record_update");
  });
});
