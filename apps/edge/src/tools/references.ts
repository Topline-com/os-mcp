// topline_find_references — generic "what uses X?" lookup.
//
// Backed entirely by the existing SQLite warehouse in the tenant's
// LocationDO — no new sync work, no new endpoints. Accepts a closed
// enum of kinds + an id, dispatches to a per-kind SELECT that uses
// json_extract / json_each over raw_payload + typed columns, returns
// a uniform {kind, id, references: [...]} shape.
//
// The closed enum is the security model: the LLM cannot smuggle
// arbitrary SQL through the handler. Every query is hard-coded.
//
// Workflows are NOT a supported kind because GHL's public API doesn't
// expose workflow internals — we cannot know which workflow references
// a given tag / field / calendar. That's documented in catalog.ts.

import { peekLocationId, str, obj } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { edgeContext } from "../request-context.js";
import { locationClient } from "../location-do-client.js";

const SUPPORTED_KINDS = [
  "tag",
  "custom_field",
  "custom_value",
  "pipeline",
  "pipeline_stage",
  "calendar",
  "user",
  "contact",
  "opportunity",
  "form",
  "survey",
] as const;
type Kind = (typeof SUPPORTED_KINDS)[number];

interface ReferenceHit {
  /** Base table the reference was found in. */
  table: string;
  /** Primary key of the row holding the reference. */
  row_id: string;
  /**
   * One-line human-readable summary of the row (e.g. the contact's
   * email, the opportunity's name, the appointment's title).
   */
  summary: string;
  /** Optional extra fields that may help disambiguate. */
  extra?: Record<string, unknown>;
}

function getClient() {
  const locId = peekLocationId();
  if (!locId) {
    throw new Error(
      "No location in request context. topline_find_references needs an authenticated connection.",
    );
  }
  const ctx = edgeContext.getStore();
  if (!ctx) {
    throw new Error("Edge request context not set.");
  }
  return locationClient(ctx.location_do, locId);
}

/**
 * Per-kind query dispatcher. Returns ONE OR MORE {sql, params} pairs.
 * Cloudflare's DO SQLite caps compound-SELECT chain depth aggressively
 * (the default SQLite limit is 500 but Workers runs with a much
 * smaller cap — a 7-way UNION ALL already errors "too many terms in
 * compound SELECT"). Kinds that span many tables return multiple
 * queries and the handler concats their rows.
 *
 * Identifiers inside the SQL are hard-coded (no caller interpolation);
 * only the search value is parameterized. Every query returns
 * (table, row_id, summary, extra) so the handler can shape the
 * uniform response without per-kind post-processing.
 */
function queryFor(kind: Kind, id: string): Array<{ sql: string; params: unknown[] }> {
  const wrap = (q: { sql: string; params: unknown[] }) => [q];
  switch (kind) {
    case "tag":
      // Tag reference = contact whose tags[] JSON array contains the
      // tag NAME. We match by name because that's how contacts.tags
      // stores them (not as ids). Caller passes either the tag id or
      // its name — we look up the name via the tags table first.
      return wrap({
        sql: `
          WITH target AS (
            SELECT name FROM tags WHERE id = ? OR name = ? LIMIT 1
          )
          SELECT 'contacts' AS "table",
                 c.id AS row_id,
                 COALESCE(c.email, c.phone, c.first_name || ' ' || c.last_name, c.id) AS summary,
                 json_object('tags', c.tags) AS extra
            FROM contacts c, target, json_each(c.tags) je
           WHERE je.value = target.name
           LIMIT 500
        `,
        params: [id, id],
      });
    case "custom_field":
      // custom_fields values live on contacts.custom_fields as a JSON
      // array of {id, value} — match by the field id.
      return wrap({
        sql: `
          SELECT 'contacts' AS "table",
                 c.id AS row_id,
                 COALESCE(c.email, c.phone, c.first_name || ' ' || c.last_name, c.id) AS summary,
                 json_object('value', json_extract(je.value, '$.value')) AS extra
            FROM contacts c, json_each(c.custom_fields) je
           WHERE json_extract(je.value, '$.id') = ?
           LIMIT 500
        `,
        params: [id],
      });
    case "custom_value":
      // Sub-account custom_values are shared merge-tag key/value pairs.
      // They aren't referenced from contact rows; the "references" are
      // in workflow/message template bodies — which we don't sync. Best
      // we can do is return the value itself.
      return wrap({
        sql: `
          SELECT 'custom_values' AS "table",
                 id AS row_id,
                 COALESCE(name, id) AS summary,
                 json_object('field_key', field_key, 'value', value) AS extra
            FROM custom_values
           WHERE id = ?
           LIMIT 1
        `,
        params: [id],
      });
    case "pipeline":
      // Opportunities on this pipeline + the pipeline's stages.
      return wrap({
        sql: `
          SELECT 'opportunities' AS "table",
                 id AS row_id,
                 COALESCE(name, id) AS summary,
                 json_object('status', status, 'stage_id', pipeline_stage_id) AS extra
            FROM opportunities
           WHERE pipeline_id = ?
          UNION ALL
          SELECT 'pipeline_stages' AS "table",
                 id AS row_id,
                 COALESCE(name, id) AS summary,
                 json_object('position', "position") AS extra
            FROM pipeline_stages
           WHERE pipeline_id = ?
           LIMIT 500
        `,
        params: [id, id],
      });
    case "pipeline_stage":
      return wrap({
        sql: `
          SELECT 'opportunities' AS "table",
                 id AS row_id,
                 COALESCE(name, id) AS summary,
                 json_object('status', status, 'pipeline_id', pipeline_id) AS extra
            FROM opportunities
           WHERE pipeline_stage_id = ?
           LIMIT 500
        `,
        params: [id],
      });
    case "calendar":
      // Appointments on this calendar. appointments is currently hidden
      // in the customer gate, but the DO still stores it — we SELECT
      // from the exposed projection via raw_payload fallback where
      // needed. For safety we only expose the id + minimal summary.
      return wrap({
        sql: `
          SELECT 'appointments' AS "table",
                 id AS row_id,
                 COALESCE(title, id) AS summary,
                 json_object('start_time', start_time, 'status', status) AS extra
            FROM appointments
           WHERE calendar_id = ?
           LIMIT 500
        `,
        params: [id],
      });
    case "user":
      // 4-way fan-out: DO's compound-SELECT cap tolerates this in one
      // query but kept as two just-in-case splits (2+2) so adding a
      // 5th source later doesn't regress.
      return [
        {
          sql: `
            SELECT 'opportunities' AS "table",
                   id AS row_id,
                   COALESCE(name, id) AS summary,
                   json_object('status', status, 'pipeline_id', pipeline_id) AS extra
              FROM opportunities WHERE assigned_to = ?
            UNION ALL
            SELECT 'contacts' AS "table",
                   id AS row_id,
                   COALESCE(email, phone, first_name || ' ' || last_name, id) AS summary,
                   json_object('type', type) AS extra
              FROM contacts WHERE assigned_to = ?
             LIMIT 500
          `,
          params: [id, id],
        },
        {
          sql: `
            SELECT 'tasks' AS "table",
                   id AS row_id,
                   COALESCE(title, id) AS summary,
                   json_object('due_date', due_date, 'completed', completed) AS extra
              FROM tasks WHERE assigned_to = ?
            UNION ALL
            SELECT 'messages' AS "table",
                   id AS row_id,
                   COALESCE(substr(body, 1, 60), type, id) AS summary,
                   json_object('type', type, 'direction', direction, 'date_added', date_added) AS extra
              FROM messages WHERE user_id = ?
             LIMIT 500
          `,
          params: [id, id],
        },
      ];
    case "contact":
      // 7-source fan-out. DO's compound-SELECT limit rejects ≥7 UNION
      // ALL terms, so we split into 3 queries (3+2+2) and the handler
      // concats the rows. Each stays well under the cap.
      return [
        {
          sql: `
            SELECT 'opportunities' AS "table",
                   id AS row_id,
                   COALESCE(name, id) AS summary,
                   json_object('status', status, 'monetary_value', monetary_value) AS extra
              FROM opportunities WHERE contact_id = ?
            UNION ALL
            SELECT 'conversations' AS "table",
                   id AS row_id,
                   COALESCE(last_message_body, type, id) AS summary,
                   json_object('type', type, 'last_message_date', last_message_date) AS extra
              FROM conversations WHERE contact_id = ?
            UNION ALL
            SELECT 'messages' AS "table",
                   id AS row_id,
                   COALESCE(substr(body, 1, 60), type, id) AS summary,
                   json_object('type', type, 'direction', direction, 'date_added', date_added) AS extra
              FROM messages WHERE contact_id = ?
             LIMIT 500
          `,
          params: [id, id, id],
        },
        {
          sql: `
            SELECT 'appointments' AS "table",
                   id AS row_id,
                   COALESCE(title, id) AS summary,
                   json_object('start_time', start_time, 'status', status) AS extra
              FROM appointments WHERE contact_id = ?
            UNION ALL
            SELECT 'tasks' AS "table",
                   id AS row_id,
                   COALESCE(title, id) AS summary,
                   json_object('due_date', due_date, 'completed', completed) AS extra
              FROM tasks WHERE contact_id = ?
             LIMIT 500
          `,
          params: [id, id],
        },
        {
          sql: `
            SELECT 'notes' AS "table",
                   id AS row_id,
                   COALESCE(substr(body, 1, 60), id) AS summary,
                   json_object('user_id', user_id, 'created_at', created_at) AS extra
              FROM notes WHERE contact_id = ?
            UNION ALL
            SELECT 'form_submissions' AS "table",
                   id AS row_id,
                   COALESCE(email, name, id) AS summary,
                   json_object('form_id', form_id, 'created_at', created_at) AS extra
              FROM form_submissions WHERE contact_id = ?
             LIMIT 500
          `,
          params: [id, id],
        },
      ];
    case "opportunity":
      // Messages / call_events tied to the contact on this opportunity.
      // Opportunities don't have direct messages; we resolve through
      // the contact_id.
      return wrap({
        sql: `
          WITH target AS (SELECT contact_id FROM opportunities WHERE id = ?)
          SELECT 'messages' AS "table",
                 m.id AS row_id,
                 COALESCE(substr(m.body, 1, 60), m.type, m.id) AS summary,
                 json_object('type', m.type, 'direction', m.direction) AS extra
            FROM messages m, target
           WHERE m.contact_id = target.contact_id
          UNION ALL
          SELECT 'call_events' AS "table",
                 ce.id AS row_id,
                 COALESCE(ce.call_type, ce.id) AS summary,
                 json_object('direction', ce.direction, 'duration_seconds', ce.duration_seconds) AS extra
            FROM call_events ce, target
           WHERE ce.contact_id = target.contact_id
           LIMIT 500
        `,
        params: [id],
      });
    case "form":
      return wrap({
        sql: `
          SELECT 'form_submissions' AS "table",
                 id AS row_id,
                 COALESCE(email, name, id) AS summary,
                 json_object('contact_id', contact_id, 'created_at', created_at) AS extra
            FROM form_submissions
           WHERE form_id = ?
           LIMIT 500
        `,
        params: [id],
      });
    case "survey":
      return wrap({
        sql: `
          SELECT 'survey_submissions' AS "table",
                 id AS row_id,
                 COALESCE(email, name, id) AS summary,
                 json_object('contact_id', contact_id, 'created_at', created_at) AS extra
            FROM survey_submissions
           WHERE survey_id = ?
           LIMIT 500
        `,
        params: [id],
      });
  }
}

export const tools: ToolDef[] = [
  {
    name: "topline_find_references",
    description:
      "Answer 'what uses X?' across synced CRM objects. Closed-enum dispatcher — runs a hard-coded SQL query per kind; no arbitrary SQL accepted. " +
      "Supported kinds: tag, custom_field, custom_value, pipeline, pipeline_stage, calendar, user, contact, opportunity, form, survey. " +
      "WORKFLOWS are NOT supported: GHL's public API does not expose workflow internals, so we cannot know which workflows reference any given object. " +
      "Returns { kind, id, references: [{ table, row_id, summary, extra }] }, capped at 500 hits. For tags, accepts either the tag id or the tag name.",
    inputSchema: obj(
      {
        kind: { type: "string", enum: [...SUPPORTED_KINDS] },
        id: str("ID of the object to search references for (or name, for tags)"),
      },
      ["kind", "id"],
    ),
    handler: async (args) => {
      const kind = args.kind as Kind;
      const id = String(args.id);
      if (!SUPPORTED_KINDS.includes(kind)) {
        throw new Error(
          `Unsupported kind: ${kind}. Valid: ${SUPPORTED_KINDS.join(", ")}. ` +
            `For workflows, GHL's API doesn't expose internals — not queryable here.`,
        );
      }
      const queries = queryFor(kind, id);
      const client = getClient();
      // Run every query in parallel; concat rows. Cloudflare DO's
      // compound-SELECT cap forces us to split wide fan-outs (contact,
      // user) across multiple queries.
      const results = await Promise.all(
        queries.map((q) => client.executeQuery(q.sql, q.params)),
      );
      const truncated = results.some((r) => r.truncated);
      const references: ReferenceHit[] = results.flatMap((result) =>
        result.rows.map((r) => {
          const row = r as Record<string, unknown>;
          const extraRaw = row.extra;
          let extra: Record<string, unknown> | undefined;
          if (typeof extraRaw === "string") {
            try {
              extra = JSON.parse(extraRaw);
            } catch {
              extra = { raw: extraRaw };
            }
          }
          return {
            table: String(row["table"] ?? ""),
            row_id: String(row.row_id ?? ""),
            summary: String(row.summary ?? ""),
            ...(extra ? { extra } : {}),
          };
        }),
      );
      return {
        kind,
        id,
        count: references.length,
        truncated,
        references,
      };
    },
  },
];
