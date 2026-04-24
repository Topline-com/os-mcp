// Derived analytics views.
//
// SQL views computed on top of the base tables. No storage cost
// (SQLite materializes on read), always consistent with the underlying
// data. The LLM sees these via describe_schema/explain_tables exactly
// like regular tables — so "show me callback time" becomes a 3-line
// SELECT against lead_response_metrics instead of a 30-line CTE
// joining messages four different ways.
//
// Rules:
//   - View definition here is the single source of truth.
//   - LocationDO drops + recreates on every DO warmup (free — it's
//     just metadata), so reshapes in code take effect next restart.
//   - Every view must work with the SELECT-only SQL gate (no PRAGMA,
//     no temp tables, no triggers).
//   - JSON extraction from raw_payload is fair game and is actually
//     the point — this is where we extract meta.call.duration,
//     call-transcript URLs, email open counts, etc.

export interface AnalyticsView {
  /** Table-like name the LLM queries. */
  name: string;
  /** One-line description surfaced by describe_schema. */
  description: string;
  /** CREATE VIEW DDL. Runs verbatim after a DROP VIEW IF EXISTS. */
  ddl: string;
  /** Underlying base tables the view touches (for describe_schema join hints). */
  base_tables: readonly string[];
}

export const ANALYTICS_VIEWS: readonly AnalyticsView[] = [
  // call_events is now a REAL synced table (see entities.ts CALL_EVENTS)
  // populated by callEventFromMessage during messages backfill. Kept out
  // of the view list because you can't CREATE VIEW over a table with the
  // same name; queries against call_events hit the typed table directly.
  {
    name: "email_events",
    description:
      "Every email message as one row, with delivery status pulled from the top-level columns. Open / click / bounce event streams live inside messages.raw_payload.meta when GHL records them — join through json_extract for those.",
    base_tables: ["messages"],
    ddl: `
      CREATE VIEW email_events AS
      SELECT
        m.id,
        m.location_id,
        m.conversation_id,
        m.contact_id,
        m.user_id,
        m.direction,
        m.status,
        m.date_added AS sent_at,
        m.body AS body_excerpt,
        json_extract(m.raw_payload, '$.meta.email.subject') AS subject,
        json_extract(m.raw_payload, '$.meta.email.from') AS from_address,
        json_extract(m.raw_payload, '$.meta.email.to') AS to_addresses
      FROM messages m
      WHERE m.type IN ('TYPE_EMAIL', 'TYPE_CUSTOM_EMAIL')
    `,
  },

  {
    name: "sms_events",
    description:
      "SMS messages one row each. Delivery status transitions (queued → sent → delivered / failed) are in messages.status.",
    base_tables: ["messages"],
    ddl: `
      CREATE VIEW sms_events AS
      SELECT
        m.id,
        m.location_id,
        m.conversation_id,
        m.contact_id,
        m.user_id,
        m.direction,
        m.status,
        m.date_added AS sent_at,
        m.body
      FROM messages m
      WHERE m.type IN ('TYPE_SMS', 'TYPE_SMS_REVIEW_REQUEST')
    `,
  },

  {
    name: "lead_response_metrics",
    description:
      "Per-contact first-inbound-call → first-outbound-callback timeline. The headline Streamlined-style metric: callback_delta_seconds between a contact's first inbound call and our first outbound call afterwards. NULL in callback columns means we never returned the call. Use this for speed-to-lead, per-rep response time, and response-rate analytics. Built atop call_events (typed table), not raw messages.",
    base_tables: ["call_events", "contacts"],
    ddl: `
      CREATE VIEW lead_response_metrics AS
      WITH first_inbound AS (
        SELECT contact_id,
               MIN(event_at) AS first_inbound_at,
               MIN(CASE WHEN COALESCE(duration_seconds, 0) > 5 THEN event_at END) AS first_real_inbound_at
          FROM call_events
         WHERE direction = 'inbound'
         GROUP BY contact_id
      ),
      first_outbound AS (
        SELECT ce.contact_id,
               MIN(ce.event_at) AS first_outbound_at,
               ce.user_id AS first_outbound_user_id
          FROM call_events ce
          JOIN first_inbound fi ON fi.contact_id = ce.contact_id
         WHERE ce.direction = 'outbound'
           AND ce.event_at > fi.first_inbound_at
         GROUP BY ce.contact_id, ce.user_id
      )
      SELECT
        c.id AS contact_id,
        c.location_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.assigned_to AS assigned_user_id,
        fi.first_inbound_at,
        fi.first_real_inbound_at,
        fo.first_outbound_at,
        fo.first_outbound_user_id,
        CASE
          WHEN fo.first_outbound_at IS NOT NULL AND fi.first_inbound_at IS NOT NULL
          THEN (strftime('%s', fo.first_outbound_at) - strftime('%s', fi.first_inbound_at))
          ELSE NULL
        END AS callback_delta_seconds,
        CASE WHEN fo.first_outbound_at IS NOT NULL THEN 1 ELSE 0 END AS called_back
      FROM contacts c
      LEFT JOIN first_inbound fi ON fi.contact_id = c.id
      LEFT JOIN first_outbound fo ON fo.contact_id = c.id
      WHERE fi.first_inbound_at IS NOT NULL
    `,
  },

  {
    name: "contact_timeline",
    description:
      "Unified chronological stream of everything that happened to a contact — messages (calls, SMS, email), opportunities created/changed, appointments booked. One row per event, ordered by event_at. Use this for 'show me the full history of contact X' or 'what touches happened in the 48 hours after form submission'.",
    base_tables: ["messages", "opportunities", "appointments"],
    ddl: `
      CREATE VIEW contact_timeline AS
      SELECT
        m.contact_id,
        m.location_id,
        'message' AS event_kind,
        m.type AS event_subtype,
        m.direction,
        m.date_added AS event_at,
        m.body AS summary,
        m.id AS source_id,
        m.user_id
      FROM messages m
      UNION ALL
      SELECT
        o.contact_id,
        o.location_id,
        'opportunity' AS event_kind,
        o.status AS event_subtype,
        NULL AS direction,
        o.created_at AS event_at,
        o.name AS summary,
        o.id AS source_id,
        o.assigned_to AS user_id
      FROM opportunities o
      WHERE o.created_at IS NOT NULL
      UNION ALL
      SELECT
        a.contact_id,
        a.location_id,
        'appointment' AS event_kind,
        a.status AS event_subtype,
        NULL AS direction,
        a.start_time AS event_at,
        a.title AS summary,
        a.id AS source_id,
        a.assigned_user_id AS user_id
      FROM appointments a
      WHERE a.start_time IS NOT NULL
    `,
  },

  {
    name: "opportunity_funnel",
    description:
      "Opportunities enriched with pipeline / stage names and the elapsed time in each status. Useful for 'which stage are deals stalling in', 'win-rate by pipeline', 'average days-to-close'.",
    base_tables: ["opportunities", "pipelines", "pipeline_stages"],
    ddl: `
      CREATE VIEW opportunity_funnel AS
      SELECT
        o.id,
        o.location_id,
        o.contact_id,
        o.name,
        o.status,
        o.monetary_value,
        o.assigned_to AS assigned_user_id,
        o.source,
        o.created_at,
        o.updated_at,
        o.last_status_change_at,
        o.last_stage_change_at,
        p.name AS pipeline_name,
        ps.name AS stage_name,
        CAST((julianday(COALESCE(o.last_status_change_at, o.updated_at)) - julianday(o.created_at)) AS REAL) AS days_since_created,
        CAST((julianday(COALESCE(o.last_stage_change_at, o.updated_at)) - julianday(COALESCE(o.last_stage_change_at, o.created_at))) AS REAL) AS days_in_current_stage
      FROM opportunities o
      LEFT JOIN pipelines p ON p.id = o.pipeline_id
      LEFT JOIN pipeline_stages ps ON ps.id = o.pipeline_stage_id
    `,
  },
];
