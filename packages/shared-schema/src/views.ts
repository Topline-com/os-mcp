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
      "Per-contact first-inbound-call → first-outbound-callback timeline. EXACTLY ONE ROW PER CONTACT. The headline Streamlined-style metric: callback_delta_seconds between a contact's first inbound call and our first outbound call afterwards. NULL in callback columns means we never returned the call. Use this for speed-to-lead, per-rep response time, and response-rate analytics.",
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
      first_outbound_ts AS (
        -- Earliest outbound call placed AFTER the contact's first inbound.
        -- GROUP BY contact_id only, so one row per contact — prevents
        -- the multi-rep inflation bug where two reps both placing
        -- callbacks would double-count that contact.
        SELECT ce.contact_id, MIN(ce.event_at) AS first_outbound_at
          FROM call_events ce
          JOIN first_inbound fi ON fi.contact_id = ce.contact_id
         WHERE ce.direction = 'outbound'
           AND ce.event_at > fi.first_inbound_at
         GROUP BY ce.contact_id
      ),
      first_outbound AS (
        -- Attach the user_id of THAT specific first outbound call.
        -- If multiple reps dialed the contact at the exact same
        -- second (rare), pick one deterministically via MIN(user_id).
        SELECT fot.contact_id,
               fot.first_outbound_at,
               (SELECT MIN(ce.user_id)
                  FROM call_events ce
                 WHERE ce.contact_id = fot.contact_id
                   AND ce.direction = 'outbound'
                   AND ce.event_at = fot.first_outbound_at) AS first_outbound_user_id
          FROM first_outbound_ts fot
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
      JOIN first_inbound fi ON fi.contact_id = c.id
      LEFT JOIN first_outbound fo ON fo.contact_id = c.id
    `,
  },

  {
    name: "contact_timeline",
    description:
      "Unified chronological stream of everything that happened to a contact — messages (calls, SMS, email), opportunities created, and appointments scheduled. One row per event, ordered by event_at. Use this for 'show me the full history of contact X' or 'what touches happened in the 48 hours after form submission'.",
    base_tables: ["messages", "opportunities", "appointments"],
    // Rollforward 2026-05-13: appointments now ship and are exposed, so
    // the third UNION branch foreshadowed by the original DDL is added.
    // call_events is INTENTIONALLY skipped here — every call_events row
    // already has a matching messages row (call_events is dual-written
    // out of messages during sync), so unioning it would double-count
    // calls in the timeline. Drilldown into call disposition / duration
    // is still available by joining contact_timeline.source_id back to
    // call_events.id when event_subtype LIKE 'TYPE_%CALL%'.
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
      WHERE a.contact_id IS NOT NULL
        AND a.start_time IS NOT NULL
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

  {
    name: "pipeline_activity_window",
    description:
      "One row per activity touch (message, call, appointment) tied to an opportunity through its contact. Filter to a window with WHERE event_at >= ? AND event_at < ? AND pipeline_id = ?. activity_class is one of 'message' | 'call' | 'appointment'. NOTE on fan-out: when a contact has multiple opportunities in the same pipeline, a single touch maps to multiple rows (one per opportunity). Use COUNT(DISTINCT source_id) when summing unique touches across opportunities; per-deal totals (GROUP BY opportunity_id, source_id) are exact.",
    base_tables: ["opportunities", "messages", "call_events", "appointments"],
    ddl: `
      CREATE VIEW pipeline_activity_window AS
      SELECT
        o.id AS opportunity_id,
        o.location_id,
        o.contact_id,
        o.name AS opportunity_name,
        o.status AS opportunity_status,
        o.monetary_value,
        o.pipeline_id,
        o.pipeline_stage_id,
        o.assigned_to AS owner_user_id,
        'message' AS activity_class,
        m.type AS activity_subtype,
        m.direction,
        m.date_added AS event_at,
        m.id AS source_id,
        m.user_id
      FROM opportunities o
      JOIN messages m ON m.contact_id = o.contact_id
      WHERE o.contact_id IS NOT NULL
      UNION ALL
      SELECT
        o.id AS opportunity_id,
        o.location_id,
        o.contact_id,
        o.name AS opportunity_name,
        o.status AS opportunity_status,
        o.monetary_value,
        o.pipeline_id,
        o.pipeline_stage_id,
        o.assigned_to AS owner_user_id,
        'call' AS activity_class,
        ce.call_type AS activity_subtype,
        ce.direction,
        ce.event_at AS event_at,
        ce.id AS source_id,
        ce.user_id
      FROM opportunities o
      JOIN call_events ce ON ce.contact_id = o.contact_id
      WHERE o.contact_id IS NOT NULL
      UNION ALL
      SELECT
        o.id AS opportunity_id,
        o.location_id,
        o.contact_id,
        o.name AS opportunity_name,
        o.status AS opportunity_status,
        o.monetary_value,
        o.pipeline_id,
        o.pipeline_stage_id,
        o.assigned_to AS owner_user_id,
        'appointment' AS activity_class,
        a.status AS activity_subtype,
        NULL AS direction,
        a.start_time AS event_at,
        a.id AS source_id,
        a.assigned_user_id AS user_id
      FROM opportunities o
      JOIN appointments a ON a.contact_id = o.contact_id
      WHERE o.contact_id IS NOT NULL
        AND a.start_time IS NOT NULL
    `,
  },

  {
    name: "pipeline_snapshot",
    description:
      "Per-stage rollup of every pipeline: opportunity_count, pipeline_value (SUM of monetary_value), and avg_days_in_stage. One row per (pipeline_id, pipeline_stage_id, opportunity status). Filter with WHERE pipeline_id = ? AND opportunity_status = 'open' for the standard 'pipeline snapshot' shape, or GROUP BY pipeline_id for an all-pipelines rollup.",
    base_tables: ["opportunities", "pipelines", "pipeline_stages"],
    ddl: `
      CREATE VIEW pipeline_snapshot AS
      SELECT
        o.location_id,
        o.pipeline_id,
        p.name AS pipeline_name,
        o.pipeline_stage_id,
        ps.name AS stage_name,
        ps.position AS stage_position,
        o.status AS opportunity_status,
        COUNT(*) AS opportunity_count,
        COALESCE(SUM(o.monetary_value), 0) AS pipeline_value,
        CAST(AVG(
          julianday(COALESCE(o.last_stage_change_at, o.updated_at, o.created_at))
          - julianday(COALESCE(o.last_stage_change_at, o.created_at))
        ) AS REAL) AS avg_days_in_stage
      FROM opportunities o
      LEFT JOIN pipelines p ON p.id = o.pipeline_id
      LEFT JOIN pipeline_stages ps ON ps.id = o.pipeline_stage_id
      GROUP BY o.location_id, o.pipeline_id, p.name, o.pipeline_stage_id, ps.name, ps.position, o.status
    `,
  },

  {
    name: "pipeline_movement_window",
    description:
      "One row per opportunity with stage/status/record movement timestamps surfaced as last_movement_at = MAX(last_stage_change_at, last_status_change_at, updated_at). Filter with WHERE last_movement_at >= ? AND last_movement_at < ? AND pipeline_id = ? to find moved deals in a window. Includes pipeline_name and stage_name so the result is readable without further joins.",
    base_tables: ["opportunities", "pipelines", "pipeline_stages"],
    ddl: `
      CREATE VIEW pipeline_movement_window AS
      SELECT
        o.id AS opportunity_id,
        o.location_id,
        o.contact_id,
        o.name AS opportunity_name,
        o.status AS opportunity_status,
        o.monetary_value,
        o.assigned_to AS owner_user_id,
        o.pipeline_id,
        p.name AS pipeline_name,
        o.pipeline_stage_id,
        ps.name AS stage_name,
        ps.position AS stage_position,
        o.created_at,
        o.updated_at,
        o.last_status_change_at,
        o.last_stage_change_at,
        COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at) AS last_movement_at,
        CASE
          WHEN o.last_stage_change_at IS NOT NULL
           AND (o.last_status_change_at IS NULL OR o.last_stage_change_at >= o.last_status_change_at)
          THEN 'stage_change'
          WHEN o.last_status_change_at IS NOT NULL THEN 'status_change'
          WHEN o.updated_at IS NOT NULL THEN 'record_update'
          ELSE NULL
        END AS last_movement_kind
      FROM opportunities o
      LEFT JOIN pipelines p ON p.id = o.pipeline_id
      LEFT JOIN pipeline_stages ps ON ps.id = o.pipeline_stage_id
    `,
  },

  {
    name: "warehouse_freshness",
    description:
      "Per-table sync freshness snapshot for the activity-critical tables that drive pipeline audits: row_count, last_synced_at (MAX of _synced_at), and lag_seconds vs. now. Use this as a deterministic readiness probe — e.g. WHERE lag_seconds > 1800 flags tables more than 30 min behind. Scoped to the 4 activity tables on purpose: Workers DO SQLite enforces a tight SQLITE_LIMIT_COMPOUND_SELECT (CREATE VIEW with > 4 UNION ALL terms fails at warmup with 'too many terms in compound SELECT' and rolls back the entire ANALYTICS_VIEWS batch). For freshness of contacts / pipeline_stages / etc., query MAX(_synced_at) on those tables directly.",
    base_tables: [
      "opportunities",
      "messages",
      "call_events",
      "appointments",
    ],
    ddl: `
      CREATE VIEW warehouse_freshness AS
      SELECT 'opportunities' AS table_name,
             COUNT(*) AS row_count,
             MAX(_synced_at) AS last_synced_at,
             CAST((julianday('now') - julianday(MAX(_synced_at))) * 86400 AS INTEGER) AS lag_seconds
        FROM opportunities
      UNION ALL
      SELECT 'messages', COUNT(*), MAX(_synced_at),
             CAST((julianday('now') - julianday(MAX(_synced_at))) * 86400 AS INTEGER)
        FROM messages
      UNION ALL
      SELECT 'call_events', COUNT(*), MAX(_synced_at),
             CAST((julianday('now') - julianday(MAX(_synced_at))) * 86400 AS INTEGER)
        FROM call_events
      UNION ALL
      SELECT 'appointments', COUNT(*), MAX(_synced_at),
             CAST((julianday('now') - julianday(MAX(_synced_at))) * 86400 AS INTEGER)
        FROM appointments
    `,
  },
];
