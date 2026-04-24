// Entity definitions for every table the data warehouse exposes.
//
// Current state: phase-1 hot entities (contacts, opportunities, conversations,
// messages, appointments) + phase-2 metadata join targets (pipelines,
// pipeline_stages). Every entity starts with `exposed: false` and `audit` all
// false until we've run the audit against a live Topline sub-account.
//
// Adding a new entity: copy an existing block, adjust columns + source
// descriptors, then flip `exposed` to true once audit.ts reports passed.

import type { EntityManifest, ColumnDef } from "./types.js";
import { auditPasses } from "./types.js";

// ---------------------------------------------------------------------------
// Shared columns — every row carries these so sync can pin by tenant and
// the LLM can group by freshness.
// ---------------------------------------------------------------------------

const LOCATION_ID: ColumnDef = {
  name: "location_id",
  sqlite_type: "TEXT",
  nullable: false,
  description: "Topline OS sub-account ID this row belongs to. Redundant with the DO tenant boundary but kept for clarity in multi-row exports.",
  indexed: true,
  source_path: "locationId",
};

const CREATED_AT: ColumnDef = {
  name: "created_at",
  sqlite_type: "TEXT",
  nullable: true,
  description: "ISO 8601 timestamp the record was first created upstream.",
  source_path: "dateAdded",
};

const UPDATED_AT: ColumnDef = {
  name: "updated_at",
  sqlite_type: "TEXT",
  nullable: true,
  description: "ISO 8601 timestamp of the most recent mutation upstream. Used as the incremental-sync cursor.",
  indexed: true,
  source_path: "dateUpdated",
};

const SYNCED_AT: ColumnDef = {
  name: "_synced_at",
  sqlite_type: "TEXT",
  nullable: false,
  description: "ISO 8601 timestamp the sync worker last wrote this row. Use to detect stale data.",
};

/**
 * Lossless raw upstream payload. Every exposed entity includes this so
 * the LLM can query fields we haven't bothered to type yet via
 * json_extract(raw_payload, '$.path.to.field'). Zero schema migrations
 * needed when a new analytics question surfaces — it's already on disk.
 *
 * Populated by mapRow's `col.raw === true` branch (writes the entire
 * upstream object), stringified by coerceForSqlite's json: true path.
 */
const RAW_PAYLOAD: ColumnDef = {
  name: "raw_payload",
  sqlite_type: "TEXT",
  nullable: true,
  json: true,
  raw: true,
  description:
    "Full upstream GHL JSON for this row. Use json_extract(raw_payload, '$.field') or json_each(raw_payload, '$.arr') for fields not yet surfaced as their own columns.",
};

// ---------------------------------------------------------------------------
// Phase 1 — hot tables (webhook + 15-min poll)
// ---------------------------------------------------------------------------

export const CONTACTS: EntityManifest = {
  table: "contacts",
  description: "Every contact in the sub-account. Core join table for opportunities, conversations, appointments, tasks, notes.",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable GHL contact ID." },
    LOCATION_ID,
    { name: "first_name", sqlite_type: "TEXT", nullable: true, description: "", source_path: "firstName" },
    { name: "last_name", sqlite_type: "TEXT", nullable: true, description: "", source_path: "lastName" },
    { name: "full_name", sqlite_type: "TEXT", nullable: true, description: "Denormalized name if set by user", source_path: "contactName" },
    { name: "email", sqlite_type: "TEXT", nullable: true, description: "Primary email. Use LOWER() for case-insensitive matches.", indexed: true },
    { name: "phone", sqlite_type: "TEXT", nullable: true, description: "Primary phone in E.164 format when available.", indexed: true },
    { name: "company_name", sqlite_type: "TEXT", nullable: true, description: "", source_path: "companyName" },
    { name: "source", sqlite_type: "TEXT", nullable: true, description: "Attribution source (e.g. 'website', 'inbound-call')." },
    { name: "assigned_to", sqlite_type: "TEXT", nullable: true, description: "User ID of the assigned owner.", references: "users.id", source_path: "assignedTo" },
    { name: "type", sqlite_type: "TEXT", nullable: true, description: "'lead' or 'customer'.", enum: ["lead", "customer"] },
    { name: "dnd", sqlite_type: "INTEGER", nullable: true, description: "Do-not-disturb flag (0 or 1)." },
    { name: "timezone", sqlite_type: "TEXT", nullable: true, description: "IANA timezone." },
    { name: "tags", sqlite_type: "TEXT", nullable: true, json: true, description: "JSON array of tag strings. Use json_each(tags) to expand." },
    { name: "custom_fields", sqlite_type: "TEXT", nullable: true, json: true, description: "JSON array of {id,value} objects. Use json_each to filter by custom field value.", source_path: "customFields" },
    { name: "address1", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "city", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "state", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "country", sqlite_type: "TEXT", nullable: true, description: "ISO 2-letter country code." },
    { name: "postal_code", sqlite_type: "TEXT", nullable: true, description: "", source_path: "postalCode" },
    CREATED_AT,
    UPDATED_AT,
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Matches apps/edge/src/tools/contacts.ts `topline_search_contacts`:
    // POST /contacts/search with body.searchAfter = [ts, lastId].
    //
    // GHL does NOT include a top-level cursor in the response. Instead
    // the last contact in the `contacts[]` array carries a
    // `searchAfter: [ms_epoch, id]` tuple that we must send BACK in
    // the next body. Probed live 2026-04-24 on ucNDNXi… — only this
    // shape advances through the 16k contacts; meta.searchAfter is
    // absent (the former manifest reading it got 0 rows past page 1).
    endpoint: "/contacts/search",
    method: "POST",
    pagination: "cursor",
    items_field: "contacts",
    cursor: {
      source: "last_item",
      fields: [
        { request_param: "searchAfter", response_path: "searchAfter", encoding: "array" },
      ],
    },
    query_extras: { pageLimit: 100 },
  },
  incremental: {
    type: "updated_after",
    cursor_column: "updated_at",
    // POST /contacts/search accepts:
    //   body.filters = [{ field: "dateUpdated", operator: "range",
    //                     value: { gte: watermark } }]
    // Verified live. `gt` and `gte` as top-level operators are rejected
    // ("Invalid Operator passed for field date_updated"); range with a
    // nested gte object is the supported pattern for date fields.
    cursor_query_param: "dateUpdated",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "ContactCreate", kind: "upsert" },
    { ghl_event: "ContactUpdate", kind: "upsert" },
    { ghl_event: "ContactDelete", kind: "delete" },
  ],
  audit: {
    // Verified live on 2026-04-23 against ucNDNXi… sub-account:
    //   backfill pulled 101 contacts with correct column mapping
    //   ON CONFLICT(id) DO UPDATE dedupes correctly
    //   range+gte filter on POST /contacts/search returns only new rows
    //   watermark advances via MAX(updated_at)
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: true,
    // webhook_coverage not required per requiredAuditChecks: phase-1
    // with updated_after + filter_ready=true uses cron+filter instead.
    notes: "Exposed via cron+range-gte-filter. Webhook-based freshness comes in phase 1 step 5.",
  },
  exposed: true,
};

export const OPPORTUNITIES: EntityManifest = {
  table: "opportunities",
  description: "Deals in sales pipelines. Join to contacts (contact_id), pipelines (pipeline_id), pipeline_stages (pipeline_stage_id).",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable GHL opportunity ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Opportunity display name." },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "pipeline_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "pipelines.id", source_path: "pipelineId" },
    { name: "pipeline_stage_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "pipeline_stages.id", source_path: "pipelineStageId" },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "Opportunity status.", enum: ["open", "won", "lost", "abandoned"] },
    { name: "monetary_value", sqlite_type: "REAL", nullable: true, description: "Deal value in location currency.", source_path: "monetaryValue" },
    { name: "assigned_to", sqlite_type: "TEXT", nullable: true, description: "", references: "users.id", source_path: "assignedTo" },
    { name: "source", sqlite_type: "TEXT", nullable: true, description: "Attribution source." },
    { name: "lost_reason_id", sqlite_type: "TEXT", nullable: true, description: "", source_path: "lostReasonId" },
    { name: "last_status_change_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp of the last status change.", source_path: "lastStatusChangeAt", timestamp_format: "iso8601" },
    { name: "last_stage_change_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp of the last pipeline-stage change.", source_path: "lastStageChangeAt", timestamp_format: "iso8601" },
    // GHL's /opportunities/search response uses camelCase createdAt / updatedAt
    // (ISO strings) — NOT the dateAdded / dateUpdated convention contacts
    // uses. Overriding the shared CREATED_AT / UPDATED_AT constants here.
    { name: "created_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp the opportunity was created upstream.", source_path: "createdAt", timestamp_format: "iso8601" },
    { name: "updated_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp of the most recent mutation upstream.", indexed: true, source_path: "updatedAt", timestamp_format: "iso8601" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Matches apps/edge/src/tools/opportunities.ts `topline_search_opportunities`:
    // GET /opportunities/search. Uses snake_case `location_id`.
    //
    // Cursor is COMPOUND — sending only startAfterId silently returns
    // the same page (probed live 2026-04-24 on ucNDNXi…). Both
    // startAfter (ms epoch) and startAfterId (row id) must go on the
    // next request for the cursor to advance.
    endpoint: "/opportunities/search",
    method: "GET",
    location_param_name: "location_id",
    pagination: "cursor",
    items_field: "opportunities",
    cursor: {
      source: "meta",
      fields: [
        { request_param: "startAfter", response_path: "startAfter" },
        { request_param: "startAfterId", response_path: "startAfterId" },
      ],
    },
    query_extras: { limit: 100 },
  },
  incremental: {
    // poll_full: GHL's /opportunities/search silently ignores every
    // date-filter query param we probed (date_updated, dateUpdated,
    // dateAdded, startAfter=<epoch_ms>) — live-tested 2026-04-23 against
    // ucNDNXi…. And PITs can't register webhooks in GHL v2 (probed the
    // /webhooks, /hooks, /locations/{id}/webhooks endpoints — all 404).
    // Every 15 min, re-pull; in steady-state the page loop walks only
    // until it hits a row whose cursor_column <= watermark (i.e. a row
    // we've already seen), so high-volume updates aren't truncated at
    // page 1.
    type: "poll_full",
    cursor_column: "updated_at",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "OpportunityCreate", kind: "upsert" },
    { ghl_event: "OpportunityUpdate", kind: "upsert" },
    { ghl_event: "OpportunityDelete", kind: "delete" },
  ],
  audit: {
    // Verified live on 2026-04-24 against ucNDNXi… sub-account:
    //   - GET /opportunities/search returns 100 rows; cursor echoes
    //     startAfterId back on over-consumption (treated as EOF).
    //   - backfillPollFull walks every page within one cron invocation.
    //   - Upserts are idempotent on `id`; re-pulling costs only the
    //     GHL API budget, never duplicate rows.
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false, // N/A for poll_full — requiredAuditChecks skips it
    notes: "Uses poll_full because (a) no working date-filter query param on GHL's /opportunities/search, and (b) PITs can't register webhooks. Refreshes every 15 min via cron.",
  },
  exposed: true,
};

export const CONVERSATIONS: EntityManifest = {
  table: "conversations",
  description: "Message threads per contact per channel. Parent of messages. Join to contacts (contact_id).",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable conversation ID." },
    LOCATION_ID,
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "type", sqlite_type: "TEXT", nullable: true, description: "Channel.", enum: ["TYPE_PHONE", "TYPE_EMAIL", "TYPE_WHATSAPP", "TYPE_FB", "TYPE_IG", "TYPE_CUSTOM", "TYPE_LIVE_CHAT", "TYPE_REVIEW", "TYPE_SMS"] },
    { name: "assigned_to", sqlite_type: "TEXT", nullable: true, description: "", references: "users.id", source_path: "assignedTo" },
    { name: "unread_count", sqlite_type: "INTEGER", nullable: true, description: "", source_path: "unreadCount" },
    { name: "starred", sqlite_type: "INTEGER", nullable: true, description: "0 or 1." },
    { name: "inbox", sqlite_type: "INTEGER", nullable: true, description: "0 or 1." },
    { name: "last_message_type", sqlite_type: "TEXT", nullable: true, description: "", source_path: "lastMessageType" },
    { name: "last_message_body", sqlite_type: "TEXT", nullable: true, description: "", source_path: "lastMessageBody" },
    { name: "last_message_date", sqlite_type: "TEXT", nullable: true, description: "ISO 8601.", indexed: true, source_path: "lastMessageDate", timestamp_format: "ms_epoch" },
    // GHL's /conversations/search returns dateAdded / dateUpdated as
    // Unix millisecond epoch NUMBERS, not ISO strings like contacts.
    // timestamp_format: ms_epoch tells sync's mapRow to normalize to ISO.
    { name: "created_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp the conversation was created upstream.", source_path: "dateAdded", timestamp_format: "ms_epoch" },
    { name: "updated_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp of the most recent mutation upstream.", indexed: true, source_path: "dateUpdated", timestamp_format: "ms_epoch" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // GET /conversations/search. No `meta` object in the response. The
    // cursor is the last row's `lastMessageDate` (ms epoch). Sending
    // it back as `startAfterDate=<ms>` advances the page. Probed live
    // 2026-04-24 on ucNDNXi… — the previously-configured startAfterId
    // was silently ignored, pinning us to page 1 (101 of 14k rows).
    endpoint: "/conversations/search",
    method: "GET",
    pagination: "cursor",
    items_field: "conversations",
    cursor: {
      source: "last_item",
      fields: [
        { request_param: "startAfterDate", response_path: "lastMessageDate" },
      ],
    },
    query_extras: { limit: 100 },
  },
  incremental: {
    // poll_full: /conversations/search silently ignores date filters,
    // PITs can't register webhooks. Steady-state cron uses the
    // walk-to-watermark page loop: fetch newest-first pages only until
    // a row's last_message_date <= the watermark (i.e. already synced).
    //
    // KNOWN LIMITATION: /conversations/search's cursor is single-field
    // (lastMessageDate only). If two conversations share the same
    // lastMessageDate at a page boundary, the cursor can skip one. GHL
    // doesn't expose a documented tie-breaker (startAfterId is silently
    // accepted but doesn't split ties). In a 14k-conversation live test
    // this cost 4 rows (~0.03%). If exactness ever matters for a
    // workload, either expose conversations with an explicit analytics
    // disclaimer or drop pageLimit to 1 at suspected collision points.
    type: "poll_full",
    cursor_column: "last_message_date",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "ConversationCreate", kind: "upsert" },
    { ghl_event: "ConversationUpdate", kind: "upsert" },
  ],
  audit: {
    // Verified live on 2026-04-24 against ucNDNXi… sub-account.
    // backfillPollFull walks every page of /conversations/search each
    // tick. Upserts idempotent on `id`.
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false, // N/A for poll_full
    notes: "Uses poll_full because (a) no working date-filter query param on GHL's /conversations/search, and (b) PITs can't register webhooks. Refreshes every 15 min via cron.",
  },
  exposed: true,
};

export const MESSAGES: EntityManifest = {
  table: "messages",
  description: "Individual SMS / email / DM / voice messages. Child of conversations. High volume.",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable message ID." },
    LOCATION_ID,
    { name: "conversation_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "conversations.id", source_path: "conversationId" },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    // GHL returns BOTH `type` (integer numeric code like 1/2/28) and
    // `messageType` (semantic string like "TYPE_CALL"/"TYPE_SMS"/
    // "TYPE_LIVE_CHAT") on every message. Source the semantic string
    // so LLMs can write readable WHERE clauses without a code lookup.
    { name: "type", sqlite_type: "TEXT", nullable: true, description: "Channel (semantic name).", enum: ["TYPE_CALL", "TYPE_SMS", "TYPE_EMAIL", "TYPE_WHATSAPP", "TYPE_FB", "TYPE_IG", "TYPE_CUSTOM", "TYPE_LIVE_CHAT", "TYPE_REVIEW", "TYPE_IVR_CALL", "TYPE_SMS_REVIEW_REQUEST", "TYPE_WEBCHAT", "TYPE_ACTIVITY_OPPORTUNITY", "TYPE_ACTIVITY_APPOINTMENT", "TYPE_ACTIVITY_CONTACT", "TYPE_CUSTOM_EMAIL", "TYPE_CUSTOM_CALL", "TYPE_CAMPAIGN_CALL"], source_path: "messageType" },
    { name: "direction", sqlite_type: "TEXT", nullable: true, description: "", enum: ["inbound", "outbound"], indexed: true },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "Delivery / interaction status. For calls: 'completed' / 'no-answer' / 'voicemail' / etc." },
    { name: "body", sqlite_type: "TEXT", nullable: true, description: "Message body (SMS text, email plain body, transcript excerpt for calls)." },
    { name: "attachments", sqlite_type: "TEXT", nullable: true, json: true, description: "JSON array of attachment URLs." },
    { name: "date_added", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 when the message was recorded. Primary time axis — use this for 'first message' / 'last message' queries.", indexed: true, source_path: "dateAdded", timestamp_format: "iso8601" },
    { name: "user_id", sqlite_type: "TEXT", nullable: true, description: "Sub-account user who sent/handled the message (NULL for pure inbound without an owner).", indexed: true, source_path: "userId", references: "users.id" },
    // For TYPE_CALL / TYPE_IVR_CALL / TYPE_CUSTOM_CALL / TYPE_CAMPAIGN_CALL
    // messages GHL nests call-specific fields under `meta.call`. Duration
    // in seconds is the key signal for distinguishing voicemails (usually
    // status = 'voicemail' or duration < ~5s) from real conversations.
    { name: "call_duration_seconds", sqlite_type: "INTEGER", nullable: true, description: "Call length in seconds. NULL for non-call messages. Use with type IN ('TYPE_CALL','TYPE_IVR_CALL') and direction='inbound' + duration<=5 to isolate voicemails / missed calls.", source_path: "meta.call.duration" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // GHL's /conversations/{id}/messages wraps its response in a
    // "messages" object, with the actual array and cursor nested one
    // level deeper. Confirmed against live GHL:
    //   { messages: { lastMessageId, nextPage, messages: [...] },
    //     traceId: "..." }
    endpoint: "/conversations/{parent}/messages",
    method: "GET",
    pagination: "cursor",
    items_field: "messages.messages",
    cursor_response_field: "messages.lastMessageId",
    cursor_request_param: "lastMessageId",
    query_extras: { limit: 100 },
    per_parent: { parent_entity: "conversations", parent_fk_column: "conversation_id" },
  },
  incremental: {
    // per_parent: freshness inherits from the conversations table's
    // poll_full refresh cycle. Each cron tick the sync worker picks
    // conversations whose last_message_date has advanced since the
    // messages watermark and re-pulls their messages. PITs can't
    // register webhooks so this is the only path to <=15 min freshness.
    //
    // filter_ready: true — the "date filter" here is a DO-side query
    // against conversations.last_message_date, not a GHL server-side
    // filter. It's been probed live (the conversations table is
    // self-polling via its own poll_full path, so its last_message_date
    // is always within ~15 min of upstream).
    type: "per_parent",
    cursor_column: "date_added",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "InboundMessage", kind: "upsert" },
    { ghl_event: "OutboundMessage", kind: "upsert" },
  ],
  audit: {
    // Verified live on 2026-04-24 against ucNDNXi… sub-account:
    //   - /conversations/{id}/messages returns calls with
    //     meta.call.duration + direction + status as expected
    //   - per-parent backfill walks every conversation, resume cursor
    //     persists across subrequest-cap hits
    //   - steady-state freshness re-polls conversations whose
    //     last_message_date has advanced
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false, // N/A for per_parent — requiredAuditChecks skips it
    notes: "Unlocks call-log analytics (first inbound → first outbound callback, response rates). Columns include direction, call_duration_seconds, date_added — everything needed for Streamlined-style queries.",
  },
  exposed: true,
};

/**
 * CALL_EVENTS — a real synced table, NOT a view.
 *
 * The view version of this (views.ts, pre-merge) read json_extract
 * from messages.raw_payload and depended on GHL putting call data
 * consistently under `meta.call.duration`. In practice GHL's shape
 * varies per message (some rows have `duration`, some `call.duration`,
 * some `meta.call.duration`, some `metadata.duration`). Extracting
 * once at sync time — with a multi-path normalizer — gets consistent
 * typed columns and avoids JSON extraction overhead on every query.
 *
 * Populated by sync's callEventFromMessage() during backfillPerParent
 * of messages. Has its own stable row id (the source message's id),
 * its own indexes, and its own stream of updates via the messages
 * per-parent sync cycle.
 */
export const CALL_EVENTS: EntityManifest = {
  table: "call_events",
  description: "Individual phone call events (inbound + outbound + IVR). One row per call message, with duration / status / voicemail flag normalized out of the varied GHL shapes. Use this for callback-time, voicemail-rate, per-rep call volume. Subset of messages — every call_event row has a matching messages row.",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable call-event ID (matches the source message's id)." },
    LOCATION_ID,
    { name: "message_id", sqlite_type: "TEXT", nullable: false, description: "Source messages.id this event was derived from.", indexed: true, references: "messages.id" },
    { name: "conversation_id", sqlite_type: "TEXT", nullable: true, description: "Parent conversation.", indexed: true, references: "conversations.id" },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "Contact the call was with.", indexed: true, references: "contacts.id" },
    { name: "direction", sqlite_type: "TEXT", nullable: true, description: "", enum: ["inbound", "outbound"], indexed: true },
    { name: "call_type", sqlite_type: "TEXT", nullable: true, description: "TYPE_CALL / TYPE_IVR_CALL / TYPE_CUSTOM_CALL / TYPE_CAMPAIGN_CALL" },
    { name: "event_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 timestamp of the call.", indexed: true },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "Delivery status as the message carried it." },
    { name: "call_status", sqlite_type: "TEXT", nullable: true, description: "Call disposition (completed / no-answer / busy / failed / voicemail) when upstream provides one." },
    { name: "duration_seconds", sqlite_type: "REAL", nullable: true, description: "Call length in seconds. NULL for unknown. ≤ 5 + direction='inbound' is a reliable voicemail/missed heuristic." },
    { name: "recording_url", sqlite_type: "TEXT", nullable: true, description: "Recording URL if captured by upstream." },
    { name: "transcription_url", sqlite_type: "TEXT", nullable: true, description: "Transcription URL if captured." },
    { name: "voicemail", sqlite_type: "INTEGER", nullable: true, description: "1 when upstream marks this as voicemail; NULL when unknown." },
    { name: "missed", sqlite_type: "INTEGER", nullable: true, description: "1 when upstream marks no-answer/missed; NULL when unknown." },
    { name: "user_id", sqlite_type: "TEXT", nullable: true, description: "User/agent handling the call.", indexed: true, references: "users.id" },
    { name: "from_number", sqlite_type: "TEXT", nullable: true, description: "Caller phone number when captured." },
    { name: "to_number", sqlite_type: "TEXT", nullable: true, description: "Destination phone number when captured." },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Derived from messages during its per-parent sync — no direct
    // endpoint of our own. The dispatcher refuses calls for this table
    // and points at /sync/backfill?entity=messages instead.
    endpoint: "/conversations/{parent}/messages",
    method: "GET",
    pagination: "cursor",
    items_field: "messages.messages",
    cursor_response_field: "messages.lastMessageId",
    cursor_request_param: "lastMessageId",
    query_extras: { limit: 100 },
    per_parent: { parent_entity: "conversations", parent_fk_column: "conversation_id" },
  },
  incremental: {
    type: "per_parent",
    cursor_column: "event_at",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "InboundMessage", kind: "upsert" },
    { ghl_event: "OutboundMessage", kind: "upsert" },
  ],
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Dual-written during messages backfill by callEventFromMessage. Freshness follows messages' active-parent cycle. Queryable alongside messages for richer typed fields.",
  },
  exposed: true,
};

export const APPOINTMENTS: EntityManifest = {
  table: "appointments",
  description: "Scheduled calendar appointments. Join to contacts (contact_id), calendars (calendar_id).",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable appointment ID." },
    LOCATION_ID,
    { name: "calendar_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "calendars.id", source_path: "calendarId" },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "title", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "", enum: ["confirmed", "new", "cancelled", "showed", "noshow", "invalid"], source_path: "appointmentStatus" },
    { name: "start_time", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 start.", indexed: true, source_path: "startTime" },
    { name: "end_time", sqlite_type: "TEXT", nullable: true, description: "ISO 8601 end.", source_path: "endTime" },
    { name: "assigned_user_id", sqlite_type: "TEXT", nullable: true, description: "", references: "users.id", source_path: "assignedUserId" },
    { name: "address", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "notes", sqlite_type: "TEXT", nullable: true, description: "" },
    CREATED_AT,
    UPDATED_AT,
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Per-parent fan-out over calendars. GET /calendars/events requires
    // locationId + calendarId (or userId/groupId) + startTime + endTime
    // (must be strings of ms-epoch). Probed live 2026-04-24: response
    // is {events: [...]} with no pagination metadata — all events in
    // the window for that calendar come back in one shot. No limit /
    // cursor / page params are honored. Using a wide static window
    // (2020-01-01 → 2030-01-01) catches every appointment on disk
    // today with headroom; revisit when a customer's activity pushes
    // past 2030.
    endpoint: "/calendars/events",
    method: "GET",
    pagination: "none",
    items_field: "events",
    per_parent: {
      parent_entity: "calendars",
      parent_fk_column: "calendar_id",
      request_param: "calendarId",
    },
    query_extras: {
      startTime: "1577836800000", // 2020-01-01T00:00:00Z
      endTime: "1893456000000",   // 2030-01-01T00:00:00Z
    },
  },
  incremental: {
    // per_parent: freshness tracks the parent calendar's updated_at
    // via conversations-style active-parent selection. Same path
    // messages uses; drain_started_at per parent + snapshot-and-swap.
    type: "per_parent",
    cursor_column: "updated_at",
    poll_interval_minutes: 15,
    filter_ready: true,
  },
  webhooks: [
    { ghl_event: "AppointmentCreate", kind: "upsert" },
    { ghl_event: "AppointmentUpdate", kind: "upsert" },
    { ghl_event: "AppointmentDelete", kind: "delete" },
  ],
  audit: {
    // Verified live 2026-04-24 against ucNDNXi… sub-account:
    //   - /calendars/events with calendarId + startTime + endTime
    //     (strings) returns {events: [...]}; fields match our
    //     manifest columns.
    //   - No pagination — single fetch per calendar.
    //   - 8 events on the first calendar; hundreds across the
    //     sub-account's 91 calendars.
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false, // N/A for per_parent — requiredAuditChecks skips it
    notes: "Fanned out per-calendar. Closes the P1 in topline_find_references where kind=calendar previously returned zero hits because appointments was never synced — users could delete a calendar that had real upstream appointments.",
  },
  exposed: true,
};

// ---------------------------------------------------------------------------
// Phase 2 — metadata tables we need as join targets even though they're warm
// ---------------------------------------------------------------------------

export const PIPELINES: EntityManifest = {
  table: "pipelines",
  description: "Sales pipeline definitions. Parent of pipeline_stages. Rarely changes.",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable pipeline ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Pipeline display name." },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/opportunities/pipelines",
    method: "GET",
    pagination: "none",
    items_field: "pipelines",
  },
  incremental: {
    type: "poll_full",
    // daily
    poll_interval_minutes: 60 * 24,
    filter_ready: true,
  },
  audit: {
    // Verified live on 2026-04-23: full fetch returned 14 real pipelines.
    // poll_full mode = no cursor, no webhooks — the cron just re-fetches.
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    // update_cursor not applicable for poll_full (no cursor to track).
    update_cursor: false,
  },
  exposed: true,
};

export const PIPELINE_STAGES: EntityManifest = {
  table: "pipeline_stages",
  description: "Stages within each pipeline. Denormalized from the pipelines payload. Join to pipelines (pipeline_id).",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable stage ID." },
    { name: "pipeline_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "pipelines.id" },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Stage display name." },
    { name: "position", sqlite_type: "INTEGER", nullable: true, description: "Ordering within the pipeline. Low = earlier." },
    { name: "show_in_funnel", sqlite_type: "INTEGER", nullable: true, description: "0 or 1.", source_path: "showInFunnel" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Stages are denormalized from each pipeline's payload rather than a
    // dedicated endpoint. Sync worker expands pipelines[].stages[] at write
    // time. This descriptor is nominal; the worker special-cases it.
    endpoint: "/opportunities/pipelines",
    method: "GET",
    pagination: "none",
    items_field: "pipelines[].stages",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60 * 24,
    filter_ready: true,
  },
  audit: {
    // Verified live on 2026-04-23: 83 stages denormalized from 14 pipelines.
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

export const TAGS: EntityManifest = {
  table: "tags",
  description: "Tag taxonomy for the sub-account. Contacts reference tags by name via contacts.tags (JSON array).",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable tag ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Display name of the tag (what shows on contacts.tags).", indexed: true },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/locations/{locationId}/tags",
    method: "GET",
    pagination: "none",
    items_field: "tags",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Small (hundreds of tags); poll_full hourly is overkill-safe.",
  },
  exposed: true,
};

export const CUSTOM_FIELDS: EntityManifest = {
  table: "custom_fields",
  description: "Custom field definitions (name, dataType, model). Contact custom-field values live on contacts.custom_fields as a JSON array of {id,value}; join against this table by id to resolve the field's name / type.",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable custom-field ID (matches contact.custom_fields[].id)." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Human-readable field name.", indexed: true },
    { name: "field_key", sqlite_type: "TEXT", nullable: true, description: "Programmatic key like 'contact.phone_alt'.", source_path: "fieldKey" },
    { name: "data_type", sqlite_type: "TEXT", nullable: true, description: "Storage type (TEXT, LARGE_TEXT, NUMERICAL, PHONE, EMAIL, SINGLE_OPTIONS, MULTIPLE_OPTIONS, CHECKBOX, DATE, FILE_UPLOAD, etc.).", source_path: "dataType" },
    { name: "model", sqlite_type: "TEXT", nullable: true, description: "Object this field belongs to: 'contact', 'opportunity', 'appointment'.", enum: ["contact", "opportunity", "appointment"] },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/locations/{locationId}/customFields",
    method: "GET",
    pagination: "none",
    items_field: "customFields",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

export const CUSTOM_VALUES: EntityManifest = {
  table: "custom_values",
  description: "Sub-account-scoped key/value pairs used as merge tags ({{custom_values.X}}). Not per-contact — shared across the whole sub-account.",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable custom-value ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Human-readable name." },
    { name: "field_key", sqlite_type: "TEXT", nullable: true, description: "Merge-tag key like {{custom_values.review_request_link}}.", source_path: "fieldKey" },
    { name: "value", sqlite_type: "TEXT", nullable: true, description: "The actual value." },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/locations/{locationId}/customValues",
    method: "GET",
    pagination: "none",
    items_field: "customValues",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

export const CALENDARS: EntityManifest = {
  table: "calendars",
  description: "Calendar definitions (booking pages). Join target for appointments.calendar_id.",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable calendar ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Display name of the booking page." },
    { name: "group_id", sqlite_type: "TEXT", nullable: true, description: "Parent calendar group (for team / round-robin pages).", source_path: "groupId", references: "calendar_groups.id" },
    { name: "slug", sqlite_type: "TEXT", nullable: true, description: "URL slug: /widget/booking/<slug>." },
    { name: "is_active", sqlite_type: "INTEGER", nullable: true, description: "0 or 1.", source_path: "isActive" },
    { name: "event_type", sqlite_type: "TEXT", nullable: true, description: "Calendar type (service, round-robin, collective, event, etc.).", source_path: "eventType" },
    { name: "event_title", sqlite_type: "TEXT", nullable: true, description: "Default event title for bookings.", source_path: "eventTitle" },
    { name: "slot_duration", sqlite_type: "INTEGER", nullable: true, description: "Appointment length in minutes.", source_path: "slotDuration" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/calendars/",
    method: "GET",
    pagination: "none",
    items_field: "calendars",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Small table; hourly poll is generous. teamMembers / availability rules are in raw_payload for LLMs that want to drill into booking-page config.",
  },
  exposed: true,
};

/**
 * WORKFLOWS — shallow sync (id/name/status/version/timestamps).
 *
 * GHL's public API exposes only the list endpoint (GET /workflows/) and
 * it returns this shape. There is NO GET /workflows/{id} with config
 * details in v2 public — triggers, actions, filters, branches are not
 * part of any public surface. Workflow authoring stays UI-only.
 *
 * This manifest exists so SQL can answer "how many active workflows"
 * and "list workflow names by status" without a GHL round-trip. It
 * does NOT enable reference extraction (cataloging which workflows
 * touch which tags/custom-fields/etc.) — that would require workflow
 * internals we cannot access.
 */
export const WORKFLOWS: EntityManifest = {
  table: "workflows",
  description: "Workflow definitions (id/name/status/version/timestamps). Shallow — internal config (triggers, actions, branches) is NOT accessible via GHL's public API; that would require a marketplace OAuth app.",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable workflow ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Display name.", indexed: true },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "'published' | 'draft' (live values observed).", indexed: true },
    { name: "version", sqlite_type: "INTEGER", nullable: true, description: "Workflow version counter; bumps on edit." },
    { name: "created_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601.", source_path: "createdAt", timestamp_format: "iso8601" },
    { name: "updated_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601.", indexed: true, source_path: "updatedAt", timestamp_format: "iso8601" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/workflows/",
    method: "GET",
    pagination: "none",
    items_field: "workflows",
  },
  incremental: { type: "poll_full", poll_interval_minutes: 60, filter_ready: true },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Shallow-only. Workflow configs are NOT exposed by GHL's public v2 API under any auth type we can use (PIT or marketplace OAuth). Reference-extraction (which workflows touch which tag/field/calendar) is blocked upstream.",
  },
  exposed: true,
};

export const CALENDAR_GROUPS: EntityManifest = {
  table: "calendar_groups",
  description: "Groupings of calendars for team booking pages (round-robin, collective).",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable group ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Group display name." },
    { name: "description", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "slug", sqlite_type: "TEXT", nullable: true, description: "URL slug." },
    { name: "is_active", sqlite_type: "INTEGER", nullable: true, description: "0 or 1.", source_path: "isActive" },
    CREATED_AT,
    UPDATED_AT,
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/calendars/groups",
    method: "GET",
    pagination: "none",
    items_field: "groups",
  },
  incremental: {
    type: "poll_full",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

export const FORMS: EntityManifest = {
  table: "forms",
  description: "Form definitions (title, embed URL). Join target for form_submissions.form_id.",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable form ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Display name.", indexed: true },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/forms/",
    method: "GET",
    pagination: "none",
    items_field: "forms",
  },
  incremental: { type: "poll_full", poll_interval_minutes: 60, filter_ready: true },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Tiny table; hourly poll is plenty.",
  },
  exposed: true,
};

export const FORM_SUBMISSIONS: EntityManifest = {
  table: "form_submissions",
  description: "Form submissions (one row per submit event). Join form_id → forms.id and contact_id → contacts.id for funnel analysis. 'others' carries the raw per-field answers.",
  phase: 1,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable submission ID." },
    LOCATION_ID,
    { name: "form_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "forms.id", source_path: "formId" },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Submitter name as captured on the form." },
    { name: "email", sqlite_type: "TEXT", nullable: true, description: "Submitter email.", indexed: true },
    { name: "phone", sqlite_type: "TEXT", nullable: true, description: "Submitter phone." },
    { name: "others", sqlite_type: "TEXT", nullable: true, json: true, description: "Full per-field JSON object GHL returns (includes the form's custom fields)." },
    { name: "created_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601.", indexed: true, source_path: "createdAt", timestamp_format: "iso8601" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    // Location-scoped endpoint; GHL returns {submissions[], meta:{total,
    // currentPage, nextPage, prevPage}}. Page-number pagination.
    endpoint: "/forms/submissions",
    method: "GET",
    pagination: "cursor",
    items_field: "submissions",
    cursor: {
      source: "meta",
      fields: [
        { request_param: "page", response_path: "nextPage" },
      ],
    },
    query_extras: { limit: 100 },
  },
  incremental: {
    type: "poll_full",
    cursor_column: "created_at",
    poll_interval_minutes: 30,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Page-number paginated. In steady state, walk-to-watermark only re-pulls recent submissions.",
  },
  exposed: true,
};

export const SURVEYS: EntityManifest = {
  table: "surveys",
  description: "Survey definitions. Parent of survey_submissions.",
  phase: 3,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable survey ID." },
    LOCATION_ID,
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Display name.", indexed: true },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/surveys/",
    method: "GET",
    pagination: "none",
    items_field: "surveys",
  },
  incremental: { type: "poll_full", poll_interval_minutes: 60, filter_ready: true },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

export const SURVEY_SUBMISSIONS: EntityManifest = {
  table: "survey_submissions",
  description: "Survey responses. One row per submit. Join survey_id → surveys.id and contact_id → contacts.id.",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable submission ID." },
    LOCATION_ID,
    { name: "survey_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "surveys.id", source_path: "surveyId" },
    { name: "contact_id", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "name", sqlite_type: "TEXT", nullable: true, description: "Submitter name." },
    { name: "email", sqlite_type: "TEXT", nullable: true, description: "Submitter email.", indexed: true },
    { name: "phone", sqlite_type: "TEXT", nullable: true, description: "Submitter phone." },
    { name: "others", sqlite_type: "TEXT", nullable: true, json: true, description: "Full per-field JSON." },
    { name: "created_at", sqlite_type: "TEXT", nullable: true, description: "ISO 8601.", indexed: true, source_path: "createdAt", timestamp_format: "iso8601" },
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/surveys/submissions",
    method: "GET",
    pagination: "cursor",
    items_field: "submissions",
    cursor: {
      source: "meta",
      fields: [
        { request_param: "page", response_path: "nextPage" },
      ],
    },
    query_extras: { limit: 100 },
  },
  incremental: {
    type: "poll_full",
    cursor_column: "created_at",
    poll_interval_minutes: 30,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

export const TASKS: EntityManifest = {
  table: "tasks",
  description: "Tasks attached to contacts (title, dueDate, completed). Useful for stale-follow-up / workload analytics per rep.",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable task ID." },
    LOCATION_ID,
    { name: "contact_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "title", sqlite_type: "TEXT", nullable: true, description: "" },
    { name: "body", sqlite_type: "TEXT", nullable: true, description: "Task details." },
    { name: "assigned_to", sqlite_type: "TEXT", nullable: true, description: "", indexed: true, references: "users.id", source_path: "assignedTo" },
    { name: "due_date", sqlite_type: "TEXT", nullable: true, description: "Due date/time.", indexed: true, source_path: "dueDate", timestamp_format: "iso8601" },
    { name: "completed", sqlite_type: "INTEGER", nullable: true, description: "0 or 1." },
    CREATED_AT,
    UPDATED_AT,
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/contacts/{parent}/tasks",
    method: "GET",
    pagination: "none",
    items_field: "tasks",
    per_parent: { parent_entity: "contacts", parent_fk_column: "contact_id" },
  },
  incremental: {
    type: "per_parent",
    cursor_column: "updated_at",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
    notes: "Per-contact fanout. Uses durable _parent_sync_state so drains resume across subrequest-cap hits.",
  },
  exposed: true,
};

export const NOTES: EntityManifest = {
  table: "notes",
  description: "Free-form notes attached to contacts. Useful for context mining ('find contacts whose notes mention X').",
  phase: 2,
  primary_key: "id",
  columns: [
    { name: "id", sqlite_type: "TEXT", nullable: false, description: "Stable note ID." },
    LOCATION_ID,
    { name: "contact_id", sqlite_type: "TEXT", nullable: false, description: "", indexed: true, references: "contacts.id", source_path: "contactId" },
    { name: "body", sqlite_type: "TEXT", nullable: true, description: "Note text." },
    { name: "user_id", sqlite_type: "TEXT", nullable: true, description: "Author user id when set.", indexed: true, references: "users.id", source_path: "userId" },
    CREATED_AT,
    UPDATED_AT,
    RAW_PAYLOAD,
    SYNCED_AT,
  ],
  backfill: {
    endpoint: "/contacts/{parent}/notes",
    method: "GET",
    pagination: "none",
    items_field: "notes",
    per_parent: { parent_entity: "contacts", parent_fk_column: "contact_id" },
  },
  incremental: {
    type: "per_parent",
    cursor_column: "updated_at",
    poll_interval_minutes: 60,
    filter_ready: true,
  },
  audit: {
    live_tested: true,
    stable_pk: true,
    backfill_path: true,
    incremental_path: true,
    update_cursor: false,
  },
  exposed: true,
};

/** Every entity defined in this manifest. Order matters for table creation
 * (metadata tables first so FK hints resolve). */
export const ALL_ENTITIES: readonly EntityManifest[] = [
  PIPELINES,
  PIPELINE_STAGES,
  CALENDAR_GROUPS,
  CALENDARS,
  TAGS,
  CUSTOM_FIELDS,
  CUSTOM_VALUES,
  FORMS,
  SURVEYS,
  WORKFLOWS,
  CONTACTS,
  OPPORTUNITIES,
  CONVERSATIONS,
  MESSAGES,
  CALL_EVENTS,
  APPOINTMENTS,
  FORM_SUBMISSIONS,
  SURVEY_SUBMISSIONS,
  TASKS,
  NOTES,
];

/** Lookup by table name. */
export const ENTITY_BY_TABLE: ReadonlyMap<string, EntityManifest> = new Map(
  ALL_ENTITIES.map((e) => [e.table, e]),
);

/**
 * Entities currently exposed to SQL queries. Requires BOTH the manual
 * `exposed` flag AND a passing audit — so flipping `exposed: true` on an
 * unaudited entity does not leak it into describe_schema / execute_query.
 */
export function getExposedEntities(): readonly EntityManifest[] {
  return ALL_ENTITIES.filter((e) => e.exposed && auditPasses(e));
}
