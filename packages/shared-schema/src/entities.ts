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
    SYNCED_AT,
  ],
  backfill: {
    // Matches apps/edge/src/tools/contacts.ts `topline_search_contacts`:
    // POST /contacts/search with body.searchAfter = [lastId], body.pageLimit.
    endpoint: "/contacts/search",
    method: "POST",
    pagination: "cursor",
    items_field: "contacts",
    cursor_response_field: "meta.searchAfter",
    cursor_request_param: "searchAfter",
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
    SYNCED_AT,
  ],
  backfill: {
    // Matches apps/edge/src/tools/opportunities.ts `topline_search_opportunities`:
    // GET /opportunities/search with query startAfterId (not POST, not `page`).
    // Note: this endpoint uses snake_case `location_id` where most GHL v2
    // endpoints use camelCase `locationId`. The live edge tool confirms it.
    endpoint: "/opportunities/search",
    method: "GET",
    location_param_name: "location_id",
    pagination: "cursor",
    items_field: "opportunities",
    cursor_response_field: "meta.startAfterId",
    cursor_request_param: "startAfterId",
    query_extras: { limit: 100 },
  },
  incremental: {
    type: "updated_after",
    cursor_column: "updated_at",
    cursor_query_param: "date_updated",
    poll_interval_minutes: 15,
    // GET /opportunities/search does not appear to support server-side
    // date filtering: passing date_updated / dateUpdated / dateAdded /
    // startAfter=<epoch_ms> either returns "property should not exist"
    // or is silently ignored (every far-future value still returned the
    // full page). Incremental relies on webhooks (step 5) or on
    // periodic full backfills. Keep filter_ready: false until GHL
    // exposes a real filter or we switch to a POST variant.
    filter_ready: false,
  },
  webhooks: [
    { ghl_event: "OpportunityCreate", kind: "upsert" },
    { ghl_event: "OpportunityUpdate", kind: "upsert" },
    { ghl_event: "OpportunityDelete", kind: "delete" },
  ],
  audit: {
    live_tested: false,
    stable_pk: false,
    backfill_path: false,
    incremental_path: false,
    update_cursor: false,
    notes: "GET /opportunities/search has no working date-filter query param — silently ignores dateUpdated/date_updated/startAfter regardless of value. Full re-backfill required until webhooks land.",
  },
  exposed: false,
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
    SYNCED_AT,
  ],
  backfill: {
    // Matches apps/edge/src/tools/conversations.ts `topline_search_conversations`:
    // GET /conversations/search with query startAfterId.
    endpoint: "/conversations/search",
    method: "GET",
    pagination: "cursor",
    items_field: "conversations",
    cursor_response_field: "meta.startAfterId",
    cursor_request_param: "startAfterId",
    query_extras: { limit: 100 },
  },
  incremental: {
    type: "updated_after",
    cursor_column: "last_message_date",
    cursor_query_param: "lastMessageDate",
    poll_interval_minutes: 15,
    // GET /conversations/search silently accepts lastMessageDate and
    // startAfterDate query params but does not filter on them (every
    // far-future value still returns the full page). Only startAfterDate=0
    // gave a degenerate 1-row response, suggesting these params have
    // some other cursor semantics entirely. Keep filter_ready: false
    // until the right mechanism is identified or webhooks land.
    filter_ready: false,
  },
  webhooks: [
    { ghl_event: "ConversationCreate", kind: "upsert" },
    { ghl_event: "ConversationUpdate", kind: "upsert" },
  ],
  audit: {
    live_tested: false,
    stable_pk: false,
    backfill_path: false,
    incremental_path: false,
    update_cursor: false,
    notes: "GET /conversations/search date query params (lastMessageDate, startAfterDate) are silently ignored. Full re-backfill required until webhooks land.",
  },
  exposed: false,
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
    { name: "type", sqlite_type: "TEXT", nullable: true, description: "Channel (semantic name).", enum: ["TYPE_CALL", "TYPE_SMS", "TYPE_EMAIL", "TYPE_WHATSAPP", "TYPE_FB", "TYPE_IG", "TYPE_CUSTOM", "TYPE_LIVE_CHAT", "TYPE_REVIEW", "TYPE_IVR_CALL", "TYPE_SMS_REVIEW_REQUEST", "TYPE_WEBCHAT"], source_path: "messageType" },
    { name: "direction", sqlite_type: "TEXT", nullable: true, description: "", enum: ["inbound", "outbound"] },
    { name: "status", sqlite_type: "TEXT", nullable: true, description: "Delivery / interaction status." },
    { name: "body", sqlite_type: "TEXT", nullable: true, description: "Message body (SMS text, email plain body, etc.)." },
    { name: "attachments", sqlite_type: "TEXT", nullable: true, json: true, description: "JSON array of attachment URLs." },
    { name: "date_added", sqlite_type: "TEXT", nullable: true, description: "ISO 8601. Primary time axis for messages.", indexed: true, source_path: "dateAdded", timestamp_format: "iso8601" },
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
    type: "per_parent",
    poll_interval_minutes: 15,
    filter_ready: false,
  },
  webhooks: [
    { ghl_event: "InboundMessage", kind: "upsert" },
    { ghl_event: "OutboundMessage", kind: "upsert" },
  ],
  audit: {
    live_tested: false,
    stable_pk: false,
    backfill_path: false,
    incremental_path: false,
    update_cursor: false,
  },
  exposed: false,
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
    SYNCED_AT,
  ],
  backfill: {
    // UNKNOWN. The current edge tools (apps/edge/src/tools/calendars.ts)
    // expose only per-ID CRUD for appointments — no list/search endpoint.
    // GHL may support listing via `/calendars/events` with time-window
    // filters, but that contract has not been probed against a live
    // sub-account. Pagination is intentionally "unknown" so the sync
    // worker and audit runner refuse to operate on this entity until
    // the contract is verified and this descriptor is updated with real
    // pagination fields. Do not add cursor_* or items_field here as a
    // placeholder — fabricated values would mislead any consumer that
    // trusts this descriptor.
    endpoint: "/calendars/events",
    method: "GET",
    pagination: "unknown",
  },
  incremental: {
    type: "updated_after",
    cursor_column: "updated_at",
    cursor_query_param: "updatedAfter",
    poll_interval_minutes: 15,
    filter_ready: false,
  },
  webhooks: [
    { ghl_event: "AppointmentCreate", kind: "upsert" },
    { ghl_event: "AppointmentUpdate", kind: "upsert" },
    { ghl_event: "AppointmentDelete", kind: "delete" },
  ],
  audit: {
    live_tested: false,
    stable_pk: false,
    backfill_path: false,
    incremental_path: false,
    update_cursor: false,
    notes: "Backfill endpoint unverified — edge tools only expose per-ID CRUD. Need to probe GHL's /calendars/events list semantics (time-window vs cursor) before shipping the sync worker.",
  },
  exposed: false,
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

/** Every entity defined in this manifest. Order matters for table creation
 * (metadata tables first so FK hints resolve). */
export const ALL_ENTITIES: readonly EntityManifest[] = [
  PIPELINES,
  PIPELINE_STAGES,
  CONTACTS,
  OPPORTUNITIES,
  CONVERSATIONS,
  MESSAGES,
  APPOINTMENTS,
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
