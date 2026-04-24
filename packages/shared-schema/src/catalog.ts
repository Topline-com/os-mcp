// GHL data-surface catalog.
//
// The entity manifest (entities.ts) describes what we sync. The catalog
// describes what EXISTS upstream — including objects we haven't built
// sync for yet, objects that require OAuth scopes we don't have, and
// objects we've decided not to pursue. Exposing this catalog to the LLM
// (via topline_describe_data_catalog) prevents silent failure modes like
// the one that shows up on Streamlined-style analytics prompts: Claude
// sees the schema, doesn't see "messages", and assumes it doesn't
// exist rather than asking whether it's hidden.
//
// Every row here is a factual claim about GHL v2. Keep it grounded in
// live probing (services.leadconnectorhq.com/*) and the marketplace
// docs (https://marketplace.gohighlevel.com/docs/). If you add an
// entry, note the probe date and what the PIT-auth response was.
//
// Status semantics:
//   exposed          — in entities.ts, exposed=true, queryable via SQL
//   syncing          — in entities.ts, exposed=false, sync runs but
//                      gated out of customer SELECTs (internal testing)
//   catalogued       — on our roadmap, manifest not yet written
//   requires_oauth   — only accessible via OAuth agency install, not
//                      our per-sub-account PIT auth
//   inaccessible     — confirmed via live probe that the endpoint is
//                      404 under PIT auth and we have no alternative path
//   declined         — decision made not to sync (document why)

import { ALL_ENTITIES } from "./entities.js";
import { auditPasses, type EntityManifest } from "./types.js";

export type CatalogStatus =
  | "exposed"
  | "syncing"
  | "catalogued"
  | "requires_oauth"
  | "inaccessible"
  | "declined";

export interface CatalogEntry {
  /** Snake-case identifier we'd use as a SQL table name. */
  name: string;
  /** Human-readable category for grouping in the catalog UI. */
  category:
    | "CRM"
    | "Communications"
    | "Pipelines"
    | "Scheduling"
    | "Lead capture"
    | "Revenue"
    | "Activity"
    | "Metadata"
    | "Marketing"
    | "Content"
    | "Platform"
    | "Agency";
  /** What this object represents upstream. */
  description: string;
  /** Current status in our warehouse. Resolved from entities.ts where relevant. */
  status: CatalogStatus;
  /** GHL v2 endpoint path (sans `/services.leadconnectorhq.com`). Null if unknown. */
  endpoint?: string;
  /** Notes: probe dates, known quirks, rationale for status. */
  notes?: string;
}

// ---------------------------------------------------------------------------
// The catalog. One entry per GHL object we know exists. Status for
// entries that have a manifest is computed dynamically in
// buildCatalog() — hardcoded status here is for objects WITHOUT a
// manifest (catalogued / requires_oauth / inaccessible / declined).
// ---------------------------------------------------------------------------

const STATIC_ENTRIES: CatalogEntry[] = [
  // CRM core
  { name: "companies", category: "CRM", description: "Company/business records that contacts can be linked to.", status: "catalogued", endpoint: "/companies/search", notes: "Probed 2026-04-24: endpoint exists under PIT; manifest not yet written." },
  { name: "users", category: "Metadata", description: "Sub-account users (owners, reps, agents). Join target for assigned_to / assigned_user_id columns.", status: "catalogued", endpoint: "/users/", notes: "Edge tool already exists; manifest pending." },
  { name: "tags", category: "Metadata", description: "Tag taxonomy for the sub-account. Contact tags are stored as JSON arrays on contacts.tags.", status: "catalogued", endpoint: "/locations/{locationId}/tags", notes: "Small table; poll_full daily is sufficient." },
  { name: "custom_fields", category: "Metadata", description: "Custom field definitions (name, dataType, picklistOptions). Contact values stored on contacts.custom_fields as a JSON array of {id,value} pairs.", status: "catalogued", endpoint: "/locations/{locationId}/customFields" },
  { name: "custom_values", category: "Metadata", description: "Sub-account-scoped key/value pairs (not per-contact). Useful for {{custom_values.X}} merge tags.", status: "catalogued", endpoint: "/locations/{locationId}/customValues" },
  { name: "custom_objects", category: "Platform", description: "User-defined object types beyond contacts/opportunities (e.g. 'Properties', 'Projects'). Records live under custom_object_records.", status: "requires_oauth", notes: "Custom objects API is marketplace-app scoped; PIT responses 401/404." },
  { name: "custom_object_records", category: "Platform", description: "Individual rows of a custom_objects type.", status: "requires_oauth" },
  { name: "associations", category: "Platform", description: "Cross-object relations (opportunity↔property, contact↔deal, etc.).", status: "requires_oauth" },

  // Communications
  { name: "call_recordings", category: "Communications", description: "Audio recording URLs + metadata per call message.", status: "catalogued", notes: "Surface via raw_payload.meta.call until a dedicated endpoint ships." },
  { name: "call_transcripts", category: "Communications", description: "Text transcripts of call recordings (when available upstream).", status: "requires_oauth", notes: "GHL's Voice AI / transcription APIs are marketplace-scoped." },
  { name: "email_events", category: "Communications", description: "Per-email delivery events (sent, delivered, opened, clicked, bounced). Streamlined calls this 'email_events'.", status: "catalogued", notes: "Upstream fields live in messages.raw_payload for type=TYPE_EMAIL. A derived view is the right ship." },
  { name: "sms_events", category: "Communications", description: "SMS delivery status history (queued, sent, delivered, failed).", status: "catalogued", notes: "Available in messages.status + raw_payload." },
  { name: "chat_events", category: "Communications", description: "Live-chat widget events (session start, visitor typing, agent joined).", status: "declined", notes: "Low analytics value; skip unless a customer asks." },

  // Scheduling & lead capture
  { name: "calendars", category: "Scheduling", description: "Calendar definitions (type, slug, availability rules). Join target for appointments.calendar_id.", status: "catalogued", endpoint: "/calendars/", notes: "Edge tool exists; small table; poll_full daily." },
  { name: "calendar_groups", category: "Scheduling", description: "Groupings of calendars for team/round-robin booking pages.", status: "catalogued", endpoint: "/calendars/groups" },
  { name: "calendar_resources", category: "Scheduling", description: "Bookable resources attached to calendars (rooms, equipment).", status: "catalogued", endpoint: "/calendars/resources" },
  { name: "appointment_notes", category: "Scheduling", description: "Notes attached to appointments. Useful for post-call summaries.", status: "catalogued", endpoint: "/appointments/{appointmentId}/notes" },
  { name: "forms", category: "Lead capture", description: "Form definitions (questions, fields).", status: "catalogued", endpoint: "/forms/" },
  { name: "form_submissions", category: "Lead capture", description: "Per-form submissions. Streamlined uses these for lead-attribution and form-funnel analysis.", status: "catalogued", endpoint: "/forms/submissions" },
  { name: "surveys", category: "Lead capture", description: "Survey definitions (multi-step forms with branching).", status: "catalogued", endpoint: "/surveys/" },
  { name: "survey_submissions", category: "Lead capture", description: "Survey responses.", status: "catalogued", endpoint: "/surveys/submissions" },
  { name: "quizzes", category: "Lead capture", description: "Quiz definitions and responses.", status: "declined", notes: "Low-volume in most sub-accounts; revisit on request." },

  // Pipeline & sales
  { name: "opportunity_status_history", category: "Pipelines", description: "Status-change events per opportunity (open→won/lost/abandoned, with timestamp and user). Derived view atop opportunities.raw_payload + stage transitions.", status: "catalogued", notes: "Compute from messages type=TYPE_ACTIVITY_OPPORTUNITY + opportunities.last_status_change_at." },
  { name: "opportunity_stage_history", category: "Pipelines", description: "Stage-transition events per opportunity.", status: "catalogued", notes: "Available via opportunities.last_stage_change_at + messages activity rows." },
  { name: "lost_reasons", category: "Pipelines", description: "Reason codes for lost opportunities (taxonomy).", status: "catalogued", endpoint: "/opportunities/loss-reasons" },

  // Revenue
  { name: "products", category: "Revenue", description: "Product catalog (name, description, prices).", status: "catalogued", endpoint: "/products/" },
  { name: "prices", category: "Revenue", description: "Price variants per product (one-time, recurring, tiered).", status: "catalogued", endpoint: "/products/{productId}/price" },
  { name: "invoices", category: "Revenue", description: "Invoice records (issued, status, amount, line items).", status: "catalogued", endpoint: "/invoices/" },
  { name: "invoice_schedules", category: "Revenue", description: "Recurring invoice templates.", status: "catalogued", endpoint: "/invoices/schedule" },
  { name: "transactions", category: "Revenue", description: "Payment transactions (successful + failed charges).", status: "catalogued", endpoint: "/payments/transactions" },
  { name: "subscriptions", category: "Revenue", description: "Active / cancelled / paused customer subscriptions.", status: "catalogued", endpoint: "/payments/subscriptions" },
  { name: "orders", category: "Revenue", description: "Order records (e-commerce + funnel checkouts).", status: "catalogued", endpoint: "/payments/orders" },
  { name: "coupons", category: "Revenue", description: "Discount code definitions.", status: "catalogued", endpoint: "/payments/coupon" },
  { name: "proposals", category: "Revenue", description: "Proposal / estimate documents (signature status).", status: "requires_oauth", notes: "Proposal module access requires marketplace install on most agencies." },

  // Activity / work
  { name: "tasks", category: "Activity", description: "Tasks attached to contacts (title, dueDate, completed).", status: "catalogued", endpoint: "/contacts/{contactId}/tasks" },
  { name: "notes", category: "Activity", description: "Free-form notes per contact.", status: "catalogued", endpoint: "/contacts/{contactId}/notes" },
  { name: "workflows", category: "Activity", description: "Workflow definitions (automation flows).", status: "catalogued", endpoint: "/workflows/" },
  { name: "workflow_events", category: "Activity", description: "Per-contact workflow enrollment + step-execution history.", status: "requires_oauth", notes: "Detailed execution events are marketplace-scoped." },
  { name: "campaigns", category: "Activity", description: "Legacy campaign definitions (pre-workflow).", status: "catalogued", endpoint: "/campaigns/" },
  { name: "trigger_links", category: "Activity", description: "Trackable short links; click events feed attribution.", status: "catalogued" },

  // Marketing / content
  { name: "funnels", category: "Content", description: "Funnel definitions (multi-page conversion flows).", status: "catalogued", endpoint: "/funnels/funnel" },
  { name: "funnel_pages", category: "Content", description: "Individual pages within a funnel.", status: "catalogued", endpoint: "/funnels/page" },
  { name: "websites", category: "Content", description: "Website builder sites (same infra as funnels).", status: "catalogued", endpoint: "/funnels/funnel", notes: "Same endpoint as funnels; distinguished by type." },
  { name: "blogs", category: "Content", description: "Blog definitions.", status: "catalogued", endpoint: "/blogs/" },
  { name: "blog_posts", category: "Content", description: "Blog posts per blog.", status: "catalogued", endpoint: "/blogs/posts" },
  { name: "courses", category: "Content", description: "Membership / course offerings.", status: "requires_oauth" },
  { name: "communities", category: "Content", description: "Community forums/groups feature.", status: "requires_oauth" },

  // Marketing ops
  { name: "email_templates", category: "Marketing", description: "Saved email templates.", status: "catalogued", endpoint: "/emails/templates" },
  { name: "sms_templates", category: "Marketing", description: "Saved SMS templates.", status: "catalogued" },
  { name: "social_planner_posts", category: "Marketing", description: "Scheduled / published social posts.", status: "catalogued", endpoint: "/social-media-posting/" },
  { name: "social_accounts", category: "Marketing", description: "Connected social media accounts per location.", status: "catalogued", endpoint: "/social-media-posting/accounts" },
  { name: "ad_accounts", category: "Marketing", description: "Connected Google Ads / Meta Ads accounts.", status: "requires_oauth" },
  { name: "ad_reports", category: "Marketing", description: "Ad-spend + performance attribution reports.", status: "requires_oauth" },

  // Platform
  { name: "media", category: "Platform", description: "Media library (images, videos, docs) uploaded to the sub-account.", status: "catalogued", endpoint: "/medias/" },
  { name: "webhooks", category: "Platform", description: "Registered webhook subscriptions.", status: "requires_oauth", notes: "Probed 2026-04-24: /webhooks, /hooks, /locations/{id}/webhooks all 404 under PIT auth. Webhooks are marketplace-OAuth-only in GHL v2 — blocking webhook-based freshness for everything." },
  { name: "voice_ai_agents", category: "Platform", description: "Voice AI agent definitions (for inbound/outbound calling).", status: "requires_oauth" },
  { name: "conversation_ai_bots", category: "Platform", description: "Conversation AI bot definitions.", status: "requires_oauth" },
  { name: "affiliates", category: "Platform", description: "Affiliate program members.", status: "requires_oauth" },
  { name: "reviews", category: "Platform", description: "Reputation module reviews (Google/Facebook aggregated).", status: "catalogued", endpoint: "/reputation/reviews" },
  { name: "phone_numbers", category: "Platform", description: "Provisioned phone numbers (twilio pass-through).", status: "catalogued" },

  // Agency-level
  { name: "locations", category: "Agency", description: "Sub-accounts within the agency. Required for multi-tenant analytics ('which accounts are doing poorly').", status: "requires_oauth", notes: "Cross-sub-account enumeration is agency-scoped. Our per-PIT connection model is single-sub-account by design." },
  { name: "snapshots", category: "Agency", description: "Agency snapshots (templated sub-account configurations).", status: "requires_oauth" },
  { name: "agency_users", category: "Agency", description: "Agency-level user roles.", status: "requires_oauth" },
];

// ---------------------------------------------------------------------------
// Public API: merge the live entity state with the static catalog.
// ---------------------------------------------------------------------------

export interface CatalogEntryResolved extends CatalogEntry {
  /** True when status is "exposed" or "syncing". */
  synced: boolean;
  /** When status === "exposed", the SQL table name to query. */
  sql_table?: string;
}

/**
 * Merge ALL_ENTITIES (what we sync) with STATIC_ENTRIES (what exists
 * upstream but isn't synced yet). Every row returned tells the LLM
 * exactly what it can query today and what's pending.
 */
export function buildCatalog(): CatalogEntryResolved[] {
  const out: CatalogEntryResolved[] = [];
  const seen = new Set<string>();

  for (const entity of ALL_ENTITIES) {
    seen.add(entity.table);
    const passed = auditPasses(entity);
    const status: CatalogStatus = entity.exposed && passed ? "exposed" : "syncing";
    out.push({
      name: entity.table,
      category: inferCategory(entity.table),
      description: entity.description,
      status,
      endpoint: entity.backfill.endpoint,
      synced: true,
      sql_table: status === "exposed" ? entity.table : undefined,
      notes: status === "syncing" ? audit_blockers(entity) : undefined,
    });
  }

  for (const e of STATIC_ENTRIES) {
    if (seen.has(e.name)) continue; // entity manifest wins
    out.push({
      ...e,
      synced: false,
    });
  }

  // Stable sort: exposed first, then syncing, then by category, then by name.
  const order: Record<CatalogStatus, number> = {
    exposed: 0,
    syncing: 1,
    catalogued: 2,
    requires_oauth: 3,
    inaccessible: 4,
    declined: 5,
  };
  out.sort((a, b) => {
    if (order[a.status] !== order[b.status]) return order[a.status] - order[b.status];
    if (a.category !== b.category) return a.category.localeCompare(b.category);
    return a.name.localeCompare(b.name);
  });
  return out;
}

function inferCategory(table: string): CatalogEntry["category"] {
  if (table === "contacts" || table === "users") return "CRM";
  if (table === "opportunities" || table === "pipelines" || table === "pipeline_stages") return "Pipelines";
  if (table === "conversations" || table === "messages") return "Communications";
  if (table === "appointments" || table === "calendars") return "Scheduling";
  return "CRM";
}

function audit_blockers(entity: EntityManifest): string {
  const failing: string[] = [];
  const record = entity.audit as unknown as Record<string, unknown>;
  for (const [k, v] of Object.entries(record)) {
    if (v === false) failing.push(k);
  }
  return failing.length > 0
    ? `Audit blocked on: ${failing.join(", ")}`
    : "Synced internally; not yet flipped to exposed.";
}
