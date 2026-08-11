import type { ToolDef } from "./tools/types.js";
import type { PersistedToolPolicy } from "./tool-policy.js";

export const PRESET_IDS = ["read_only_crm", "sales", "marketing", "analytics", "all"] as const;
export type ToolPresetId = (typeof PRESET_IDS)[number];

export type ToolSelection =
  | { kind: "all" }
  | { kind: "preset"; preset_id: ToolPresetId }
  | { kind: "custom"; tool_ids: string[] };

export interface ToolPresetDefinition {
  id: ToolPresetId;
  label: string;
  description: string;
  tool_ids: readonly string[] | "all";
}

export type ClientTarget = "generic" | "copilot_studio";

export interface ClientCompatibilityAssessment {
  compatible: boolean;
  warnings: string[];
  error?: string;
}

const READ_ONLY_CRM = [
  "topline_ping",
  "topline_setup_check",
  "topline_search_contacts",
  "topline_get_contact",
  "topline_search_conversations",
  "topline_get_conversation",
  "topline_get_messages",
  "topline_list_pipelines",
  "topline_search_opportunities",
  "topline_get_opportunity",
  "topline_list_calendars",
  "topline_get_calendar_slots",
  "topline_get_calendar",
  "topline_list_contact_tasks",
  "topline_list_contact_notes",
  "topline_list_custom_fields",
  "topline_get_custom_field",
  "topline_list_custom_values",
  "topline_get_custom_value",
  "topline_list_workflows",
  "topline_list_tags",
  "topline_get_location",
  "topline_list_users",
  "topline_get_user",
  "topline_list_forms",
  "topline_list_form_submissions",
  "topline_list_surveys",
  "topline_list_survey_submissions",
] as const;

const SALES = [
  "topline_ping",
  "topline_setup_check",
  "topline_search_contacts",
  "topline_get_contact",
  "topline_create_contact",
  "topline_update_contact",
  "topline_add_contact_tags",
  "topline_remove_contact_tags",
  "topline_upsert_contact",
  "topline_add_contact_to_workflow",
  "topline_remove_contact_from_workflow",
  "topline_search_conversations",
  "topline_get_conversation",
  "topline_get_messages",
  "topline_send_message",
  "topline_create_conversation",
  "topline_list_pipelines",
  "topline_search_opportunities",
  "topline_get_opportunity",
  "topline_create_opportunity",
  "topline_update_opportunity",
  "topline_list_calendars",
  "topline_get_calendar_slots",
  "topline_create_appointment",
  "topline_update_appointment",
  "topline_list_contact_tasks",
  "topline_create_task",
  "topline_update_task",
  "topline_list_contact_notes",
  "topline_create_note",
] as const;

const MARKETING = [
  "topline_ping",
  "topline_setup_check",
  "topline_email_template",
  "topline_email_campaign",
  "topline_email_campaign_stats",
  "topline_email_campaign_recipients",
  "topline_fb_targeting",
  "topline_fb_reporting",
  "topline_google_targeting",
  "topline_google_reporting",
  "topline_linkedin_targeting",
  "topline_linkedin_reporting",
  "topline_get_marketing_config",
  "topline_set_marketing_config",
  "topline_init_attribution_fields",
  "topline_register_campaign_utm",
  "topline_get_campaign_utm",
  "topline_list_campaign_utms",
  "topline_build_utm_url",
  "topline_lint_utm",
  "topline_list_spend_providers",
  "topline_list_spend_transactions",
  "topline_get_channel_spend",
  "topline_list_spend_classification_rules",
  "topline_add_spend_classification_rule",
  "topline_reconcile_spend",
  "topline_get_marketing_dashboard",
  "topline_list_forms",
  "topline_list_form_submissions",
  "topline_list_surveys",
] as const;

const ANALYTICS = [
  "topline_describe_data_catalog",
  "topline_describe_schema",
  "topline_explain_tables",
  "topline_execute_query",
  "topline_utilize_api",
  "topline_query_doctor",
  "topline_warehouse_freshness",
  "topline_pipeline_snapshot",
  "topline_pipeline_audit",
  "topline_contact_audit",
  "topline_owner_audit",
  "topline_find_references",
] as const;

export const TOOL_PRESETS: Readonly<Record<ToolPresetId, ToolPresetDefinition>> = {
  read_only_crm: {
    id: "read_only_crm",
    label: "Read-only CRM",
    description: "Search and inspect CRM records without mutation tools.",
    tool_ids: READ_ONLY_CRM,
  },
  sales: {
    id: "sales",
    label: "Sales",
    description: "Core contact, conversation, pipeline, appointment, task, and note workflows.",
    tool_ids: SALES,
  },
  marketing: {
    id: "marketing",
    label: "Marketing",
    description: "Campaign, advertising insight, attribution, spend, and marketing reporting tools.",
    tool_ids: MARKETING,
  },
  analytics: {
    id: "analytics",
    label: "Analytics",
    description: "Read-only warehouse schema, query, audit, and reference tools.",
    tool_ids: ANALYTICS,
  },
  all: {
    id: "all",
    label: "All tools",
    description: "Every current and future published tool for this connection.",
    tool_ids: "all",
  },
};

export function compileToolSelection(
  selection: ToolSelection,
  registry: readonly ToolDef[],
): PersistedToolPolicy {
  if (selection.kind === "all" || (selection.kind === "preset" && selection.preset_id === "all")) {
    return { version: 1, mode: "all" };
  }

  const requested =
    selection.kind === "preset"
      ? TOOL_PRESETS[selection.preset_id]?.tool_ids
      : selection.tool_ids;
  if (!requested || requested === "all") {
    throw new Error("Unknown tool preset.");
  }

  const requestedIds = new Set(requested);
  const catalogIds = new Set(registry.map((tool) => tool.name));
  const unknown = [...requestedIds].filter((id) => !catalogIds.has(id));
  if (unknown.length > 0) {
    throw new Error("Tool selection contains unavailable tool IDs.");
  }

  return {
    version: 1,
    mode: "allow",
    tool_ids: registry.map((tool) => tool.name).filter((id) => requestedIds.has(id)),
  };
}

export function assessClientCompatibility(
  toolCount: number,
  target: ClientTarget,
): ClientCompatibilityAssessment {
  if (!Number.isSafeInteger(toolCount) || toolCount < 0) {
    throw new Error("Tool count must be a non-negative integer.");
  }

  const warnings =
    toolCount > 30
      ? [
          `${toolCount} tools exceeds Microsoft's recommended 25–30 tools for Copilot Studio performance and selection quality.`,
        ]
      : [];

  if (target === "copilot_studio" && toolCount > 128) {
    return {
      compatible: false,
      warnings,
      error: `Copilot Studio supports at most 128 tools per agent; this selection contains ${toolCount}.`,
    };
  }

  return { compatible: true, warnings };
}
