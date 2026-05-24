// Email Campaigns tools — wraps the CRM's email-marketing surface.
//
// 4 umbrella tools cover template management, campaign lifecycle (send /
// schedule), stats, and recipient queries. Each takes an `action`
// discriminator and dispatches to the right HTTP call.
//
// IMPORTANT: endpoint paths in this file are best-effort based on the
// existing catalog hints (/emails/templates, /campaigns/) and standard
// CRM API conventions. They have NOT been verified against the live
// PIT — runtime confirmation happens after merge. If a path is wrong
// the tool returns a clear error from the upstream API and we patch
// here.

import { toplineFetch, getLocationId } from "@topline/shared";
import { obj, objLoose, str, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";

const TEMPLATE_ACTIONS = ["list", "get", "create", "update", "delete"] as const;
const CAMPAIGN_ACTIONS = ["list", "get", "create", "update", "delete", "send", "schedule"] as const;
const STATS_ACTIONS = ["get"] as const;
const RECIPIENTS_ACTIONS = ["list"] as const;

function requireArg<T>(args: Record<string, unknown>, key: string, action: string): T {
  const v = args[key];
  if (v === undefined || v === null || v === "") {
    throw new Error(`'${key}' is required for action='${action}'.`);
  }
  return v as T;
}

export const tools: ToolDef[] = [
  {
    name: "topline_email_template",
    description:
      "Manage saved email templates. Actions: " +
      "`list` (all templates for the location), " +
      "`get` (single template by `templateId`), " +
      "`create` (new template — provide `body` with name, subject, html, etc.), " +
      "`update` (edit an existing template by `templateId`), " +
      "`delete` (remove a template). " +
      "NOTE: endpoint path is best-effort (/emails/builder/); live PIT smoke test pending. If a call returns 404, update the path or open an issue.",
    inputSchema: obj(
      {
        action: { type: "string", enum: TEMPLATE_ACTIONS, description: "Which operation to perform." },
        templateId: str("Template id — required for get / update / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof TEMPLATE_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/emails/builder`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: { locationId: loc } });
        case "get": {
          const id = requireArg<string>(args, "templateId", action);
          return toplineFetch(`${base}/${id}`, { query: { locationId: loc } });
        }
        case "create":
          return toplineFetch(base, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        case "update": {
          const id = requireArg<string>(args, "templateId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "PUT",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        case "delete": {
          const id = requireArg<string>(args, "templateId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "DELETE",
            query: { locationId: loc },
          });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_email_template.`);
      }
    },
  },

  {
    name: "topline_email_campaign",
    description:
      "Manage email campaigns (broadcasts / scheduled sends). Actions: " +
      "`list` (all campaigns for the location), " +
      "`get` (single campaign by `campaignId`), " +
      "`create` (new campaign — provide `body` with name, subject, recipients, template/html, etc.), " +
      "`update` (edit an existing campaign), " +
      "`delete` (remove a campaign), " +
      "`send` (send immediately), " +
      "`schedule` (schedule for a future timestamp — pass `sendAt` in body). " +
      "NOTE: endpoint path is best-effort (/campaigns/); live PIT smoke test pending.",
    inputSchema: obj(
      {
        action: { type: "string", enum: CAMPAIGN_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id — required for get / update / delete / send / schedule."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof CAMPAIGN_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/campaigns`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: { locationId: loc } });
        case "get": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}`, { query: { locationId: loc } });
        }
        case "create":
          return toplineFetch(base, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        case "update": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "PUT",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        case "delete": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "DELETE",
            query: { locationId: loc },
          });
        }
        case "send": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}/send`, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        case "schedule": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}/schedule`, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_email_campaign.`);
      }
    },
  },

  {
    name: "topline_email_campaign_stats",
    description:
      "Aggregated performance stats for an email campaign — sends, opens, clicks, bounces, unsubscribes. Action: `get` by `campaignId`.",
    inputSchema: obj(
      {
        action: { type: "string", enum: STATS_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id."),
        locationId,
      },
      ["action", "campaignId"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const id = requireArg<string>(args, "campaignId", args.action as string);
      return toplineFetch(`/campaigns/${id}/stats`, { query: { locationId: loc } });
    },
  },

  {
    name: "topline_email_campaign_recipients",
    description:
      "List recipients of an email campaign (with per-recipient delivery / engagement status if available). Action: `list` by `campaignId`.",
    inputSchema: obj(
      {
        action: { type: "string", enum: RECIPIENTS_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id."),
        locationId,
      },
      ["action", "campaignId"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const id = requireArg<string>(args, "campaignId", args.action as string);
      return toplineFetch(`/campaigns/${id}/recipients`, { query: { locationId: loc } });
    },
  },
];
