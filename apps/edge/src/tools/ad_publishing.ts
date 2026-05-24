// Ad Publishing tools — wraps the CRM's /ad-publishing/ surface for
// Facebook (incl. Instagram via FB Pages), Google Ads, and LinkedIn Ads.
//
// ~24 umbrella tools cover ~75 documented endpoints. One tool per
// (network, resource) pair, each with an `action` discriminator that
// selects the sub-operation. Handlers validate per-action required
// params and dispatch to the right HTTP call.
//
// White-label note: tool descriptions and comments refer to "the CRM",
// never the vendor by name. Public-repo rule.
//
// Live PIT smoke-testing: catalog previously flagged ad_accounts /
// ad_reports as `requires_oauth` without probe evidence. We expose
// tools optimistically and demote to requires_oauth in catalog if
// runtime calls 401/403.

import { toplineFetch, getLocationId } from "@topline/shared";
import { obj, objLoose, str, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";

function requireArg<T>(args: Record<string, unknown>, key: string, action: string): T {
  const v = args[key];
  if (v === undefined || v === null || v === "") {
    throw new Error(`'${key}' is required for action='${action}'.`);
  }
  return v as T;
}

// ---------------------------------------------------------------------------
// FACEBOOK (also covers Instagram via Facebook Pages)
// ---------------------------------------------------------------------------

const FB_INTEGRATION_ACTIONS = [
  "get",
  "create",
  "delete",
  "get_me",
  "list_pages",
  "get_page_instagram",
  "delete_page",
  "set_default_page",
  "get_lead_form",
] as const;

const FB_AD_ACCOUNT_ACTIONS = ["list", "get", "delete"] as const;
const FB_LEAD_FORM_ACTIONS = [
  "list_page_forms",
  "create_page_form",
  "list_conversation_forms",
  "create_conversation_form",
] as const;
const FB_CAMPAIGN_ACTIONS = ["get", "upsert", "publish", "pause", "resume", "duplicate", "delete"] as const;
const FB_ADSET_ACTIONS = ["upsert", "pause", "resume", "duplicate", "delete"] as const;
const FB_AD_ACTIONS = ["upsert", "pause", "resume", "duplicate", "delete"] as const;
const FB_ENTITY_ACTIONS = ["get"] as const;
const FB_TARGETING_ACTIONS = ["search"] as const;
const FB_PIXEL_ACTIONS = ["list", "upsert"] as const;
const FB_CUSTOM_AUDIENCE_ACTIONS = [
  "list",
  "update",
  "delete",
  "add_member",
  "remove_member",
  "batch_update_members",
] as const;
const FB_REPORTING_ACTIONS = ["data", "campaign", "list"] as const;

// ---------------------------------------------------------------------------
// GOOGLE
// ---------------------------------------------------------------------------

const GOOGLE_INTEGRATION_ACTIONS = ["get", "create", "get_me"] as const;
const GOOGLE_AD_ACCOUNT_ACTIONS = ["list", "get", "delete"] as const;
const GOOGLE_CAMPAIGN_ACTIONS = ["upsert", "get", "publish_ad"] as const;
const GOOGLE_AUDIENCE_ACTIONS = ["list", "get", "upsert", "create_offline_user_list_job"] as const;
const GOOGLE_CONVERSION_ACTIONS = ["list", "upsert", "get", "delete", "list_goals"] as const;
const GOOGLE_TARGETING_ACTIONS = ["search"] as const;
const GOOGLE_REPORTING_ACTIONS = ["data", "list", "campaign"] as const;

// ---------------------------------------------------------------------------
// LINKEDIN
// ---------------------------------------------------------------------------

const LI_INTEGRATION_ACTIONS = ["get", "create", "get_me"] as const;
const LI_AD_ACCOUNT_ACTIONS = ["list", "get", "delete"] as const;
const LI_CAMPAIGN_ACTIONS = ["upsert", "get", "publish", "update_status"] as const;
const LI_LEAD_FORM_ACTIONS = ["list", "create"] as const;
const LI_TARGETING_ACTIONS = ["search"] as const;
const LI_REPORTING_ACTIONS = ["data", "list", "campaign"] as const;

export const tools: ToolDef[] = [
  // -------------------------------------------------------------------------
  // FACEBOOK
  // -------------------------------------------------------------------------

  {
    name: "topline_fb_integration",
    description:
      "Manage the location's Facebook integration (also surfaces Instagram via FB Pages). Actions: " +
      "`get` / `create` / `delete` (the integration itself); " +
      "`get_me` (the authenticated FB user); " +
      "`list_pages` (FB pages available on the integration); " +
      "`get_page_instagram` (Instagram accounts attached to a page — provide `pageId`); " +
      "`delete_page` (remove a page connection); " +
      "`set_default_page` (mark a page as default for publishing); " +
      "`get_lead_form` (single lead form by `leadFormId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_INTEGRATION_ACTIONS, description: "Which operation to perform." },
        pageId: str("Facebook Page id — required for get_page_instagram."),
        leadFormId: str("Lead form id — required for get_lead_form."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_INTEGRATION_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      switch (action) {
        case "get":
          return toplineFetch(`/ad-publishing/facebook/integration`, { query: q });
        case "create":
          return toplineFetch(`/ad-publishing/facebook/integration`, { method: "POST", query: q, body: args.body ?? {} });
        case "delete":
          return toplineFetch(`/ad-publishing/facebook/integration`, { method: "DELETE", query: q });
        case "get_me":
          return toplineFetch(`/ad-publishing/facebook/me`, { query: q });
        case "list_pages":
          return toplineFetch(`/ad-publishing/facebook/pages`, { query: q });
        case "get_page_instagram": {
          const pageId = requireArg<string>(args, "pageId", action);
          return toplineFetch(`/ad-publishing/facebook/page/${pageId}/instagram`, { query: q });
        }
        case "delete_page":
          return toplineFetch(`/ad-publishing/facebook/page`, { method: "DELETE", query: q, body: args.body ?? {} });
        case "set_default_page":
          return toplineFetch(`/ad-publishing/facebook/page/default`, { method: "PUT", query: q, body: args.body ?? {} });
        case "get_lead_form": {
          const id = requireArg<string>(args, "leadFormId", action);
          return toplineFetch(`/ad-publishing/facebook/lead-form/${id}`, { query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_integration.`);
      }
    },
  },

  {
    name: "topline_fb_ad_account",
    description:
      "Manage connected Facebook ad accounts. Actions: `list` (all connected), `get` (by `adAccountId`), `delete` (disconnect).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_AD_ACCOUNT_ACTIONS, description: "Which operation to perform." },
        adAccountId: str("Ad account id — required for get / delete."),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_AD_ACCOUNT_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/ad-accounts`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "get": {
          const id = requireArg<string>(args, "adAccountId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "delete": {
          const id = requireArg<string>(args, "adAccountId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_ad_account.`);
      }
    },
  },

  {
    name: "topline_fb_lead_form",
    description:
      "Manage Facebook lead forms (page-level + conversation-level). Actions: " +
      "`list_page_forms` (by `pageId`), `create_page_form` (by `pageId` + `body`), " +
      "`list_conversation_forms`, `create_conversation_form` (provide `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_LEAD_FORM_ACTIONS, description: "Which operation to perform." },
        pageId: str("Facebook Page id — required for list_page_forms / create_page_form."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_LEAD_FORM_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      switch (action) {
        case "list_page_forms": {
          const pageId = requireArg<string>(args, "pageId", action);
          return toplineFetch(`/ad-publishing/facebook/page/${pageId}/forms`, { query: q });
        }
        case "create_page_form": {
          const pageId = requireArg<string>(args, "pageId", action);
          return toplineFetch(`/ad-publishing/facebook/page/${pageId}/forms`, {
            method: "POST",
            query: q,
            body: args.body ?? {},
          });
        }
        case "list_conversation_forms":
          return toplineFetch(`/ad-publishing/facebook/conversation-forms`, { query: q });
        case "create_conversation_form":
          return toplineFetch(`/ad-publishing/facebook/conversation-forms`, {
            method: "POST",
            query: q,
            body: args.body ?? {},
          });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_lead_form.`);
      }
    },
  },

  {
    name: "topline_fb_campaign",
    description:
      "Manage Facebook ad campaigns. Actions: " +
      "`get` (single campaign with entities by `campaignId`), " +
      "`upsert` (create or update — provide full campaign in `body`), " +
      "`publish` (publish a draft by `campaignId`), " +
      "`pause`, `resume`, `duplicate`, `delete` (by `campaignId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_CAMPAIGN_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id — required for get / publish / pause / resume / duplicate / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_CAMPAIGN_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/campaigns`;
      switch (action) {
        case "get": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`/ad-publishing/facebook/campaign/${id}`, { query: q });
        }
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "publish": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}/publish`, { method: "POST", query: q, body: args.body ?? {} });
        }
        case "pause":
        case "resume":
        case "duplicate": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}/${action}`, { method: "POST", query: q, body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_campaign.`);
      }
    },
  },

  {
    name: "topline_fb_adset",
    description:
      "Manage Facebook ad sets. Actions: `upsert` (create or update — provide ad set in `body`), `pause`, `resume`, `duplicate`, `delete` (by `adSetId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_ADSET_ACTIONS, description: "Which operation to perform." },
        adSetId: str("Ad set id — required for pause / resume / duplicate / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_ADSET_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/adsets`;
      switch (action) {
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "pause":
        case "resume":
        case "duplicate": {
          const id = requireArg<string>(args, "adSetId", action);
          return toplineFetch(`${base}/${id}/${action}`, { method: "POST", query: q, body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "adSetId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_adset.`);
      }
    },
  },

  {
    name: "topline_fb_ad",
    description:
      "Manage Facebook ad creatives. Actions: `upsert` (create or update — provide ad in `body`), `pause`, `resume`, `duplicate`, `delete` (by `adId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_AD_ACTIONS, description: "Which operation to perform." },
        adId: str("Ad id — required for pause / resume / duplicate / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_AD_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/ads`;
      switch (action) {
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "pause":
        case "resume":
        case "duplicate": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}/${action}`, { method: "POST", query: q, body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_ad.`);
      }
    },
  },

  {
    name: "topline_fb_entity",
    description:
      "Generic Facebook entity lookup endpoint. Action: `get` (returns the requested entity — pass identifying params in `query`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_ENTITY_ACTIONS, description: "Which operation to perform." },
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      return toplineFetch(`/ad-publishing/facebook/entity`, { query });
    },
  },

  {
    name: "topline_fb_targeting",
    description:
      "Search Facebook targeting options — geo locations, interests, behaviors, demographics. Action: `search` (pass search params like `query`, `type`, `country` in `body` or `query`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_TARGETING_ACTIONS, description: "Which operation to perform." },
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      return toplineFetch(`/ad-publishing/facebook/targeting/search`, { query });
    },
  },

  {
    name: "topline_fb_pixel",
    description:
      "Manage Facebook conversion pixels for the location. Actions: `list`, `upsert` (create or update — provide pixel config in `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_PIXEL_ACTIONS, description: "Which operation to perform." },
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_PIXEL_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/pixels`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_pixel.`);
      }
    },
  },

  {
    name: "topline_fb_custom_audience",
    description:
      "Manage Facebook custom audiences and their members. Actions: " +
      "`list`, " +
      "`update` (by `audienceId`), " +
      "`delete` (by `audienceId`), " +
      "`add_member` (single — by `audienceId` + member info in `body`), " +
      "`remove_member` (single — by `audienceId` + member info in `body`), " +
      "`batch_update_members` (bulk — by `audienceId` + member array in `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_CUSTOM_AUDIENCE_ACTIONS, description: "Which operation to perform." },
        audienceId: str("Audience id — required for update / delete / add_member / remove_member / batch_update_members."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_CUSTOM_AUDIENCE_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/facebook/custom-audience`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "update": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}`, { method: "PUT", query: q, body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        case "add_member": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}/member`, { method: "PUT", query: q, body: args.body ?? {} });
        }
        case "remove_member": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}/member`, { method: "DELETE", query: q, body: args.body ?? {} });
        }
        case "batch_update_members": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}/member/batch`, { method: "PUT", query: q, body: args.body ?? {} });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_custom_audience.`);
      }
    },
  },

  {
    name: "topline_fb_reporting",
    description:
      "Facebook ads reporting. Actions: " +
      "`data` (aggregated metrics across campaigns / ad sets / ads — pass filters in `query`), " +
      "`campaign` (metrics for a specific campaign by `campaignId`), " +
      "`list` (list campaigns / ad sets / ads with reporting data — pass filters in `query`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: FB_REPORTING_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id — required for action='campaign'."),
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof FB_REPORTING_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      switch (action) {
        case "data":
          return toplineFetch(`/ad-publishing/facebook/reporting`, { query });
        case "campaign": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`/ad-publishing/facebook/reporting/campaign/${id}`, { query });
        }
        case "list":
          return toplineFetch(`/ad-publishing/facebook/reporting/list`, { query });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_fb_reporting.`);
      }
    },
  },

  // -------------------------------------------------------------------------
  // GOOGLE
  // -------------------------------------------------------------------------

  {
    name: "topline_google_integration",
    description:
      "Manage the location's Google Ads integration. Actions: `get`, `create` (provide auth payload in `body`), `get_me` (authenticated Google user).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_INTEGRATION_ACTIONS, description: "Which operation to perform." },
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_INTEGRATION_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      switch (action) {
        case "get":
          return toplineFetch(`/ad-publishing/google/integration`, { query: q });
        case "create":
          return toplineFetch(`/ad-publishing/google/integration`, { method: "POST", query: q, body: args.body ?? {} });
        case "get_me":
          return toplineFetch(`/ad-publishing/google/me`, { query: q });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_integration.`);
      }
    },
  },

  {
    name: "topline_google_ad_account",
    description:
      "Manage connected Google ad accounts. Actions: `list` (all connected), `get` (by `adAccountId`), `delete` (disconnect).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_AD_ACCOUNT_ACTIONS, description: "Which operation to perform." },
        adAccountId: str("Ad account id — required for get / delete."),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_AD_ACCOUNT_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/google/ad-accounts`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "get": {
          const id = requireArg<string>(args, "adAccountId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "delete": {
          const id = requireArg<string>(args, "adAccountId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_ad_account.`);
      }
    },
  },

  {
    name: "topline_google_campaign",
    description:
      "Manage Google Ads campaigns (creates or updates the full structure: campaign, ad groups, ads, keywords). Actions: " +
      "`upsert` (provide full campaign in `body`), " +
      "`get` (by `adId`), " +
      "`publish_ad` (publish a draft ad by `adId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_CAMPAIGN_ACTIONS, description: "Which operation to perform." },
        adId: str("Ad id — required for get / publish_ad."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_CAMPAIGN_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/google/ads`;
      switch (action) {
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "get": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "publish_ad": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}/publish`, { method: "POST", query: q, body: args.body ?? {} });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_campaign.`);
      }
    },
  },

  {
    name: "topline_google_audience",
    description:
      "Manage Google Ads audiences. Actions: " +
      "`list` (all combined audiences), " +
      "`get` (by `audienceId`), " +
      "`upsert` (create or update — provide audience config in `body`), " +
      "`create_offline_user_list_job` (offline-conversion user-list upload job — provide job config in `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_AUDIENCE_ACTIONS, description: "Which operation to perform." },
        audienceId: str("Audience id — required for get."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_AUDIENCE_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/google/audiences`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "get": {
          const id = requireArg<string>(args, "audienceId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "create_offline_user_list_job":
          return toplineFetch(`/ad-publishing/google/segments/offline-user-list-job`, {
            method: "POST",
            query: q,
            body: args.body ?? {},
          });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_audience.`);
      }
    },
  },

  {
    name: "topline_google_conversion",
    description:
      "Manage Google Ads conversion actions and conversion goals. Actions: " +
      "`list` (all conversions), " +
      "`upsert` (create or update — provide conversion config in `body`), " +
      "`get` (by `conversionId`), " +
      "`delete` (by `conversionId`), " +
      "`list_goals` (available conversion goals for the location).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_CONVERSION_ACTIONS, description: "Which operation to perform." },
        conversionId: str("Conversion id — required for get / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_CONVERSION_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/google/conversions`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: q });
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "get": {
          const id = requireArg<string>(args, "conversionId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "delete": {
          const id = requireArg<string>(args, "conversionId", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE", query: q });
        }
        case "list_goals":
          return toplineFetch(`/ad-publishing/google/conversion-goals`, { query: q });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_conversion.`);
      }
    },
  },

  {
    name: "topline_google_targeting",
    description:
      "Search Google Ads targeting options. Action: `search` (pass search params in `query`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_TARGETING_ACTIONS, description: "Which operation to perform." },
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      return toplineFetch(`/ad-publishing/google/targeting`, { query });
    },
  },

  {
    name: "topline_google_reporting",
    description:
      "Google Ads reporting. Actions: " +
      "`data` (aggregated metrics across campaigns — pass filters in `query`), " +
      "`list` (list with reporting data), " +
      "`campaign` (metrics for a specific campaign by `campaignId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: GOOGLE_REPORTING_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id — required for action='campaign'."),
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof GOOGLE_REPORTING_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      switch (action) {
        case "data":
          return toplineFetch(`/ad-publishing/google/reporting`, { query });
        case "list":
          return toplineFetch(`/ad-publishing/google/reporting/list`, { query });
        case "campaign": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`/ad-publishing/google/reporting/campaign/${id}`, { query });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_google_reporting.`);
      }
    },
  },

  // -------------------------------------------------------------------------
  // LINKEDIN
  // -------------------------------------------------------------------------

  {
    name: "topline_linkedin_integration",
    description:
      "Manage the location's LinkedIn Ads integration. Actions: `get`, `create` (provide auth payload in `body`), `get_me` (authenticated LinkedIn user).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_INTEGRATION_ACTIONS, description: "Which operation to perform." },
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LI_INTEGRATION_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      switch (action) {
        case "get":
          return toplineFetch(`/ad-publishing/linkedin/integration`, { query: q });
        case "create":
          return toplineFetch(`/ad-publishing/linkedin/integration`, { method: "POST", query: q, body: args.body ?? {} });
        case "get_me":
          return toplineFetch(`/ad-publishing/linkedin/me`, { query: q });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_linkedin_integration.`);
      }
    },
  },

  {
    name: "topline_linkedin_ad_account",
    description:
      "Manage connected LinkedIn ad accounts. Actions: `list`, `get` (current/details), `delete` (disconnect).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_AD_ACCOUNT_ACTIONS, description: "Which operation to perform." },
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LI_AD_ACCOUNT_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      switch (action) {
        case "list":
          return toplineFetch(`/ad-publishing/linkedin/ad-accounts`, { query: q });
        case "get":
          return toplineFetch(`/ad-publishing/linkedin/ad-account`, { query: q });
        case "delete":
          return toplineFetch(`/ad-publishing/linkedin/ad-account`, { method: "DELETE", query: q });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_linkedin_ad_account.`);
      }
    },
  },

  {
    name: "topline_linkedin_campaign",
    description:
      "Manage LinkedIn ad campaign groups (with nested campaigns and ads). Actions: " +
      "`upsert` (create or update — provide full campaign group in `body`), " +
      "`get` (by `adId` — returns the campaign group + nested campaigns / ad groups / ads), " +
      "`publish` (publish a draft by `adId`), " +
      "`update_status` (pause, resume, archive an ad or campaign by `adId` — provide status in `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_CAMPAIGN_ACTIONS, description: "Which operation to perform." },
        adId: str("Campaign-group ad id — required for get / publish / update_status."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LI_CAMPAIGN_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const base = `/ad-publishing/linkedin/ads`;
      switch (action) {
        case "upsert":
          return toplineFetch(base, { method: "PUT", query: q, body: args.body ?? {} });
        case "get": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}`, { query: q });
        }
        case "publish": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`${base}/${id}/publish`, { method: "POST", query: q, body: args.body ?? {} });
        }
        case "update_status": {
          const id = requireArg<string>(args, "adId", action);
          return toplineFetch(`/ad-publishing/linkedin/${id}/status`, {
            method: "PATCH",
            query: q,
            body: args.body ?? {},
          });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_linkedin_campaign.`);
      }
    },
  },

  {
    name: "topline_linkedin_lead_form",
    description:
      "Manage LinkedIn lead forms per ad account. Actions: `list` (by `accountId`), `create` (by `accountId` + form config in `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_LEAD_FORM_ACTIONS, description: "Which operation to perform." },
        accountId: str("LinkedIn ad account id."),
        body: objLoose({}, []),
        locationId,
      },
      ["action", "accountId"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LI_LEAD_FORM_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const q = { locationId: loc };
      const accountId = requireArg<string>(args, "accountId", action);
      switch (action) {
        case "list":
          return toplineFetch(`/ad-publishing/linkedin/${accountId}/forms`, { query: q });
        case "create":
          return toplineFetch(`/ad-publishing/linkedin/${accountId}/form`, {
            method: "POST",
            query: q,
            body: args.body ?? {},
          });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_linkedin_lead_form.`);
      }
    },
  },

  {
    name: "topline_linkedin_targeting",
    description:
      "Search LinkedIn targeting options (location, industry, job title facets). Action: `search` (pass search params in `query`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_TARGETING_ACTIONS, description: "Which operation to perform." },
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      return toplineFetch(`/ad-publishing/linkedin/targeting/search`, { query });
    },
  },

  {
    name: "topline_linkedin_reporting",
    description:
      "LinkedIn Ads reporting. Actions: " +
      "`data` (aggregated metrics across campaign groups / campaigns / ads), " +
      "`list` (list with reporting data — pass filters in `query`), " +
      "`campaign` (metrics for a specific campaign group by `campaignId`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LI_REPORTING_ACTIONS, description: "Which operation to perform." },
        campaignId: str("Campaign id — required for action='campaign'."),
        query: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LI_REPORTING_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: loc,
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      switch (action) {
        case "data":
          return toplineFetch(`/ad-publishing/linkedin/reporting`, { query });
        case "list":
          return toplineFetch(`/ad-publishing/linkedin/reporting/list`, { query });
        case "campaign": {
          const id = requireArg<string>(args, "campaignId", action);
          return toplineFetch(`/ad-publishing/linkedin/reporting/campaign/${id}`, { query });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_linkedin_reporting.`);
      }
    },
  },
];
