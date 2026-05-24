// Social Planner tools — wraps the CRM's /social-media-posting/ surface.
//
// 4 umbrella tools cover 26 documented endpoints. Each tool takes an
// `action` discriminator. Handlers validate that the action's required
// params are present and dispatch to the right HTTP call.
//
// White-label note: tool descriptions and comments refer to "the CRM" /
// "the connected CRM", never the vendor by name. Public-repo rule.

import { toplineFetch, getLocationId } from "@topline/shared";
import { obj, objLoose, str, arr, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";

const POST_ACTIONS = ["list", "get", "create", "update", "delete", "bulk_delete"] as const;
const ACCOUNT_ACTIONS = ["list", "delete"] as const;
const CSV_ACTIONS = ["upload", "list", "get", "finalize", "delete", "assign_accounts"] as const;
const OAUTH_ACTIONS = ["start", "get_accounts", "attach_accounts"] as const;
const OAUTH_PLATFORMS = ["facebook", "instagram", "linkedin", "twitter"] as const;

function requireArg<T>(args: Record<string, unknown>, key: string, action: string): T {
  const v = args[key];
  if (v === undefined || v === null || v === "") {
    throw new Error(`'${key}' is required for action='${action}'.`);
  }
  return v as T;
}

export const tools: ToolDef[] = [
  {
    name: "topline_social_post",
    description:
      "Manage scheduled and published social media posts across connected accounts (Facebook, Instagram, LinkedIn, X, Google Business Profile, TikTok). Actions: " +
      "`list` (search posts by date / account / status, POSTs the filter body), " +
      "`get` (single post by id), " +
      "`create` (schedule a new post; provide `accountIds`, `summary`, optional `scheduleDate`, attachments, etc. in the body), " +
      "`update` (edit an existing post), " +
      "`delete` (remove a single post), " +
      "`bulk_delete` (remove multiple posts by `postIds`). Pass `action` plus the action's required params.",
    inputSchema: obj(
      {
        action: { type: "string", enum: POST_ACTIONS, description: "Which operation to perform." },
        id: str("Post id — required for get / update / delete."),
        postIds: arr({ type: "string" }, "Array of post ids — required for bulk_delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof POST_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/social-media-posting/${loc}/posts`;
      switch (action) {
        case "list":
          return toplineFetch(`${base}/list`, { method: "POST", body: args.body ?? {} });
        case "get": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`);
        }
        case "create":
          return toplineFetch(base, { method: "POST", body: args.body ?? {} });
        case "update": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`, { method: "PUT", body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE" });
        }
        case "bulk_delete": {
          const ids = requireArg<string[]>(args, "postIds", action);
          return toplineFetch(`${base}/bulk-delete`, { method: "POST", body: { postIds: ids } });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_social_post.`);
      }
    },
  },

  {
    name: "topline_social_account",
    description:
      "Manage connected social-media accounts for a location. Actions: " +
      "`list` (returns connected accounts + groups), " +
      "`delete` (disconnect an account by id). " +
      "Note: connecting (oauth) lives on the topline_social_oauth tool.",
    inputSchema: obj(
      {
        action: { type: "string", enum: ACCOUNT_ACTIONS, description: "Which operation to perform." },
        id: str("Account id — required for delete."),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof ACCOUNT_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/social-media-posting/${loc}/accounts`;
      switch (action) {
        case "list":
          return toplineFetch(base);
        case "delete": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE" });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_social_account.`);
      }
    },
  },

  {
    name: "topline_social_csv",
    description:
      "Bulk-import social posts via CSV. Workflow: upload → assign_accounts → finalize. Actions: " +
      "`upload` (POST a CSV file or URL in the body), " +
      "`list` (all imports), " +
      "`get` (single import by id), " +
      "`assign_accounts` (link the upload to one or more connected accounts), " +
      "`finalize` (PATCH to schedule all posts from the import), " +
      "`delete` (remove an import).",
    inputSchema: obj(
      {
        action: { type: "string", enum: CSV_ACTIONS, description: "Which operation to perform." },
        id: str("CSV import id — required for get / finalize / delete."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof CSV_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/social-media-posting/${loc}/csv`;
      switch (action) {
        case "upload":
          return toplineFetch(base, { method: "POST", body: args.body ?? {} });
        case "list":
          return toplineFetch(base);
        case "get": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`);
        }
        case "finalize": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`, { method: "PATCH", body: args.body ?? {} });
        }
        case "delete": {
          const id = requireArg<string>(args, "id", action);
          return toplineFetch(`${base}/${id}`, { method: "DELETE" });
        }
        case "assign_accounts":
          return toplineFetch(`/social-media-posting/${loc}/set-accounts`, {
            method: "POST",
            body: args.body ?? {},
          });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_social_csv.`);
      }
    },
  },

  {
    name: "topline_social_oauth",
    description:
      "Connect a social-media account via OAuth. Three-step flow per platform: " +
      "`start` returns a URL the user must open in a browser to authorize; " +
      "`get_accounts` fetches the accounts/pages available after the user authorizes (call with the `accountId` returned by the platform's callback); " +
      "`attach_accounts` finalizes the connection by attaching one or more of those accounts/pages to this location. " +
      "Supported platforms: `facebook`, `instagram`, `linkedin`, `twitter` (deprecated — X access is best-effort).",
    inputSchema: obj(
      {
        action: { type: "string", enum: OAUTH_ACTIONS, description: "Which step of the OAuth flow." },
        platform: { type: "string", enum: OAUTH_PLATFORMS, description: "Which platform to connect." },
        accountId: str("Platform-side account id returned by the OAuth callback — required for get_accounts and attach_accounts."),
        body: objLoose({}, []),
        locationId,
      },
      ["action", "platform"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof OAUTH_ACTIONS)[number];
      const platform = args.platform as (typeof OAUTH_PLATFORMS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      switch (action) {
        case "start":
          return toplineFetch(`/social-media-posting/oauth/${platform}/start`, {
            query: { locationId: loc },
          });
        case "get_accounts": {
          const accountId = requireArg<string>(args, "accountId", action);
          return toplineFetch(
            `/social-media-posting/oauth/${loc}/${platform}/accounts/${accountId}`,
          );
        }
        case "attach_accounts": {
          const accountId = requireArg<string>(args, "accountId", action);
          return toplineFetch(
            `/social-media-posting/oauth/${loc}/${platform}/accounts/${accountId}`,
            { method: "POST", body: args.body ?? {} },
          );
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_social_oauth.`);
      }
    },
  },
];
