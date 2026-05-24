// Agent Studio tools — wraps the CRM's /agent-studio/ surface.
//
// 3 umbrella tools cover 11 documented endpoints (8 active + 3 deprecated).
// Each tool takes an `action` discriminator. Handlers validate that the
// action's required params are present and dispatch to the right HTTP call.
//
// White-label note: tool descriptions and comments refer to "the CRM's
// Agent Studio", never the vendor by name. Public-repo rule.

import { toplineFetch, getLocationId } from "@topline/shared";
import { obj, objLoose, str, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";

const AGENT_ACTIONS = ["list", "get", "create", "update_metadata", "delete", "execute"] as const;
const VERSION_ACTIONS = ["update", "publish"] as const;
const LEGACY_ACTIONS = ["list", "get", "execute"] as const;

function requireArg<T>(args: Record<string, unknown>, key: string, action: string): T {
  const v = args[key];
  if (v === undefined || v === null || v === "") {
    throw new Error(`'${key}' is required for action='${action}'.`);
  }
  return v as T;
}

export const tools: ToolDef[] = [
  {
    name: "topline_agent",
    description:
      "Manage AI agents in the CRM's Agent Studio. Actions: " +
      "`list` (all agents for the location), " +
      "`get` (single agent with metadata + versions by `agentId`), " +
      "`create` (new agent + initial draft version — provide the agent config in `body`), " +
      "`update_metadata` (rename or update description on an existing agent), " +
      "`delete` (remove an agent and all its versions), " +
      "`execute` (run an agent synchronously with input params — provide `agentId` + `body`).",
    inputSchema: obj(
      {
        action: { type: "string", enum: AGENT_ACTIONS, description: "Which operation to perform." },
        agentId: str("Agent id — required for get / update_metadata / delete / execute."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof AGENT_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/agent-studio/agent`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: { locationId: loc } });
        case "get": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}`, { query: { locationId: loc } });
        }
        case "create":
          return toplineFetch(base, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        case "update_metadata": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "PATCH",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        case "delete": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}`, {
            method: "DELETE",
            query: { locationId: loc },
          });
        }
        case "execute": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}/execute`, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_agent.`);
      }
    },
  },

  {
    name: "topline_agent_version",
    description:
      "Manage individual versions of an agent — drafts and the production-promoted version. Actions: " +
      "`update` (PATCH details of a specific version by `versionId` — system prompt, tools, model, persona, voice, etc.), " +
      "`publish` (promote a draft version to production).",
    inputSchema: obj(
      {
        action: { type: "string", enum: VERSION_ACTIONS, description: "Which operation to perform." },
        versionId: str("Version id — required for update / publish."),
        body: objLoose({}, []),
        locationId,
      },
      ["action", "versionId"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof VERSION_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const versionId = requireArg<string>(args, "versionId", action);
      switch (action) {
        case "update":
          return toplineFetch(`/agent-studio/agent/versions/${versionId}`, {
            method: "PATCH",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        case "publish":
          return toplineFetch(`/agent-studio/agent/versions/${versionId}/publish`, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        default:
          throw new Error(`Unknown action '${action as string}' for topline_agent_version.`);
      }
    },
  },

  {
    name: "topline_agent_legacy",
    description:
      "DEPRECATED — use topline_agent for new work. Legacy public-api endpoints kept for back-compat with older integrations. Actions: " +
      "`list` (all agents), " +
      "`get` (single agent by `agentId`), " +
      "`execute` (synchronous run with input).",
    inputSchema: obj(
      {
        action: { type: "string", enum: LEGACY_ACTIONS, description: "Which operation to perform." },
        agentId: str("Agent id — required for get / execute."),
        body: objLoose({}, []),
        locationId,
      },
      ["action"],
    ),
    handler: async (args) => {
      const action = args.action as (typeof LEGACY_ACTIONS)[number];
      const loc = getLocationId(args.locationId as string | undefined);
      const base = `/agent-studio/public-api/agents`;
      switch (action) {
        case "list":
          return toplineFetch(base, { query: { locationId: loc } });
        case "get": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}`, { query: { locationId: loc } });
        }
        case "execute": {
          const id = requireArg<string>(args, "agentId", action);
          return toplineFetch(`${base}/${id}/execute`, {
            method: "POST",
            query: { locationId: loc },
            body: args.body ?? {},
          });
        }
        default:
          throw new Error(`Unknown action '${action as string}' for topline_agent_legacy.`);
      }
    },
  },
];
