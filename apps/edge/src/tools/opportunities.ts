import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import {
  contactId,
  opportunityId,
  pipelineId,
  pipelineStageId,
  locationId,
  str,
  num,
  arr,
  obj,
  limitProp,
  startAfterIdProp,
} from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_pipelines",
    description: "List all opportunity pipelines in the sub-account with their stages.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const query = { locationId: getLocationId(args.locationId as string | undefined) };
      return await toplineFetch("/opportunities/pipelines", { query });
    },
  },
  {
    name: "topline_search_opportunities",
    description: "Search opportunities. Filter by pipeline, stage, status, contact, or free text.",
    inputSchema: obj({
      query: str("Free-text search"),
      pipelineId: str("Restrict to one pipeline"),
      pipelineStageId: str("Restrict to one stage"),
      assignedTo: str("User ID"),
      contactId: str(),
      status: { type: "string", enum: ["open", "won", "lost", "abandoned", "all"] },
      limit: limitProp,
      startAfterId: startAfterIdProp,
      locationId,
    }),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        location_id: getLocationId(args.locationId as string | undefined),
        limit: (args.limit as number) ?? 25,
      };
      for (const [k, apiK] of [
        ["query", "q"],
        ["pipelineId", "pipeline_id"],
        ["pipelineStageId", "pipeline_stage_id"],
        ["assignedTo", "assigned_to"],
        ["contactId", "contact_id"],
        ["status", "status"],
        ["startAfterId", "startAfterId"],
      ] as const) {
        const v = args[k];
        if (v !== undefined) query[apiK] = String(v);
      }
      return await toplineFetch("/opportunities/search", { query });
    },
  },
  {
    name: "topline_get_opportunity",
    description: "Fetch a single opportunity by ID.",
    inputSchema: obj({ opportunityId }, ["opportunityId"]),
    handler: async (args) => toplineFetch(`/opportunities/${args.opportunityId}`),
  },
  {
    name: "topline_create_opportunity",
    description: "Create a new opportunity in a pipeline stage, associated with a contact.",
    inputSchema: obj(
      {
        pipelineId,
        pipelineStageId,
        contactId,
        name: str("Opportunity name / title"),
        monetaryValue: num("Deal value in USD"),
        status: { type: "string", enum: ["open", "won", "lost", "abandoned"] },
        assignedTo: str("User ID to assign to"),
        tags: arr({ type: "string" }),
        locationId,
      },
      ["pipelineId", "pipelineStageId", "contactId", "name"],
    ),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        locationId: getLocationId(args.locationId as string | undefined),
        pipelineId: args.pipelineId,
        pipelineStageId: args.pipelineStageId,
        contactId: args.contactId,
        name: args.name,
        status: args.status ?? "open",
      };
      if (args.monetaryValue !== undefined) body.monetaryValue = args.monetaryValue;
      if (args.assignedTo) body.assignedTo = args.assignedTo;
      if (args.tags) body.tags = args.tags;
      return await toplineFetch("/opportunities/", { method: "POST", body });
    },
  },
  {
    name: "topline_update_opportunity",
    description: "Update an opportunity — move stages, change value, mark won/lost, etc.",
    inputSchema: obj(
      {
        opportunityId,
        name: str(),
        pipelineStageId: str(),
        monetaryValue: num(),
        status: { type: "string", enum: ["open", "won", "lost", "abandoned"] },
        assignedTo: str(),
      },
      ["opportunityId"],
    ),
    handler: async (args) => {
      const { opportunityId: id, ...rest } = args;
      return await toplineFetch(`/opportunities/${id}`, { method: "PUT", body: rest });
    },
  },
  {
    name: "topline_delete_opportunity",
    description: "Delete an opportunity.",
    inputSchema: obj({ opportunityId }, ["opportunityId"]),
    handler: async (args) => toplineFetch(`/opportunities/${args.opportunityId}`, { method: "DELETE" }),
  },
];
