import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, formId, str, obj, limitProp, startAfterIdProp } from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_forms",
    description: "List all forms on the sub-account.",
    inputSchema: obj({ locationId, limit: limitProp, startAfterId: startAfterIdProp }),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: getLocationId(args.locationId as string | undefined),
      };
      if (args.limit) query.limit = args.limit as number;
      if (args.startAfterId) query.skip = String(args.startAfterId);
      return await toplineFetch("/forms/", { query });
    },
  },
  {
    name: "topline_list_form_submissions",
    description: "List submissions for a specific form.",
    inputSchema: obj(
      {
        formId,
        limit: limitProp,
        startAt: str("ISO date — only submissions after this date"),
        endAt: str("ISO date — only submissions before this date"),
        locationId,
      },
      ["formId"],
    ),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: getLocationId(args.locationId as string | undefined),
        formId: String(args.formId),
      };
      if (args.limit) query.limit = args.limit as number;
      if (args.startAt) query.startAt = String(args.startAt);
      if (args.endAt) query.endAt = String(args.endAt);
      return await toplineFetch("/forms/submissions", { query });
    },
  },
];
