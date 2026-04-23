import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, surveyId, str, obj, limitProp, startAfterIdProp } from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_surveys",
    description: "List all surveys on the sub-account.",
    inputSchema: obj({ locationId, limit: limitProp, startAfterId: startAfterIdProp }),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: getLocationId(args.locationId as string | undefined),
      };
      if (args.limit) query.limit = args.limit as number;
      if (args.startAfterId) query.skip = String(args.startAfterId);
      return await toplineFetch("/surveys/", { query });
    },
  },
  {
    name: "topline_list_survey_submissions",
    description: "List submissions for a specific survey.",
    inputSchema: obj(
      {
        surveyId,
        limit: limitProp,
        startAt: str("ISO date"),
        endAt: str("ISO date"),
        locationId,
      },
      ["surveyId"],
    ),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: getLocationId(args.locationId as string | undefined),
        surveyId: String(args.surveyId),
      };
      if (args.limit) query.limit = args.limit as number;
      if (args.startAt) query.startAt = String(args.startAt);
      if (args.endAt) query.endAt = String(args.endAt);
      return await toplineFetch("/surveys/submissions", { query });
    },
  },
];
