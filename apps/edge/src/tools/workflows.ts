import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, obj } from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_workflows",
    description: "List all workflows in the sub-account. Returns workflow IDs, names, and status.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const query = { locationId: getLocationId(args.locationId as string | undefined) };
      return await toplineFetch("/workflows/", { query });
    },
  },
];
