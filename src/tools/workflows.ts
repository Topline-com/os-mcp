import { toplineFetch, getLocationId } from "../client.js";
import type { ToolDef } from "./types.js";
import { locationId, obj } from "../schemas.js";

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
