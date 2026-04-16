import { toplineFetch, getLocationId } from "../client.js";
import type { ToolDef } from "./types.js";
import { locationId, obj } from "../schemas.js";

export const tools: ToolDef[] = [
  {
    name: "topline_list_tags",
    description: "List all tags available on the sub-account.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/tags`);
    },
  },
];
