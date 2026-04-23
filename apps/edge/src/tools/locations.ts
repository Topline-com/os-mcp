import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, obj } from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_get_location",
    description: "Fetch full details of the current sub-account (location) — name, address, timezone, business info.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}`);
    },
  },
];
