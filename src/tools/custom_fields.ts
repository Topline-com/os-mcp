import { toplineFetch, getLocationId } from "../client.js";
import type { ToolDef } from "./types.js";
import { locationId, str, obj } from "../schemas.js";

const customFieldId = str("Custom field ID");

export const tools: ToolDef[] = [
  {
    name: "topline_list_custom_fields",
    description:
      "List all custom fields configured on the sub-account (for contacts and opportunities). Useful before creating/updating contacts with custom data.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customFields`);
    },
  },
  {
    name: "topline_get_custom_field",
    description: "Get details of a single custom field.",
    inputSchema: obj({ customFieldId, locationId }, ["customFieldId"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customFields/${args.customFieldId}`);
    },
  },
];
