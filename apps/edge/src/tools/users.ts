import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, str, obj } from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_users",
    description: "List users on the sub-account. Useful for finding user IDs to assign tasks, opportunities, or appointments.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const query = { locationId: getLocationId(args.locationId as string | undefined) };
      return await toplineFetch("/users/", { query });
    },
  },
  {
    name: "topline_get_user",
    description: "Fetch a single user by ID.",
    inputSchema: obj({ userId: str("User ID") }, ["userId"]),
    handler: async (args) => toplineFetch(`/users/${args.userId}`),
  },
];
