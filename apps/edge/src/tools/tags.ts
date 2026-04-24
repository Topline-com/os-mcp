import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, str, obj } from "@topline/shared";

const tagId = str("Tag ID");

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
  {
    name: "topline_create_tag",
    description:
      "Create a new tag on the sub-account. Tags are referenced by NAME from contact.tags JSON arrays, so use a concise human-readable name.",
    inputSchema: obj(
      { name: str("Tag display name, e.g. 'VIP' or 'needs-followup'"), locationId },
      ["name"],
    ),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/tags`, {
        method: "POST",
        body: { name: args.name },
      });
    },
  },
  {
    name: "topline_update_tag",
    description: "Rename a tag. Existing contacts tagged with the old name get the rename transitively.",
    inputSchema: obj({ tagId, name: str("New tag name"), locationId }, ["tagId", "name"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/tags/${args.tagId}`, {
        method: "PUT",
        body: { name: args.name },
      });
    },
  },
  {
    name: "topline_delete_tag",
    description:
      "Delete a tag from the sub-account. Contacts that were tagged lose the tag reference; contact records themselves are not touched.",
    inputSchema: obj({ tagId, locationId }, ["tagId"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/tags/${args.tagId}`, {
        method: "DELETE",
      });
    },
  },
];
