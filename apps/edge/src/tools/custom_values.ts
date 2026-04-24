// Custom-values tools — sub-account-scoped merge-tag key/value pairs.
// Not per-contact; these are referenced in workflows/messages as
// {{custom_values.some_key}}.
//
// Live-probed 2026-04-24: POST / PUT / DELETE all reachable under PIT
// at /locations/{locationId}/customValues/*. Field shape inferred from
// 422 responses: `name` (required string), `value` (string).

import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, str, obj } from "@topline/shared";

const customValueId = str("Custom value ID");

export const tools: ToolDef[] = [
  {
    name: "topline_list_custom_values",
    description:
      "List all sub-account custom values. Use before setting workflow merge tags or composing messages that reference {{custom_values.X}}.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customValues`);
    },
  },
  {
    name: "topline_get_custom_value",
    description: "Get details of one custom value by id.",
    inputSchema: obj({ customValueId, locationId }, ["customValueId"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customValues/${args.customValueId}`);
    },
  },
  {
    name: "topline_create_custom_value",
    description:
      "Create a new sub-account custom value. Returns the created record including its id and the auto-generated merge-tag key ({{custom_values.<slug>}}).",
    inputSchema: obj(
      {
        name: str("Display name (e.g. 'Review Request Link')"),
        value: str("The value to store. Used verbatim when merge tags are expanded."),
        locationId,
      },
      ["name", "value"],
    ),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      const { locationId: _, ...body } = args;
      return await toplineFetch(`/locations/${id}/customValues`, {
        method: "POST",
        body,
      });
    },
  },
  {
    name: "topline_update_custom_value",
    description:
      "Update an existing custom value's name and/or value. Pass only the fields you want to change.",
    inputSchema: obj(
      {
        customValueId,
        name: str("New display name"),
        value: str("New stored value"),
        locationId,
      },
      ["customValueId"],
    ),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      const { customValueId: cvid, locationId: _, ...body } = args;
      return await toplineFetch(`/locations/${id}/customValues/${cvid}`, {
        method: "PUT",
        body,
      });
    },
  },
  {
    name: "topline_delete_custom_value",
    description:
      "Delete a sub-account custom value. Any merge tags referencing the deleted key will render empty in workflows/messages.",
    inputSchema: obj({ customValueId, locationId }, ["customValueId"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customValues/${args.customValueId}`, {
        method: "DELETE",
      });
    },
  },
];
