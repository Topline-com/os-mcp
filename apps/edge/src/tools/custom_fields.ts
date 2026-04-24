import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { locationId, str, obj, arr } from "@topline/shared";

const customFieldId = str("Custom field ID");

// Accepted dataType values per live probe 2026-04-24 (422 response listed them):
//   TEXT, LARGE_TEXT, NUMERICAL, PHONE, MONETORY, CHECKBOX,
//   SINGLE_OPTIONS, MULTIPLE_OPTIONS, DATE, TEXTBOX_LIST, RADIO,
//   FILE_UPLOAD, SIGNATURE
const dataTypeDesc =
  "Field data type. One of: TEXT | LARGE_TEXT | NUMERICAL | PHONE | MONETORY | CHECKBOX | SINGLE_OPTIONS | MULTIPLE_OPTIONS | DATE | TEXTBOX_LIST | RADIO | FILE_UPLOAD | SIGNATURE.";

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
  {
    name: "topline_create_custom_field",
    description:
      "Create a new custom field on the sub-account. After creation, the returned `id` can be referenced when setting custom-field values on contacts or opportunities. For SINGLE_OPTIONS / MULTIPLE_OPTIONS / RADIO / CHECKBOX fields, supply `options` as an array of {label, value} pairs.",
    inputSchema: obj(
      {
        name: str("Display name (e.g. 'Deal Priority')"),
        dataType: str(dataTypeDesc),
        model: str("Object this field applies to: 'contact' or 'opportunity'. Default 'contact'."),
        placeholder: str("Placeholder text shown on the field UI"),
        position: str("Render order (integer-as-string)"),
        options: arr(
          obj({ label: str("Option label"), value: str("Option value (slug)") }, ["label", "value"]),
          "For SINGLE_OPTIONS / MULTIPLE_OPTIONS / RADIO / CHECKBOX: list of pickable values.",
        ),
        locationId,
      },
      ["name", "dataType"],
    ),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      const { locationId: _, ...body } = args;
      return await toplineFetch(`/locations/${id}/customFields`, {
        method: "POST",
        body,
      });
    },
  },
  {
    name: "topline_update_custom_field",
    description:
      "Update a custom field's name / options / placeholder. Pass only the fields you want to change.",
    inputSchema: obj(
      {
        customFieldId,
        name: str("New display name"),
        placeholder: str("New placeholder"),
        position: str("New render order"),
        options: arr(
          obj({ label: str("Option label"), value: str("Option value (slug)") }, ["label", "value"]),
          "Full replacement list for option-typed fields.",
        ),
        locationId,
      },
      ["customFieldId"],
    ),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      const { customFieldId: cfid, locationId: _, ...body } = args;
      return await toplineFetch(`/locations/${id}/customFields/${cfid}`, {
        method: "PUT",
        body,
      });
    },
  },
  {
    name: "topline_delete_custom_field",
    description:
      "Delete a custom field. This removes the field definition but does not affect contact/opportunity rows that already have a value stored — those become orphaned data accessible only via raw_payload.",
    inputSchema: obj({ customFieldId, locationId }, ["customFieldId"]),
    handler: async (args) => {
      const id = getLocationId(args.locationId as string | undefined);
      return await toplineFetch(`/locations/${id}/customFields/${args.customFieldId}`, {
        method: "DELETE",
      });
    },
  },
];
