import { toplineFetch, getLocationId } from "../client.js";
import type { ToolDef } from "./types.js";
import {
  contactId,
  locationId,
  str,
  bool,
  arr,
  obj,
  objLoose,
  limitProp,
  startAfterIdProp,
  tagName,
} from "../schemas.js";

export const tools: ToolDef[] = [
  {
    name: "topline_search_contacts",
    description:
      "Search contacts by free-text query (name, email, phone) and/or tag. Returns up to 100 per call with a cursor for pagination.",
    inputSchema: obj({
      query: str("Free-text search: name, email, or phone"),
      tags: arr({ type: "string" }, "Filter to contacts with ALL of these tags"),
      limit: limitProp,
      startAfterId: startAfterIdProp,
      locationId,
    }),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        locationId: getLocationId(args.locationId as string | undefined),
        pageLimit: (args.limit as number) ?? 25,
      };
      if (args.query) body.query = args.query;
      if (args.tags && Array.isArray(args.tags) && args.tags.length) {
        body.filters = [{ field: "tags", operator: "contains", value: args.tags }];
      }
      if (args.startAfterId) body.searchAfter = [args.startAfterId];
      return await toplineFetch("/contacts/search", { method: "POST", body });
    },
  },
  {
    name: "topline_get_contact",
    description: "Fetch a single contact by ID. Returns all standard fields and custom fields.",
    inputSchema: obj({ contactId }, ["contactId"]),
    handler: async (args) => toplineFetch(`/contacts/${args.contactId}`),
  },
  {
    name: "topline_create_contact",
    description:
      "Create a new contact. At minimum provide a name or email or phone. Custom fields go in customFields as {id, value} pairs.",
    inputSchema: obj({
      firstName: str(),
      lastName: str(),
      name: str("Full name (use instead of firstName+lastName if you prefer)"),
      email: str(),
      phone: str("E.164 format preferred, e.g. +14155551212"),
      tags: arr({ type: "string" }),
      source: str("Attribution source (e.g. 'claude', 'website')"),
      customFields: arr(objLoose({ id: str(), value: {} })),
      locationId,
    }),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        locationId: getLocationId(args.locationId as string | undefined),
      };
      for (const k of ["firstName", "lastName", "name", "email", "phone", "source"]) {
        if (args[k] !== undefined) body[k] = args[k];
      }
      if (args.tags) body.tags = args.tags;
      if (args.customFields) body.customFields = args.customFields;
      return await toplineFetch("/contacts/", { method: "POST", body });
    },
  },
  {
    name: "topline_update_contact",
    description: "Update fields on an existing contact.",
    inputSchema: obj({
      contactId,
      firstName: str(),
      lastName: str(),
      email: str(),
      phone: str(),
      tags: arr({ type: "string" }, "REPLACES the tag list. Use add/remove tag tools for incremental changes."),
      customFields: arr(objLoose({ id: str(), value: {} })),
    }, ["contactId"]),
    handler: async (args) => {
      const { contactId: id, ...rest } = args;
      return await toplineFetch(`/contacts/${id}`, { method: "PUT", body: rest });
    },
  },
  {
    name: "topline_delete_contact",
    description: "Permanently delete a contact. Irreversible.",
    inputSchema: obj({ contactId }, ["contactId"]),
    handler: async (args) => toplineFetch(`/contacts/${args.contactId}`, { method: "DELETE" }),
  },
  {
    name: "topline_add_contact_tags",
    description: "Add one or more tags to a contact.",
    inputSchema: obj(
      { contactId, tags: arr({ type: "string" }, "Tag names to add") },
      ["contactId", "tags"],
    ),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/tags`, {
        method: "POST",
        body: { tags: args.tags },
      }),
  },
  {
    name: "topline_remove_contact_tags",
    description: "Remove one or more tags from a contact.",
    inputSchema: obj(
      { contactId, tags: arr({ type: "string" }) },
      ["contactId", "tags"],
    ),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/tags`, {
        method: "DELETE",
        body: { tags: args.tags },
      }),
  },
  {
    name: "topline_upsert_contact",
    description:
      "Create a contact, or update the existing one if email/phone matches. Use this when you're not sure whether the contact already exists.",
    inputSchema: obj({
      firstName: str(),
      lastName: str(),
      email: str(),
      phone: str(),
      tags: arr({ type: "string" }),
      source: str(),
      customFields: arr(objLoose({ id: str(), value: {} })),
      locationId,
    }),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        locationId: getLocationId(args.locationId as string | undefined),
      };
      for (const k of ["firstName", "lastName", "email", "phone", "source"]) {
        if (args[k] !== undefined) body[k] = args[k];
      }
      if (args.tags) body.tags = args.tags;
      if (args.customFields) body.customFields = args.customFields;
      return await toplineFetch("/contacts/upsert", { method: "POST", body });
    },
  },
  {
    name: "topline_add_contact_to_workflow",
    description: "Enroll a contact in a workflow.",
    inputSchema: obj(
      { contactId, workflowId: str("Workflow ID") },
      ["contactId", "workflowId"],
    ),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/workflow/${args.workflowId}`, {
        method: "POST",
      }),
  },
  {
    name: "topline_remove_contact_from_workflow",
    description: "Remove a contact from a workflow.",
    inputSchema: obj(
      { contactId, workflowId: str("Workflow ID") },
      ["contactId", "workflowId"],
    ),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/workflow/${args.workflowId}`, {
        method: "DELETE",
      }),
  },
];
