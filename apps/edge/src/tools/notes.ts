import { toplineFetch } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { contactId, str, obj } from "@topline/shared";

const noteId = str("Note ID");

export const tools: ToolDef[] = [
  {
    name: "topline_list_contact_notes",
    description: "List all notes on a contact.",
    inputSchema: obj({ contactId }, ["contactId"]),
    handler: async (args) => toplineFetch(`/contacts/${args.contactId}/notes`),
  },
  {
    name: "topline_create_note",
    description: "Create a note on a contact.",
    inputSchema: obj(
      { contactId, body: str("Note text"), userId: str("User ID to attribute the note to") },
      ["contactId", "body"],
    ),
    handler: async (args) => {
      const { contactId: cid, ...rest } = args;
      return await toplineFetch(`/contacts/${cid}/notes`, { method: "POST", body: rest });
    },
  },
  {
    name: "topline_update_note",
    description: "Update the body of a note.",
    inputSchema: obj({ contactId, noteId, body: str("New note text") }, ["contactId", "noteId", "body"]),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/notes/${args.noteId}`, {
        method: "PUT",
        body: { body: args.body },
      }),
  },
  {
    name: "topline_delete_note",
    description: "Delete a note.",
    inputSchema: obj({ contactId, noteId }, ["contactId", "noteId"]),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/notes/${args.noteId}`, { method: "DELETE" }),
  },
];
