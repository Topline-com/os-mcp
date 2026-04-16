import { toplineFetch } from "../client.js";
import type { ToolDef } from "./types.js";
import { contactId, str, bool, obj } from "../schemas.js";

const taskId = str("Task ID");

export const tools: ToolDef[] = [
  {
    name: "topline_list_contact_tasks",
    description: "List all tasks for a contact.",
    inputSchema: obj({ contactId }, ["contactId"]),
    handler: async (args) => toplineFetch(`/contacts/${args.contactId}/tasks`),
  },
  {
    name: "topline_create_task",
    description: "Create a task associated with a contact.",
    inputSchema: obj(
      {
        contactId,
        title: str("Task title"),
        body: str("Task description / notes"),
        dueDate: str("ISO 8601 due date"),
        assignedTo: str("User ID"),
        completed: bool("Mark already-completed"),
      },
      ["contactId", "title", "dueDate"],
    ),
    handler: async (args) => {
      const { contactId: cid, ...rest } = args;
      return await toplineFetch(`/contacts/${cid}/tasks`, { method: "POST", body: rest });
    },
  },
  {
    name: "topline_update_task",
    description: "Update a task — change title, body, due date, completion status.",
    inputSchema: obj(
      {
        contactId,
        taskId,
        title: str(),
        body: str(),
        dueDate: str(),
        completed: bool(),
        assignedTo: str(),
      },
      ["contactId", "taskId"],
    ),
    handler: async (args) => {
      const { contactId: cid, taskId: tid, ...rest } = args;
      return await toplineFetch(`/contacts/${cid}/tasks/${tid}`, { method: "PUT", body: rest });
    },
  },
  {
    name: "topline_delete_task",
    description: "Delete a task.",
    inputSchema: obj({ contactId, taskId }, ["contactId", "taskId"]),
    handler: async (args) =>
      toplineFetch(`/contacts/${args.contactId}/tasks/${args.taskId}`, { method: "DELETE" }),
  },
];
