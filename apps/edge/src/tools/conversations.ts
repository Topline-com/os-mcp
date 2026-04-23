import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import {
  contactId,
  conversationId,
  locationId,
  str,
  arr,
  obj,
  objLoose,
  limitProp,
  startAfterIdProp,
} from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_search_conversations",
    description: "Search conversations for the sub-account. Filter by contact, status, or query.",
    inputSchema: obj({
      contactId: str("Only return conversations with this contact"),
      query: str("Free-text search"),
      status: { type: "string", enum: ["all", "read", "unread", "starred", "recents"] },
      limit: limitProp,
      startAfterId: startAfterIdProp,
      locationId,
    }),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        locationId: getLocationId(args.locationId as string | undefined),
        limit: (args.limit as number) ?? 25,
      };
      if (args.contactId) query.contactId = String(args.contactId);
      if (args.query) query.query = String(args.query);
      if (args.status) query.status = String(args.status);
      if (args.startAfterId) query.startAfterId = String(args.startAfterId);
      return await toplineFetch("/conversations/search", { query });
    },
  },
  {
    name: "topline_get_conversation",
    description: "Fetch a single conversation thread including recent messages.",
    inputSchema: obj({ conversationId }, ["conversationId"]),
    handler: async (args) => toplineFetch(`/conversations/${args.conversationId}`),
  },
  {
    name: "topline_get_messages",
    description: "List messages in a conversation.",
    inputSchema: obj(
      { conversationId, limit: limitProp, lastMessageId: str("Cursor for pagination") },
      ["conversationId"],
    ),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {};
      if (args.limit) query.limit = args.limit as number;
      if (args.lastMessageId) query.lastMessageId = String(args.lastMessageId);
      return await toplineFetch(`/conversations/${args.conversationId}/messages`, { query });
    },
  },
  {
    name: "topline_send_message",
    description:
      "Send a message (SMS, Email, WhatsApp, or Facebook/Instagram DM) to a contact. The contact must already exist.",
    inputSchema: obj(
      {
        contactId,
        type: {
          type: "string",
          enum: ["SMS", "Email", "WhatsApp", "IG", "FB", "Custom", "Live_Chat"],
          description: "Channel to send through",
        },
        message: str("Plain-text message body (for SMS/chat channels)"),
        subject: str("Subject line (Email only)"),
        html: str("HTML body (Email only, takes precedence over message)"),
        attachments: arr({ type: "string" }, "Public URLs to attach"),
        fromNumber: str("Sender phone number (SMS only, must be a number on the sub-account)"),
        toNumber: str("Override destination number (SMS only)"),
        emailFrom: str("Email from-address override"),
        emailTo: str("Email to-address override"),
      },
      ["contactId", "type"],
    ),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        contactId: args.contactId,
        type: args.type,
      };
      for (const k of ["message", "subject", "html", "attachments", "fromNumber", "toNumber", "emailFrom", "emailTo"]) {
        if (args[k] !== undefined) body[k] = args[k];
      }
      return await toplineFetch("/conversations/messages", { method: "POST", body });
    },
  },
  {
    name: "topline_create_conversation",
    description: "Create a new conversation with a contact (rarely needed — send_message usually creates one implicitly).",
    inputSchema: obj({ contactId, locationId }, ["contactId"]),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        contactId: args.contactId,
        locationId: getLocationId(args.locationId as string | undefined),
      };
      return await toplineFetch("/conversations/", { method: "POST", body });
    },
  },
];
