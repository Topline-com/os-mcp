import { toplineFetch, getLocationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import {
  appointmentId,
  calendarId,
  contactId,
  locationId,
  str,
  num,
  obj,
} from "@topline/shared";

export const tools: ToolDef[] = [
  {
    name: "topline_list_calendars",
    description: "List all calendars configured on the sub-account.",
    inputSchema: obj({ locationId }),
    handler: async (args) => {
      const query = { locationId: getLocationId(args.locationId as string | undefined) };
      return await toplineFetch("/calendars/", { query });
    },
  },
  {
    name: "topline_get_calendar_slots",
    description: "Get available time slots for a calendar within a date range (ms epoch).",
    inputSchema: obj(
      {
        calendarId,
        startDate: num("Start timestamp (ms since epoch)"),
        endDate: num("End timestamp (ms since epoch)"),
        timezone: str("IANA timezone (e.g. 'America/New_York')"),
      },
      ["calendarId", "startDate", "endDate"],
    ),
    handler: async (args) => {
      const query: Record<string, string | number | boolean | undefined> = {
        startDate: args.startDate as number,
        endDate: args.endDate as number,
      };
      if (args.timezone) query.timezone = String(args.timezone);
      return await toplineFetch(`/calendars/${args.calendarId}/free-slots`, { query });
    },
  },
  {
    name: "topline_create_appointment",
    description: "Book an appointment on a calendar for a contact.",
    inputSchema: obj(
      {
        calendarId,
        contactId,
        startTime: str("ISO 8601 start time, e.g. '2026-04-20T15:00:00-04:00'"),
        endTime: str("ISO 8601 end time (optional — inferred from calendar if omitted)"),
        title: str("Appointment title"),
        appointmentStatus: {
          type: "string",
          enum: ["new", "confirmed", "cancelled", "showed", "noshow", "invalid"],
        },
        assignedUserId: str("User ID to assign"),
        address: str("Location or meeting link"),
        locationId,
      },
      ["calendarId", "contactId", "startTime"],
    ),
    handler: async (args) => {
      const body: Record<string, unknown> = {
        locationId: getLocationId(args.locationId as string | undefined),
        calendarId: args.calendarId,
        contactId: args.contactId,
        startTime: args.startTime,
      };
      for (const k of ["endTime", "title", "appointmentStatus", "assignedUserId", "address"]) {
        if (args[k] !== undefined) body[k] = args[k];
      }
      return await toplineFetch("/calendars/events/appointments", { method: "POST", body });
    },
  },
  {
    name: "topline_update_appointment",
    description: "Update an appointment (reschedule, change status, reassign).",
    inputSchema: obj(
      {
        appointmentId,
        startTime: str("ISO 8601"),
        endTime: str("ISO 8601"),
        title: str(),
        appointmentStatus: {
          type: "string",
          enum: ["new", "confirmed", "cancelled", "showed", "noshow", "invalid"],
        },
        assignedUserId: str(),
        address: str(),
      },
      ["appointmentId"],
    ),
    handler: async (args) => {
      const { appointmentId: id, ...rest } = args;
      return await toplineFetch(`/calendars/events/appointments/${id}`, { method: "PUT", body: rest });
    },
  },
  {
    name: "topline_delete_appointment",
    description: "Cancel and delete an appointment.",
    inputSchema: obj({ appointmentId }, ["appointmentId"]),
    handler: async (args) =>
      toplineFetch(`/calendars/events/appointments/${args.appointmentId}`, { method: "DELETE" }),
  },

  // Calendar-definition writes. Live-probed 2026-04-24 under PIT:
  //   PUT    /calendars/{id}  → reachable (400 on bad id means handler ran)
  //   DELETE /calendars/{id}  → reachable
  //   POST   /calendars/      → 403 "token does not have access to this location"
  //                              (requires marketplace OAuth scope; omitted here)
  {
    name: "topline_get_calendar",
    description:
      "Get the full calendar definition (availability rules, team members, slot duration, etc.).",
    inputSchema: obj({ calendarId }, ["calendarId"]),
    handler: async (args) => toplineFetch(`/calendars/${args.calendarId}`),
  },
  {
    name: "topline_update_calendar",
    description:
      "Update a calendar's name, description, slot duration, availability, team members, or event title. Pass only the fields you want to change. The calendar must already exist — calendar CREATE is not available under PIT (marketplace OAuth only).",
    inputSchema: obj(
      {
        calendarId,
        name: str("New calendar name"),
        description: str("New description"),
        slug: str("URL slug (shows in public booking URLs)"),
        isActive: { type: "boolean", description: "Enable / disable the calendar" },
        slotDuration: num("Appointment length in minutes"),
        slotBuffer: num("Buffer (minutes) between appointments"),
        eventTitle: str("Default event title template"),
        eventColor: str("Hex color shown in the UI"),
        appoinmentPerSlot: num("Max concurrent bookings per slot"),
        allowReschedule: { type: "boolean" },
        allowCancellation: { type: "boolean" },
      },
      ["calendarId"],
    ),
    handler: async (args) => {
      const { calendarId: cid, ...body } = args;
      return await toplineFetch(`/calendars/${cid}`, { method: "PUT", body });
    },
  },
  {
    name: "topline_delete_calendar",
    description:
      "Delete a calendar. All future appointments on this calendar are cancelled. Past appointments are retained as historical records.",
    inputSchema: obj({ calendarId }, ["calendarId"]),
    handler: async (args) =>
      toplineFetch(`/calendars/${args.calendarId}`, { method: "DELETE" }),
  },
];
