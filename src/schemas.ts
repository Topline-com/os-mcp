// Shared JSON Schema fragments reused across tool definitions.
// We author JSON Schema directly — MCP clients (Claude Desktop/Code) validate
// tool arguments against these before invoking the handler.

export const str = (description?: string) => ({ type: "string" as const, ...(description ? { description } : {}) });
export const num = (description?: string) => ({ type: "number" as const, ...(description ? { description } : {}) });
export const bool = (description?: string) => ({ type: "boolean" as const, ...(description ? { description } : {}) });
export const arr = (items: unknown, description?: string) => ({
  type: "array" as const,
  items,
  ...(description ? { description } : {}),
});
export const obj = (properties: Record<string, unknown>, required: string[] = []) => ({
  type: "object" as const,
  properties,
  required,
  additionalProperties: false,
});
export const objLoose = (properties: Record<string, unknown>, required: string[] = []) => ({
  type: "object" as const,
  properties,
  required,
});

// Common fields
export const contactId = str("Contact ID");
export const locationId = str("Location (sub-account) ID. Defaults to TOPLINE_LOCATION_ID env var if omitted.");
export const opportunityId = str("Opportunity ID");
export const pipelineId = str("Pipeline ID");
export const pipelineStageId = str("Pipeline stage ID");
export const conversationId = str("Conversation ID");
export const calendarId = str("Calendar ID");
export const appointmentId = str("Appointment ID");
export const workflowId = str("Workflow ID");
export const userId = str("User ID");
export const tagName = str("Tag name");
export const formId = str("Form ID");
export const surveyId = str("Survey ID");
export const limitProp = num("Results per page (max 100, default 25)");
export const startAfterIdProp = str("Cursor from a previous page");
