import { toplineFetch, getLocationId } from "../client.js";
import { BRAND_NAME } from "../branding.js";
import type { ToolDef } from "./types.js";

export const tools: ToolDef[] = [
  {
    name: "topline_ping",
    description: `Verify the ${BRAND_NAME} Private Integration Token works and return basic location info. Call this first to confirm setup.`,
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    handler: async () => {
      const id = getLocationId();
      const data = await toplineFetch<Record<string, unknown>>(`/locations/${id}`);
      const loc = (data as { location?: Record<string, unknown> }).location ?? data;
      return {
        ok: true,
        brand: BRAND_NAME,
        locationId: id,
        locationName: (loc as { name?: string }).name ?? null,
        timezone: (loc as { timezone?: string }).timezone ?? null,
      };
    },
  },
];
