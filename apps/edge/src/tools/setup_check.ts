import { toplineFetch, getLocationId, peekPit, peekLocationId, ToplineApiError } from "@topline/shared";
import { BRAND_NAME } from "@topline/shared";
import type { ToolDef } from "./types.js";

type ProbeStatus = "ok" | "forbidden" | "error" | "skipped";

interface ProbeResult {
  area: string;
  scope: string;
  status: ProbeStatus;
  detail: string;
}

interface Probe {
  area: string;
  scope: string; // Human-readable scope name the client needs to tick
  run: (locationId: string) => Promise<void>;
}

// Each probe issues a minimal GET against the endpoint class. We only care
// whether the call succeeds or is rejected by scope/auth — not the payload.
const PROBES: Probe[] = [
  {
    area: "contacts",
    scope: "contacts.readonly / contacts.write",
    run: async (locationId) => {
      await toplineFetch("/contacts/", { query: { locationId, limit: 1 } });
    },
  },
  {
    area: "conversations",
    scope: "conversations.readonly / conversations.write",
    run: async (locationId) => {
      await toplineFetch("/conversations/search", { query: { locationId, limit: 1 } });
    },
  },
  {
    area: "opportunities",
    scope: "opportunities.readonly / opportunities.write",
    run: async (locationId) => {
      await toplineFetch("/opportunities/pipelines", { query: { locationId } });
    },
  },
  {
    area: "calendars",
    scope: "calendars.readonly / calendars.write",
    run: async (locationId) => {
      await toplineFetch("/calendars/", { query: { locationId } });
    },
  },
  {
    area: "workflows",
    scope: "workflows.readonly",
    run: async (locationId) => {
      await toplineFetch("/workflows/", { query: { locationId } });
    },
  },
  {
    area: "forms",
    scope: "forms.readonly",
    run: async (locationId) => {
      await toplineFetch("/forms/", { query: { locationId, limit: 1 } });
    },
  },
  {
    area: "surveys",
    scope: "surveys.readonly",
    run: async (locationId) => {
      await toplineFetch("/surveys/", { query: { locationId, limit: 1 } });
    },
  },
  {
    area: "users",
    scope: "users.readonly",
    run: async (locationId) => {
      await toplineFetch("/users/", { query: { locationId } });
    },
  },
  {
    area: "custom_fields",
    scope: "locations/customFields.readonly",
    run: async (locationId) => {
      await toplineFetch(`/locations/${locationId}/customFields`);
    },
  },
  {
    area: "tags",
    scope: "locations/tags.readonly",
    run: async (locationId) => {
      await toplineFetch(`/locations/${locationId}/tags`);
    },
  },
  {
    area: "custom_values",
    scope: "locations/customValues.readonly",
    run: async (locationId) => {
      await toplineFetch(`/locations/${locationId}/customValues`);
    },
  },
  {
    area: "medias",
    scope: "medias.readonly",
    run: async (locationId) => {
      // /medias/files requires altType + altId + a non-empty type filter
      // (returns 422 "type must be a string; type should not be empty"
      // without it). Probed 2026-05-23 — type=image is sufficient to
      // verify scope access without listing every media type.
      await toplineFetch(`/medias/files`, {
        query: { altType: "location", altId: locationId, type: "image", limit: 1 },
      });
    },
  },
  // ---- Marketing OS surfaces (P1A–D) ----
  {
    area: "social_planner",
    scope: "socialplanner/post.readonly + account.readonly",
    run: async (locationId) => {
      await toplineFetch(`/social-media-posting/${locationId}/accounts`);
    },
  },
  {
    area: "ad_publishing_facebook",
    scope: "ad-publishing/facebook.readonly",
    run: async (locationId) => {
      await toplineFetch(`/ad-publishing/facebook/integration`, { query: { locationId } });
    },
  },
  {
    area: "ad_publishing_google",
    scope: "ad-publishing/google.readonly",
    run: async (locationId) => {
      await toplineFetch(`/ad-publishing/google/integration`, { query: { locationId } });
    },
  },
  {
    area: "ad_publishing_linkedin",
    scope: "ad-publishing/linkedin.readonly",
    run: async (locationId) => {
      await toplineFetch(`/ad-publishing/linkedin/integration`, { query: { locationId } });
    },
  },
  {
    area: "email_campaigns",
    scope: "campaigns.readonly",
    run: async (locationId) => {
      // /campaigns/ rejects `limit` (422 "property limit should not exist").
      // locationId alone is sufficient — the list is naturally paginated
      // upstream. Probed 2026-05-23.
      await toplineFetch(`/campaigns/`, { query: { locationId } });
    },
  },
  {
    area: "email_templates",
    scope: "emails/builder.readonly",
    run: async (locationId) => {
      await toplineFetch(`/emails/builder`, { query: { locationId, limit: 1 } });
    },
  },
  {
    area: "agent_studio",
    scope: "agent-studio.readonly",
    run: async (locationId) => {
      await toplineFetch(`/agent-studio/agent`, { query: { locationId } });
    },
  },
];

async function runProbe(p: Probe, locationId: string): Promise<ProbeResult> {
  try {
    await p.run(locationId);
    return { area: p.area, scope: p.scope, status: "ok", detail: "Accessible." };
  } catch (err) {
    if (err instanceof ToplineApiError) {
      if (err.statusCode === 403) {
        return {
          area: p.area,
          scope: p.scope,
          status: "forbidden",
          detail: `Missing scope. Edit your ${BRAND_NAME} Private Integration and tick the ${p.scope} scope, then regenerate the token.`,
        };
      }
      return {
        area: p.area,
        scope: p.scope,
        status: "error",
        detail: `${err.statusCode}: ${err.message}`,
      };
    }
    return {
      area: p.area,
      scope: p.scope,
      status: "error",
      detail: err instanceof Error ? err.message : String(err),
    };
  }
}

function maskToken(token: string): string {
  if (token.length <= 8) return `${token.slice(0, 4)}…`;
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}

export const tools: ToolDef[] = [
  {
    name: "topline_setup_check",
    description: `End-to-end setup verification for ${BRAND_NAME} MCP. Confirms the Private Integration Token is valid, resolves the location, and probes every major CRM scope: core (contacts, conversations, opportunities, calendars, workflows, forms, surveys, users, custom fields, custom values, tags, medias) + marketing OS surfaces (social planner, ad publishing for Facebook / Google / LinkedIn, email campaigns + templates, agent studio). Returns a structured pass/fail report so you can guide the user to fix any missing scopes. Call this immediately after a client finishes setup.`,
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    handler: async () => {
      const brand = BRAND_NAME;
      const pit = peekPit();
      const locEnv = peekLocationId();

      // Auth presence check (before any network call).
      if (!pit) {
        return {
          brand,
          auth: {
            ok: false,
            detail: `Private Integration Token is missing. Local setup: add TOPLINE_PIT to your Claude config env block. Remote setup: reconnect the custom connector in Claude and paste a valid pit- token.`,
          },
          location: { ok: false, detail: "Not checked — auth missing." },
          scopes: [],
          summary: `Setup incomplete: no Private Integration Token found.`,
        };
      }
      if (!locEnv) {
        return {
          brand,
          auth: { ok: true, tokenPrefix: maskToken(pit) },
          location: {
            ok: false,
            detail: `Location ID is missing. Find it in ${brand} → Settings → Business Info. Local setup: add TOPLINE_LOCATION_ID to your Claude config env block. Remote setup: reconnect the custom connector and enter the Location ID in the popup form.`,
          },
          scopes: [],
          summary: `Setup incomplete: Location ID is not set.`,
        };
      }

      // Resolve location (this also confirms auth works).
      let locationOk = false;
      let locationName: string | null = null;
      let locationTimezone: string | null = null;
      let locationDetail = "";
      let locationId: string;
      try {
        locationId = getLocationId();
        const data = await toplineFetch<Record<string, unknown>>(`/locations/${locationId}`);
        const loc = (data as { location?: Record<string, unknown> }).location ?? data;
        locationName = (loc as { name?: string }).name ?? null;
        locationTimezone = (loc as { timezone?: string }).timezone ?? null;
        locationOk = true;
        locationDetail = "Resolved.";
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return {
          brand,
          auth: {
            ok: false,
            tokenPrefix: maskToken(pit),
            detail: message,
          },
          location: { ok: false, id: locEnv, detail: message },
          scopes: [],
          summary: `Setup failed at auth/location stage: ${message}`,
        };
      }

      // Probe every scope in parallel — fast, and failures are isolated per probe.
      const results = await Promise.all(PROBES.map((p) => runProbe(p, locationId)));

      const okCount = results.filter((r) => r.status === "ok").length;
      const forbidden = results.filter((r) => r.status === "forbidden");
      const errored = results.filter((r) => r.status === "error");

      let summary: string;
      if (okCount === results.length) {
        summary = `All ${results.length} scope areas OK. ${brand} MCP is fully set up.`;
      } else {
        const parts: string[] = [`${okCount}/${results.length} scope areas OK.`];
        if (forbidden.length > 0) {
          parts.push(
            `Missing scopes: ${forbidden.map((r) => r.area).join(", ")}. Open ${brand} → Settings → Private Integrations, edit the integration, click Select All scopes, save, and regenerate the token if prompted.`,
          );
        }
        if (errored.length > 0) {
          parts.push(`Unexpected errors: ${errored.map((r) => `${r.area} (${r.detail})`).join("; ")}`);
        }
        summary = parts.join(" ");
      }

      return {
        brand,
        auth: { ok: true, tokenPrefix: maskToken(pit) },
        location: {
          ok: locationOk,
          id: locationId,
          name: locationName,
          timezone: locationTimezone,
          detail: locationDetail,
        },
        scopes: results,
        summary,
      };
    },
  },
];
