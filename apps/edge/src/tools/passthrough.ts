import { toplineFetch, getLocationId } from "@topline/shared";
import { BRAND_NAME } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { str, obj, objLoose } from "@topline/shared";

const METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"] as const;
type Method = (typeof METHODS)[number];

export const tools: ToolDef[] = [
  {
    name: "topline_request",
    description:
      `Generic passthrough to any ${BRAND_NAME} API v2 endpoint. Use this when no dedicated tool fits. ` +
      `Provide the path (e.g. "/contacts/" or "/opportunities/pipelines") and HTTP method. ` +
      `If the path or query needs a locationId and you don't provide one, the configured sub-account location is used automatically. ` +
      `Returns the parsed JSON response.`,
    inputSchema: obj(
      {
        method: { type: "string", enum: METHODS, description: "HTTP method" },
        path: str("API path, starting with '/'. Example: '/contacts/' or '/opportunities/123'"),
        query: objLoose({}, []),
        body: objLoose({}, []),
        injectLocationId: {
          type: "boolean",
          description:
            "If true (default), auto-inject locationId into the query string when absent. Set false for endpoints that don't accept it.",
        },
      },
      ["method", "path"],
    ),
    handler: async (args) => {
      const method = String(args.method).toUpperCase() as Method;
      if (!METHODS.includes(method)) {
        throw new Error(`Invalid method '${method}'. Must be one of ${METHODS.join(", ")}.`);
      }
      const path = String(args.path || "");
      if (!path.startsWith("/")) {
        throw new Error(`Path must start with '/'. Got: ${path}`);
      }

      const query: Record<string, string | number | boolean | undefined> = {
        ...((args.query as Record<string, string | number | boolean | undefined>) ?? {}),
      };
      const inject = args.injectLocationId !== false;
      if (inject && query.locationId === undefined && !path.includes("locationId=")) {
        query.locationId = getLocationId();
      }

      return await toplineFetch(path, {
        method,
        query,
        body: args.body,
      });
    },
  },
];
