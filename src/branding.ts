export const BRAND_NAME = process.env.TOPLINE_BRAND_NAME?.trim() || "Topline";

export const SERVER_INFO = {
  name: `${BRAND_NAME.toLowerCase().replace(/\s+/g, "-")}-mcp`,
  version: "0.1.0",
};
