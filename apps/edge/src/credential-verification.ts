import { getToplineApiBaseUrl } from "@topline/shared";

const PLACEHOLDER_API_BASE_URL = "https://api.example.com";

export async function verifyCredentials(pit: string, locationId: string): Promise<void> {
  const cleanPit = pit.trim();
  const cleanLocationId = locationId.trim();
  const base = getToplineApiBaseUrl();
  if (!base || base === PLACEHOLDER_API_BASE_URL) {
    throw new Error("topline_api_base_url_missing");
  }
  const response = await fetch(
    `${base}/locations/${encodeURIComponent(cleanLocationId)}`,
    {
      headers: {
        Authorization: `Bearer ${cleanPit}`,
        Version: "2021-07-28",
      },
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (!response.ok) throw new Error("credential_verification_failed");
}
