import { toplineApiUrl } from "@topline/shared";

export async function verifyCredentials(pit: string, locationId: string): Promise<void> {
  const response = await fetch(
    toplineApiUrl(`/locations/${encodeURIComponent(locationId)}`),
    {
      headers: {
        Authorization: `Bearer ${pit}`,
        Version: "2021-07-28",
      },
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (!response.ok) throw new Error("credential_verification_failed");
}
