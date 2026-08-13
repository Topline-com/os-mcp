const PLACEHOLDER_API_BASE_URL = "https://api.example.com";

export interface CredentialVerificationEnv {
  TOPLINE_API_BASE_URL?: string;
}

function apiBaseUrl(env?: CredentialVerificationEnv): string {
  return (env?.TOPLINE_API_BASE_URL || process.env.TOPLINE_API_BASE_URL || PLACEHOLDER_API_BASE_URL)
    .trim()
    .replace(/\/$/, "");
}

export async function verifyCredentials(
  pit: string,
  locationId: string,
  env?: CredentialVerificationEnv,
): Promise<void> {
  const cleanPit = pit.trim();
  const cleanLocationId = locationId.trim();
  const base = apiBaseUrl(env);
  if (!base || base === PLACEHOLDER_API_BASE_URL) {
    throw new Error("topline_api_base_url_missing");
  }
  let response: Response;
  try {
    response = await fetch(
      `${base}/locations/${encodeURIComponent(cleanLocationId)}`,
      {
        headers: {
          Authorization: `Bearer ${cleanPit}`,
          Version: "2021-07-28",
          Accept: "application/json",
          "User-Agent": "ToplineOS-MCP/0.2",
        },
        signal: AbortSignal.timeout(10_000),
      },
    );
  } catch {
    throw new Error("credential_verification_unreachable");
  }
  if (!response.ok) throw new Error(`credential_verification_failed:${response.status}`);
}
