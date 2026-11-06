import { isPrintableAscii } from "@topline/shared";

/**
 * The error a save path shows when a pasted PIT or Location ID has a
 * character outside printable ASCII, or null when both are fine. The words
 * name the field and never echo the value or any part of it.
 */
export function credentialCharacterError(
  pit: string,
  locationId: string,
  brand: string,
): string | null {
  if (!isPrintableAscii(pit)) {
    return `Private Integration Token has a character that can't be part of a token. Re-copy it from ${brand} → Settings → Private Integrations and paste only the token.`;
  }
  if (!isPrintableAscii(locationId)) {
    return `Location ID has a character that can't be part of a Location ID. Re-copy it from ${brand} → Settings → Business Info and paste only the ID.`;
  }
  return null;
}

/**
 * The `pattern` both credential forms put on their PIT and Location ID
 * inputs, so the browser stops a bad paste before it's sent. Surrounding
 * whitespace is allowed because the server trims it. The server still checks
 * every submission.
 */
export const PRINTABLE_ASCII_INPUT_PATTERN = String.raw`\s*[\x20-\x7E]+\s*`;
