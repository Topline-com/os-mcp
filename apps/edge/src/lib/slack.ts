// Slack notifier — posts to a tenant's incoming-webhook URL.
//
// Config resolution (env wins):
//   1. TOPLINE_SLACK_WEBHOOK_URL env var (per-deployment)
//   2. _topline_marketing_config.slack.webhook_url (per-location custom value)
//
// If neither is configured, sendSlack() is a no-op (returns ok:false with
// reason: "not_configured"). Callers shouldn't treat that as an error —
// not every tenant runs the Slack integration.

import { loadMarketingConfig } from "./marketing_config.js";

export interface SlackBlock {
  type: string;
  [key: string]: unknown;
}

export interface SlackMessage {
  text: string;
  blocks?: SlackBlock[];
  /** Optional: override channel for webhooks tied to a workspace. Usually unused. */
  channel?: string;
}

export interface SendSlackResult {
  ok: boolean;
  source?: "env" | "marketing_config";
  reason?: string;
  status?: number;
}

async function resolveWebhookUrl(
  locationIdOverride?: string,
): Promise<{ url: string; source: "env" | "marketing_config" } | null> {
  const envUrl = process.env.TOPLINE_SLACK_WEBHOOK_URL?.trim();
  if (envUrl) return { url: envUrl, source: "env" };
  try {
    const cfg = await loadMarketingConfig(locationIdOverride);
    const cfgUrl = cfg.slack?.webhook_url?.trim();
    if (cfgUrl) return { url: cfgUrl, source: "marketing_config" };
  } catch {
    // marketing_config may not exist yet; treat as no slack config.
  }
  return null;
}

export async function sendSlack(
  message: SlackMessage,
  locationIdOverride?: string,
): Promise<SendSlackResult> {
  const resolved = await resolveWebhookUrl(locationIdOverride);
  if (!resolved) return { ok: false, reason: "not_configured" };
  const res = await fetch(resolved.url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(message),
  });
  return { ok: res.ok, source: resolved.source, status: res.status };
}

export async function describeSlackConfig(
  locationIdOverride?: string,
): Promise<{ configured: boolean; source?: "env" | "marketing_config" }> {
  const resolved = await resolveWebhookUrl(locationIdOverride);
  if (!resolved) return { configured: false };
  return { configured: true, source: resolved.source };
}
