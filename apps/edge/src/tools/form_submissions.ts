// Form submission visibility tools — Slack notifier + dispatch helpers.
//
// P6 ships the action-tool surface. The webhook receiver that wires a
// CRM workflow → this worker is a separate Worker route (out of scope
// for this PR); for now the recommended flow is:
//
//   - Tenant configures a CRM workflow trigger on form submission.
//   - The workflow calls topline_dispatch_form_submission via the MCP
//     (passes contact + form payload).
//   - This tool classifies channel, formats a Slack block, posts.
//
// Or, the LLM polls topline_list_form_submissions (existing) on a
// schedule and dispatches new submissions through this tool.

import { obj, objLoose, str, locationId } from "@topline/shared";
import type { ToolDef } from "./types.js";
import { sendSlack, describeSlackConfig } from "../lib/slack.js";
import { loadMarketingConfig } from "../lib/marketing_config.js";
import { classifyChannel } from "../lib/utm.js";

export const tools: ToolDef[] = [
  {
    name: "topline_get_slack_config",
    description:
      "Show whether Slack notifications are wired and which source is active " +
      "(env var TOPLINE_SLACK_WEBHOOK_URL takes precedence; otherwise " +
      "marketing_config.slack.webhook_url). Returns { configured: bool, source?: 'env' | 'marketing_config' }.",
    inputSchema: obj({ locationId }),
    handler: async (args) => describeSlackConfig(args.locationId as string | undefined),
  },

  {
    name: "topline_notify_slack",
    description:
      "Send a free-form message to the location's configured Slack channel. " +
      "Use this for one-off alerts (e.g., 'Hot lead landed', 'Pipeline anomaly'). " +
      "For structured form-submission notifications, use topline_dispatch_form_submission. " +
      "No-ops gracefully (ok: false, reason: 'not_configured') if no webhook URL is set.",
    inputSchema: obj(
      {
        text: str("Plain-text fallback (also shown in mobile/notifications)."),
        blocks: objLoose({}, []),
        locationId,
      },
      ["text"],
    ),
    handler: async (args) => {
      const result = await sendSlack(
        {
          text: args.text as string,
          blocks: args.blocks
            ? ((args.blocks as { blocks?: unknown }).blocks as never) ??
              ([] as never)
            : undefined,
        },
        args.locationId as string | undefined,
      );
      return result;
    },
  },

  {
    name: "topline_dispatch_form_submission",
    description:
      "Process a single form submission: classify its channel from captured " +
      "UTM/referrer, format a Slack notification, and post. Pass `submission` " +
      "with at minimum { contactId, contactName, formName, utm_source?, " +
      "utm_medium?, utm_campaign?, referrer? }. Returns { ok, channel, slack_ok }. " +
      "No-ops Slack if not configured; still classifies and returns the channel " +
      "so the dashboard widget feed (P5) can render the row.",
    inputSchema: obj(
      {
        submission: objLoose(
          {
            contactId: str("CRM contact id."),
            contactName: str("Display name."),
            formName: str("Which form was submitted."),
            utm_source: str(),
            utm_medium: str(),
            utm_campaign: str(),
            referrer: str(),
            contactUrl: str("Optional click-through link to the contact in the CRM."),
          },
          ["contactId", "formName"],
        ),
        locationId,
      },
      ["submission"],
    ),
    handler: async (args) => {
      const submission = args.submission as {
        contactId: string;
        contactName?: string;
        formName: string;
        utm_source?: string;
        utm_medium?: string;
        utm_campaign?: string;
        referrer?: string;
        contactUrl?: string;
      };
      const taxonomy = await loadMarketingConfig(args.locationId as string | undefined);
      const channel = classifyChannel(
        {
          utmSource: submission.utm_source,
          utmMedium: submission.utm_medium,
          referrer: submission.referrer,
        },
        taxonomy,
      );

      const headline = `📩 New form submission — ${submission.formName}`;
      const lines = [
        `*Contact:* ${submission.contactName ?? submission.contactId}`,
        `*Channel:* \`${channel}\``,
      ];
      if (submission.utm_campaign) lines.push(`*Campaign:* ${submission.utm_campaign}`);
      if (submission.contactUrl) lines.push(`<${submission.contactUrl}|Open contact>`);
      const text = `${headline}\n${lines.join("\n")}`;

      const slack = await sendSlack(
        {
          text,
          blocks: [
            { type: "section", text: { type: "mrkdwn", text: `*${headline}*` } },
            { type: "section", text: { type: "mrkdwn", text: lines.join("\n") } },
          ],
        },
        args.locationId as string | undefined,
      );

      return {
        ok: true,
        channel,
        slack_ok: slack.ok,
        slack_source: slack.source,
        slack_reason: slack.reason,
      };
    },
  },
];
