// Spend classification — apply the tenant's marketing_config.spend_rules
// to a list of SpendTxn, producing ClassifiedSpendTxn (each row gets a
// `channel` from the tenant's source taxonomy).

import type { MarketingConfig } from "../../config/defaults.js";
import type { SpendTxn, ClassifiedSpendTxn } from "./types.js";

export function classifyTransactions(
  txns: SpendTxn[],
  config: MarketingConfig,
): ClassifiedSpendTxn[] {
  const rules = config.spend_rules ?? [];
  return txns.map((txn) => {
    for (let i = 0; i < rules.length; i++) {
      const rule = rules[i];
      if (matches(rule.when, txn)) {
        return { ...txn, channel: rule.classify_as, matched_rule_index: i };
      }
    }
    return { ...txn, channel: "needs_review" };
  });
}

function matches(
  when: { merchant_regex?: string; memo_regex?: string; account_regex?: string },
  txn: SpendTxn,
): boolean {
  if (when.merchant_regex && txn.merchant && new RegExp(when.merchant_regex, "i").test(txn.merchant))
    return true;
  if (when.memo_regex && txn.memo && new RegExp(when.memo_regex, "i").test(txn.memo)) return true;
  if (when.account_regex && txn.account && new RegExp(when.account_regex, "i").test(txn.account))
    return true;
  return false;
}

export function rollupByChannel(
  classified: ClassifiedSpendTxn[],
): Array<{ channel: string; total: number; count: number }> {
  const map = new Map<string, { total: number; count: number }>();
  for (const t of classified) {
    const cur = map.get(t.channel) ?? { total: 0, count: 0 };
    cur.total += t.amount;
    cur.count += 1;
    map.set(t.channel, cur);
  }
  return [...map.entries()]
    .map(([channel, { total, count }]) => ({ channel, total, count }))
    .sort((a, b) => b.total - a.total);
}
