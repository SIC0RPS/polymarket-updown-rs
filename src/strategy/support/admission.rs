// Strategy support required for behavior: budget/admission filtering and
// decision queueing before intents leave strategy ownership.
use super::market_ranking::CtfEntryCandidate;
use super::*;
impl MakerRebateMmStrategy {
    pub(crate) fn venue_min_size(&self, top: &BookTop) -> f64 {
        normalized_min_order_size(top.min_order_size)
    }

    pub(crate) fn queue_market_quotes_filtered(
        &mut self,
        ctf_entry_candidates: &[CtfEntryCandidate],
        cycle_now_ms: u64,
        exit_market_ids: &HashSet<String>,
    ) {
        for market in ctf_entry_candidates
            .iter()
            .filter(|market| exit_market_ids.contains(market.selection.market_id.as_str()))
        {
            self.note_ctf_admission(&market.selection.market_id, "filtered_active_exit_market");
        }
        let filtered_candidates: Vec<&CtfEntryCandidate> = ctf_entry_candidates
            .iter()
            .filter(|market| !exit_market_ids.contains(market.selection.market_id.as_str()))
            .collect();
        self.queue_market_quotes_inner(&filtered_candidates, cycle_now_ms);
    }

    fn queue_market_quotes_inner(
        &mut self,
        ctf_entry_candidates: &[&CtfEntryCandidate],
        cycle_now_ms: u64,
    ) {
        let base_cfg = self.cfg.clone();
        for market in ctf_entry_candidates {
            self.cfg = base_cfg
                .config_for_symbol(&market.selection.underlying)
                .clone();
            if self.market_ctf_entry_ready(&market.selection, &market.up, &market.down) {
                if self.market_is_blocked_for_entry(&market.selection) {
                    continue;
                } else {
                    if self.cfg.oracle_buy_and_hold {
                        self.queue_oracle_buy_and_hold(market, cycle_now_ms);
                    } else {
                        self.queue_ctf_inventory_for_paired_market(market, cycle_now_ms);
                    }
                }
                continue;
            }

            self.note_quote_block("ctf_split_entry_required");
            self.note_ctf_admission(&market.selection.market_id, "ctf_split_entry_required");
        }
        self.cfg = base_cfg;
    }
}
