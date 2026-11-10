use crate::strategy::{MakerRebateMmStrategy, StrategyGateDropCounters};

impl StrategyGateDropCounters {
    pub(crate) fn saturating_add_assign(&mut self, other: StrategyGateDropCounters) {
        self.oracle = self.oracle.saturating_add(other.oracle);
        self.book = self.book.saturating_add(other.book);
        self.stale = self.stale.saturating_add(other.stale);
        self.open_delay = self.open_delay.saturating_add(other.open_delay);
        self.open_price = self.open_price.saturating_add(other.open_price);
        self.entry_time = self.entry_time.saturating_add(other.entry_time);
        self.blocked_market = self.blocked_market.saturating_add(other.blocked_market);
        self.markout_toxicity = self.markout_toxicity.saturating_add(other.markout_toxicity);
    }
}

impl MakerRebateMmStrategy {
    pub(crate) fn reset_quote_block_reasons(&mut self) {
        self.quote_block_reasons_cycle.clear();
        self.quote_block_detail_cycle = None;
        self.ctf_admission_by_market.clear();
    }

    pub(in crate::strategy) fn note_quote_block(&mut self, reason: &'static str) {
        let entry = self.quote_block_reasons_cycle.entry(reason).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    pub(crate) fn note_quote_block_with_detail(&mut self, reason: &'static str, detail: String) {
        self.note_quote_block(reason);
        if detail.trim().is_empty() {
            return;
        }
        match self.quote_block_detail_cycle.as_mut() {
            Some((existing_reason, existing_detail)) if *existing_reason == reason => {
                if existing_detail.len() < 1_500 {
                    existing_detail.push_str(" ; ");
                    existing_detail.push_str(detail.trim());
                }
            }
            None => {
                self.quote_block_detail_cycle = Some((reason, detail));
            }
            Some(_) => {}
        }
    }

    pub(crate) fn note_ctf_admission(
        &mut self,
        market_id: &crate::polymarket::identity::MarketId,
        reason: impl Into<String>,
    ) {
        self.ctf_admission_by_market
            .insert(market_id.to_string(), reason.into());
    }
}
