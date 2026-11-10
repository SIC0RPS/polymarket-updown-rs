// Core paired-maker contract: routes lifecycle-authorized managed paired exits.
#[path = "exit_router/paired_sell.rs"]
mod paired_sell;

use crate::strategy::paired_maker::MakerRebateMmStrategy;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PairedFirstManagedSellFairValues {
    pub up_fair: f64,
    pub down_fair: f64,
}

impl MakerRebateMmStrategy {
    pub(crate) const PAIRED_FIRST_MANAGED_SELL_CLOSE_HINT: &'static str =
        "paired_first_managed_sell";
    pub(crate) const PAIRED_POST_PROCEEDS_SELL_CLOSE_HINT: &'static str =
        "paired_post_proceeds_sell";
}
