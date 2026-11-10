use super::{
    matching, ExchangeSim, RestingQueueState, VenueDiagnosticKind, VenueOrder,
    VenueSubmissionConsequence,
};
use crate::execution::SimSubmittedOrder;

impl ExchangeSim {
    fn prepare_submission(
        &mut self,
        submitted: SimSubmittedOrder,
        ts_ms: u64,
    ) -> Option<VenueOrder> {
        let submitted_order_id = submitted.order_id.trim().to_string();
        let order = match VenueOrder::from_submission(submitted, ts_ms) {
            Ok(order) => order,
            Err(reason) => {
                self.execution_stats.rejected_orders =
                    self.execution_stats.rejected_orders.saturating_add(1);
                self.record_diagnostic(
                    ts_ms,
                    None,
                    &submitted_order_id,
                    VenueDiagnosticKind::ProtocolError,
                    reason,
                );
                return None;
            }
        };

        if !self.seen_order_ids.insert(order.order_id.clone()) {
            self.push_rejected_update(&order, ts_ms, "duplicate_order_id");
            return None;
        }

        self.execution_stats.note_submitted(&order.market_id);
        Some(order)
    }

    pub(crate) fn reject_order(
        &mut self,
        submitted: SimSubmittedOrder,
        ts_ms: u64,
        reason: &'static str,
    ) -> VenueSubmissionConsequence {
        let Some(order) = self.prepare_submission(submitted, ts_ms) else {
            return VenueSubmissionConsequence::Rejected;
        };
        self.push_rejected_update(&order, ts_ms, reason);
        VenueSubmissionConsequence::Rejected
    }

    pub(crate) fn submit_order(
        &mut self,
        submitted: SimSubmittedOrder,
        ts_ms: u64,
    ) -> VenueSubmissionConsequence {
        let Some(order) = self.prepare_submission(submitted, ts_ms) else {
            return VenueSubmissionConsequence::Rejected;
        };
        if matches!(order.time_in_force, crate::types::TimeInForce::Gtd { expiration_ms } if expiration_ms <= ts_ms)
        {
            self.push_rejected_update(&order, ts_ms, "gtd_expired_before_submission");
            return VenueSubmissionConsequence::Rejected;
        }
        let invalid_post_only_tif = order.post_only && order.time_in_force.is_immediate();
        if invalid_post_only_tif {
            self.push_rejected_update(&order, ts_ms, "post_only_with_taker_tif");
            return VenueSubmissionConsequence::Rejected;
        }
        let crosses = match matching::order_crosses_book(self, &order, ts_ms) {
            Ok(crosses) => crosses,
            Err(reason) => {
                self.push_rejected_update(&order, ts_ms, reason);
                return VenueSubmissionConsequence::Rejected;
            }
        };
        if order.post_only && crosses {
            self.push_rejected_update(&order, ts_ms, "post_only_would_cross");
            return VenueSubmissionConsequence::Rejected;
        }

        if order.time_in_force.is_immediate() {
            self.submit_immediate(order, ts_ms);
            return VenueSubmissionConsequence::Accepted;
        }

        self.submit_resting_capable(order, ts_ms)
    }

    pub fn cancel_order(&mut self, order_id: &str, ts_ms: u64) {
        let order_id = order_id.trim();
        let Some(order) = self.resting_orders.remove(order_id) else {
            self.record_diagnostic(
                ts_ms,
                None,
                order_id,
                VenueDiagnosticKind::ProtocolError,
                "unknown_cancel_order_id",
            );
            return;
        };
        self.resting_queues.remove(order_id);
        self.push_order_update(
            &order,
            ts_ms,
            "CANCELED",
            "CANCELLATION",
            Some("central_cancel"),
        );
    }

    pub fn cancel_orders(&mut self, order_ids: &[String], ts_ms: u64) {
        for order_id in order_ids {
            self.cancel_order(order_id, ts_ms);
        }
    }

    fn submit_immediate(&mut self, mut order: VenueOrder, ts_ms: u64) {
        match matching::immediate_fill(self, &order, ts_ms) {
            Ok(fill) => {
                order.matched_size_units_6dp = fill.qty_units_6dp;
                super::fill_event::emit_fill(self, &order, &fill, ts_ms);
                let status = if order.remaining_units_6dp() == 0 {
                    "MATCHED"
                } else {
                    "CANCELED"
                };
                self.push_order_update(
                    &order,
                    ts_ms,
                    status,
                    "CANCELLATION",
                    (status == "CANCELED").then_some("fak_partial_fill_remainder_canceled"),
                );
            }
            Err(reason) => {
                self.push_order_update(&order, ts_ms, "CANCELED", "CANCELLATION", Some(reason));
            }
        }
    }

    fn submit_resting_capable(
        &mut self,
        mut order: VenueOrder,
        ts_ms: u64,
    ) -> VenueSubmissionConsequence {
        let crosses = match matching::order_crosses_book(self, &order, ts_ms) {
            Ok(crosses) => crosses,
            Err(reason) => {
                self.push_rejected_update(&order, ts_ms, reason);
                return VenueSubmissionConsequence::Rejected;
            }
        };
        let queue_ahead_units_6dp =
            match super::resting_orders::displayed_queue_ahead_units_6dp(self, &order, ts_ms) {
                Ok(units) => units,
                Err(reason) => {
                    self.push_rejected_update(&order, ts_ms, reason);
                    return VenueSubmissionConsequence::Rejected;
                }
            };
        if !order.post_only && crosses {
            match matching::marketable_limit_fill(self, &order, ts_ms) {
                Ok(fill) => {
                    order.matched_size_units_6dp = fill.qty_units_6dp;
                    super::fill_event::emit_fill(self, &order, &fill, ts_ms);
                    if order.remaining_units_6dp() == 0 {
                        self.push_order_update(&order, ts_ms, "MATCHED", "PLACEMENT", None);
                        return VenueSubmissionConsequence::Accepted;
                    }
                }
                Err(
                    "captured_l1_invalidated_by_trade"
                    | "captured_l2_invalidated_by_trade"
                    | "captured_l1_fully_consumed"
                    | "captured_l2_fully_consumed"
                    | "captured_l1_crossing_displayed_size_zero"
                    | "captured_l2_crossing_displayed_size_zero"
                    | "captured_l2_no_coherent_crossing",
                ) => {}
                Err(reason) => {
                    self.push_rejected_update(&order, ts_ms, reason);
                    return VenueSubmissionConsequence::Rejected;
                }
            }
        }

        self.resting_queues.insert(
            order.order_id.clone(),
            RestingQueueState {
                queue_ahead_units_6dp,
            },
        );
        self.push_order_update(&order, ts_ms, "LIVE", "PLACEMENT", None);
        self.resting_orders.insert(order.order_id.clone(), order);
        VenueSubmissionConsequence::Accepted
    }
}
