use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::DashboardState;

#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(crate) enum OperatorCommand {
    Shutdown,
    Pause { enabled: bool },
    Redeem,
    Cancel { order_ids: Vec<String> },
}

#[derive(Debug, Serialize)]
pub(crate) struct OperatorCommandAck {
    pub command: &'static str,
    pub accepted: bool,
    pub canceled_order_ids: Vec<String>,
    pub not_canceled_orders: Vec<CancelFailure>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CancelFailure {
    pub order_id: String,
    pub reason: String,
}

pub(crate) async fn forward(
    state: &DashboardState,
    command: OperatorCommand,
) -> Result<OperatorCommandAck> {
    match command {
        OperatorCommand::Shutdown => {
            let runtime_config = state.runtime_config_state.snapshot().await;
            anyhow::ensure!(
                runtime_config.pause,
                "shutdown requires effective operator pause"
            );

            let runtime_state = state.observability.runtime_state();
            let runtime = runtime_state.borrow().clone();
            anyhow::ensure!(
                runtime.open_orders.orders.is_empty(),
                "shutdown requires zero resident open orders"
            );
            anyhow::ensure!(
                runtime
                    .portfolio
                    .positions
                    .values()
                    .all(|position| position.qty_units_6dp == 0),
                "shutdown requires zero owned portfolio inventory"
            );

            let live_ops = state.execution.live_ops().snapshot();
            anyhow::ensure!(
                live_ops.inventory.active.is_empty(),
                "shutdown requires zero active CTF inventory operations"
            );
            anyhow::ensure!(
                live_ops.redeem.redeemables.is_empty(),
                "shutdown requires zero redeemable inventory"
            );
            anyhow::ensure!(
                live_ops.redeem.conditions.iter().all(|condition| {
                    !matches!(
                        condition.status.as_str(),
                        "pending" | "submitted" | "retrying" | "mined" | "reconciliation_required"
                    )
                }),
                "shutdown requires zero active settlement or redemption operations"
            );

            let remote_open_orders = state
                .execution
                .polymarket_open_orders()
                .await
                .map_err(|_| anyhow::anyhow!("shutdown could not verify remote open orders"))?;
            anyhow::ensure!(
                remote_open_orders.is_empty(),
                "shutdown requires zero remote open orders"
            );

            state.shutdown_request.cancel();
            Ok(OperatorCommandAck {
                command: "shutdown",
                accepted: true,
                canceled_order_ids: Vec::new(),
                not_canceled_orders: Vec::new(),
            })
        }
        OperatorCommand::Pause { enabled } => {
            crate::runtime_files::write_pause_control_state_file(
                enabled,
                "dashboard",
                "operator_command",
                "typed dashboard command",
            )?;
            Ok(OperatorCommandAck {
                command: "pause",
                accepted: true,
                canceled_order_ids: Vec::new(),
                not_canceled_orders: Vec::new(),
            })
        }
        OperatorCommand::Redeem => {
            state.execution.trigger_redeem_now()?;
            Ok(OperatorCommandAck {
                command: "redeem",
                accepted: true,
                canceled_order_ids: Vec::new(),
                not_canceled_orders: Vec::new(),
            })
        }
        OperatorCommand::Cancel { order_ids } => {
            anyhow::ensure!(
                !order_ids.is_empty(),
                "cancel command requires at least one order id"
            );
            let ack = state.execution.cancel_orders(&order_ids).await?;
            Ok(OperatorCommandAck {
                command: "cancel",
                accepted: true,
                canceled_order_ids: ack.canceled,
                not_canceled_orders: ack
                    .not_canceled
                    .into_iter()
                    .map(|(order_id, reason)| CancelFailure { order_id, reason })
                    .collect(),
            })
        }
    }
}
