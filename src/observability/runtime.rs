use super::*;

pub struct ObservabilityRuntime {
    obs: Observability,
    _telemetry: TelemetryGuards,
    action_logger_task: Option<JoinHandle<Result<()>>>,
    operator_surface: Option<super::operator_surface_ui::OperatorUiSurface>,
}

pub struct ObservabilityOperatorSurfaceStart {
    pub cfg: crate::config::ObservabilityDashboardConfig,
    pub obs: Observability,
    pub exec: crate::execution::ExecutionClient,
    pub shutdown: CancellationToken,
    pub runtime_config_state: crate::runtime_control::SharedRuntimeConfigState,
    pub risk: Option<crate::risk::RiskHandle>,
    pub price_history_root: std::path::PathBuf,
    pub price_symbols: std::collections::BTreeMap<String, String>,
}

impl ObservabilityRuntime {
    pub async fn start(cfg: &ObservabilityConfig, mode: &str) -> Result<Self> {
        let telemetry = telemetry::init(&cfg.logging)?;

        let shutdown = CancellationToken::new();

        let (actions_tx, actions_rx) = mpsc::channel::<ActionEvent>(4096);
        let started_at_ms = crate::event_ledger::now_ms();
        let initial_state = UiState::new(started_at_ms, mode.to_string());
        let (ui_state_tx, _) = watch::channel::<UiState>(initial_state);
        let (runtime_state_tx, _) =
            watch::channel::<RuntimeState>(RuntimeState::new(started_at_ms, mode.to_string()));
        let (live_account_refresh_seq_tx, _) = watch::channel::<u64>(0);

        ui_state_tx.send_modify(|s| s.log_level = cfg.logging.level.clone());

        let (portfolio_reset_tx, _) = watch::channel::<u64>(0);

        let obs = Observability {
            actions_tx,
            ui_state_tx,
            runtime_state_tx,
            live_account_refresh_seq_tx,
            portfolio_reset_tx,
            shutdown: shutdown.clone(),
            filter_reload: telemetry.filter_reload.clone(),
            flood_control: telemetry.flood_control.clone(),
            action_log_enabled: cfg.enabled && cfg.logging.action_log_enabled,
            action_log_log_ticks: cfg.logging.log_ticks,
        };

        let action_logger_task = if cfg.enabled && cfg.logging.action_log_enabled {
            let path = PathBuf::from(cfg.logging.action_log_path.clone());
            let flush_every = cfg.logging.action_log_flush_every;
            let max_bytes = cfg.logging.action_log_max_bytes;
            let shutdown = shutdown.clone();
            Some(tokio::spawn(async move {
                action_log::run_action_logger(actions_rx, &path, flush_every, max_bytes, shutdown)
                    .await
            }))
        } else {
            None
        };

        Ok(Self {
            obs,
            _telemetry: telemetry,
            action_logger_task,
            operator_surface: None,
        })
    }

    pub fn handle(&self) -> Observability {
        self.obs.clone()
    }

    pub async fn start_operator_surface(
        &mut self,
        args: ObservabilityOperatorSurfaceStart,
    ) -> Result<()> {
        self.operator_surface =
            Some(super::operator_surface_ui::start_operator_surface(args).await?);
        Ok(())
    }

    pub async fn shutdown_operator_surface(&mut self) -> Result<()> {
        if let Some(surface) = self.operator_surface.take() {
            surface.shutdown().await?;
        }
        Ok(())
    }

    pub async fn shutdown(self) {
        if let Err(error) = self.shutdown_with_result().await {
            tracing::error!(target: "observability", error = %error, "observability_shutdown_failed");
        }
    }

    pub async fn shutdown_with_result(mut self) -> Result<()> {
        let mut result = self.shutdown_operator_surface().await;
        self.obs.shutdown.cancel();
        if let Some(handle) = self.action_logger_task {
            if let Err(error) = handle
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
            {
                result = Err(match result {
                    Ok(()) => error.context("observability action logger shutdown failed"),
                    Err(prior) => {
                        prior.context(format!("action logger shutdown also failed: {error:#}"))
                    }
                });
            }
        }
        result
    }
}
