use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::{
    config::{Settings, StrategyMode},
    execution::{ExecutionGateway, LoadedAccount},
    market::MarketDiscoveryService,
    notifier::Notifier,
    orchestrator::Orchestrator,
};

pub struct App {
    settings: Settings,
    notifier: Notifier,
    market_discovery: MarketDiscoveryService,
    execution_gateway: Option<Arc<ExecutionGateway>>,
}

impl App {
    pub async fn boot(settings: Settings) -> Result<Self> {
        let notifier = Notifier::from_settings(&settings).await?;
        let market_discovery = MarketDiscoveryService::new(
            &settings.network,
            &settings.market,
            settings.strategy.round_interval_secs,
        )?;
        let execution_gateway = match LoadedAccount::from_config(settings.primary_account()?) {
            Ok(account) => Some(Arc::new(ExecutionGateway::from_account(
                &settings.network,
                &settings.execution,
                account,
            )?)),
            Err(error) => {
                warn!(
                    ?error,
                    "execution gateway disabled until account env vars are configured"
                );
                None
            }
        };

        Ok(Self {
            settings,
            notifier,
            market_discovery,
            execution_gateway,
        })
    }

    pub async fn run(self) -> Result<()> {
        self.log_boot_summary();
        self.notifier.send_startup(&self.settings).await;
        self.spawn_background_redeem_scan();

        let orchestrator = Orchestrator::new(
            &self.settings,
            &self.notifier,
            &self.market_discovery,
            self.execution_gateway.clone(),
        );

        tokio::select! {
            result = orchestrator.run_forever() => {
                self.notifier.send_shutdown(&self.settings).await;
                result
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                info!("received ctrl-c, shutting down");
                self.notifier.send_shutdown(&self.settings).await;
                Ok(())
            }
        }
    }

    fn log_boot_summary(&self) {
        info!(
            instance = %self.settings.app.instance_name,
            dry_run = self.settings.app.dry_run,
            strategy_mode = ?self.settings.strategy.mode,
            accounts = self.settings.enabled_accounts().count(),
            primary_account = %self.settings.routing.primary_account,
            series = %self.settings.market.series_slug,
            "booting pm alpha"
        );

        if let Some(gateway) = &self.execution_gateway {
            let runtime = gateway.runtime_config();
            let relayer_key_owner = gateway
                .account()
                .relayer_credentials
                .as_ref()
                .map(|credentials| credentials.owner_address.to_string())
                .unwrap_or_default();
            info!(
                account = %gateway.account().name,
                signer = %gateway.account().signer_address,
                funder = %gateway.account().funder_address,
                signature_type = ?gateway.account().signature_type,
                relayer_key_owner,
                clob_execution_mode = ?runtime.clob_execution_mode,
                onchain_execution_mode = ?runtime.onchain_execution_mode,
                max_batch_orders = runtime.max_batch_orders,
                "execution gateway ready"
            );
        }

        if self.settings.strategy.mode == StrategyMode::OpenPostDualBuyPriceGuard {
            info!(
                after_open_start_secs = self.settings.strategy.quote_start_after_open_secs,
                after_open_cancel_secs = self.settings.strategy.quote_cancel_after_open_secs,
                yes_price = self.settings.strategy.yes_price,
                no_price = self.settings.strategy.no_price,
                "open-post dual-buy strategy parameters"
            );
        }
    }

    fn spawn_background_redeem_scan(&self) {
        if !self.settings.execution.settled_redeem_scan_enabled {
            return;
        }

        let Some(gateway) = self.execution_gateway.clone() else {
            warn!("settled redeem scan enabled but execution gateway is unavailable");
            return;
        };

        let settings = self.settings.clone();
        let market_discovery = self.market_discovery.clone();
        tokio::spawn(async move {
            run_background_redeem_scan(settings, market_discovery, gateway).await;
        });
    }
}

async fn run_background_redeem_scan(
    settings: Settings,
    market_discovery: MarketDiscoveryService,
    gateway: Arc<ExecutionGateway>,
) {
    info!(
        interval_secs = settings.execution.settled_redeem_scan_interval_secs,
        lookback_secs = settings.execution.settled_redeem_scan_lookback_secs,
        "background settled redeem scan enabled"
    );

    loop {
        if let Err(error) =
            run_background_redeem_scan_once(&settings, &market_discovery, &gateway).await
        {
            warn!(?error, "background settled redeem scan iteration failed");
        }

        sleep(Duration::from_secs(
            settings.execution.settled_redeem_scan_interval_secs,
        ))
        .await;
    }
}

async fn run_background_redeem_scan_once(
    settings: &Settings,
    market_discovery: &MarketDiscoveryService,
    gateway: &ExecutionGateway,
) -> Result<()> {
    let user = gateway.account().funder_address.to_checksum(None);
    let mut rounds = match market_discovery.discover_redeemable_rounds(&user).await {
        Ok(rounds) => {
            debug!(
                round_count = rounds.len(),
                user, "background settled redeem scan fetched redeemable proxy-wallet positions"
            );
            rounds
        }
        Err(error) => {
            warn!(
                ?error,
                user,
                "background settled redeem scan could not load redeemable positions; falling back to recent settled rounds only"
            );
            Vec::new()
        }
    };

    let limit = settled_redeem_scan_limit(settings);
    let recent_rounds = market_discovery
        .discover_recent_settled_rounds(settings.execution.settled_redeem_scan_lookback_secs, limit)
        .await?;
    debug!(
        round_count = recent_rounds.len(),
        lookback_secs = settings.execution.settled_redeem_scan_lookback_secs,
        limit,
        "background settled redeem scan fetched candidate rounds"
    );

    for round in recent_rounds {
        if rounds
            .iter()
            .all(|candidate| candidate.condition_id != round.condition_id)
        {
            rounds.push(round);
        }
    }

    for round in rounds {
        match gateway.redeem_positions_if_resolved(&round).await {
            Ok(Some(receipt)) => {
                info!(
                    condition_id = %round.condition_id,
                    market_slug = %round.market_slug,
                    tx_hash = %receipt.tx_hash,
                    block_number = ?receipt.block_number,
                    "background sweep redeemed settled conditional tokens back to collateral"
                );
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    ?error,
                    condition_id = %round.condition_id,
                    market_slug = %round.market_slug,
                    "background sweep failed to redeem settled positions"
                );
            }
        }
    }

    Ok(())
}

fn settled_redeem_scan_limit(settings: &Settings) -> usize {
    let round_interval_secs = settings.strategy.round_interval_secs.max(1);
    let rounds_in_window =
        settings.execution.settled_redeem_scan_lookback_secs / round_interval_secs + 1;
    rounds_in_window.saturating_mul(16).clamp(200, 2_000) as usize
}
