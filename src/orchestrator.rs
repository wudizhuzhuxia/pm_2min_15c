use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use crate::{
    config::{Settings, StrategyMode},
    execution::{
        ExecutionGateway, OpenOrderResponse, OrderBookResponse, OrderStatusResponse, OrderType,
        PostOrderResponse,
    },
    market::{MarketDiscoveryService, RoundDescriptor},
    notifier::Notifier,
    paper::{
        PaperBranchState, PaperExitReason, PaperLimitExitPosition, PaperMakerOrder,
        PaperMakerOrderStatus, PaperRoundState, PaperRuntime, exit_reason_label, limit_exit_label,
        make_paper_buy_fill, make_paper_sell_fill,
    },
    realtime::{ReactiveRealtimeFeed, RealtimeBestQuote, RealtimeOrderUpdate},
    strategy::{LegSide, OrderPlan, OrderSide, StrategySnapshot, leg_label, side_label},
};

const MAIN_LOOP_TICK_MS: u64 = 100;
const DISCOVERY_REFRESH_SECS: i64 = 2;
const REDEEM_RETRY_LIMIT: usize = 900;
const REACTIVE_SELL_BALANCE_POLL_MS: u64 = 150;
const REACTIVE_SELL_SETTLEMENT_GRACE_MS: i64 = 2_000;
const REACTIVE_BEST_QUOTE_MAX_STALENESS_MS: i64 = 1_500;
const SUBMISSION_RECOVERY_POLL_MS: i64 = 750;

pub struct Orchestrator<'a> {
    settings: &'a Settings,
    notifier: &'a Notifier,
    market_discovery: &'a MarketDiscoveryService,
    execution_gateway: Option<Arc<ExecutionGateway>>,
}

impl<'a> Orchestrator<'a> {
    pub fn new(
        settings: &'a Settings,
        notifier: &'a Notifier,
        market_discovery: &'a MarketDiscoveryService,
        execution_gateway: Option<Arc<ExecutionGateway>>,
    ) -> Self {
        Self {
            settings,
            notifier,
            market_discovery,
            execution_gateway,
        }
    }

    pub async fn run_forever(&self) -> Result<()> {
        let strategy = StrategySnapshot::from_config(&self.settings.strategy)?;
        let mut managed = HashMap::<String, ManagedRound>::new();
        let mut next_discovery_at = Utc::now();
        let mut paper_runtime = if strategy.uses_paper_trading() {
            let runtime = PaperRuntime::new(
                &self.settings.app.instance_name,
                &strategy.paper_output_dir,
                strategy.paper_fee_rebate_rate,
            )?;
            info!(
                paper_output_dir = %runtime.session_dir().display(),
                "paper trading runtime ready"
            );
            Some(runtime)
        } else {
            None
        };
        let mut realtime_feed = if strategy.requires_realtime_quotes() && !self.settings.app.dry_run
        {
            if strategy.uses_paper_trading() {
                Some(ReactiveRealtimeFeed::spawn_market_only())
            } else {
                self.execution_gateway
                    .clone()
                    .map(ReactiveRealtimeFeed::spawn)
            }
        } else {
            None
        };
        let mut carried_market_updates = Vec::<crate::realtime::RealtimeMarketUpdate>::new();

        loop {
            let now = Utc::now();
            if now >= next_discovery_at || managed.len() < strategy.window_size_rounds {
                if let Err(error) = self.refresh_managed_rounds(&strategy, &mut managed).await {
                    warn!(?error, "failed to refresh upcoming rounds");
                    self.notify_error(format!("refresh upcoming rounds failed: {error:#}"))
                        .await;
                }
                next_discovery_at = now + ChronoDuration::seconds(DISCOVERY_REFRESH_SECS);
            }

            let mut market_changed_assets = HashSet::<String>::new();
            market_changed_assets.extend(
                carried_market_updates
                    .drain(..)
                    .map(|update| update.asset_id),
            );
            if let Some(feed) = realtime_feed.as_mut() {
                market_changed_assets.extend(
                    feed.drain_market_updates()
                        .into_iter()
                        .map(|update| update.asset_id),
                );
                feed.sync_rounds(managed.values().map(|managed| &managed.round));
                if strategy.uses_reactive_taker_flip() {
                    self.process_realtime_order_updates(&strategy, &mut managed, feed)
                        .await?;
                }
            }

            let mut ordered_ids = managed
                .values()
                .map(|round| (round.round.opens_at, round.round.condition_id.clone()))
                .collect::<Vec<_>>();
            ordered_ids
                .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

            for (_, condition_id) in ordered_ids {
                let Some(round) = managed.get_mut(&condition_id) else {
                    continue;
                };

                let result = if strategy.uses_paper_trading() {
                    let runtime = paper_runtime
                        .as_mut()
                        .context("paper runtime is unavailable for paper strategy mode")?;
                    self.drive_paper_round(
                        &strategy,
                        round,
                        realtime_feed.as_ref(),
                        &market_changed_assets,
                        runtime,
                    )
                    .await
                } else {
                    self.drive_round(&strategy, round).await
                };

                if let Err(error) = result {
                    warn!(
                        ?error,
                        condition_id = %round.round.condition_id,
                        market_slug = %round.round.market_slug,
                        "managed round iteration failed"
                    );
                    self.notify_error(format!(
                        "condition_id: {}\nmarket_slug: {}\nerror: {error:#}",
                        round.round.condition_id, round.round.market_slug
                    ))
                    .await;
                }
            }

            managed.retain(|_, round| !round.completed);
            if strategy.uses_paper_trading() {
                let wait_for = next_paper_wait_duration(&strategy, &managed, next_discovery_at);
                if let Some(feed) = realtime_feed.as_mut() {
                    if let Some(update) = feed.wait_for_market_update(wait_for).await {
                        carried_market_updates.push(update);
                    }
                } else {
                    tokio::time::sleep(wait_for).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(MAIN_LOOP_TICK_MS)).await;
            }
        }
    }

    async fn refresh_managed_rounds(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut HashMap<String, ManagedRound>,
    ) -> Result<()> {
        let now = Utc::now();
        let lookahead_secs = self.settings.market.discovery_lookahead_secs.max(
            strategy
                .round_interval_secs
                .saturating_mul(strategy.window_size_rounds as u64 + 3),
        );
        let limit = strategy.window_size_rounds.saturating_mul(8).clamp(4, 64);
        let rounds = self
            .market_discovery
            .discover_upcoming_rounds(lookahead_secs, limit)
            .await?;

        for round in rounds {
            if managed.len() >= strategy.window_size_rounds {
                break;
            }
            if managed.contains_key(&round.condition_id) {
                continue;
            }
            if round_cancel_at(&round, strategy) <= now {
                continue;
            }

            info!(
                condition_id = %round.condition_id,
                market_slug = %round.market_slug,
                opens_at = %round.opens_at,
                settles_at = %round.settles_at,
                "tracking future round"
            );

            if !self.settings.app.dry_run {
                if let Some(gateway) = &self.execution_gateway {
                    if let Err(error) = gateway.prewarm_round(&round).await {
                        warn!(
                            ?error,
                            condition_id = %round.condition_id,
                            "failed to prewarm round metadata"
                        );
                    }
                }
            }

            managed.insert(round.condition_id.clone(), ManagedRound::new(round));
        }

        Ok(())
    }

    async fn drive_paper_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        realtime_feed: Option<&ReactiveRealtimeFeed>,
        market_changed_assets: &HashSet<String>,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let now = Utc::now();

        if managed.paper_state.is_none() {
            managed.paper_state = Some(PaperRoundState::new());
        }

        if !managed.orders_submitted
            && now >= round_quote_start_at(&managed.round, strategy)
            && now < round_cancel_at(&managed.round, strategy)
        {
            self.submit_paper_orders(strategy, managed, paper_runtime)
                .await?;
        }

        if should_monitor_paper_round(managed) {
            self.monitor_paper_round(
                strategy,
                managed,
                realtime_feed,
                market_changed_assets,
                paper_runtime,
            )
            .await?;
        }

        if !paper_pre_open_cancel_processed(managed)
            && now >= round_cancel_at(&managed.round, strategy)
        {
            self.cancel_paper_resting_orders(managed, paper_runtime)?;
        }

        if let Some(paper_state) = managed.paper_state.as_ref() {
            if paper_state.has_triggered()
                && !paper_state.summaries_recorded
                && now >= managed.round.settles_at
            {
                self.finalize_paper_round(strategy, managed, paper_runtime)
                    .await?;
            } else if paper_state.pre_open_cancel_processed
                && !managed.completed
                && now >= managed.round.settles_at
            {
                managed.completed = true;
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    "paper round completed without simulated trigger"
                );
            }
        }

        Ok(())
    }

    async fn drive_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        let now = Utc::now();

        if !managed.split_attempted
            && matches!(strategy.mode, StrategyMode::PreSplitDualSell)
            && now >= round_split_at(&managed.round, strategy)
            && now < managed.round.opens_at
        {
            self.maybe_split_round(strategy, managed).await?;
        }

        if !managed.orders_submitted
            && !managed.submission_attempted
            && now >= round_quote_start_at(&managed.round, strategy)
            && now < round_cancel_at(&managed.round, strategy)
        {
            self.maybe_submit_orders(strategy, managed).await?;
        }

        if managed.pending_submission.is_some()
            && !managed.cancel_processed
            && now < round_cancel_at(&managed.round, strategy)
        {
            self.recover_timed_out_submission(managed).await?;
        }

        if strategy.uses_open_post_price_guard()
            && managed.orders_submitted
            && !managed.cancel_processed
            && now < round_cancel_at(&managed.round, strategy)
        {
            self.monitor_open_post_strategy(managed).await?;
        }

        if strategy.uses_reactive_taker_flip()
            && managed.orders_submitted
            && !managed.cancel_processed
            && now < round_cancel_at(&managed.round, strategy)
        {
            self.monitor_reactive_fills(strategy, managed).await?;
        }

        if !managed.cancel_processed && now >= round_cancel_at(&managed.round, strategy) {
            self.cancel_resting_orders(strategy, managed).await?;
        }

        if managed.cancel_processed && !managed.completed {
            self.finalize_round(strategy, managed).await?;
        }

        Ok(())
    }

    async fn submit_paper_orders(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let gateway = self.live_gateway()?;
        let plans = strategy.order_plans();
        let mut maker_orders = Vec::with_capacity(plans.len());

        for plan in plans {
            let token_id = token_id_for_leg(&managed.round, plan.leg);
            let mut order = PaperMakerOrder::new(plan.leg, plan.price, plan.size);
            if let Some(book) = gateway.fetch_order_book(token_id).await? {
                if book
                    .best_ask()
                    .map(|best_ask| best_ask <= plan.price)
                    .unwrap_or(false)
                {
                    order.status = PaperMakerOrderStatus::Rejected;
                    order.rejection_reason = Some("post only would cross".to_owned());
                }
            }
            maker_orders.push(order);
        }

        managed.orders_submitted = true;
        let paper_state = managed
            .paper_state
            .as_mut()
            .context("paper state was not initialized before order submission")?;
        paper_state.maker_orders = maker_orders.clone();
        paper_state.pending_entry_eval = true;
        paper_state.pending_exit_eval = false;

        paper_runtime.log_round_submitted(&managed.round, &maker_orders)?;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            order_count = maker_orders.len(),
            quote_start_at = %round_quote_start_at(&managed.round, strategy),
            cancel_at = %round_cancel_at(&managed.round, strategy),
            "submitted paper pre-open maker orders"
        );

        Ok(())
    }

    async fn monitor_paper_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        realtime_feed: Option<&ReactiveRealtimeFeed>,
        market_changed_assets: &HashSet<String>,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let now = Utc::now();
        let needs_entry_eval = paper_entry_eval_requested(managed, market_changed_assets);
        let mut needs_exit_eval =
            paper_exit_eval_requested(strategy, managed, market_changed_assets, now);
        let mut entry_triggered = false;

        if needs_entry_eval
            && !managed
                .paper_state
                .as_ref()
                .map(PaperRoundState::has_triggered)
                .unwrap_or(false)
        {
            let gateway = self.live_gateway()?;
            let mut trigger = None;
            let mut canceled_legs = Vec::new();

            let resting_orders = managed
                .paper_state
                .as_ref()
                .context("paper state missing during maker monitoring")?
                .maker_orders
                .iter()
                .filter(|order| order.status == PaperMakerOrderStatus::Resting)
                .map(|order| (order.leg, order.price, order.size))
                .collect::<Vec<_>>();

            for (leg, price, size) in resting_orders {
                let token_id = token_id_for_leg(&managed.round, leg);
                let book = match realtime_feed {
                    Some(feed) => feed.order_book(token_id).await,
                    None => None,
                }
                .or(gateway.fetch_order_book(token_id).await?);
                let Some(book) = book else {
                    continue;
                };
                if book
                    .best_ask()
                    .map(|best_ask| best_ask > price)
                    .unwrap_or(true)
                {
                    continue;
                }
                if book.ask_depth_through_price(price) < size {
                    continue;
                }

                trigger = Some((leg, price, size));
                break;
            }

            if let Some(paper_state) = managed.paper_state.as_mut() {
                paper_state.pending_entry_eval = false;
            }

            if let Some((filled_leg, maker_price, maker_size)) = trigger {
                let paper_state = managed
                    .paper_state
                    .as_mut()
                    .context("paper state missing when applying maker trigger")?;
                let filled_at = Utc::now();
                for order in &mut paper_state.maker_orders {
                    if order.leg == filled_leg {
                        order.status = PaperMakerOrderStatus::Filled;
                        order.filled_at = Some(filled_at);
                    } else if order.status == PaperMakerOrderStatus::Resting {
                        order.status = PaperMakerOrderStatus::Canceled;
                        canceled_legs.push(order.leg);
                    }
                }
                paper_state.triggered_leg = Some(filled_leg);
                paper_state.trigger_at = Some(filled_at);
                trigger = Some((filled_leg, maker_price, maker_size));
            }

            if let Some((filled_leg, maker_price, maker_size)) = trigger {
                paper_runtime.log_event(
                    &managed.round,
                    "paper_maker_filled",
                    None,
                    serde_json::json!({
                        "filled_leg": leg_label(filled_leg),
                        "maker_price": maker_price,
                        "maker_size": maker_size,
                    }),
                )?;

                for leg in canceled_legs {
                    paper_runtime.log_event(
                        &managed.round,
                        "paper_opposite_maker_canceled",
                        None,
                        serde_json::json!({
                            "leg": leg_label(leg),
                        }),
                    )?;
                }

                if strategy.uses_paper_tpsl() {
                    self.execute_paper_entry(
                        strategy,
                        managed,
                        realtime_feed,
                        paper_runtime,
                        filled_leg,
                        maker_price,
                        maker_size,
                    )
                    .await?;
                } else if strategy.uses_paper_limit_exit() {
                    self.execute_paper_limit_exit_entry(
                        strategy,
                        managed,
                        paper_runtime,
                        filled_leg,
                        maker_price,
                        maker_size,
                    )?;
                }
                entry_triggered = true;
            }
        }

        if entry_triggered {
            needs_exit_eval = true;
        }

        if needs_exit_eval {
            if strategy.uses_paper_tpsl() {
                self.monitor_paper_branches(
                    strategy,
                    managed,
                    realtime_feed,
                    market_changed_assets,
                    paper_runtime,
                )
                .await?;
            } else if strategy.uses_paper_limit_exit() {
                self.monitor_paper_limit_exit_position(managed, realtime_feed, paper_runtime, now)
                    .await?;
            }
        }

        Ok(())
    }

    async fn execute_paper_entry(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        realtime_feed: Option<&ReactiveRealtimeFeed>,
        paper_runtime: &mut PaperRuntime,
        filled_leg: LegSide,
        maker_price: Decimal,
        maker_size: Decimal,
    ) -> Result<()> {
        let gateway = self.live_gateway()?;
        let taker_leg = opposite_leg(filled_leg);
        let target_size = (maker_size + strategy.paper_extra_shares).trunc_with_scale(2);
        let token_id = token_id_for_leg(&managed.round, taker_leg);
        let token_id_value = gateway.token_id_for_leg(&managed.round, taker_leg)?;
        let metadata = gateway.market_metadata(token_id_value).await?;
        let taker_fill = match realtime_feed {
            Some(feed) => feed.order_book(token_id).await,
            None => None,
        }
        .or(gateway.fetch_order_book(token_id).await?)
        .and_then(|book| book.estimate_buy_for_size(target_size))
        .and_then(|estimate| make_paper_buy_fill(estimate, &metadata));

        if let Some(fill) = taker_fill {
            paper_runtime.log_event(
                &managed.round,
                "paper_taker_buy",
                None,
                serde_json::json!({
                    "filled_leg": leg_label(filled_leg),
                    "taker_leg": leg_label(taker_leg),
                    "target_size": target_size,
                    "filled_size": fill.size,
                    "limit_price": fill.limit_price,
                    "quote": fill.quote,
                    "fee_usdc": fill.fee_usdc,
                    "average_price": fill.average_price(),
                }),
            )?;
        } else {
            paper_runtime.log_event(
                &managed.round,
                "paper_taker_buy",
                None,
                serde_json::json!({
                    "filled_leg": leg_label(filled_leg),
                    "taker_leg": leg_label(taker_leg),
                    "target_size": target_size,
                    "filled_size": "0",
                    "reason": "no usable ask depth",
                }),
            )?;
        }

        let branches = strategy
            .paper_take_profit_percents
            .iter()
            .copied()
            .map(|take_profit_percent| {
                let mut branch =
                    PaperBranchState::new(take_profit_percent, strategy.paper_stop_loss_price);
                branch.initialize(filled_leg, maker_price, maker_size, taker_leg, taker_fill);
                branch
            })
            .collect::<Vec<_>>();

        for branch in &branches {
            paper_runtime.log_event(
                &managed.round,
                "paper_branch_initialized",
                Some(branch.take_profit_percent),
                serde_json::json!({
                    "filled_leg": leg_label(filled_leg),
                    "taker_leg": leg_label(taker_leg),
                    "maker_size": maker_size,
                    "maker_price": maker_price,
                    "taker_size": branch.taker_size,
                    "taker_average_price": branch.taker_average_price,
                    "speculative_size_initial": branch.speculative_size_initial,
                    "take_profit_price": branch.take_profit_price,
                    "stop_loss_price": branch.stop_loss_price,
                    "pair_cost_delta_usdc": branch.pair_cost_delta_usdc,
                }),
            )?;
        }

        let paper_state = managed
            .paper_state
            .as_mut()
            .context("paper state missing when recording entry branches")?;
        paper_state.branches = branches;
        paper_state.pending_entry_eval = false;
        paper_state.pending_exit_eval = paper_state
            .branches
            .iter()
            .any(PaperBranchState::has_speculative_position);
        Ok(())
    }

    fn execute_paper_limit_exit_entry(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        paper_runtime: &mut PaperRuntime,
        filled_leg: LegSide,
        maker_price: Decimal,
        maker_size: Decimal,
    ) -> Result<()> {
        let label = limit_exit_label(
            strategy.paper_limit_exit_price,
            strategy.paper_force_taker_exit_before_settle_secs,
        );
        let position = PaperLimitExitPosition::new(
            label.clone(),
            filled_leg,
            maker_price,
            maker_size,
            strategy.paper_limit_exit_price,
            paper_force_taker_exit_at(&managed.round, strategy),
        );

        paper_runtime.log_event_label(
            &managed.round,
            "paper_limit_exit_armed",
            Some(label),
            serde_json::json!({
                "leg": leg_label(filled_leg),
                "entry_price": maker_price,
                "entry_size": maker_size,
                "limit_exit_price": strategy.paper_limit_exit_price,
                "force_taker_exit_at": position.force_taker_exit_at,
            }),
        )?;

        let paper_state = managed
            .paper_state
            .as_mut()
            .context("paper state missing when recording limit-exit entry")?;
        paper_state.limit_exit_position = Some(position);
        paper_state.pending_entry_eval = false;
        paper_state.pending_exit_eval = true;
        Ok(())
    }

    async fn monitor_paper_branches(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        realtime_feed: Option<&ReactiveRealtimeFeed>,
        market_changed_assets: &HashSet<String>,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let Some(spec_leg) = managed.paper_state.as_ref().and_then(|paper_state| {
            paper_state
                .branches
                .iter()
                .find(|branch| branch.has_speculative_position())
                .and_then(PaperBranchState::speculative_leg)
        }) else {
            return Ok(());
        };

        if !paper_exit_eval_requested(strategy, managed, market_changed_assets, Utc::now()) {
            return Ok(());
        }

        let token_id = token_id_for_leg(&managed.round, spec_leg);
        let opposite_leg = opposite_leg(spec_leg);
        let opposite_token_id = token_id_for_leg(&managed.round, opposite_leg);
        let gateway = self.live_gateway()?;
        let spec_book = match realtime_feed {
            Some(feed) => feed.order_book(token_id).await,
            None => None,
        }
        .or(gateway.fetch_order_book(token_id).await?);
        let opposite_book = match realtime_feed {
            Some(feed) => feed.order_book(opposite_token_id).await,
            None => None,
        }
        .or(gateway.fetch_order_book(opposite_token_id).await?);
        let Some(book) = spec_book else {
            paper_runtime.log_event(
                &managed.round,
                "paper_exit_check",
                None,
                serde_json::json!({
                    "spec_leg": leg_label(spec_leg),
                    "opposite_leg": leg_label(opposite_leg),
                    "status": "missing_spec_book",
                }),
            )?;
            return Ok(());
        };
        let spec_best_bid = book.best_bid();
        let spec_best_ask = book.best_ask();
        let opposite_best_bid = opposite_book.as_ref().and_then(OrderBookResponse::best_bid);
        let opposite_best_ask = opposite_book.as_ref().and_then(OrderBookResponse::best_ask);
        let single_leg_ref = spec_best_bid;
        let composite_ref =
            opposite_best_ask.map(|price| (Decimal::ONE - price).trunc_with_scale(4));

        let (actions, branch_checks) = {
            let paper_state = managed
                .paper_state
                .as_ref()
                .context("paper state missing when scanning branch exits")?;
            let actions = paper_state
                .branches
                .iter()
                .enumerate()
                .filter_map(|(index, branch)| {
                    if branch.is_settled() || !branch.has_speculative_position() {
                        return None;
                    }

                    let reason = if branch.should_take_profit(single_leg_ref) {
                        Some(PaperExitReason::TakeProfit)
                    } else if branch.should_stop_loss(single_leg_ref) {
                        Some(PaperExitReason::StopLoss)
                    } else {
                        None
                    }?;

                    Some((
                        index,
                        branch.take_profit_percent,
                        branch.speculative_size_remaining,
                        reason,
                    ))
                })
                .collect::<Vec<_>>();
            let branch_checks = paper_state
                .branches
                .iter()
                .filter(|branch| !branch.is_settled() && branch.has_speculative_position())
                .map(|branch| {
                    let composite_tp_hit = composite_ref
                        .zip(branch.take_profit_price)
                        .map(|(value, target)| value >= target)
                        .unwrap_or(false);
                    let composite_sl_hit = composite_ref
                        .map(|value| value <= branch.stop_loss_price)
                        .unwrap_or(false);
                    serde_json::json!({
                        "take_profit_percent": branch.take_profit_percent,
                        "take_profit_price": branch.take_profit_price,
                        "stop_loss_price": branch.stop_loss_price,
                        "single_leg_tp_hit": branch.should_take_profit(single_leg_ref),
                        "single_leg_sl_hit": branch.should_stop_loss(single_leg_ref),
                        "composite_tp_hit": composite_tp_hit,
                        "composite_sl_hit": composite_sl_hit,
                    })
                })
                .collect::<Vec<_>>();
            (actions, branch_checks)
        };

        if let Some(paper_state) = managed.paper_state.as_mut() {
            paper_state.pending_exit_eval = false;
            paper_state.exit_diagnostics.observe(
                spec_best_bid,
                spec_best_ask,
                opposite_best_bid,
                opposite_best_ask,
                single_leg_ref,
                composite_ref,
            );
        }

        paper_runtime.log_event(
            &managed.round,
            "paper_exit_check",
            None,
            serde_json::json!({
                "spec_leg": leg_label(spec_leg),
                "opposite_leg": leg_label(opposite_leg),
                "spec_best_bid": spec_best_bid,
                "spec_best_ask": spec_best_ask,
                "opposite_best_bid": opposite_best_bid,
                "opposite_best_ask": opposite_best_ask,
                "single_leg_ref": single_leg_ref,
                "composite_ref": composite_ref,
                "branch_checks": branch_checks,
            }),
        )?;

        if actions.is_empty() {
            return Ok(());
        }

        let token_id_value = gateway.token_id_for_leg(&managed.round, spec_leg)?;
        let metadata = gateway.market_metadata(token_id_value).await?;

        for (index, take_profit_percent, target_size, reason) in actions {
            let Some(fill) = book
                .estimate_sell_for_size(target_size)
                .and_then(|estimate| make_paper_sell_fill(estimate, &metadata))
            else {
                continue;
            };

            let branch = managed
                .paper_state
                .as_mut()
                .context("paper state missing when applying branch exit")?
                .branches
                .get_mut(index)
                .context("paper branch index out of bounds")?;
            branch.apply_speculative_exit(&fill, reason)?;

            paper_runtime.log_event(
                &managed.round,
                "paper_branch_exit",
                Some(take_profit_percent),
                serde_json::json!({
                    "reason": exit_reason_label(reason),
                    "leg": leg_label(spec_leg),
                    "best_bid": spec_best_bid,
                    "best_ask": spec_best_ask,
                    "opposite_best_bid": opposite_best_bid,
                    "opposite_best_ask": opposite_best_ask,
                    "single_leg_ref": single_leg_ref,
                    "composite_ref": composite_ref,
                    "filled_size": fill.size,
                    "quote": fill.quote,
                    "fee_usdc": fill.fee_usdc,
                    "average_price": fill.average_price(),
                    "remaining_speculative_size": branch.speculative_size_remaining,
                }),
            )?;
        }

        Ok(())
    }

    async fn monitor_paper_limit_exit_position(
        &self,
        managed: &mut ManagedRound,
        realtime_feed: Option<&ReactiveRealtimeFeed>,
        paper_runtime: &mut PaperRuntime,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let Some((label, held_leg, remaining_size, exit_limit_price, force_taker_exit_at)) =
            managed
                .paper_state
                .as_ref()
                .and_then(|paper_state| paper_state.limit_exit_position.as_ref())
                .filter(|position| position.has_open_position())
                .map(|position| {
                    (
                        position.label.clone(),
                        position.leg,
                        position.remaining_size,
                        position.exit_limit_price,
                        position.force_taker_exit_at,
                    )
                })
        else {
            return Ok(());
        };

        let token_id = token_id_for_leg(&managed.round, held_leg);
        let opposite_leg = opposite_leg(held_leg);
        let opposite_token_id = token_id_for_leg(&managed.round, opposite_leg);
        let gateway = self.live_gateway()?;
        let held_book = match realtime_feed {
            Some(feed) => feed.order_book(token_id).await,
            None => None,
        }
        .or(gateway.fetch_order_book(token_id).await?);
        let opposite_book = match realtime_feed {
            Some(feed) => feed.order_book(opposite_token_id).await,
            None => None,
        }
        .or(gateway.fetch_order_book(opposite_token_id).await?);

        let Some(book) = held_book else {
            paper_runtime.log_event_label(
                &managed.round,
                "paper_exit_check",
                Some(label),
                serde_json::json!({
                    "held_leg": leg_label(held_leg),
                    "opposite_leg": leg_label(opposite_leg),
                    "status": "missing_held_book",
                }),
            )?;
            return Ok(());
        };

        let best_bid = book.best_bid();
        let best_ask = book.best_ask();
        let opposite_best_bid = opposite_book.as_ref().and_then(OrderBookResponse::best_bid);
        let opposite_best_ask = opposite_book.as_ref().and_then(OrderBookResponse::best_ask);
        let single_leg_ref = best_bid;
        let composite_ref =
            opposite_best_ask.map(|price| (Decimal::ONE - price).trunc_with_scale(4));
        let bid_depth_through_limit = book.bid_depth_through_price(exit_limit_price);
        let deadline_reached = now >= force_taker_exit_at;
        let limit_sell_fillable = !deadline_reached && bid_depth_through_limit >= remaining_size;

        if let Some(paper_state) = managed.paper_state.as_mut() {
            paper_state.pending_exit_eval = false;
            paper_state.exit_diagnostics.observe(
                best_bid,
                best_ask,
                opposite_best_bid,
                opposite_best_ask,
                single_leg_ref,
                composite_ref,
            );
        }

        paper_runtime.log_event_label(
            &managed.round,
            "paper_exit_check",
            Some(label.clone()),
            serde_json::json!({
                "held_leg": leg_label(held_leg),
                "opposite_leg": leg_label(opposite_leg),
                "remaining_size": remaining_size,
                "limit_exit_price": exit_limit_price,
                "force_taker_exit_at": force_taker_exit_at,
                "deadline_reached": deadline_reached,
                "limit_sell_fillable": limit_sell_fillable,
                "bid_depth_through_limit": bid_depth_through_limit,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "opposite_best_bid": opposite_best_bid,
                "opposite_best_ask": opposite_best_ask,
                "single_leg_ref": single_leg_ref,
                "composite_ref": composite_ref,
            }),
        )?;

        if limit_sell_fillable {
            let position = managed
                .paper_state
                .as_mut()
                .context("paper state missing when applying limit-exit fill")?
                .limit_exit_position
                .as_mut()
                .context("paper limit-exit position missing during maker exit")?;
            position.apply_limit_exit(remaining_size)?;

            paper_runtime.log_event_label(
                &managed.round,
                "paper_limit_exit_fill",
                Some(position.label.clone()),
                serde_json::json!({
                    "reason": exit_reason_label(PaperExitReason::LimitSell),
                    "leg": leg_label(held_leg),
                    "limit_exit_price": exit_limit_price,
                    "filled_size": remaining_size,
                    "quote": (remaining_size * exit_limit_price).trunc_with_scale(6),
                    "remaining_size": position.remaining_size,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                }),
            )?;
            return Ok(());
        }

        if !deadline_reached {
            return Ok(());
        }

        let token_id_value = gateway.token_id_for_leg(&managed.round, held_leg)?;
        let metadata = gateway.market_metadata(token_id_value).await?;
        let taker_fill = book
            .estimate_sell_for_size(remaining_size)
            .and_then(|estimate| make_paper_sell_fill(estimate, &metadata));

        let position = managed
            .paper_state
            .as_mut()
            .context("paper state missing when applying forced taker exit")?
            .limit_exit_position
            .as_mut()
            .context("paper limit-exit position missing during forced taker exit")?;

        if let Some(fill) = taker_fill {
            position.apply_taker_exit(&fill)?;
            paper_runtime.log_event_label(
                &managed.round,
                "paper_forced_taker_exit",
                Some(position.label.clone()),
                serde_json::json!({
                    "reason": exit_reason_label(PaperExitReason::ForcedTakerExit),
                    "leg": leg_label(held_leg),
                    "filled_size": fill.size,
                    "quote": fill.quote,
                    "fee_usdc": fill.fee_usdc,
                    "average_price": fill.average_price(),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "remaining_size": position.remaining_size,
                }),
            )?;
        } else {
            paper_runtime.log_event_label(
                &managed.round,
                "paper_forced_taker_exit",
                Some(position.label.clone()),
                serde_json::json!({
                    "leg": leg_label(held_leg),
                    "remaining_size": position.remaining_size,
                    "status": "no_usable_bid_depth",
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                }),
            )?;
        }

        Ok(())
    }

    fn cancel_paper_resting_orders(
        &self,
        managed: &mut ManagedRound,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let paper_state = managed
            .paper_state
            .as_mut()
            .context("paper state missing during cancel")?;
        let mut canceled = 0usize;

        for order in &mut paper_state.maker_orders {
            if order.status == PaperMakerOrderStatus::Resting {
                order.status = PaperMakerOrderStatus::Canceled;
                canceled += 1;
            }
        }

        paper_state.pre_open_cancel_processed = true;
        paper_runtime.log_event(
            &managed.round,
            "paper_pre_open_cancel",
            None,
            serde_json::json!({
                "canceled": canceled,
            }),
        )?;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            canceled,
            "processed paper pre-open cancel for resting orders"
        );
        Ok(())
    }

    async fn finalize_paper_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        paper_runtime: &mut PaperRuntime,
    ) -> Result<()> {
        let paper_state = managed
            .paper_state
            .as_mut()
            .context("paper state missing during finalize")?;
        let Some(outcome) = self
            .market_discovery
            .fetch_round_outcome(&managed.round)
            .await?
        else {
            if !paper_state.waiting_resolution_logged {
                paper_runtime.log_event(
                    &managed.round,
                    "paper_waiting_for_resolution",
                    None,
                    serde_json::json!({
                        "settles_at": managed.round.settles_at,
                        "now": Utc::now(),
                    }),
                )?;
                paper_state.waiting_resolution_logged = true;
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    settles_at = %managed.round.settles_at,
                    "paper round reached settle time but is still waiting for resolved outcome"
                );
            }
            return Ok(());
        };
        if paper_state.summaries_recorded {
            managed.completed = true;
            return Ok(());
        }

        if strategy.uses_paper_limit_exit() {
            if let Some(position) = paper_state.limit_exit_position.as_mut() {
                position.settle(&outcome, Utc::now());
                paper_runtime.log_event_label(
                    &managed.round,
                    "paper_limit_exit_settled",
                    Some(position.label.clone()),
                    serde_json::json!({
                        "yes_payout": outcome.yes_payout,
                        "no_payout": outcome.no_payout,
                        "settlement_value": position.settlement_value,
                        "yes_balance": position.yes_balance,
                        "no_balance": position.no_balance,
                    }),
                )?;

                paper_runtime.log_event_label(
                    &managed.round,
                    "paper_exit_diagnostic_summary",
                    Some(position.label.clone()),
                    serde_json::json!({
                        "held_leg": leg_label(position.leg),
                        "opposite_leg": leg_label(opposite_leg(position.leg)),
                        "limit_exit_price": position.exit_limit_price,
                        "force_taker_exit_at": position.force_taker_exit_at,
                        "observations": paper_state.exit_diagnostics.observations,
                        "max_spec_best_bid": paper_state.exit_diagnostics.max_spec_best_bid,
                        "min_spec_best_bid": paper_state.exit_diagnostics.min_spec_best_bid,
                        "max_spec_best_ask": paper_state.exit_diagnostics.max_spec_best_ask,
                        "min_spec_best_ask": paper_state.exit_diagnostics.min_spec_best_ask,
                        "max_opposite_best_bid": paper_state.exit_diagnostics.max_opp_best_bid,
                        "min_opposite_best_bid": paper_state.exit_diagnostics.min_opp_best_bid,
                        "max_opposite_best_ask": paper_state.exit_diagnostics.max_opp_best_ask,
                        "min_opposite_best_ask": paper_state.exit_diagnostics.min_opp_best_ask,
                        "max_single_leg_ref": paper_state.exit_diagnostics.max_single_leg_ref,
                        "min_single_leg_ref": paper_state.exit_diagnostics.min_single_leg_ref,
                        "max_composite_ref": paper_state.exit_diagnostics.max_composite_ref,
                        "min_composite_ref": paper_state.exit_diagnostics.min_composite_ref,
                        "exit_reason": position.exit_reason.map(exit_reason_label),
                    }),
                )?;

                if let Some(summary) =
                    position.summary(&managed.round, paper_runtime.fee_rebate_rate())
                {
                    paper_runtime.record_summary(summary.clone())?;
                    paper_runtime.log_event_label(
                        &managed.round,
                        "paper_limit_exit_summary",
                        Some(position.label.clone()),
                        serde_json::json!({
                            "net_pnl_normal": summary.net_pnl_normal,
                            "net_pnl_rebate": summary.net_pnl_rebate,
                            "fees_usdc": summary.total_fees_usdc,
                            "exit_reason": summary.exit_reason,
                        }),
                    )?;
                }
            }

            paper_state.summaries_recorded = true;
            managed.completed = true;
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                "paper limit-exit round settled and summary written"
            );
            return Ok(());
        }

        for branch in &mut paper_state.branches {
            branch.settle(&outcome, Utc::now());
            paper_runtime.log_event(
                &managed.round,
                "paper_branch_settled",
                Some(branch.take_profit_percent),
                serde_json::json!({
                    "yes_payout": outcome.yes_payout,
                    "no_payout": outcome.no_payout,
                    "settlement_value": branch.settlement_value,
                    "yes_balance": branch.yes_balance,
                    "no_balance": branch.no_balance,
                }),
            )?;
        }

        if let Some(spec_leg) = paper_state
            .branches
            .iter()
            .find_map(PaperBranchState::speculative_leg)
        {
            paper_runtime.log_event(
                &managed.round,
                "paper_exit_diagnostic_summary",
                None,
                serde_json::json!({
                    "spec_leg": leg_label(spec_leg),
                    "opposite_leg": leg_label(opposite_leg(spec_leg)),
                    "observations": paper_state.exit_diagnostics.observations,
                    "max_spec_best_bid": paper_state.exit_diagnostics.max_spec_best_bid,
                    "min_spec_best_bid": paper_state.exit_diagnostics.min_spec_best_bid,
                    "max_spec_best_ask": paper_state.exit_diagnostics.max_spec_best_ask,
                    "min_spec_best_ask": paper_state.exit_diagnostics.min_spec_best_ask,
                    "max_opposite_best_bid": paper_state.exit_diagnostics.max_opp_best_bid,
                    "min_opposite_best_bid": paper_state.exit_diagnostics.min_opp_best_bid,
                    "max_opposite_best_ask": paper_state.exit_diagnostics.max_opp_best_ask,
                    "min_opposite_best_ask": paper_state.exit_diagnostics.min_opp_best_ask,
                    "max_single_leg_ref": paper_state.exit_diagnostics.max_single_leg_ref,
                    "min_single_leg_ref": paper_state.exit_diagnostics.min_single_leg_ref,
                    "max_composite_ref": paper_state.exit_diagnostics.max_composite_ref,
                    "min_composite_ref": paper_state.exit_diagnostics.min_composite_ref,
                    "branch_thresholds": paper_state.branches.iter().map(|branch| serde_json::json!({
                        "take_profit_percent": branch.take_profit_percent,
                        "take_profit_price": branch.take_profit_price,
                        "stop_loss_price": branch.stop_loss_price,
                        "exit_reason": branch.spec_exit_reason.map(exit_reason_label),
                    })).collect::<Vec<_>>(),
                }),
            )?;
        }

        for branch in &paper_state.branches {
            if let Some(summary) = branch.summary(&managed.round, paper_runtime.fee_rebate_rate()) {
                paper_runtime.record_summary(summary.clone())?;
                paper_runtime.log_event(
                    &managed.round,
                    "paper_branch_summary",
                    Some(branch.take_profit_percent),
                    serde_json::json!({
                        "net_pnl_normal": summary.net_pnl_normal,
                        "net_pnl_rebate": summary.net_pnl_rebate,
                        "pair_cost_delta_usdc": summary.pair_cost_delta_usdc,
                        "fees_usdc": summary.total_fees_usdc,
                        "exit_reason": summary.exit_reason,
                    }),
                )?;
            }
        }

        paper_state.summaries_recorded = true;
        managed.completed = true;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            branch_count = paper_state.branches.len(),
            "paper round settled and summaries written"
        );
        Ok(())
    }

    async fn maybe_split_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        managed.split_attempted = true;

        if self.settings.app.dry_run {
            managed.split_confirmed = true;
            info!(
                condition_id = %managed.round.condition_id,
                amount = %strategy.split_amount(),
                "dry-run split completed"
            );
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        match gateway
            .split_position(&managed.round, strategy.split_amount())
            .await
        {
            Ok(receipt) => {
                managed.split_confirmed = true;
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    amount_usdc = %receipt.amount_usdc,
                    tx_hash = %receipt.tx_hash,
                    "split completed for managed round"
                );
            }
            Err(error) => {
                managed.split_error = Some(error.to_string());
                warn!(
                    ?error,
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    "split failed for managed round; skipping order placement for this round"
                );
            }
        }

        Ok(())
    }

    async fn maybe_submit_orders(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        if strategy.uses_open_post_price_guard() {
            managed.submission_attempted = true;
        }

        if matches!(strategy.mode, StrategyMode::PreSplitDualSell) && !managed.split_confirmed {
            if managed.split_attempted {
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    "split not confirmed; skipping order submission for this round"
                );
                managed.orders_submitted = true;
            }
            return Ok(());
        }

        let plans = strategy.order_plans();

        if self.settings.app.dry_run {
            managed.orders_submitted = true;
            managed.orders = plans
                .into_iter()
                .map(|plan| ManagedOrder::simulated(plan))
                .collect();
            info!(
                condition_id = %managed.round.condition_id,
                order_count = managed.orders.len(),
                "dry-run orders marked as simulated"
            );
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        let mut payloads = Vec::with_capacity(plans.len());
        for plan in plans {
            let payload = match plan.side {
                OrderSide::Buy => {
                    gateway
                        .build_leg_buy_order(
                            &managed.round,
                            plan.leg,
                            plan.price,
                            plan.size,
                            OrderType::GTC,
                            plan.post_only,
                        )
                        .await?
                }
                OrderSide::Sell => {
                    gateway
                        .build_leg_sell_order(
                            &managed.round,
                            plan.leg,
                            plan.price,
                            plan.size,
                            OrderType::GTC,
                            plan.post_only,
                        )
                        .await?
                }
            };
            payloads.push((plan, payload));
        }

        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            order_count = payloads.len(),
            quote_start_at = %round_quote_start_at(&managed.round, strategy),
            cancel_at = %round_cancel_at(&managed.round, strategy),
            "submitting pre-open CLOB orders"
        );

        let raw_orders = payloads
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<Vec<_>>();
        let responses = match gateway.post_orders(&raw_orders).await {
            Ok(responses) => responses,
            Err(error) if is_request_timeout_error(&error) => {
                let plans = payloads
                    .iter()
                    .map(|(plan, _)| plan.clone())
                    .collect::<Vec<_>>();
                managed.orders_submitted = true;
                managed.pending_submission = Some(PendingSubmissionRecovery::new(plans));
                warn!(
                    ?error,
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    order_count = payloads.len(),
                    "batch order request timed out after submit attempt; entering recovery mode to avoid duplicate reposts"
                );
                self.recover_timed_out_submission(managed).await?;
                return Ok(());
            }
            Err(error) => {
                if strategy.uses_open_post_price_guard() {
                    managed.cancel_processed = true;
                    managed.completed = true;
                    warn!(
                        ?error,
                        condition_id = %managed.round.condition_id,
                        market_slug = %managed.round.market_slug,
                        "open-post single submission attempt failed; skipping this round"
                    );
                    return Ok(());
                }
                return Err(error);
            }
        };
        if responses.len() != payloads.len() {
            return Err(anyhow::anyhow!(
                "expected {} batch responses, received {}",
                payloads.len(),
                responses.len()
            ));
        }

        managed.orders_submitted = true;
        managed.pending_submission = None;
        managed.orders.clear();

        for ((plan, _), response) in payloads.into_iter().zip(responses.into_iter()) {
            self.record_order_response(strategy, managed, plan, response);
        }

        Ok(())
    }

    fn record_order_response(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        plan: OrderPlan,
        response: PostOrderResponse,
    ) {
        let purpose = plan_purpose(strategy.mode, &plan);
        let error_message = response.error_message().map(str::to_owned);

        if response.has_live_order() {
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                purpose,
                leg = leg_label(plan.leg),
                side = side_label(plan.side),
                price = %plan.price,
                size = %plan.size,
                order_id = %response.order_id,
                status = %response.status,
                "order accepted by CLOB"
            );
            managed.orders.push(ManagedOrder::live(plan, response));
            return;
        }

        warn!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            purpose,
            leg = leg_label(plan.leg),
            side = side_label(plan.side),
            price = %plan.price,
            size = %plan.size,
            post_only_cross = response.is_post_only_would_cross(),
            error = error_message.as_deref().unwrap_or("unknown"),
            raw_status = %response.status,
            "order was not resting after batch submission"
        );
        managed.orders.push(ManagedOrder::rejected(plan, response));
    }

    async fn recover_timed_out_submission(&self, managed: &mut ManagedRound) -> Result<()> {
        let Some(recovery) = managed.pending_submission.as_mut() else {
            return Ok(());
        };
        if !recovery.should_check_now() {
            return Ok(());
        }

        recovery.last_check_at = Some(Utc::now());
        let gateway = self.live_gateway()?;
        let open_orders = gateway
            .fetch_open_orders(Some(&managed.round.market_id), None)
            .await?;

        let mut recovered = Vec::new();
        for plan in &recovery.plans {
            if managed
                .orders
                .iter()
                .any(|order| order.leg == plan.leg && order.side == plan.side)
            {
                continue;
            }

            let token_id = gateway
                .token_id_for_leg(&managed.round, plan.leg)?
                .to_string();
            let Some(open_order) = open_orders.iter().find(|order| {
                order.id.trim().len() > 0
                    && order.asset_id == token_id
                    && order_side_matches_open_order(plan.side, &order.side)
                    && order.price == Some(plan.price)
                    && order.original_size == Some(plan.size)
                    && !managed
                        .orders
                        .iter()
                        .any(|existing| existing.order_id.as_deref() == Some(order.id.as_str()))
                    && !recovered.iter().any(|existing: &ManagedOrder| {
                        existing.order_id.as_deref() == Some(order.id.as_str())
                    })
            }) else {
                continue;
            };

            recovered.push(ManagedOrder::from_open_order(plan.clone(), open_order));
        }

        for order in recovered {
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                leg = leg_label(order.leg),
                side = side_label(order.side),
                price = %order.price,
                size = %order.size,
                order_id = %order.order_id.as_deref().unwrap_or(""),
                "recovered open order after batch submit timeout"
            );
            managed.orders.push(order);
        }

        if managed.orders.len() >= recovery.plans.len() {
            managed.pending_submission = None;
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                recovered_orders = managed.orders.len(),
                "submission recovery completed"
            );
        }

        Ok(())
    }

    async fn monitor_open_post_strategy(&self, managed: &mut ManagedRound) -> Result<()> {
        let mut first_matched_leg = None;

        if !self.settings.app.dry_run {
            let gateway = self.live_gateway()?;
            for order in &mut managed.orders {
                if !order.needs_status_poll() {
                    continue;
                }

                let Some(order_id) = order.order_id.clone() else {
                    continue;
                };

                let status = match gateway.fetch_order_status(&order_id).await {
                    Ok(Some(status)) => status,
                    Ok(None) => continue,
                    Err(error) => {
                        warn!(
                            ?error,
                            condition_id = %managed.round.condition_id,
                            market_slug = %managed.round.market_slug,
                            leg = leg_label(order.leg),
                            side = side_label(order.side),
                            order_id,
                            "failed to poll maker order status during open-post monitoring"
                        );
                        continue;
                    }
                };

                let previous_matched_size = order.last_matched_size;
                order.observe_status(&status);
                if order.last_matched_size > previous_matched_size {
                    info!(
                        condition_id = %managed.round.condition_id,
                        market_slug = %managed.round.market_slug,
                        leg = leg_label(order.leg),
                        side = side_label(order.side),
                        maker_price = %order.price,
                        order_id,
                        matched_size = %order.last_matched_size,
                        fully_matched = status.is_fully_matched(),
                        status = %status.status,
                        "observed maker order match progress"
                    );
                }

                if first_matched_leg.is_none() && order.last_matched_size > Decimal::ZERO {
                    first_matched_leg = Some(order.leg);
                }
            }
        }

        if let Some(filled_leg) = first_matched_leg {
            self.note_open_post_first_match(managed, filled_leg);
        }

        Ok(())
    }

    async fn monitor_reactive_fills(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        if !strategy.uses_reactive_taker_flip() || self.settings.app.dry_run {
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        let mut triggers = Vec::new();

        for order in &mut managed.orders {
            if !order.needs_status_poll() {
                continue;
            }

            let Some(order_id) = order.order_id.clone() else {
                continue;
            };

            let status = match gateway.fetch_order_status(&order_id).await {
                Ok(Some(status)) => status,
                Ok(None) => {
                    debug!(
                        condition_id = %managed.round.condition_id,
                        market_slug = %managed.round.market_slug,
                        leg = leg_label(order.leg),
                        side = side_label(order.side),
                        order_id,
                        "maker order status not found during reactive poll; will retry"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        ?error,
                        condition_id = %managed.round.condition_id,
                        market_slug = %managed.round.market_slug,
                        leg = leg_label(order.leg),
                        side = side_label(order.side),
                        order_id,
                        "failed to poll maker order status during reactive monitoring"
                    );
                    continue;
                }
            };
            let previous_matched_size = order.last_matched_size;
            order.observe_status(&status);

            if order.last_matched_size > previous_matched_size {
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    leg = leg_label(order.leg),
                    side = side_label(order.side),
                    maker_price = %order.price,
                    order_id,
                    matched_size = %order.last_matched_size,
                    fully_matched = status.is_fully_matched(),
                    status = %status.status,
                    "observed maker order match progress"
                );
            }

            if status.is_fully_matched() && !order.trigger_attempted {
                order.mark_fully_matched();
                triggers.push(ReactiveTrigger {
                    filled_leg: order.leg,
                    maker_size: order.size,
                });
            }
        }

        for trigger in triggers {
            self.execute_reactive_taker_flip(strategy, managed, trigger, None)
                .await?;
        }

        Ok(())
    }

    async fn execute_reactive_taker_flip(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
        trigger: ReactiveTrigger,
        realtime_best_quote: Option<RealtimeBestQuote>,
    ) -> Result<()> {
        if self.settings.app.dry_run {
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        let opposite_leg = opposite_leg(trigger.filled_leg);
        let sell_deadline = managed.round.opens_at
            + ChronoDuration::milliseconds(REACTIVE_SELL_SETTLEMENT_GRACE_MS);

        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            filled_leg = leg_label(trigger.filled_leg),
            opposite_leg = leg_label(opposite_leg),
            maker_size = %trigger.maker_size,
            opposite_taker_usdc = %strategy.reactive_opposite_taker_usdc,
            "maker order fully matched; executing reactive taker flip"
        );

        match self
            .submit_reactive_taker_buy(
                gateway,
                &managed.round,
                opposite_leg,
                strategy.reactive_opposite_taker_usdc,
                strategy.reactive_buy_slippage_ticks,
                realtime_best_quote,
            )
            .await
        {
            Ok(Some(response)) => log_reactive_taker_response(
                &managed.round,
                "reactive_taker_buy",
                opposite_leg,
                OrderSide::Buy,
                &response,
            ),
            Ok(None) => warn!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                leg = leg_label(opposite_leg),
                "skipped reactive taker buy because book had no usable ask depth"
            ),
            Err(error) => warn!(
                ?error,
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                leg = leg_label(opposite_leg),
                "reactive taker buy failed"
            ),
        }

        match self
            .submit_reactive_taker_sell(
                gateway,
                &managed.round,
                trigger.filled_leg,
                trigger.maker_size,
                sell_deadline,
            )
            .await
        {
            Ok(Some(response)) => log_reactive_taker_response(
                &managed.round,
                "reactive_taker_sell",
                trigger.filled_leg,
                OrderSide::Sell,
                &response,
            ),
            Ok(None) => warn!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                leg = leg_label(trigger.filled_leg),
                "skipped reactive taker sell because book had no usable bid depth"
            ),
            Err(error) => warn!(
                ?error,
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                leg = leg_label(trigger.filled_leg),
                "reactive taker sell failed"
            ),
        }

        Ok(())
    }

    async fn submit_reactive_taker_buy(
        &self,
        gateway: &ExecutionGateway,
        round: &RoundDescriptor,
        leg: LegSide,
        quote_budget: Decimal,
        slippage_ticks: u32,
        realtime_best_quote: Option<RealtimeBestQuote>,
    ) -> Result<Option<PostOrderResponse>> {
        let token_id = token_id_for_leg(round, leg);
        let token_id_value = gateway.token_id_for_leg(round, leg)?;
        let metadata = gateway.market_metadata(token_id_value).await?;
        let decimals = metadata.tick_size.scale();
        let live_best_ask = realtime_best_quote
            .filter(|quote| {
                Utc::now()
                    .signed_duration_since(quote.updated_at)
                    .num_milliseconds()
                    <= REACTIVE_BEST_QUOTE_MAX_STALENESS_MS
            })
            .and_then(|quote| quote.best_ask);

        let (limit_price, estimated_size, estimated_quote) = if let Some(best_ask) = live_best_ask {
            let limit_price =
                apply_reactive_buy_slippage(best_ask, metadata.tick_size, slippage_ticks);
            let estimated_size =
                estimate_market_buy_size_for_quote(quote_budget, limit_price, decimals);
            if estimated_size <= Decimal::ZERO {
                return Ok(None);
            }
            (
                limit_price,
                estimated_size,
                quote_budget.trunc_with_scale(decimals),
            )
        } else {
            let Some(book) = gateway.fetch_order_book(token_id).await? else {
                return Ok(None);
            };
            let Some(estimate) = book.estimate_buy_for_quote(quote_budget) else {
                return Ok(None);
            };
            let limit_price =
                apply_reactive_buy_slippage(estimate.price, metadata.tick_size, slippage_ticks);
            let estimated_size =
                estimate_market_buy_size_for_quote(quote_budget, limit_price, decimals);
            if estimated_size <= Decimal::ZERO {
                return Ok(None);
            }
            (limit_price, estimated_size, estimate.quote)
        };

        let payload = gateway
            .build_leg_buy_market_order_by_quote(
                round,
                leg,
                limit_price,
                quote_budget,
                OrderType::FAK,
            )
            .await?;

        info!(
            condition_id = %round.condition_id,
            market_slug = %round.market_slug,
            leg = leg_label(leg),
            limit_price = %limit_price,
            size = %estimated_size,
            estimated_quote = %estimated_quote,
            slippage_ticks,
            realtime_best_ask = ?live_best_ask,
            "submitting reactive taker buy"
        );

        post_single_order(gateway, payload).await
    }

    async fn process_realtime_order_updates(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut HashMap<String, ManagedRound>,
        realtime_feed: &mut ReactiveRealtimeFeed,
    ) -> Result<()> {
        if !strategy.uses_reactive_taker_flip() || self.settings.app.dry_run {
            return Ok(());
        }

        for update in realtime_feed.drain_order_updates() {
            self.apply_realtime_order_update(strategy, managed, realtime_feed, update)
                .await?;
        }

        Ok(())
    }

    async fn apply_realtime_order_update(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut HashMap<String, ManagedRound>,
        realtime_feed: &ReactiveRealtimeFeed,
        update: RealtimeOrderUpdate,
    ) -> Result<()> {
        let Some(round) = managed.get_mut(&update.market) else {
            return Ok(());
        };

        if round.cancel_processed || !round.orders_submitted {
            return Ok(());
        }

        let Some(order) = round
            .orders
            .iter_mut()
            .find(|order| order.order_id.as_deref() == Some(update.order_id.as_str()))
        else {
            return Ok(());
        };

        let previous_matched_size = order.last_matched_size;
        order.observe_realtime_status(update.size_matched, &update.status);

        if order.last_matched_size > previous_matched_size {
            info!(
                condition_id = %round.round.condition_id,
                market_slug = %round.round.market_slug,
                leg = leg_label(order.leg),
                side = side_label(order.side),
                maker_price = %order.price,
                order_id = %update.order_id,
                matched_size = %order.last_matched_size,
                fully_matched = order.is_fully_matched(),
                status = %update.status,
                "observed maker order match progress"
            );
        }

        if order.is_fully_matched() && !order.trigger_attempted {
            order.mark_fully_matched();
            let trigger = ReactiveTrigger {
                filled_leg: order.leg,
                maker_size: order.size,
            };
            let opposite_asset_id =
                token_id_for_leg(&round.round, opposite_leg(trigger.filled_leg));
            let realtime_best_quote = realtime_feed.best_quote(opposite_asset_id).await;
            self.execute_reactive_taker_flip(strategy, round, trigger, realtime_best_quote)
                .await?;
        }

        Ok(())
    }

    async fn submit_reactive_taker_sell(
        &self,
        gateway: &ExecutionGateway,
        round: &RoundDescriptor,
        leg: LegSide,
        size: Decimal,
        deadline: DateTime<Utc>,
    ) -> Result<Option<PostOrderResponse>> {
        let Some(sell_size) = self
            .wait_for_reactive_sell_balance(gateway, round, leg, size, deadline)
            .await?
        else {
            return Ok(None);
        };

        let token_id = token_id_for_leg(round, leg);
        let Some(book) = gateway.fetch_order_book(token_id).await? else {
            return Ok(None);
        };
        let Some(estimate) = book.estimate_sell_for_size(sell_size) else {
            return Ok(None);
        };

        let payload = gateway
            .build_leg_sell_order(round, leg, estimate.price, sell_size, OrderType::FAK, false)
            .await?;

        info!(
            condition_id = %round.condition_id,
            market_slug = %round.market_slug,
            leg = leg_label(leg),
            limit_price = %estimate.price,
            size = %sell_size,
            estimated_quote = %estimate.quote,
            "submitting reactive taker sell"
        );

        let response = post_single_order(gateway, payload).await?;
        if let Some(response) = response.as_ref() {
            if response.is_insufficient_balance() && Utc::now() < deadline {
                warn!(
                    condition_id = %round.condition_id,
                    market_slug = %round.market_slug,
                    leg = leg_label(leg),
                    size = %sell_size,
                    deadline = %deadline,
                    "reactive taker sell hit balance lag despite observed holdings"
                );
            }
        }

        Ok(response)
    }

    async fn wait_for_reactive_sell_balance(
        &self,
        gateway: &ExecutionGateway,
        round: &RoundDescriptor,
        leg: LegSide,
        required_size: Decimal,
        deadline: DateTime<Utc>,
    ) -> Result<Option<Decimal>> {
        loop {
            let (yes_balance, no_balance) = gateway.round_token_balances(round).await?;
            let last_balance = match leg {
                LegSide::Yes => yes_balance,
                LegSide::No => no_balance,
            }
            .trunc_with_scale(2);

            if last_balance >= required_size {
                return Ok(Some(required_size));
            }

            if Utc::now() >= deadline {
                warn!(
                    condition_id = %round.condition_id,
                    market_slug = %round.market_slug,
                    leg = leg_label(leg),
                    observed_balance = %last_balance,
                    required_size = %required_size,
                    deadline = %deadline,
                    "reactive taker sell skipped because filled balance did not settle before deadline"
                );
                return Ok(None);
            }

            tokio::time::sleep(Duration::from_millis(REACTIVE_SELL_BALANCE_POLL_MS)).await;
        }
    }

    async fn cancel_resting_orders(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        if strategy.uses_reactive_taker_flip() {
            self.monitor_reactive_fills(strategy, managed).await?;
        }

        if self.settings.app.dry_run {
            for order in &mut managed.orders {
                order.mark_canceled();
            }
            managed.cancel_processed = true;
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                "dry-run cancel completed"
            );
            return Ok(());
        }

        if managed.pending_submission.is_some() && managed.orders.is_empty() {
            warn!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                "submit timeout recovery did not recover any open-order ids before cancel window; manual audit may be required"
            );
        }

        let live_ids = managed
            .orders
            .iter()
            .filter(|order| order.needs_cancel())
            .filter_map(|order| order.order_id.as_deref())
            .collect::<Vec<_>>();

        if live_ids.is_empty() {
            managed.cancel_processed = true;
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                "no live orders remained to cancel before open"
            );
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        let response = gateway.cancel_orders(&live_ids).await?;
        for order in &mut managed.orders {
            let Some(order_id) = order.order_id.as_deref() else {
                continue;
            };

            if response
                .canceled
                .iter()
                .any(|canceled| canceled == order_id)
            {
                order.mark_canceled();
                continue;
            }

            if let Some(reason) = response.not_canceled.get(order_id) {
                order.mark_cancel_failed(reason.clone());
            }
        }

        managed.cancel_processed = true;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            canceled = response.canceled.len(),
            not_canceled = response.not_canceled.len(),
            "processed pre-open cancel for resting orders"
        );

        Ok(())
    }

    fn note_open_post_first_match(&self, managed: &mut ManagedRound, filled_leg: LegSide) {
        let Some(state) = managed.open_post_state.as_mut() else {
            return;
        };
        if state.triggered_leg.is_some() {
            return;
        }

        state.triggered_leg = Some(filled_leg);
        self.maybe_spawn_redeem_watch(managed, "first_match");
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            filled_leg = leg_label(filled_leg),
            "first maker match observed; keeping the opposite leg live until cancel window"
        );
    }

    async fn finalize_round(
        &self,
        strategy: &StrategySnapshot,
        managed: &mut ManagedRound,
    ) -> Result<()> {
        if self.settings.app.dry_run {
            managed.completed = true;
            info!(
                condition_id = %managed.round.condition_id,
                market_slug = %managed.round.market_slug,
                "dry-run round finalized"
            );
            return Ok(());
        }

        let gateway = self.live_gateway()?;
        let (mut yes_balance, mut no_balance) =
            gateway.round_token_balances(&managed.round).await?;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            yes_balance = %yes_balance,
            no_balance = %no_balance,
            "token balances after cancel"
        );

        if matches!(strategy.mode, StrategyMode::PreSplitDualSell) {
            let mergeable = yes_balance.min(no_balance);
            if mergeable > rust_decimal::Decimal::ZERO {
                let receipt = gateway.merge_position(&managed.round, mergeable).await?;
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    amount_usdc = %receipt.amount_usdc,
                    tx_hash = %receipt.tx_hash,
                    "merged cancel-time full-set balance back to collateral"
                );
                let balances = gateway.round_token_balances(&managed.round).await?;
                yes_balance = balances.0;
                no_balance = balances.1;
                info!(
                    condition_id = %managed.round.condition_id,
                    market_slug = %managed.round.market_slug,
                    yes_balance = %yes_balance,
                    no_balance = %no_balance,
                    "token balances after merge"
                );
            }
        }

        if yes_balance > rust_decimal::Decimal::ZERO || no_balance > rust_decimal::Decimal::ZERO {
            self.maybe_spawn_redeem_watch(managed, "finalize_round");
        }

        managed.completed = true;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            "managed round completed and removed from active future window"
        );
        Ok(())
    }

    fn spawn_redeem_task(&self, round: RoundDescriptor) {
        let Some(gateway) = self.execution_gateway.clone() else {
            return;
        };

        let poll_interval =
            Duration::from_millis(self.settings.execution.relayer_poll_interval_ms.max(250));
        tokio::spawn(async move {
            let wait_until = round.settles_at + ChronoDuration::seconds(1);
            let delay = (wait_until - Utc::now())
                .to_std()
                .unwrap_or_else(|_| Duration::from_secs(0));
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            for _ in 0..REDEEM_RETRY_LIMIT {
                match gateway.redeem_positions_if_resolved(&round).await {
                    Ok(Some(receipt)) => {
                        info!(
                            condition_id = %round.condition_id,
                            market_slug = %round.market_slug,
                            tx_hash = %receipt.tx_hash,
                            "redeemed settled round balances"
                        );
                        return;
                    }
                    Ok(None) => match gateway.round_token_balances(&round).await {
                        Ok((yes_balance, no_balance)) => {
                            if yes_balance <= rust_decimal::Decimal::ZERO
                                && no_balance <= rust_decimal::Decimal::ZERO
                            {
                                info!(
                                    condition_id = %round.condition_id,
                                    market_slug = %round.market_slug,
                                    "redeem watcher exited because no token balance remained"
                                );
                                return;
                            }
                        }
                        Err(error) => {
                            warn!(
                                ?error,
                                condition_id = %round.condition_id,
                                market_slug = %round.market_slug,
                                "redeem watcher failed to fetch token balances"
                            );
                        }
                    },
                    Err(error) => {
                        warn!(
                            ?error,
                            condition_id = %round.condition_id,
                            market_slug = %round.market_slug,
                            "redeem watcher iteration failed"
                        );
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }

            warn!(
                condition_id = %round.condition_id,
                market_slug = %round.market_slug,
                "redeem watcher gave up after max retries; background redeem scan can still catch it later"
            );
        });
    }

    fn maybe_spawn_redeem_watch(&self, managed: &mut ManagedRound, reason: &str) {
        if managed.redeem_task_spawned {
            return;
        }

        managed.redeem_task_spawned = true;
        info!(
            condition_id = %managed.round.condition_id,
            market_slug = %managed.round.market_slug,
            reason,
            settles_at = %managed.round.settles_at,
            "registered auto-redeem watcher for round"
        );
        self.spawn_redeem_task(managed.round.clone());
    }

    fn live_gateway(&self) -> Result<&ExecutionGateway> {
        self.execution_gateway
            .as_deref()
            .context("execution gateway is unavailable; configure account env vars or use dry_run")
    }

    async fn notify_error(&self, body: String) {
        if !self.settings.telegram.send_errors {
            return;
        }

        if let Err(error) = self.notifier.send("error", body).await {
            warn!(?error, "failed to send error notification");
        }
    }
}

#[derive(Debug, Clone)]
struct ManagedRound {
    round: RoundDescriptor,
    split_attempted: bool,
    split_confirmed: bool,
    split_error: Option<String>,
    submission_attempted: bool,
    orders_submitted: bool,
    orders: Vec<ManagedOrder>,
    pending_submission: Option<PendingSubmissionRecovery>,
    paper_state: Option<PaperRoundState>,
    open_post_state: Option<OpenPostRoundState>,
    redeem_task_spawned: bool,
    cancel_processed: bool,
    completed: bool,
}

impl ManagedRound {
    fn new(round: RoundDescriptor) -> Self {
        Self {
            round,
            split_attempted: false,
            split_confirmed: false,
            split_error: None,
            submission_attempted: false,
            orders_submitted: false,
            orders: Vec::new(),
            pending_submission: None,
            paper_state: None,
            open_post_state: Some(OpenPostRoundState::new()),
            redeem_task_spawned: false,
            cancel_processed: false,
            completed: false,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingSubmissionRecovery {
    plans: Vec<OrderPlan>,
    last_check_at: Option<DateTime<Utc>>,
}

impl PendingSubmissionRecovery {
    fn new(plans: Vec<OrderPlan>) -> Self {
        Self {
            plans,
            last_check_at: None,
        }
    }

    fn should_check_now(&self) -> bool {
        self.last_check_at.map_or(true, |last_check_at| {
            Utc::now()
                .signed_duration_since(last_check_at)
                .num_milliseconds()
                >= SUBMISSION_RECOVERY_POLL_MS
        })
    }
}

#[derive(Debug, Clone)]
struct OpenPostRoundState {
    triggered_leg: Option<LegSide>,
}

impl OpenPostRoundState {
    fn new() -> Self {
        Self {
            triggered_leg: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedOrderStatus {
    Live,
    PartiallyMatched,
    FullyMatched,
    Rejected,
    Canceled,
    CancelFailed,
    Simulated,
}

#[derive(Debug, Clone)]
struct ManagedOrder {
    leg: LegSide,
    side: OrderSide,
    price: Decimal,
    size: Decimal,
    order_id: Option<String>,
    status: ManagedOrderStatus,
    exchange_status: String,
    error_message: Option<String>,
    last_matched_size: Decimal,
    trigger_attempted: bool,
}

impl ManagedOrder {
    fn live(plan: OrderPlan, response: PostOrderResponse) -> Self {
        let error_message = response.error_message().map(str::to_owned);
        Self {
            leg: plan.leg,
            side: plan.side,
            price: plan.price,
            size: plan.size,
            order_id: Some(response.order_id),
            status: ManagedOrderStatus::Live,
            exchange_status: response.status,
            error_message,
            last_matched_size: Decimal::ZERO,
            trigger_attempted: false,
        }
    }

    fn from_open_order(plan: OrderPlan, open_order: &OpenOrderResponse) -> Self {
        let mut managed = Self {
            leg: plan.leg,
            side: plan.side,
            price: plan.price,
            size: plan.size,
            order_id: Some(open_order.id.clone()),
            status: ManagedOrderStatus::Live,
            exchange_status: open_order.status.clone(),
            error_message: None,
            last_matched_size: Decimal::ZERO,
            trigger_attempted: false,
        };

        if let Some(size_matched) = open_order.size_matched {
            managed.observe_realtime_status(size_matched, &open_order.status);
        }

        managed
    }

    fn rejected(plan: OrderPlan, response: PostOrderResponse) -> Self {
        let error_message = response.error_message().map(str::to_owned);
        Self {
            leg: plan.leg,
            side: plan.side,
            price: plan.price,
            size: plan.size,
            order_id: None,
            status: ManagedOrderStatus::Rejected,
            exchange_status: response.status,
            error_message,
            last_matched_size: Decimal::ZERO,
            trigger_attempted: false,
        }
    }

    fn simulated(plan: OrderPlan) -> Self {
        Self {
            leg: plan.leg,
            side: plan.side,
            price: plan.price,
            size: plan.size,
            order_id: None,
            status: ManagedOrderStatus::Simulated,
            exchange_status: "simulated".to_owned(),
            error_message: None,
            last_matched_size: Decimal::ZERO,
            trigger_attempted: false,
        }
    }

    fn needs_status_poll(&self) -> bool {
        matches!(
            self.status,
            ManagedOrderStatus::Live | ManagedOrderStatus::PartiallyMatched
        ) && self.order_id.is_some()
    }

    fn needs_cancel(&self) -> bool {
        matches!(
            self.status,
            ManagedOrderStatus::Live | ManagedOrderStatus::PartiallyMatched
        ) && self.order_id.is_some()
    }

    fn observe_status(&mut self, status: &OrderStatusResponse) {
        self.exchange_status = status.status.clone();
        if let Some(size_matched) = status.size_matched {
            if size_matched > self.last_matched_size {
                self.last_matched_size = size_matched;
            }
        }

        if status.is_fully_matched() {
            self.status = ManagedOrderStatus::FullyMatched;
        } else if status.has_any_match() {
            self.status = ManagedOrderStatus::PartiallyMatched;
        }
    }

    fn observe_realtime_status(&mut self, size_matched: Decimal, status: &str) {
        self.exchange_status = status.to_owned();
        if size_matched > self.last_matched_size {
            self.last_matched_size = size_matched.min(self.size);
        }

        if self.last_matched_size >= self.size || status_implies_filled(status) {
            self.status = ManagedOrderStatus::FullyMatched;
        } else if self.last_matched_size > Decimal::ZERO {
            self.status = ManagedOrderStatus::PartiallyMatched;
        }
    }

    fn is_fully_matched(&self) -> bool {
        self.status == ManagedOrderStatus::FullyMatched || self.last_matched_size >= self.size
    }

    fn mark_fully_matched(&mut self) {
        self.status = ManagedOrderStatus::FullyMatched;
        self.trigger_attempted = true;
    }

    fn mark_canceled(&mut self) {
        self.status = ManagedOrderStatus::Canceled;
        self.exchange_status = "canceled".to_owned();
    }

    fn mark_cancel_failed(&mut self, reason: String) {
        self.status = ManagedOrderStatus::CancelFailed;
        self.error_message = Some(reason);
    }
}

#[derive(Debug, Clone, Copy)]
struct ReactiveTrigger {
    filled_leg: LegSide,
    maker_size: Decimal,
}

fn plan_purpose(mode: StrategyMode, plan: &OrderPlan) -> &'static str {
    match (mode, plan.leg, plan.side) {
        (StrategyMode::PreSplitDualSell, LegSide::Yes, OrderSide::Sell) => "yes_exit",
        (StrategyMode::PreSplitDualSell, LegSide::No, OrderSide::Sell) => "no_exit",
        (StrategyMode::PreOpenDualBuy, LegSide::Yes, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyTakerFlip, LegSide::Yes, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyPaperTpsl, LegSide::Yes, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyPaperLimitExit, LegSide::Yes, OrderSide::Buy)
        | (StrategyMode::OpenPostDualBuyPriceGuard, LegSide::Yes, OrderSide::Buy) => "yes_entry",
        (StrategyMode::PreOpenDualBuy, LegSide::No, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyTakerFlip, LegSide::No, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyPaperTpsl, LegSide::No, OrderSide::Buy)
        | (StrategyMode::PreOpenDualBuyPaperLimitExit, LegSide::No, OrderSide::Buy)
        | (StrategyMode::OpenPostDualBuyPriceGuard, LegSide::No, OrderSide::Buy) => "no_entry",
        (StrategyMode::PreSplitDualSell, LegSide::Yes, OrderSide::Buy) => "yes_buy",
        (StrategyMode::PreSplitDualSell, LegSide::No, OrderSide::Buy) => "no_buy",
        (StrategyMode::PreOpenDualBuy, LegSide::Yes, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyTakerFlip, LegSide::Yes, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyPaperTpsl, LegSide::Yes, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyPaperLimitExit, LegSide::Yes, OrderSide::Sell)
        | (StrategyMode::OpenPostDualBuyPriceGuard, LegSide::Yes, OrderSide::Sell) => "yes_sell",
        (StrategyMode::PreOpenDualBuy, LegSide::No, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyTakerFlip, LegSide::No, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyPaperTpsl, LegSide::No, OrderSide::Sell)
        | (StrategyMode::PreOpenDualBuyPaperLimitExit, LegSide::No, OrderSide::Sell)
        | (StrategyMode::OpenPostDualBuyPriceGuard, LegSide::No, OrderSide::Sell) => "no_sell",
    }
}

fn opposite_leg(leg: LegSide) -> LegSide {
    match leg {
        LegSide::Yes => LegSide::No,
        LegSide::No => LegSide::Yes,
    }
}

fn token_id_for_leg<'a>(round: &'a RoundDescriptor, leg: LegSide) -> &'a str {
    match leg {
        LegSide::Yes => &round.yes_token_id,
        LegSide::No => &round.no_token_id,
    }
}

async fn post_single_order(
    gateway: &ExecutionGateway,
    payload: crate::execution::RawSignedOrder,
) -> Result<Option<PostOrderResponse>> {
    let mut responses = gateway.post_orders(&[payload]).await?;
    Ok(responses.pop())
}

fn log_reactive_taker_response(
    round: &RoundDescriptor,
    purpose: &str,
    leg: LegSide,
    side: OrderSide,
    response: &PostOrderResponse,
) {
    info!(
        condition_id = %round.condition_id,
        market_slug = %round.market_slug,
        purpose,
        leg = leg_label(leg),
        side = side_label(side),
        success = response.success,
        order_id = %response.order_id,
        status = %response.status,
        error = response.error_message().unwrap_or(""),
        trade_ids = ?response.trade_ids,
        tx_hashes = ?response.transactions_hashes,
        "reactive taker order response"
    );
}

fn round_quote_start_at(round: &RoundDescriptor, strategy: &StrategySnapshot) -> DateTime<Utc> {
    if strategy.uses_open_post_price_guard() {
        round.opens_at + ChronoDuration::seconds(strategy.quote_start_after_open_secs as i64)
    } else {
        round.opens_at - ChronoDuration::seconds(strategy.quote_start_before_open_secs as i64)
    }
}

fn round_cancel_at(round: &RoundDescriptor, strategy: &StrategySnapshot) -> DateTime<Utc> {
    if strategy.uses_open_post_price_guard() {
        round.opens_at + ChronoDuration::seconds(strategy.quote_cancel_after_open_secs as i64)
    } else {
        round.opens_at - ChronoDuration::milliseconds(strategy.quote_cancel_before_open_ms as i64)
    }
}

fn paper_force_taker_exit_at(
    round: &RoundDescriptor,
    strategy: &StrategySnapshot,
) -> DateTime<Utc> {
    round.settles_at
        - ChronoDuration::seconds(strategy.paper_force_taker_exit_before_settle_secs as i64)
}

fn round_split_at(round: &RoundDescriptor, strategy: &StrategySnapshot) -> DateTime<Utc> {
    round.opens_at - ChronoDuration::seconds(strategy.pre_split_before_open_secs as i64)
}

fn apply_reactive_buy_slippage(price: Decimal, tick_size: Decimal, slippage_ticks: u32) -> Decimal {
    let buffered = price + tick_size * Decimal::from(slippage_ticks);
    buffered
        .min(Decimal::ONE - tick_size)
        .trunc_with_scale(tick_size.scale())
}

fn estimate_market_buy_size_for_quote(
    quote_budget: Decimal,
    limit_price: Decimal,
    price_scale: u32,
) -> Decimal {
    if quote_budget <= Decimal::ZERO || limit_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    (quote_budget.trunc_with_scale(price_scale) / limit_price).trunc_with_scale(price_scale + 2)
}

fn next_paper_wait_duration(
    strategy: &StrategySnapshot,
    managed: &HashMap<String, ManagedRound>,
    next_discovery_at: DateTime<Utc>,
) -> Duration {
    let now = Utc::now();
    let mut next_wake = next_discovery_at;

    for managed_round in managed.values() {
        if managed_round.completed {
            continue;
        }

        if !managed_round.orders_submitted {
            next_wake = next_wake.min(round_quote_start_at(&managed_round.round, strategy));
        }

        if !paper_pre_open_cancel_processed(managed_round) {
            next_wake = next_wake.min(round_cancel_at(&managed_round.round, strategy));
        }

        if let Some(paper_state) = managed_round.paper_state.as_ref() {
            if strategy.uses_paper_limit_exit() {
                if let Some(position) = paper_state
                    .limit_exit_position
                    .as_ref()
                    .filter(|position| position.has_open_position())
                {
                    next_wake = next_wake.min(position.force_taker_exit_at);
                    next_wake = next_wake.min(managed_round.round.settles_at);
                    continue;
                }
            }

            if paper_state.has_triggered() && !paper_state.summaries_recorded {
                next_wake = next_wake.min(managed_round.round.settles_at);
            } else if paper_state.pre_open_cancel_processed && !managed_round.completed {
                next_wake = next_wake.min(managed_round.round.settles_at);
            }
        }
    }

    if next_wake <= now {
        Duration::ZERO
    } else {
        (next_wake - now)
            .to_std()
            .unwrap_or_else(|_| Duration::from_millis(MAIN_LOOP_TICK_MS))
    }
}

fn paper_entry_eval_requested(
    managed: &ManagedRound,
    market_changed_assets: &HashSet<String>,
) -> bool {
    let Some(paper_state) = managed.paper_state.as_ref() else {
        return false;
    };

    if paper_state.pending_entry_eval {
        return true;
    }

    paper_state
        .maker_orders
        .iter()
        .filter(|order| order.status == PaperMakerOrderStatus::Resting)
        .any(|order| market_changed_assets.contains(token_id_for_leg(&managed.round, order.leg)))
}

fn paper_exit_eval_requested(
    strategy: &StrategySnapshot,
    managed: &ManagedRound,
    market_changed_assets: &HashSet<String>,
    now: DateTime<Utc>,
) -> bool {
    let Some(paper_state) = managed.paper_state.as_ref() else {
        return false;
    };

    if paper_state.pending_exit_eval {
        return true;
    }

    if strategy.uses_paper_limit_exit()
        && paper_state
            .limit_exit_position
            .as_ref()
            .map(|position| position.should_force_taker_exit(now))
            .unwrap_or(false)
    {
        return true;
    }

    let Some(active_leg) = paper_state.active_exit_leg() else {
        return false;
    };

    market_changed_assets.contains(token_id_for_leg(&managed.round, active_leg))
        || market_changed_assets
            .contains(token_id_for_leg(&managed.round, opposite_leg(active_leg)))
}

fn paper_pre_open_cancel_processed(managed: &ManagedRound) -> bool {
    managed
        .paper_state
        .as_ref()
        .map(|paper_state| paper_state.pre_open_cancel_processed)
        .unwrap_or(false)
}

fn should_monitor_paper_round(managed: &ManagedRound) -> bool {
    if !managed.orders_submitted || managed.completed {
        return false;
    }

    !paper_pre_open_cancel_processed(managed)
        || managed
            .paper_state
            .as_ref()
            .map(|paper_state| paper_state.has_triggered() && !paper_state.summaries_recorded)
            .unwrap_or(false)
}

fn status_implies_filled(status: &str) -> bool {
    let normalized = status.trim().to_ascii_lowercase();
    normalized.contains("matched") || normalized.contains("filled")
}

fn order_side_matches_open_order(expected: OrderSide, actual: &str) -> bool {
    let normalized = actual.trim().to_ascii_lowercase();
    match expected {
        OrderSide::Buy => normalized == "buy",
        OrderSide::Sell => normalized == "sell",
    }
}

fn is_request_timeout_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|source| source.downcast_ref::<reqwest::Error>())
        .any(reqwest::Error::is_timeout)
        || error.chain().any(|source| {
            source
                .to_string()
                .to_ascii_lowercase()
                .contains("timed out")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use rust_decimal::Decimal;

    fn sample_round() -> RoundDescriptor {
        let opens_at = Utc::now() + ChronoDuration::minutes(1);
        RoundDescriptor {
            market_id: "market-id".to_owned(),
            condition_id: "condition-id".to_owned(),
            market_slug: "btc-updown-5m-123".to_owned(),
            question: "sample".to_owned(),
            yes_token_id: "yes-token".to_owned(),
            no_token_id: "no-token".to_owned(),
            opens_at,
            settles_at: opens_at + ChronoDuration::minutes(5),
        }
    }

    fn sample_strategy(mode: StrategyMode) -> StrategySnapshot {
        StrategySnapshot {
            mode,
            round_interval_secs: 300,
            window_size_rounds: 2,
            quote_start_before_open_secs: 180,
            quote_cancel_before_open_ms: 1_000,
            pre_split_before_open_secs: 240,
            quote_start_after_open_secs: 0,
            quote_cancel_after_open_secs: 120,
            order_size: Decimal::new(5, 0),
            yes_price: Decimal::new(46, 2),
            no_price: Decimal::new(46, 2),
            open_price_observation_max_deviation: Decimal::new(80, 0),
            open_price_max_deviation: Decimal::new(50, 0),
            reactive_opposite_taker_usdc: Decimal::ZERO,
            reactive_buy_slippage_ticks: 2,
            paper_extra_shares: Decimal::new(10, 0),
            paper_stop_loss_price: Decimal::new(50, 2),
            paper_take_profit_percents: vec![Decimal::new(5, 2)],
            paper_limit_exit_price: Decimal::new(50, 2),
            paper_force_taker_exit_before_settle_secs: 120,
            paper_fee_rebate_rate: Decimal::ZERO,
            paper_output_dir: "paper-output".to_owned(),
        }
    }

    #[test]
    fn paper_round_keeps_monitoring_after_pre_open_cancel_when_triggered() {
        let mut managed = ManagedRound::new(sample_round());
        managed.orders_submitted = true;
        managed.paper_state = Some(PaperRoundState::new());

        let paper_state = managed.paper_state.as_mut().expect("paper state");
        paper_state.pre_open_cancel_processed = true;
        paper_state.triggered_leg = Some(LegSide::Yes);

        assert!(should_monitor_paper_round(&managed));
    }

    #[test]
    fn paper_round_stops_monitoring_after_pre_open_cancel_without_trigger() {
        let mut managed = ManagedRound::new(sample_round());
        managed.orders_submitted = true;
        managed.paper_state = Some(PaperRoundState::new());

        let paper_state = managed.paper_state.as_mut().expect("paper state");
        paper_state.pre_open_cancel_processed = true;

        assert!(!should_monitor_paper_round(&managed));
    }

    #[test]
    fn paper_limit_exit_eval_triggers_at_force_taker_deadline() {
        let strategy = sample_strategy(StrategyMode::PreOpenDualBuyPaperLimitExit);
        let mut managed = ManagedRound::new(sample_round());
        managed.paper_state = Some(PaperRoundState::new());

        let force_at = paper_force_taker_exit_at(&managed.round, &strategy);
        managed
            .paper_state
            .as_mut()
            .expect("paper state")
            .limit_exit_position = Some(PaperLimitExitPosition::new(
            "limit_50.00c_tminus120s".to_owned(),
            LegSide::Yes,
            Decimal::new(46, 2),
            Decimal::new(5, 0),
            strategy.paper_limit_exit_price,
            force_at,
        ));

        assert!(paper_exit_eval_requested(
            &strategy,
            &managed,
            &HashSet::new(),
            force_at,
        ));
    }

    #[test]
    fn next_paper_wait_duration_wakes_for_limit_exit_force_sell_deadline() {
        let strategy = sample_strategy(StrategyMode::PreOpenDualBuyPaperLimitExit);
        let mut managed_round = ManagedRound::new(sample_round());
        managed_round.orders_submitted = true;
        managed_round.paper_state = Some(PaperRoundState::new());

        let force_at = paper_force_taker_exit_at(&managed_round.round, &strategy);
        let paper_state = managed_round.paper_state.as_mut().expect("paper state");
        paper_state.pre_open_cancel_processed = true;
        paper_state.limit_exit_position = Some(PaperLimitExitPosition::new(
            "limit_50.00c_tminus120s".to_owned(),
            LegSide::Yes,
            Decimal::new(46, 2),
            Decimal::new(5, 0),
            strategy.paper_limit_exit_price,
            force_at,
        ));

        let mut managed = HashMap::new();
        managed.insert(managed_round.round.condition_id.clone(), managed_round);

        let wait = next_paper_wait_duration(
            &strategy,
            &managed,
            Utc::now() + ChronoDuration::minutes(30),
        );
        let wake_at = Utc::now() + ChronoDuration::from_std(wait).expect("chrono duration");
        let delta_ms = (wake_at - force_at).num_milliseconds().abs();

        assert!(
            delta_ms <= 1_000,
            "expected wake time near force exit deadline, delta_ms={delta_ms}"
        );
    }
}
