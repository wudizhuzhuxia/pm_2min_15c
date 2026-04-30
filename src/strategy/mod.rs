use anyhow::{Result, bail};
use rust_decimal::Decimal;

use crate::config::{StrategyConfig, StrategyMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LegSide {
    Yes,
    No,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub mode: StrategyMode,
    pub round_interval_secs: u64,
    pub window_size_rounds: usize,
    pub quote_start_before_open_secs: u64,
    pub quote_cancel_before_open_ms: u64,
    pub pre_split_before_open_secs: u64,
    pub quote_start_after_open_secs: u64,
    pub quote_cancel_after_open_secs: u64,
    pub order_size: Decimal,
    pub yes_price: Decimal,
    pub no_price: Decimal,
    pub open_price_observation_max_deviation: Decimal,
    pub open_price_max_deviation: Decimal,
    pub reactive_opposite_taker_usdc: Decimal,
    pub reactive_buy_slippage_ticks: u32,
    pub paper_extra_shares: Decimal,
    pub paper_stop_loss_price: Decimal,
    pub paper_take_profit_percents: Vec<Decimal>,
    pub paper_limit_exit_price: Decimal,
    pub paper_force_taker_exit_before_settle_secs: u64,
    pub paper_fee_rebate_rate: Decimal,
    pub paper_output_dir: String,
}

impl StrategySnapshot {
    pub fn from_config(config: &StrategyConfig) -> Result<Self> {
        let snapshot = Self {
            mode: config.mode,
            round_interval_secs: config.round_interval_secs,
            window_size_rounds: config.window_size_rounds,
            quote_start_before_open_secs: config.quote_start_before_open_secs,
            quote_cancel_before_open_ms: config.quote_cancel_before_open_ms,
            pre_split_before_open_secs: config.pre_split_before_open_secs,
            quote_start_after_open_secs: config.quote_start_after_open_secs,
            quote_cancel_after_open_secs: config.quote_cancel_after_open_secs,
            order_size: decimal_from_f64(config.order_size)?,
            yes_price: decimal_from_f64(config.yes_price)?,
            no_price: decimal_from_f64(config.no_price)?,
            open_price_observation_max_deviation: decimal_from_f64(
                config.open_price_observation_max_deviation,
            )?,
            open_price_max_deviation: decimal_from_f64(config.open_price_max_deviation)?,
            reactive_opposite_taker_usdc: decimal_from_f64(config.reactive_opposite_taker_usdc)?,
            reactive_buy_slippage_ticks: config.reactive_buy_slippage_ticks,
            paper_extra_shares: decimal_from_f64(config.paper_extra_shares)?,
            paper_stop_loss_price: decimal_from_f64(config.paper_stop_loss_price)?,
            paper_take_profit_percents: config
                .paper_take_profit_percents
                .iter()
                .copied()
                .map(decimal_percent_to_ratio)
                .collect::<Result<Vec<_>>>()?,
            paper_limit_exit_price: decimal_from_f64(config.paper_limit_exit_price)?,
            paper_force_taker_exit_before_settle_secs: config
                .paper_force_taker_exit_before_settle_secs,
            paper_fee_rebate_rate: decimal_from_f64(config.paper_fee_rebate_rate)?,
            paper_output_dir: config.paper_output_dir.clone(),
        };

        if snapshot.order_size <= Decimal::ZERO {
            bail!("strategy.order_size must be greater than zero");
        }

        Ok(snapshot)
    }

    pub fn split_amount(&self) -> Decimal {
        self.order_size
    }

    pub fn uses_reactive_taker_flip(&self) -> bool {
        self.mode == StrategyMode::PreOpenDualBuyTakerFlip
    }

    pub fn uses_open_post_price_guard(&self) -> bool {
        self.mode == StrategyMode::OpenPostDualBuyPriceGuard
    }

    pub fn uses_paper_tpsl(&self) -> bool {
        self.mode == StrategyMode::PreOpenDualBuyPaperTpsl
    }

    pub fn uses_paper_limit_exit(&self) -> bool {
        self.mode == StrategyMode::PreOpenDualBuyPaperLimitExit
    }

    pub fn uses_paper_trading(&self) -> bool {
        self.uses_paper_tpsl() || self.uses_paper_limit_exit()
    }

    pub fn requires_realtime_quotes(&self) -> bool {
        self.uses_reactive_taker_flip() || self.uses_paper_trading()
    }

    pub fn order_plans(&self) -> [OrderPlan; 2] {
        let side = match self.mode {
            StrategyMode::PreSplitDualSell => OrderSide::Sell,
            StrategyMode::PreOpenDualBuy
            | StrategyMode::PreOpenDualBuyTakerFlip
            | StrategyMode::PreOpenDualBuyPaperTpsl
            | StrategyMode::PreOpenDualBuyPaperLimitExit
            | StrategyMode::OpenPostDualBuyPriceGuard => OrderSide::Buy,
        };

        [
            OrderPlan {
                leg: LegSide::Yes,
                side,
                price: self.yes_price,
                size: self.order_size,
                post_only: true,
            },
            OrderPlan {
                leg: LegSide::No,
                side,
                price: self.no_price,
                size: self.order_size,
                post_only: true,
            },
        ]
    }
}

#[derive(Debug, Clone)]
pub struct OrderPlan {
    pub leg: LegSide,
    pub side: OrderSide,
    pub price: Decimal,
    pub size: Decimal,
    pub post_only: bool,
}

pub fn leg_label(leg: LegSide) -> &'static str {
    match leg {
        LegSide::Yes => "yes",
        LegSide::No => "no",
    }
}

pub fn side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    Decimal::from_str_exact(&value.to_string())
        .map_err(|error| anyhow::anyhow!("failed to parse decimal value {value}: {error}"))
}

fn decimal_percent_to_ratio(value: f64) -> Result<Decimal> {
    Ok(decimal_from_f64(value)? / Decimal::from(100u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config(mode: StrategyMode) -> StrategyConfig {
        StrategyConfig {
            mode,
            round_interval_secs: 300,
            window_size_rounds: 2,
            quote_start_before_open_secs: 180,
            quote_cancel_before_open_ms: 1_000,
            pre_split_before_open_secs: 240,
            quote_start_after_open_secs: 0,
            quote_cancel_after_open_secs: 120,
            order_size: 5.0,
            yes_price: 0.54,
            no_price: 0.54,
            open_price_observation_max_deviation: 80.0,
            open_price_max_deviation: 50.0,
            reactive_opposite_taker_usdc: 2.0,
            reactive_buy_slippage_ticks: 2,
            paper_extra_shares: 10.0,
            paper_stop_loss_price: 0.50,
            paper_take_profit_percents: vec![5.0, 10.0, 15.0, 20.0],
            paper_limit_exit_price: 0.50,
            paper_force_taker_exit_before_settle_secs: 120,
            paper_fee_rebate_rate: 0.30,
            paper_output_dir: "logs/paper".to_owned(),
        }
    }

    #[test]
    fn split_sell_mode_builds_sell_orders() {
        let snapshot =
            StrategySnapshot::from_config(&sample_config(StrategyMode::PreSplitDualSell))
                .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Sell));
        assert!(matches!(orders[1].side, OrderSide::Sell));
    }

    #[test]
    fn direct_buy_mode_builds_buy_orders() {
        let snapshot = StrategySnapshot::from_config(&sample_config(StrategyMode::PreOpenDualBuy))
            .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Buy));
        assert!(matches!(orders[1].side, OrderSide::Buy));
    }

    #[test]
    fn reactive_buy_mode_also_builds_buy_orders() {
        let snapshot =
            StrategySnapshot::from_config(&sample_config(StrategyMode::PreOpenDualBuyTakerFlip))
                .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Buy));
        assert!(matches!(orders[1].side, OrderSide::Buy));
        assert!(snapshot.uses_reactive_taker_flip());
    }

    #[test]
    fn paper_mode_also_builds_buy_orders() {
        let snapshot =
            StrategySnapshot::from_config(&sample_config(StrategyMode::PreOpenDualBuyPaperTpsl))
                .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Buy));
        assert!(matches!(orders[1].side, OrderSide::Buy));
        assert!(snapshot.uses_paper_tpsl());
        assert!(snapshot.requires_realtime_quotes());
    }

    #[test]
    fn paper_limit_exit_mode_also_builds_buy_orders() {
        let snapshot = StrategySnapshot::from_config(&sample_config(
            StrategyMode::PreOpenDualBuyPaperLimitExit,
        ))
        .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Buy));
        assert!(matches!(orders[1].side, OrderSide::Buy));
        assert!(snapshot.uses_paper_limit_exit());
        assert!(snapshot.uses_paper_trading());
        assert!(snapshot.requires_realtime_quotes());
    }

    #[test]
    fn open_post_guard_mode_also_builds_buy_orders() {
        let snapshot =
            StrategySnapshot::from_config(&sample_config(StrategyMode::OpenPostDualBuyPriceGuard))
                .expect("snapshot");
        let orders = snapshot.order_plans();

        assert!(matches!(orders[0].side, OrderSide::Buy));
        assert!(matches!(orders[1].side, OrderSide::Buy));
        assert!(snapshot.uses_open_post_price_guard());
        assert!(!snapshot.requires_realtime_quotes());
    }
}
