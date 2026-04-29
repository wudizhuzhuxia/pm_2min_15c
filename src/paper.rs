use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
use serde_json::json;

use crate::{
    execution::{TakerOrderEstimate, TokenMetadata},
    market::{ResolvedRoundOutcome, RoundDescriptor},
    strategy::{LegSide, leg_label},
};

const USDC_SCALE: u32 = 6;
const SHARE_SCALE: u32 = 2;
const SVG_WIDTH: f64 = 1200.0;
const SVG_HEIGHT: f64 = 720.0;
const SVG_MARGIN_LEFT: f64 = 80.0;
const SVG_MARGIN_RIGHT: f64 = 220.0;
const SVG_MARGIN_TOP: f64 = 60.0;
const SVG_MARGIN_BOTTOM: f64 = 60.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperMakerOrderStatus {
    Resting,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperExitReason {
    TakeProfit,
    StopLoss,
    LimitSell,
    ForcedTakerExit,
    Settlement,
}

#[derive(Debug, Clone)]
pub struct PaperMakerOrder {
    pub leg: LegSide,
    pub price: Decimal,
    pub size: Decimal,
    pub status: PaperMakerOrderStatus,
    pub rejection_reason: Option<String>,
    pub filled_at: Option<DateTime<Utc>>,
}

impl PaperMakerOrder {
    pub fn new(leg: LegSide, price: Decimal, size: Decimal) -> Self {
        Self {
            leg,
            price,
            size,
            status: PaperMakerOrderStatus::Resting,
            rejection_reason: None,
            filled_at: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaperRoundState {
    pub maker_orders: Vec<PaperMakerOrder>,
    pub branches: Vec<PaperBranchState>,
    pub limit_exit_position: Option<PaperLimitExitPosition>,
    pub triggered_leg: Option<LegSide>,
    pub trigger_at: Option<DateTime<Utc>>,
    pub pre_open_cancel_processed: bool,
    pub summaries_recorded: bool,
    pub waiting_resolution_logged: bool,
    pub pending_entry_eval: bool,
    pub pending_exit_eval: bool,
    pub exit_diagnostics: PaperExitDiagnostics,
}

impl PaperRoundState {
    pub fn new() -> Self {
        Self {
            maker_orders: Vec::new(),
            branches: Vec::new(),
            limit_exit_position: None,
            triggered_leg: None,
            trigger_at: None,
            pre_open_cancel_processed: false,
            summaries_recorded: false,
            waiting_resolution_logged: false,
            pending_entry_eval: false,
            pending_exit_eval: false,
            exit_diagnostics: PaperExitDiagnostics::default(),
        }
    }

    pub fn has_triggered(&self) -> bool {
        self.triggered_leg.is_some()
    }

    pub fn all_branches_settled(&self) -> bool {
        !self.branches.is_empty() && self.branches.iter().all(PaperBranchState::is_settled)
    }

    pub fn active_exit_leg(&self) -> Option<LegSide> {
        self.branches
            .iter()
            .find(|branch| branch.has_speculative_position())
            .and_then(PaperBranchState::speculative_leg)
            .or_else(|| {
                self.limit_exit_position
                    .as_ref()
                    .filter(|position| position.has_open_position())
                    .map(|position| position.leg)
            })
    }
}

#[derive(Debug, Clone, Default)]
pub struct PaperExitDiagnostics {
    pub observations: u64,
    pub max_spec_best_bid: Option<Decimal>,
    pub min_spec_best_bid: Option<Decimal>,
    pub max_spec_best_ask: Option<Decimal>,
    pub min_spec_best_ask: Option<Decimal>,
    pub max_opp_best_bid: Option<Decimal>,
    pub min_opp_best_bid: Option<Decimal>,
    pub max_opp_best_ask: Option<Decimal>,
    pub min_opp_best_ask: Option<Decimal>,
    pub max_single_leg_ref: Option<Decimal>,
    pub min_single_leg_ref: Option<Decimal>,
    pub max_composite_ref: Option<Decimal>,
    pub min_composite_ref: Option<Decimal>,
}

impl PaperExitDiagnostics {
    pub fn observe(
        &mut self,
        spec_best_bid: Option<Decimal>,
        spec_best_ask: Option<Decimal>,
        opp_best_bid: Option<Decimal>,
        opp_best_ask: Option<Decimal>,
        single_leg_ref: Option<Decimal>,
        composite_ref: Option<Decimal>,
    ) {
        self.observations += 1;
        update_bounds(
            &mut self.min_spec_best_bid,
            &mut self.max_spec_best_bid,
            spec_best_bid,
        );
        update_bounds(
            &mut self.min_spec_best_ask,
            &mut self.max_spec_best_ask,
            spec_best_ask,
        );
        update_bounds(
            &mut self.min_opp_best_bid,
            &mut self.max_opp_best_bid,
            opp_best_bid,
        );
        update_bounds(
            &mut self.min_opp_best_ask,
            &mut self.max_opp_best_ask,
            opp_best_ask,
        );
        update_bounds(
            &mut self.min_single_leg_ref,
            &mut self.max_single_leg_ref,
            single_leg_ref,
        );
        update_bounds(
            &mut self.min_composite_ref,
            &mut self.max_composite_ref,
            composite_ref,
        );
    }
}

fn update_bounds(
    min_slot: &mut Option<Decimal>,
    max_slot: &mut Option<Decimal>,
    value: Option<Decimal>,
) {
    let Some(value) = value else {
        return;
    };

    if min_slot.map(|current| value < current).unwrap_or(true) {
        *min_slot = Some(value);
    }
    if max_slot.map(|current| value > current).unwrap_or(true) {
        *max_slot = Some(value);
    }
}

#[derive(Debug, Clone)]
pub struct PaperLimitExitPosition {
    pub label: String,
    pub leg: LegSide,
    pub entry_size: Decimal,
    pub remaining_size: Decimal,
    pub entry_price: Decimal,
    pub exit_limit_price: Decimal,
    pub force_taker_exit_at: DateTime<Utc>,
    pub yes_balance: Decimal,
    pub no_balance: Decimal,
    pub maker_entry_quote: Decimal,
    pub exit_quote: Decimal,
    pub settlement_value: Decimal,
    pub total_fees_usdc: Decimal,
    pub exit_reason: Option<PaperExitReason>,
    pub settled_at: Option<DateTime<Utc>>,
}

impl PaperLimitExitPosition {
    pub fn new(
        label: String,
        leg: LegSide,
        entry_price: Decimal,
        entry_size: Decimal,
        exit_limit_price: Decimal,
        force_taker_exit_at: DateTime<Utc>,
    ) -> Self {
        let maker_entry_quote = (entry_price * entry_size).trunc_with_scale(USDC_SCALE);
        let mut position = Self {
            label,
            leg,
            entry_size: entry_size.trunc_with_scale(SHARE_SCALE),
            remaining_size: entry_size.trunc_with_scale(SHARE_SCALE),
            entry_price,
            exit_limit_price,
            force_taker_exit_at,
            yes_balance: Decimal::ZERO,
            no_balance: Decimal::ZERO,
            maker_entry_quote,
            exit_quote: Decimal::ZERO,
            settlement_value: Decimal::ZERO,
            total_fees_usdc: Decimal::ZERO,
            exit_reason: None,
            settled_at: None,
        };
        match leg {
            LegSide::Yes => position.yes_balance = position.entry_size,
            LegSide::No => position.no_balance = position.entry_size,
        }
        position
    }

    pub fn has_open_position(&self) -> bool {
        self.remaining_size > Decimal::ZERO
    }

    pub fn should_force_taker_exit(&self, now: DateTime<Utc>) -> bool {
        self.has_open_position() && now >= self.force_taker_exit_at
    }

    pub fn apply_limit_exit(&mut self, fill_size: Decimal) -> Result<()> {
        let fill_size = fill_size
            .min(self.remaining_size)
            .trunc_with_scale(SHARE_SCALE);
        if fill_size <= Decimal::ZERO {
            return Ok(());
        }

        self.reduce_balance(fill_size)?;
        self.exit_quote += (fill_size * self.exit_limit_price).trunc_with_scale(USDC_SCALE);
        self.total_fees_usdc += maker_fee_usdc();
        self.remaining_size = (self.remaining_size - fill_size)
            .max(Decimal::ZERO)
            .trunc_with_scale(SHARE_SCALE);
        if self.remaining_size <= Decimal::ZERO {
            self.exit_reason = Some(PaperExitReason::LimitSell);
        }
        Ok(())
    }

    pub fn apply_taker_exit(&mut self, fill: &PaperFill) -> Result<()> {
        if fill.size <= Decimal::ZERO {
            return Ok(());
        }

        self.reduce_balance(fill.size)?;
        self.exit_quote += fill.quote;
        self.total_fees_usdc += fill.fee_usdc;
        self.remaining_size = (self.remaining_size - fill.size)
            .max(Decimal::ZERO)
            .trunc_with_scale(SHARE_SCALE);
        self.exit_reason = Some(PaperExitReason::ForcedTakerExit);
        Ok(())
    }

    pub fn settle(&mut self, outcome: &ResolvedRoundOutcome, settled_at: DateTime<Utc>) {
        self.settlement_value = (self.yes_balance * outcome.yes_payout
            + self.no_balance * outcome.no_payout)
            .trunc_with_scale(USDC_SCALE);
        self.settled_at = Some(settled_at);
        if self.exit_reason.is_none() {
            self.exit_reason = Some(PaperExitReason::Settlement);
        }
    }

    pub fn gross_pnl_before_fees(&self) -> Decimal {
        (self.exit_quote + self.settlement_value - self.maker_entry_quote)
            .trunc_with_scale(USDC_SCALE)
    }

    pub fn net_pnl_normal(&self) -> Decimal {
        (self.gross_pnl_before_fees() - self.total_fees_usdc).trunc_with_scale(USDC_SCALE)
    }

    pub fn fee_rebate_usdc(&self, fee_rebate_rate: Decimal) -> Decimal {
        (self.total_fees_usdc * fee_rebate_rate).trunc_with_scale(USDC_SCALE)
    }

    pub fn net_pnl_with_rebate(&self, fee_rebate_rate: Decimal) -> Decimal {
        (self.net_pnl_normal() + self.fee_rebate_usdc(fee_rebate_rate)).trunc_with_scale(USDC_SCALE)
    }

    pub fn summary(
        &self,
        round: &RoundDescriptor,
        fee_rebate_rate: Decimal,
    ) -> Option<PaperRoundSummary> {
        Some(PaperRoundSummary {
            settled_at: self.settled_at?,
            condition_id: round.condition_id.clone(),
            market_slug: round.market_slug.clone(),
            take_profit_percent: self.label.clone(),
            maker_leg: leg_label(self.leg).to_owned(),
            taker_leg: String::new(),
            maker_size: self.entry_size,
            taker_size: Decimal::ZERO,
            speculative_size_initial: self.entry_size,
            speculative_size_remaining: self.remaining_size,
            maker_entry_quote: self.maker_entry_quote,
            taker_entry_quote: Decimal::ZERO,
            exit_quote: self.exit_quote,
            settlement_value: self.settlement_value,
            total_fees_usdc: self.total_fees_usdc,
            fee_rebate_usdc: self.fee_rebate_usdc(fee_rebate_rate),
            pair_cost_delta_usdc: Decimal::ZERO,
            gross_pnl_before_fees: self.gross_pnl_before_fees(),
            net_pnl_normal: self.net_pnl_normal(),
            net_pnl_rebate: self.net_pnl_with_rebate(fee_rebate_rate),
            exit_reason: self
                .exit_reason
                .map(exit_reason_label)
                .unwrap_or("unknown")
                .to_owned(),
        })
    }

    fn reduce_balance(&mut self, fill_size: Decimal) -> Result<()> {
        match self.leg {
            LegSide::Yes => {
                if self.yes_balance < fill_size {
                    return Err(anyhow::anyhow!(
                        "paper limit-exit attempted to sell more YES than held"
                    ));
                }
                self.yes_balance -= fill_size;
            }
            LegSide::No => {
                if self.no_balance < fill_size {
                    return Err(anyhow::anyhow!(
                        "paper limit-exit attempted to sell more NO than held"
                    ));
                }
                self.no_balance -= fill_size;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PaperBranchState {
    pub take_profit_percent: Decimal,
    pub stop_loss_price: Decimal,
    pub maker_leg: Option<LegSide>,
    pub taker_leg: Option<LegSide>,
    pub maker_size: Decimal,
    pub taker_size: Decimal,
    pub speculative_size_initial: Decimal,
    pub speculative_size_remaining: Decimal,
    pub taker_average_price: Option<Decimal>,
    pub take_profit_price: Option<Decimal>,
    pub yes_balance: Decimal,
    pub no_balance: Decimal,
    pub maker_entry_quote: Decimal,
    pub taker_entry_quote: Decimal,
    pub exit_quote: Decimal,
    pub settlement_value: Decimal,
    pub total_fees_usdc: Decimal,
    pub pair_cost_delta_usdc: Decimal,
    pub spec_exit_reason: Option<PaperExitReason>,
    pub settled_at: Option<DateTime<Utc>>,
}

impl PaperBranchState {
    pub fn new(take_profit_percent: Decimal, stop_loss_price: Decimal) -> Self {
        Self {
            take_profit_percent,
            stop_loss_price,
            maker_leg: None,
            taker_leg: None,
            maker_size: Decimal::ZERO,
            taker_size: Decimal::ZERO,
            speculative_size_initial: Decimal::ZERO,
            speculative_size_remaining: Decimal::ZERO,
            taker_average_price: None,
            take_profit_price: None,
            yes_balance: Decimal::ZERO,
            no_balance: Decimal::ZERO,
            maker_entry_quote: Decimal::ZERO,
            taker_entry_quote: Decimal::ZERO,
            exit_quote: Decimal::ZERO,
            settlement_value: Decimal::ZERO,
            total_fees_usdc: Decimal::ZERO,
            pair_cost_delta_usdc: Decimal::ZERO,
            spec_exit_reason: None,
            settled_at: None,
        }
    }

    pub fn initialize(
        &mut self,
        maker_leg: LegSide,
        maker_price: Decimal,
        maker_size: Decimal,
        taker_leg: LegSide,
        taker_fill: Option<PaperFill>,
    ) {
        self.maker_leg = Some(maker_leg);
        self.taker_leg = Some(taker_leg);
        self.maker_size = maker_size;
        self.maker_entry_quote = (maker_price * maker_size).trunc_with_scale(USDC_SCALE);
        match maker_leg {
            LegSide::Yes => self.yes_balance += maker_size,
            LegSide::No => self.no_balance += maker_size,
        }

        let Some(fill) = taker_fill else {
            return;
        };

        self.taker_size = fill.size;
        self.taker_average_price = fill.average_price();
        self.take_profit_price = self
            .taker_average_price
            .map(|price| (price * (Decimal::ONE + self.take_profit_percent)).trunc_with_scale(4));
        self.taker_entry_quote = fill.quote;
        self.total_fees_usdc = fill.fee_usdc;
        match taker_leg {
            LegSide::Yes => self.yes_balance += fill.size,
            LegSide::No => self.no_balance += fill.size,
        }

        let paired_size = maker_size.min(fill.size).trunc_with_scale(SHARE_SCALE);
        let taker_average_price = fill.average_price().unwrap_or(Decimal::ZERO);
        self.pair_cost_delta_usdc = (paired_size
            * (maker_price + taker_average_price - Decimal::ONE))
            .trunc_with_scale(USDC_SCALE);
        self.speculative_size_initial = (fill.size - maker_size)
            .max(Decimal::ZERO)
            .trunc_with_scale(SHARE_SCALE);
        self.speculative_size_remaining = self.speculative_size_initial;
    }

    pub fn speculative_leg(&self) -> Option<LegSide> {
        self.taker_leg
    }

    pub fn is_settled(&self) -> bool {
        self.settled_at.is_some()
    }

    pub fn has_speculative_position(&self) -> bool {
        self.speculative_size_remaining > Decimal::ZERO
    }

    pub fn should_take_profit(&self, best_bid: Option<Decimal>) -> bool {
        self.has_speculative_position()
            && self.take_profit_price.is_some()
            && best_bid
                .zip(self.take_profit_price)
                .map(|(best_bid, target)| best_bid >= target)
                .unwrap_or(false)
    }

    pub fn should_stop_loss(&self, best_bid: Option<Decimal>) -> bool {
        self.has_speculative_position()
            && best_bid
                .map(|best_bid| best_bid <= self.stop_loss_price)
                .unwrap_or(false)
    }

    pub fn apply_speculative_exit(
        &mut self,
        fill: &PaperFill,
        reason: PaperExitReason,
    ) -> Result<()> {
        let Some(speculative_leg) = self.speculative_leg() else {
            return Ok(());
        };

        if fill.size <= Decimal::ZERO {
            return Ok(());
        }

        match speculative_leg {
            LegSide::Yes => {
                if self.yes_balance < fill.size {
                    return Err(anyhow::anyhow!(
                        "paper exit attempted to sell more YES than held"
                    ));
                }
                self.yes_balance -= fill.size;
            }
            LegSide::No => {
                if self.no_balance < fill.size {
                    return Err(anyhow::anyhow!(
                        "paper exit attempted to sell more NO than held"
                    ));
                }
                self.no_balance -= fill.size;
            }
        }

        self.exit_quote += fill.quote;
        self.total_fees_usdc += fill.fee_usdc;
        self.speculative_size_remaining = (self.speculative_size_remaining - fill.size)
            .max(Decimal::ZERO)
            .trunc_with_scale(SHARE_SCALE);
        self.spec_exit_reason = Some(reason);
        Ok(())
    }

    pub fn settle(&mut self, outcome: &ResolvedRoundOutcome, settled_at: DateTime<Utc>) {
        self.settlement_value = (self.yes_balance * outcome.yes_payout
            + self.no_balance * outcome.no_payout)
            .trunc_with_scale(USDC_SCALE);
        self.settled_at = Some(settled_at);
        if self.spec_exit_reason.is_none() {
            self.spec_exit_reason = Some(PaperExitReason::Settlement);
        }
    }

    pub fn gross_pnl_before_fees(&self) -> Decimal {
        (self.exit_quote + self.settlement_value - self.maker_entry_quote - self.taker_entry_quote)
            .trunc_with_scale(USDC_SCALE)
    }

    pub fn net_pnl_normal(&self) -> Decimal {
        (self.gross_pnl_before_fees() - self.total_fees_usdc).trunc_with_scale(USDC_SCALE)
    }

    pub fn fee_rebate_usdc(&self, fee_rebate_rate: Decimal) -> Decimal {
        (self.total_fees_usdc * fee_rebate_rate).trunc_with_scale(USDC_SCALE)
    }

    pub fn net_pnl_with_rebate(&self, fee_rebate_rate: Decimal) -> Decimal {
        (self.net_pnl_normal() + self.fee_rebate_usdc(fee_rebate_rate)).trunc_with_scale(USDC_SCALE)
    }

    pub fn summary(
        &self,
        round: &RoundDescriptor,
        fee_rebate_rate: Decimal,
    ) -> Option<PaperRoundSummary> {
        Some(PaperRoundSummary {
            settled_at: self.settled_at?,
            condition_id: round.condition_id.clone(),
            market_slug: round.market_slug.clone(),
            take_profit_percent: percent_label(self.take_profit_percent),
            maker_leg: self.maker_leg.map(leg_label).unwrap_or_default().to_owned(),
            taker_leg: self.taker_leg.map(leg_label).unwrap_or_default().to_owned(),
            maker_size: self.maker_size,
            taker_size: self.taker_size,
            speculative_size_initial: self.speculative_size_initial,
            speculative_size_remaining: self.speculative_size_remaining,
            maker_entry_quote: self.maker_entry_quote,
            taker_entry_quote: self.taker_entry_quote,
            exit_quote: self.exit_quote,
            settlement_value: self.settlement_value,
            total_fees_usdc: self.total_fees_usdc,
            fee_rebate_usdc: self.fee_rebate_usdc(fee_rebate_rate),
            pair_cost_delta_usdc: self.pair_cost_delta_usdc,
            gross_pnl_before_fees: self.gross_pnl_before_fees(),
            net_pnl_normal: self.net_pnl_normal(),
            net_pnl_rebate: self.net_pnl_with_rebate(fee_rebate_rate),
            exit_reason: self
                .spec_exit_reason
                .map(exit_reason_label)
                .unwrap_or("unknown")
                .to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PaperFill {
    pub size: Decimal,
    pub quote: Decimal,
    pub fee_usdc: Decimal,
    pub limit_price: Decimal,
}

impl PaperFill {
    pub fn average_price(&self) -> Option<Decimal> {
        if self.size <= Decimal::ZERO {
            return None;
        }
        Some((self.quote / self.size).trunc_with_scale(4))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperRoundSummary {
    pub settled_at: DateTime<Utc>,
    pub condition_id: String,
    pub market_slug: String,
    pub take_profit_percent: String,
    pub maker_leg: String,
    pub taker_leg: String,
    pub maker_size: Decimal,
    pub taker_size: Decimal,
    pub speculative_size_initial: Decimal,
    pub speculative_size_remaining: Decimal,
    pub maker_entry_quote: Decimal,
    pub taker_entry_quote: Decimal,
    pub exit_quote: Decimal,
    pub settlement_value: Decimal,
    pub total_fees_usdc: Decimal,
    pub fee_rebate_usdc: Decimal,
    pub pair_cost_delta_usdc: Decimal,
    pub gross_pnl_before_fees: Decimal,
    pub net_pnl_normal: Decimal,
    pub net_pnl_rebate: Decimal,
    pub exit_reason: String,
}

pub struct PaperRuntime {
    instance_name: String,
    fee_rebate_rate: Decimal,
    session_dir: PathBuf,
    events_path: PathBuf,
    summaries_path: PathBuf,
    pnl_normal_path: PathBuf,
    pnl_rebate_path: PathBuf,
    summaries: Vec<PaperRoundSummary>,
}

impl PaperRuntime {
    pub fn new(
        instance_name: &str,
        output_dir: impl AsRef<Path>,
        fee_rebate_rate: Decimal,
    ) -> Result<Self> {
        let base_dir = output_dir.as_ref();
        let session_dir = base_dir.join(Utc::now().format("%Y%m%dT%H%M%SZ").to_string());
        fs::create_dir_all(&session_dir).with_context(|| {
            format!(
                "failed to create paper simulation output directory {}",
                session_dir.display()
            )
        })?;

        let events_path = session_dir.join("paper_events.jsonl");
        let summaries_path = session_dir.join("paper_branch_summaries.csv");
        let pnl_normal_path = session_dir.join("paper_pnl_normal.svg");
        let pnl_rebate_path = session_dir.join("paper_pnl_rebate.svg");

        let mut summary_file = File::create(&summaries_path).with_context(|| {
            format!(
                "failed to create paper summary file {}",
                summaries_path.display()
            )
        })?;
        summary_file
            .write_all(SUMMARY_HEADER.as_bytes())
            .with_context(|| {
                format!(
                    "failed to write paper summary header to {}",
                    summaries_path.display()
                )
            })?;

        Ok(Self {
            instance_name: instance_name.to_owned(),
            fee_rebate_rate,
            session_dir,
            events_path,
            summaries_path,
            pnl_normal_path,
            pnl_rebate_path,
            summaries: Vec::new(),
        })
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    pub fn log_event(
        &mut self,
        round: &RoundDescriptor,
        event_type: &str,
        take_profit_percent: Option<Decimal>,
        details: serde_json::Value,
    ) -> Result<()> {
        self.log_event_label(
            round,
            event_type,
            take_profit_percent.map(percent_label),
            details,
        )
    }

    pub fn log_event_label(
        &mut self,
        round: &RoundDescriptor,
        event_type: &str,
        label: Option<String>,
        details: serde_json::Value,
    ) -> Result<()> {
        let record = PaperEventRecord {
            timestamp: Utc::now(),
            instance_name: self.instance_name.clone(),
            condition_id: round.condition_id.clone(),
            market_slug: round.market_slug.clone(),
            event_type: event_type.to_owned(),
            take_profit_percent: label,
            details,
        };
        append_json_line(&self.events_path, &record)
    }

    pub fn log_round_submitted(
        &mut self,
        round: &RoundDescriptor,
        maker_orders: &[PaperMakerOrder],
    ) -> Result<()> {
        self.log_event(
            round,
            "paper_orders_submitted",
            None,
            json!({
                "orders": maker_orders.iter().map(|order| {
                    json!({
                        "leg": leg_label(order.leg),
                        "price": order.price,
                        "size": order.size,
                        "status": order.status,
                    })
                }).collect::<Vec<_>>(),
            }),
        )
    }

    pub fn record_summary(&mut self, summary: PaperRoundSummary) -> Result<()> {
        append_summary_csv(&self.summaries_path, &summary)?;
        self.summaries.push(summary);
        self.render_charts()
    }

    fn render_charts(&self) -> Result<()> {
        write_equity_chart(
            &self.pnl_normal_path,
            "Paper PnL (Normal Fees)",
            &self.summaries,
            EquityMetric::NormalFees,
        )?;
        write_equity_chart(
            &self.pnl_rebate_path,
            "Paper PnL (30% Fee Rebate)",
            &self.summaries,
            EquityMetric::FeeRebate,
        )?;
        Ok(())
    }

    pub fn fee_rebate_rate(&self) -> Decimal {
        self.fee_rebate_rate
    }
}

pub fn maker_fee_usdc() -> Decimal {
    Decimal::ZERO
}

pub fn taker_fee_usdc(size: Decimal, average_price: Decimal, fee_rate_bps: u32) -> Decimal {
    if size <= Decimal::ZERO || average_price <= Decimal::ZERO || fee_rate_bps == 0 {
        return Decimal::ZERO;
    }

    let fee_rate = Decimal::from(fee_rate_bps) / Decimal::from(10_000u32);
    (size * fee_rate * average_price * (Decimal::ONE - average_price)).trunc_with_scale(USDC_SCALE)
}

pub fn make_paper_buy_fill(
    estimate: TakerOrderEstimate,
    metadata: &TokenMetadata,
) -> Option<PaperFill> {
    if estimate.size <= Decimal::ZERO {
        return None;
    }

    let average_price = (estimate.quote / estimate.size).trunc_with_scale(4);
    Some(PaperFill {
        size: estimate.size.trunc_with_scale(SHARE_SCALE),
        quote: estimate.quote.trunc_with_scale(USDC_SCALE),
        fee_usdc: taker_fee_usdc(estimate.size, average_price, metadata.fee_rate_bps),
        limit_price: estimate.price,
    })
}

pub fn make_paper_sell_fill(
    estimate: TakerOrderEstimate,
    metadata: &TokenMetadata,
) -> Option<PaperFill> {
    if estimate.size <= Decimal::ZERO {
        return None;
    }

    let average_price = (estimate.quote / estimate.size).trunc_with_scale(4);
    Some(PaperFill {
        size: estimate.size.trunc_with_scale(SHARE_SCALE),
        quote: estimate.quote.trunc_with_scale(USDC_SCALE),
        fee_usdc: taker_fee_usdc(estimate.size, average_price, metadata.fee_rate_bps),
        limit_price: estimate.price,
    })
}

#[derive(Debug, Serialize)]
struct PaperEventRecord {
    timestamp: DateTime<Utc>,
    instance_name: String,
    condition_id: String,
    market_slug: String,
    event_type: String,
    take_profit_percent: Option<String>,
    details: serde_json::Value,
}

const SUMMARY_HEADER: &str = "settled_at,condition_id,market_slug,take_profit_percent,maker_leg,taker_leg,maker_size,taker_size,speculative_size_initial,speculative_size_remaining,maker_entry_quote,taker_entry_quote,exit_quote,settlement_value,total_fees_usdc,fee_rebate_usdc,pair_cost_delta_usdc,gross_pnl_before_fees,net_pnl_normal,net_pnl_rebate,exit_reason\n";

fn append_json_line(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open paper event log {}", path.display()))?;
    serde_json::to_writer(&mut file, value)
        .with_context(|| format!("failed to encode paper event log {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to append paper event newline {}", path.display()))?;
    Ok(())
}

fn append_summary_csv(path: &Path, summary: &PaperRoundSummary) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open paper summary file {}", path.display()))?;
    let row = [
        summary.settled_at.to_rfc3339(),
        csv_escape(&summary.condition_id),
        csv_escape(&summary.market_slug),
        csv_escape(&summary.take_profit_percent),
        csv_escape(&summary.maker_leg),
        csv_escape(&summary.taker_leg),
        summary.maker_size.to_string(),
        summary.taker_size.to_string(),
        summary.speculative_size_initial.to_string(),
        summary.speculative_size_remaining.to_string(),
        summary.maker_entry_quote.to_string(),
        summary.taker_entry_quote.to_string(),
        summary.exit_quote.to_string(),
        summary.settlement_value.to_string(),
        summary.total_fees_usdc.to_string(),
        summary.fee_rebate_usdc.to_string(),
        summary.pair_cost_delta_usdc.to_string(),
        summary.gross_pnl_before_fees.to_string(),
        summary.net_pnl_normal.to_string(),
        summary.net_pnl_rebate.to_string(),
        csv_escape(&summary.exit_reason),
    ]
    .join(",");
    file.write_all(row.as_bytes())
        .with_context(|| format!("failed to append paper summary row {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to append paper summary newline {}", path.display()))?;
    Ok(())
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[derive(Clone, Copy)]
enum EquityMetric {
    NormalFees,
    FeeRebate,
}

fn write_equity_chart(
    path: &Path,
    title: &str,
    summaries: &[PaperRoundSummary],
    metric: EquityMetric,
) -> Result<()> {
    let mut grouped = BTreeMap::<String, Vec<f64>>::new();
    for summary in summaries {
        let series = grouped
            .entry(summary.take_profit_percent.clone())
            .or_default();
        let previous = series.last().copied().unwrap_or(0.0);
        let delta = match metric {
            EquityMetric::NormalFees => summary.net_pnl_normal,
            EquityMetric::FeeRebate => summary.net_pnl_rebate,
        }
        .to_f64()
        .unwrap_or(0.0);
        series.push(previous + delta);
    }

    let max_points = grouped.values().map(Vec::len).max().unwrap_or(1).max(1) as f64;
    let mut min_y = grouped
        .values()
        .flat_map(|points| points.iter().copied())
        .fold(0.0, f64::min);
    let mut max_y = grouped
        .values()
        .flat_map(|points| points.iter().copied())
        .fold(0.0, f64::max);
    if (max_y - min_y).abs() < f64::EPSILON {
        min_y -= 1.0;
        max_y += 1.0;
    } else {
        let padding = (max_y - min_y) * 0.12;
        min_y -= padding;
        max_y += padding;
    }

    let chart_width = SVG_WIDTH - SVG_MARGIN_LEFT - SVG_MARGIN_RIGHT;
    let chart_height = SVG_HEIGHT - SVG_MARGIN_TOP - SVG_MARGIN_BOTTOM;
    let x_for = |index: usize| {
        SVG_MARGIN_LEFT + (index as f64) * chart_width / (max_points.max(1.0) - 1.0).max(1.0)
    };
    let y_for =
        |value: f64| SVG_MARGIN_TOP + (max_y - value) * chart_height / (max_y - min_y).max(1e-9);

    let mut svg = String::new();
    svg.push_str(&format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}">"#
    ));
    svg.push_str(r##"<rect width="100%" height="100%" fill="#ffffff"/>"##);
    svg.push_str(&format!(
        r##"<text x="{:.1}" y="32" font-size="24" font-family="Menlo, monospace" fill="#111827">{}</text>"##,
        SVG_MARGIN_LEFT,
        escape_svg(title)
    ));

    for step in 0..=5 {
        let y_value = min_y + (max_y - min_y) * (step as f64) / 5.0;
        let y = y_for(y_value);
        svg.push_str(&format!(
            r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#e5e7eb" stroke-width="1"/>"##,
            SVG_MARGIN_LEFT,
            y,
            SVG_MARGIN_LEFT + chart_width,
            y
        ));
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" font-size="12" font-family="Menlo, monospace" fill="#6b7280" text-anchor="end">{:.3}</text>"##,
            SVG_MARGIN_LEFT - 10.0,
            y + 4.0,
            y_value
        ));
    }

    svg.push_str(&format!(
        r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#111827" stroke-width="2"/>"##,
        SVG_MARGIN_LEFT,
        SVG_MARGIN_TOP,
        SVG_MARGIN_LEFT,
        SVG_MARGIN_TOP + chart_height
    ));
    svg.push_str(&format!(
        r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#111827" stroke-width="2"/>"##,
        SVG_MARGIN_LEFT,
        SVG_MARGIN_TOP + chart_height,
        SVG_MARGIN_LEFT + chart_width,
        SVG_MARGIN_TOP + chart_height
    ));

    for (series_index, (label, points)) in grouped.iter().enumerate() {
        let color = palette_color(series_index);
        if !points.is_empty() {
            let polyline = points
                .iter()
                .enumerate()
                .map(|(index, value)| format!("{:.1},{:.1}", x_for(index), y_for(*value)))
                .collect::<Vec<_>>()
                .join(" ");
            svg.push_str(&format!(
                r#"<polyline fill="none" stroke="{}" stroke-width="3" points="{}"/>"#,
                color, polyline
            ));
            for (index, value) in points.iter().enumerate() {
                svg.push_str(&format!(
                    r#"<circle cx="{:.1}" cy="{:.1}" r="3.5" fill="{}"/>"#,
                    x_for(index),
                    y_for(*value),
                    color
                ));
            }
        }

        let legend_y = SVG_MARGIN_TOP + 20.0 + series_index as f64 * 24.0;
        let legend_x = SVG_MARGIN_LEFT + chart_width + 24.0;
        svg.push_str(&format!(
            r#"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="{}" stroke-width="4"/>"#,
            legend_x,
            legend_y,
            legend_x + 18.0,
            legend_y,
            color
        ));
        let last_value = points.last().copied().unwrap_or(0.0);
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" font-size="14" font-family="Menlo, monospace" fill="#111827">{} ({:.3})</text>"##,
            legend_x + 28.0,
            legend_y + 4.0,
            escape_svg(label),
            last_value
        ));
    }

    svg.push_str("</svg>");
    fs::write(path, svg)
        .with_context(|| format!("failed to write paper pnl chart {}", path.display()))?;
    Ok(())
}

fn palette_color(index: usize) -> &'static str {
    const COLORS: [&str; 6] = [
        "#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed", "#0891b2",
    ];
    COLORS[index % COLORS.len()]
}

fn escape_svg(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn percent_label(percent_ratio: Decimal) -> String {
    format!(
        "tp_{}pct",
        (percent_ratio * Decimal::from(100u32)).trunc_with_scale(2)
    )
}

pub fn limit_exit_label(limit_price: Decimal, force_before_settle_secs: u64) -> String {
    format!(
        "limit_{}c_tminus{}s",
        (limit_price * Decimal::from(100u32)).trunc_with_scale(2),
        force_before_settle_secs
    )
}

pub fn exit_reason_label(reason: PaperExitReason) -> &'static str {
    match reason {
        PaperExitReason::TakeProfit => "take_profit",
        PaperExitReason::StopLoss => "stop_loss",
        PaperExitReason::LimitSell => "limit_sell",
        PaperExitReason::ForcedTakerExit => "forced_taker_exit",
        PaperExitReason::Settlement => "settlement",
    }
}
