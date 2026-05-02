use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;

use crate::{
    config::{MarketConfig, NetworkConfig},
    http::{build_http_client, join_url},
};

#[derive(Debug, Clone, Copy)]
pub enum BinanceInterval {
    Minutes15,
    Minutes30,
}

impl BinanceInterval {
    fn as_str(self) -> &'static str {
        match self {
            Self::Minutes15 => "15m",
            Self::Minutes30 => "30m",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinanceKline {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub low: Decimal,
    pub close: Decimal,
}

#[derive(Debug, Clone)]
pub struct BinanceFilterSnapshot {
    pub symbol: String,
    pub current_price: Decimal,
    pub support_hit: bool,
    pub support_reason: Option<String>,
    pub rsi_value: Decimal,
    pub rsi_pass: bool,
}

#[derive(Clone)]
pub struct BinanceMarketDataService {
    client: reqwest::Client,
    base_url: String,
    symbol: String,
}

impl BinanceMarketDataService {
    pub fn new(network: &NetworkConfig, market: &MarketConfig) -> Result<Self> {
        let client = build_http_client(network, "pm-alpha-binance")?;
        Ok(Self {
            client,
            base_url: network.binance_rest_url.clone(),
            symbol: market.binance_symbol.clone(),
        })
    }

    pub async fn evaluate_support_and_rsi(
        &self,
        support_lookback_candles: usize,
        support_tolerance_ratio: Decimal,
        ema_period: usize,
        rsi_period: usize,
        rsi_max: Decimal,
    ) -> Result<BinanceFilterSnapshot> {
        let fifteen = self
            .fetch_klines(
                BinanceInterval::Minutes15,
                required_limit(support_lookback_candles, ema_period, rsi_period),
            )
            .await?;
        let thirty = self
            .fetch_klines(
                BinanceInterval::Minutes30,
                required_limit(support_lookback_candles, ema_period, rsi_period),
            )
            .await?;

        let current_price = fifteen
            .last()
            .map(|k| k.close)
            .context("15m klines are empty")?;
        let rsi_value = calculate_rsi(&fifteen, rsi_period)?;
        let support = support_hit(
            &fifteen,
            support_lookback_candles,
            support_tolerance_ratio,
            ema_period,
        )
        .or_else(|| {
            support_hit(
                &thirty,
                support_lookback_candles,
                support_tolerance_ratio,
                ema_period,
            )
        });

        Ok(BinanceFilterSnapshot {
            symbol: self.symbol.clone(),
            current_price,
            support_hit: support.is_some(),
            support_reason: support,
            rsi_value,
            rsi_pass: rsi_value < rsi_max,
        })
    }

    async fn fetch_klines(
        &self,
        interval: BinanceInterval,
        limit: usize,
    ) -> Result<Vec<BinanceKline>> {
        let response = self
            .client
            .get(join_url(&self.base_url, "/api/v3/klines"))
            .query(&[
                ("symbol", self.symbol.as_str()),
                ("interval", interval.as_str()),
                ("limit", &limit.to_string()),
            ])
            .send()
            .await
            .context("binance klines request failed")?
            .error_for_status()
            .context("binance klines request returned non-success status")?;

        let rows = response
            .json::<Vec<Vec<serde_json::Value>>>()
            .await
            .context("failed to decode binance klines response")?;

        rows.into_iter()
            .map(parse_kline_row)
            .collect::<Result<Vec<_>>>()
    }
}

fn parse_kline_row(row: Vec<serde_json::Value>) -> Result<BinanceKline> {
    if row.len() < 7 {
        bail!("binance kline row is too short");
    }

    Ok(BinanceKline {
        open_time: Utc
            .timestamp_millis_opt(
                row[0]
                    .as_i64()
                    .context("binance kline open_time is missing")?,
            )
            .single()
            .context("binance kline open_time is invalid")?,
        low: parse_decimal(&row[3], "low")?,
        close: parse_decimal(&row[4], "close")?,
        close_time: Utc
            .timestamp_millis_opt(
                row[6]
                    .as_i64()
                    .context("binance kline close_time is missing")?,
            )
            .single()
            .context("binance kline close_time is invalid")?,
    })
}

fn parse_decimal(value: &serde_json::Value, field: &str) -> Result<Decimal> {
    Decimal::from_str_exact(
        value
            .as_str()
            .with_context(|| format!("binance kline field '{field}' is not a string"))?,
    )
    .with_context(|| format!("failed to parse binance decimal field '{field}'"))
}

fn required_limit(support_lookback_candles: usize, ema_period: usize, rsi_period: usize) -> usize {
    support_lookback_candles
        .max(ema_period)
        .max(rsi_period + 1)
        .max(20)
}

fn support_hit(
    klines: &[BinanceKline],
    lookback: usize,
    tolerance_ratio: Decimal,
    ema_period: usize,
) -> Option<String> {
    let current_price = klines.last()?.close;
    let recent_low = klines.iter().rev().take(lookback).map(|k| k.low).min()?;

    if current_price >= recent_low
        && percent_distance(current_price, recent_low)? <= tolerance_ratio
    {
        return Some(format!(
            "{} recent_low({})",
            interval_label(klines),
            recent_low.normalize()
        ));
    }

    let ema = calculate_ema(klines, ema_period)?;
    if current_price >= ema && percent_distance(current_price, ema)? <= tolerance_ratio {
        return Some(format!(
            "{} ema{}({})",
            interval_label(klines),
            ema_period,
            ema.normalize()
        ));
    }

    None
}

fn interval_label(klines: &[BinanceKline]) -> &'static str {
    if klines.len() >= 2 {
        let delta = klines[1]
            .open_time
            .signed_duration_since(klines[0].open_time)
            .num_minutes();
        if delta == 30 {
            return "30m";
        }
    }
    "15m"
}

fn percent_distance(current: Decimal, anchor: Decimal) -> Option<Decimal> {
    if anchor <= Decimal::ZERO {
        return None;
    }
    Some((current - anchor).abs() / anchor)
}

fn calculate_ema(klines: &[BinanceKline], period: usize) -> Option<Decimal> {
    if period == 0 || klines.len() < period {
        return None;
    }

    let period_decimal = Decimal::from(period as u32);
    let multiplier = Decimal::from(2u32) / (period_decimal + Decimal::ONE);
    let mut ema = average_close(&klines[..period])?;
    for kline in &klines[period..] {
        ema = (kline.close - ema) * multiplier + ema;
    }
    Some(ema)
}

fn calculate_rsi(klines: &[BinanceKline], period: usize) -> Result<Decimal> {
    if period == 0 || klines.len() <= period {
        bail!("not enough klines to calculate RSI");
    }

    let mut gains = Decimal::ZERO;
    let mut losses = Decimal::ZERO;
    for window in klines.windows(2).take(period) {
        let delta = window[1].close - window[0].close;
        if delta > Decimal::ZERO {
            gains += delta;
        } else {
            losses += -delta;
        }
    }

    let period_decimal = Decimal::from(period as u32);
    let mut avg_gain = gains / period_decimal;
    let mut avg_loss = losses / period_decimal;

    for window in klines.windows(2).skip(period) {
        let delta = window[1].close - window[0].close;
        let gain = if delta > Decimal::ZERO {
            delta
        } else {
            Decimal::ZERO
        };
        let loss = if delta < Decimal::ZERO {
            -delta
        } else {
            Decimal::ZERO
        };
        avg_gain = ((avg_gain * (period_decimal - Decimal::ONE)) + gain) / period_decimal;
        avg_loss = ((avg_loss * (period_decimal - Decimal::ONE)) + loss) / period_decimal;
    }

    if avg_loss <= Decimal::ZERO {
        return Ok(Decimal::from(100u32));
    }

    let rs = avg_gain / avg_loss;
    Ok(Decimal::from(100u32) - (Decimal::from(100u32) / (Decimal::ONE + rs)))
}

fn average_close(klines: &[BinanceKline]) -> Option<Decimal> {
    let sum = klines.iter().fold(Decimal::ZERO, |acc, k| acc + k.close);
    Some(sum / Decimal::from(klines.len() as u32))
}
