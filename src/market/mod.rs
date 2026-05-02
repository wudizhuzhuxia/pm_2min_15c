#![allow(dead_code)]

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::{
    config::MarketConfig,
    http::{build_http_client, join_url},
};

#[derive(Debug, Clone)]
pub struct RoundDescriptor {
    pub market_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub question: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub opens_at: DateTime<Utc>,
    pub settles_at: DateTime<Utc>,
}

impl RoundDescriptor {
    pub fn opens_at_unix_ms(&self) -> i64 {
        self.opens_at.timestamp_millis()
    }

    pub fn settles_at_unix_ms(&self) -> i64 {
        self.settles_at.timestamp_millis()
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedRoundOutcome {
    pub yes_payout: Decimal,
    pub no_payout: Decimal,
}

#[derive(Debug, Clone)]
pub struct RoundReferencePriceSnapshot {
    pub open_price: Decimal,
    pub current_price: Decimal,
    pub open_source: String,
    pub current_source: String,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct MarketDiscoveryService {
    client: Client,
    gamma_base_url: String,
    data_api_base_url: String,
    config: MarketConfig,
    round_interval_secs: u64,
}

impl MarketDiscoveryService {
    pub fn new(
        network: &crate::config::NetworkConfig,
        market: &MarketConfig,
        round_interval_secs: u64,
    ) -> Result<Self> {
        let client = build_http_client(network, "pm-alpha-market-discovery")?;
        Ok(Self {
            client,
            gamma_base_url: network.gamma_rest_url.clone(),
            data_api_base_url: network.data_api_url.clone(),
            config: market.clone(),
            round_interval_secs,
        })
    }

    pub async fn discover_round(&self) -> Result<RoundDescriptor> {
        let markets = self
            .fetch_candidate_markets(self.config.discovery_lookahead_secs, 200)
            .await?;
        let now = Utc::now();
        let max_open = now + Duration::seconds(self.config.discovery_lookahead_secs as i64);
        let matcher = SeriesMatcher::new(&self.config.series_slug);

        let round = markets
            .into_iter()
            .filter_map(|market| self.market_to_round(market))
            .filter(|round| is_upcoming_round(round, now, max_open))
            .filter(|round| matcher.matches(round))
            .min_by(|left, right| compare_rounds(left, right))
            .with_context(|| {
                format!(
                    "unable to discover an active or upcoming market matching '{}'",
                    self.config.series_slug
                )
            })?;

        info!(
            market_slug = %round.market_slug,
            condition_id = %round.condition_id,
            yes_token_id = %round.yes_token_id,
            no_token_id = %round.no_token_id,
            opens_at = %round.opens_at,
            settles_at = %round.settles_at,
            "discovered trading round"
        );

        Ok(round)
    }

    pub async fn discover_upcoming_rounds(
        &self,
        lookahead_secs: u64,
        limit: usize,
    ) -> Result<Vec<RoundDescriptor>> {
        let markets = self.fetch_candidate_markets(lookahead_secs, 2_000).await?;
        let now = Utc::now();
        let max_open = now + Duration::seconds(lookahead_secs as i64);
        let matcher = SeriesMatcher::new(&self.config.series_slug);
        let mut rounds = markets
            .into_iter()
            .filter_map(|market| self.market_to_round(market))
            .filter(|round| is_upcoming_round(round, now, max_open))
            .filter(|round| matcher.matches(round))
            .collect::<Vec<_>>();

        rounds.sort_by(compare_rounds);
        rounds.dedup_by(|left, right| left.condition_id == right.condition_id);
        rounds.truncate(limit);
        Ok(rounds)
    }

    pub async fn discover_latest_round(&self, lookahead_secs: u64) -> Result<RoundDescriptor> {
        if let Some(round) = self
            .discover_latest_round_via_listing(lookahead_secs)
            .await?
        {
            return Ok(round);
        }

        let round = self
            .discover_latest_round_via_slug_probe(lookahead_secs)
            .await?;
        info!(
            market_slug = %round.market_slug,
            condition_id = %round.condition_id,
            yes_token_id = %round.yes_token_id,
            no_token_id = %round.no_token_id,
            opens_at = %round.opens_at,
            settles_at = %round.settles_at,
            "discovered latest trading round via slug probe fallback"
        );
        Ok(round)
    }

    async fn discover_latest_round_via_listing(
        &self,
        lookahead_secs: u64,
    ) -> Result<Option<RoundDescriptor>> {
        let markets = self.fetch_candidate_markets(lookahead_secs, 2_000).await?;
        let now = Utc::now();
        let max_open = now + Duration::seconds(lookahead_secs as i64);
        let matcher = SeriesMatcher::new(&self.config.series_slug);

        Ok(markets
            .into_iter()
            .filter_map(|market| self.market_to_round(market))
            .filter(|round| is_upcoming_round(round, now, max_open))
            .filter(|round| matcher.matches(round))
            .max_by(|left, right| compare_rounds(left, right)))
    }

    async fn discover_latest_round_via_slug_probe(
        &self,
        lookahead_secs: u64,
    ) -> Result<RoundDescriptor> {
        let seed_round = self
            .discover_round()
            .await
            .context("unable to seed latest-round slug probe from a nearby round")?;
        let slug_prefix = strip_timestamp_suffix(&seed_round.market_slug).with_context(|| {
            format!(
                "unable to derive slug prefix from nearby round slug '{}'",
                seed_round.market_slug
            )
        })?;

        let now = Utc::now();
        let max_open = now + Duration::seconds(lookahead_secs as i64);
        let latest_settle = align_down_to_round(
            (max_open + Duration::seconds(self.round_interval_secs as i64)).timestamp(),
            self.round_interval_secs,
        );
        let matcher = SeriesMatcher::new(&self.config.series_slug);
        let rounds_to_probe = (lookahead_secs / self.round_interval_secs).saturating_add(4);

        for step in 0..=rounds_to_probe {
            let settle_ts = latest_settle - (step as i64 * self.round_interval_secs as i64);
            let slug = format!("{slug_prefix}-{settle_ts}");
            let Some(market) = self.fetch_market_by_slug(&slug).await? else {
                continue;
            };
            let Some(round) = self.market_to_round(market) else {
                continue;
            };
            if !is_upcoming_round(&round, now, max_open) || !matcher.matches(&round) {
                continue;
            }

            return Ok(round);
        }

        bail!(
            "unable to discover the latest active market matching '{}' via listing or slug probe",
            self.config.series_slug
        )
    }

    pub async fn discover_recent_settled_rounds(
        &self,
        lookback_secs: u64,
        limit: usize,
    ) -> Result<Vec<RoundDescriptor>> {
        let markets = self
            .fetch_recent_settled_markets(lookback_secs, limit)
            .await?;
        Ok(self.collect_recent_settled_rounds(markets, Utc::now(), lookback_secs))
    }

    pub async fn fetch_round_outcome(
        &self,
        round: &RoundDescriptor,
    ) -> Result<Option<ResolvedRoundOutcome>> {
        let Some(market) = self.fetch_market_by_slug(&round.market_slug).await? else {
            return Ok(None);
        };
        Ok(market.resolved_outcome())
    }

    pub async fn fetch_round_reference_price_snapshot(
        &self,
        round: &RoundDescriptor,
    ) -> Result<Option<RoundReferencePriceSnapshot>> {
        let Some(market) = self.fetch_market_by_slug(&round.market_slug).await? else {
            return Ok(None);
        };
        Ok(market.reference_price_snapshot())
    }

    pub async fn discover_redeemable_rounds(&self, user: &str) -> Result<Vec<RoundDescriptor>> {
        const PAGE_LIMIT: usize = 100;
        const MAX_OFFSET: usize = 10_000;

        let mut offset = 0usize;
        let mut grouped = HashMap::<String, RedeemableRoundBuilder>::new();

        debug!(
            user,
            page_limit = PAGE_LIMIT,
            max_offset = MAX_OFFSET,
            "scanning redeemable positions for settled rounds"
        );

        loop {
            let positions = self
                .fetch_redeemable_positions(user, PAGE_LIMIT, offset)
                .await?;
            if positions.is_empty() {
                break;
            }

            for position in &positions {
                if position.size <= Decimal::ZERO {
                    continue;
                }

                let Some(condition_id) = position.condition_id.clone() else {
                    continue;
                };

                let builder = grouped
                    .entry(condition_id.clone())
                    .or_insert_with(|| RedeemableRoundBuilder::new(condition_id));
                builder.add(position);
            }

            if positions.len() < PAGE_LIMIT || offset >= MAX_OFFSET {
                break;
            }

            offset += PAGE_LIMIT;
        }

        let mut rounds = grouped
            .into_values()
            .filter_map(|builder| builder.into_round(self.round_interval_secs))
            .collect::<Vec<_>>();
        rounds.sort_by(|left, right| {
            left.settles_at
                .cmp(&right.settles_at)
                .then_with(|| left.opens_at.cmp(&right.opens_at))
                .then_with(|| left.market_slug.cmp(&right.market_slug))
        });
        rounds.dedup_by(|left, right| left.condition_id == right.condition_id);
        Ok(rounds)
    }

    async fn fetch_candidate_markets(
        &self,
        lookahead_secs: u64,
        limit: i32,
    ) -> Result<Vec<GammaMarket>> {
        let now = Utc::now();
        let query = GammaMarketsQuery {
            limit,
            active: Some(true),
            closed: Some(false),
            start_date_max: Some(now + Duration::seconds(lookahead_secs as i64)),
            end_date_min: Some(now - Duration::seconds(self.round_interval_secs as i64)),
            end_date_max: Some(now + Duration::seconds(lookahead_secs as i64)),
        };

        self.fetch_markets(&query).await
    }

    async fn fetch_recent_settled_markets(
        &self,
        lookback_secs: u64,
        limit: usize,
    ) -> Result<Vec<GammaMarket>> {
        let now = Utc::now();
        let query = GammaMarketsQuery {
            limit: limit.clamp(1, 2_000) as i32,
            active: None,
            closed: None,
            start_date_max: None,
            end_date_min: Some(now - Duration::seconds(lookback_secs as i64)),
            end_date_max: Some(now),
        };

        self.fetch_markets(&query).await
    }

    async fn fetch_markets(&self, query: &GammaMarketsQuery) -> Result<Vec<GammaMarket>> {
        let response = self
            .client
            .get(join_url(&self.gamma_base_url, "/markets"))
            .query(query)
            .send()
            .await
            .context("failed to request gamma markets")?
            .error_for_status()
            .context("gamma markets request returned non-success status")?
            .json::<Vec<GammaMarket>>()
            .await
            .context("failed to decode gamma markets response")?;

        debug!(count = response.len(), "fetched gamma candidate markets");
        Ok(response)
    }

    async fn fetch_redeemable_positions(
        &self,
        user: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DataApiPosition>> {
        const MAX_ATTEMPTS: usize = 3;
        let mut last_error = None;

        for attempt in 1..=MAX_ATTEMPTS {
            let response = match self
                .client
                .get(join_url(&self.data_api_base_url, "/positions"))
                .query(&[
                    ("user", user.to_owned()),
                    ("limit", limit.to_string()),
                    ("offset", offset.to_string()),
                    ("sizeThreshold", "0".to_owned()),
                    ("redeemable", "true".to_owned()),
                ])
                .send()
                .await
                .context("failed to request data-api positions")
            {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            max_attempts = MAX_ATTEMPTS,
                            user,
                            offset,
                            ?error,
                            "data-api positions request failed; retrying"
                        );
                        sleep(Duration::milliseconds(250 * attempt as i64).to_std()?).await;
                        continue;
                    }
                    break;
                }
            };

            let response = match response
                .error_for_status()
                .context("data-api positions request returned non-success status")
            {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            max_attempts = MAX_ATTEMPTS,
                            user,
                            offset,
                            ?error,
                            "data-api positions request returned non-success status; retrying"
                        );
                        sleep(Duration::milliseconds(250 * attempt as i64).to_std()?).await;
                        continue;
                    }
                    break;
                }
            };

            let body = match response
                .text()
                .await
                .context("failed to read data-api positions response body")
            {
                Ok(body) => body,
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            max_attempts = MAX_ATTEMPTS,
                            user,
                            offset,
                            ?error,
                            "failed to read data-api positions response body; retrying"
                        );
                        sleep(Duration::milliseconds(250 * attempt as i64).to_std()?).await;
                        continue;
                    }
                    break;
                }
            };

            match serde_json::from_str::<Vec<DataApiPosition>>(&body)
                .context("failed to decode data-api positions response")
            {
                Ok(response) => {
                    debug!(
                        user,
                        offset,
                        count = response.len(),
                        "fetched redeemable positions for proxy wallet"
                    );
                    return Ok(response);
                }
                Err(error) => {
                    let preview = preview_response_body(&body, 240);
                    last_error = Some(format!("{error:#}"));
                    if attempt < MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            max_attempts = MAX_ATTEMPTS,
                            user,
                            offset,
                            body_len = body.len(),
                            body_preview = %preview,
                            ?error,
                            "failed to decode data-api positions response; retrying"
                        );
                        sleep(Duration::milliseconds(250 * attempt as i64).to_std()?).await;
                        continue;
                    }
                    break;
                }
            }
        }

        bail!(
            "failed to fetch redeemable positions after {} attempts (user={}, offset={}): {}",
            MAX_ATTEMPTS,
            user,
            offset,
            last_error.unwrap_or_else(|| "unknown error".to_owned())
        );
    }

    async fn fetch_market_by_slug(&self, slug: &str) -> Result<Option<GammaMarket>> {
        let response = self
            .client
            .get(join_url(
                &self.gamma_base_url,
                &format!("/markets/slug/{slug}"),
            ))
            .send()
            .await
            .with_context(|| format!("failed to request gamma market by slug '{}'", slug))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        response
            .error_for_status()
            .with_context(|| {
                format!(
                    "gamma market-by-slug request returned non-success for '{}'",
                    slug
                )
            })?
            .json::<GammaMarket>()
            .await
            .with_context(|| {
                format!(
                    "failed to decode gamma market-by-slug response for '{}'",
                    slug
                )
            })
            .map(Some)
    }

    fn collect_recent_settled_rounds(
        &self,
        markets: Vec<GammaMarket>,
        now: DateTime<Utc>,
        lookback_secs: u64,
    ) -> Vec<RoundDescriptor> {
        let min_settle = now - Duration::seconds(lookback_secs as i64);
        let matcher = SeriesMatcher::new(&self.config.series_slug);
        let mut rounds = markets
            .into_iter()
            .filter_map(|market| self.market_to_round_allow_closed_book(market))
            .filter(|round| round.settles_at >= min_settle && round.settles_at <= now)
            .filter(|round| matcher.matches(round))
            .collect::<Vec<_>>();

        rounds.sort_by(|left, right| {
            left.settles_at
                .cmp(&right.settles_at)
                .then_with(|| left.opens_at.cmp(&right.opens_at))
                .then_with(|| left.market_slug.cmp(&right.market_slug))
        });

        let mut seen = HashSet::new();
        rounds.retain(|round| seen.insert(round.condition_id.clone()));
        rounds
    }

    fn market_to_round(&self, market: GammaMarket) -> Option<RoundDescriptor> {
        self.market_to_round_impl(market, true)
    }

    fn market_to_round_allow_closed_book(&self, market: GammaMarket) -> Option<RoundDescriptor> {
        self.market_to_round_impl(market, false)
    }

    fn market_to_round_impl(
        &self,
        market: GammaMarket,
        require_order_book: bool,
    ) -> Option<RoundDescriptor> {
        if require_order_book && !market.enable_order_book.unwrap_or(true) {
            return None;
        }

        let settles_at = market.end_date?;
        // Gamma `start_date` for these short-round markets is not reliable enough for scheduling
        // pre-open actions, so derive the opening time from the settlement timestamp.
        let opens_at = settles_at - Duration::seconds(self.round_interval_secs as i64);

        let condition_id = market.condition_id?;
        let token_ids = market.clob_token_ids?;
        if token_ids.len() != 2 {
            return None;
        }

        let question = market
            .question
            .unwrap_or_else(|| market.slug.clone().unwrap_or_default());
        let market_slug = market.slug.unwrap_or_default();

        Some(RoundDescriptor {
            market_id: market.id,
            condition_id,
            market_slug,
            question,
            yes_token_id: token_ids[0].clone(),
            no_token_id: token_ids[1].clone(),
            opens_at,
            settles_at,
        })
    }
}

fn compare_rounds(left: &RoundDescriptor, right: &RoundDescriptor) -> Ordering {
    left.opens_at
        .cmp(&right.opens_at)
        .then_with(|| left.settles_at.cmp(&right.settles_at))
        .then_with(|| left.market_slug.cmp(&right.market_slug))
}

fn is_upcoming_round(round: &RoundDescriptor, now: DateTime<Utc>, max_open: DateTime<Utc>) -> bool {
    round.opens_at > now && round.opens_at <= max_open && round.settles_at > round.opens_at
}

struct SeriesMatcher {
    normalized: String,
    tokens: Vec<String>,
}

impl SeriesMatcher {
    fn new(series_slug: &str) -> Self {
        let normalized = normalize(series_slug);
        let tokens = tokenize(&normalized);

        Self { normalized, tokens }
    }

    fn matches(&self, round: &RoundDescriptor) -> bool {
        let haystacks = [
            normalize(&round.market_slug),
            normalize(&round.question),
            normalize(&round.condition_id),
        ];
        let haystack_tokens = haystacks
            .iter()
            .map(|haystack| tokenize(haystack))
            .collect::<Vec<_>>();

        if haystacks
            .iter()
            .any(|haystack| haystack.contains(&self.normalized))
        {
            return true;
        }

        self.tokens.iter().all(|token| {
            haystack_tokens.iter().any(|tokens| {
                !token.is_empty() && tokens.iter().any(|candidate| candidate == token)
            })
        })
    }
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
}

fn tokenize(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect()
}

fn strip_timestamp_suffix(slug: &str) -> Option<String> {
    let (prefix, suffix) = slug.rsplit_once('-')?;
    if suffix.chars().all(|ch| ch.is_ascii_digit()) && suffix.len() >= 10 {
        return Some(prefix.to_owned());
    }
    None
}

fn align_down_to_round(unix_secs: i64, round_interval_secs: u64) -> i64 {
    let interval = round_interval_secs.max(1) as i64;
    unix_secs - unix_secs.rem_euclid(interval)
}

fn preview_response_body(body: &str, max_chars: usize) -> String {
    let mut preview = body.chars().take(max_chars).collect::<String>();
    if body.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n")
}

#[derive(Debug, Default, serde::Serialize)]
struct GammaMarketsQuery {
    limit: i32,
    active: Option<bool>,
    closed: Option<bool>,
    start_date_max: Option<DateTime<Utc>>,
    end_date_min: Option<DateTime<Utc>>,
    end_date_max: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
    question: Option<String>,
    description: Option<String>,
    slug: Option<String>,
    condition_id: Option<String>,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    active: Option<bool>,
    closed: Option<bool>,
    enable_order_book: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_token_ids")]
    clob_token_ids: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    outcomes: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_decimal_vec")]
    outcome_prices: Option<Vec<Decimal>>,
    #[serde(default)]
    uma_resolution_status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    current_value: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    current_price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    current_px: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    open_value: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    open_price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    open_px: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    x_axis_value: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    y_axis_value: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    seconds_delay_current_value: Option<Decimal>,
    #[serde(default)]
    updated_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl GammaMarket {
    fn resolved_outcome(&self) -> Option<ResolvedRoundOutcome> {
        let prices = self.outcome_prices.as_ref()?;
        if prices.len() < 2 {
            return None;
        }

        let status_resolved = self
            .uma_resolution_status
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case("resolved"))
            .unwrap_or(false);
        let payout_resolved = prices.iter().any(|price| *price == Decimal::ONE);
        if !status_resolved && !payout_resolved {
            return None;
        }

        Some(ResolvedRoundOutcome {
            yes_payout: prices[0],
            no_payout: prices[1],
        })
    }

    fn reference_price_snapshot(&self) -> Option<RoundReferencePriceSnapshot> {
        let parsed_question_price = self
            .question
            .as_deref()
            .and_then(extract_first_price_from_text);
        let parsed_description_price = self
            .description
            .as_deref()
            .and_then(extract_first_price_from_text);

        let candidate_open_prices = [
            self.open_value.map(|price| ("openValue", price)),
            self.open_price.map(|price| ("openPrice", price)),
            self.open_px.map(|price| ("openPx", price)),
            parsed_question_price.map(|price| ("question", price)),
            parsed_description_price.map(|price| ("description", price)),
            self.x_axis_value.map(|price| ("xAxisValue", price)),
            self.y_axis_value.map(|price| ("yAxisValue", price)),
            find_extra_decimal(&self.extra, &["openValue", "openPrice", "openPx"]),
        ];
        let (open_source, open_price) = first_decimal_candidate(&candidate_open_prices)?;

        let candidate_current_prices = [
            self.current_value.map(|price| ("currentValue", price)),
            self.current_price.map(|price| ("currentPrice", price)),
            self.current_px.map(|price| ("currentPx", price)),
            self.seconds_delay_current_value
                .map(|price| ("secondsDelayCurrentValue", price)),
            axis_candidate("xAxisValue", self.x_axis_value, open_price),
            axis_candidate("yAxisValue", self.y_axis_value, open_price),
            find_extra_decimal(
                &self.extra,
                &[
                    "currentValue",
                    "currentPrice",
                    "currentPx",
                    "secondsDelayCurrentValue",
                ],
            ),
        ];
        let (current_source, current_price) = first_decimal_candidate(&candidate_current_prices)?;

        Some(RoundReferencePriceSnapshot {
            open_price,
            current_price,
            open_source: open_source.to_owned(),
            current_source: current_source.to_owned(),
            fetched_at: self.updated_at.unwrap_or_else(Utc::now),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataApiPosition {
    asset: String,
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    size: Decimal,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    opposite_asset: Option<String>,
    #[serde(default)]
    end_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct RedeemableRoundBuilder {
    condition_id: String,
    question: Option<String>,
    market_slug: Option<String>,
    settles_at: Option<DateTime<Utc>>,
    yes_token_id: Option<String>,
    no_token_id: Option<String>,
}

impl RedeemableRoundBuilder {
    fn new(condition_id: String) -> Self {
        Self {
            condition_id,
            ..Self::default()
        }
    }

    fn add(&mut self, position: &DataApiPosition) {
        if self.question.is_none() {
            self.question = position
                .title
                .clone()
                .filter(|value| !value.trim().is_empty());
        }
        if self.market_slug.is_none() {
            self.market_slug = position
                .slug
                .clone()
                .filter(|value| !value.trim().is_empty());
        }
        if self.settles_at.is_none() {
            self.settles_at = position.end_date;
        }

        let opposite = position
            .opposite_asset
            .clone()
            .filter(|value| !value.trim().is_empty());
        let normalized_outcome = position
            .outcome
            .as_deref()
            .map(normalize)
            .unwrap_or_default();

        match normalized_outcome.as_str() {
            "yes" => {
                self.yes_token_id = Some(position.asset.clone());
                if self.no_token_id.is_none() {
                    self.no_token_id = opposite;
                }
            }
            "no" => {
                self.no_token_id = Some(position.asset.clone());
                if self.yes_token_id.is_none() {
                    self.yes_token_id = opposite;
                }
            }
            _ => {
                if self.yes_token_id.is_none() {
                    self.yes_token_id = Some(position.asset.clone());
                }
                if self.no_token_id.is_none() {
                    self.no_token_id = opposite;
                }
            }
        }
    }

    fn into_round(self, round_interval_secs: u64) -> Option<RoundDescriptor> {
        let settles_at = self.settles_at?;
        let yes_token_id = self.yes_token_id?;
        let no_token_id = self.no_token_id?;
        let market_slug = self
            .market_slug
            .unwrap_or_else(|| self.condition_id.clone());
        let question = self.question.unwrap_or_else(|| market_slug.clone());

        Some(RoundDescriptor {
            market_id: self.condition_id.clone(),
            condition_id: self.condition_id,
            market_slug,
            question,
            yes_token_id,
            no_token_id,
            opens_at: settles_at - Duration::seconds(round_interval_secs as i64),
            settles_at,
        })
    }
}

fn deserialize_token_ids<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<serde_json::Value>::deserialize(deserializer)?;

    let Some(value) = raw else {
        return Ok(None);
    };

    let ids = match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => {
            let parsed = serde_json::from_str::<Vec<String>>(&text).unwrap_or_else(|_| {
                text.split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned)
                    .collect()
            });
            Some(parsed)
        }
        serde_json::Value::Array(items) => {
            let parsed = items
                .into_iter()
                .filter_map(|item| match item {
                    serde_json::Value::String(text) => Some(text),
                    serde_json::Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Some(parsed)
        }
        other => {
            return Err(serde::de::Error::custom(format!(
                "unexpected clobTokenIds payload: {other}"
            )));
        }
    };

    Ok(ids.filter(|items| !items.is_empty()))
}

fn deserialize_string_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<serde_json::Value>::deserialize(deserializer)?;

    let Some(value) = raw else {
        return Ok(None);
    };

    let values = match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => {
            let parsed = serde_json::from_str::<Vec<String>>(&text).unwrap_or_else(|_| {
                text.split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned)
                    .collect()
            });
            Some(parsed)
        }
        serde_json::Value::Array(items) => Some(
            items
                .into_iter()
                .filter_map(|item| match item {
                    serde_json::Value::String(text) => Some(text),
                    serde_json::Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ),
        other => {
            return Err(serde::de::Error::custom(format!(
                "unexpected string-array payload: {other}"
            )));
        }
    };

    Ok(values.filter(|items| !items.is_empty()))
}

fn deserialize_decimal_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<Decimal>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let strings = deserialize_string_vec(deserializer)?;
    strings
        .map(|items| {
            items
                .into_iter()
                .map(|item| item.parse::<Decimal>().map_err(serde::de::Error::custom))
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .transpose()
}

fn deserialize_optional_decimal<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = raw else {
        return Ok(None);
    };

    match value {
        Value::Null => Ok(None),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<Decimal>()
                .map(Some)
                .map_err(serde::de::Error::custom)
        }
        Value::Number(number) => number
            .to_string()
            .parse::<Decimal>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        other => Err(serde::de::Error::custom(format!(
            "unexpected decimal payload: {other}"
        ))),
    }
}

fn first_decimal_candidate<'a>(
    candidates: &'a [Option<(&'a str, Decimal)>],
) -> Option<(&'a str, Decimal)> {
    candidates
        .iter()
        .flatten()
        .copied()
        .find(|(_, price)| *price > Decimal::ZERO)
}

fn axis_candidate<'a>(
    label: &'a str,
    candidate: Option<Decimal>,
    open_price: Decimal,
) -> Option<(&'a str, Decimal)> {
    let price = candidate?;
    if price <= Decimal::ZERO || price == open_price {
        return None;
    }
    Some((label, price))
}

fn find_extra_decimal<'a>(
    extra: &'a HashMap<String, Value>,
    keys: &[&'a str],
) -> Option<(&'a str, Decimal)> {
    for key in keys {
        let Some(value) = extra.get(*key) else {
            continue;
        };
        let parsed = match value {
            Value::String(text) => text.trim().parse::<Decimal>().ok(),
            Value::Number(number) => number.to_string().parse::<Decimal>().ok(),
            _ => None,
        };
        if let Some(price) = parsed.filter(|price| *price > Decimal::ZERO) {
            return Some((key, price));
        }
    }
    None
}

fn extract_first_price_from_text(text: &str) -> Option<Decimal> {
    let mut current = String::new();
    let mut saw_digit = false;
    let mut saw_currency = false;

    for ch in text.chars() {
        match ch {
            '$' => {
                current.clear();
                saw_digit = false;
                saw_currency = true;
            }
            '0'..='9' => {
                current.push(ch);
                saw_digit = true;
            }
            ',' | '.' if saw_currency || saw_digit => current.push(ch),
            _ => {
                if saw_digit {
                    break;
                }
                current.clear();
                saw_currency = false;
            }
        }
    }

    if !saw_digit {
        return None;
    }

    current.retain(|ch| ch != ',');
    current
        .parse::<Decimal>()
        .ok()
        .filter(|value| *value > Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_with_slug(slug: &str) -> RoundDescriptor {
        let now = Utc::now();
        RoundDescriptor {
            market_id: "test-market".to_owned(),
            condition_id: "test-condition".to_owned(),
            market_slug: slug.to_owned(),
            question: slug.to_owned(),
            yes_token_id: "1".to_owned(),
            no_token_id: "2".to_owned(),
            opens_at: now,
            settles_at: now + Duration::seconds(300),
        }
    }

    #[test]
    fn series_matcher_matches_exact_5m_token() {
        let matcher = SeriesMatcher::new("btc-5m");
        assert!(matcher.matches(&round_with_slug("btc-updown-5m-1775880000")));
    }

    #[test]
    fn series_matcher_does_not_match_15m_when_searching_5m() {
        let matcher = SeriesMatcher::new("btc-5m");
        assert!(!matcher.matches(&round_with_slug("btc-updown-15m-1775888100")));
    }

    #[test]
    fn upcoming_round_filter_rejects_already_open_rounds() {
        let now = Utc::now();
        let round = RoundDescriptor {
            market_id: "current-round".to_owned(),
            condition_id: "cond".to_owned(),
            market_slug: "btc-updown-5m-current".to_owned(),
            question: "btc".to_owned(),
            yes_token_id: "1".to_owned(),
            no_token_id: "2".to_owned(),
            opens_at: now - Duration::seconds(30),
            settles_at: now + Duration::seconds(270),
        };

        assert!(!is_upcoming_round(
            &round,
            now,
            now + Duration::seconds(600)
        ));
    }

    #[test]
    fn upcoming_round_filter_accepts_next_round_before_open() {
        let now = Utc::now();
        let round = RoundDescriptor {
            market_id: "next-round".to_owned(),
            condition_id: "cond".to_owned(),
            market_slug: "btc-updown-5m-next".to_owned(),
            question: "btc".to_owned(),
            yes_token_id: "1".to_owned(),
            no_token_id: "2".to_owned(),
            opens_at: now + Duration::seconds(120),
            settles_at: now + Duration::seconds(420),
        };

        assert!(is_upcoming_round(&round, now, now + Duration::seconds(600)));
    }

    #[test]
    fn market_to_round_uses_settlement_minus_interval_for_open_time() {
        let network = crate::config::NetworkConfig::default();
        let market = MarketConfig::default();
        let service = MarketDiscoveryService::new(&network, &market, 300).expect("service");
        let settles_at = Utc::now() + Duration::seconds(420);
        let stale_start = settles_at - Duration::days(1);

        let round = service
            .market_to_round(GammaMarket {
                id: "gamma-id".to_owned(),
                question: Some("BTC".to_owned()),
                slug: Some("btc-updown-5m-test".to_owned()),
                condition_id: Some("cond".to_owned()),
                start_date: Some(stale_start),
                end_date: Some(settles_at),
                active: Some(true),
                closed: Some(false),
                enable_order_book: Some(true),
                clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                outcomes: None,
                outcome_prices: None,
                uma_resolution_status: None,
                ..Default::default()
            })
            .expect("round");

        assert_eq!(round.opens_at, settles_at - Duration::seconds(300));
    }

    #[test]
    fn collect_recent_settled_rounds_filters_and_dedupes() {
        let network = crate::config::NetworkConfig::default();
        let market = MarketConfig::default();
        let service = MarketDiscoveryService::new(&network, &market, 300).expect("service");
        let now = Utc::now();

        let rounds = service.collect_recent_settled_rounds(
            vec![
                GammaMarket {
                    id: "old".to_owned(),
                    question: Some("BTC".to_owned()),
                    slug: Some("btc-updown-5m-old".to_owned()),
                    condition_id: Some("old".to_owned()),
                    start_date: None,
                    end_date: Some(now - Duration::hours(8)),
                    active: Some(false),
                    closed: Some(true),
                    enable_order_book: Some(true),
                    clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                    outcomes: None,
                    outcome_prices: None,
                    uma_resolution_status: None,
                    ..Default::default()
                },
                GammaMarket {
                    id: "recent-a".to_owned(),
                    question: Some("BTC".to_owned()),
                    slug: Some("btc-updown-5m-a".to_owned()),
                    condition_id: Some("dup".to_owned()),
                    start_date: None,
                    end_date: Some(now - Duration::minutes(10)),
                    active: Some(false),
                    closed: Some(true),
                    enable_order_book: Some(true),
                    clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                    outcomes: None,
                    outcome_prices: None,
                    uma_resolution_status: None,
                    ..Default::default()
                },
                GammaMarket {
                    id: "recent-b".to_owned(),
                    question: Some("BTC".to_owned()),
                    slug: Some("btc-updown-5m-b".to_owned()),
                    condition_id: Some("dup".to_owned()),
                    start_date: None,
                    end_date: Some(now - Duration::minutes(5)),
                    active: Some(false),
                    closed: Some(true),
                    enable_order_book: Some(true),
                    clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                    outcomes: None,
                    outcome_prices: None,
                    uma_resolution_status: None,
                    ..Default::default()
                },
                GammaMarket {
                    id: "other-series".to_owned(),
                    question: Some("ETH".to_owned()),
                    slug: Some("eth-updown-5m-a".to_owned()),
                    condition_id: Some("eth".to_owned()),
                    start_date: None,
                    end_date: Some(now - Duration::minutes(5)),
                    active: Some(false),
                    closed: Some(true),
                    enable_order_book: Some(true),
                    clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                    outcomes: None,
                    outcome_prices: None,
                    uma_resolution_status: None,
                    ..Default::default()
                },
            ],
            now,
            3600,
        );

        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].condition_id, "dup");
        assert_eq!(rounds[0].market_slug, "btc-updown-5m-a");
    }

    #[test]
    fn collect_recent_settled_rounds_keeps_closed_book_markets_for_redeem_scan() {
        let network = crate::config::NetworkConfig::default();
        let market = MarketConfig::default();
        let service = MarketDiscoveryService::new(&network, &market, 300).expect("service");
        let now = Utc::now();

        let rounds = service.collect_recent_settled_rounds(
            vec![GammaMarket {
                id: "recent-closed-book".to_owned(),
                question: Some("BTC".to_owned()),
                slug: Some("btc-updown-5m-closed-book".to_owned()),
                condition_id: Some("closed-book".to_owned()),
                start_date: None,
                end_date: Some(now - Duration::minutes(5)),
                active: Some(false),
                closed: Some(true),
                enable_order_book: Some(false),
                clob_token_ids: Some(vec!["1".to_owned(), "2".to_owned()]),
                outcomes: None,
                outcome_prices: None,
                uma_resolution_status: None,
                ..Default::default()
            }],
            now,
            3600,
        );

        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].condition_id, "closed-book");
    }

    #[test]
    fn redeemable_round_builder_maps_yes_and_no_assets() {
        let settles_at = Utc::now() + Duration::minutes(5);
        let mut builder = RedeemableRoundBuilder::new("cond-1".to_owned());

        builder.add(&DataApiPosition {
            asset: "yes-asset".to_owned(),
            condition_id: Some("cond-1".to_owned()),
            size: Decimal::ONE,
            title: Some("BTC up/down".to_owned()),
            slug: Some("btc-updown-5m-1".to_owned()),
            outcome: Some("Yes".to_owned()),
            opposite_asset: Some("no-asset".to_owned()),
            end_date: Some(settles_at),
        });
        builder.add(&DataApiPosition {
            asset: "no-asset".to_owned(),
            condition_id: Some("cond-1".to_owned()),
            size: Decimal::ONE,
            title: Some("BTC up/down".to_owned()),
            slug: Some("btc-updown-5m-1".to_owned()),
            outcome: Some("No".to_owned()),
            opposite_asset: Some("yes-asset".to_owned()),
            end_date: Some(settles_at),
        });

        let round = builder.into_round(300).expect("round");
        assert_eq!(round.yes_token_id, "yes-asset");
        assert_eq!(round.no_token_id, "no-asset");
        assert_eq!(round.market_slug, "btc-updown-5m-1");
        assert_eq!(round.opens_at, settles_at - Duration::seconds(300));
    }

    #[test]
    fn strip_timestamp_suffix_extracts_market_prefix() {
        assert_eq!(
            strip_timestamp_suffix("btc-updown-5m-1776099000").as_deref(),
            Some("btc-updown-5m")
        );
        assert_eq!(strip_timestamp_suffix("btc-updown-5m"), None);
    }

    #[test]
    fn align_down_to_round_snaps_to_interval_boundary() {
        assert_eq!(align_down_to_round(1776099123, 300), 1776099000);
        assert_eq!(align_down_to_round(1776099000, 300), 1776099000);
    }

    #[test]
    fn reference_price_snapshot_prefers_explicit_open_and_current_fields() {
        let market = GammaMarket {
            question: Some("Will BTC be above $95,000 at close?".to_owned()),
            open_px: Some(Decimal::new(95_000, 0)),
            current_px: Some(Decimal::new(95_032, 0)),
            ..Default::default()
        };

        let snapshot = market.reference_price_snapshot().expect("snapshot");
        assert_eq!(snapshot.open_price, Decimal::new(95_000, 0));
        assert_eq!(snapshot.current_price, Decimal::new(95_032, 0));
        assert_eq!(snapshot.open_source, "openPx");
        assert_eq!(snapshot.current_source, "currentPx");
    }

    #[test]
    fn reference_price_snapshot_can_fall_back_to_question_and_axis_values() {
        let market = GammaMarket {
            question: Some("Will BTC be above $95,000 at close?".to_owned()),
            x_axis_value: Some(Decimal::new(95_000, 0)),
            y_axis_value: Some(Decimal::new(95_041, 0)),
            ..Default::default()
        };

        let snapshot = market.reference_price_snapshot().expect("snapshot");
        assert_eq!(snapshot.open_price, Decimal::new(95_000, 0));
        assert_eq!(snapshot.current_price, Decimal::new(95_041, 0));
        assert_eq!(snapshot.open_source, "question");
        assert_eq!(snapshot.current_source, "yAxisValue");
    }
}
