use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::{
    execution::{ExecutionGateway, OrderBookLevel, OrderBookResponse},
    market::RoundDescriptor,
};

const MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const WS_HEARTBEAT_INTERVAL_SECS: u64 = 10;
const WS_RECONNECT_DELAY_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy)]
pub struct RealtimeBestQuote {
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RealtimeOrderUpdate {
    pub market: String,
    pub order_id: String,
    pub status: String,
    pub size_matched: Decimal,
    pub original_size: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct RealtimeMarketUpdate {
    pub asset_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DesiredSubscriptions {
    markets: Vec<String>,
    asset_ids: Vec<String>,
}

pub struct ReactiveRealtimeFeed {
    desired_tx: watch::Sender<DesiredSubscriptions>,
    order_rx: mpsc::UnboundedReceiver<RealtimeOrderUpdate>,
    market_rx: mpsc::UnboundedReceiver<RealtimeMarketUpdate>,
    best_quotes: Arc<RwLock<HashMap<String, RealtimeBestQuote>>>,
    order_books: Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    last_sent: DesiredSubscriptions,
}

impl ReactiveRealtimeFeed {
    pub fn spawn(gateway: Arc<ExecutionGateway>) -> Self {
        let (desired_tx, desired_rx) = watch::channel(DesiredSubscriptions::default());
        let (order_tx, order_rx) = mpsc::unbounded_channel();
        let (market_tx, market_rx) = mpsc::unbounded_channel();
        let best_quotes = Arc::new(RwLock::new(HashMap::new()));
        let order_books = Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(run_user_ws_loop(
            gateway.account().credentials.clone(),
            desired_rx.clone(),
            order_tx,
        ));
        tokio::spawn(run_market_ws_loop(
            desired_rx,
            best_quotes.clone(),
            order_books.clone(),
            market_tx,
        ));

        Self {
            desired_tx,
            order_rx,
            market_rx,
            best_quotes,
            order_books,
            last_sent: DesiredSubscriptions::default(),
        }
    }

    pub fn spawn_market_only() -> Self {
        let (desired_tx, desired_rx) = watch::channel(DesiredSubscriptions::default());
        let (_order_tx, order_rx) = mpsc::unbounded_channel();
        let (market_tx, market_rx) = mpsc::unbounded_channel();
        let best_quotes = Arc::new(RwLock::new(HashMap::new()));
        let order_books = Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(run_market_ws_loop(
            desired_rx,
            best_quotes.clone(),
            order_books.clone(),
            market_tx,
        ));

        Self {
            desired_tx,
            order_rx,
            market_rx,
            best_quotes,
            order_books,
            last_sent: DesiredSubscriptions::default(),
        }
    }

    pub fn sync_rounds<'a, I>(&mut self, rounds: I)
    where
        I: IntoIterator<Item = &'a RoundDescriptor>,
    {
        let mut markets = Vec::new();
        let mut asset_ids = Vec::new();

        for round in rounds {
            markets.push(round.condition_id.clone());
            asset_ids.push(round.yes_token_id.clone());
            asset_ids.push(round.no_token_id.clone());
        }

        markets.sort();
        markets.dedup();
        asset_ids.sort();
        asset_ids.dedup();

        let desired = DesiredSubscriptions { markets, asset_ids };
        if desired != self.last_sent {
            let _ = self.desired_tx.send(desired.clone());
            self.last_sent = desired;
        }
    }

    pub fn drain_order_updates(&mut self) -> Vec<RealtimeOrderUpdate> {
        let mut updates = Vec::new();
        while let Ok(update) = self.order_rx.try_recv() {
            updates.push(update);
        }
        updates
    }

    pub fn drain_market_updates(&mut self) -> Vec<RealtimeMarketUpdate> {
        let mut updates = Vec::new();
        while let Ok(update) = self.market_rx.try_recv() {
            updates.push(update);
        }
        updates
    }

    pub async fn best_quote(&self, asset_id: &str) -> Option<RealtimeBestQuote> {
        self.best_quotes.read().await.get(asset_id).copied()
    }

    pub async fn order_book(&self, asset_id: &str) -> Option<OrderBookResponse> {
        self.order_books
            .read()
            .await
            .get(asset_id)
            .map(|book| book.to_order_book_response(asset_id))
    }

    pub async fn wait_for_market_update(
        &mut self,
        timeout: Duration,
    ) -> Option<RealtimeMarketUpdate> {
        tokio::time::timeout(timeout, self.market_rx.recv())
            .await
            .ok()
            .flatten()
    }
}

async fn run_user_ws_loop(
    credentials: crate::execution::ApiCredentials,
    mut desired_rx: watch::Receiver<DesiredSubscriptions>,
    order_tx: mpsc::UnboundedSender<RealtimeOrderUpdate>,
) {
    loop {
        let desired = desired_rx.borrow().clone();
        if desired.markets.is_empty() {
            if desired_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        match connect_async(USER_WS_URL).await {
            Ok((mut ws, _)) => {
                info!(
                    market_count = desired.markets.len(),
                    "connected reactive user ws"
                );

                let auth = UserSubscriptionRequest {
                    auth: UserSubscriptionAuth {
                        api_key: credentials.api_key.clone(),
                        secret: credentials.secret.clone(),
                        passphrase: credentials.passphrase.clone(),
                    },
                    kind: "user",
                };
                if let Err(error) = send_json(&mut ws, &auth).await {
                    warn!(?error, "failed to send reactive user ws auth request");
                    tokio::time::sleep(Duration::from_millis(WS_RECONNECT_DELAY_MS)).await;
                    continue;
                }

                let subscribe = UserSubscriptionUpdate {
                    operation: "subscribe",
                    markets: desired.markets.clone(),
                };
                if let Err(error) = send_json(&mut ws, &subscribe).await {
                    warn!(
                        ?error,
                        "failed to send reactive user ws market subscriptions"
                    );
                    tokio::time::sleep(Duration::from_millis(WS_RECONNECT_DELAY_MS)).await;
                    continue;
                }

                let mut heartbeat =
                    tokio::time::interval(Duration::from_secs(WS_HEARTBEAT_INTERVAL_SECS));
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(error) = ws.send(Message::Text("{}".into())).await {
                                warn!(?error, "reactive user ws heartbeat failed");
                                break;
                            }
                        }
                        changed = desired_rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            break;
                        }
                        message = ws.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(error) = handle_user_message(text.as_ref(), &order_tx) {
                                        debug!(?error, "ignored reactive user ws payload");
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = ws.send(Message::Pong(payload)).await {
                                        warn!(?error, "reactive user ws pong failed");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    debug!(?frame, "reactive user ws closed");
                                    break;
                                }
                                Some(Ok(_)) => {}
                                Some(Err(error)) => {
                                    warn!(?error, "reactive user ws receive failed");
                                    break;
                                }
                                None => break,
                            }
                        }
                    }
                }
            }
            Err(error) => warn!(?error, "failed to connect reactive user ws"),
        }

        tokio::time::sleep(Duration::from_millis(WS_RECONNECT_DELAY_MS)).await;
    }
}

async fn run_market_ws_loop(
    mut desired_rx: watch::Receiver<DesiredSubscriptions>,
    best_quotes: Arc<RwLock<HashMap<String, RealtimeBestQuote>>>,
    order_books: Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    market_tx: mpsc::UnboundedSender<RealtimeMarketUpdate>,
) {
    loop {
        let desired = desired_rx.borrow().clone();
        if desired.asset_ids.is_empty() {
            if desired_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        match connect_async(MARKET_WS_URL).await {
            Ok((mut ws, _)) => {
                info!(
                    asset_count = desired.asset_ids.len(),
                    "connected reactive market ws"
                );
                let subscribe = MarketSubscriptionRequest {
                    asset_ids: desired.asset_ids.clone(),
                    kind: "market",
                    custom_feature_enabled: true,
                };
                if let Err(error) = send_json(&mut ws, &subscribe).await {
                    warn!(?error, "failed to send reactive market ws subscriptions");
                    tokio::time::sleep(Duration::from_millis(WS_RECONNECT_DELAY_MS)).await;
                    continue;
                }

                let mut heartbeat =
                    tokio::time::interval(Duration::from_secs(WS_HEARTBEAT_INTERVAL_SECS));
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(error) = ws.send(Message::Text("{}".into())).await {
                                warn!(?error, "reactive market ws heartbeat failed");
                                break;
                            }
                        }
                        changed = desired_rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            break;
                        }
                        message = ws.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(error) = handle_market_message(
                                        text.as_ref(),
                                        &best_quotes,
                                        &order_books,
                                        &market_tx,
                                    )
                                    .await
                                    {
                                        debug!(?error, "ignored reactive market ws payload");
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = ws.send(Message::Pong(payload)).await {
                                        warn!(?error, "reactive market ws pong failed");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    debug!(?frame, "reactive market ws closed");
                                    break;
                                }
                                Some(Ok(_)) => {}
                                Some(Err(error)) => {
                                    warn!(?error, "reactive market ws receive failed");
                                    break;
                                }
                                None => break,
                            }
                        }
                    }
                }
            }
            Err(error) => warn!(?error, "failed to connect reactive market ws"),
        }

        tokio::time::sleep(Duration::from_millis(WS_RECONNECT_DELAY_MS)).await;
    }
}

async fn send_json<S, T>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    value: &T,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: serde::Serialize,
{
    ws.send(Message::Text(serde_json::to_string(value)?.into()))
        .await?;
    Ok(())
}

fn handle_user_message(
    payload: &str,
    order_tx: &mpsc::UnboundedSender<RealtimeOrderUpdate>,
) -> anyhow::Result<()> {
    let trimmed = payload.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok(());
    }

    let value = serde_json::from_str::<serde_json::Value>(trimmed)?;
    let Some(event_type) = value.get("event_type").and_then(|value| value.as_str()) else {
        return Ok(());
    };

    if event_type != "order" {
        return Ok(());
    }

    let event = serde_json::from_value::<UserOrderEvent>(value)?;
    if event.size_matched <= Decimal::ZERO && !status_implies_filled(&event.status) {
        return Ok(());
    }

    let _ = order_tx.send(RealtimeOrderUpdate {
        market: event.market,
        order_id: event.id,
        status: event.status,
        size_matched: event.size_matched,
        original_size: event.original_size,
    });

    Ok(())
}

async fn handle_market_message(
    payload: &str,
    best_quotes: &Arc<RwLock<HashMap<String, RealtimeBestQuote>>>,
    order_books: &Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    market_tx: &mpsc::UnboundedSender<RealtimeMarketUpdate>,
) -> anyhow::Result<()> {
    let trimmed = payload.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok(());
    }

    let value = serde_json::from_str::<serde_json::Value>(trimmed)?;
    let Some(event_type) = value.get("event_type").and_then(|value| value.as_str()) else {
        return Ok(());
    };

    match event_type {
        "book" => {
            let event = serde_json::from_value::<BookEvent>(value)?;
            replace_order_book(order_books, &event).await;
            publish_market_update(best_quotes, order_books, market_tx, &event.asset_id).await;
        }
        "best_bid_ask" => {
            let event = serde_json::from_value::<BestBidAskEvent>(value)?;
            update_best_quote(best_quotes, &event.asset_id, event.best_bid, event.best_ask).await;
        }
        "price_change" => {
            let event = serde_json::from_value::<PriceChangeEvent>(value)?;
            for change in event.price_changes {
                apply_price_change(order_books, &change).await;
                publish_market_update(best_quotes, order_books, market_tx, &change.asset_id).await;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn update_best_quote(
    best_quotes: &Arc<RwLock<HashMap<String, RealtimeBestQuote>>>,
    asset_id: &str,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
) {
    best_quotes.write().await.insert(
        asset_id.to_owned(),
        RealtimeBestQuote {
            best_bid,
            best_ask,
            updated_at: Utc::now(),
        },
    );
}

async fn replace_order_book(
    order_books: &Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    event: &BookEvent,
) {
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();

    for level in &event.bids {
        if let (Some(price), Some(size)) = (level.price, level.size) {
            if price > Decimal::ZERO && size > Decimal::ZERO {
                bids.insert(price, size);
            }
        }
    }

    for level in &event.asks {
        if let (Some(price), Some(size)) = (level.price, level.size) {
            if price > Decimal::ZERO && size > Decimal::ZERO {
                asks.insert(price, size);
            }
        }
    }

    order_books.write().await.insert(
        event.asset_id.clone(),
        RealtimeOrderBook {
            bids,
            asks,
            updated_at: Utc::now(),
        },
    );
}

async fn apply_price_change(
    order_books: &Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    change: &PriceChangeItem,
) {
    let Some(side) = change.side.as_deref().and_then(parse_price_change_side) else {
        return;
    };
    let Some(price) = change.price else {
        return;
    };
    let size = change.size.unwrap_or(Decimal::ZERO);
    let now = Utc::now();

    let mut books = order_books.write().await;
    let book = books
        .entry(change.asset_id.clone())
        .or_insert_with(RealtimeOrderBook::default);
    book.updated_at = now;

    let levels = match side {
        PriceChangeSide::Bid => &mut book.bids,
        PriceChangeSide::Ask => &mut book.asks,
    };

    if size <= Decimal::ZERO {
        levels.remove(&price);
    } else {
        levels.insert(price, size);
    }
}

async fn publish_market_update(
    best_quotes: &Arc<RwLock<HashMap<String, RealtimeBestQuote>>>,
    order_books: &Arc<RwLock<HashMap<String, RealtimeOrderBook>>>,
    market_tx: &mpsc::UnboundedSender<RealtimeMarketUpdate>,
    asset_id: &str,
) {
    let (best_bid, best_ask, updated_at) = {
        let books = order_books.read().await;
        let Some(book) = books.get(asset_id) else {
            return;
        };
        (book.best_bid(), book.best_ask(), book.updated_at)
    };

    update_best_quote(best_quotes, asset_id, best_bid, best_ask).await;
    let _ = market_tx.send(RealtimeMarketUpdate {
        asset_id: asset_id.to_owned(),
        updated_at,
    });
}

fn status_implies_filled(status: &str) -> bool {
    let normalized = status.trim().to_ascii_lowercase();
    normalized.contains("matched") || normalized.contains("filled")
}

fn deserialize_decimal_option<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|text| Decimal::from_str(text.trim()).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Decimal::from_str(value.trim()).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
struct UserOrderEvent {
    id: String,
    market: String,
    status: String,
    #[serde(default, deserialize_with = "deserialize_decimal")]
    size_matched: Decimal,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    original_size: Option<Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
struct BookEvent {
    asset_id: String,
    #[serde(default)]
    bids: Vec<OrderBookLevel>,
    #[serde(default)]
    asks: Vec<OrderBookLevel>,
}

#[derive(Debug, Clone, Deserialize)]
struct BestBidAskEvent {
    asset_id: String,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    best_ask: Option<Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
struct PriceChangeEvent {
    price_changes: Vec<PriceChangeItem>,
}

#[derive(Debug, Clone, Deserialize)]
struct PriceChangeItem {
    asset_id: String,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    size: Option<Decimal>,
    #[serde(default)]
    side: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RealtimeOrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    updated_at: DateTime<Utc>,
}

impl RealtimeOrderBook {
    fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    fn to_order_book_response(&self, asset_id: &str) -> OrderBookResponse {
        let bids = self
            .bids
            .iter()
            .rev()
            .map(|(price, size)| OrderBookLevel {
                price: Some(*price),
                size: Some(*size),
            })
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .map(|(price, size)| OrderBookLevel {
                price: Some(*price),
                size: Some(*size),
            })
            .collect::<Vec<_>>();

        OrderBookResponse {
            asset_id: Some(asset_id.to_owned()),
            market: None,
            hash: None,
            timestamp: Some(self.updated_at.to_rfc3339()),
            bids,
            asks,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PriceChangeSide {
    Bid,
    Ask,
}

fn parse_price_change_side(value: &str) -> Option<PriceChangeSide> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "bid" | "buy" | "bids" => Some(PriceChangeSide::Bid),
        "ask" | "sell" | "asks" => Some(PriceChangeSide::Ask),
        _ => None,
    }
}

#[derive(Debug, serde::Serialize)]
struct MarketSubscriptionRequest<'a> {
    #[serde(rename = "assets_ids")]
    asset_ids: Vec<String>,
    #[serde(rename = "type")]
    kind: &'a str,
    custom_feature_enabled: bool,
}

#[derive(Debug, serde::Serialize)]
struct UserSubscriptionRequest {
    auth: UserSubscriptionAuth,
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct UserSubscriptionAuth {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

#[derive(Debug, serde::Serialize)]
struct UserSubscriptionUpdate<'a> {
    operation: &'a str,
    markets: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_price_change_side_accepts_bid_and_ask_variants() {
        assert!(matches!(
            parse_price_change_side("BUY"),
            Some(PriceChangeSide::Bid)
        ));
        assert!(matches!(
            parse_price_change_side("ask"),
            Some(PriceChangeSide::Ask)
        ));
        assert!(parse_price_change_side("unknown").is_none());
    }

    #[test]
    fn realtime_order_book_converts_back_to_sorted_response() {
        let mut book = RealtimeOrderBook::default();
        book.updated_at = Utc::now();
        book.bids
            .insert(Decimal::from_str("0.48").unwrap(), Decimal::from(4));
        book.bids
            .insert(Decimal::from_str("0.50").unwrap(), Decimal::from(2));
        book.asks
            .insert(Decimal::from_str("0.53").unwrap(), Decimal::from(3));
        book.asks
            .insert(Decimal::from_str("0.55").unwrap(), Decimal::from(5));

        let response = book.to_order_book_response("asset-1");
        assert_eq!(response.best_bid(), Decimal::from_str("0.50").ok());
        assert_eq!(response.best_ask(), Decimal::from_str("0.53").ok());
        assert_eq!(response.bids.len(), 2);
        assert_eq!(response.asks.len(), 2);
    }
}
