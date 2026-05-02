use std::{env, fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Settings {
    pub app: AppConfig,
    pub network: NetworkConfig,
    pub market: MarketConfig,
    pub strategy: StrategyConfig,
    pub execution: ExecutionConfig,
    pub routing: RoutingConfig,
    pub telemetry: TelemetryConfig,
    pub telegram: TelegramConfig,
    pub accounts: Vec<AccountConfig>,
}

impl Settings {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let settings: Self = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<()> {
        if self.accounts.is_empty() {
            bail!("at least one [[accounts]] entry is required");
        }

        let enabled_accounts = self
            .accounts
            .iter()
            .filter(|account| account.enabled)
            .count();
        if enabled_accounts == 0 {
            bail!("at least one enabled account is required");
        }

        let primary = self.primary_account()?;
        if !primary.enabled {
            bail!(
                "routing.primary_account '{}' must reference an enabled account",
                self.routing.primary_account
            );
        }

        if self.strategy.round_interval_secs == 0 {
            bail!("strategy.round_interval_secs must be greater than zero");
        }

        if self.strategy.window_size_rounds == 0 {
            bail!("strategy.window_size_rounds must be greater than zero");
        }

        if self.strategy.order_size <= 0.0 {
            bail!("strategy.order_size must be greater than zero");
        }

        if !(0.0..=1.0).contains(&self.strategy.yes_price) || self.strategy.yes_price == 0.0 {
            bail!("strategy.yes_price must be greater than 0 and less than or equal to 1");
        }

        if !(0.0..=1.0).contains(&self.strategy.no_price) || self.strategy.no_price == 0.0 {
            bail!("strategy.no_price must be greater than 0 and less than or equal to 1");
        }

        if self.market.binance_symbol.trim().is_empty() {
            bail!("market.binance_symbol must not be empty");
        }

        if self.strategy.open_price_observation_max_deviation < 0.0 {
            bail!(
                "strategy.open_price_observation_max_deviation must be greater than or equal to zero"
            );
        }

        if self.strategy.open_price_max_deviation < 0.0 {
            bail!("strategy.open_price_max_deviation must be greater than or equal to zero");
        }

        if self.strategy.binance_support_lookback_candles == 0 {
            bail!("strategy.binance_support_lookback_candles must be greater than zero");
        }

        if self.strategy.binance_support_tolerance_percent < 0.0 {
            bail!(
                "strategy.binance_support_tolerance_percent must be greater than or equal to zero"
            );
        }

        if self.strategy.binance_ema_period == 0 {
            bail!("strategy.binance_ema_period must be greater than zero");
        }

        if self.strategy.binance_rsi_period == 0 {
            bail!("strategy.binance_rsi_period must be greater than zero");
        }

        if !(0.0..=100.0).contains(&self.strategy.binance_rsi_max) {
            bail!("strategy.binance_rsi_max must be between 0 and 100");
        }

        let round_interval_ms = self.strategy.round_interval_secs.saturating_mul(1_000);
        if self.strategy.quote_cancel_before_open_ms >= round_interval_ms {
            bail!(
                "strategy.quote_cancel_before_open_ms must be smaller than one full round interval"
            );
        }

        if self.strategy.quote_start_after_open_secs >= self.strategy.round_interval_secs {
            bail!(
                "strategy.quote_start_after_open_secs must be smaller than one full round interval"
            );
        }

        if self.strategy.quote_cancel_after_open_secs == 0 {
            bail!("strategy.quote_cancel_after_open_secs must be greater than zero");
        }

        if self.strategy.quote_cancel_after_open_secs >= self.strategy.round_interval_secs {
            bail!(
                "strategy.quote_cancel_after_open_secs must be smaller than one full round interval"
            );
        }

        if self.strategy.quote_start_after_open_secs >= self.strategy.quote_cancel_after_open_secs {
            bail!(
                "strategy.quote_start_after_open_secs must be smaller than strategy.quote_cancel_after_open_secs"
            );
        }

        if self.strategy.mode == StrategyMode::PreSplitDualSell
            && self.strategy.pre_split_before_open_secs < self.strategy.quote_start_before_open_secs
        {
            bail!(
                "strategy.pre_split_before_open_secs must be greater than or equal to strategy.quote_start_before_open_secs in pre_split_dual_sell mode"
            );
        }

        if self.strategy.mode == StrategyMode::PreOpenDualBuyTakerFlip
            && self.strategy.reactive_opposite_taker_usdc <= 0.0
        {
            bail!(
                "strategy.reactive_opposite_taker_usdc must be greater than zero in pre_open_dual_buy_taker_flip mode"
            );
        }

        if matches!(
            self.strategy.mode,
            StrategyMode::PreOpenDualBuyPaperTpsl | StrategyMode::PreOpenDualBuyPaperLimitExit
        ) {
            if !(0.0..=1.0).contains(&self.strategy.paper_fee_rebate_rate) {
                bail!(
                    "strategy.paper_fee_rebate_rate must be between 0 and 1 in paper strategy modes"
                );
            }

            if self.strategy.paper_output_dir.trim().is_empty() {
                bail!("strategy.paper_output_dir must not be empty in paper strategy modes");
            }
        }

        if self.strategy.mode == StrategyMode::PreOpenDualBuyPaperTpsl {
            if self.strategy.paper_extra_shares <= 0.0 {
                bail!(
                    "strategy.paper_extra_shares must be greater than zero in pre_open_dual_buy_paper_tpsl mode"
                );
            }

            if !(0.0..=1.0).contains(&self.strategy.paper_stop_loss_price)
                || self.strategy.paper_stop_loss_price == 0.0
            {
                bail!(
                    "strategy.paper_stop_loss_price must be greater than 0 and less than or equal to 1 in pre_open_dual_buy_paper_tpsl mode"
                );
            }

            if self.strategy.paper_take_profit_percents.is_empty() {
                bail!(
                    "strategy.paper_take_profit_percents must contain at least one take-profit percent in pre_open_dual_buy_paper_tpsl mode"
                );
            }

            if self
                .strategy
                .paper_take_profit_percents
                .iter()
                .any(|percent| *percent <= 0.0)
            {
                bail!(
                    "strategy.paper_take_profit_percents must contain only positive values in pre_open_dual_buy_paper_tpsl mode"
                );
            }
        }

        if self.strategy.mode == StrategyMode::PreOpenDualBuyPaperLimitExit {
            if !(0.0..=1.0).contains(&self.strategy.paper_limit_exit_price)
                || self.strategy.paper_limit_exit_price == 0.0
            {
                bail!(
                    "strategy.paper_limit_exit_price must be greater than 0 and less than or equal to 1 in pre_open_dual_buy_paper_limit_exit mode"
                );
            }

            if self.strategy.paper_force_taker_exit_before_settle_secs == 0 {
                bail!(
                    "strategy.paper_force_taker_exit_before_settle_secs must be greater than zero in pre_open_dual_buy_paper_limit_exit mode"
                );
            }

            if self.strategy.paper_force_taker_exit_before_settle_secs
                >= self.strategy.round_interval_secs
            {
                bail!(
                    "strategy.paper_force_taker_exit_before_settle_secs must be smaller than one full round interval in pre_open_dual_buy_paper_limit_exit mode"
                );
            }
        }

        if self.execution.max_batch_orders == 0 || self.execution.max_batch_orders > 15 {
            bail!("execution.max_batch_orders must be between 1 and 15");
        }

        if self.execution.relayer_poll_interval_ms == 0 {
            bail!("execution.relayer_poll_interval_ms must be greater than zero");
        }

        if self.execution.settled_redeem_scan_enabled {
            if self.execution.settled_redeem_scan_interval_secs == 0 {
                bail!("execution.settled_redeem_scan_interval_secs must be greater than zero");
            }

            if self.execution.settled_redeem_scan_lookback_secs == 0 {
                bail!("execution.settled_redeem_scan_lookback_secs must be greater than zero");
            }
        }

        if self.network.connect_timeout_ms == 0 {
            bail!("network.connect_timeout_ms must be greater than zero");
        }

        if self.network.request_timeout_ms == 0 {
            bail!("network.request_timeout_ms must be greater than zero");
        }

        if self.network.keepalive_interval_secs == 0 {
            bail!("network.keepalive_interval_secs must be greater than zero");
        }

        if self.network.transaction_timeout_secs == 0 {
            bail!("network.transaction_timeout_secs must be greater than zero");
        }

        if self.telegram.enabled {
            if self.telegram.bot_token_env.trim().is_empty() {
                bail!("telegram.bot_token_env is required when telegram.enabled = true");
            }

            if self.telegram.chat_ids.is_empty() {
                bail!(
                    "telegram.chat_ids must contain at least one chat id when telegram.enabled = true"
                );
            }

            let _ = self.telegram.bot_token()?;
        }

        Ok(())
    }

    pub fn primary_account(&self) -> Result<&AccountConfig> {
        self.accounts
            .iter()
            .find(|account| account.name == self.routing.primary_account)
            .context("routing.primary_account does not match any [[accounts]].name")
    }

    pub fn enabled_accounts(&self) -> impl Iterator<Item = &AccountConfig> {
        self.accounts.iter().filter(|account| account.enabled)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub instance_name: String,
    pub dry_run: bool,
    pub log_level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            instance_name: "pm-alpha".to_owned(),
            dry_run: false,
            log_level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    pub clob_rest_url: String,
    pub relayer_rest_url: String,
    pub gamma_rest_url: String,
    pub data_api_url: String,
    pub binance_rest_url: String,
    pub polygon_rpc_url_env: String,
    pub prefer_http2: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub keepalive_interval_secs: u64,
    pub transaction_timeout_secs: u64,
    pub tcp_nodelay: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            clob_rest_url: "https://clob.polymarket.com".to_owned(),
            relayer_rest_url: "https://relayer-v2.polymarket.com".to_owned(),
            gamma_rest_url: "https://gamma-api.polymarket.com".to_owned(),
            data_api_url: "https://data-api.polymarket.com".to_owned(),
            binance_rest_url: "https://api.binance.com".to_owned(),
            polygon_rpc_url_env: "POLYGON_RPC_URL".to_owned(),
            prefer_http2: true,
            connect_timeout_ms: 200,
            request_timeout_ms: 800,
            keepalive_interval_secs: 20,
            transaction_timeout_secs: 30,
            tcp_nodelay: true,
        }
    }
}

impl NetworkConfig {
    pub fn polygon_rpc_url(&self) -> Result<String> {
        env::var(&self.polygon_rpc_url_env).with_context(|| {
            format!(
                "polygon rpc url env '{}' is not set in the environment",
                self.polygon_rpc_url_env
            )
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MarketConfig {
    pub series_slug: String,
    pub discovery_lookahead_secs: u64,
    pub binance_symbol: String,
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            series_slug: "btc-5m".to_owned(),
            discovery_lookahead_secs: 1_200,
            binance_symbol: "BTCUSDT".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMode {
    PreSplitDualSell,
    PreOpenDualBuy,
    PreOpenDualBuyTakerFlip,
    PreOpenDualBuyPaperTpsl,
    PreOpenDualBuyPaperLimitExit,
    OpenPostDualBuyPriceGuard,
    BinanceCycleUpSingle,
}

impl Default for StrategyMode {
    fn default() -> Self {
        Self::PreSplitDualSell
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    pub mode: StrategyMode,
    pub round_interval_secs: u64,
    pub window_size_rounds: usize,
    pub quote_start_before_open_secs: u64,
    pub quote_cancel_before_open_ms: u64,
    pub pre_split_before_open_secs: u64,
    pub quote_start_after_open_secs: u64,
    pub quote_cancel_after_open_secs: u64,
    pub order_size: f64,
    pub yes_price: f64,
    pub no_price: f64,
    pub open_price_observation_max_deviation: f64,
    pub open_price_max_deviation: f64,
    pub reactive_opposite_taker_usdc: f64,
    pub reactive_buy_slippage_ticks: u32,
    pub binance_support_lookback_candles: usize,
    pub binance_support_tolerance_percent: f64,
    pub binance_ema_period: usize,
    pub binance_rsi_period: usize,
    pub binance_rsi_max: f64,
    pub paper_extra_shares: f64,
    pub paper_stop_loss_price: f64,
    pub paper_take_profit_percents: Vec<f64>,
    pub paper_limit_exit_price: f64,
    pub paper_force_taker_exit_before_settle_secs: u64,
    pub paper_fee_rebate_rate: f64,
    pub paper_output_dir: String,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            mode: StrategyMode::PreSplitDualSell,
            round_interval_secs: 300,
            window_size_rounds: 2,
            quote_start_before_open_secs: 180,
            quote_cancel_before_open_ms: 1_000,
            pre_split_before_open_secs: 240,
            quote_start_after_open_secs: 60,
            quote_cancel_after_open_secs: 120,
            order_size: 5.0,
            yes_price: 0.54,
            no_price: 0.54,
            open_price_observation_max_deviation: 80.0,
            open_price_max_deviation: 50.0,
            reactive_opposite_taker_usdc: 2.0,
            reactive_buy_slippage_ticks: 2,
            binance_support_lookback_candles: 5,
            binance_support_tolerance_percent: 0.3,
            binance_ema_period: 20,
            binance_rsi_period: 14,
            binance_rsi_max: 35.0,
            paper_extra_shares: 10.0,
            paper_stop_loss_price: 0.50,
            paper_take_profit_percents: vec![5.0, 10.0, 15.0, 20.0],
            paper_limit_exit_price: 0.50,
            paper_force_taker_exit_before_settle_secs: 120,
            paper_fee_rebate_rate: 0.30,
            paper_output_dir: "logs/paper".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnchainExecutionMode {
    Rpc,
    Relayer,
}

impl Default for OnchainExecutionMode {
    fn default() -> Self {
        Self::Rpc
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClobExecutionMode {
    Rust,
    PythonHelper,
}

impl Default for ClobExecutionMode {
    fn default() -> Self {
        Self::Rust
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub max_batch_orders: usize,
    pub clob_execution_mode: ClobExecutionMode,
    pub clob_helper_python_bin: String,
    pub clob_helper_script: String,
    pub onchain_execution_mode: OnchainExecutionMode,
    pub refresh_metadata_on_start: bool,
    pub auto_approve_ctf: bool,
    pub auto_approve_collateral: bool,
    pub relayer_require_safe_deployed: bool,
    pub relayer_poll_interval_ms: u64,
    pub tick_size: String,
    pub neg_risk: bool,
    pub fee_rate_bps: u16,
    pub settled_redeem_scan_enabled: bool,
    pub settled_redeem_scan_interval_secs: u64,
    pub settled_redeem_scan_lookback_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_batch_orders: 15,
            clob_execution_mode: ClobExecutionMode::Rust,
            clob_helper_python_bin: "python3".to_owned(),
            clob_helper_script: "scripts/clob_helper.py".to_owned(),
            onchain_execution_mode: OnchainExecutionMode::Rpc,
            refresh_metadata_on_start: true,
            auto_approve_ctf: true,
            auto_approve_collateral: true,
            relayer_require_safe_deployed: true,
            relayer_poll_interval_ms: 2_000,
            tick_size: "0.01".to_owned(),
            neg_risk: false,
            fee_rate_bps: 0,
            settled_redeem_scan_enabled: false,
            settled_redeem_scan_interval_secs: 60,
            settled_redeem_scan_lookback_secs: 21_600,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    pub primary_account: String,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            primary_account: "acc1".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    pub log_json: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self { log_json: true }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelegramConfig {
    pub enabled: bool,
    pub bot_token_env: String,
    pub chat_ids: Vec<i64>,
    pub send_startup: bool,
    pub send_shutdown: bool,
    pub send_errors: bool,
    pub disable_link_preview: bool,
    pub parse_mode: Option<String>,
}

impl TelegramConfig {
    pub fn bot_token(&self) -> Result<String> {
        env::var(&self.bot_token_env).with_context(|| {
            format!(
                "telegram bot token env '{}' is not set in the environment",
                self.bot_token_env
            )
        })
    }
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token_env: "PM_TELEGRAM_BOT_TOKEN".to_owned(),
            chat_ids: Vec::new(),
            send_startup: true,
            send_shutdown: true,
            send_errors: true,
            disable_link_preview: true,
            parse_mode: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignatureType {
    Eoa,
    Magic,
    BrowserProxy,
    GnosisSafe,
}

impl Default for SignatureType {
    fn default() -> Self {
        Self::Eoa
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AccountConfig {
    pub name: String,
    pub enabled: bool,
    pub chain_id: u64,
    pub signature_type: SignatureType,
    pub funder_address: String,
    pub private_key_env: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub api_passphrase_env: String,
    pub relayer_api_key_env: String,
    pub relayer_api_key_address_env: String,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            name: "acc1".to_owned(),
            enabled: true,
            chain_id: 137,
            signature_type: SignatureType::Eoa,
            funder_address: String::new(),
            private_key_env: "PM_ACC1_PRIVATE_KEY".to_owned(),
            api_key_env: "PM_ACC1_API_KEY".to_owned(),
            api_secret_env: "PM_ACC1_API_SECRET".to_owned(),
            api_passphrase_env: "PM_ACC1_API_PASSPHRASE".to_owned(),
            relayer_api_key_env: "PM_ACC1_RELAYER_API_KEY".to_owned(),
            relayer_api_key_address_env: "PM_ACC1_RELAYER_API_KEY_ADDRESS".to_owned(),
        }
    }
}
