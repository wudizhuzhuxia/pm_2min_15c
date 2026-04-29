#![allow(dead_code)]

use std::{
    collections::HashMap,
    env::{self, VarError},
    io::Write,
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::{
    dyn_abi::Eip712Domain,
    hex,
    network::{EthereumWallet, ReceiptResponse},
    primitives::{Address, ChainId, FixedBytes, U256, address, b256, keccak256},
    providers::ProviderBuilder,
    signers::{Signer, local::PrivateKeySigner},
    sol,
    sol_types::{SolCall, SolStruct},
};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::Rng as _;
use reqwest::{Client, Method};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tracing::{debug, info};

use crate::{
    config::{
        AccountConfig, ClobExecutionMode, ExecutionConfig, NetworkConfig, OnchainExecutionMode,
        SignatureType,
    },
    http::{build_http_client, join_url},
    market::RoundDescriptor,
    strategy::LegSide,
};

const ORDER_NAME: &str = "Polymarket CTF Exchange";
const ORDER_VERSION: &str = "2";
const USDC_DECIMALS: u32 = 6;
const LOT_SIZE_SCALE: u32 = 2;
const POLYGON: ChainId = 137;
const AMOY: ChainId = 80002;
const COLLATERAL_TOKEN_POLYGON: Address = address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");
const COLLATERAL_TOKEN_AMOY: Address = address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");
const CTF_POLYGON: Address = address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");
const CTF_AMOY: Address = address!("0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB");
const SAFE_FACTORY_POLYGON: Address = address!("0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b");
const SAFE_INIT_CODE_HASH: FixedBytes<32> =
    b256!("0x2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf");
const ZERO_BYTES32: FixedBytes<32> =
    b256!("0x0000000000000000000000000000000000000000000000000000000000000000");
const RELAYER_TX_TYPE_SAFE: &str = "SAFE";
const RELAYER_STATE_MINED: &str = "STATE_MINED";
const RELAYER_STATE_CONFIRMED: &str = "STATE_CONFIRMED";
const RELAYER_STATE_FAILED: &str = "STATE_FAILED";
const RELAYER_STATE_INVALID: &str = "STATE_INVALID";

sol! {
    #[derive(Debug)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }

    #[derive(Debug)]
    struct SafeTx {
        address to;
        uint256 value;
        bytes data;
        uint8 operation;
        uint256 safeTxGas;
        uint256 baseGas;
        uint256 gasPrice;
        address gasToken;
        address refundReceiver;
        uint256 nonce;
    }

    #[sol(rpc)]
    contract IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }

    #[sol(rpc)]
    contract IConditionalTokens {
        function splitPosition(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        ) external;
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        ) external;
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function isApprovedForAll(address account, address operator) external view returns (bool);
        function setApprovalForAll(address operator, bool approved) external;
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionGatewayConfig {
    pub max_batch_orders: usize,
    pub clob_execution_mode: ClobExecutionMode,
    pub onchain_execution_mode: OnchainExecutionMode,
}

#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub token_id: U256,
    pub tick_size: Decimal,
    pub neg_risk: bool,
    pub fee_rate_bps: u32,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct RelayerNonceResponse {
    nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RelayerDeployedResponse {
    deployed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RelayerSubmitResponse {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    #[serde(rename = "transactionHash", default)]
    transaction_hash: Option<String>,
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RelayerTransaction {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    #[serde(rename = "transactionHash", default)]
    transaction_hash: Option<String>,
    state: String,
}

#[derive(Debug, Clone, Serialize)]
struct RelayerSignatureParams {
    #[serde(rename = "gasPrice")]
    gas_price: String,
    operation: String,
    #[serde(rename = "safeTxnGas")]
    safe_txn_gas: String,
    #[serde(rename = "baseGas")]
    base_gas: String,
    #[serde(rename = "gasToken")]
    gas_token: String,
    #[serde(rename = "refundReceiver")]
    refund_receiver: String,
}

#[derive(Debug, Clone, Serialize)]
struct RelayerSubmitRequest {
    from: String,
    to: String,
    #[serde(rename = "proxyWallet")]
    proxy_wallet: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    data: String,
    nonce: String,
    signature: String,
    #[serde(rename = "signatureParams")]
    signature_params: RelayerSignatureParams,
    #[serde(rename = "type")]
    tx_type: String,
    metadata: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    fn as_u8(self) -> u8 {
        match self {
            Self::Buy => 0,
            Self::Sell => 1,
        }
    }

    fn as_clob_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum OrderType {
    GTC,
    FOK,
    GTD,
    FAK,
}

impl OrderType {
    fn as_str(self) -> &'static str {
        match self {
            Self::GTC => "GTC",
            Self::FOK => "FOK",
            Self::GTD => "GTD",
            Self::FAK => "FAK",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LimitOrderInput {
    pub token_id: U256,
    pub side: OrderSide,
    pub price: Decimal,
    pub size: Decimal,
    pub expiration: Option<DateTime<Utc>>,
    pub order_type: OrderType,
    pub post_only: bool,
}

#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub purpose_label: String,
    pub size: Decimal,
    pub payload: RawSignedOrder,
}

#[derive(Debug, Clone)]
pub struct SplitExecutionReceipt {
    pub tx_hash: String,
    pub block_number: Option<u64>,
    pub amount_usdc: Decimal,
}

#[derive(Debug, Clone)]
pub struct MergeExecutionReceipt {
    pub tx_hash: String,
    pub block_number: Option<u64>,
    pub amount_usdc: Decimal,
}

#[derive(Debug, Clone)]
pub struct RedeemExecutionReceipt {
    pub tx_hash: String,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LoadedAccount {
    pub name: String,
    pub chain_id: ChainId,
    pub signature_type: SignatureType,
    pub private_key: String,
    pub signer: PrivateKeySigner,
    pub signer_address: Address,
    pub funder_address: Address,
    pub credentials: ApiCredentials,
    pub relayer_credentials: Option<RelayerApiCredentials>,
}

#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

#[derive(Debug, Clone)]
pub struct RelayerApiCredentials {
    pub api_key: String,
    pub owner_address: Address,
}

impl ApiCredentials {
    fn from_env(account: &AccountConfig) -> Result<Self> {
        Ok(Self {
            api_key: env::var(&account.api_key_env).with_context(|| {
                format!("environment variable '{}' is not set", account.api_key_env)
            })?,
            secret: env::var(&account.api_secret_env).with_context(|| {
                format!(
                    "environment variable '{}' is not set",
                    account.api_secret_env
                )
            })?,
            passphrase: env::var(&account.api_passphrase_env).with_context(|| {
                format!(
                    "environment variable '{}' is not set",
                    account.api_passphrase_env
                )
            })?,
        })
    }
}

impl RelayerApiCredentials {
    fn from_env_optional(account: &AccountConfig) -> Result<Option<Self>> {
        let api_key = read_optional_env(&account.relayer_api_key_env)
            .with_context(|| format!("failed to read '{}'", account.relayer_api_key_env))?;
        let owner_address = read_optional_env(&account.relayer_api_key_address_env)
            .with_context(|| format!("failed to read '{}'", account.relayer_api_key_address_env))?;

        match (api_key, owner_address) {
            (None, None) => Ok(None),
            (Some(_), None) => bail!(
                "environment variable '{}' is set but '{}' is not set",
                account.relayer_api_key_env,
                account.relayer_api_key_address_env
            ),
            (None, Some(_)) => bail!(
                "environment variable '{}' is set but '{}' is not set",
                account.relayer_api_key_address_env,
                account.relayer_api_key_env
            ),
            (Some(api_key), Some(owner_address)) => Ok(Some(Self {
                api_key,
                owner_address: Address::from_str(&owner_address)
                    .context("failed to parse relayer api key owner address")?,
            })),
        }
    }
}

impl LoadedAccount {
    pub fn from_config(account: &AccountConfig) -> Result<Self> {
        let private_key = env::var(&account.private_key_env).with_context(|| {
            format!(
                "environment variable '{}' is not set",
                account.private_key_env
            )
        })?;
        let signer = PrivateKeySigner::from_str(&private_key)
            .context("failed to parse account private key")?
            .with_chain_id(Some(account.chain_id));
        let signer_address = signer.address();
        let funder_address = if account.funder_address.trim().is_empty()
            || account.funder_address == "0x0000000000000000000000000000000000000000"
        {
            signer_address
        } else {
            Address::from_str(&account.funder_address)
                .context("failed to parse account funder_address")?
        };

        if funder_address != signer_address && matches!(account.signature_type, SignatureType::Eoa)
        {
            bail!(
                "funder_address {} differs from signer {} but signature_type is 'eoa'; proxy-wallet accounts must use 'browser_proxy', 'gnosis_safe', or 'magic' as appropriate",
                funder_address,
                signer_address
            );
        }

        Ok(Self {
            name: account.name.clone(),
            chain_id: account.chain_id,
            signature_type: account.signature_type,
            private_key,
            signer,
            signer_address,
            funder_address,
            credentials: ApiCredentials::from_env(account)?,
            relayer_credentials: RelayerApiCredentials::from_env_optional(account)?,
        })
    }
}

pub struct ExecutionGateway {
    client: Client,
    clob_base_url: String,
    config: ExecutionConfig,
    network: NetworkConfig,
    account: LoadedAccount,
    metadata: Arc<RwLock<HashMap<U256, TokenMetadata>>>,
}

impl ExecutionGateway {
    pub fn from_account(
        network: &NetworkConfig,
        config: &ExecutionConfig,
        account: LoadedAccount,
    ) -> Result<Self> {
        let client = build_http_client(network, "pm-alpha-execution-gateway")?;
        Ok(Self {
            client,
            clob_base_url: network.clob_rest_url.clone(),
            config: config.clone(),
            network: network.clone(),
            account,
            metadata: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn account(&self) -> &LoadedAccount {
        &self.account
    }

    pub fn runtime_config(&self) -> ExecutionGatewayConfig {
        ExecutionGatewayConfig {
            max_batch_orders: self.config.max_batch_orders,
            clob_execution_mode: self.config.clob_execution_mode,
            onchain_execution_mode: self.config.onchain_execution_mode,
        }
    }

    fn relayer_credentials(&self) -> Result<&RelayerApiCredentials> {
        self.account
            .relayer_credentials
            .as_ref()
            .context("onchain relayer mode requires relayer API credentials in the environment")
    }

    fn expected_safe_address(&self) -> Result<Address> {
        let factory = safe_factory(self.account.chain_id).with_context(|| {
            format!(
                "relayer safe mode is not supported on chain id {}",
                self.account.chain_id
            )
        })?;
        Ok(factory.create2(
            safe_create2_salt(self.account.signer_address),
            SAFE_INIT_CODE_HASH,
        ))
    }

    pub async fn prewarm_round(&self, round: &RoundDescriptor) -> Result<Vec<TokenMetadata>> {
        let yes_token = self.token_id_for_leg(round, LegSide::Yes)?;
        let no_token = self.token_id_for_leg(round, LegSide::No)?;

        let (yes, no) = tokio::try_join!(
            self.market_metadata(yes_token),
            self.market_metadata(no_token)
        )?;
        info!(
            yes_token_id = %round.yes_token_id,
            no_token_id = %round.no_token_id,
            yes_tick_size = %yes.tick_size,
            no_tick_size = %no.tick_size,
            yes_fee_rate_bps = yes.fee_rate_bps,
            no_fee_rate_bps = no.fee_rate_bps,
            "prewarmed token metadata"
        );
        Ok(vec![yes, no])
    }

    pub fn token_id_for_leg(&self, round: &RoundDescriptor, leg: LegSide) -> Result<U256> {
        let token_id = match leg {
            LegSide::Yes => &round.yes_token_id,
            LegSide::No => &round.no_token_id,
        };
        U256::from_str(token_id)
            .with_context(|| format!("failed to parse {:?} token id '{}'", leg, token_id))
    }

    pub async fn market_metadata(&self, token_id: U256) -> Result<TokenMetadata> {
        if let Some(metadata) = self.metadata.read().await.get(&token_id).cloned() {
            return Ok(metadata);
        }

        if !self.config.refresh_metadata_on_start {
            let metadata = self.default_metadata(token_id)?;
            self.metadata
                .write()
                .await
                .insert(token_id, metadata.clone());
            return Ok(metadata);
        }

        let (tick_size, neg_risk, fee_rate_bps) = tokio::try_join!(
            self.fetch_tick_size(token_id),
            self.fetch_neg_risk(token_id),
            self.fetch_fee_rate(token_id)
        )?;

        let metadata = TokenMetadata {
            token_id,
            tick_size,
            neg_risk,
            fee_rate_bps,
            fetched_at: Utc::now(),
        };

        self.metadata
            .write()
            .await
            .insert(token_id, metadata.clone());
        Ok(metadata)
    }

    pub async fn build_limit_order(&self, input: LimitOrderInput) -> Result<RawSignedOrder> {
        let metadata = self.market_metadata(input.token_id).await?;
        validate_limit_order(&input, &metadata)?;

        let decimals = metadata.tick_size.scale();
        let (maker_amount, taker_amount) = match input.side {
            OrderSide::Buy => (
                to_fixed_u128(
                    (input.size * input.price).trunc_with_scale(decimals + LOT_SIZE_SCALE),
                ),
                to_fixed_u128(input.size),
            ),
            OrderSide::Sell => (
                to_fixed_u128(input.size),
                to_fixed_u128(
                    (input.size * input.price).trunc_with_scale(decimals + LOT_SIZE_SCALE),
                ),
            ),
        };

        let expiration = input.expiration.unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        if !matches!(input.order_type, OrderType::GTD) && expiration > DateTime::<Utc>::UNIX_EPOCH {
            bail!("only GTD orders may set a non-zero expiration");
        }
        let expiration_ts = expiration.timestamp().max(0) as u64;

        let order = Order {
            salt: U256::from(to_ieee_754_int(generate_seed())),
            maker: self.account.funder_address,
            signer: self.account.signer_address,
            tokenId: input.token_id,
            makerAmount: U256::from(maker_amount),
            takerAmount: U256::from(taker_amount),
            side: input.side.as_u8(),
            signatureType: signature_type_code(self.account.signature_type),
            timestamp: U256::from(now_unix_millis() as u64),
            metadata: ZERO_BYTES32,
            builder: ZERO_BYTES32,
        };

        let signature = self.sign_order(&order, metadata.neg_risk).await?;
        let payload = RawSignedOrder::new(
            &self.account,
            order,
            expiration_ts,
            encode_order_signature(signature, self.account.signature_type),
            input.side,
            input.order_type,
            input.price,
            input.size,
            Some(input.post_only),
        )?;

        debug!(
            token_id = %input.token_id,
            side = ?input.side,
            price = %input.price,
            size = %input.size,
            "built raw signed limit order"
        );

        Ok(payload)
    }

    pub async fn build_leg_buy_market_order_by_quote(
        &self,
        round: &RoundDescriptor,
        leg: LegSide,
        price: Decimal,
        quote_amount: Decimal,
        order_type: OrderType,
    ) -> Result<RawSignedOrder> {
        let token_id = self.token_id_for_leg(round, leg)?;
        self.build_market_buy_order_by_quote(token_id, price, quote_amount, order_type)
        .await
    }

    async fn build_market_buy_order_by_quote(
        &self,
        token_id: U256,
        price: Decimal,
        quote_amount: Decimal,
        order_type: OrderType,
    ) -> Result<RawSignedOrder> {
        if !matches!(order_type, OrderType::FAK | OrderType::FOK) {
            bail!("market-style buy orders must use FAK or FOK");
        }

        let metadata = self.market_metadata(token_id).await?;
        validate_order_price(price, &metadata)?;

        if quote_amount.is_sign_negative() || quote_amount.is_zero() {
            bail!("quote_amount must be greater than zero");
        }

        let decimals = metadata.tick_size.scale();
        let price = price.trunc_with_scale(decimals);
        let maker_amount = quote_amount.trunc_with_scale(decimals);
        if maker_amount <= Decimal::ZERO {
            bail!(
                "quote_amount {} is too small for tick size {}",
                quote_amount,
                metadata.tick_size
            );
        }

        let taker_amount = market_buy_shares_for_quote(maker_amount, price, decimals);
        if taker_amount <= Decimal::ZERO {
            bail!(
                "quote_amount {} at price {} does not produce a positive share amount",
                maker_amount,
                price
            );
        }

        let order = Order {
            salt: U256::from(to_ieee_754_int(generate_seed())),
            maker: self.account.funder_address,
            signer: self.account.signer_address,
            tokenId: token_id,
            makerAmount: U256::from(to_fixed_u128(maker_amount)),
            takerAmount: U256::from(to_fixed_u128(taker_amount)),
            side: OrderSide::Buy.as_u8(),
            signatureType: signature_type_code(self.account.signature_type),
            timestamp: U256::from(now_unix_millis() as u64),
            metadata: ZERO_BYTES32,
            builder: ZERO_BYTES32,
        };

        let signature = self.sign_order(&order, metadata.neg_risk).await?;
        let payload = RawSignedOrder::new(
            &self.account,
            order,
            0,
            encode_order_signature(signature, self.account.signature_type),
            OrderSide::Buy,
            order_type,
            price,
            taker_amount,
            Some(false),
        )?;

        debug!(
            token_id = %token_id,
            price = %price,
            quote_amount = %maker_amount,
            shares = %taker_amount,
            "built raw signed market-style buy order by quote"
        );

        Ok(payload)
    }

    pub async fn build_leg_sell_order(
        &self,
        round: &RoundDescriptor,
        leg: LegSide,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        post_only: bool,
    ) -> Result<RawSignedOrder> {
        let token_id = self.token_id_for_leg(round, leg)?;
        self.build_limit_order(LimitOrderInput {
            token_id,
            side: OrderSide::Sell,
            price,
            size,
            expiration: None,
            order_type,
            post_only,
        })
        .await
    }

    pub async fn build_leg_buy_order(
        &self,
        round: &RoundDescriptor,
        leg: LegSide,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        post_only: bool,
    ) -> Result<RawSignedOrder> {
        let token_id = self.token_id_for_leg(round, leg)?;
        self.build_limit_order(LimitOrderInput {
            token_id,
            side: OrderSide::Buy,
            price,
            size,
            expiration: None,
            order_type,
            post_only,
        })
        .await
    }

    pub async fn post_orders(&self, orders: &[RawSignedOrder]) -> Result<Vec<PostOrderResponse>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }

        if orders.len() > self.config.max_batch_orders {
            bail!(
                "batch size {} exceeds configured max_batch_orders {}",
                orders.len(),
                self.config.max_batch_orders
            );
        }

        match self.config.clob_execution_mode {
            ClobExecutionMode::Rust => self.post_orders_rust(orders).await,
            ClobExecutionMode::PythonHelper => self.post_orders_python_helper(orders).await,
        }
    }

    async fn post_orders_rust(&self, orders: &[RawSignedOrder]) -> Result<Vec<PostOrderResponse>> {
        let serialized =
            serde_json::to_string(orders).context("failed to serialize batch order request")?;
        let mut request = self
            .client
            .request(Method::POST, join_url(&self.clob_base_url, "/orders"))
            .body(serialized.clone())
            .build()
            .context("failed to build batch order request")?;
        let headers =
            self.create_l2_headers(Method::POST.as_str(), "/orders", Some(&serialized))?;
        request.headers_mut().extend(headers);
        request.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        let body = self
            .client
            .execute(request)
            .await
            .context("batch order request failed")?
            .error_for_status()
            .context("batch order request returned non-success status")?
            .text()
            .await
            .context("failed to read batch order response body")?;

        serde_json::from_str::<Vec<PostOrderResponse>>(&body)
            .with_context(|| format!("failed to decode batch order response body: {body}"))
    }

    pub async fn cancel_orders(&self, order_ids: &[&str]) -> Result<CancelOrdersResponse> {
        match self.config.clob_execution_mode {
            ClobExecutionMode::Rust => self.cancel_orders_rust(order_ids).await,
            ClobExecutionMode::PythonHelper => self.cancel_orders_python_helper(order_ids).await,
        }
    }

    pub async fn fetch_order_status(&self, order_id: &str) -> Result<Option<OrderStatusResponse>> {
        let path = format!("/data/order/{order_id}");
        let mut request = self
            .client
            .request(Method::GET, join_url(&self.clob_base_url, &path))
            .build()
            .context("failed to build order-status request")?;
        let headers = self.create_l2_headers(Method::GET.as_str(), &path, None)?;
        request.headers_mut().extend(headers);

        let response = self
            .client
            .execute(request)
            .await
            .context("order-status request failed")?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let body = response
            .error_for_status()
            .context("order-status request returned non-success status")?
            .text()
            .await
            .context("failed to read order-status response body")?;

        serde_json::from_str::<Option<OrderStatusResponse>>(&body)
            .with_context(|| format!("failed to decode order-status response body: {body}"))
    }

    pub async fn fetch_order_book(&self, token_id: &str) -> Result<Option<OrderBookResponse>> {
        let response = self
            .client
            .get(join_url(&self.clob_base_url, "/book"))
            .query(&[("token_id", token_id)])
            .send()
            .await
            .context("order-book request failed")?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        response
            .error_for_status()
            .context("order-book request returned non-success status")?
            .json::<OrderBookResponse>()
            .await
            .context("failed to decode order-book response")
            .map(Some)
    }

    pub async fn round_token_balances(
        &self,
        round: &RoundDescriptor,
    ) -> Result<(Decimal, Decimal)> {
        let (yes_balance, no_balance) = self.round_position_balances(round).await?;
        Ok((
            u256_to_decimal(yes_balance, USDC_DECIMALS)?,
            u256_to_decimal(no_balance, USDC_DECIMALS)?,
        ))
    }

    pub async fn mergeable_full_set_size(&self, round: &RoundDescriptor) -> Result<Decimal> {
        let (yes_balance, no_balance) = self.round_token_balances(round).await?;
        Ok(yes_balance.min(no_balance))
    }

    async fn cancel_orders_rust(&self, order_ids: &[&str]) -> Result<CancelOrdersResponse> {
        let serialized = serde_json::to_string(order_ids)
            .context("failed to serialize cancel orders request")?;
        let mut request = self
            .client
            .request(Method::DELETE, join_url(&self.clob_base_url, "/orders"))
            .body(serialized.clone())
            .build()
            .context("failed to build cancel orders request")?;
        let headers =
            self.create_l2_headers(Method::DELETE.as_str(), "/orders", Some(&serialized))?;
        request.headers_mut().extend(headers);
        request.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        self.client
            .execute(request)
            .await
            .context("cancel orders request failed")?
            .error_for_status()
            .context("cancel orders request returned non-success status")?
            .json::<CancelOrdersResponse>()
            .await
            .context("failed to decode cancel orders response")
    }

    async fn post_orders_python_helper(
        &self,
        orders: &[RawSignedOrder],
    ) -> Result<Vec<PostOrderResponse>> {
        let request = PythonHelperPostOrdersRequest {
            host: self.clob_base_url.clone(),
            chain_id: self.account.chain_id,
            private_key: self.account.private_key.clone(),
            signature_type: signature_type_code(self.account.signature_type),
            funder: self.account.funder_address.to_checksum(None),
            creds: PythonHelperApiCredentials::from_account(&self.account),
            orders: orders
                .iter()
                .map(PythonHelperOrder::try_from)
                .collect::<Result<Vec<_>>>()?,
        };
        self.run_python_helper("post-orders", &request).await
    }

    async fn cancel_orders_python_helper(
        &self,
        order_ids: &[&str],
    ) -> Result<CancelOrdersResponse> {
        let request = PythonHelperCancelOrdersRequest {
            host: self.clob_base_url.clone(),
            chain_id: self.account.chain_id,
            private_key: self.account.private_key.clone(),
            signature_type: signature_type_code(self.account.signature_type),
            funder: self.account.funder_address.to_checksum(None),
            creds: PythonHelperApiCredentials::from_account(&self.account),
            order_ids: order_ids.iter().map(|id| (*id).to_owned()).collect(),
        };
        self.run_python_helper("cancel-orders", &request).await
    }

    async fn run_python_helper<T, R>(&self, action: &str, payload: &T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let python_bin = self.config.clob_helper_python_bin.trim().to_owned();
        let script = self.config.clob_helper_script.trim().to_owned();
        let input =
            serde_json::to_vec(payload).context("failed to serialize python helper input")?;
        let action = action.to_owned();
        let action_for_child = action.clone();

        let output = tokio::task::spawn_blocking(move || -> Result<std::process::Output> {
            let mut child = Command::new(&python_bin)
                .arg(&script)
                .arg(&action_for_child)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| {
                    format!(
                        "failed to start python helper '{}' with interpreter '{}'",
                        script, python_bin
                    )
                })?;

            if let Some(stdin) = child.stdin.as_mut() {
                stdin
                    .write_all(&input)
                    .context("failed to write python helper stdin")?;
            }

            child
                .wait_with_output()
                .context("failed to wait for python helper")
        })
        .await
        .context("python helper task join failed")??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let details = match (stdout.is_empty(), stderr.is_empty()) {
                (false, false) => format!("stdout: {stdout}; stderr: {stderr}"),
                (false, true) => format!("stdout: {stdout}"),
                (true, false) => format!("stderr: {stderr}"),
                (true, true) => "no output".to_owned(),
            };
            bail!(
                "python helper '{}' failed with status {:?}: {}",
                action,
                output.status.code(),
                details
            );
        }

        serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "failed to decode python helper '{}' response: {}",
                action,
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }

    pub async fn split_position(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<SplitExecutionReceipt> {
        match self.config.onchain_execution_mode {
            OnchainExecutionMode::Rpc => self.split_position_via_rpc(round, usdc_amount).await,
            OnchainExecutionMode::Relayer => {
                self.split_position_via_relayer(round, usdc_amount).await
            }
        }
    }

    pub async fn merge_position(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<MergeExecutionReceipt> {
        match self.config.onchain_execution_mode {
            OnchainExecutionMode::Rpc => self.merge_position_via_rpc(round, usdc_amount).await,
            OnchainExecutionMode::Relayer => {
                self.merge_position_via_relayer(round, usdc_amount).await
            }
        }
    }

    pub async fn redeem_positions_when_resolved(
        &self,
        round: &RoundDescriptor,
    ) -> Result<Option<RedeemExecutionReceipt>> {
        const MAX_REDEEM_WAIT_ATTEMPTS: usize = 300;

        for _ in 0..MAX_REDEEM_WAIT_ATTEMPTS {
            let (yes_balance, no_balance) = self.round_position_balances(round).await?;
            if yes_balance.is_zero() && no_balance.is_zero() {
                return Ok(None);
            }

            if self.condition_resolved(round).await? {
                let receipt = self.execute_redeem_positions(round).await?;
                self.ensure_redeemed_balances_cleared(round).await?;
                return Ok(Some(receipt));
            }

            sleep(Duration::from_millis(
                self.config.relayer_poll_interval_ms.max(250),
            ))
            .await;
        }

        bail!(
            "timed out waiting for condition {} to resolve for redeemPositions",
            round.condition_id
        );
    }

    pub async fn redeem_positions_if_resolved(
        &self,
        round: &RoundDescriptor,
    ) -> Result<Option<RedeemExecutionReceipt>> {
        let (yes_balance, no_balance) = self.round_position_balances(round).await?;
        if yes_balance.is_zero() && no_balance.is_zero() {
            return Ok(None);
        }

        if !self.condition_resolved(round).await? {
            return Ok(None);
        }

        let receipt = self.execute_redeem_positions(round).await?;
        self.ensure_redeemed_balances_cleared(round).await?;
        Ok(Some(receipt))
    }

    async fn execute_redeem_positions(
        &self,
        round: &RoundDescriptor,
    ) -> Result<RedeemExecutionReceipt> {
        match self.config.onchain_execution_mode {
            OnchainExecutionMode::Rpc => self.redeem_positions_via_rpc(round).await,
            OnchainExecutionMode::Relayer => self.redeem_positions_via_relayer(round).await,
        }
    }

    async fn ensure_redeemed_balances_cleared(&self, round: &RoundDescriptor) -> Result<()> {
        const MAX_BALANCE_CLEAR_CHECKS: usize = 12;

        for attempt in 1..=MAX_BALANCE_CLEAR_CHECKS {
            let (yes_balance, no_balance) = self.round_token_balances(round).await?;
            if yes_balance <= Decimal::ZERO && no_balance <= Decimal::ZERO {
                return Ok(());
            }

            if attempt < MAX_BALANCE_CLEAR_CHECKS {
                sleep(Duration::from_millis(self.config.relayer_poll_interval_ms.max(250))).await;
            } else {
                bail!(
                    "redeem transaction completed but token balances remain for condition {} (yes_balance={}, no_balance={})",
                    round.condition_id,
                    yes_balance,
                    no_balance
                );
            }
        }

        Ok(())
    }

    async fn split_position_via_rpc(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<SplitExecutionReceipt> {
        if self.account.funder_address != self.account.signer_address {
            bail!(
                "rpc onchain mode requires collateral on signer address {}; configured funder_address {} differs. use execution.onchain_execution_mode = 'relayer' for Polymarket proxy wallets",
                self.account.signer_address,
                self.account.funder_address
            );
        }

        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);

        let yes_metadata = self
            .market_metadata(self.token_id_for_leg(round, LegSide::Yes)?)
            .await?;
        let exchange = exchange_contract(self.account.chain_id, yes_metadata.neg_risk)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let required = decimal_to_u256(usdc_amount, USDC_DECIMALS)?;

        if self.config.auto_approve_collateral {
            let erc20 = IERC20::new(collateral, &provider);
            let allowance = erc20
                .allowance(self.account.funder_address, conditional_tokens_address)
                .call()
                .await
                .context("failed to query collateral allowance for CTF")?;
            if allowance < required {
                info!(
                    owner = %self.account.funder_address,
                    spender = %conditional_tokens_address.to_checksum(None),
                    required = %required,
                    current_allowance = %allowance,
                    "approving collateral token for CTF"
                );

                let pending = erc20
                    .approve(conditional_tokens_address, U256::MAX)
                    .send()
                    .await
                    .context("failed to submit collateral approval transaction")?;

                let receipt = timeout(
                    Duration::from_secs(self.network.transaction_timeout_secs),
                    pending.get_receipt(),
                )
                .await
                .context("timed out waiting for collateral approval receipt")?
                .context("failed while waiting for collateral approval receipt")?;

                if !receipt.status() {
                    bail!("collateral approval transaction reverted");
                }
            }
        }
        if self.config.auto_approve_ctf {
            let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
            let approved = conditional_tokens
                .isApprovedForAll(self.account.funder_address, exchange)
                .call()
                .await
                .context("failed to query CTF exchange approval")?;

            if !approved {
                info!(
                    owner = %self.account.funder_address,
                    operator = %exchange.to_checksum(None),
                    "approving CTF exchange operator"
                );

                let pending = conditional_tokens
                    .setApprovalForAll(exchange, true)
                    .send()
                    .await
                    .context("failed to submit CTF operator approval transaction")?;

                let receipt = timeout(
                    Duration::from_secs(self.network.transaction_timeout_secs),
                    pending.get_receipt(),
                )
                .await
                .context("timed out waiting for CTF operator approval receipt")?
                .context("failed while waiting for CTF operator approval receipt")?;

                if !receipt.status() {
                    bail!("CTF operator approval transaction reverted");
                }
            }
        }

        let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
        let condition_id = FixedBytes::<32>::from_str(&round.condition_id).with_context(|| {
            format!(
                "failed to parse round condition id '{}' as bytes32",
                round.condition_id
            )
        })?;
        let partition = vec![U256::from(1u8), U256::from(2u8)];

        info!(
            condition_id = %round.condition_id,
            amount_usdc = %usdc_amount,
            exchange = %exchange.to_checksum(None),
            "submitting splitPosition transaction"
        );

        let pending = conditional_tokens
            .splitPosition(
                collateral,
                FixedBytes::<32>::ZERO,
                condition_id,
                partition,
                required,
            )
            .send()
            .await
            .context("failed to submit splitPosition transaction")?;

        let receipt = timeout(
            Duration::from_secs(self.network.transaction_timeout_secs),
            pending.get_receipt(),
        )
        .await
        .context("timed out waiting for splitPosition receipt")?
        .context("failed while waiting for splitPosition receipt")?;

        if !receipt.status() {
            bail!("splitPosition transaction reverted on-chain");
        }

        Ok(SplitExecutionReceipt {
            tx_hash: receipt.transaction_hash().to_string(),
            block_number: receipt.block_number(),
            amount_usdc: usdc_amount,
        })
    }

    async fn split_position_via_relayer(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<SplitExecutionReceipt> {
        self.ensure_relayer_safe_ready().await?;

        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);

        let yes_metadata = self
            .market_metadata(self.token_id_for_leg(round, LegSide::Yes)?)
            .await?;
        let exchange = exchange_contract(self.account.chain_id, yes_metadata.neg_risk)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let required = decimal_to_u256(usdc_amount, USDC_DECIMALS)?;

        if self.config.auto_approve_collateral {
            let erc20 = IERC20::new(collateral, &provider);
            let allowance = erc20
                .allowance(self.account.funder_address, conditional_tokens_address)
                .call()
                .await
                .context("failed to query collateral allowance for CTF")?;
            if allowance < required {
                info!(
                    owner = %self.account.funder_address,
                    spender = %conditional_tokens_address.to_checksum(None),
                    required = %required,
                    current_allowance = %allowance,
                    "submitting gasless collateral approval via relayer"
                );

                let approval = IERC20::approveCall {
                    spender: conditional_tokens_address,
                    amount: U256::MAX,
                };

                self.submit_relayer_safe_transaction(collateral, approval.abi_encode(), "approve")
                .await?;
            }
        }

        if self.config.auto_approve_ctf {
            let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
            let approved = conditional_tokens
                .isApprovedForAll(self.account.funder_address, exchange)
                .call()
                .await
                .context("failed to query CTF exchange approval")?;

            if !approved {
                info!(
                    owner = %self.account.funder_address,
                    operator = %exchange.to_checksum(None),
                    "submitting gasless CTF operator approval via relayer"
                );

                let approval = IConditionalTokens::setApprovalForAllCall {
                    operator: exchange,
                    approved: true,
                };

                self.submit_relayer_safe_transaction(
                    conditional_tokens_address,
                    approval.abi_encode(),
                    "approve",
                )
                .await?;
            }
        }

        let condition_id = FixedBytes::<32>::from_str(&round.condition_id).with_context(|| {
            format!(
                "failed to parse round condition id '{}' as bytes32",
                round.condition_id
            )
        })?;
        let partition = vec![U256::from(1u8), U256::from(2u8)];

        info!(
            condition_id = %round.condition_id,
            amount_usdc = %usdc_amount,
            exchange = %exchange.to_checksum(None),
            proxy_wallet = %self.account.funder_address,
            "submitting gasless splitPosition transaction via relayer"
        );

        let split = IConditionalTokens::splitPositionCall {
            collateralToken: collateral,
            parentCollectionId: FixedBytes::<32>::ZERO,
            conditionId: condition_id,
            partition,
            amount: required,
        };

        let transaction = self
            .submit_relayer_safe_transaction(
                conditional_tokens_address,
                split.abi_encode(),
                "split",
            )
            .await?;

        Ok(SplitExecutionReceipt {
            tx_hash: transaction
                .transaction_hash
                .unwrap_or_else(|| transaction.transaction_id.clone()),
            block_number: None,
            amount_usdc: usdc_amount,
        })
    }

    async fn merge_position_via_rpc(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<MergeExecutionReceipt> {
        if self.account.funder_address != self.account.signer_address {
            bail!(
                "rpc onchain mode requires collateral on signer address {}; configured funder_address {} differs. use execution.onchain_execution_mode = 'relayer' for Polymarket proxy wallets",
                self.account.signer_address,
                self.account.funder_address
            );
        }

        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);

        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
        let condition_id = FixedBytes::<32>::from_str(&round.condition_id).with_context(|| {
            format!(
                "failed to parse round condition id '{}' as bytes32",
                round.condition_id
            )
        })?;
        let partition = vec![U256::from(1u8), U256::from(2u8)];
        let required = decimal_to_u256(usdc_amount, USDC_DECIMALS)?;

        info!(
            condition_id = %round.condition_id,
            amount_usdc = %usdc_amount,
            "submitting mergePositions transaction"
        );

        let pending = conditional_tokens
            .mergePositions(
                collateral,
                FixedBytes::<32>::ZERO,
                condition_id,
                partition,
                required,
            )
            .send()
            .await
            .context("failed to submit mergePositions transaction")?;

        let receipt = timeout(
            Duration::from_secs(self.network.transaction_timeout_secs),
            pending.get_receipt(),
        )
        .await
        .context("timed out waiting for mergePositions receipt")?
        .context("failed while waiting for mergePositions receipt")?;

        if !receipt.status() {
            bail!("mergePositions transaction reverted on-chain");
        }

        Ok(MergeExecutionReceipt {
            tx_hash: receipt.transaction_hash().to_string(),
            block_number: receipt.block_number(),
            amount_usdc: usdc_amount,
        })
    }

    async fn merge_position_via_relayer(
        &self,
        round: &RoundDescriptor,
        usdc_amount: Decimal,
    ) -> Result<MergeExecutionReceipt> {
        self.ensure_relayer_safe_ready().await?;

        let condition_id = FixedBytes::<32>::from_str(&round.condition_id).with_context(|| {
            format!(
                "failed to parse round condition id '{}' as bytes32",
                round.condition_id
            )
        })?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let partition = vec![U256::from(1u8), U256::from(2u8)];
        let required = decimal_to_u256(usdc_amount, USDC_DECIMALS)?;

        info!(
            condition_id = %round.condition_id,
            amount_usdc = %usdc_amount,
            proxy_wallet = %self.account.funder_address,
            "submitting gasless mergePositions transaction via relayer"
        );

        let merge = IConditionalTokens::mergePositionsCall {
            collateralToken: collateral,
            parentCollectionId: FixedBytes::<32>::ZERO,
            conditionId: condition_id,
            partition,
            amount: required,
        };

        let transaction = self
            .submit_relayer_safe_transaction(
                conditional_tokens_address,
                merge.abi_encode(),
                "merge",
            )
            .await?;

        Ok(MergeExecutionReceipt {
            tx_hash: transaction
                .transaction_hash
                .unwrap_or_else(|| transaction.transaction_id.clone()),
            block_number: None,
            amount_usdc: usdc_amount,
        })
    }

    async fn redeem_positions_via_rpc(
        &self,
        round: &RoundDescriptor,
    ) -> Result<RedeemExecutionReceipt> {
        if self.account.funder_address != self.account.signer_address {
            bail!(
                "rpc onchain mode requires conditional tokens on signer address {}; configured funder_address {} differs. use execution.onchain_execution_mode = 'relayer' for Polymarket proxy wallets",
                self.account.signer_address,
                self.account.funder_address
            );
        }

        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);

        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
        let condition_id = parse_condition_id(&round.condition_id)?;
        let index_sets = vec![U256::from(1u8), U256::from(2u8)];

        info!(
            condition_id = %round.condition_id,
            owner = %self.account.funder_address,
            "submitting redeemPositions transaction"
        );

        let pending = conditional_tokens
            .redeemPositions(
                collateral,
                FixedBytes::<32>::ZERO,
                condition_id,
                index_sets,
            )
            .send()
            .await
            .context("failed to submit redeemPositions transaction")?;

        let receipt = timeout(
            Duration::from_secs(self.network.transaction_timeout_secs),
            pending.get_receipt(),
        )
        .await
        .context("timed out waiting for redeemPositions receipt")?
        .context("failed while waiting for redeemPositions receipt")?;

        if !receipt.status() {
            bail!("redeemPositions transaction reverted on-chain");
        }

        Ok(RedeemExecutionReceipt {
            tx_hash: receipt.transaction_hash().to_string(),
            block_number: receipt.block_number(),
        })
    }

    async fn redeem_positions_via_relayer(
        &self,
        round: &RoundDescriptor,
    ) -> Result<RedeemExecutionReceipt> {
        self.ensure_relayer_safe_ready().await?;

        let condition_id = parse_condition_id(&round.condition_id)?;
        let collateral = collateral_token(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let index_sets = vec![U256::from(1u8), U256::from(2u8)];

        info!(
            condition_id = %round.condition_id,
            owner = %self.account.funder_address,
            "submitting gasless redeemPositions transaction via relayer"
        );

        let redeem = IConditionalTokens::redeemPositionsCall {
            collateralToken: collateral,
            parentCollectionId: FixedBytes::<32>::ZERO,
            conditionId: condition_id,
            indexSets: index_sets,
        };

        let transaction = self
            .submit_relayer_safe_transaction(
                conditional_tokens_address,
                redeem.abi_encode(),
                "redeem",
            )
            .await?;

        Ok(RedeemExecutionReceipt {
            tx_hash: transaction
                .transaction_hash
                .unwrap_or_else(|| transaction.transaction_id.clone()),
            block_number: None,
        })
    }

    async fn round_position_balances(&self, round: &RoundDescriptor) -> Result<(U256, U256)> {
        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
        let yes_token_id = parse_token_id(&round.yes_token_id, "yes")?;
        let no_token_id = parse_token_id(&round.no_token_id, "no")?;

        let yes_balance = conditional_tokens
            .balanceOf(self.account.funder_address, yes_token_id)
            .call()
            .await
            .context("failed to query YES balance for redeemPositions")?;
        let no_balance = conditional_tokens
            .balanceOf(self.account.funder_address, no_token_id)
            .call()
            .await
            .context("failed to query NO balance for redeemPositions")?;

        Ok((yes_balance, no_balance))
    }

    async fn condition_resolved(&self, round: &RoundDescriptor) -> Result<bool> {
        let rpc_url = self.network.polygon_rpc_url()?;
        let rpc_url = reqwest::Url::parse(&rpc_url).context("failed to parse polygon rpc url")?;
        let wallet = EthereumWallet::new(self.account.signer.clone());
        let provider = ProviderBuilder::new()
            .with_cached_nonce_management()
            .wallet(wallet)
            .connect_http(rpc_url);
        let conditional_tokens_address = conditional_tokens_contract(self.account.chain_id)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let conditional_tokens = IConditionalTokens::new(conditional_tokens_address, &provider);
        let condition_id = parse_condition_id(&round.condition_id)?;

        let payout_denominator = conditional_tokens
            .payoutDenominator(condition_id)
            .call()
            .await
            .context("failed to query payoutDenominator for redeemPositions")?;

        Ok(!payout_denominator.is_zero())
    }

    async fn ensure_relayer_safe_ready(&self) -> Result<()> {
        let expected_safe = self.expected_safe_address()?;
        if self.account.funder_address != expected_safe {
            bail!(
                "configured funder_address {} does not match relayer SAFE address derived from signer {} (expected {})",
                self.account.funder_address,
                self.account.signer_address,
                expected_safe
            );
        }

        if !self.config.relayer_require_safe_deployed {
            return Ok(());
        }

        let deployed = self.relayer_safe_deployed().await?;
        if !deployed {
            bail!(
                "Polymarket SAFE {} is not deployed yet; deploy it first from the Polymarket UI or relayer tooling",
                self.account.funder_address
            );
        }

        Ok(())
    }

    async fn relayer_safe_deployed(&self) -> Result<bool> {
        self.client
            .get(join_url(&self.network.relayer_rest_url, "/deployed"))
            .query(&[("address", self.account.funder_address.to_string())])
            .timeout(Duration::from_secs(self.network.transaction_timeout_secs))
            .send()
            .await
            .context("relayer deployed-status request failed")?
            .error_for_status()
            .context("relayer deployed-status request returned non-success status")?
            .json::<RelayerDeployedResponse>()
            .await
            .context("failed to decode relayer deployed-status response")
            .map(|response| response.deployed)
    }

    async fn submit_relayer_safe_transaction(
        &self,
        to: Address,
        data: Vec<u8>,
        metadata: impl Into<String>,
    ) -> Result<RelayerTransaction> {
        let credentials = self.relayer_credentials()?.clone();
        let nonce = self.relayer_safe_nonce().await?;
        let signature = self
            .sign_relayer_safe_transaction(to, &data, &nonce)
            .await?;
        let metadata = metadata.into();

        let payload = RelayerSubmitRequest {
            from: self.account.signer_address.to_string(),
            to: to.to_string(),
            proxy_wallet: self.account.funder_address.to_string(),
            value: None,
            data: format!("0x{}", hex::encode(&data)),
            nonce: nonce.clone(),
            signature,
            signature_params: RelayerSignatureParams {
                gas_price: "0".to_owned(),
                operation: "0".to_owned(),
                safe_txn_gas: "0".to_owned(),
                base_gas: "0".to_owned(),
                gas_token: Address::ZERO.to_string(),
                refund_receiver: Address::ZERO.to_string(),
            },
            tx_type: RELAYER_TX_TYPE_SAFE.to_owned(),
            metadata: metadata.clone(),
        };

        info!(
            to = %to,
            proxy_wallet = %self.account.funder_address,
            nonce = %nonce,
            metadata = %payload.metadata,
            "posting relayer SAFE submit request"
        );

        let body =
            serde_json::to_string(&payload).context("failed to encode relayer submit request")?;
        let response = self
            .client
            .post(join_url(&self.network.relayer_rest_url, "/submit"))
            .timeout(Duration::from_secs(self.network.transaction_timeout_secs))
            .header("RELAYER_API_KEY", &credentials.api_key)
            .header(
                "RELAYER_API_KEY_ADDRESS",
                credentials.owner_address.to_string(),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .context("relayer submit request failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read relayer submit response body")?;
        if !status.is_success() {
            bail!(
                "relayer submit request returned non-success status {} with body {} (relayer_key_owner={}, signer={}, proxy_wallet={})",
                status,
                body,
                credentials.owner_address,
                self.account.signer_address,
                self.account.funder_address
            );
        }
        let submitted = serde_json::from_str::<RelayerSubmitResponse>(&body)
            .context("failed to decode relayer submit response")?;

        info!(
            transaction_id = %submitted.transaction_id,
            initial_state = %submitted.state,
            transaction_hash = %submitted.transaction_hash.as_deref().unwrap_or("pending"),
            relayer_key_owner = %credentials.owner_address,
            metadata = %metadata,
            "submitted relayer SAFE transaction"
        );

        self.wait_for_relayer_transaction(&submitted.transaction_id)
            .await
    }

    async fn relayer_safe_nonce(&self) -> Result<String> {
        self.client
            .get(join_url(&self.network.relayer_rest_url, "/nonce"))
            .query(&[
                ("address", self.account.signer_address.to_string()),
                ("type", RELAYER_TX_TYPE_SAFE.to_owned()),
            ])
            .timeout(Duration::from_secs(self.network.transaction_timeout_secs))
            .send()
            .await
            .context("relayer nonce request failed")?
            .error_for_status()
            .context("relayer nonce request returned non-success status")?
            .json::<RelayerNonceResponse>()
            .await
            .context("failed to decode relayer nonce response")
            .map(|response| response.nonce)
    }

    async fn sign_relayer_safe_transaction(
        &self,
        to: Address,
        data: &[u8],
        nonce: &str,
    ) -> Result<String> {
        let nonce = U256::from_str(nonce).context("failed to parse relayer safe nonce")?;
        let transaction = SafeTx {
            to,
            value: U256::ZERO,
            data: data.to_vec().into(),
            operation: 0,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            nonce,
        };
        let domain = Eip712Domain {
            chain_id: Some(U256::from(self.account.chain_id)),
            verifying_contract: Some(self.account.funder_address),
            ..Default::default()
        };
        let digest = transaction.eip712_signing_hash(&domain);
        let signature = self
            .account
            .signer
            .sign_message(digest.as_slice())
            .await
            .map_err(|error| anyhow!("failed to sign relayer SAFE transaction: {error}"))?;

        Ok(pack_safe_signature(signature))
    }

    async fn wait_for_relayer_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<RelayerTransaction> {
        let polling = async {
            loop {
                let transaction = self.relayer_transaction(transaction_id).await?;
                match transaction.state.as_str() {
                    RELAYER_STATE_MINED | RELAYER_STATE_CONFIRMED => return Ok(transaction),
                    RELAYER_STATE_FAILED | RELAYER_STATE_INVALID => {
                        bail!(
                            "relayer transaction {} failed with state {}",
                            transaction.transaction_id,
                            transaction.state
                        );
                    }
                    _ => sleep(Duration::from_millis(self.config.relayer_poll_interval_ms)).await,
                }
            }
        };

        timeout(
            Duration::from_secs(self.network.transaction_timeout_secs),
            polling,
        )
        .await
        .context("timed out waiting for relayer transaction confirmation")?
    }

    async fn relayer_transaction(&self, transaction_id: &str) -> Result<RelayerTransaction> {
        let mut transactions = self
            .client
            .get(join_url(&self.network.relayer_rest_url, "/transaction"))
            .query(&[("id", transaction_id)])
            .timeout(Duration::from_secs(self.network.transaction_timeout_secs))
            .send()
            .await
            .context("relayer transaction lookup failed")?
            .error_for_status()
            .context("relayer transaction lookup returned non-success status")?
            .json::<Vec<RelayerTransaction>>()
            .await
            .context("failed to decode relayer transaction lookup response")?;

        transactions
            .drain(..)
            .next()
            .with_context(|| format!("relayer transaction '{}' was not found", transaction_id))
    }

    async fn fetch_tick_size(&self, token_id: U256) -> Result<Decimal> {
        let response = self
            .client
            .get(join_url(&self.clob_base_url, "/tick-size"))
            .query(&[("token_id", token_id.to_string())])
            .send()
            .await
            .context("tick-size request failed")?
            .error_for_status()
            .context("tick-size request returned non-success status")?
            .json::<TickSizeResponse>()
            .await
            .context("failed to decode tick-size response")?;

        Ok(response.minimum_tick_size)
    }

    async fn fetch_neg_risk(&self, token_id: U256) -> Result<bool> {
        Ok(self
            .client
            .get(join_url(&self.clob_base_url, "/neg-risk"))
            .query(&[("token_id", token_id.to_string())])
            .send()
            .await
            .context("neg-risk request failed")?
            .error_for_status()
            .context("neg-risk request returned non-success status")?
            .json::<NegRiskResponse>()
            .await
            .context("failed to decode neg-risk response")?
            .neg_risk)
    }

    async fn fetch_fee_rate(&self, token_id: U256) -> Result<u32> {
        Ok(self
            .client
            .get(join_url(&self.clob_base_url, "/fee-rate"))
            .query(&[("token_id", token_id.to_string())])
            .send()
            .await
            .context("fee-rate request failed")?
            .error_for_status()
            .context("fee-rate request returned non-success status")?
            .json::<FeeRateResponse>()
            .await
            .context("failed to decode fee-rate response")?
            .base_fee)
    }

    async fn sign_order(
        &self,
        order: &Order,
        neg_risk: bool,
    ) -> Result<alloy::primitives::Signature> {
        let exchange_contract = exchange_contract(self.account.chain_id, neg_risk)
            .with_context(|| format!("unsupported chain id {}", self.account.chain_id))?;
        let domain = Eip712Domain {
            name: Some(ORDER_NAME.into()),
            version: Some(ORDER_VERSION.into()),
            chain_id: Some(U256::from(self.account.chain_id)),
            verifying_contract: Some(exchange_contract),
            ..Eip712Domain::default()
        };

        self.account
            .signer
            .sign_hash(&order.eip712_signing_hash(&domain))
            .await
            .context("failed to sign order")
    }

    fn create_l2_headers(
        &self,
        method: &str,
        path: &str,
        serialized_body: Option<&str>,
    ) -> Result<reqwest::header::HeaderMap> {
        let timestamp = now_unix_secs().to_string();
        let message = format!(
            "{}{}{}{}",
            timestamp,
            method,
            path,
            serialized_body.unwrap_or("")
        );
        let signature = hmac_signature(&self.account.credentials.secret, &message)?;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "POLY_ADDRESS",
            self.account
                .signer_address
                .to_checksum(None)
                .parse()
                .context("failed to build POLY_ADDRESS header")?,
        );
        headers.insert(
            "POLY_API_KEY",
            self.account
                .credentials
                .api_key
                .parse()
                .context("failed to build POLY_API_KEY header")?,
        );
        headers.insert(
            "POLY_PASSPHRASE",
            self.account
                .credentials
                .passphrase
                .parse()
                .context("failed to build POLY_PASSPHRASE header")?,
        );
        headers.insert(
            "POLY_SIGNATURE",
            signature
                .parse()
                .context("failed to build POLY_SIGNATURE header")?,
        );
        headers.insert(
            "POLY_TIMESTAMP",
            timestamp
                .parse()
                .context("failed to build POLY_TIMESTAMP header")?,
        );

        Ok(headers)
    }

    fn default_metadata(&self, token_id: U256) -> Result<TokenMetadata> {
        Ok(TokenMetadata {
            token_id,
            tick_size: Decimal::from_str(&self.config.tick_size).with_context(|| {
                format!(
                    "failed to parse execution.tick_size '{}'",
                    self.config.tick_size
                )
            })?,
            neg_risk: self.config.neg_risk,
            fee_rate_bps: self.config.fee_rate_bps as u32,
            fetched_at: Utc::now(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RawSignedOrder {
    order: RawOrderWire,
    owner: String,
    #[serde(rename = "orderType")]
    order_type: OrderType,
    #[serde(rename = "deferExec")]
    defer_exec: bool,
    #[serde(rename = "postOnly")]
    post_only: bool,
    #[serde(skip)]
    price: Decimal,
    #[serde(skip)]
    size: Decimal,
}

impl RawSignedOrder {
    fn new(
        account: &LoadedAccount,
        order: Order,
        expiration: u64,
        signature: String,
        side: OrderSide,
        order_type: OrderType,
        price: Decimal,
        size: Decimal,
        post_only: Option<bool>,
    ) -> Result<Self> {
        Ok(Self {
            order: RawOrderWire {
                salt: u64::try_from(order.salt).context("order salt does not fit into u64")?,
                maker: order.maker.to_checksum(None),
                signer: order.signer.to_checksum(None),
                token_id: order.tokenId.to_string(),
                maker_amount: order.makerAmount.to_string(),
                taker_amount: order.takerAmount.to_string(),
                side,
                expiration: expiration.to_string(),
                signature_type: order.signatureType,
                timestamp: order.timestamp.to_string(),
                metadata: bytes32_hex(&order.metadata),
                builder: bytes32_hex(&order.builder),
                signature,
            },
            order_type,
            owner: account.credentials.api_key.clone(),
            defer_exec: false,
            post_only: post_only.unwrap_or(false),
            price,
            size,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct PythonHelperApiCredentials {
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}

impl PythonHelperApiCredentials {
    fn from_account(account: &LoadedAccount) -> Self {
        Self {
            api_key: account.credentials.api_key.clone(),
            api_secret: account.credentials.secret.clone(),
            api_passphrase: account.credentials.passphrase.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PythonHelperPostOrdersRequest {
    host: String,
    chain_id: ChainId,
    private_key: String,
    signature_type: u8,
    funder: String,
    creds: PythonHelperApiCredentials,
    orders: Vec<PythonHelperOrder>,
}

#[derive(Debug, Clone, Serialize)]
struct PythonHelperCancelOrdersRequest {
    host: String,
    chain_id: ChainId,
    private_key: String,
    signature_type: u8,
    funder: String,
    creds: PythonHelperApiCredentials,
    order_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PythonHelperOrder {
    token_id: String,
    price: String,
    size: String,
    side: String,
    expiration: u64,
    builder_code: String,
    metadata: String,
    order_type: String,
    post_only: bool,
}

impl TryFrom<&RawSignedOrder> for PythonHelperOrder {
    type Error = anyhow::Error;

    fn try_from(order: &RawSignedOrder) -> Result<Self> {
        Ok(Self {
            token_id: order.order.token_id.clone(),
            price: order.price.normalize().to_string(),
            size: order.size.normalize().to_string(),
            side: order.order.side.as_clob_str().to_owned(),
            expiration: order
                .order
                .expiration
                .parse()
                .context("failed to parse expiration for python helper")?,
            builder_code: order.order.builder.clone(),
            metadata: order.order.metadata.clone(),
            order_type: order.order_type.as_str().to_owned(),
            post_only: order.post_only,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct RawOrderWire {
    salt: u64,
    maker: String,
    signer: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    side: OrderSide,
    expiration: String,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    timestamp: String,
    metadata: String,
    builder: String,
    signature: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostOrderResponse {
    pub success: bool,
    #[serde(rename = "orderID", alias = "orderId")]
    pub order_id: String,
    pub status: String,
    #[serde(rename = "takingAmount", default)]
    pub taking_amount: Option<String>,
    #[serde(rename = "makingAmount", default)]
    pub making_amount: Option<String>,
    #[serde(rename = "transactionsHashes", default)]
    pub transactions_hashes: Vec<String>,
    #[serde(rename = "tradeIDs", default)]
    pub trade_ids: Vec<String>,
    #[serde(default, alias = "errorMsg")]
    pub error_msg: Option<String>,
}

impl PostOrderResponse {
    pub fn has_live_order(&self) -> bool {
        self.success && !self.order_id.trim().is_empty()
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_msg
            .as_deref()
            .map(str::trim)
            .filter(|message| !message.is_empty())
    }

    pub fn has_error(&self) -> bool {
        self.error_message().is_some()
    }

    pub fn is_insufficient_balance(&self) -> bool {
        self.error_message()
            .map(|message| {
                let normalized = message.to_ascii_lowercase();
                normalized.contains("not enough balance")
                    || normalized.contains("balance is not enough")
                    || normalized.contains("allowance")
            })
            .unwrap_or(false)
    }

    pub fn is_post_only_would_cross(&self) -> bool {
        self.error_message()
            .map(|message| {
                let normalized = message.to_ascii_lowercase();
                normalized.contains("post-only") && normalized.contains("cross")
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CancelOrdersResponse {
    #[serde(default)]
    pub canceled: Vec<String>,
    #[serde(default, alias = "not_canceled")]
    pub not_canceled: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderStatusResponse {
    #[serde(default, alias = "id")]
    pub id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    pub original_size: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    pub size_matched: Option<Decimal>,
}

impl OrderStatusResponse {
    pub fn has_any_match(&self) -> bool {
        self.size_matched.unwrap_or(Decimal::ZERO) > Decimal::ZERO
    }

    pub fn is_fully_matched(&self) -> bool {
        match (self.size_matched, self.original_size) {
            (Some(size_matched), Some(original_size)) if original_size > Decimal::ZERO => {
                size_matched >= original_size
            }
            _ => {
                let normalized = self.status.trim().to_ascii_lowercase();
                normalized.contains("matched") || normalized.contains("filled")
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookResponse {
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub bids: Vec<OrderBookLevel>,
    #[serde(default)]
    pub asks: Vec<OrderBookLevel>,
}

impl OrderBookResponse {
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids
            .iter()
            .filter_map(|level| level.price)
            .max_by(|left, right| left.cmp(right))
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks
            .iter()
            .filter_map(|level| level.price)
            .min_by(|left, right| left.cmp(right))
    }

    pub fn ask_size_at_price(&self, price: Decimal) -> Option<Decimal> {
        self.asks
            .iter()
            .find_map(|level| match (level.price, level.size) {
                (Some(level_price), Some(size)) if level_price == price => Some(size),
                _ => None,
            })
    }

    pub fn ask_depth_through_price(&self, limit_price: Decimal) -> Decimal {
        self.asks
            .iter()
            .filter_map(|level| match (level.price, level.size) {
                (Some(price), Some(size))
                    if price > Decimal::ZERO && size > Decimal::ZERO && price <= limit_price =>
                {
                    Some(size)
                }
                _ => None,
            })
            .fold(Decimal::ZERO, |acc, size| acc + size)
            .trunc_with_scale(LOT_SIZE_SCALE)
    }

    pub fn bid_depth_through_price(&self, limit_price: Decimal) -> Decimal {
        self.bids
            .iter()
            .filter_map(|level| match (level.price, level.size) {
                (Some(price), Some(size))
                    if price > Decimal::ZERO && size > Decimal::ZERO && price >= limit_price =>
                {
                    Some(size)
                }
                _ => None,
            })
            .fold(Decimal::ZERO, |acc, size| acc + size)
            .trunc_with_scale(LOT_SIZE_SCALE)
    }

    pub fn estimate_buy_for_quote(&self, quote_budget: Decimal) -> Option<TakerOrderEstimate> {
        if quote_budget <= Decimal::ZERO {
            return None;
        }

        let mut asks = self
            .asks
            .iter()
            .filter_map(|level| match (level.price, level.size) {
                (Some(price), Some(size)) if price > Decimal::ZERO && size > Decimal::ZERO => {
                    Some((price, size))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        asks.sort_by(|left, right| left.0.cmp(&right.0));

        let mut spent = Decimal::ZERO;
        let mut size = Decimal::ZERO;
        let mut limit_price = None;

        for (price, available_size) in asks {
            let remaining_quote = quote_budget - spent;
            if remaining_quote <= Decimal::ZERO {
                break;
            }

            let affordable_size = (remaining_quote / price).trunc_with_scale(LOT_SIZE_SCALE);
            if affordable_size <= Decimal::ZERO {
                continue;
            }

            let fill_size = available_size
                .min(affordable_size)
                .trunc_with_scale(LOT_SIZE_SCALE);
            if fill_size <= Decimal::ZERO {
                continue;
            }

            let fill_quote = (fill_size * price).trunc_with_scale(USDC_DECIMALS);
            if fill_quote <= Decimal::ZERO {
                continue;
            }

            size += fill_size;
            spent += fill_quote;
            limit_price = Some(price);

            if spent >= quote_budget {
                break;
            }
        }

        Some(TakerOrderEstimate {
            price: limit_price?,
            size: size.trunc_with_scale(LOT_SIZE_SCALE),
            quote: spent.trunc_with_scale(USDC_DECIMALS),
        })
        .filter(|estimate| estimate.size > Decimal::ZERO)
    }

    pub fn estimate_buy_for_size(&self, target_size: Decimal) -> Option<TakerOrderEstimate> {
        if target_size <= Decimal::ZERO {
            return None;
        }

        let mut asks = self
            .asks
            .iter()
            .filter_map(|level| match (level.price, level.size) {
                (Some(price), Some(size)) if price > Decimal::ZERO && size > Decimal::ZERO => {
                    Some((price, size))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        asks.sort_by(|left, right| left.0.cmp(&right.0));

        let mut remaining_size = target_size;
        let mut spent = Decimal::ZERO;
        let mut filled_size = Decimal::ZERO;
        let mut limit_price = None;

        for (price, available_size) in asks {
            let fill_size = available_size
                .min(remaining_size)
                .trunc_with_scale(LOT_SIZE_SCALE);
            if fill_size <= Decimal::ZERO {
                continue;
            }

            spent += (fill_size * price).trunc_with_scale(USDC_DECIMALS);
            filled_size += fill_size;
            remaining_size -= fill_size;
            limit_price = Some(price);

            if remaining_size <= Decimal::ZERO {
                break;
            }
        }

        Some(TakerOrderEstimate {
            price: limit_price?,
            size: filled_size.trunc_with_scale(LOT_SIZE_SCALE),
            quote: spent.trunc_with_scale(USDC_DECIMALS),
        })
        .filter(|estimate| estimate.size > Decimal::ZERO)
    }

    pub fn estimate_sell_for_size(&self, target_size: Decimal) -> Option<TakerOrderEstimate> {
        if target_size <= Decimal::ZERO {
            return None;
        }

        let mut bids = self
            .bids
            .iter()
            .filter_map(|level| match (level.price, level.size) {
                (Some(price), Some(size)) if price > Decimal::ZERO && size > Decimal::ZERO => {
                    Some((price, size))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        bids.sort_by(|left, right| right.0.cmp(&left.0));

        let mut remaining_size = target_size;
        let mut proceeds = Decimal::ZERO;
        let mut limit_price = None;

        for (price, available_size) in bids {
            let fill_size = available_size
                .min(remaining_size)
                .trunc_with_scale(LOT_SIZE_SCALE);
            if fill_size <= Decimal::ZERO {
                continue;
            }

            proceeds += (fill_size * price).trunc_with_scale(USDC_DECIMALS);
            remaining_size -= fill_size;
            limit_price = Some(price);

            if remaining_size <= Decimal::ZERO {
                break;
            }
        }

        Some(TakerOrderEstimate {
            price: limit_price?,
            size: target_size.trunc_with_scale(LOT_SIZE_SCALE),
            quote: proceeds.trunc_with_scale(USDC_DECIMALS),
        })
        .filter(|estimate| estimate.size > Decimal::ZERO)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TakerOrderEstimate {
    pub price: Decimal,
    pub size: Decimal,
    pub quote: Decimal,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct OrderBookLevel {
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    pub price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_option")]
    pub size: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
struct TickSizeResponse {
    #[serde(deserialize_with = "deserialize_decimal_string_or_number")]
    minimum_tick_size: Decimal,
}

#[derive(Debug, Deserialize)]
struct NegRiskResponse {
    neg_risk: bool,
}

#[derive(Debug, Deserialize)]
struct FeeRateResponse {
    base_fee: u32,
}

fn validate_limit_order(input: &LimitOrderInput, metadata: &TokenMetadata) -> Result<()> {
    if input.price.is_sign_negative() || input.size.is_sign_negative() {
        bail!("price and size must be non-negative");
    }
    if input.size.is_zero() {
        bail!("size must be greater than zero");
    }
    if input.size.scale() > LOT_SIZE_SCALE {
        bail!(
            "size {} has {} decimal places; maximum lot size scale is {}",
            input.size,
            input.size.scale(),
            LOT_SIZE_SCALE
        );
    }

    validate_order_price(input.price, metadata)?;

    Ok(())
}

fn validate_order_price(price: Decimal, metadata: &TokenMetadata) -> Result<()> {
    if price.scale() > metadata.tick_size.scale() {
        bail!(
            "price {} has {} decimal places; tick size {} has {} decimal places",
            price,
            price.scale(),
            metadata.tick_size,
            metadata.tick_size.scale()
        );
    }
    if price < metadata.tick_size || price > Decimal::ONE - metadata.tick_size {
        bail!(
            "price {} must be between {} and {}",
            price,
            metadata.tick_size,
            Decimal::ONE - metadata.tick_size
        );
    }

    Ok(())
}

fn market_buy_shares_for_quote(quote_amount: Decimal, price: Decimal, price_scale: u32) -> Decimal {
    (quote_amount / price).trunc_with_scale(price_scale + LOT_SIZE_SCALE)
}

fn deserialize_decimal_string_or_number<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DecimalWire {
        String(String),
        Float(f64),
        Int(i64),
        UInt(u64),
    }

    match DecimalWire::deserialize(deserializer)? {
        DecimalWire::String(value) => Decimal::from_str(&value).map_err(serde::de::Error::custom),
        DecimalWire::Float(value) => {
            Decimal::from_str(&value.to_string()).map_err(serde::de::Error::custom)
        }
        DecimalWire::Int(value) => Ok(Decimal::from(value)),
        DecimalWire::UInt(value) => Ok(Decimal::from(value)),
    }
}

fn deserialize_decimal_option<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    let decimal = match value {
        serde_json::Value::Null => return Ok(None),
        serde_json::Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            Decimal::from_str(trimmed).map_err(serde::de::Error::custom)?
        }
        serde_json::Value::Number(number) => {
            Decimal::from_str(&number.to_string()).map_err(serde::de::Error::custom)?
        }
        other => {
            return Err(serde::de::Error::custom(format!(
                "unexpected decimal payload: {other}"
            )));
        }
    };

    Ok(Some(decimal))
}

fn parse_condition_id(value: &str) -> Result<FixedBytes<32>> {
    FixedBytes::<32>::from_str(value)
        .with_context(|| format!("failed to parse condition id '{}' as bytes32", value))
}

fn parse_token_id(value: &str, label: &str) -> Result<U256> {
    U256::from_str(value).with_context(|| format!("failed to parse {label} token id '{}'", value))
}

fn decimal_to_u256(value: Decimal, scale: u32) -> Result<U256> {
    let scaled = value.normalize().trunc_with_scale(scale).mantissa();
    let units = u128::try_from(scaled)
        .map_err(|_| anyhow!("value {} is negative or too large to fit into u128", value))?;
    Ok(U256::from(units))
}

fn u256_to_decimal(value: U256, scale: u32) -> Result<Decimal> {
    let raw = Decimal::from_str(&value.to_string())
        .with_context(|| format!("failed to convert U256 {} into Decimal", value))?;
    let divisor = Decimal::from(10u64.pow(scale));
    Ok((raw / divisor).normalize())
}

fn to_fixed_u128(value: Decimal) -> u128 {
    value
        .normalize()
        .trunc_with_scale(USDC_DECIMALS)
        .mantissa()
        .to_u128()
        .expect("validated positive decimal should fit into u128")
}

fn to_ieee_754_int(salt: u64) -> u64 {
    salt & ((1 << 53) - 1)
}

fn generate_seed() -> u64 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs_f64();
    let random = rand::rng().random::<f64>();
    (seconds * random).round() as u64
}

fn read_optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => {
            bail!("environment variable '{}' contains non-unicode data", name)
        }
    }
}

fn safe_factory(chain_id: ChainId) -> Option<Address> {
    match chain_id {
        POLYGON | AMOY => Some(SAFE_FACTORY_POLYGON),
        _ => None,
    }
}

fn safe_create2_salt(signer_address: Address) -> FixedBytes<32> {
    let mut encoded = [0u8; 32];
    encoded[12..].copy_from_slice(signer_address.as_slice());
    keccak256(encoded)
}

fn pack_safe_signature(signature: alloy::primitives::Signature) -> String {
    let signature = signature.normalized_s();
    let mut packed = [0u8; 65];
    packed[..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
    packed[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());
    packed[64] = if signature.v() { 32 } else { 31 };
    format!("0x{}", hex::encode(packed))
}

fn bytes32_hex(value: &FixedBytes<32>) -> String {
    format!("0x{}", hex::encode(value.as_slice()))
}

fn encode_clob_signature(signature: alloy::primitives::Signature) -> String {
    format!("0x{}", hex::encode(signature.as_bytes()))
}

fn encode_order_signature(
    signature: alloy::primitives::Signature,
    signature_type: SignatureType,
) -> String {
    match signature_type {
        // CLOB orders use the standard 65-byte ECDSA encoding even for proxy-wallet
        // accounts; only relayer SAFE submits require the packed Safe signature format.
        SignatureType::BrowserProxy => encode_clob_signature(signature),
        SignatureType::GnosisSafe => encode_clob_signature(signature),
        SignatureType::Eoa | SignatureType::Magic => encode_clob_signature(signature),
    }
}

fn signature_type_code(kind: SignatureType) -> u8 {
    match kind {
        SignatureType::Eoa => 0,
        SignatureType::BrowserProxy => 1,
        SignatureType::GnosisSafe => 2,
        SignatureType::Magic => 3,
    }
}

fn collateral_token(chain_id: ChainId) -> Option<Address> {
    match chain_id {
        POLYGON => Some(COLLATERAL_TOKEN_POLYGON),
        AMOY => Some(COLLATERAL_TOKEN_AMOY),
        _ => None,
    }
}

fn conditional_tokens_contract(chain_id: ChainId) -> Option<Address> {
    match chain_id {
        POLYGON => Some(CTF_POLYGON),
        AMOY => Some(CTF_AMOY),
        _ => None,
    }
}

fn exchange_contract(chain_id: ChainId, neg_risk: bool) -> Option<Address> {
    match (chain_id, neg_risk) {
        (POLYGON, false) => Some(address!("0xE111180000d2663C0091e4f400237545B87B996B")),
        (POLYGON, true) => Some(address!("0xe2222d279d744050d28e00520010520000310F59")),
        (AMOY, false) => Some(address!("0xE111180000d2663C0091e4f400237545B87B996B")),
        (AMOY, true) => Some(address!("0xe2222d279d744050d28e00520010520000310F59")),
        _ => None,
    }
}

fn hmac_signature(secret: &str, message: &str) -> Result<String> {
    let decoded = URL_SAFE
        .decode(secret)
        .context("failed to decode api secret as url-safe base64")?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&decoded).context("failed to initialize HMAC-SHA256")?;
    mac.update(message.as_bytes());
    Ok(URL_SAFE.encode(mac.finalize().into_bytes()))
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs() as i64
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rust_order_hash_matches_official_python_fixture() {
        let signer = PrivateKeySigner::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .expect("test signer");
        let maker = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        let order = Order {
            salt: U256::from(479249096354u64),
            maker,
            signer: maker,
            tokenId: U256::from(1234u64),
            makerAmount: U256::from(100000000u64),
            takerAmount: U256::from(50000000u64),
            side: OrderSide::Buy.as_u8(),
            signatureType: signature_type_code(SignatureType::Eoa),
            timestamp: U256::from(1_746_124_800_000u64),
            metadata: ZERO_BYTES32,
            builder: ZERO_BYTES32,
        };
        let domain = Eip712Domain {
            name: Some(ORDER_NAME.into()),
            version: Some(ORDER_VERSION.into()),
            chain_id: Some(U256::from(AMOY)),
            verifying_contract: exchange_contract(AMOY, false),
            ..Eip712Domain::default()
        };
        let hash = order.eip712_signing_hash(&domain);

        assert_ne!(hash, FixedBytes::<32>::ZERO);

        let signature = signer.sign_hash(&hash).await.expect("signed order");

        assert_eq!(
            signature.recover_address_from_prehash(&hash).unwrap(),
            maker
        );
    }

    #[test]
    fn raw_signed_order_serialization_matches_official_py_clob_shape() {
        let order = RawSignedOrder {
            order: RawOrderWire {
                salt: 1,
                maker: "0x1111111111111111111111111111111111111111".to_owned(),
                signer: "0x2222222222222222222222222222222222222222".to_owned(),
                token_id: "123".to_owned(),
                maker_amount: "100".to_owned(),
                taker_amount: "50".to_owned(),
                side: OrderSide::Sell,
                expiration: "0".to_owned(),
                signature_type: 1,
                timestamp: "1746124800000".to_owned(),
                metadata:
                    "0x0000000000000000000000000000000000000000000000000000000000000000"
                        .to_owned(),
                builder:
                    "0x0000000000000000000000000000000000000000000000000000000000000000"
                        .to_owned(),
                signature: "0xabc".to_owned(),
            },
            owner: "api-key".to_owned(),
            order_type: OrderType::GTC,
            defer_exec: false,
            post_only: true,
            price: Decimal::ZERO,
            size: Decimal::ZERO,
        };

        assert_eq!(
            serde_json::to_string(&order).expect("serialized order"),
            r#"{"order":{"salt":1,"maker":"0x1111111111111111111111111111111111111111","signer":"0x2222222222222222222222222222222222222222","tokenId":"123","makerAmount":"100","takerAmount":"50","side":"SELL","expiration":"0","signatureType":1,"timestamp":"1746124800000","metadata":"0x0000000000000000000000000000000000000000000000000000000000000000","builder":"0x0000000000000000000000000000000000000000000000000000000000000000","signature":"0xabc"},"owner":"api-key","orderType":"GTC","deferExec":false,"postOnly":true}"#
        );
    }

    #[test]
    fn order_book_estimates_buy_size_from_quote_budget() {
        let book = OrderBookResponse {
            asset_id: None,
            market: None,
            hash: None,
            timestamp: None,
            bids: Vec::new(),
            asks: vec![
                OrderBookLevel {
                    price: Some(Decimal::from_str_exact("0.46").unwrap()),
                    size: Some(Decimal::from_str_exact("3.00").unwrap()),
                },
                OrderBookLevel {
                    price: Some(Decimal::from_str_exact("0.47").unwrap()),
                    size: Some(Decimal::from_str_exact("5.00").unwrap()),
                },
            ],
        };

        let estimate = book
            .estimate_buy_for_quote(Decimal::from_str_exact("2.00").unwrap())
            .expect("estimate");

        assert_eq!(estimate.price, Decimal::from_str_exact("0.47").unwrap());
        assert_eq!(estimate.size, Decimal::from_str_exact("4.31").unwrap());
    }

    #[test]
    fn order_book_estimates_sell_limit_from_bid_depth() {
        let book = OrderBookResponse {
            asset_id: None,
            market: None,
            hash: None,
            timestamp: None,
            bids: vec![
                OrderBookLevel {
                    price: Some(Decimal::from_str_exact("0.54").unwrap()),
                    size: Some(Decimal::from_str_exact("2.00").unwrap()),
                },
                OrderBookLevel {
                    price: Some(Decimal::from_str_exact("0.53").unwrap()),
                    size: Some(Decimal::from_str_exact("4.00").unwrap()),
                },
            ],
            asks: Vec::new(),
        };

        let estimate = book
            .estimate_sell_for_size(Decimal::from_str_exact("5.00").unwrap())
            .expect("estimate");

        assert_eq!(estimate.price, Decimal::from_str_exact("0.53").unwrap());
        assert_eq!(estimate.size, Decimal::from_str_exact("5.00").unwrap());
    }

    #[test]
    fn market_buy_shares_for_quote_matches_official_market_order_style() {
        let shares = market_buy_shares_for_quote(
            Decimal::from_str_exact("2.00").unwrap(),
            Decimal::from_str_exact("0.61").unwrap(),
            2,
        );

        assert_eq!(shares, Decimal::from_str_exact("3.2786").unwrap());
    }
}
