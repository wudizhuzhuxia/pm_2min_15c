use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use tracing::warn;

use crate::config::{Settings, TelegramConfig};

pub enum Notifier {
    Disabled,
    Telegram(TelegramNotifier),
}

impl Notifier {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        if !settings.telegram.enabled {
            return Ok(Self::Disabled);
        }

        let notifier = TelegramNotifier::new(&settings.telegram)?;
        Ok(Self::Telegram(notifier))
    }

    pub async fn send(&self, title: &str, body: impl AsRef<str>) -> Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Telegram(notifier) => notifier.send(title, body.as_ref()).await,
        }
    }

    pub async fn send_startup(&self, settings: &Settings) {
        if !settings.telegram.send_startup {
            return;
        }

        let primary = settings
            .primary_account()
            .map(|account| account.name.as_str())
            .unwrap_or("unknown");

        let body = format!(
            "instance: {}\nstrategy_mode: {:?}\nprimary_account: {}\ndry_run: {}",
            settings.app.instance_name, settings.strategy.mode, primary, settings.app.dry_run
        );

        if let Err(error) = self.send("startup", body).await {
            warn!(?error, "failed to send startup notification");
        }
    }

    pub async fn send_shutdown(&self, settings: &Settings) {
        if !settings.telegram.send_shutdown {
            return;
        }

        if let Err(error) = self
            .send(
                "shutdown",
                format!("instance: {}", settings.app.instance_name),
            )
            .await
        {
            warn!(?error, "failed to send shutdown notification");
        }
    }
}

pub struct TelegramNotifier {
    client: Client,
    config: TelegramConfig,
    bot_token: String,
}

impl TelegramNotifier {
    fn new(config: &TelegramConfig) -> Result<Self> {
        let bot_token = config.bot_token()?;
        let client = Client::builder()
            .use_rustls_tls()
            .http2_adaptive_window(true)
            .build()
            .context("failed to build telegram http client")?;

        Ok(Self {
            client,
            config: config.clone(),
            bot_token,
        })
    }

    async fn send(&self, title: &str, body: &str) -> Result<()> {
        for &chat_id in &self.config.chat_ids {
            let parse_mode = self
                .config
                .parse_mode
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned);
            let payload = TelegramSendMessageRequest {
                chat_id,
                text: format!("[pm-alpha:{}]\n{}", title, body),
                parse_mode,
                disable_web_page_preview: self.config.disable_link_preview,
            };

            self.client
                .post(format!(
                    "https://api.telegram.org/bot{}/sendMessage",
                    self.bot_token
                ))
                .json(&payload)
                .send()
                .await
                .context("telegram send request failed")?
                .error_for_status()
                .context("telegram send request returned non-success status")?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct TelegramSendMessageRequest {
    chat_id: i64,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
    disable_web_page_preview: bool,
}
