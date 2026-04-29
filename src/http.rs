use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{
    Client,
    header::{ACCEPT, CONNECTION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT},
};

use crate::config::NetworkConfig;

pub fn build_http_client(network: &NetworkConfig, user_agent: &'static str) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(user_agent));
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let mut builder = Client::builder()
        .default_headers(headers)
        .connect_timeout(Duration::from_millis(network.connect_timeout_ms))
        .timeout(Duration::from_millis(network.request_timeout_ms))
        .pool_idle_timeout(Duration::from_secs(network.keepalive_interval_secs))
        .tcp_nodelay(network.tcp_nodelay)
        .use_rustls_tls();

    if network.prefer_http2 {
        builder = builder
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(network.keepalive_interval_secs));
    }

    builder
        .build()
        .context("failed to build shared HTTP client")
}

pub fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}
