use std::{env, path::PathBuf};

use anyhow::{Context, Result};
use pm_alpha_1_0::{app::App, config::Settings};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = config_path();
    let settings = Settings::load(&config_path)
        .with_context(|| format!("unable to boot using config {}", config_path.display()))?;

    init_tracing(&settings)?;

    let app = App::boot(settings).await?;
    app.run().await
}

fn config_path() -> PathBuf {
    let chosen = env::args()
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| env::var("PM_CONFIG").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("config/pm-alpha.toml"));

    if chosen.exists() {
        chosen
    } else {
        PathBuf::from("config/pm-alpha.example.toml")
    }
}

fn init_tracing(settings: &Settings) -> Result<()> {
    let env_filter = EnvFilter::try_new(settings.app.log_level.clone())
        .or_else(|_| EnvFilter::try_new("info"))
        .context("failed to initialize log filter")?;

    let builder = fmt().with_env_filter(env_filter);

    if settings.telemetry.log_json {
        builder.json().init();
    } else {
        builder.compact().init();
    }

    Ok(())
}
