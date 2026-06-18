use kubesavings_agent::{collector, config, sender};

use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() {
    // Structured JSON logging
    tracing_subscriber::fmt()
        .json()
        .with_span_events(FmtSpan::NONE)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "kubesavings_agent_start"
    );

    // Load config
    let config = match config::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "config_error");
            eprintln!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    info!(
        cluster_id = %config.cluster_id,
        api_endpoint = %config.api_endpoint,
        "config_loaded"
    );

    // Collect metrics
    let snapshot = match collector::collect(&config).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "collection_failed");
            eprintln!("Failed to collect metrics: {}", e);
            std::process::exit(1);
        }
    };

    // Send snapshot
    match sender::send_snapshot(&config, &snapshot).await {
        Ok(resp) => {
            info!(
                recommendations = resp.recommendations,
                total_savings_usd = resp.total_savings_usd,
                "agent_run_completed"
            );
        }
        Err(e) => {
            error!(error = %e, "send_failed");
            eprintln!("Failed to send snapshot: {}", e);
            std::process::exit(1);
        }
    }
}
