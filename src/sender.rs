use prost::Message;
use reqwest::Client;
use thiserror::Error;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tracing::{info, warn};

use crate::config::Config;
use crate::types::{AgentSnapshot, SnapshotResponse};

#[derive(Debug, Error)]
pub enum SenderError {
    #[error("HTTP request failed: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Authentication failed (401). Check your KUBESAVINGS_API_KEY.")]
    Unauthorized,
    #[error("Server error {status}: {body}")]
    ServerError { status: u16, body: String },
    #[error("Failed to decode server response as protobuf")]
    Decode,
}

/// Internal error type for retry logic — lets us distinguish retryable from non-retryable errors.
#[derive(Debug)]
enum RetryableError {
    Unauthorized,
    Decode,
    Transient { status: u16, body: String },
    Reqwest(reqwest::Error),
}

impl std::fmt::Display for RetryableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryableError::Unauthorized => write!(f, "Unauthorized (401)"),
            RetryableError::Decode => write!(f, "Failed to decode protobuf response"),
            RetryableError::Transient { status, body } => {
                write!(f, "Transient error HTTP {}: {}", status, body)
            }
            RetryableError::Reqwest(e) => write!(f, "Request error: {}", e),
        }
    }
}

impl std::error::Error for RetryableError {}

pub async fn send_snapshot(
    config: &Config,
    snapshot: &AgentSnapshot,
) -> Result<SnapshotResponse, SenderError> {
    let client = Client::builder()
        .use_rustls_tls()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("kubesavings-agent/1.0")
        .build()
        .map_err(SenderError::Reqwest)?;

    let url = format!(
        "{}/api/clusters/{}/snapshot",
        config.api_endpoint.trim_end_matches('/'),
        config.cluster_id
    );
    let api_key = config.api_key.clone();
    let body_bytes = snapshot.encode_to_vec();

    // Exponential backoff: 5s → 20s → 80s (capped at 120s), with jitter, 3 attempts
    let retry_strategy = ExponentialBackoff::from_millis(5000)
        .factor(4)
        .max_delay(std::time::Duration::from_secs(120))
        .map(jitter)
        .take(3);

    let result = Retry::spawn(retry_strategy, || {
        let client = client.clone();
        let url = url.clone();
        let api_key = api_key.clone();
        let body_bytes = body_bytes.clone();

        async move {
            let resp = client
                .post(&url)
                .header("X-Api-Key", &api_key)
                .header("Content-Type", "application/x-protobuf")
                .body(body_bytes)
                .send()
                .await
                .map_err(RetryableError::Reqwest)?;

            let status = resp.status();

            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(RetryableError::Unauthorized);
            }

            if status.is_success() {
                let bytes = resp.bytes().await.map_err(RetryableError::Reqwest)?;
                let response =
                    SnapshotResponse::decode(bytes).map_err(|_| RetryableError::Decode)?;
                return Ok(response);
            }

            let status_u16 = status.as_u16();
            let body = resp.text().await.unwrap_or_default();
            warn!(status = status_u16, body = %body, "server_error_will_retry");
            Err(RetryableError::Transient {
                status: status_u16,
                body,
            })
        }
    })
    .await;

    match result {
        Ok(resp) => {
            info!(
                recommendations = resp.recommendations,
                total_savings_usd = resp.total_savings_usd,
                "snapshot_sent"
            );
            Ok(resp)
        }
        Err(RetryableError::Unauthorized) => Err(SenderError::Unauthorized),
        Err(RetryableError::Decode) => Err(SenderError::Decode),
        Err(RetryableError::Reqwest(e)) => Err(SenderError::Reqwest(e)),
        Err(RetryableError::Transient { status, body }) => {
            Err(SenderError::ServerError { status, body })
        }
    }
}
