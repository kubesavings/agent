use std::time::Duration;

use prost::Message;
use reqwest::Client;
use thiserror::Error;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::RetryIf;
use tracing::{info, warn};

use crate::config::Config;
use crate::types::{AgentSnapshot, SnapshotResponse};

/// Per-attempt HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on how much of an error body is retained, so a misbehaving or hostile
/// endpoint cannot flood the structured logs.
const MAX_ERROR_BODY_BYTES: usize = 1024;

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
    Http { status: u16, body: String },
    Reqwest(reqwest::Error),
}

impl std::fmt::Display for RetryableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryableError::Unauthorized => write!(f, "Unauthorized (401)"),
            RetryableError::Decode => write!(f, "Failed to decode protobuf response"),
            RetryableError::Http { status, body } => {
                write!(f, "HTTP {}: {}", status, body)
            }
            RetryableError::Reqwest(e) => write!(f, "Request error: {}", e),
        }
    }
}

impl std::error::Error for RetryableError {}

/// Statuses worth another attempt: request timeout, rate limit, and any 5xx.
///
/// Everything else in the 4xx range (403 revoked key, 404 unknown cluster,
/// 400/422 malformed body) is a client-side defect that no amount of retrying
/// will fix — retrying only burns the CronJob's `activeDeadlineSeconds`.
fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

impl RetryableError {
    fn is_retryable(&self) -> bool {
        match self {
            // Connection resets, DNS blips and timeouts are worth another attempt.
            RetryableError::Reqwest(_) => true,
            RetryableError::Http { status, .. } => is_retryable_status(*status),
            RetryableError::Unauthorized | RetryableError::Decode => false,
        }
    }
}

/// Backoff delays between attempts, without jitter.
///
/// `ExponentialBackoff::from_millis(n)` sets the exponential *base* (the delay
/// grows by a factor of `n` each step) while `factor` is a flat multiplier — so
/// the curve is `base^attempt * factor`. Base 4 with a 1250ms factor yields
/// 5s then 20s.
///
/// Two retries after the initial attempt is 3 attempts total. Worst case the
/// sender occupies `25s` of backoff plus `3 * REQUEST_TIMEOUT`, which stays
/// inside the chart's 300s `activeDeadlineSeconds` with room for collection.
fn backoff_delays() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(4)
        .factor(1250)
        .max_delay(Duration::from_secs(60))
        .take(2)
}

/// Truncate a server-supplied body to a bounded, log-safe size.
fn truncate_body(mut body: String) -> String {
    if body.len() > MAX_ERROR_BODY_BYTES {
        let mut end = MAX_ERROR_BODY_BYTES;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        body.push_str("… (truncated)");
    }
    body
}

pub async fn send_snapshot(
    config: &Config,
    snapshot: &AgentSnapshot,
) -> Result<SnapshotResponse, SenderError> {
    let client = Client::builder()
        .use_rustls_tls()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(concat!("kubesavings-agent/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(SenderError::Reqwest)?;

    let url = format!(
        "{}/api/clusters/{}/snapshot",
        config.api_endpoint.trim_end_matches('/'),
        config.cluster_id
    );
    let api_key = config.api_key.clone();
    let body_bytes = snapshot.encode_to_vec();

    let retry_strategy = backoff_delays().map(jitter);

    let result = RetryIf::spawn(
        retry_strategy,
        || {
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
                let body = truncate_body(resp.text().await.unwrap_or_default());
                if is_retryable_status(status_u16) {
                    warn!(status = status_u16, body = %body, "server_error_will_retry");
                } else {
                    warn!(status = status_u16, body = %body, "client_error_not_retrying");
                }
                Err(RetryableError::Http {
                    status: status_u16,
                    body,
                })
            }
        },
        RetryableError::is_retryable,
    )
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
        Err(RetryableError::Http { status, body }) => {
            Err(SenderError::ServerError { status, body })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // ── Backoff schedule ───────────────────────────────────────────────────────
    //
    // These pin the delay curve numerically. `tokio-retry`'s `from_millis` is the
    // exponential *base*, not the first delay, so an unpinned strategy silently
    // drifts into delays far longer than intended — long enough to blow through
    // the CronJob's activeDeadlineSeconds before the last attempt runs.

    #[test]
    fn backoff_delays_are_5s_then_20s() {
        let delays: Vec<Duration> = backoff_delays().collect();
        assert_eq!(
            delays,
            vec![Duration::from_secs(5), Duration::from_secs(20)],
            "backoff curve drifted"
        );
    }

    #[test]
    fn worst_case_send_fits_inside_cronjob_deadline() {
        // helm/templates/cronjob.yaml sets activeDeadlineSeconds: 300, shared with
        // collection. Attempts = retries + 1.
        const CRONJOB_DEADLINE: Duration = Duration::from_secs(300);
        let delays: Vec<Duration> = backoff_delays().collect();
        let attempts = delays.len() as u32 + 1;
        let worst_case: Duration = delays.iter().sum::<Duration>() + REQUEST_TIMEOUT * attempts;

        assert!(
            worst_case < CRONJOB_DEADLINE,
            "worst-case send {worst_case:?} exceeds the {CRONJOB_DEADLINE:?} job deadline"
        );
    }

    // ── Retry classification ───────────────────────────────────────────────────

    #[test]
    fn only_timeout_ratelimit_and_5xx_are_retryable() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(status), "{status} should be retryable");
        }
        for status in [400, 401, 403, 404, 409, 422, 200, 301] {
            assert!(
                !is_retryable_status(status),
                "{status} should not be retryable"
            );
        }
    }

    #[test]
    fn auth_and_decode_failures_are_never_retried() {
        assert!(!RetryableError::Unauthorized.is_retryable());
        assert!(!RetryableError::Decode.is_retryable());
        assert!(RetryableError::Http {
            status: 503,
            body: String::new()
        }
        .is_retryable());
        assert!(!RetryableError::Http {
            status: 403,
            body: String::new()
        }
        .is_retryable());
    }

    // ── Log-safety ─────────────────────────────────────────────────────────────

    #[test]
    fn truncate_body_bounds_length_and_respects_char_boundaries() {
        assert_eq!(truncate_body("short".to_string()), "short");

        let long = truncate_body("a".repeat(MAX_ERROR_BODY_BYTES * 4));
        assert!(long.len() < MAX_ERROR_BODY_BYTES + 32);
        assert!(long.ends_with("(truncated)"));

        // A multi-byte char straddling the cut must not panic or split.
        let multibyte = truncate_body("é".repeat(MAX_ERROR_BODY_BYTES));
        assert!(multibyte.ends_with("(truncated)"));
    }

    // ── End-to-end against a mock backend ──────────────────────────────────────

    fn test_config(endpoint: String) -> Config {
        Config {
            api_endpoint: endpoint,
            api_key: "test-key".to_string(),
            cluster_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            include_namespaces: vec![],
            exclude_namespaces: vec![],
            cloud_provider: None,
        }
    }

    /// Protobuf encoding of a `SnapshotResponse` the backend would return.
    fn ok_response(recommendations: i64, savings: f64) -> Vec<u8> {
        SnapshotResponse {
            recommendations,
            total_savings_usd: savings,
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn posts_protobuf_to_the_cluster_snapshot_path_and_decodes_the_reply() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/api/clusters/550e8400-e29b-41d4-a716-446655440000/snapshot",
            ))
            .and(header("X-Api-Key", "test-key"))
            .and(header("Content-Type", "application/x-protobuf"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ok_response(7, 123.5)))
            .expect(1)
            .mount(&server)
            .await;

        let resp = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
            .await
            .expect("send should succeed");

        assert_eq!(resp.recommendations, 7);
        assert_eq!(resp.total_savings_usd, 123.5);
    }

    #[tokio::test]
    async fn unauthorized_fails_immediately_without_retrying() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1) // exactly one attempt — no backoff, no retry
            .mount(&server)
            .await;

        let err = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
            .await
            .expect_err("401 must fail");

        assert!(matches!(err, SenderError::Unauthorized));
    }

    #[tokio::test]
    async fn client_errors_fail_fast_without_burning_the_backoff() {
        // A revoked key (403) or unknown cluster (404) can never succeed on retry.
        for status in [400u16, 403, 404, 422] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_string("nope"))
                .expect(1)
                .mount(&server)
                .await;

            let err = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
                .await
                .expect_err("client error must fail");

            match err {
                SenderError::ServerError { status: s, .. } => assert_eq!(s, status),
                other => panic!("{status} mapped to unexpected error: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn server_errors_are_retried_until_the_strategy_is_exhausted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
            // 2 retries + the initial attempt.
            .expect(3)
            .mount(&server)
            .await;

        let err = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
            .await
            .expect_err("sustained 503 must fail");

        match err {
            SenderError::ServerError { status, body } => {
                assert_eq!(status, 503);
                assert_eq!(body, "upstream down");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn a_success_after_a_transient_failure_is_returned() {
        let server = MockServer::start().await;
        // wiremock serves mounted mocks in order, honoring `up_to_n_times`.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ok_response(2, 9.0)))
            .expect(1)
            .mount(&server)
            .await;

        let resp = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
            .await
            .expect("should recover after one 500");

        assert_eq!(resp.recommendations, 2);
    }

    #[tokio::test]
    async fn undecodable_success_body_is_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            // Valid HTTP 200 but the body is not a SnapshotResponse.
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0xff; 8]))
            .expect(1)
            .mount(&server)
            .await;

        let err = send_snapshot(&test_config(server.uri()), &AgentSnapshot::default())
            .await
            .expect_err("garbage body must fail");

        assert!(matches!(err, SenderError::Decode));
    }
}
