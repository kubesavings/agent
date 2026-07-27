use std::env;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Required environment variable '{0}' is missing or empty.\nSet KUBESAVINGS_API_KEY and KUBESAVINGS_CLUSTER_ID before running the agent.")]
    MissingRequired(String),
    #[error(
        "KUBESAVINGS_API_ENDPOINT '{0}' is invalid.\n\
         Must be an https:// URL (http://localhost is allowed for local testing).\n\
         Example: https://app.kubesavings.io"
    )]
    InvalidEndpoint(String),
    #[error(
        "KUBESAVINGS_CLUSTER_ID '{0}' is invalid.\n\
         Must contain only alphanumeric characters and hyphens (UUID format)."
    )]
    InvalidClusterId(String),
    #[error(
        "{var} contains an invalid namespace '{name}'.\n\
         Namespaces must be RFC 1123 labels: lowercase alphanumerics and '-', at most 63 characters."
    )]
    InvalidNamespace { var: &'static str, name: String },
}

pub struct Config {
    pub api_endpoint: String,
    pub api_key: String,
    pub cluster_id: String,
    pub include_namespaces: Vec<String>,
    pub exclude_namespaces: Vec<String>,
    pub cloud_provider: Option<String>,
}

/// Hand-written so the API key can never reach a log line, a panic message, or
/// a `dbg!` left behind in a future change. Everything else is safe to print.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("api_endpoint", &self.api_endpoint)
            .field("api_key", &"<redacted>")
            .field("cluster_id", &self.cluster_id)
            .field("include_namespaces", &self.include_namespaces)
            .field("exclude_namespaces", &self.exclude_namespaces)
            .field("cloud_provider", &self.cloud_provider)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let api_key = Self::require_env("KUBESAVINGS_API_KEY")?;
        let cluster_id = Self::validated_cluster_id(Self::require_env("KUBESAVINGS_CLUSTER_ID")?)?;
        let api_endpoint = Self::validated_endpoint(
            env::var("KUBESAVINGS_API_ENDPOINT")
                .unwrap_or_else(|_| "https://app.kubesavings.io".to_string()),
        )?;

        let include_namespaces = Self::validated_namespaces(
            "KUBESAVINGS_INCLUDE_NAMESPACES",
            Self::parse_csv(env::var("KUBESAVINGS_INCLUDE_NAMESPACES").unwrap_or_default()),
        )?;

        let exclude_namespaces = Self::validated_namespaces(
            "KUBESAVINGS_EXCLUDE_NAMESPACES",
            Self::parse_csv(
                env::var("KUBESAVINGS_EXCLUDE_NAMESPACES")
                    .unwrap_or_else(|_| "kube-system,kube-public,kube-node-lease".to_string()),
            ),
        )?;

        let cloud_provider = env::var("KUBESAVINGS_CLOUD_PROVIDER")
            .ok()
            .filter(|s| !s.is_empty());

        Ok(Config {
            api_endpoint,
            api_key,
            cluster_id,
            include_namespaces,
            exclude_namespaces,
            cloud_provider,
        })
    }

    /// Validate and normalize the endpoint URL.
    ///
    /// Rules:
    /// - Must be a valid URL parseable by the `url` crate.
    /// - Scheme must be `https` (or `http` only for localhost/127.0.0.1 dev use).
    /// - The stored value is stripped to just the origin (`scheme://host[:port]`),
    ///   so injected paths like `../../billing/webhook` are silently eliminated.
    fn validated_endpoint(raw: String) -> Result<String, ConfigError> {
        let parsed = Url::parse(&raw).map_err(|_| ConfigError::InvalidEndpoint(raw.clone()))?;

        let host = parsed.host_str().unwrap_or("");
        let is_localhost = matches!(host, "localhost" | "127.0.0.1" | "::1");

        match parsed.scheme() {
            "https" => {}
            "http" if is_localhost => {}
            _ => return Err(ConfigError::InvalidEndpoint(raw)),
        }

        // Return only the origin — strips any path/query/fragment an attacker injected.
        Ok(parsed.origin().ascii_serialization())
    }

    /// Validate cluster_id contains only UUID-safe characters (hex digits and hyphens).
    ///
    /// Rejects slashes, dots, or any char that could form a path-traversal segment.
    /// Max length 36 matches the UUID format used by the backend (uuid4).
    fn validated_cluster_id(raw: String) -> Result<String, ConfigError> {
        if raw.len() > 36 {
            return Err(ConfigError::InvalidClusterId(raw));
        }
        if !raw.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            return Err(ConfigError::InvalidClusterId(raw));
        }
        Ok(raw)
    }

    /// Validate every namespace name against the RFC 1123 label rules Kubernetes
    /// itself enforces.
    ///
    /// These names are interpolated straight into Kubernetes API request paths
    /// (`kube` builds `namespaces/{ns}/…` with no percent-encoding), so a value
    /// carrying `/` or `..` would address a different API path than intended.
    /// This mirrors the hardening already applied to `cluster_id`.
    fn validated_namespaces(
        var: &'static str,
        names: Vec<String>,
    ) -> Result<Vec<String>, ConfigError> {
        for name in &names {
            let valid = !name.is_empty()
                && name.len() <= 63
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                && !name.starts_with('-')
                && !name.ends_with('-');
            if !valid {
                return Err(ConfigError::InvalidNamespace {
                    var,
                    name: name.clone(),
                });
            }
        }
        Ok(names)
    }

    fn require_env(key: &str) -> Result<String, ConfigError> {
        match env::var(key) {
            Ok(val) if !val.trim().is_empty() => Ok(val),
            _ => Err(ConfigError::MissingRequired(key.to_string())),
        }
    }

    fn parse_csv(s: String) -> Vec<String> {
        if s.is_empty() {
            return vec![];
        }
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_https_endpoint() {
        let result = Config::validated_endpoint("https://app.kubesavings.io".to_string());
        assert_eq!(result.unwrap(), "https://app.kubesavings.io");
    }

    #[test]
    fn accepts_https_with_path_and_strips_it() {
        let result =
            Config::validated_endpoint("https://app.kubesavings.io/injected/path".to_string());
        assert_eq!(result.unwrap(), "https://app.kubesavings.io");
    }

    #[test]
    fn accepts_http_localhost() {
        let result = Config::validated_endpoint("http://localhost:8000".to_string());
        assert_eq!(result.unwrap(), "http://localhost:8000");
    }

    #[test]
    fn rejects_http_non_localhost() {
        let result = Config::validated_endpoint("http://attacker.example.com".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_http_attacker_with_path() {
        let result = Config::validated_endpoint("http://attacker.example.com/steal".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_non_http_scheme() {
        assert!(Config::validated_endpoint("file:///etc/passwd".to_string()).is_err());
        assert!(Config::validated_endpoint("ftp://attacker.com".to_string()).is_err());
        assert!(Config::validated_endpoint("gopher://attacker.com".to_string()).is_err());
    }

    #[test]
    fn rejects_invalid_url() {
        assert!(Config::validated_endpoint("not a url".to_string()).is_err());
        assert!(Config::validated_endpoint(String::new()).is_err());
    }

    #[test]
    fn accepts_valid_uuid_cluster_id() {
        let id = "550e8400-e29b-41d4-a716-446655440000".to_string();
        assert_eq!(Config::validated_cluster_id(id.clone()).unwrap(), id);
    }

    #[test]
    fn rejects_path_traversal_cluster_id() {
        assert!(Config::validated_cluster_id("abc/../../billing/webhook".to_string()).is_err());
        assert!(Config::validated_cluster_id("../secret".to_string()).is_err());
        assert!(Config::validated_cluster_id("id\x00null".to_string()).is_err());
    }

    #[test]
    fn rejects_oversized_cluster_id() {
        let long = "a".repeat(37);
        assert!(Config::validated_cluster_id(long).is_err());
    }

    // ── Namespace validation ───────────────────────────────────────────────────

    #[test]
    fn accepts_valid_namespace_names() {
        let names = vec![
            "default".to_string(),
            "kube-system".to_string(),
            "team-a-1".to_string(),
            "a".repeat(63),
        ];
        assert_eq!(
            Config::validated_namespaces("VAR", names.clone()).unwrap(),
            names
        );
        // An empty list (no filtering configured) is valid.
        assert!(Config::validated_namespaces("VAR", vec![])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rejects_namespace_path_traversal() {
        // These would otherwise be interpolated raw into the K8s API request path.
        for bad in [
            "../../nodes",
            "default/../kube-system",
            "ns?watch=true",
            "ns#frag",
            "Default",
            "ns name",
            "ns\x00",
        ] {
            assert!(
                Config::validated_namespaces("VAR", vec![bad.to_string()]).is_err(),
                "should have rejected {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_malformed_namespace_labels() {
        for bad in ["-leading", "trailing-", &"a".repeat(64)] {
            assert!(
                Config::validated_namespaces("VAR", vec![bad.to_string()]).is_err(),
                "should have rejected {bad:?}"
            );
        }
    }

    #[test]
    fn one_bad_namespace_rejects_the_whole_list() {
        let names = vec!["good".to_string(), "../bad".to_string()];
        let err = Config::validated_namespaces("KUBESAVINGS_INCLUDE_NAMESPACES", names)
            .expect_err("must reject");
        // The error names the offending variable and value so the operator can fix it.
        let msg = err.to_string();
        assert!(msg.contains("KUBESAVINGS_INCLUDE_NAMESPACES"), "{msg}");
        assert!(msg.contains("../bad"), "{msg}");
    }

    // ── CSV parsing ────────────────────────────────────────────────────────────

    #[test]
    fn parse_csv_trims_and_drops_empty_entries() {
        assert_eq!(
            Config::parse_csv(" a , b,c ".to_string()),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(Config::parse_csv(String::new()).is_empty());
        assert!(Config::parse_csv(",,".to_string()).is_empty());
        assert!(Config::parse_csv("   ".to_string()).is_empty());
        assert_eq!(
            Config::parse_csv("single".to_string()),
            vec!["single".to_string()]
        );
    }

    // ── Credential hygiene ─────────────────────────────────────────────────────

    #[test]
    fn debug_output_never_contains_the_api_key() {
        let config = Config {
            api_endpoint: "https://app.kubesavings.io".to_string(),
            api_key: "super-secret-key-value".to_string(),
            cluster_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            include_namespaces: vec![],
            exclude_namespaces: vec!["kube-system".to_string()],
            cloud_provider: None,
        };

        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("super-secret-key-value"),
            "api key leaked into Debug output: {rendered}"
        );
        assert!(rendered.contains("<redacted>"), "{rendered}");
        // The non-sensitive fields are still useful for troubleshooting.
        assert!(rendered.contains("app.kubesavings.io"), "{rendered}");
        assert!(rendered.contains("550e8400"), "{rendered}");
    }
}
