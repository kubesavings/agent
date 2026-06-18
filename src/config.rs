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
}

#[derive(Debug)]
pub struct Config {
    pub api_endpoint: String,
    pub api_key: String,
    pub cluster_id: String,
    pub include_namespaces: Vec<String>,
    pub exclude_namespaces: Vec<String>,
    pub cloud_provider: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let api_key = Self::require_env("KUBESAVINGS_API_KEY")?;
        let cluster_id = Self::validated_cluster_id(Self::require_env("KUBESAVINGS_CLUSTER_ID")?)?;
        let api_endpoint = Self::validated_endpoint(
            env::var("KUBESAVINGS_API_ENDPOINT")
                .unwrap_or_else(|_| "https://app.kubesavings.io".to_string()),
        )?;

        let include_namespaces =
            Self::parse_csv(env::var("KUBESAVINGS_INCLUDE_NAMESPACES").unwrap_or_default());

        let exclude_namespaces = Self::parse_csv(
            env::var("KUBESAVINGS_EXCLUDE_NAMESPACES")
                .unwrap_or_else(|_| "kube-system,kube-public,kube-node-lease".to_string()),
        );

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
}
