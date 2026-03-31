use std::env;
use std::str::FromStr;
use std::time::Duration;

use dotenvy::dotenv;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub postgres: PostgresConfig,
    pub kafka: KafkaConfig,
    pub app: RuntimeConfig,
}

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub db_name: String,
    pub user: String,
    pub password: String,
    pub max_connections: u32,
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub security_protocol: Option<String>,
    pub enable_auto_commit: bool,
    pub auto_offset_reset: String,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub log_level: String,
    pub health_bind_addr: String,
    pub batch_size: usize,
    pub batch_timeout: Duration,
    pub circuit_breaker_failure_threshold: usize,
    pub circuit_breaker_reset_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    MissingVar(&'static str),
    #[error("invalid value for {key}: {value}")]
    InvalidVar { key: &'static str, value: String },
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        dotenv().ok();

        Ok(Self {
            postgres: PostgresConfig {
                host: required_var("DB_HOST")?,
                port: parse_var("DB_PORT")?,
                db_name: required_var("DB_NAME")?,
                user: required_var("DB_USER")?,
                password: required_var("DB_PASSWORD")?,
                max_connections: parse_var("DB_MAX_CONNECTIONS")?,
            },
            kafka: KafkaConfig {
                brokers: required_var("KAFKA_BROKERS")?,
                topic: required_var("KAFKA_TOPIC")?,
                group_id: required_var("KAFKA_GROUP_ID")?,
                sasl_mechanism: optional_var("KAFKA_SASL_MECHANISM"),
                username: optional_var("KAFKA_USERNAME"),
                password: optional_var("KAFKA_PASSWORD"),
                security_protocol: optional_var("KAFKA_SECURITY_PROTOCOL"),
                enable_auto_commit: parse_var_with_default("KAFKA_ENABLE_AUTO_COMMIT", false)?,
                auto_offset_reset: var_with_default("KAFKA_AUTO_OFFSET_RESET", "latest"),
            },
            app: RuntimeConfig {
                log_level: var_with_default("LOG_LEVEL", "info"),
                health_bind_addr: var_with_default("HEALTH_BIND_ADDR", "0.0.0.0:8080"),
                batch_size: parse_var("BATCH_SIZE")?,
                batch_timeout: Duration::from_millis(parse_var("BATCH_TIMEOUT_MS")?),
                circuit_breaker_failure_threshold: parse_var("CIRCUIT_BREAKER_FAILURE_THRESHOLD")?,
                circuit_breaker_reset_timeout: Duration::from_millis(parse_var(
                    "CIRCUIT_BREAKER_RESET_TIMEOUT_MS",
                )?),
            },
        })
    }
}

fn required_var(key: &'static str) -> Result<String, ConfigError> {
    env::var(key).map_err(|_| ConfigError::MissingVar(key))
}

fn optional_var(key: &'static str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn var_with_default(key: &'static str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_var<T>(key: &'static str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    let value = required_var(key)?;
    value
        .parse::<T>()
        .map_err(|_| ConfigError::InvalidVar { key, value })
}

fn parse_var_with_default<T>(key: &'static str, default: T) -> Result<T, ConfigError>
where
    T: FromStr + Copy,
{
    match env::var(key) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|_| ConfigError::InvalidVar { key, value }),
        Err(_) => Ok(default),
    }
}
