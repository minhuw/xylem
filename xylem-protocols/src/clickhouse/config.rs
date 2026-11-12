//! ClickHouse protocol configuration

use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ClickHouse native protocol configuration
///
/// Every query is sent with the same `settings`. Set exactly one of `query`
/// and `queries`.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// Database queries run in (default: the user's default database)
    #[serde(default)]
    pub database: String,
    /// User to log in as (default: "default")
    #[serde(default = "default_user")]
    pub user: String,
    /// Password, written in the profile (default: none, an empty password)
    #[serde(default)]
    pub password: Option<String>,
    /// Name of the environment variable that holds the password, read when
    /// the run starts
    #[serde(default)]
    pub password_env: Option<String>,
    /// The SQL text of every query
    #[serde(default)]
    pub query: Option<String>,
    /// Queries, each drawn with probability weight / total weight
    #[serde(default)]
    pub queries: Option<Vec<ClickHouseQueryConfig>>,
    /// Query settings sent with every query, such as
    /// `{ max_threads = 1 }`. They are marked important, so the server
    /// fails a query whose setting it does not know (default: none)
    #[serde(default)]
    pub settings: BTreeMap<String, ClickHouseSettingValue>,
}

/// One query of a weighted query mix
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClickHouseQueryConfig {
    /// SQL text
    pub sql: String,
    /// Relative weight (default: 1)
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// A setting value; the server parses it from its text
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum ClickHouseSettingValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl ClickHouseSettingValue {
    /// The value as the text ClickHouse parses: booleans as 1 and 0
    pub fn to_text(&self) -> String {
        match self {
            Self::Bool(value) => u8::from(*value).to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::String(value) => value.clone(),
        }
    }
}

impl std::fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_env", &self.password_env)
            .field("query", &self.query)
            .field("queries", &self.queries)
            .field("settings", &self.settings)
            .finish()
    }
}

impl ClickHouseConfig {
    /// Check what can be checked without connecting
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.user.is_empty(), "ClickHouse user must not be empty");
        for (name, value) in [("database", &self.database), ("user", &self.user)] {
            ensure!(!value.contains('\0'), "ClickHouse {name} must not contain NUL");
        }
        ensure!(
            !(self.password.is_some() && self.password_env.is_some()),
            "ClickHouse sets both password and password_env; set at most one"
        );
        if let Some(name) = &self.password_env {
            ensure!(!name.is_empty(), "ClickHouse password_env must not be empty");
        }
        let queries = self.weighted_queries()?;
        for (sql, weight) in &queries {
            ensure!(!sql.trim().is_empty(), "ClickHouse queries must not be empty");
            ensure!(
                weight.is_finite() && *weight >= 0.0,
                "ClickHouse query weight {weight} is not a non-negative number"
            );
        }
        ensure!(
            queries.iter().any(|(_, weight)| *weight > 0.0),
            "ClickHouse query weights are all zero"
        );
        for name in self.settings.keys() {
            ensure!(!name.is_empty(), "ClickHouse setting names must not be empty");
        }
        Ok(())
    }

    /// The SQL texts and weights of the queries to send
    pub fn weighted_queries(&self) -> Result<Vec<(String, f64)>> {
        match (&self.query, &self.queries) {
            (Some(query), None) => Ok(vec![(query.clone(), 1.0)]),
            (None, Some(queries)) => {
                ensure!(!queries.is_empty(), "ClickHouse queries must not be empty");
                Ok(queries.iter().map(|query| (query.sql.clone(), query.weight)).collect())
            }
            (Some(_), Some(_)) => bail!("ClickHouse sets both query and queries; set exactly one"),
            (None, None) => bail!("ClickHouse needs a query or queries; set exactly one"),
        }
    }

    /// The password, from the profile, from the environment variable
    /// `password_env` names, or empty. Errors name the variable, never the
    /// password.
    pub fn resolve_password(&self) -> Result<String> {
        let Some(name) = &self.password_env else {
            return Ok(self.password.clone().unwrap_or_default());
        };
        match std::env::var(name) {
            Ok(password) => Ok(password),
            Err(std::env::VarError::NotPresent) => {
                bail!("ClickHouse password_env: environment variable {name} is not set")
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                bail!("ClickHouse password_env: environment variable {name} is not valid UTF-8")
            }
        }
    }
}

fn default_user() -> String {
    "default".to_string()
}

fn default_weight() -> f64 {
    1.0
}
