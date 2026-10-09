//! Settings that are never logged, and may be read from the environment.
//!
//! Apart from [`crate::config`] so a layer that takes a secret — a store's connection
//! string — need not depend on the settings file that names it.

use serde::Deserialize;
use serde::de;

/// A required, non-empty setting that is never logged: an RPC endpoint carrying an API
/// key, or a database connection string carrying a password.
///
/// Written either as the value itself or as the environment variable that holds it:
///
/// ```toml
/// http_url = "https://base-rpc.publicnode.com"   # the value, for a public or local one
/// http_url = { env = "INDEXER_HTTP_URL" }        # read from the environment at load
/// ```
///
/// The variable is read once, when the settings load, so a deployment missing one fails at
/// startup naming it rather than at first use. The settings file names the variable and
/// the platform's secret manager injects it, so the file stays in git and the indexer
/// never depends on which manager that is. `Debug` prints `[redacted]`, so the settings
/// can be logged whole.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// The value, for the one call that has to hand it to a client. Never log it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FromEnv {
            env: String,
        }

        #[derive(Deserialize)]
        #[serde(
            untagged,
            expecting = "a non-empty string, or `{ env = \"NAME\" }` naming an environment variable"
        )]
        enum Written {
            Value(String),
            FromEnv(FromEnv),
        }

        let value = match Written::deserialize(deserializer)? {
            Written::Value(value) => value,
            // The variable's value is never put in an error: `VarError::NotUnicode`
            // displays the bytes it rejected, which would print the secret.
            Written::FromEnv(FromEnv { env }) => match std::env::var(&env) {
                Ok(value) if value.trim().is_empty() => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is empty"
                    )));
                }
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is not set"
                    )));
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is not valid UTF-8"
                    )));
                }
            },
        };
        if value.trim().is_empty() {
            return Err(de::Error::custom("a secret setting cannot be empty"));
        }
        Ok(Self(value))
    }
}
