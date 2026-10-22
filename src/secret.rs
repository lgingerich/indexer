//! Settings that are never logged, and may be read from the environment.
//!
//! Apart from [`crate::config`] so a layer that takes a secret — a store's connection
//! string — need not depend on the settings file that names it.

use std::cell::Cell;
use std::env::VarError;

use serde::Deserialize;
use serde::de;

/// Where a `{ env = "NAME" }` secret is read from: a lookup by name, answering as
/// [`std::env::var`] does.
///
/// An input to the settings parse rather than a fixed read of the process's environment,
/// so a test can parse a deployment's settings file with stand-in values: setting the
/// process's variables needs `unsafe`, which the crate denies. A deployment passes
/// [`process_env`].
pub type Env = fn(&str) -> Result<String, VarError>;

/// The process's environment: [`Env`] as a deployment reads it.
///
/// # Errors
///
/// As [`std::env::var`]: the variable is unset or not UTF-8.
pub fn process_env(name: &str) -> Result<String, VarError> {
    std::env::var(name)
}

thread_local! {
    /// The [`Env`] the settings parse under way on this thread reads, set by [`reading`].
    ///
    /// A thread-local because serde's derived `Deserialize` hands a field nothing but the
    /// deserializer, so this is the one way a value reaches [`Secret`] from the caller of
    /// the parse without a seed threaded through every settings type, including the ones
    /// in the sinks. Scoped to the parse and to its thread, so nothing else sees it.
    static ENV: Cell<Option<Env>> = const { Cell::new(None) };
}

/// Runs `parse` with every [`Secret`] it deserializes reading `env`, and restores the
/// previous lookup after, even if `parse` panics.
pub(crate) fn reading<T>(env: Env, parse: impl FnOnce() -> T) -> T {
    /// Puts back the lookup that was in place before.
    struct Restore(Option<Env>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ENV.set(self.0);
        }
    }
    let _restore = Restore(ENV.replace(Some(env)));
    parse()
}

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
/// The variable is read once, when the settings load, from the [`Env`] the parse was given,
/// so a deployment missing one fails at startup naming it rather than at first use. Outside
/// a settings parse there is no environment to read, and the `env` form is refused. The settings file names the variable and
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
            Written::FromEnv(FromEnv { env }) => match lookup(&env)? {
                Ok(value) if value.trim().is_empty() => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is empty"
                    )));
                }
                Ok(value) => value,
                Err(VarError::NotPresent) => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is not set"
                    )));
                }
                Err(VarError::NotUnicode(_)) => {
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

/// Looks `name` up in the [`Env`] of the settings parse under way.
fn lookup<E: de::Error>(name: &str) -> Result<Result<String, VarError>, E> {
    let Some(env) = ENV.get() else {
        return Err(E::custom(format!(
            "environment variable `{name}` named outside a settings parse, which has no \
             environment to read"
        )));
    };
    Ok(env(name))
}
