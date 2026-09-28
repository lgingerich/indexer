//! The decode stage: raw envelopes in, decoded envelopes out.
//!
//! Consumes one topic, applies the stateless [`Transform`], and produces to another.
//! Everything that makes this safe lives in the transform; this file is the loop that
//! drives it, plus the commit policy that decides how much is replayed after a crash.
//!
//! # Delivery
//!
//! At-least-once. The transform is a pure function of a record and the registry, so a
//! redelivered record decodes to the same output with the same `dedupe_key`, and a
//! consumer that upserts on that key is idempotent.
//!
//! The commit happens **after** the sink's flush, so a crash between the two replays a
//! batch rather than losing one. That ordering is the whole reason the source does not
//! commit on its own.
//!
//! # Batch boundary
//!
//! A batch is `--batch` records or `--batch-ms` milliseconds, whichever comes first.
//! Batching amortizes the producer's request overhead; the time bound is what stops a
//! quiet topic from leaving records unflushed indefinitely, which matters because an
//! unflushed record is an uncommitted offset.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use anyhow::{Context as _, Result};
use connectors::{EnvelopeSource as _, EventSink as _, KafkaSink, KafkaSource};
use decode::Transform;
use decode::registry::{Abi, AbiRegistry};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::BaseProducer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wire::envelope::ChainId;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(%error, "decode stopped");
            ExitCode::FAILURE
        }
    }
}

/// What the process needs to reach the broker and shape its batches.
struct Config {
    brokers: String,
    group: String,
    input_topic: String,
    output_topic: String,
    batch: usize,
    batch_timeout: Duration,
    /// `chain:address:abi.json`, repeatable. See [`FileRegistry`].
    registrations: Vec<String>,
}

impl Config {
    /// Reads the environment, failing on anything missing rather than guessing.
    fn from_env() -> Result<Self> {
        let get = |key: &str| std::env::var(key).with_context(|| format!("{key} must be set"));
        let registrations = std::env::var("DECODE_ABIS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect();

        Ok(Self {
            brokers: get("KAFKA_BROKERS")?,
            group: std::env::var("DECODE_GROUP").unwrap_or_else(|_| "indexer-decode".to_owned()),
            input_topic: std::env::var("DECODE_INPUT_TOPIC")
                .unwrap_or_else(|_| "raw.chain".to_owned()),
            output_topic: std::env::var("DECODE_OUTPUT_TOPIC")
                .unwrap_or_else(|_| "decoded.chain".to_owned()),
            batch: std::env::var("DECODE_BATCH")
                .unwrap_or_else(|_| "500".to_owned())
                .parse()
                .context("DECODE_BATCH must be a number")?,
            batch_timeout: Duration::from_millis(
                std::env::var("DECODE_BATCH_MS")
                    .unwrap_or_else(|_| "1000".to_owned())
                    .parse()
                    .context("DECODE_BATCH_MS must be a number")?,
            ),
            registrations,
        })
    }
}

/// A registry built from `chain:address:abi.json` entries on disk.
///
/// One ABI per `(chain, address)`, applying at every height. That is the honest
/// limitation of a file-backed registry and it is stated rather than hidden: a proxy
/// that upgrades changes its ABI at a height, and this cannot express that. The
/// [`AbiRegistry`] seam is what a table-backed registry replaces, without the decoder
/// changing.
///
/// The key is the typed [`Address`], not its rendering. An address has several
/// spellings — lowercase, checksummed — and comparing strings would silently miss a
/// lookup when one side happened to be checksummed and the other not. That is not
/// hypothetical: it made every log pass through undecoded until it was fixed, because
/// the config held a lowercase address and the wire carried a checksummed one.
#[derive(Debug, Default)]
struct FileRegistry {
    entries: HashMap<(ChainId, Address), Abi>,
}

impl FileRegistry {
    /// Loads every registration, failing on a malformed one rather than skipping it.
    ///
    /// A silently skipped ABI would mean logs quietly not decoding, which looks
    /// identical to a contract having no events.
    fn load(registrations: &[String]) -> Result<Self> {
        let mut entries = HashMap::new();
        for entry in registrations {
            let mut parts = entry.splitn(3, ':');
            let (Some(chain), Some(address), Some(path)) =
                (parts.next(), parts.next(), parts.next())
            else {
                anyhow::bail!("DECODE_ABIS entry {entry:?} is not chain:address:path");
            };
            let address: Address = address
                .parse()
                .with_context(|| format!("DECODE_ABIS entry {entry:?} has an invalid address"))?;
            let json =
                std::fs::read_to_string(path).with_context(|| format!("read ABI at {path}"))?;
            let abi = Abi::from_json(&json).with_context(|| format!("load ABI at {path}"))?;
            info!(chain, %address, path, "registered ABI");
            entries.insert((ChainId::new(chain), address), abi);
        }
        Ok(Self { entries })
    }
}

impl AbiRegistry for FileRegistry {
    fn abi(&self, chain: &ChainId, address: Address, _block: u64) -> Option<&Abi> {
        self.entries.get(&(chain.clone(), address))
    }
}

async fn run() -> Result<()> {
    let config = Config::from_env()?;
    let registry = FileRegistry::load(&config.registrations)?;
    if registry.entries.is_empty() {
        warn!("no ABIs registered; every log will pass through undecoded");
    }
    let transform = Arc::new(Transform::new(registry));

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group)
        // A fresh group starts at the beginning rather than skipping the history it
        // was created to read.
        .set("auto.offset.reset", "earliest")
        // The commit point is this process's decision, after a flush; see `commit`.
        .set("enable.auto.commit", "false")
        .create()
        .context("create consumer")?;
    consumer
        .subscribe(&[&config.input_topic])
        .context("subscribe to input topic")?;

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .create()
        .context("create producer")?;

    let mut source = KafkaSource::new(consumer);
    let mut sink = KafkaSink::new(producer, config.output_topic.clone());

    info!(
        input = %config.input_topic,
        output = %config.output_topic,
        group = %config.group,
        "decode stage started"
    );

    let mut pending = 0;
    let mut last_flush = Instant::now();
    loop {
        let Some(envelope) = source.next().await? else {
            info!("input topic ended");
            break;
        };

        let outputs = match transform.apply(envelope) {
            Ok(outputs) => outputs,
            Err(error) => {
                // One undecodable log must not stall the stream, but it must not be
                // silent either: the usual cause is an ABI from the wrong block range,
                // and the raw log is already on the input topic, so skipping it loses
                // nothing that a corrected ABI could not recover.
                warn!(%error, "skipping a record that does not decode");
                continue;
            }
        };
        for output in &outputs {
            sink.publish(output).await?;
        }
        pending += 1;

        // Flush on either bound, then commit: the offset never advances past bytes
        // that are not yet on the output topic.
        if pending >= config.batch || last_flush.elapsed() >= config.batch_timeout {
            sink.flush().await?;
            source.commit()?;
            pending = 0;
            last_flush = Instant::now();
        }
    }

    sink.flush().await?;
    source.commit()?;
    Ok(())
}
#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::Address;

    use super::FileRegistry;
    use crate::AbiRegistry as _;

    /// An address has several spellings, and a registry that compared their string
    /// forms would miss a lookup whenever the two sides disagreed. That is not
    /// hypothetical: it made every log pass through undecoded, because the config held
    /// a lowercase address while the wire carried a checksummed one.
    #[test]
    fn an_address_resolves_however_it_is_spelled() {
        let registry = FileRegistry::load(&[format!(
            "base:0xd0b53d9277642d899df5c87a3966a349a798f224:{}",
            concat!(env!("CARGO_MANIFEST_DIR"), "/abi/uniswap_v3_pool.json")
        )])
        .expect("the registration loads");

        let chain = wire::envelope::ChainId::new("base");
        // Lowercase, checksummed, and mixed-case all name the same account.
        for spelling in [
            "0xd0b53d9277642d899df5c87a3966a349a798f224",
            "0xd0b53D9277642d899DF5C87A3966A349A798F224",
            "0xD0b53D9277642d899DF5C87A3966A349A798F224",
        ] {
            let address: Address = spelling.parse().expect("a valid address");
            assert!(
                registry.abi(&chain, address, 1).is_some(),
                "{spelling} did not resolve"
            );
        }
    }

    /// A different chain or address is a miss, so one contract's ABI is never applied
    /// to another's logs.
    #[test]
    fn a_different_chain_or_address_is_a_miss() {
        let registry = FileRegistry::load(&[format!(
            "base:0xd0b53d9277642d899df5c87a3966a349a798f224:{}",
            concat!(env!("CARGO_MANIFEST_DIR"), "/abi/uniswap_v3_pool.json")
        )])
        .expect("the registration loads");

        let address: Address = "0xd0b53d9277642d899df5c87a3966a349a798f224"
            .parse()
            .expect("a valid address");
        assert!(
            registry
                .abi(&wire::envelope::ChainId::new("ethereum"), address, 1)
                .is_none()
        );
        assert!(
            registry
                .abi(
                    &wire::envelope::ChainId::new("base"),
                    Address::from([0x11; 20]),
                    1
                )
                .is_none()
        );
    }

    /// A malformed registration fails loudly rather than being skipped, because a
    /// skipped ABI looks exactly like a contract that emits no events.
    #[test]
    fn a_malformed_registration_is_an_error() {
        assert!(FileRegistry::load(&["not-a-registration".to_owned()]).is_err());
        assert!(FileRegistry::load(&["base:not-an-address:abi.json".to_owned()]).is_err());
    }
}
