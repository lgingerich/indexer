//! Re-decodes retained raw logs without RPC access or rewriting existing rows.
//!
//! ```bash
//! cargo run --example redecode_logs -- registry.toml indexer.duckdb base 0xADDRESS 100 200
//! ```
//!
//! The final block bound is optional and exclusive. Repeated runs append duplicates.

#![expect(clippy::print_stdout, clippy::print_stderr)]

#[cfg(feature = "duckdb")]
mod replay {
    use std::path::PathBuf;

    use alloy_primitives::Address;
    use indexer::decode::Decoder;
    use indexer::decode::{ContractRegistry, RegistryError};
    use indexer::sink::duckdb::{DuckDbSettings, DuckDbSink, ReplayError, StoreError};
    use indexer::wire::envelope::ChainId;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub(super) enum Error {
        #[error("usage: redecode_logs REGISTRY DB CHAIN ADDRESS FROM_BLOCK [TO_BLOCK_EXCLUSIVE]")]
        Usage,
        #[error("invalid block bound: {0}")]
        Block(#[from] std::num::ParseIntError),
        #[error("invalid address: {0}")]
        Address(#[from] serde_json::Error),
        #[error(transparent)]
        Registry(#[from] RegistryError),
        #[error(transparent)]
        Store(#[from] StoreError),
        #[error(transparent)]
        Replay(#[from] ReplayError),
    }

    pub(super) fn run() -> Result<(), Error> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if !(5..=6).contains(&args.len()) {
            return Err(Error::Usage);
        }
        let registry = ContractRegistry::from_file(&args[0])?;
        let address: Address = serde_json::from_value(serde_json::Value::String(args[3].clone()))?;
        let from_block = args[4].parse()?;
        let to_block = args.get(5).map(|bound| bound.parse()).transpose()?;
        let mut sink = DuckDbSink::open(&DuckDbSettings {
            path: PathBuf::from(&args[1]),
            ..DuckDbSettings::default()
        })?;
        let count = sink.redecode(
            &Decoder::new(registry),
            &ChainId::new(&args[2]),
            address,
            from_block,
            to_block,
        )?;
        println!("{count} decoded rows appended");
        Ok(())
    }
}

fn main() -> std::process::ExitCode {
    #[cfg(feature = "duckdb")]
    {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
        match replay::run() {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                std::process::ExitCode::FAILURE
            }
        }
    }
    #[cfg(not(feature = "duckdb"))]
    {
        eprintln!("redecode_logs requires the duckdb feature");
        std::process::ExitCode::FAILURE
    }
}
