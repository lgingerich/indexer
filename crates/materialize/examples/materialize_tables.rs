//! Materializes a decoded NDJSON stream into table rows, printed as JSON.
//!
//! A way to *see* what the layers produce before a store exists. It reads decoded
//! records, and for each one prints the faithful per-event row and, when an
//! extractor recognizes the event, the semantic trade row.
//!
//! ```bash
//! cargo run -p materialize --example materialize_tables -- decoded.ndjson
//! ```

#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use materialize::{Column, Value, tables};
use wire::envelope::{Envelope, Event};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: materialize_tables <decoded.ndjson>");
        return ExitCode::FAILURE;
    };

    let contents = std::fs::read_to_string(&path).expect("input reads");
    let (mut events, mut trades) = (0_usize, 0_usize);
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let envelope: Envelope = serde_json::from_str(line).expect("envelope parses");
        let Event::Decoded(decoded) = &envelope.event else {
            continue;
        };

        let produced = tables(decoded);
        events += 1;
        println!(
            "{}",
            serde_json::json!({
                "table": produced.event.table,
                "columns": columns(&produced.event.columns),
            })
        );
        if let Some(trade) = produced.trade {
            trades += 1;
            println!(
                "{}",
                serde_json::json!({
                    "table": "dex.trades",
                    "project": trade.project,
                    "protocol": trade.protocol,
                    "columns": columns(&trade.columns),
                })
            );
        }
    }

    eprintln!("{events} decoded events, {trades} of them trades");
    ExitCode::SUCCESS
}

fn columns(columns: &[Column]) -> serde_json::Map<String, serde_json::Value> {
    columns
        .iter()
        .map(|column| {
            let value = match &column.value {
                Value::Integer(n) => serde_json::json!(n),
                Value::Decimal(s) | Value::Text(s) => serde_json::json!(s),
                Value::Bool(b) => serde_json::json!(b),
            };
            (column.name.clone(), value)
        })
        .collect()
}
