//! Typed rows as newline-delimited JSON, one file per table.
//!
//! A row sink that writes files rather than a database, because it makes materialized
//! output *inspectable* without standing anything up: `jq` reads it, a spreadsheet
//! opens it, and a loader can bulk-import it later. It is the materialize stage's
//! equivalent of the bus's stdout sink — a human/pipe view, not a production store.
//!
//! # Why one file per table
//!
//! Web-scale tables do not belong in one file: a single `Swap` file grows without
//! bound, cannot be opened incrementally, and no bulk loader wants it. One file per
//! table at least keeps the partition honest, and the directory is the natural unit a
//! loader or an upload job takes.
//!
//! `ponytail:` one file per table is the whole partitioning story, so a long run
//! produces files too large to open. Partition by date the way a real store would —
//! `table/date=YYYY-MM-DD/part.ndjson` — if these are ever loaded rather than read.
//!
//! # Ordering
//!
//! Rows are appended in the order they arrive, and [`flush`](RowSink::flush) is the
//! durability point: buffered lines are written together and the file is flushed once,
//! so a crash loses a batch rather than a row. That matches the source's commit policy,
//! which advances an offset only after this flush returns.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::column::Value;
use crate::{Row, RowSink};

/// Appends rows to `<directory>/<table>.ndjson`.
///
/// A directory rather than a file, because a table is a file and a run produces
/// several. Created lazily on the first row for each table, so a run that sees no
/// `Swap` events leaves no empty `Swap` file behind.
#[derive(Debug)]
pub struct JsonLinesRowSink {
    directory: PathBuf,
    entries: BTreeMap<String, Entry>,
}

impl JsonLinesRowSink {
    /// Builds a sink rooted at `directory`, creating it if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub fn new(directory: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory)?;
        Ok(Self {
            directory,
            entries: BTreeMap::new(),
        })
    }

    /// The directory rows are written under.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// The file for `table`.
    #[must_use]
    pub fn path_for(&self, table: &str) -> PathBuf {
        self.directory.join(format!("{table}.ndjson"))
    }

    /// Renders one row as a JSON object, columns in their stable order.
    fn render(table: &str, row: &dyn Row) -> anyhow::Result<String> {
        // A `BTreeMap` of rendered values rather than a `Value` tree, so the object
        // preserves the row's own column order rather than sorting it.
        let columns: serde_json::Map<String, serde_json::Value> = row
            .columns()
            .iter()
            .map(|column| {
                let value = match &column.value {
                    Value::Integer(n) => serde_json::Value::from(*n),
                    Value::Decimal(s) | Value::Text(s) => serde_json::Value::from(s.clone()),
                    Value::Bool(b) => serde_json::Value::from(*b),
                };
                (column.name.clone(), value)
            })
            .collect();

        let object = serde_json::json!({
            "table": table,
            "columns": columns,
        });
        Ok(serde_json::to_string(&object)?)
    }
}

/// One table's buffered lines and open file.
#[derive(Debug, Default)]
struct Entry {
    lines: Vec<String>,
    file: Option<std::fs::File>,
}

impl RowSink for JsonLinesRowSink {
    async fn write(&mut self, table: &str, row: &dyn Row) -> anyhow::Result<()> {
        let line = Self::render(table, row)?;
        self.entries
            .entry(table.to_owned())
            .or_default()
            .lines
            .push(line);
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        // Resolved once up front: the loop holds a mutable borrow of `entries`, so it
        // cannot also borrow `self` to build a path inside it.
        let directory = self.directory.clone();
        for (table, entry) in &mut self.entries {
            if entry.lines.is_empty() {
                continue;
            }
            // Opened on first use and kept, so a run does not pay an open per batch.
            if entry.file.is_none() {
                entry.file = Some(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(directory.join(format!("{table}.ndjson")))?,
                );
            }
            let Some(file) = entry.file.as_mut() else {
                continue;
            };
            for line in &entry.lines {
                writeln!(file, "{line}")?;
            }
            // The durability point: one flush for the whole batch, so a crash between
            // this and an offset commit replays the batch rather than losing it.
            file.flush()?;
            entry.lines.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use crate::column::{Column, Value};
    use crate::{Row, RowSink as _};

    use super::JsonLinesRowSink;

    /// A row with the two value shapes that matter: a number and a wide decimal.
    struct SampleRow;

    impl Row for SampleRow {
        fn table(&self) -> &'static str {
            "Swap"
        }

        fn columns(&self) -> &[Column] {
            static COLUMNS: std::sync::LazyLock<Vec<Column>> = std::sync::LazyLock::new(|| {
                vec![
                    Column {
                        name: "amount0".to_owned(),
                        value: Value::Integer(-3_180_585_820_646_654),
                    },
                    Column {
                        name: "sqrt_price_x96".to_owned(),
                        value: Value::Decimal("4115542941155561242646778".to_owned()),
                    },
                    Column {
                        name: "pool".to_owned(),
                        value: Value::Text("0xdeadbeef".to_owned()),
                    },
                ]
            });
            &COLUMNS
        }
    }
    fn temp_dir(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("materialize-sink-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        path
    }

    /// Rows land in a file named for their table, and the account narrows to the
    /// types a store can hold: an `i64` stays a number, a wide decimal stays exact.
    #[tokio::test]
    async fn rows_write_to_a_file_named_for_their_table() {
        let dir = temp_dir("by-table");
        let mut sink = JsonLinesRowSink::new(&dir).expect("sink opens");
        sink.write("Swap", &SampleRow).await.expect("row buffers");
        sink.flush().await.expect("batch flushes");

        let text = std::fs::read_to_string(dir.join("Swap.ndjson")).expect("file reads");
        let line: serde_json::Value = serde_json::from_str(text.trim()).expect("line parses");
        assert_eq!(line["table"], "Swap");
        assert_eq!(line["columns"]["amount0"], -3_180_585_820_646_654_i64);
        // Exact text, not a rounded float.
        assert_eq!(
            line["columns"]["sqrt_price_x96"],
            "4115542941155561242646778"
        );
        assert_eq!(line["columns"]["pool"], "0xdeadbeef");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Nothing is durable before the flush, which is what makes the caller's commit
    /// point correct.
    #[tokio::test]
    async fn nothing_is_written_before_the_flush() {
        let dir = temp_dir("before-flush");
        let mut sink = JsonLinesRowSink::new(&dir).expect("sink opens");
        sink.write("Swap", &SampleRow).await.expect("row buffers");
        assert!(
            !dir.join("Swap.ndjson").exists(),
            "a buffered row must not be on disk before the flush"
        );
        sink.flush().await.expect("batch flushes");
        assert!(dir.join("Swap.ndjson").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Two tables produce two files, so a run does not merge unrelated rows.
    #[tokio::test]
    async fn each_table_gets_its_own_file() {
        struct Other;
        impl Row for Other {
            fn table(&self) -> &'static str {
                "Transfer"
            }
            fn columns(&self) -> &[Column] {
                static COLUMNS: std::sync::LazyLock<Vec<Column>> = std::sync::LazyLock::new(|| {
                    vec![Column {
                        name: "value".to_owned(),
                        value: Value::Integer(1),
                    }]
                });
                &COLUMNS
            }
        }

        let dir = temp_dir("two-tables");
        let mut sink = JsonLinesRowSink::new(&dir).expect("sink opens");
        sink.write("Swap", &SampleRow).await.expect("row buffers");
        sink.write("Transfer", &Other).await.expect("row buffers");
        sink.flush().await.expect("batch flushes");

        assert!(dir.join("Swap.ndjson").exists());
        assert!(dir.join("Transfer.ndjson").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A run that sees no rows of a table leaves no file for it, so the directory
    /// reflects what was actually materialized.
    #[tokio::test]
    async fn an_unwritten_table_leaves_no_file() {
        let dir = temp_dir("no-empty");
        let mut sink = JsonLinesRowSink::new(&dir).expect("sink opens");
        sink.flush().await.expect("an empty flush is not an error");
        assert_eq!(
            std::fs::read_dir(&dir).expect("dir reads").count(),
            0,
            "an empty run must not create files"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
