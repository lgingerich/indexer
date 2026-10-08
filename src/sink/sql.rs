//! Every SQL statement the stores run, rendered from table definitions.
//!
//! Statements are built here from a [`TableDef`], never written by hand at a call site,
//! so identifier quoting, the primary key, and each table's indexes are decided in one
//! place. What differs between engines — type names, how a staging table is made — is a
//! [`Dialect`]. Every statement filters by chain as `$1`; both engines accept `$n`
//! placeholders.

use std::fmt::Write as _;

use crate::sink::table::{ColumnType, Index, PRIMARY_KEY, TableDef, TableError};

/// What an engine says differently.
pub trait Dialect {
    /// Whether the engine builds the tables' secondary indexes.
    const INDEXES: bool;

    /// The type a column of `kind` is stored as.
    fn type_name(kind: ColumnType) -> String;

    /// Makes `schema` the session's default, so later statements name tables unqualified.
    fn use_schema(schema: &str) -> String;

    /// Creates `staging`, an empty table shaped like `table`, inside the flush's
    /// transaction.
    fn create_staging(table: &TableDef, staging: &str) -> String;

    /// How a statement names `staging`.
    #[must_use]
    fn staging(staging: &str) -> String {
        ident(staging)
    }

    /// Drops `staging` after its merge, for an engine that does not drop it at commit.
    fn drop_staging(staging: &str) -> Option<String>;

    /// Lists table `$1`'s columns in the session's schema, as name and type rows in
    /// column order.
    const DESCRIBE: &'static str;

    /// The type a column of `kind` is reported as by [`DESCRIBE`](Self::DESCRIBE).
    #[must_use]
    fn reported_type(kind: ColumnType) -> String {
        Self::type_name(kind)
    }
}

/// `name` as a quoted SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The table's columns, quoted and comma-separated.
pub(crate) fn column_list(table: &TableDef) -> String {
    list(table.columns.iter().map(|column| column.name.as_ref()))
}

fn list<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    names.into_iter().map(ident).collect::<Vec<_>>().join(", ")
}

/// Creates the database schema `schema` if it does not exist.
pub(crate) fn create_schema(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {}", ident(schema))
}

/// Creates `table` if it does not exist, keyed on its primary key.
pub(crate) fn create_table<D: Dialect>(table: &TableDef) -> String {
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", ident(&table.name));
    for column in &table.columns {
        let null = if column.nullable { "" } else { " NOT NULL" };
        let _ = write!(
            sql,
            "{} {}{null}, ",
            ident(&column.name),
            D::type_name(column.kind)
        );
    }
    let _ = write!(sql, "PRIMARY KEY ({}))", list(PRIMARY_KEY));
    sql
}

/// Creates each of `table`'s indexes that does not exist, for an engine that builds them.
pub(crate) fn create_indexes<D: Dialect>(table: &TableDef) -> Vec<String> {
    if !D::INDEXES {
        return Vec::new();
    }
    table
        .indexes
        .iter()
        .map(|index| {
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
                ident(&index_name(&table.name, index)),
                ident(&table.name),
                list(index.columns.iter().map(AsRef::as_ref))
            )
        })
        .collect()
}

/// `PostgreSQL`'s identifier limit, in bytes; a longer name is silently truncated.
const MAX_IDENTIFIER: usize = 63;

/// `{table}_{columns}_index`, or, when that would pass [`MAX_IDENTIFIER`], cut short with
/// a hash of the whole name, so two long tables never truncate to one index name.
fn index_name(table: &str, index: &Index) -> String {
    let name = format!("{table}_{}_index", index.columns.join("_"));
    if name.len() <= MAX_IDENTIFIER {
        return name;
    }
    let hash = alloy_primitives::hex::encode(&alloy_primitives::keccak256(&name)[..4]);
    let mut end = MAX_IDENTIFIER - hash.len() - 1;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}_{hash}", &name[..end])
}

/// Merges `staging` into `table`, replacing a row whose primary key is already stored.
///
/// The staging table holds each key once, so the conflict clause only meets rows an
/// earlier batch wrote.
pub(crate) fn upsert<D: Dialect>(table: &TableDef, staging: &str) -> String {
    let columns = column_list(table);
    let assignments = table
        .columns
        .iter()
        .filter(|column| !PRIMARY_KEY.contains(&column.name.as_ref()))
        .map(|column| {
            let name = ident(&column.name);
            format!("{name} = EXCLUDED.{name}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) SELECT {columns} FROM {} \
         ON CONFLICT ({}) DO UPDATE SET {assignments}",
        ident(&table.name),
        D::staging(staging),
        list(PRIMARY_KEY)
    )
}

/// Deletes chain `$1`'s rows of block `$2`, for a table whose rows belong to a block.
pub(crate) fn delete_block(table: &TableDef) -> Option<String> {
    table.block_hash.as_ref().map(|column| {
        format!(
            "DELETE FROM {} WHERE \"chain\" = $1 AND {} = $2",
            ident(&table.name),
            ident(column)
        )
    })
}

/// Deletes chain `$1`'s rows whose `column` is below `$2`.
///
/// # Errors
///
/// Returns [`TableError::UnknownColumn`] when the table has no `column`.
pub(crate) fn delete_below(table: &TableDef, column: &str) -> Result<String, TableError> {
    table.require(column)?;
    Ok(format!(
        "DELETE FROM {} WHERE \"chain\" = $1 AND {} < $2",
        ident(&table.name),
        ident(column)
    ))
}

/// Selects chain `$1`'s rows of `table`.
pub(crate) fn select<'a>(table: &'a TableDef, columns: &'a [&'a str]) -> Select<'a> {
    Select {
        table,
        columns,
        newest_first: None,
        limit: false,
    }
}

/// A `SELECT` of one chain's rows.
#[derive(Debug)]
pub(crate) struct Select<'a> {
    table: &'a TableDef,
    columns: &'a [&'a str],
    newest_first: Option<&'a str>,
    limit: bool,
}

impl<'a> Select<'a> {
    /// Orders by `column`, highest first.
    pub(crate) const fn newest_first(mut self, column: &'a str) -> Self {
        self.newest_first = Some(column);
        self
    }

    /// Returns at most `$2` rows.
    pub(crate) const fn limit(mut self) -> Self {
        self.limit = true;
        self
    }

    /// The statement.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::UnknownColumn`] when it names a column the table does not
    /// have.
    pub(crate) fn render(&self) -> Result<String, TableError> {
        for column in self.columns.iter().chain(&self.newest_first) {
            self.table.require(column)?;
        }
        let mut sql = format!(
            "SELECT {} FROM {} WHERE \"chain\" = $1",
            list(self.columns.iter().copied()),
            ident(&self.table.name)
        );
        if let Some(column) = self.newest_first {
            let _ = write!(sql, " ORDER BY {} DESC", ident(column));
        }
        if self.limit {
            sql.push_str(" LIMIT $2");
        }
        Ok(sql)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sink::table::{Column, Schema, Table, TableId};

    struct Test;

    impl Dialect for Test {
        const INDEXES: bool = true;
        const DESCRIBE: &'static str = "DESCRIBE $1";

        fn type_name(kind: ColumnType) -> String {
            format!("{kind:?}")
        }

        fn use_schema(schema: &str) -> String {
            format!("USE {}", ident(schema))
        }

        fn create_staging(table: &TableDef, staging: &str) -> String {
            format!("STAGE {} {}", ident(&table.name), ident(staging))
        }

        fn drop_staging(_: &str) -> Option<String> {
            None
        }
    }

    fn table() -> TableDef {
        TableDef::builder(TableId::Event(0), "t")
            .column(Column::new("block_hash", ColumnType::Text, false))
            .column(Column::new("note", ColumnType::Text, true))
            .reorg_by("block_hash")
            .build()
            .expect("a valid table")
    }

    #[test]
    fn a_table_is_keyed_and_indexed_from_its_definition() {
        let def = table();
        assert_eq!(
            create_table::<Test>(&def),
            "CREATE TABLE IF NOT EXISTS \"t\" (\"block_hash\" Text NOT NULL, \"note\" Text, \
             \"chain\" Text NOT NULL, \"dedupe_key\" Text NOT NULL, \
             PRIMARY KEY (\"chain\", \"dedupe_key\"))"
        );
        assert_eq!(
            create_indexes::<Test>(&def),
            [
                "CREATE INDEX IF NOT EXISTS \"t_chain_block_hash_index\" ON \"t\" (\"chain\", \"block_hash\")"
            ]
        );
        assert_eq!(
            upsert::<Test>(&def, "s"),
            "INSERT INTO \"t\" (\"block_hash\", \"note\", \"chain\", \"dedupe_key\") \
             SELECT \"block_hash\", \"note\", \"chain\", \"dedupe_key\" FROM \"s\" \
             ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET \
             \"block_hash\" = EXCLUDED.\"block_hash\", \"note\" = EXCLUDED.\"note\""
        );
        assert_eq!(
            delete_block(&def).as_deref(),
            Some("DELETE FROM \"t\" WHERE \"chain\" = $1 AND \"block_hash\" = $2")
        );
    }

    #[test]
    fn reads_name_checked_columns() {
        let schema = Schema::new().expect("the dataset tables");
        let ledger = schema.dataset(Table::AcceptedBlock);
        assert_eq!(
            select(ledger, &["height", "hash"])
                .newest_first("height")
                .limit()
                .render()
                .expect("known columns"),
            "SELECT \"height\", \"hash\" FROM \"accepted_blocks\" WHERE \"chain\" = $1 \
             ORDER BY \"height\" DESC LIMIT $2"
        );
        assert_eq!(
            delete_below(ledger, "height").expect("a known column"),
            "DELETE FROM \"accepted_blocks\" WHERE \"chain\" = $1 AND \"height\" < $2"
        );
        assert!(matches!(
            select(ledger, &["nope"]).render(),
            Err(TableError::UnknownColumn { column, .. }) if column == "nope"
        ));
    }

    /// Two long tables that share a prefix get distinct index names within the limit.
    #[test]
    fn long_index_names_stay_distinct_and_within_the_limit() {
        let index = Index {
            columns: vec!["chain".into(), "block_hash".into()],
        };
        let first = index_name(&format!("{}_a", "x".repeat(60)), &index);
        let second = index_name(&format!("{}_b", "x".repeat(60)), &index);
        assert!(first.len() <= MAX_IDENTIFIER && second.len() <= MAX_IDENTIFIER);
        assert_ne!(first, second);
    }
}
