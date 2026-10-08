//! Each decoded event's typed table, generated from its ABI at startup.
//!
//! An event's table has the log's columns, then one column per argument in ABI order,
//! then the block's columns:
//!
//! ```text
//! uniswap_v3_pool_swap
//!   address, transaction_hash, transaction_index, log_index,
//!   sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick,
//!   block_number, block_hash, block_timestamp, chain, dedupe_key
//! ```
//!
//! An argument's column is its name in `snake_case`; an unnamed argument is `arg{n}`, and
//! a name that repeats an earlier column gets `_{n}` appended. A row's key is the decoded
//! record's own `dedupe_key`.

use std::sync::Arc;

use alloy_primitives::B256;

use crate::wire::envelope::{ChainId, Decoded, DecodedArg};
use crate::wire::typed::{AbiType, TypedValue};

use super::{Column, ColumnType, PRIMARY_KEY, Row, Schema, TableDef, TableError, TableId, Value};

/// The columns before an event's arguments.
const LEADING: [(&str, ColumnType); 4] = [
    ("address", ColumnType::Text),
    ("transaction_hash", ColumnType::Text),
    ("transaction_index", ColumnType::Uint),
    ("log_index", ColumnType::Uint),
];

/// The columns after an event's arguments, before the primary key.
const TRAILING: [(&str, ColumnType); 3] = [
    ("block_number", ColumnType::Uint),
    ("block_hash", ColumnType::Text),
    ("block_timestamp", ColumnType::Timestamp),
];

impl Schema {
    /// Adds the table for one event, whose arguments are `params` in ABI order with their
    /// ABI names.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::DuplicateTable`], adding nothing, when a table is already
    /// named `name`, and another [`TableError`] when the generated table is inconsistent.
    pub fn add_event(
        &mut self,
        (protocol, contract, event_id): (&str, &str, B256),
        name: String,
        params: Vec<Column>,
    ) -> Result<(), TableError> {
        if self.tables.iter().any(|table| table.name == name) {
            return Err(TableError::DuplicateTable { table: name });
        }
        let reserved: Vec<&str> = LEADING
            .iter()
            .chain(&TRAILING)
            .map(|(name, _)| *name)
            .chain(PRIMARY_KEY)
            .collect();
        let mut names: Vec<String> = Vec::with_capacity(params.len());
        for (position, param) in params.iter().enumerate() {
            let mut column = snake_case(&param.name);
            if column.is_empty() {
                column = format!("arg{position}");
            }
            if reserved.contains(&column.as_str()) || names.contains(&column) {
                column = format!("{column}_{position}");
            }
            names.push(column);
        }
        let id = TableId::Event(self.events.len());
        let mut table = TableDef::builder(id, name);
        for (column, kind) in LEADING {
            table = table.column(Column::new(column, kind, false));
        }
        for (column, param) in names.into_iter().zip(params) {
            table = table.column(Column::new(column, param.kind, param.nullable));
        }
        for (column, kind) in TRAILING {
            table = table.column(Column::new(column, kind, false));
        }
        let table = table.reorg_by("block_hash").build()?;
        self.events.insert(
            (protocol.to_owned(), contract.to_owned(), event_id),
            self.tables.len(),
        );
        self.tables.push(Arc::new(table));
        Ok(())
    }

    /// A decoded record's row in its event's table, or `None` when no table holds its
    /// event — which a record decoded against the same catalog never is.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Width`] when the record's arguments do not fill its table, and
    /// [`TableError::Json`] when an argument's document does not render.
    pub fn event_row(&self, chain: &ChainId, decoded: &Decoded) -> Result<Option<Row>, TableError> {
        let key = (
            decoded.protocol.clone(),
            decoded.contract.clone(),
            decoded.event_id,
        );
        let Some(table) = self
            .events
            .get(&key)
            .and_then(|&index| self.tables.get(index))
        else {
            return Ok(None);
        };
        let mut arguments: Vec<&DecodedArg> = decoded.indexed.iter().chain(&decoded.body).collect();
        arguments.sort_by_key(|argument| argument.position);
        let mut values = vec![
            hex(decoded.address),
            hex(decoded.transaction_hash),
            Value::Uint(decoded.transaction_index),
            Value::Uint(decoded.log_index),
        ];
        for (argument, column) in arguments
            .iter()
            .zip(table.columns.iter().skip(LEADING.len()))
        {
            values.push(
                cell(column.kind, argument).map_err(|source| TableError::Json {
                    table: table.name.clone(),
                    source,
                })?,
            );
        }
        values.extend([
            Value::Uint(decoded.block_number),
            hex(decoded.block_hash),
            Value::Timestamp(decoded.block_timestamp),
        ]);
        Row::new(table, values, chain.as_str(), decoded.dedupe_key()).map(Some)
    }
}

/// `sqrtPriceX96` as `sqrt_price_x96`: words split at a lower-to-upper change and before
/// the last capital of a run, lowercased, with leading underscores dropped and anything
/// but letters and digits replaced by `_`.
#[must_use]
pub fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.trim_start_matches('_').chars().collect();
    let mut out = String::with_capacity(chars.len() + 4);
    for (index, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            let previous = chars[index - 1];
            let next_is_lower = chars.get(index + 1).is_some_and(char::is_ascii_lowercase);
            if previous.is_ascii_lowercase()
                || previous.is_ascii_digit()
                || (previous.is_ascii_uppercase() && next_is_lower)
            {
                out.push('_');
            }
        }
        out.push(if c.is_ascii_alphanumeric() {
            c.to_ascii_lowercase()
        } else {
            '_'
        });
    }
    out
}

/// One decoded argument's cell, in the column type its table declares.
///
/// A scalar is the cell [`scalar`] renders, and an array of scalars is a list of them. A
/// tuple, or an array holding tuples or arrays, is a document in Allium's decoded `params`
/// form (see [`Params`]) rather than the raw ABI form the `decoded_logs` table keeps.
fn cell(kind: ColumnType, argument: &DecodedArg) -> Result<Value, serde_json::Error> {
    Ok(match (kind, &argument.value) {
        (
            ColumnType::List(element),
            TypedValue::Array { value } | TypedValue::FixedArray { value, .. },
        ) => Value::List(value.iter().map(|value| scalar(*element, value)).collect()),
        (ColumnType::Document, value) => Value::json(&Params {
            value,
            abi_type: &argument.abi_type,
        })?,
        (kind, value) => scalar(kind, value),
    })
}

/// One decoded scalar, in the column type its table declares.
///
/// The decoder has already checked each value against its declared width, so a `uint64`
/// fits [`Value::Uint`] and an `int64` fits [`Value::Int`]. A `string` that is not text,
/// or holds a NUL `PostgreSQL` would reject, is null; the raw log keeps its bytes.
fn scalar(kind: ColumnType, value: &TypedValue) -> Value {
    match (kind, value) {
        (ColumnType::Uint, TypedValue::Uint { value, .. }) => Value::Uint(value.saturating_to()),
        (ColumnType::Int, TypedValue::Int { value, .. }) => Value::Int(value.as_i64()),
        (ColumnType::BigInt, TypedValue::Uint { value, .. }) => Value::unsigned(*value),
        (ColumnType::BigInt, TypedValue::Int { value, .. }) => Value::BigInt {
            negative: value.is_negative(),
            magnitude: value.unsigned_abs(),
        },
        (ColumnType::Bool, TypedValue::Bool { value }) => Value::Bool(*value),
        (ColumnType::Text, TypedValue::Address { value }) => hex(value),
        (ColumnType::Text, TypedValue::IndexedHash { value }) => hex(value),
        (ColumnType::Text, TypedValue::Function { value }) => hex(value),
        (ColumnType::Text, TypedValue::FixedBytes { value, .. } | TypedValue::Bytes { value }) => {
            hex(value)
        }
        (ColumnType::Text, TypedValue::String { text, .. }) => text
            .as_ref()
            .filter(|text| !text.contains('\0'))
            .map_or(Value::Null, |text| Value::Text(text.clone())),
        _ => Value::Null,
    }
}

/// A hash, address, or bytes as the node's lowercase `0x` hex.
fn hex(value: impl std::fmt::LowerHex) -> Value {
    Value::Text(format!("{value:#x}"))
}

/// A composite decoded argument as Allium renders decoded `params`: a tuple is an object
/// keyed by its components' ABI names, an array is a list, every integer is a decimal
/// string, and an address or bytes is `0x` hex. A string that is not text is `null`.
///
/// Decimal strings rather than JSON numbers, as Allium has them, so a consumer that parses
/// JSON numbers as doubles cannot round a `uint256`. An unnamed component is keyed
/// `arg{n}`, as an unnamed argument's column is.
struct Params<'a> {
    value: &'a TypedValue,
    /// The value's declared type. An array's elements share it, since a tuple array's
    /// type carries its element tuple's components.
    abi_type: &'a AbiType,
}

impl serde::Serialize for Params<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap as _, SerializeSeq as _};

        match self.value {
            TypedValue::Bool { value } => serializer.serialize_bool(*value),
            TypedValue::Uint { value, .. } => serializer.collect_str(value),
            TypedValue::Int { value, .. } => serializer.collect_str(value),
            TypedValue::Address { value } => serializer.collect_str(&format_args!("{value:#x}")),
            TypedValue::IndexedHash { value } => {
                serializer.collect_str(&format_args!("{value:#x}"))
            }
            TypedValue::Function { value } => serializer.collect_str(&format_args!("{value:#x}")),
            TypedValue::FixedBytes { value, .. } | TypedValue::Bytes { value } => {
                serializer.collect_str(&format_args!("{value:#x}"))
            }
            TypedValue::String { text, .. } => match text {
                Some(text) => serializer.serialize_str(text),
                None => serializer.serialize_none(),
            },
            TypedValue::Array { value } | TypedValue::FixedArray { value, .. } => {
                let mut list = serializer.serialize_seq(Some(value.len()))?;
                for element in value {
                    list.serialize_element(&Params {
                        value: element,
                        abi_type: self.abi_type,
                    })?;
                }
                list.end()
            }
            TypedValue::Tuple { value } => {
                let mut object = serializer.serialize_map(Some(value.len()))?;
                for (position, (component, declared)) in
                    value.iter().zip(&self.abi_type.components).enumerate()
                {
                    if declared.name.is_empty() {
                        object.serialize_key(&format_args!("arg{position}"))?;
                    } else {
                        object.serialize_key(&declared.name)?;
                    }
                    object.serialize_value(&Params {
                        value: component,
                        abi_type: &declared.abi_type,
                    })?;
                }
                object.end()
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes, I256, TxHash, U256};

    use crate::sink::table::{Column, ColumnType, Schema, TableError, TableId, Value};
    use crate::wire::envelope::{ChainId, Decoded, DecodedArg};
    use crate::wire::typed::{AbiType, TypedValue};

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn chain() -> ChainId {
        ChainId::new("ethereum")
    }

    /// A decoded record with no arguments, for a test to fill in.
    fn decoded() -> Decoded {
        Decoded {
            event_id: hash(0x08),
            name: "E".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "p".to_owned(),
            contract: "C".to_owned(),
            selector: hash(0x07),
            signature: "E()".to_owned(),
            anonymous: false,
            transaction_hash: TxHash::from([0x11; 32]),
            transaction_index: 3,
            log_index: 7,
            indexed: Vec::new(),
            body: Vec::new(),
            block_number: 100,
            block_hash: hash(0x01),
            block_timestamp: 1_700_000_000,
        }
    }

    fn arg(position: usize, name: &str, value: TypedValue) -> DecodedArg {
        DecodedArg {
            position,
            name: name.to_owned(),
            abi_type: AbiType {
                kind: String::new(),
                components: Vec::new(),
            },
            value,
        }
    }

    /// Argument 5, `ids`: an `int24[2]` holding `[-1, 7]`.
    fn ids() -> DecodedArg {
        arg(
            5,
            "ids",
            TypedValue::FixedArray {
                value: vec![
                    TypedValue::Int {
                        value: I256::MINUS_ONE,
                        bits: 24,
                    },
                    TypedValue::Int {
                        value: I256::try_from(7).expect("fits"),
                        bits: 24,
                    },
                ],
                size: 2,
            },
        )
    }

    /// Argument 6, `legs`: a `(int256 delta, address, (bool flag, bytes data, string
    /// text) inner)[]` holding one tuple, whose string is not text.
    fn legs() -> DecodedArg {
        let param: alloy_json_abi::EventParam = serde_json::from_str(
            r#"{"name":"legs","type":"tuple[]","indexed":false,"components":[
                {"name":"delta","type":"int256"},
                {"name":"","type":"address"},
                {"name":"inner","type":"tuple","components":[
                    {"name":"flag","type":"bool"},
                    {"name":"data","type":"bytes"},
                    {"name":"text","type":"string"}
                ]}
            ]}"#,
        )
        .expect("the ABI parses");
        DecodedArg {
            position: 6,
            name: param.name.clone(),
            abi_type: AbiType::from(&param),
            value: TypedValue::Array {
                value: vec![TypedValue::Tuple {
                    value: vec![
                        TypedValue::Int {
                            value: I256::MINUS_ONE,
                            bits: 256,
                        },
                        TypedValue::Address {
                            value: Address::repeat_byte(1),
                        },
                        TypedValue::Tuple {
                            value: vec![
                                TypedValue::Bool { value: true },
                                TypedValue::Bytes {
                                    value: Bytes::from_static(&[0xff]),
                                },
                                TypedValue::String {
                                    value: Bytes::from_static(&[0xff]),
                                    text: None,
                                },
                            ],
                        },
                    ],
                }],
            },
        }
    }

    /// A schema with one event table, `p_c_e`, for event `0x09…` of contract `p.C`.
    fn event_schema() -> Schema {
        let column = |name: &str, kind| Column::new(name.to_owned(), kind, false);
        let mut schema = Schema::new().expect("the dataset tables");
        schema
            .add_event(
                ("p", "C", hash(0x09)),
                "p_c_e".to_owned(),
                vec![
                    column("amount", ColumnType::BigInt),
                    column("tick", ColumnType::Int),
                    column("", ColumnType::Uint),
                    column("blockHash", ColumnType::Text),
                    Column::new("note", ColumnType::Text, true),
                    column("ids", ColumnType::list(ColumnType::Int).expect("a scalar")),
                    column("legs", ColumnType::Document),
                ],
            )
            .expect("a new table");
        schema
    }

    /// An event's table gets a column per argument named from the ABI: unnamed ones by
    /// position, and one repeating a log column with its position appended. A name is
    /// taken once.
    #[test]
    fn an_event_table_names_a_column_per_argument() {
        let mut schema = event_schema();
        let table = schema.tables().last().expect("the event table");
        let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_ref()).collect();
        assert_eq!(
            names[4..11],
            [
                "amount",
                "tick",
                "arg2",
                "block_hash_3",
                "note",
                "ids",
                "legs"
            ]
        );
        assert!(matches!(
            schema.add_event(("p", "C", hash(0x0a)), "p_c_e".to_owned(), Vec::new()),
            Err(TableError::DuplicateTable { table }) if table == "p_c_e"
        ));
    }

    /// A decoded record fills its event's table: wide integers exact with their sign, a
    /// string that is not storable text as null, an array of scalars as a typed list, and
    /// an array of tuples as one document in Allium's `params` form.
    #[test]
    fn a_decoded_record_fills_its_event_table() {
        let schema = event_schema();
        let id = hash(0x09);
        let decoded = Decoded {
            event_id: id,
            protocol: "p".to_owned(),
            contract: "C".to_owned(),
            indexed: vec![arg(
                3,
                "blockHash",
                TypedValue::IndexedHash { value: hash(0x07) },
            )],
            body: vec![
                arg(
                    0,
                    "amount",
                    TypedValue::Int {
                        value: I256::MIN,
                        bits: 256,
                    },
                ),
                arg(
                    1,
                    "tick",
                    TypedValue::Int {
                        value: I256::try_from(-197_317).expect("fits"),
                        bits: 24,
                    },
                ),
                arg(
                    2,
                    "",
                    TypedValue::Uint {
                        value: U256::from(3000),
                        bits: 24,
                    },
                ),
                arg(
                    4,
                    "note",
                    TypedValue::String {
                        value: Bytes::from_static(b"a\0b"),
                        text: Some("a\0b".to_owned()),
                    },
                ),
                ids(),
                legs(),
            ],
            ..decoded()
        };
        let row = schema
            .event_row(&chain(), &decoded)
            .expect("a row")
            .expect("the event has a table");
        assert_eq!(row.table().id, TableId::Event(0));
        assert_eq!(row.dedupe_key(), decoded.dedupe_key());
        assert_eq!(
            row.value("amount"),
            Some(&Value::BigInt {
                negative: true,
                magnitude: I256::MIN.unsigned_abs(),
            })
        );
        assert_eq!(row.value("tick"), Some(&Value::Int(-197_317)));
        assert_eq!(row.value("arg2"), Some(&Value::Uint(3000)));
        assert_eq!(
            row.text("block_hash_3"),
            Some(format!("{:#x}", hash(0x07)).as_str())
        );
        assert_eq!(
            row.value("note"),
            Some(&Value::Null),
            "PostgreSQL rejects a NUL"
        );
        assert_eq!(
            row.value("ids"),
            Some(&Value::List(vec![Value::Int(-1), Value::Int(7)]))
        );
        assert_eq!(
            row.value("legs"),
            Some(&Value::Document(
                r#"[{"delta":"-1","arg1":"0x0101010101010101010101010101010101010101","inner":{"flag":true,"data":"0xff","text":null}}]"#
                    .to_owned()
            )),
            "keys in ABI order, unnamed by position, integers as decimal strings"
        );
        assert_eq!(
            row.text("block_hash"),
            Some(format!("{:#x}", decoded.block_hash).as_str())
        );

        let other = Decoded {
            contract: "Other".to_owned(),
            ..decoded
        };
        assert!(
            schema
                .event_row(&chain(), &other)
                .expect("no row to build")
                .is_none()
        );
    }
}
