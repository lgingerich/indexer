//! How an event's inputs map to its table's columns.

use alloy_dyn_abi::DynSolType;

use super::super::abi::{InputSpec, topic_hashed};
use crate::sink::table::{Column, ColumnType};

/// How one input is stored in its event's table: a column under its ABI name.
///
/// A scalar is its own typed column and an array of scalars a list of that type. A tuple,
/// or an array of tuples or arrays, is one document, so a tuple's components stay together.
pub(super) fn param(input: &InputSpec<'_>) -> Column {
    let (kind, required) = match input.ty {
        ty if input.indexed && topic_hashed(ty) => (ColumnType::Text, true),
        DynSolType::Array(element) | DynSolType::FixedArray(element, _) => scalar(element)
            .and_then(|(element, _)| ColumnType::list(element))
            .map_or((ColumnType::Document, true), |list| (list, true)),
        ty => scalar(ty).unwrap_or((ColumnType::Document, true)),
    };
    Column::new(input.name.to_owned(), kind, !required)
}

/// A scalar ABI type's column type and whether every value fills it; `None` for an array
/// or a tuple.
const fn scalar(ty: &DynSolType) -> Option<(ColumnType, bool)> {
    Some(match ty {
        DynSolType::Bool => (ColumnType::Bool, true),
        DynSolType::Uint(bits) if *bits <= 64 => (ColumnType::Uint, true),
        DynSolType::Int(bits) if *bits <= 64 => (ColumnType::Int, true),
        DynSolType::Uint(_) | DynSolType::Int(_) => (ColumnType::BigInt, true),
        DynSolType::Address
        | DynSolType::Function
        | DynSolType::FixedBytes(_)
        | DynSolType::Bytes => (ColumnType::Text, true),
        // Null when the bytes are not text.
        DynSolType::String => (ColumnType::Text, false),
        _ => return None,
    })
}
