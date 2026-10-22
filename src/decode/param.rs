//! The declared type of an ABI parameter, as the wire publishes it.
//!
//! Kept in decode rather than beside [`AbiType`]: reading an ABI file is decode's job,
//! and the wire contract should not depend on `alloy-json-abi` to describe a type.

use alloy_json_abi::Param;

use crate::wire::typed::{AbiComponent, AbiType};

/// The declared type of a parameter whose Solidity type is `ty` and whose tuple
/// components, empty for any other type, are `components`.
///
/// Takes the two fields rather than a parameter, so an event's input (`EventParam`) and
/// a tuple's component (`Param`), which carry the same two fields, share one body.
pub(crate) fn abi_type(ty: &str, components: &[Param]) -> AbiType {
    AbiType {
        kind: ty.to_owned(),
        components: components
            .iter()
            .map(|component| AbiComponent {
                name: component.name.clone(),
                abi_type: abi_type(&component.ty, &component.components),
            })
            .collect(),
    }
}
