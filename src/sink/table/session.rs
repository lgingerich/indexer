//! What a run of a store decoded: one `sessions` row a store writes with the first block
//! it writes after opening.
//!
//! A backfill reads the newest one to know that the running indexer decodes a protocol,
//! from which block, and with which event definitions, since a decoded row's key names
//! its definition. The row belongs to its block, so a reorg that orphans the block
//! deletes it, and the store writes a new one with the block that replaces it.

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// One run of a store: the first block it wrote, and every event it decoded from there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The first block the run wrote.
    pub block_number: u64,
    /// That block's hash.
    pub block_hash: B256,
    /// That block's timestamp.
    pub block_timestamp: u64,
    /// Every event definition the run decoded, sorted.
    pub events: Vec<SessionEvent>,
}

impl Session {
    /// A key unique per block, so writing a block's session again replaces it.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{:#x}:session", self.block_hash)
    }
}

/// One event definition a run decoded, as its decoded records name it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SessionEvent {
    /// The manifest's protocol.
    pub protocol: String,
    /// The contract's name.
    pub contract: String,
    /// The hash of the event's definition, part of every decoded record's key.
    pub event_id: B256,
    /// The event's table.
    pub table: String,
}

#[cfg(feature = "store")]
impl Session {
    /// A stored session's `events` document, read back.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidStoredValue`] when the document is not a list of events.
    pub(crate) fn events_from(
        document: &str,
    ) -> Result<Vec<SessionEvent>, crate::sink::store_error::InvalidStoredValue> {
        serde_json::from_str(document).map_err(|_| {
            crate::sink::store_error::InvalidStoredValue::new("sessions.events", document)
        })
    }
}
