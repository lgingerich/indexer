//! What both EVM sources check a fetched block against, and the order its rows become
//! events in.
//!
//! The RPC source and the `HyperSync` source read different wire formats, but a block
//! from either must be the same block: so each decodes its own rows, and the checks
//! and the event order they share live here, worded once.

use alloy_primitives::Bloom;

use super::{SourceError, inconsistent, malformed};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::Event;

/// One block's rows, decoded, before they become its events.
#[derive(Debug, Default)]
pub(super) struct BlockRows {
    /// The `blocks` row, when selected.
    pub(super) block: Option<Block>,
    /// Each transaction, in index order, with its logs when they are selected.
    pub(super) transactions: Vec<(Transaction, Vec<Log>)>,
    /// Logs read without their transactions, in `logIndex` order.
    pub(super) logs: Vec<Log>,
}

impl BlockRows {
    /// The events, in the order every source emits them: the block row, then each
    /// transaction followed by its logs, then the logs read without a transaction. A
    /// consumer so sees every transaction before its logs.
    pub(super) fn into_events(self) -> Vec<Event> {
        let Self {
            block,
            transactions,
            logs,
        } = self;
        let mut events = Vec::new();
        events.extend(block.map(|block| Event::Block(Box::new(block))));
        for (transaction, own) in transactions {
            events.push(Event::Transaction(Box::new(transaction)));
            events.extend(own.into_iter().map(|log| Event::Log(Box::new(log))));
        }
        events.extend(logs.into_iter().map(|log| Event::Log(Box::new(log))));
        events
    }
}

/// A block fetched for `requested` is at that height.
pub(super) fn check_height(
    context: &str,
    requested: u64,
    received: u64,
) -> Result<(), SourceError> {
    if requested == received {
        return Ok(());
    }
    Err(malformed(
        context,
        format!("requested block {requested}, received {received}"),
    ))
}

/// A block's logs come in strictly ascending `logIndex` order. They are refused rather
/// than sorted, since a source that misorders them is not one to trust with the rest of
/// the answer.
pub(super) fn check_log_order<T: PartialOrd>(
    context: &str,
    height: u64,
    indices: impl IntoIterator<Item = T>,
) -> Result<(), SourceError> {
    let mut previous = None;
    for index in indices {
        if previous.as_ref().is_some_and(|previous| *previous >= index) {
            return Err(malformed(
                context,
                format!("logs of block {height} are not in ascending logIndex order"),
            ));
        }
        previous = Some(index);
    }
    Ok(())
}

/// A block whose `logsBloom` is set has logs, so a source that answered none for it has
/// not indexed the block yet; asking again can succeed.
pub(super) fn check_logs_present(
    context: &str,
    height: u64,
    returned: bool,
    bloom: &Bloom,
) -> Result<(), SourceError> {
    if returned || *bloom == Bloom::ZERO {
        return Ok(());
    }
    Err(inconsistent(
        context,
        format!("no logs returned for block {height}, whose logsBloom is set"),
    ))
}

/// Every row of a range's answer went to one of its blocks: `leftover` is a row that
/// did not, which lies outside the blocks `from ..= last` or came out of order.
pub(super) fn check_no_leftovers(
    context: &str,
    from: u64,
    last: u64,
    leftover: bool,
) -> Result<(), SourceError> {
    if !leftover {
        return Ok(());
    }
    Err(malformed(
        context,
        format!("rows outside the blocks {from}..={last}, or out of order"),
    ))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Bloom;

    use super::{check_height, check_log_order, check_logs_present, check_no_leftovers};
    use crate::ingest::source::SourceError;

    #[test]
    fn each_check_refuses_what_it_names() {
        assert!(check_height("test", 5, 5).is_ok());
        assert!(matches!(
            check_height("test", 5, 6),
            Err(SourceError::Malformed { .. })
        ));

        assert!(check_log_order("test", 1, [0, 1, 3]).is_ok());
        assert!(
            check_log_order("test", 1, [0, 2, 2]).is_err(),
            "a duplicate"
        );
        assert!(check_log_order("test", 1, [1, 0]).is_err(), "misordered");

        let bloom = Bloom::repeat_byte(1);
        assert!(check_logs_present("test", 1, false, &Bloom::ZERO).is_ok());
        assert!(check_logs_present("test", 1, true, &bloom).is_ok());
        assert!(matches!(
            check_logs_present("test", 1, false, &bloom),
            Err(SourceError::Inconsistent { .. })
        ));

        assert!(check_no_leftovers("test", 1, 2, false).is_ok());
        assert!(check_no_leftovers("test", 1, 2, true).is_err());
    }
}
