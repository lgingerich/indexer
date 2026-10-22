//! Why ingestion cannot continue safely.

use thiserror::Error;

use crate::ingest::source::SourceError;
use crate::sink::SinkError;
use crate::wire::envelope::BlockMeta;

/// Why ingestion cannot continue safely.
///
/// The identity-bearing variants box their [`BlockMeta`] fields: an unboxed pair is 160
/// bytes, which would make every `Result` in the pipeline carry that on the error path.
/// The allocations happen only when a run is already stopping.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// A block read for one height came back at another.
    #[error("requested the block at height {requested}, received {actual:?}")]
    WrongHeight {
        /// The height asked for.
        requested: u64,
        /// The block returned.
        actual: Box<BlockMeta>,
    },
    /// Returned block identity disagrees with the request.
    #[error("requested block {expected:?}, received {actual:?}")]
    IdentityMismatch {
        /// Expected identity.
        expected: Box<BlockMeta>,
        /// Returned identity.
        actual: Box<BlockMeta>,
    },
    /// Heights or parent hashes do not form a contiguous chain.
    #[error("block {actual:?} does not extend {previous:?}")]
    BrokenLink {
        /// Expected predecessor.
        previous: Box<BlockMeta>,
        /// Returned successor.
        actual: Box<BlockMeta>,
    },
    /// A fork reaches below the oldest identity the window still remembers.
    #[error("fork at {actual:?} is below the recovered floor {floor:?}")]
    UndoWindowExceeded {
        /// Oldest retained identity.
        floor: Box<BlockMeta>,
        /// The unretained replacement.
        actual: Box<BlockMeta>,
    },
    /// A requested historical start is above the sampled head.
    #[error("historical start {start} is above the sampled head {head}")]
    StartAboveHead {
        /// Requested start height.
        start: u64,
        /// Sampled head height.
        head: u64,
    },
    /// A start height was requested above the block after the store's tip.
    ///
    /// Resuming from the store would index heights below the start, and starting there
    /// would leave a gap above the tip, so the run refuses to guess which was meant. A
    /// start at or below the next height is covered by the stored history and resumes.
    #[error(
        "a start at height {start} was requested, but the store holds blocks only through \
         {tip}; starting there would leave a gap, so remove ingest.start_block to resume \
         from the store"
    )]
    StartAboveHistory {
        /// Requested start height.
        start: u64,
        /// The restored tip's height.
        tip: u64,
    },
    /// A requested stop is above the sampled head.
    #[error("stop {stop} is above the sampled head {head}")]
    StopAboveHead {
        /// Requested stop height.
        stop: u64,
        /// Sampled head height.
        head: u64,
    },
    /// A requested stop is below the height the run starts at: the requested start, or
    /// the sampled head when none was requested.
    #[error("stop {stop} is below the start {start}")]
    StopBelowStart {
        /// Requested stop height.
        stop: u64,
        /// The height the run starts at.
        start: u64,
    },
    /// A source that does not follow heads has no stored blocks and no start height to
    /// backfill from: starting at the head leaves no buried height below it.
    #[error(
        "a source that does not follow the chain needs ingest.start_block, or a store that \
         already holds blocks"
    )]
    BackfillWithoutStart,
    /// A fresh run on a source that does not follow heads was asked to start above the
    /// heights it backfills: those buried under its finality depth.
    #[error(
        "a source that does not follow the chain indexes only buried heights, through \
         {buried} now, and ingest.start_block {start} is above them; start lower, or run \
         on a source that follows the chain"
    )]
    StartNotBuried {
        /// Requested start height.
        start: u64,
        /// The highest buried height under the sampled head.
        buried: u64,
    },
    /// A range fetch returned other than one block per requested height.
    #[error("source returned {returned} blocks for heights {from}..={to}")]
    InvalidRange {
        /// First requested height.
        from: u64,
        /// Last requested height.
        to: u64,
        /// How many blocks it returned.
        returned: usize,
    },
    /// A height-only source changed views during reconciliation.
    #[error("source changed canonical views during reconciliation")]
    UnstableSource,
    /// A live head arrived with no accepted block to reconcile it against. Startup
    /// accepts one before going live, so this is a pipeline bug.
    #[error("no accepted block to reconcile a head against")]
    NoTip,
    /// Height progression would overflow.
    #[error("cannot advance beyond height {height}")]
    HeightOverflow {
        /// Last height.
        height: u64,
    },
    /// A live subscription ended without an error item.
    #[error("head subscription closed")]
    SubscriptionClosed,
    /// Source failure.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Sink failure.
    #[error(transparent)]
    Sink(#[from] SinkError),
}
