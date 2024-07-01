#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod compression;
mod filters;
mod segment;

use alloy_primitives::BlockNumber;
pub use compression::Compression;
pub use filters::{Filters, InclusionFilter, PerfectHashingFunction};
pub use segment::{SegmentConfig, SegmentHeader, SegmentRangeInclusive, StaticFileSegment};

/// Default static file block count.
/// Specifies the number of blocks contained in each static file.
/// The initial value is equal to 500.00
pub const BLOCKS_PER_STATIC_FILE: u64 = 500_000;

/// Highest static file block numbers, per data segment.
/// This struct keeps track of the highest block numbers for each type of static file segment.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct HighestStaticFiles {
    /// Highest static file block of headers.
    /// If `None`, no static file is available for headers.
    pub headers: Option<BlockNumber>,
    /// Highest static file block of receipts.
    /// If `None`, no static file is available for receipts.
    pub receipts: Option<BlockNumber>,
    /// Highest static file block of transactions.
    /// If `None`, no static file is available for transactions.
    pub transactions: Option<BlockNumber>,
}

impl HighestStaticFiles {
    /// Returns the highest static file block number for a given segment, if it exists.
    pub const fn highest(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        match segment {
            StaticFileSegment::Headers => self.headers,
            StaticFileSegment::Transactions => self.transactions,
            StaticFileSegment::Receipts => self.receipts,
        }
    }

    /// Returns a mutable reference to the highest static file block number for a given segment.
    pub fn as_mut(&mut self, segment: StaticFileSegment) -> &mut Option<BlockNumber> {
        match segment {
            StaticFileSegment::Headers => &mut self.headers,
            StaticFileSegment::Transactions => &mut self.transactions,
            StaticFileSegment::Receipts => &mut self.receipts,
        }
    }

    /// Returns the minimum block number among all segments.
    pub fn min(&self) -> Option<u64> {
        [self.headers, self.transactions, self.receipts].iter().filter_map(|&option| option).min()
    }

    /// Returns the maximum block number among all segments.
    pub fn max(&self) -> Option<u64> {
        [self.headers, self.transactions, self.receipts].iter().filter_map(|&option| option).max()
    }
}

/// Each static file has a fixed number of blocks. This function calculates the range
/// where the requested block is positioned. Used for determining the segment filename.
pub const fn find_fixed_range(block: BlockNumber) -> SegmentRangeInclusive {
    let start = (block / BLOCKS_PER_STATIC_FILE) * BLOCKS_PER_STATIC_FILE;
    SegmentRangeInclusive::new(start, start + BLOCKS_PER_STATIC_FILE - 1)
}
