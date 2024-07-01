/// The segments refer to different categories or types of data that can be stored in static files.
/// These segments are defined by the StaticFileSegment enum, which categorizes various types of data that can 
/// be serialized and stored in a static file format for efficient access and retrieval.
use crate::{BlockNumber, Compression, Filters, InclusionFilter};
use alloy_primitives::TxNumber;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{ops::RangeInclusive, str::FromStr};
use strum::{AsRefStr, EnumIter, EnumString};

/// Segment of the data that can be moved to static files.
#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    EnumString,
    EnumIter,
    AsRefStr,
    Display,
)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum StaticFileSegment {
    #[strum(serialize = "headers")]
    /// Static File segment responsible for the `CanonicalHeaders`, `Headers`,
    /// `HeaderTerminalDifficulties` tables.
    Headers,

    #[strum(serialize = "transactions")]
    /// Static File segment responsible for the `Transactions` table.
    Transactions,

    #[strum(serialize = "receipts")]
    /// Static File segment responsible for the `Receipts` table.
    Receipts,
}

impl StaticFileSegment {
    /// Returns the segment as a string.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Headers => "headers",
            Self::Transactions => "transactions",
            Self::Receipts => "receipts",
        }
    }

    /// Returns the default configuration of the segment.
    pub const fn config(&self) -> SegmentConfig {
        let default_config = SegmentConfig {
            filters: Filters::WithFilters(
                InclusionFilter::Cuckoo,
                crate::PerfectHashingFunction::Fmph,
            ),
            compression: Compression::Lz4,
        };

        match self {
            Self::Headers | Self::Transactions | Self::Receipts => default_config,
        }
    }

    /// Returns the number of columns for the segment.
    pub const fn columns(&self) -> usize {
        match self {
            Self::Headers => 3,
            Self::Transactions | Self::Receipts => 1,
        }
    }

    /// Returns the default file name for the provided segment and range.
    pub fn filename(&self, block_range: &SegmentRangeInclusive) -> String {
        format!("static_file_{}_{}_{}", self.as_ref(), block_range.start(), block_range.end())
    }

    /// Returns file name for the provided segment and range, alongside filters, compression.
    pub fn filename_with_configuration(
        &self,
        filters: Filters,
        compression: Compression,
        block_range: &SegmentRangeInclusive,
    ) -> String {
        let prefix = self.filename(block_range);

        let filters_name = match filters {
            Filters::WithFilters(inclusion_filter, phf) => {
                format!("{}-{}", inclusion_filter.as_ref(), phf.as_ref())
            }
            Filters::WithoutFilters => "none".to_string(),
        };

        format!("{prefix}_{}_{}", filters_name, compression.as_ref())
    }

    /// Parses a filename into a `StaticFileSegment` and its expected block range.
    pub fn parse_filename(name: &str) -> Option<(Self, SegmentRangeInclusive)> {
        let mut parts = name.split('_');
        if !(parts.next() == Some("static") && parts.next() == Some("file")) {
            return None;
        }

        let segment = Self::from_str(parts.next()?).ok()?;
        let (block_start, block_end) = (parts.next()?.parse().ok()?, parts.next()?.parse().ok()?);

        if block_start > block_end {
            return None;
        }

        Some((segment, SegmentRangeInclusive::new(block_start, block_end)))
    }

    /// Returns `true` if the segment is `StaticFileSegment::Headers`.
    pub const fn is_headers(&self) -> bool {
        matches!(self, Self::Headers)
    }

    /// Returns `true` if the segment is `StaticFileSegment::Receipts`.
    pub const fn is_receipts(&self) -> bool {
        matches!(self, Self::Receipts)
    }
}

/// A segment header that contains information common to all segments. Used for storage.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct SegmentHeader {
    expected_block_range: SegmentRangeInclusive,
    block_range: Option<SegmentRangeInclusive>,
    tx_range: Option<SegmentRangeInclusive>,
    segment: StaticFileSegment,
}

impl SegmentHeader {
    /// Returns [`SegmentHeader`].
    pub const fn new(
        expected_block_range: SegmentRangeInclusive,
        block_range: Option<SegmentRangeInclusive>,
        tx_range: Option<SegmentRangeInclusive>,
        segment: StaticFileSegment,
    ) -> Self {
        Self {
            expected_block_range,
            block_range,
            tx_range,
            segment,
        }
    }

    /// Returns the static file segment kind.
    pub const fn segment(&self) -> StaticFileSegment {
        self.segment
    }

    /// Returns the block range.
    pub const fn block_range(&self) -> Option<&SegmentRangeInclusive> {
        self.block_range.as_ref()
    }

    /// Returns the transaction range.
    pub const fn tx_range(&self) -> Option<&SegmentRangeInclusive> {
        self.tx_range.as_ref()
    }

    /// The expected block start of the segment.
    pub const fn expected_block_start(&self) -> BlockNumber {
        self.expected_block_range.start()
    }

    /// The expected block end of the segment.
    pub const fn expected_block_end(&self) -> BlockNumber {
        self.expected_block_range.end()
    }

    /// Returns the first block number of the segment.
    pub fn block_start(&self) -> Option<BlockNumber> {
        self.block_range.as_ref().map(|b| b.start())
    }

    /// Returns the last block number of the segment.
    pub fn block_end(&self) -> Option<BlockNumber> {
        self.block_range.as_ref().map(|b| b.end())
    }

    /// Returns the first transaction number of the segment.
    pub fn tx_start(&self) -> Option<TxNumber> {
        self.tx_range.as_ref().map(|t| t.start())
    }

    /// Returns the last transaction number of the segment.
    pub fn tx_end(&self) -> Option<TxNumber> {
        self.tx_range.as_ref().map(|t| t.end())
    }

    /// Number of transactions.
    pub fn tx_len(&self) -> Option<u64> {
        self.tx_range
            .as_ref()
            .map(|r| (r.end() + 1) - r.start())
    }

    /// Number of blocks.
    pub fn block_len(&self) -> Option<u64> {
        self.block_range
            .as_ref()
            .map(|r| (r.end() + 1) - r.start())
    }

    /// Increments block end range depending on segment.
    /// increment_block method in the SegmentHeader struct adjusts or extends the end boundary of the block range (block_range)
    /// based on the type of data segment (StaticFileSegment)
    /// This functionality is crucial for managing and dynamically updating the range of blocks 
    ///within different segments of static files or data efficiently.


    pub fn increment_block(&mut self) -> BlockNumber {
        if let Some(block_range) = &mut self.block_range {
            block_range.end += 1;
            block_range.end
        } else {
            self.block_range = Some(SegmentRangeInclusive::new(
                self.expected_block_start(),
                self.expected_block_start(),
            ));
            self.expected_block_start()
        }
    }

    /// Increments tx end range depending on segment.
    /// Modifies the end boundary of the transaction range (tx_range) in the SegmentHeader struct.
    pub fn increment_tx(&mut self) {
        match self.segment {
            StaticFileSegment::Headers => (),
            StaticFileSegment::Transactions | StaticFileSegment::Receipts => {
                if let Some(tx_range) = &mut self.tx_range {
                    tx_range.end += 1;
                } else {
                    self.tx_range = Some(SegmentRangeInclusive::new(0, 0));
                }
            }
        }
    }

    /// Removes `num` elements from end of tx or block range.
    /// The ability to remove elements from the end of a range (tx_range or block_range)
    /// in the SegmentHeader struct provides flexibility and control over how data ranges are managed within the application.
    /// It supports efficient memory usage, data management practices like pruning, and ensures accurate representation of the current state of stored data.


    pub fn prune(&mut self, num: u64) {
        match self.segment {
            StaticFileSegment::Headers => {
                if let Some(range) = &mut self.block_range {
                    if num > range.end {
                        self.block_range = None;
                    } else {
                        range.end = range.end.saturating_sub(num);
                    }
                };
            }
            StaticFileSegment::Transactions | StaticFileSegment::Receipts => {
                if let Some(range) = &mut self.tx_range {
                    if num > range.end {
                        self.tx_range = None;
                    } else {
                        range.end = range.end.saturating_sub(num);
                    }
                };
            }
        };
    }

    /// Sets a new `block_range`.
    pub fn set_block_range(&mut self, block_start: BlockNumber, block_end: BlockNumber) {
        if let Some(block_range) = &mut self.block_range {
            block_range.start = block_start;
            block_range.end = block_end;
        } else {
            self.block_range = Some(SegmentRangeInclusive::new(block_start, block_end))
        }
    }

    /// Sets a new `tx_range`.
    pub fn set_tx_range(&mut self, tx_start: TxNumber, tx_end: TxNumber) {
        if let Some(tx_range) = &mut self.tx_range {
            tx_range.start = tx_start;
            tx_range.end = tx_end;
        } else {
            self.tx_range = Some(SegmentRangeInclusive::new(tx_start, tx_end))
        }
    }

    /// Returns the row offset which depends on whether the segment is block or transaction based.
   
