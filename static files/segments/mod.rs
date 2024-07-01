//! Module for handling static files, segments, and related utilities.

// Import necessary dependencies and modules
mod transactions;
pub use transactions::Transactions; // Export `Transactions` module

mod headers;
pub use headers::Headers; // Export `Headers` module

mod receipts;
pub use receipts::Receipts; // Export `Receipts` module

// Standard library and external crate imports
use alloy_primitives::BlockNumber;
use reth_db::{RawKey, RawTable}; // Database related imports
use reth_db_api::{cursor::DbCursorRO, database::Database, table::Table, transaction::DbTx}; // Database API imports
use reth_nippy_jar::NippyJar; // Import for NippyJar type
use reth_provider::{
    providers::StaticFileProvider, DatabaseProviderRO, ProviderError, TransactionsProviderExt,
}; // Provider related imports
use reth_static_file_types::{
    find_fixed_range, Compression, Filters, InclusionFilter, PerfectHashingFunction, SegmentConfig,
    SegmentHeader, StaticFileSegment,
}; // Static file types and configurations
use reth_storage_errors::provider::ProviderResult; // Error handling related to providers
use std::{ops::RangeInclusive, path::Path}; // Standard library imports

// Define a type alias for Rows
pub(crate) type Rows<const COLUMNS: usize> = [Vec<Vec<u8>>; COLUMNS];

/// A trait representing a segment that moves data to static files.
pub trait Segment<DB: Database>: Send + Sync {
    /// Returns the `StaticFileSegment`.
    fn segment(&self) -> StaticFileSegment;

    /// Copies data to static files for the provided block range.
    fn copy_to_static_files(
        &self,
        provider: DatabaseProviderRO<DB>,
        static_file_provider: StaticFileProvider,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()>;

    /// Creates a static file of data for the provided block range.
    fn create_static_file_file(
        &self,
        provider: &DatabaseProviderRO<DB>,
        directory: &Path,
        config: SegmentConfig,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()>;
}

/// Prepares a `NippyJar`(NippyJar seems to encapsulate functionality related to data compression, storage, and possibly retrieval)
/// according to the desired configuration.
pub(crate) fn prepare_jar<DB: Database, const COLUMNS: usize>(
    provider: &DatabaseProviderRO<DB>,
    directory: impl AsRef<Path>,
    segment: StaticFileSegment,
    segment_config: SegmentConfig,
    block_range: RangeInclusive<BlockNumber>,
    total_rows: usize,
    prepare_compression: impl Fn() -> ProviderResult<Rows<COLUMNS>>,
) -> ProviderResult<NippyJar<SegmentHeader>> {
    // Determine transaction range based on the segment type
    let tx_range = match segment {
        StaticFileSegment::Headers => None,
        StaticFileSegment::Receipts | StaticFileSegment::Transactions => {
            Some(provider.transaction_range_by_block_range(block_range.clone())?.into())
        }
    };

    // Initialize a `NippyJar` instance
    let mut nippy_jar = NippyJar::new(
        COLUMNS,
        &directory.as_ref().join(segment.filename(&find_fixed_range(*block_range.end())).as_str()),
        SegmentHeader::new(block_range.clone().into(), Some(block_range.into()), tx_range, segment),
    );

    // Handle compression based on segment configuration
    nippy_jar = match segment_config.compression {
        Compression::Lz4 => nippy_jar.with_lz4(),
        Compression::Zstd => nippy_jar.with_zstd(false, 0),
        Compression::ZstdWithDictionary => {
            let dataset = prepare_compression()?;
            nippy_jar = nippy_jar.with_zstd(true, 5_000_000);
            nippy_jar.prepare_compression(dataset.to_vec())
                .map_err(|e| ProviderError::NippyJar(e.to_string()))?;
            nippy_jar
        }
        Compression::Uncompressed => nippy_jar,
    };

    // Handle inclusion filters and perfect hashing functions
    if let Filters::WithFilters(inclusion_filter, phf) = segment_config.filters {
        nippy_jar = match inclusion_filter {
            InclusionFilter::Cuckoo => nippy_jar.with_cuckoo_filter(total_rows),
        };
        nippy_jar = match phf {
            PerfectHashingFunction::Fmph => nippy_jar.with_fmph(),
            PerfectHashingFunction::GoFmph => nippy_jar.with_gofmph(),
        };
    }

    Ok(nippy_jar)
}

/// Generates the dataset for compression using the most recent rows.
pub(crate) fn dataset_for_compression<DB: Database, T: Table<Key = u64>>(
    provider: &DatabaseProviderRO<DB>,
    range: &RangeInclusive<u64>,
    range_len: usize,
) -> ProviderResult<Vec<Vec<u8>>> {
    let mut cursor = provider.tx_ref().cursor_read::<RawTable<T>>()?;
    Ok(cursor.walk_back(Some(RawKey::from(*range.end())))?
        .take(range_len.min(1000))
        .map(|row| row.map(|(_key, value)| value.into_value()).expect("should exist"))
        .collect::<Vec<_>>())
}
