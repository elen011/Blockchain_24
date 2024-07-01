use crate::segments::{dataset_for_compression, prepare_jar, Segment, SegmentHeader};
use alloy_primitives::BlockNumber;
use reth_db::{static_file::create_static_file_T1_T2_T3, tables, RawKey, RawTable};
use reth_db_api::{cursor::DbCursorRO, database::Database, transaction::DbTx};
use reth_provider::{
    providers::{StaticFileProvider, StaticFileWriter},
    DatabaseProviderRO,
};
use reth_static_file_types::{SegmentConfig, StaticFileSegment};
use reth_storage_errors::provider::ProviderResult;
use std::{ops::RangeInclusive, path::Path};

/// Static File segment responsible for [`StaticFileSegment::Headers`] part of data.
#[derive(Debug, Default)]
pub struct Headers;

impl<DB: Database> Segment<DB> for Headers {
    /// Returns the specific segment handled by this struct.
    fn segment(&self) -> StaticFileSegment {
        StaticFileSegment::Headers
    }

    /// Copies header-related data within the specified block range to static files.
    fn copy_to_static_files(
        &self,
        provider: DatabaseProviderRO<DB>,
        static_file_provider: StaticFileProvider,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()> {
        // Retrieve a writer for the static file segment within the specified block range
        let mut static_file_writer =
            static_file_provider.get_writer(*block_range.start(), StaticFileSegment::Headers)?;

        // Obtain cursors to read headers, header terminal difficulties, and canonical headers
        let mut headers_cursor = provider.tx_ref().cursor_read::<tables::Headers>()?;
        let headers_walker = headers_cursor.walk_range(block_range.clone())?;

        let mut header_td_cursor =
            provider.tx_ref().cursor_read::<tables::HeaderTerminalDifficulties>()?;
        let header_td_walker = header_td_cursor.walk_range(block_range.clone())?;

        let mut canonical_headers_cursor =
            provider.tx_ref().cursor_read::<tables::CanonicalHeaders>()?;
        let canonical_headers_walker = canonical_headers_cursor.walk_range(block_range)?;

        // Iterate over the data from all three tables in sync
        for ((header_entry, header_td_entry), canonical_header_entry) in
            headers_walker.zip(header_td_walker).zip(canonical_headers_walker)
        {
            // Extract data entries from each cursor
            let (header_block, header) = header_entry?;
            let (header_td_block, header_td) = header_td_entry?;
            let (canonical_header_block, canonical_header) = canonical_header_entry?;

            // Assert that blocks match across all three entries
            debug_assert_eq!(header_block, header_td_block);
            debug_assert_eq!(header_td_block, canonical_header_block);

            // Append the header to the static file and verify the resulting block number
            let _static_file_block =
                static_file_writer.append_header(header, header_td.0, canonical_header)?;
            debug_assert_eq!(_static_file_block, header_block);
        }

        Ok(())
    }

    /// Creates a static file for the header segment with compressed data.
    fn create_static_file_file(
        &self,
        provider: &DatabaseProviderRO<DB>,
        directory: &Path,
        config: SegmentConfig,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()> {
        let range_len = block_range.clone().count();

        // Prepare data for compression using a closure
        let jar = prepare_jar::<DB, 3>(
            provider,
            directory,
            StaticFileSegment::Headers,
            config,
            block_range.clone(),
            range_len,
            || {
                Ok([
                    dataset_for_compression::<DB, tables::Headers>(
                        provider,
                        &block_range,
                        range_len,
                    )?,
                    dataset_for_compression::<DB, tables::HeaderTerminalDifficulties>(
                        provider,
                        &block_range,
                        range_len,
                    )?,
                    dataset_for_compression::<DB, tables::CanonicalHeaders>(
                        provider,
                        &block_range,
                        range_len,
                    )?,
                ])
            },
        )?;
        // Generate list of hashes for filters & PHF
        // Retrieve hashes if filters are enabled
        let mut cursor = provider.tx_ref().cursor_read::<RawTable<tables::CanonicalHeaders>>()?;
        let hashes = if config.filters.has_filters() {
            Some(
                cursor
                    .walk(Some(RawKey::from(*block_range.start())))?
                    .take(range_len)
                    .map(|row| row.map(|(_key, value)| value.into_value()).map_err(|e| e.into())),
            )
        } else {
            None
        };

        // Create the static file for headers using the prepared data
        create_static_file_T1_T2_T3::<
            tables::Headers,
            tables::HeaderTerminalDifficulties,
            tables::CanonicalHeaders,
            BlockNumber,
            SegmentHeader,
        >(
            provider.tx_ref(),
            block_range,
            None,  // No dictionary needed as it's prepared beforehand
            None::<Vec<std::vec::IntoIter<Vec<u8>>>>,  // No additional hashes needed
            hashes,  // Use the retrieved hashes if any
            range_len,
            jar,  // Use the prepared compressed data
        )?;

        Ok(())
    }
}
