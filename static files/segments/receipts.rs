use crate::segments::{dataset_for_compression, prepare_jar, Segment};
use alloy_primitives::{BlockNumber, TxNumber};
use reth_db::{static_file::create_static_file_T1, tables};
use reth_db_api::{cursor::DbCursorRO, database::Database, transaction::DbTx};
use reth_provider::{
    providers::{StaticFileProvider, StaticFileWriter},
    BlockReader, DatabaseProviderRO, TransactionsProviderExt,
};
use reth_static_file_types::{SegmentConfig, SegmentHeader, StaticFileSegment};
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use std::{ops::RangeInclusive, path::Path};

/// Static File segment responsible for [`StaticFileSegment::Receipts`] part of data.
#[derive(Debug, Default)]
pub struct Receipts;

impl<DB: Database> Segment<DB> for Receipts {
    /// Returns the specific `StaticFileSegment` that this segment handles (`StaticFileSegment::Receipts`).
    fn segment(&self) -> StaticFileSegment {
        StaticFileSegment::Receipts
    }

    /// Copies data to static files for the provided block range.
    /// [`StaticFileProvider`] will handle the management of and writing to files.
    fn copy_to_static_files(
        &self,
        provider: DatabaseProviderRO<DB>,
        static_file_provider: StaticFileProvider,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()> {
        // Get a writer for the static file segment based on the starting block number
        let mut static_file_writer =
            static_file_provider.get_writer(*block_range.start(), StaticFileSegment::Receipts)?;

        // Iterate over each block in the specified range
        for block in block_range {
            // Increment the block number in the static file writer
            let _static_file_block =
                static_file_writer.increment_block(StaticFileSegment::Receipts, block)?;
            debug_assert_eq!(_static_file_block, block);

            // Retrieve transaction indices for the current block
            let block_body_indices = provider
                .block_body_indices(block)?
                .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;

            // Create a cursor to read receipts from the database
            let mut receipts_cursor = provider.tx_ref().cursor_read::<tables::Receipts>()?;

            // Walk through receipts within the block's transaction range
            let receipts_walker = receipts_cursor.walk_range(block_body_indices.tx_num_range())?;

            // Append receipts to the static file using the writer
            static_file_writer.append_receipts(
                receipts_walker.map(|result| result.map_err(ProviderError::from)),
            )?;
        }

        Ok(())
    }

    /// Creates a static file for receipt data based on the block range and configuration provided.
    fn create_static_file_file(
        &self,
        provider: &DatabaseProviderRO<DB>,
        directory: &Path,
        config: SegmentConfig,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<()> {
        // Retrieve the transaction range for the specified block range
        let tx_range = provider.transaction_range_by_block_range(block_range.clone())?;
        let tx_range_len = tx_range.clone().count();

        // Prepare a NippyJar for compression and storage
        let jar = prepare_jar::<DB, 1>(
            provider,
            directory,
            StaticFileSegment::Receipts,
            config,
            block_range,
            tx_range_len,
            || {
                Ok([dataset_for_compression::<DB, tables::Receipts>(
                    provider,
                    &tx_range,
                    tx_range_len,
                )?])
            },
        )?;

        // Generate list of hashes for filters & PHF
        let hashes = if config.filters.has_filters() {
            Some(
                provider
                    .transaction_hashes_by_range(*tx_range.start()..(*tx_range.end() + 1))?
                    .into_iter()
                    .map(|(tx, _)| Ok(tx)),
            )
        } else {
            None
        };

        // Create the static file using the provided function
        create_static_file_T1::<tables::Receipts, TxNumber, SegmentHeader>(
            provider.tx_ref(),
            tx_range,
            None,
            // We already prepared the dictionary beforehand
            None::<Vec<std::vec::IntoIter<Vec<u8>>>>,
            hashes,
            tx_range_len,
            jar,
        )?;

        Ok(())
    }
}
