// Import necessary modules and functions from the crate and external dependencies
use crate::segments::{dataset_for_compression, prepare_jar, Segment};
use alloy_primitives::{BlockNumber, TxNumber};
use reth_db::{static_file::create_static_file_T1, tables}; // Import database and table utilities
use reth_db_api::{cursor::DbCursorRO, database::Database, transaction::DbTx}; // Import database APIs
use reth_provider::{ // Import provider-related utilities
    providers::{StaticFileProvider, StaticFileWriter}, // Static file providers
    BlockReader, DatabaseProviderRO, TransactionsProviderExt, // Providers for block reading and transactions
};
use reth_static_file_types::{SegmentConfig, SegmentHeader, StaticFileSegment}; // Import static file related types
use reth_storage_errors::provider::{ProviderError, ProviderResult}; // Import error handling utilities
use std::{ops::RangeInclusive, path::Path}; // Import standard library utilities

/// Static File segment responsible for [`StaticFileSegment::Transactions`] part of data.
#[derive(Debug, Default)]
pub struct Transactions;

impl<DB: Database> Segment<DB> for Transactions {
    /// Returns the specific `StaticFileSegment` that this segment handles (`StaticFileSegment::Transactions`).
    fn segment(&self) -> StaticFileSegment {
        StaticFileSegment::Transactions
    }

    /// Copy transactions from the database table [`tables::Transactions`] to static files
    /// with segment [`StaticFileSegment::Transactions`] for the provided block range.
    fn copy_to_static_files(
        &self,
        provider: DatabaseProviderRO<DB>, // Database provider read-only reference
        static_file_provider: StaticFileProvider, // Static file provider
        block_range: RangeInclusive<BlockNumber>, // Range of blocks to process
    ) -> ProviderResult<()> {
        // Get a writer for the static file segment based on the starting block number
        let mut static_file_writer = static_file_provider
            .get_writer(*block_range.start(), StaticFileSegment::Transactions)?;

        // Iterate over each block in the specified range
        for block in block_range {
            // Increment the block number in the static file writer
            let _static_file_block =
                static_file_writer.increment_block(StaticFileSegment::Transactions, block)?;
            debug_assert_eq!(_static_file_block, block);

            // Retrieve transaction indices for the current block
            let block_body_indices = provider
                .block_body_indices(block)?
                .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;

            // Create a cursor to read transactions from the database
            let mut transactions_cursor =
                provider.tx_ref().cursor_read::<tables::Transactions>()?;

            // Walk through transactions within the block's transaction range
            let transactions_walker =
                transactions_cursor.walk_range(block_body_indices.tx_num_range())?;

            // Append each transaction to the static file using the writer
            for entry in transactions_walker {
                let (tx_number, transaction) = entry?;
                static_file_writer.append_transaction(tx_number, transaction)?;
            }
        }

        Ok(())
    }

    /// Create a static file for transaction data based on the block range and configuration provided.
    fn create_static_file_file(
        &self,
        provider: &DatabaseProviderRO<DB>, // Database provider read-only reference
        directory: &Path, // Path to the directory where static file will be saved
        config: SegmentConfig, // Configuration for the static file segment
        block_range: RangeInclusive<BlockNumber>, // Range of blocks to process
    ) -> ProviderResult<()> {
        // Retrieve the transaction range for the specified block range
        let tx_range = provider.transaction_range_by_block_range(block_range.clone())?;
        let tx_range_len = tx_range.clone().count();

        // Prepare a NippyJar for compression and storage
        let jar = prepare_jar::<DB, 1>(
            provider,
            directory,
            StaticFileSegment::Transactions,
            config,
            block_range,
            tx_range_len,
            || {
                Ok([dataset_for_compression::<DB, tables::Transactions>(
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
        create_static_file_T1::<tables::Transactions, TxNumber, SegmentHeader>(
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
