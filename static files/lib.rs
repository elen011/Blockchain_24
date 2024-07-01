//! Static file producer implementation.

#![doc(
    // Sets the URL for the logo displayed in the HTML documentation.
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    // Sets the URL for the favicon displayed in the HTML documentation.
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    // Sets the base URL for the issue tracker in the HTML documentation.
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod event;
pub mod segments;
mod static_file_producer;

// Re-exports the `StaticFileProducerEvent` from the `event` module.
pub use event::StaticFileProducerEvent;

// Re-exports several items from the `static_file_producer` module.
pub use static_file_producer::{
    StaticFileProducer,          // Main struct for producing static files.
    StaticFileProducerInner,     // Internal structure for the producer.
    StaticFileProducerResult,    // Result type for the producer's operations.
    StaticFileProducerWithResult,// Wrapper struct for the producer with result handling.
    StaticFileTargets,           // Configuration for target static files.
};

// Re-export all items from the `reth_static_file_types` crate for convenience.
pub use reth_static_file_types::*;
