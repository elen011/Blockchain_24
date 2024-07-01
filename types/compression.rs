use strum::AsRefStr;

/// Static File compression types.
/// Defines the different types of compression that can be applied to static files.
#[derive(Debug, Copy, Clone, Default, AsRefStr)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum Compression {
    /// LZ4 compression algorithm.
    /// LZ4 is a lossless data compression algorithm that is focused on compression and decompression speed. 
    /// It belongs to the LZ77 family of byte-oriented compression schemes. 
    /// ITis a fast compression algorithm that offers a good balance between speed and compression ratio.
    /// LZ4 only uses a dictionary-matching stage 
    #[strum(serialize = "lz4")]
    Lz4,
    /// Zstandard (Zstd) compression algorithm.
    /// Zstandard is a lossless data compression algorithm
    /// Known for high compression ratios and fast decompression speeds.
    #[strum(serialize = "zstd")]
    Zstd,
    /// Zstandard (Zstd) compression algorithm with a dictionary.
    /// Zstd with dictionary is an enhanced compression method using a predefined dictionary for better compression performance
    /// When utilizing a dictionary, Zstd can effectively compress data by referencing pre-sampled data patterns contained within the dictionary
    #[strum(serialize = "zstd-dict")]
    ZstdWithDictionary,
    /// No compression.
    /// Indicates that the static file is not compressed.
    #[strum(serialize = "uncompressed")]
    #[default]
    Uncompressed,
}
