use strum::AsRefStr;

/// Static File filters.
/// Enum representing whether static files use filters or not.
#[derive(Debug, Copy, Clone)]
pub enum Filters {
    /// Static File uses filters with `InclusionFilter` and `PerfectHashingFunction`.
    WithFilters(InclusionFilter, PerfectHashingFunction),
    /// Static File doesn't use any filters.
    WithoutFilters,
}

impl Filters {
    /// Returns `true` if static file uses filters.
    pub const fn has_filters(&self) -> bool {
        matches!(self, Self::WithFilters(_, _))
    }
}

/// Static File inclusion filter. Also see [Filters].
/// Enum representing different types of inclusion filters for static files.
#[derive(Debug, Copy, Clone, AsRefStr)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum InclusionFilter {
    #[strum(serialize = "cuckoo")]
    /// Cuckoo filter
    /// A Cuckoo filter is a probabilistic data structure used for testing set membership, 
    /// with improved efficiency in terms of space utilization and deletion operations.
    Cuckoo,
}

/// Static File perfect hashing function. Also see [Filters].
/// Enum representing different types of perfect hashing functions for static files.
#[derive(Debug, Copy, Clone, AsRefStr)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum PerfectHashingFunction {
    #[strum(serialize = "fmph")]
    /// Fingerprint-Based Minimal Perfect Hash Function (a specialized hashing technique used to achieve minimal perfect hashing for a set of keys or elements)
    /// Fingerprint-Based Minimal Perfect Hash Functions are specialized algorithms designed to minimize 
    /// collisions and optimize memory usage for mapping keys to unique hash values efficiently.
    Fmph,
    #[strum(serialize = "gofmph")]
    /// Fingerprint-Based Minimal Perfect Hash Function with Group Optimization (designed to achieve minimal perfect hashing for a given set of keys or elements)
    GoFmph,
}
