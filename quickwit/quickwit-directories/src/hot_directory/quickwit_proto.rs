/// Encodes the position of a slice.
///
/// This message expresses that
/// the file bytes [start..start + len)
/// are stored at the address [addr..addr+len)
#[derive(Debug, Default)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileSliceEntry {
    #[prost(uint64, tag = "1")]
    pub start: u64,
    #[prost(uint64, tag = "2")]
    pub len: u64,
    #[prost(uint64, tag = "3")]
    pub addr: u64,
}
#[derive(Debug, Default)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileEntry {
    #[prost(string, tag = "1")]
    pub filepath: ::prost::alloc::string::String,
    /// Entire file length.
    #[prost(uint64, tag = "2")]
    pub file_length: u64,
    #[prost(message, repeated, tag = "3")]
    pub file_slices: ::prost::alloc::vec::Vec<FileSliceEntry>,
}
#[derive(Debug, Default)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheMeta {
    #[prost(message, repeated, tag = "1")]
    pub file_entries: ::prost::alloc::vec::Vec<FileEntry>,
}
