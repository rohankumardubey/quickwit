#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocBatchV2 {
    #[prost(bytes = "bytes", tag = "1")]
    pub doc_buffer: ::prost::bytes::Bytes,
    #[prost(uint32, repeated, tag = "2")]
    pub doc_lengths: ::prost::alloc::vec::Vec<u32>,
}
