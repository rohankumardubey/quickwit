#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<GetOpenShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOpenShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<GetOpenShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOpenShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shards: ::prost::alloc::vec::Vec<super::shard::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub commit_type: u32,
    #[prost(message, repeated, tag = "3")]
    pub subrequests: ::prost::alloc::vec::Vec<PersistSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "4")]
    pub doc_batch: ::core::option::Option<super::ingest::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistResponse {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub successes: ::prost::alloc::vec::Vec<PersistSuccess>,
    #[prost(message, repeated, tag = "3")]
    pub failures: ::prost::alloc::vec::Vec<PersistFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistSuccess {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, optional, tag = "4")]
    pub replication_position_inclusive: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistFailure {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    /// ingest.DocBatchV2 doc_batch = 4;
    /// ingest.IngestError error = 5;
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SynReplicationMessage {
    #[prost(oneof = "syn_replication_message::Message", tags = "1, 2")]
    pub message: ::core::option::Option<syn_replication_message::Message>,
}
/// Nested message and enum types in `SynReplicationMessage`.
pub mod syn_replication_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        OpenRequest(super::OpenReplicationStreamRequest),
        #[prost(message, tag = "2")]
        ReplicateRequest(super::ReplicateRequest),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckReplicationMessage {
    #[prost(oneof = "ack_replication_message::Message", tags = "1, 3")]
    pub message: ::core::option::Option<ack_replication_message::Message>,
}
/// Nested message and enum types in `AckReplicationMessage`.
pub mod ack_replication_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        OpenResponse(super::OpenReplicationStreamResponse),
        #[prost(message, tag = "3")]
        ReplicateResponse(super::ReplicateResponse),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenReplicationStreamRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub follower_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenReplicationStreamResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub follower_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub commit_type: u32,
    #[prost(message, repeated, tag = "4")]
    pub subrequests: ::prost::alloc::vec::Vec<ReplicateSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, optional, tag = "4")]
    pub from_position_exclusive: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "6")]
    pub doc_batch: ::core::option::Option<super::ingest::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateResponse {
    #[prost(string, tag = "1")]
    pub follower_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub successes: ::prost::alloc::vec::Vec<ReplicateSuccess>,
    #[prost(message, repeated, tag = "3")]
    pub failures: ::prost::alloc::vec::Vec<ReplicateFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateSuccess {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, optional, tag = "4")]
    pub replica_position_inclusive: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateFailure {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    /// ingest.DocBatchV2 doc_batch = 4;
    /// ingest.IngestError error = 5;
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<TruncateSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, tag = "4")]
    pub to_position_inclusive: u64,
}
/// TODO
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenFetchStreamRequest {
    #[prost(string, tag = "1")]
    pub client_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub subscriptions: ::prost::alloc::vec::Vec<SubscribeToShard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeToShard {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, optional, tag = "4")]
    pub from_position_exclusive: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub to_position_inclusive: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsubscribeFromShard {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenFetchStreamResponse {
    #[prost(string, tag = "1")]
    pub ingester_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub fetch_stream_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchPayload {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(uint64, tag = "4")]
    pub from_position_inclusive: u64,
    #[prost(message, optional, tag = "5")]
    pub doc_batch: ::core::option::Option<super::ingest::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchMessage {
    #[prost(oneof = "fetch_message::Message", tags = "1, 2")]
    pub message: ::core::option::Option<fetch_message::Message>,
}
/// Nested message and enum types in `FetchMessage`.
pub mod fetch_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        Response(super::OpenFetchStreamResponse),
        #[prost(message, tag = "2")]
        Payload(super::FetchPayload),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateFetchStreamRequest {
    #[prost(string, tag = "1")]
    pub fetch_stream_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub subscriptions: ::prost::alloc::vec::Vec<SubscribeToShard>,
    #[prost(message, repeated, tag = "3")]
    pub unsubscriptions: ::prost::alloc::vec::Vec<UnsubscribeFromShard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateFetchStreamResponse {}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
pub type IngesterServiceStream<T> = quickwit_common::ServiceStream<
    crate::ingest_v2::Result<T>,
>;
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IngesterService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn get_open_shards(
        &mut self,
        request: GetOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOpenShardsResponse>;
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest_v2::Result<PersistResponse>;
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>>;
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>>;
    async fn update_fetch_stream(
        &mut self,
        request: UpdateFetchStreamRequest,
    ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse>;
    async fn truncate(
        &mut self,
        request: TruncateRequest,
    ) -> crate::ingest_v2::Result<TruncateResponse>;
}
dyn_clone::clone_trait_object!(IngesterService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIngesterService {
    fn clone(&self) -> Self {
        MockIngesterService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IngesterServiceClient {
    inner: Box<dyn IngesterService>,
}
impl IngesterServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngesterService,
    {
        Self { inner: Box::new(instance) }
    }
    pub fn from_channel<C>(channel: C) -> Self
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        IngesterServiceClient::new(
            IngesterServiceGrpcClientAdapter::new(
                ingester_service_grpc_client::IngesterServiceGrpcClient::new(channel),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngesterServiceMailbox<A>: IngesterService,
    {
        IngesterServiceClient::new(IngesterServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IngesterServiceTowerBlockBuilder {
        IngesterServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIngesterService {
        MockIngesterService::new()
    }
}
#[async_trait::async_trait]
impl IngesterService for IngesterServiceClient {
    async fn get_open_shards(
        &mut self,
        request: GetOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOpenShardsResponse> {
        self.inner.get_open_shards(request).await
    }
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest_v2::Result<PersistResponse> {
        self.inner.persist(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>> {
        self.inner.open_replication_stream(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>> {
        self.inner.open_fetch_stream(request).await
    }
    async fn update_fetch_stream(
        &mut self,
        request: UpdateFetchStreamRequest,
    ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse> {
        self.inner.update_fetch_stream(request).await
    }
    async fn truncate(
        &mut self,
        request: TruncateRequest,
    ) -> crate::ingest_v2::Result<TruncateResponse> {
        self.inner.truncate(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockIngesterServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockIngesterService>>,
    }
    #[async_trait::async_trait]
    impl IngesterService for MockIngesterServiceWrapper {
        async fn get_open_shards(
            &mut self,
            request: GetOpenShardsRequest,
        ) -> crate::ingest_v2::Result<GetOpenShardsResponse> {
            self.inner.lock().await.get_open_shards(request).await
        }
        async fn persist(
            &mut self,
            request: PersistRequest,
        ) -> crate::ingest_v2::Result<PersistResponse> {
            self.inner.lock().await.persist(request).await
        }
        async fn open_replication_stream(
            &mut self,
            request: quickwit_common::ServiceStream<SynReplicationMessage>,
        ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>> {
            self.inner.lock().await.open_replication_stream(request).await
        }
        async fn open_fetch_stream(
            &mut self,
            request: OpenFetchStreamRequest,
        ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>> {
            self.inner.lock().await.open_fetch_stream(request).await
        }
        async fn update_fetch_stream(
            &mut self,
            request: UpdateFetchStreamRequest,
        ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse> {
            self.inner.lock().await.update_fetch_stream(request).await
        }
        async fn truncate(
            &mut self,
            request: TruncateRequest,
        ) -> crate::ingest_v2::Result<TruncateResponse> {
            self.inner.lock().await.truncate(request).await
        }
    }
    impl From<MockIngesterService> for IngesterServiceClient {
        fn from(mock: MockIngesterService) -> Self {
            let mock_wrapper = MockIngesterServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            IngesterServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<GetOpenShardsRequest> for Box<dyn IngesterService> {
    type Response = GetOpenShardsResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GetOpenShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.get_open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<PersistRequest> for Box<dyn IngesterService> {
    type Response = PersistResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PersistRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.persist(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<quickwit_common::ServiceStream<SynReplicationMessage>>
for Box<dyn IngesterService> {
    type Response = IngesterServiceStream<AckReplicationMessage>;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_replication_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<OpenFetchStreamRequest> for Box<dyn IngesterService> {
    type Response = IngesterServiceStream<FetchMessage>;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: OpenFetchStreamRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_fetch_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<UpdateFetchStreamRequest> for Box<dyn IngesterService> {
    type Response = UpdateFetchStreamResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: UpdateFetchStreamRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.update_fetch_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<TruncateRequest> for Box<dyn IngesterService> {
    type Response = TruncateResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: TruncateRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.truncate(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IngesterServiceTowerBlock {
    get_open_shards_svc: quickwit_common::tower::BoxService<
        GetOpenShardsRequest,
        GetOpenShardsResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
    persist_svc: quickwit_common::tower::BoxService<
        PersistRequest,
        PersistResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
    open_replication_stream_svc: quickwit_common::tower::BoxService<
        quickwit_common::ServiceStream<SynReplicationMessage>,
        IngesterServiceStream<AckReplicationMessage>,
        crate::ingest_v2::IngestErrorV2,
    >,
    open_fetch_stream_svc: quickwit_common::tower::BoxService<
        OpenFetchStreamRequest,
        IngesterServiceStream<FetchMessage>,
        crate::ingest_v2::IngestErrorV2,
    >,
    update_fetch_stream_svc: quickwit_common::tower::BoxService<
        UpdateFetchStreamRequest,
        UpdateFetchStreamResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
    truncate_svc: quickwit_common::tower::BoxService<
        TruncateRequest,
        TruncateResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
}
impl Clone for IngesterServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            get_open_shards_svc: self.get_open_shards_svc.clone(),
            persist_svc: self.persist_svc.clone(),
            open_replication_stream_svc: self.open_replication_stream_svc.clone(),
            open_fetch_stream_svc: self.open_fetch_stream_svc.clone(),
            update_fetch_stream_svc: self.update_fetch_stream_svc.clone(),
            truncate_svc: self.truncate_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IngesterService for IngesterServiceTowerBlock {
    async fn get_open_shards(
        &mut self,
        request: GetOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOpenShardsResponse> {
        self.get_open_shards_svc.ready().await?.call(request).await
    }
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest_v2::Result<PersistResponse> {
        self.persist_svc.ready().await?.call(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>> {
        self.open_replication_stream_svc.ready().await?.call(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>> {
        self.open_fetch_stream_svc.ready().await?.call(request).await
    }
    async fn update_fetch_stream(
        &mut self,
        request: UpdateFetchStreamRequest,
    ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse> {
        self.update_fetch_stream_svc.ready().await?.call(request).await
    }
    async fn truncate(
        &mut self,
        request: TruncateRequest,
    ) -> crate::ingest_v2::Result<TruncateResponse> {
        self.truncate_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IngesterServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    get_open_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            GetOpenShardsRequest,
            GetOpenShardsResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    persist_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            PersistRequest,
            PersistResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_replication_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            quickwit_common::ServiceStream<SynReplicationMessage>,
            IngesterServiceStream<AckReplicationMessage>,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_fetch_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            OpenFetchStreamRequest,
            IngesterServiceStream<FetchMessage>,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    update_fetch_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            UpdateFetchStreamRequest,
            UpdateFetchStreamResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    truncate_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            TruncateRequest,
            TruncateResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
}
impl IngesterServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOpenShardsRequest,
                Response = GetOpenShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<GetOpenShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                PersistRequest,
                Response = PersistResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PersistRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                quickwit_common::ServiceStream<SynReplicationMessage>,
                Response = IngesterServiceStream<AckReplicationMessage>,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                OpenFetchStreamRequest,
                Response = IngesterServiceStream<FetchMessage>,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenFetchStreamRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                UpdateFetchStreamRequest,
                Response = UpdateFetchStreamResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<UpdateFetchStreamRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                TruncateRequest,
                Response = TruncateResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<TruncateRequest>>::Future: Send + 'static,
    {
        self
            .get_open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self.persist_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
            .open_replication_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .open_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .update_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self.truncate_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn get_open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOpenShardsRequest,
                Response = GetOpenShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<GetOpenShardsRequest>>::Future: Send + 'static,
    {
        self.get_open_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn persist_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                PersistRequest,
                Response = PersistResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PersistRequest>>::Future: Send + 'static,
    {
        self.persist_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn open_replication_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                quickwit_common::ServiceStream<SynReplicationMessage>,
                Response = IngesterServiceStream<AckReplicationMessage>,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
        >>::Future: Send + 'static,
    {
        self
            .open_replication_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn open_fetch_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                OpenFetchStreamRequest,
                Response = IngesterServiceStream<FetchMessage>,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenFetchStreamRequest>>::Future: Send + 'static,
    {
        self
            .open_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn update_fetch_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                UpdateFetchStreamRequest,
                Response = UpdateFetchStreamResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<UpdateFetchStreamRequest>>::Future: Send + 'static,
    {
        self
            .update_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn truncate_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                TruncateRequest,
                Response = TruncateResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<TruncateRequest>>::Future: Send + 'static,
    {
        self.truncate_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> IngesterServiceClient
    where
        T: IngesterService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T, C>(self, channel: C) -> IngesterServiceClient
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        self.build_from_boxed(Box::new(IngesterServiceClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IngesterServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngesterServiceMailbox<A>: IngesterService,
    {
        self.build_from_boxed(Box::new(IngesterServiceClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IngesterService>,
    ) -> IngesterServiceClient {
        let get_open_shards_svc = if let Some(layer) = self.get_open_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let persist_svc = if let Some(layer) = self.persist_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_replication_stream_svc = if let Some(layer)
            = self.open_replication_stream_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_fetch_stream_svc = if let Some(layer) = self.open_fetch_stream_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let update_fetch_stream_svc = if let Some(layer) = self.update_fetch_stream_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let truncate_svc = if let Some(layer) = self.truncate_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IngesterServiceTowerBlock {
            get_open_shards_svc,
            persist_svc,
            open_replication_stream_svc,
            open_fetch_stream_svc,
            update_fetch_stream_svc,
            truncate_svc,
        };
        IngesterServiceClient::new(tower_block)
    }
}
#[derive(Debug, Clone)]
struct MailboxAdapter<A: quickwit_actors::Actor, E> {
    inner: quickwit_actors::Mailbox<A>,
    phantom: std::marker::PhantomData<E>,
}
impl<A, E> std::ops::Deref for MailboxAdapter<A, E>
where
    A: quickwit_actors::Actor,
{
    type Target = quickwit_actors::Mailbox<A>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug)]
pub struct IngesterServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::ingest_v2::IngestErrorV2>,
}
impl<A: quickwit_actors::Actor> IngesterServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IngesterServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for IngesterServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::ingest_v2::IngestErrorV2: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
        //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
        //! mailbox in `poll_ready` and consume it in `call`.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, message: M) -> Self::Future {
        let mailbox = self.inner.clone();
        let fut = async move {
            mailbox.ask_for_res(message).await.map_err(|error| error.into())
        };
        Box::pin(fut)
    }
}
#[async_trait::async_trait]
impl<A> IngesterService for IngesterServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    IngesterServiceMailbox<
        A,
    >: tower::Service<
            GetOpenShardsRequest,
            Response = GetOpenShardsResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<GetOpenShardsResponse, crate::ingest_v2::IngestErrorV2>,
        >
        + tower::Service<
            PersistRequest,
            Response = PersistResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<PersistResponse, crate::ingest_v2::IngestErrorV2>,
        >
        + tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
            Response = IngesterServiceStream<AckReplicationMessage>,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<
                IngesterServiceStream<AckReplicationMessage>,
                crate::ingest_v2::IngestErrorV2,
            >,
        >
        + tower::Service<
            OpenFetchStreamRequest,
            Response = IngesterServiceStream<FetchMessage>,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<
                IngesterServiceStream<FetchMessage>,
                crate::ingest_v2::IngestErrorV2,
            >,
        >
        + tower::Service<
            UpdateFetchStreamRequest,
            Response = UpdateFetchStreamResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<
                UpdateFetchStreamResponse,
                crate::ingest_v2::IngestErrorV2,
            >,
        >
        + tower::Service<
            TruncateRequest,
            Response = TruncateResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<TruncateResponse, crate::ingest_v2::IngestErrorV2>,
        >,
{
    async fn get_open_shards(
        &mut self,
        request: GetOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOpenShardsResponse> {
        self.call(request).await
    }
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest_v2::Result<PersistResponse> {
        self.call(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>> {
        self.call(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>> {
        self.call(request).await
    }
    async fn update_fetch_stream(
        &mut self,
        request: UpdateFetchStreamRequest,
    ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse> {
        self.call(request).await
    }
    async fn truncate(
        &mut self,
        request: TruncateRequest,
    ) -> crate::ingest_v2::Result<TruncateResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IngesterServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> IngesterServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> IngesterService
for IngesterServiceGrpcClientAdapter<
    ingester_service_grpc_client::IngesterServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn get_open_shards(
        &mut self,
        request: GetOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOpenShardsResponse> {
        self.inner
            .get_open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest_v2::Result<PersistResponse> {
        self.inner
            .persist(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<AckReplicationMessage>> {
        self.inner
            .open_replication_stream(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream.map_err(|error| error.into())
            })
            .map_err(|error| error.into())
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest_v2::Result<IngesterServiceStream<FetchMessage>> {
        self.inner
            .open_fetch_stream(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream.map_err(|error| error.into())
            })
            .map_err(|error| error.into())
    }
    async fn update_fetch_stream(
        &mut self,
        request: UpdateFetchStreamRequest,
    ) -> crate::ingest_v2::Result<UpdateFetchStreamResponse> {
        self.inner
            .update_fetch_stream(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn truncate(
        &mut self,
        request: TruncateRequest,
    ) -> crate::ingest_v2::Result<TruncateResponse> {
        self.inner
            .truncate(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IngesterServiceGrpcServerAdapter {
    inner: Box<dyn IngesterService>,
}
impl IngesterServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngesterService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl ingester_service_grpc_server::IngesterServiceGrpc
for IngesterServiceGrpcServerAdapter {
    async fn get_open_shards(
        &self,
        request: tonic::Request<GetOpenShardsRequest>,
    ) -> Result<tonic::Response<GetOpenShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .get_open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn persist(
        &self,
        request: tonic::Request<PersistRequest>,
    ) -> Result<tonic::Response<PersistResponse>, tonic::Status> {
        self.inner
            .clone()
            .persist(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    type OpenReplicationStreamStream = quickwit_common::ServiceStream<
        tonic::Result<AckReplicationMessage>,
    >;
    async fn open_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<SynReplicationMessage>>,
    ) -> Result<tonic::Response<Self::OpenReplicationStreamStream>, tonic::Status> {
        self.inner
            .clone()
            .open_replication_stream({
                let streaming: tonic::Streaming<_> = request.into_inner();
                quickwit_common::ServiceStream::from(streaming)
            })
            .await
            .map(|stream| tonic::Response::new(stream.map_err(|error| error.into())))
            .map_err(|error| error.into())
    }
    type OpenFetchStreamStream = quickwit_common::ServiceStream<
        tonic::Result<FetchMessage>,
    >;
    async fn open_fetch_stream(
        &self,
        request: tonic::Request<OpenFetchStreamRequest>,
    ) -> Result<tonic::Response<Self::OpenFetchStreamStream>, tonic::Status> {
        self.inner
            .clone()
            .open_fetch_stream(request.into_inner())
            .await
            .map(|stream| tonic::Response::new(stream.map_err(|error| error.into())))
            .map_err(|error| error.into())
    }
    async fn update_fetch_stream(
        &self,
        request: tonic::Request<UpdateFetchStreamRequest>,
    ) -> Result<tonic::Response<UpdateFetchStreamResponse>, tonic::Status> {
        self.inner
            .clone()
            .update_fetch_stream(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn truncate(
        &self,
        request: tonic::Request<TruncateRequest>,
    ) -> Result<tonic::Response<TruncateResponse>, tonic::Status> {
        self.inner
            .clone()
            .truncate(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod ingester_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IngesterServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngesterServiceGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> IngesterServiceGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> IngesterServiceGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            IngesterServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// / Returns the list of open shards for one or multiple indexes.
        pub async fn get_open_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOpenShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/GetOpenShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ingester.IngesterService", "GetOpenShards"));
            self.inner.unary(req, path, codec).await
        }
        /// / Persists batches of documents to primary shards owned by a leader.
        pub async fn persist(
            &mut self,
            request: impl tonic::IntoRequest<super::PersistRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PersistResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/Persist",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ingester.IngesterService", "Persist"));
            self.inner.unary(req, path, codec).await
        }
        /// / Opens a replication stream from a leader to a follower.
        pub async fn open_replication_stream(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::SynReplicationMessage,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::AckReplicationMessage>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/OpenReplicationStream",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ingester.IngesterService", "OpenReplicationStream"),
                );
            self.inner.streaming(req, path, codec).await
        }
        /// / Opens a new stream of documents stored in one or multiple shards. The first message of the stream returns the ID of the newly created stream,
        /// / which can be used later to update the stream with new shards or to close it.
        pub async fn open_fetch_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::FetchMessage>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/OpenFetchStream",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ingester.IngesterService", "OpenFetchStream"));
            self.inner.server_streaming(req, path, codec).await
        }
        /// / Updates a fetch stream.
        pub async fn update_fetch_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateFetchStreamResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/UpdateFetchStream",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ingester.IngesterService", "UpdateFetchStream"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// / Truncates the shards at the given positions. Callers should this RPC on leaders and they will forward to followers.
        pub async fn truncate(
            &mut self,
            request: impl tonic::IntoRequest<super::TruncateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TruncateResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ingester.IngesterService/Truncate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ingester.IngesterService", "Truncate"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingester_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IngesterServiceGrpcServer.
    #[async_trait]
    pub trait IngesterServiceGrpc: Send + Sync + 'static {
        /// / Returns the list of open shards for one or multiple indexes.
        async fn get_open_shards(
            &self,
            request: tonic::Request<super::GetOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOpenShardsResponse>,
            tonic::Status,
        >;
        /// / Persists batches of documents to primary shards owned by a leader.
        async fn persist(
            &self,
            request: tonic::Request<super::PersistRequest>,
        ) -> std::result::Result<tonic::Response<super::PersistResponse>, tonic::Status>;
        /// Server streaming response type for the OpenReplicationStream method.
        type OpenReplicationStreamStream: futures_core::Stream<
                Item = std::result::Result<super::AckReplicationMessage, tonic::Status>,
            >
            + Send
            + 'static;
        /// / Opens a replication stream from a leader to a follower.
        async fn open_replication_stream(
            &self,
            request: tonic::Request<tonic::Streaming<super::SynReplicationMessage>>,
        ) -> std::result::Result<
            tonic::Response<Self::OpenReplicationStreamStream>,
            tonic::Status,
        >;
        /// Server streaming response type for the OpenFetchStream method.
        type OpenFetchStreamStream: futures_core::Stream<
                Item = std::result::Result<super::FetchMessage, tonic::Status>,
            >
            + Send
            + 'static;
        /// / Opens a new stream of documents stored in one or multiple shards. The first message of the stream returns the ID of the newly created stream,
        /// / which can be used later to update the stream with new shards or to close it.
        async fn open_fetch_stream(
            &self,
            request: tonic::Request<super::OpenFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::OpenFetchStreamStream>,
            tonic::Status,
        >;
        /// / Updates a fetch stream.
        async fn update_fetch_stream(
            &self,
            request: tonic::Request<super::UpdateFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateFetchStreamResponse>,
            tonic::Status,
        >;
        /// / Truncates the shards at the given positions. Callers should this RPC on leaders and they will forward to followers.
        async fn truncate(
            &self,
            request: tonic::Request<super::TruncateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TruncateResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct IngesterServiceGrpcServer<T: IngesterServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngesterServiceGrpc> IngesterServiceGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IngesterServiceGrpcServer<T>
    where
        T: IngesterServiceGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/ingester.IngesterService/GetOpenShards" => {
                    #[allow(non_camel_case_types)]
                    struct GetOpenShardsSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::GetOpenShardsRequest>
                    for GetOpenShardsSvc<T> {
                        type Response = super::GetOpenShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetOpenShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_open_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetOpenShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ingester.IngesterService/Persist" => {
                    #[allow(non_camel_case_types)]
                    struct PersistSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::PersistRequest>
                    for PersistSvc<T> {
                        type Response = super::PersistResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PersistRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).persist(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PersistSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ingester.IngesterService/OpenReplicationStream" => {
                    #[allow(non_camel_case_types)]
                    struct OpenReplicationStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::StreamingService<super::SynReplicationMessage>
                    for OpenReplicationStreamSvc<T> {
                        type Response = super::AckReplicationMessage;
                        type ResponseStream = T::OpenReplicationStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::SynReplicationMessage>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).open_replication_stream(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = OpenReplicationStreamSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ingester.IngesterService/OpenFetchStream" => {
                    #[allow(non_camel_case_types)]
                    struct OpenFetchStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::ServerStreamingService<
                        super::OpenFetchStreamRequest,
                    > for OpenFetchStreamSvc<T> {
                        type Response = super::FetchMessage;
                        type ResponseStream = T::OpenFetchStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenFetchStreamRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).open_fetch_stream(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = OpenFetchStreamSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ingester.IngesterService/UpdateFetchStream" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateFetchStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::UpdateFetchStreamRequest>
                    for UpdateFetchStreamSvc<T> {
                        type Response = super::UpdateFetchStreamResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateFetchStreamRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).update_fetch_stream(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateFetchStreamSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ingester.IngesterService/Truncate" => {
                    #[allow(non_camel_case_types)]
                    struct TruncateSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::TruncateRequest>
                    for TruncateSvc<T> {
                        type Response = super::TruncateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TruncateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).truncate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TruncateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: IngesterServiceGrpc> Clone for IngesterServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: IngesterServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngesterServiceGrpc> tonic::server::NamedService
    for IngesterServiceGrpcServer<T> {
        const NAME: &'static str = "ingester.IngesterService";
    }
}
