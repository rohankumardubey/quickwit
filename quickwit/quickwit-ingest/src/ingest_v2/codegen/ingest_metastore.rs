#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsRequest {
    #[prost(message, repeated, tag = "3")]
    pub subrequests: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "4")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsSubresponse {
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
pub struct ListShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<ListShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::shard::ShardState", optional, tag = "3")]
    pub shard_state: ::core::option::Option<i32>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<ListShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubresponse {
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
pub struct RenewShardLeasesRequest {
    #[prost(message, repeated, tag = "2")]
    pub subrequests: ::prost::alloc::vec::Vec<RenewShardLeasesSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewShardLeasesSubrequest {
    #[prost(string, tag = "1")]
    pub lessee_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "4")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewShardLeasesResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<RenewShardLeasesSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewShardLeasesSubresponse {
    #[prost(string, tag = "1")]
    pub lessee_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub lease_renewal_results: ::prost::alloc::vec::Vec<LeaseRenewalResult>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseRenewalResult {
    #[prost(uint64, tag = "1")]
    pub shard_id: u64,
    #[prost(enumeration = "LeaseRenewalOutcome", tag = "2")]
    pub renewal_outcome: i32,
    #[prost(message, optional, tag = "3")]
    pub shard: ::core::option::Option<super::shard::Shard>,
    #[prost(uint64, optional, tag = "4")]
    pub lease_expiration_timestamp_millis: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LeaseRenewalOutcome {
    Acquired = 0,
    Renewed = 1,
}
impl LeaseRenewalOutcome {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LeaseRenewalOutcome::Acquired => "ACQUIRED",
            LeaseRenewalOutcome::Renewed => "RENEWED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ACQUIRED" => Some(Self::Acquired),
            "RENEWED" => Some(Self::Renewed),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LeaseState {
    Available = 0,
    Active = 1,
}
impl LeaseState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LeaseState::Available => "AVAILABLE",
            LeaseState::Active => "ACTIVE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "AVAILABLE" => Some(Self::Available),
            "ACTIVE" => Some(Self::Active),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IngestMetastoreService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse>;
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::ingest_v2::Result<ListShardsResponse>;
    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> crate::ingest_v2::Result<RenewShardLeasesResponse>;
}
dyn_clone::clone_trait_object!(IngestMetastoreService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIngestMetastoreService {
    fn clone(&self) -> Self {
        MockIngestMetastoreService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IngestMetastoreServiceClient {
    inner: Box<dyn IngestMetastoreService>,
}
impl IngestMetastoreServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestMetastoreService,
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
        IngestMetastoreServiceClient::new(
            IngestMetastoreServiceGrpcClientAdapter::new(
                ingest_metastore_service_grpc_client::IngestMetastoreServiceGrpcClient::new(
                    channel,
                ),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngestMetastoreServiceMailbox<A>: IngestMetastoreService,
    {
        IngestMetastoreServiceClient::new(IngestMetastoreServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IngestMetastoreServiceTowerBlockBuilder {
        IngestMetastoreServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIngestMetastoreService {
        MockIngestMetastoreService::new()
    }
}
#[async_trait::async_trait]
impl IngestMetastoreService for IngestMetastoreServiceClient {
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse> {
        self.inner.get_or_create_open_shards(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::ingest_v2::Result<ListShardsResponse> {
        self.inner.list_shards(request).await
    }
    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> crate::ingest_v2::Result<RenewShardLeasesResponse> {
        self.inner.renew_shard_leases(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockIngestMetastoreServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockIngestMetastoreService>>,
    }
    #[async_trait::async_trait]
    impl IngestMetastoreService for MockIngestMetastoreServiceWrapper {
        async fn get_or_create_open_shards(
            &mut self,
            request: GetOrCreateOpenShardsRequest,
        ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse> {
            self.inner.lock().await.get_or_create_open_shards(request).await
        }
        async fn list_shards(
            &mut self,
            request: ListShardsRequest,
        ) -> crate::ingest_v2::Result<ListShardsResponse> {
            self.inner.lock().await.list_shards(request).await
        }
        async fn renew_shard_leases(
            &mut self,
            request: RenewShardLeasesRequest,
        ) -> crate::ingest_v2::Result<RenewShardLeasesResponse> {
            self.inner.lock().await.renew_shard_leases(request).await
        }
    }
    impl From<MockIngestMetastoreService> for IngestMetastoreServiceClient {
        fn from(mock: MockIngestMetastoreService) -> Self {
            let mock_wrapper = MockIngestMetastoreServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            IngestMetastoreServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<GetOrCreateOpenShardsRequest> for Box<dyn IngestMetastoreService> {
    type Response = GetOrCreateOpenShardsResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GetOrCreateOpenShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.get_or_create_open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListShardsRequest> for Box<dyn IngestMetastoreService> {
    type Response = ListShardsResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<RenewShardLeasesRequest> for Box<dyn IngestMetastoreService> {
    type Response = RenewShardLeasesResponse;
    type Error = crate::ingest_v2::IngestErrorV2;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: RenewShardLeasesRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.renew_shard_leases(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IngestMetastoreServiceTowerBlock {
    get_or_create_open_shards_svc: quickwit_common::tower::BoxService<
        GetOrCreateOpenShardsRequest,
        GetOrCreateOpenShardsResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
    list_shards_svc: quickwit_common::tower::BoxService<
        ListShardsRequest,
        ListShardsResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
    renew_shard_leases_svc: quickwit_common::tower::BoxService<
        RenewShardLeasesRequest,
        RenewShardLeasesResponse,
        crate::ingest_v2::IngestErrorV2,
    >,
}
impl Clone for IngestMetastoreServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            get_or_create_open_shards_svc: self.get_or_create_open_shards_svc.clone(),
            list_shards_svc: self.list_shards_svc.clone(),
            renew_shard_leases_svc: self.renew_shard_leases_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IngestMetastoreService for IngestMetastoreServiceTowerBlock {
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse> {
        self.get_or_create_open_shards_svc.ready().await?.call(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::ingest_v2::Result<ListShardsResponse> {
        self.list_shards_svc.ready().await?.call(request).await
    }
    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> crate::ingest_v2::Result<RenewShardLeasesResponse> {
        self.renew_shard_leases_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IngestMetastoreServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    get_or_create_open_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestMetastoreService>,
            GetOrCreateOpenShardsRequest,
            GetOrCreateOpenShardsResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestMetastoreService>,
            ListShardsRequest,
            ListShardsResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
    #[allow(clippy::type_complexity)]
    renew_shard_leases_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestMetastoreService>,
            RenewShardLeasesRequest,
            RenewShardLeasesResponse,
            crate::ingest_v2::IngestErrorV2,
        >,
    >,
}
impl IngestMetastoreServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestMetastoreService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                RenewShardLeasesRequest,
                Response = RenewShardLeasesResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<RenewShardLeasesRequest>>::Future: Send + 'static,
    {
        self
            .get_or_create_open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .renew_shard_leases_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn get_or_create_open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestMetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
    {
        self
            .get_or_create_open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn list_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestMetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
    {
        self.list_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn renew_shard_leases_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestMetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                RenewShardLeasesRequest,
                Response = RenewShardLeasesResponse,
                Error = crate::ingest_v2::IngestErrorV2,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<RenewShardLeasesRequest>>::Future: Send + 'static,
    {
        self
            .renew_shard_leases_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn build<T>(self, instance: T) -> IngestMetastoreServiceClient
    where
        T: IngestMetastoreService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T, C>(self, channel: C) -> IngestMetastoreServiceClient
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
        self.build_from_boxed(
            Box::new(IngestMetastoreServiceClient::from_channel(channel)),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IngestMetastoreServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngestMetastoreServiceMailbox<A>: IngestMetastoreService,
    {
        self.build_from_boxed(
            Box::new(IngestMetastoreServiceClient::from_mailbox(mailbox)),
        )
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IngestMetastoreService>,
    ) -> IngestMetastoreServiceClient {
        let get_or_create_open_shards_svc = if let Some(layer)
            = self.get_or_create_open_shards_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_shards_svc = if let Some(layer) = self.list_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let renew_shard_leases_svc = if let Some(layer) = self.renew_shard_leases_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IngestMetastoreServiceTowerBlock {
            get_or_create_open_shards_svc,
            list_shards_svc,
            renew_shard_leases_svc,
        };
        IngestMetastoreServiceClient::new(tower_block)
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
pub struct IngestMetastoreServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::ingest_v2::IngestErrorV2>,
}
impl<A: quickwit_actors::Actor> IngestMetastoreServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IngestMetastoreServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for IngestMetastoreServiceMailbox<A>
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
impl<A> IngestMetastoreService for IngestMetastoreServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    IngestMetastoreServiceMailbox<
        A,
    >: tower::Service<
            GetOrCreateOpenShardsRequest,
            Response = GetOrCreateOpenShardsResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<
                GetOrCreateOpenShardsResponse,
                crate::ingest_v2::IngestErrorV2,
            >,
        >
        + tower::Service<
            ListShardsRequest,
            Response = ListShardsResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<ListShardsResponse, crate::ingest_v2::IngestErrorV2>,
        >
        + tower::Service<
            RenewShardLeasesRequest,
            Response = RenewShardLeasesResponse,
            Error = crate::ingest_v2::IngestErrorV2,
            Future = BoxFuture<RenewShardLeasesResponse, crate::ingest_v2::IngestErrorV2>,
        >,
{
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse> {
        self.call(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::ingest_v2::Result<ListShardsResponse> {
        self.call(request).await
    }
    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> crate::ingest_v2::Result<RenewShardLeasesResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IngestMetastoreServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> IngestMetastoreServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> IngestMetastoreService
for IngestMetastoreServiceGrpcClientAdapter<
    ingest_metastore_service_grpc_client::IngestMetastoreServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::ingest_v2::Result<GetOrCreateOpenShardsResponse> {
        self.inner
            .get_or_create_open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::ingest_v2::Result<ListShardsResponse> {
        self.inner
            .list_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> crate::ingest_v2::Result<RenewShardLeasesResponse> {
        self.inner
            .renew_shard_leases(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IngestMetastoreServiceGrpcServerAdapter {
    inner: Box<dyn IngestMetastoreService>,
}
impl IngestMetastoreServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestMetastoreService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl ingest_metastore_service_grpc_server::IngestMetastoreServiceGrpc
for IngestMetastoreServiceGrpcServerAdapter {
    async fn get_or_create_open_shards(
        &self,
        request: tonic::Request<GetOrCreateOpenShardsRequest>,
    ) -> Result<tonic::Response<GetOrCreateOpenShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .get_or_create_open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_shards(
        &self,
        request: tonic::Request<ListShardsRequest>,
    ) -> Result<tonic::Response<ListShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn renew_shard_leases(
        &self,
        request: tonic::Request<RenewShardLeasesRequest>,
    ) -> Result<tonic::Response<RenewShardLeasesResponse>, tonic::Status> {
        self.inner
            .clone()
            .renew_shard_leases(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod ingest_metastore_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IngestMetastoreServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngestMetastoreServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> IngestMetastoreServiceGrpcClient<T>
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
        ) -> IngestMetastoreServiceGrpcClient<InterceptedService<T, F>>
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
            IngestMetastoreServiceGrpcClient::new(
                InterceptedService::new(inner, interceptor),
            )
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
        pub async fn get_or_create_open_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOrCreateOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOrCreateOpenShardsResponse>,
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
                "/ingest_metastore.IngestMetastoreService/GetOrCreateOpenShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ingest_metastore.IngestMetastoreService",
                        "GetOrCreateOpenShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
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
                "/ingest_metastore.IngestMetastoreService/ListShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ingest_metastore.IngestMetastoreService",
                        "ListShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn renew_shard_leases(
            &mut self,
            request: impl tonic::IntoRequest<super::RenewShardLeasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RenewShardLeasesResponse>,
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
                "/ingest_metastore.IngestMetastoreService/RenewShardLeases",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ingest_metastore.IngestMetastoreService",
                        "RenewShardLeases",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingest_metastore_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IngestMetastoreServiceGrpcServer.
    #[async_trait]
    pub trait IngestMetastoreServiceGrpc: Send + Sync + 'static {
        async fn get_or_create_open_shards(
            &self,
            request: tonic::Request<super::GetOrCreateOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOrCreateOpenShardsResponse>,
            tonic::Status,
        >;
        async fn list_shards(
            &self,
            request: tonic::Request<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
            tonic::Status,
        >;
        async fn renew_shard_leases(
            &self,
            request: tonic::Request<super::RenewShardLeasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RenewShardLeasesResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct IngestMetastoreServiceGrpcServer<T: IngestMetastoreServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngestMetastoreServiceGrpc> IngestMetastoreServiceGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for IngestMetastoreServiceGrpcServer<T>
    where
        T: IngestMetastoreServiceGrpc,
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
                "/ingest_metastore.IngestMetastoreService/GetOrCreateOpenShards" => {
                    #[allow(non_camel_case_types)]
                    struct GetOrCreateOpenShardsSvc<T: IngestMetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: IngestMetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::GetOrCreateOpenShardsRequest>
                    for GetOrCreateOpenShardsSvc<T> {
                        type Response = super::GetOrCreateOpenShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetOrCreateOpenShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_or_create_open_shards(request).await
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
                        let method = GetOrCreateOpenShardsSvc(inner);
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
                "/ingest_metastore.IngestMetastoreService/ListShards" => {
                    #[allow(non_camel_case_types)]
                    struct ListShardsSvc<T: IngestMetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngestMetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListShardsRequest>
                    for ListShardsSvc<T> {
                        type Response = super::ListShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list_shards(request).await };
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
                        let method = ListShardsSvc(inner);
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
                "/ingest_metastore.IngestMetastoreService/RenewShardLeases" => {
                    #[allow(non_camel_case_types)]
                    struct RenewShardLeasesSvc<T: IngestMetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: IngestMetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::RenewShardLeasesRequest>
                    for RenewShardLeasesSvc<T> {
                        type Response = super::RenewShardLeasesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RenewShardLeasesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).renew_shard_leases(request).await
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
                        let method = RenewShardLeasesSvc(inner);
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
    impl<T: IngestMetastoreServiceGrpc> Clone for IngestMetastoreServiceGrpcServer<T> {
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
    impl<T: IngestMetastoreServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngestMetastoreServiceGrpc> tonic::server::NamedService
    for IngestMetastoreServiceGrpcServer<T> {
        const NAME: &'static str = "ingest_metastore.IngestMetastoreService";
    }
}
