// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use quickwit_common::tower::Pool;
use quickwit_types::NodeId;
use siphasher::sip::SipHasher;
use tokio::sync::RwLock;

use super::error::IngestErrorV2;
use super::routing::RoutingTable;
use super::{
    GetOpenShardsRequest, IngestRequestV2, IngestResponseV2, IngestRouterService, IngesterService,
    IngesterServiceClient, PersistRequest, PersistSubrequest,
};
use crate::ingest_v2::DocBatchBuilderV2;
use crate::{GetOpenShardsSubrequest, IngesterPool};

type LeaderId = String;

#[derive(Clone)]
pub struct IngestRouter {
    node_id: NodeId,
    ingester_pool: IngesterPool,
    replication_factor: usize,
    inner: Arc<RwLock<InnerRouter>>,
}

struct InnerRouter {
    preferred_ingester: Option<IngesterServiceClient>, // TODO: Remove this logic.
    routing_table: RoutingTable,
}

impl IngestRouter {
    pub fn new(
        node_id: NodeId,
        ingester_pool: Pool<NodeId, IngesterServiceClient>,
        replication_factor: usize,
    ) -> Self {
        let inner = InnerRouter {
            preferred_ingester: None,
            routing_table: RoutingTable::default(),
        };
        Self {
            node_id,
            ingester_pool,
            replication_factor,
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn ingest_v2(
        &mut self,
        ingest_request: IngestRequestV2,
    ) -> super::Result<IngestResponseV2> {
        self.refresh_routing_table(&ingest_request).await?;

        // TODO: Remove partitioning logic from the router and replace with round-robin.

        let mut doc_batch_builders: Vec<DocBatchBuilderV2> = Vec::new();
        let mut persist_subrequests: HashMap<&LeaderId, Vec<PersistSubrequest>> = HashMap::new();
        let inner_guard = self.inner.read().await;

        // TODO: Here would be the most optimal place to split the body of the HTTP request into
        // lines, validate, transform and then pack the docs into compressed batches routed
        // to the right shards.
        for ingest_subrequest in ingest_request.subrequests {
            let routing_entry = inner_guard
                .routing_table
                .find_entry(&ingest_subrequest.index_uid, &ingest_subrequest.source_id)
                .expect("TODO");

            if routing_entry.len() == 1 {
                let shard = &routing_entry.shards()[0];
                let persist_subrequest = PersistSubrequest {
                    index_uid: ingest_subrequest.index_uid,
                    source_id: ingest_subrequest.source_id,
                    shard_id: shard.shard_id.clone(),
                    doc_batch: ingest_subrequest.doc_batch,
                };
                persist_subrequests
                    .entry(&shard.leader_id)
                    .or_default()
                    .push(persist_subrequest);
                continue;
            }
            doc_batch_builders.resize_with(routing_entry.len(), DocBatchBuilderV2::default);

            for doc in ingest_subrequest.docs() {
                let mut hasher = SipHasher::default(); // TODO: Use a better hash function?
                doc.hash(&mut hasher);
                let hash_key = hasher.finish();
                let shard_idx = routing_entry.route_doc(hash_key);
                doc_batch_builders[shard_idx].add_doc(&doc[..]);
            }
            for (shard, doc_batch_builder) in routing_entry
                .shards()
                .iter()
                .zip(doc_batch_builders.drain(..))
            {
                if !doc_batch_builder.is_empty() {
                    let doc_batch = doc_batch_builder.build();
                    let persist_subrequest = PersistSubrequest {
                        index_uid: ingest_subrequest.index_uid.clone(),
                        source_id: ingest_subrequest.source_id.clone(),
                        shard_id: shard.shard_id.clone(),
                        doc_batch: Some(doc_batch),
                    };
                    persist_subrequests
                        .entry(&shard.leader_id)
                        .or_default()
                        .push(persist_subrequest);
                }
            }
        }
        // drop(inner_guard);
        let mut futures = FuturesUnordered::new();

        for (leader_id, subrequests) in persist_subrequests {
            let leader_id: NodeId = leader_id.clone().into();
            let mut ingester = self.ingester_pool.get(&leader_id).await.expect("TODO");
            let persist_request = PersistRequest {
                leader_id: leader_id.into(),
                subrequests,
                commit_type: ingest_request.commit_type,
            };
            let future = async move { ingester.persist(persist_request).await };
            futures.push(future);
        }
        while let Some(future_res) = futures.next().await {
            // TODO: Handle errors.
            future_res?;
        }
        Ok(IngestResponseV2 {
            successes: Vec::new(), // TODO
            failures: Vec::new(),
        })
    }

    async fn preferred_ingester(&mut self) -> super::Result<IngesterServiceClient> {
        let inner_guard = self.inner.read().await;
        if let Some(ingester) = inner_guard.preferred_ingester.clone() {
            return Ok(ingester);
        }
        drop(inner_guard);
        let mut inner_guard = self.inner.write().await;

        // Select local ingester.
        if let Some(ingester) = self.ingester_pool.get(&self.node_id).await {
            inner_guard.preferred_ingester = Some(ingester.clone());
            return Ok(ingester);
        }
        // TODO: Or, select ingester with highest affinity.
        if let Some(ingester) = self.ingester_pool.any().await {
            inner_guard.preferred_ingester = Some(ingester.clone());
            return Ok(ingester);
        }
        let num_ingesters = self.ingester_pool.len().await;
        let replication_factor = self.replication_factor;

        Err(IngestErrorV2::ServiceUnavailable {
            num_ingesters,
            replication_factor,
        })
    }

    async fn refresh_routing_table(
        &mut self,
        ingest_request: &IngestRequestV2,
    ) -> super::Result<()> {
        let inner_guard = self.inner.read().await;

        let routing_table = &inner_guard.routing_table;
        let mut subrequests = Vec::new();

        for ingest_subrequest in &ingest_request.subrequests {
            if !routing_table
                .contains_entry(&ingest_subrequest.index_uid, &ingest_subrequest.source_id)
            {
                let subrequest = GetOpenShardsSubrequest {
                    index_uid: ingest_subrequest.index_uid.clone(),
                    source_id: ingest_subrequest.source_id.clone(),
                };
                subrequests.push(subrequest);
            }
        }
        if subrequests.is_empty() {
            return Ok(());
        }
        drop(inner_guard);
        let request = GetOpenShardsRequest { subrequests };
        let mut ingester = self.preferred_ingester().await?;
        let response = ingester.get_open_shards(request).await?;
        let mut inner_guard = self.inner.write().await;
        for subresponse in response.subresponses {
            inner_guard.routing_table.insert_shards(
                subresponse.index_uid,
                subresponse.source_id,
                subresponse.shards,
            );
        }
        Ok(())
    }
}

#[async_trait]
impl IngestRouterService for IngestRouter {
    async fn ingest(&mut self, request: IngestRequestV2) -> super::Result<IngestResponseV2> {
        self.ingest_v2(request).await
    }
}

impl fmt::Debug for IngestRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestRouter").finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        CommitType, GetOpenShardsResponse, GetOpenShardsSubresponse, IngestSubrequest,
        PersistResponse, Shard,
    };

    impl IngestRouter {
        async fn set_preferred_ingester(&mut self, ingester: IngesterServiceClient) {
            let mut inner_guard = self.inner.write().await;
            inner_guard.preferred_ingester = Some(ingester);
        }

        async fn reset_preferred_ingester(&mut self) {
            let mut inner_guard = self.inner.write().await;
            inner_guard.preferred_ingester = None;
        }
    }

    #[tokio::test]
    async fn test_router_refresh_routing_table() {
        let node_id = "test-router".into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(node_id, ingester_pool.clone(), replication_factor);

        let ingest_request = IngestRequestV2 {
            commit_type: CommitType::Auto as u32,
            subrequests: Vec::new(),
        };
        router.refresh_routing_table(&ingest_request).await.unwrap();
        assert!(router.inner.read().await.routing_table.is_empty());

        let mut ingester_mock = IngesterServiceClient::mock();
        ingester_mock
            .expect_get_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOpenShardsResponse {
                    subresponses: vec![GetOpenShardsSubresponse {
                        index_uid: "test-index:0".to_string(),
                        source_id: "test-source".to_string(),
                        shards: vec![Shard {
                            shard_id: 0,
                            end_hash_key_exclusive: u64::MAX,
                            ..Default::default()
                        }],
                    }],
                };
                Ok(response)
            });
        let ingester: IngesterServiceClient = ingester_mock.into();
        ingester_pool
            .insert("test-ingester".into(), ingester.clone())
            .await;

        let ingest_request = IngestRequestV2 {
            commit_type: CommitType::Auto as u32,
            subrequests: vec![IngestSubrequest {
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            }],
        };
        router.refresh_routing_table(&ingest_request).await.unwrap();

        let inner_router_guard = router.inner.read().await;

        let routing_entry_0 = inner_router_guard
            .routing_table
            .find_entry("test-index:0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.shards()[0].shard_id, 0);
        drop(inner_router_guard);

        let mut ingester_mock = IngesterServiceClient::mock();
        ingester_mock
            .expect_get_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:1");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOpenShardsResponse {
                    subresponses: vec![GetOpenShardsSubresponse {
                        index_uid: "test-index:1".to_string(),
                        source_id: "test-source".to_string(),
                        shards: vec![
                            Shard {
                                shard_id: 0,
                                end_hash_key_exclusive: u64::MAX / 2,
                                ..Default::default()
                            },
                            Shard {
                                shard_id: 1,
                                start_hash_key_inclusive: u64::MAX / 2,
                                end_hash_key_exclusive: u64::MAX,
                                ..Default::default()
                            },
                        ],
                    }],
                };
                Ok(response)
            });
        let ingester: IngesterServiceClient = ingester_mock.into();
        ingester_pool
            .insert("test-ingester".into(), ingester.clone())
            .await;

        let ingest_request = IngestRequestV2 {
            commit_type: CommitType::Auto as u32,
            subrequests: vec![
                IngestSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
                IngestSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
            ],
        };

        router.reset_preferred_ingester().await;
        router.refresh_routing_table(&ingest_request).await.unwrap();

        let inner_router_guard = router.inner.read().await;

        let routing_entry_0 = inner_router_guard
            .routing_table
            .find_entry("test-index:0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.shards()[0].shard_id, 0);

        let routing_entry_1 = inner_router_guard
            .routing_table
            .find_entry("test-index:1", "test-source")
            .unwrap();
        assert_eq!(routing_entry_1.len(), 2);
        assert_eq!(routing_entry_1.shards()[0].shard_id, 0);
        assert_eq!(routing_entry_1.shards()[1].shard_id, 1);
    }

    #[tokio::test]
    async fn test_router_ingest() {
        let node_id = "test-router".into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(node_id, ingester_pool.clone(), replication_factor);

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_get_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 2);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_uid, "test-index:1");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOpenShardsResponse {
                    subresponses: vec![
                        GetOpenShardsSubresponse {
                            index_uid: "test-index:0".to_string(),
                            source_id: "test-source".to_string(),
                            shards: vec![Shard {
                                shard_id: 0,
                                leader_id: "test-ingester-0".to_string(),
                                end_hash_key_exclusive: u64::MAX,
                                ..Default::default()
                            }],
                        },
                        GetOpenShardsSubresponse {
                            index_uid: "test-index:1".to_string(),
                            source_id: "test-source".to_string(),
                            shards: vec![
                                Shard {
                                    shard_id: 0,
                                    leader_id: "test-ingester-0".to_string(),
                                    end_hash_key_exclusive: u64::MAX / 2,
                                    ..Default::default()
                                },
                                Shard {
                                    shard_id: 1,
                                    leader_id: "test-ingester-1".to_string(),
                                    start_hash_key_inclusive: u64::MAX / 2,
                                    end_hash_key_exclusive: u64::MAX,
                                    ..Default::default()
                                },
                            ],
                        },
                    ],
                };
                Ok(response)
            });
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.commit_type, CommitType::Auto as u32);
                assert_eq!(request.subrequests.len(), 2);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 0);
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-foo"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [12]);

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_uid, "test-index:1");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 0);
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-qux??"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [14]);

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
        ingester_pool
            .insert("test-ingester-0".into(), ingester_0.clone())
            .await;

        let mut ingester_mock_1 = IngesterServiceClient::mock();
        ingester_mock_1
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-1");
                assert_eq!(request.commit_type, CommitType::Auto as u32);
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:1");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-bar!"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [13]);

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_1: IngesterServiceClient = ingester_mock_1.into();
        ingester_pool
            .insert("test-ingester-1".into(), ingester_1)
            .await;

        let ingest_request = IngestRequestV2 {
            commit_type: CommitType::Auto as u32,
            subrequests: vec![
                IngestSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(crate::DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-foo"),
                        doc_lengths: vec![12],
                    }),
                },
                IngestSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(crate::DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-bar!test-doc-qux??"),
                        doc_lengths: vec![13, 14],
                    }),
                },
            ],
        };
        router.set_preferred_ingester(ingester_0).await;
        router.ingest_v2(ingest_request).await.unwrap();
    }
}
