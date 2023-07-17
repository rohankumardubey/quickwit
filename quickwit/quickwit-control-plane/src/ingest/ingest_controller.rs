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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_config::INGEST_SOURCE_ID;
use quickwit_ingest::{IngesterPool, IngesterService, PingRequest, PingResponse};
use quickwit_metastore::Metastore;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    CloseShardsRequest as MetastoreCloseShardsRequest,
    CloseShardsResponse as MetastoreCloseShardsResponse, CloseShardsResponse,
    CloseShardsSubrequest as MetastoreCloseShardsSubrequest,
    CloseShardsSuccess as MetastoreCloseShardsSucces, ListShardsRequest, ListShardsSubrequest,
    OpenShardsRequest, OpenShardsSubrequest,
};
use quickwit_proto::types::{NodeId, SourceId};
use quickwit_proto::IndexUid;
use tokio::time::timeout;
use tracing::warn;

use crate::proto::control_plane::{
    CloseShardsRequest, CloseShardsResponse, GetOpenShardsRequest, GetOpenShardsResponse,
    GetOpenShardsSubrequest, GetOpenShardsSubresponse,
};

#[derive(Debug, Default)]
struct ShardTable {
    shards: HashMap<(IndexUid, SourceId), Vec<Shard>>,
}

impl ShardTable {
    /// Finds the open shards that satisfies the [`GetOpenShardsRequest`] request.
    fn find_open_shards(
        &self,
        get_open_shards_subrequest: &GetOpenShardsSubrequest,
        unavailable_ingesters: &HashSet<String>,
    ) -> Option<GetOpenShardsSubresponse> {
        let key = (
            get_open_shards_subrequest.index_uid.clone().into(),
            get_open_shards_subrequest.source_id.clone(),
        );
        let open_shards: Vec<Shard> = self
            .shards
            .get(&key)?
            .iter()
            .filter(|shard| shard.is_open() && !unavailable_ingesters.contains(&shard.leader_id))
            .cloned()
            .collect();
        if open_shards.is_empty() {
            return None;
        }
        let get_open_shards_subresponse = GetOpenShardsSubresponse {
            index_uid: key.0.into(),
            source_id: key.1,
            open_shards,
        };
        Some(get_open_shards_subresponse)
    }

    /// Updates the shard table with the open shards returned by the metastore.
    fn update_open_shards(&mut self, open_shards_response: &CloseShardsResponse) {
        for open_shards_subresponse in &open_shards_response.subresponses {
            let key = (
                open_shards_subresponse.index_uid.clone().into(),
                open_shards_subresponse.source_id.clone(),
            );
            self.shards
                .insert(key, open_shards_subresponse.open_shards.clone());
        }
    }

    /// Evicts the shards in the shard table that are marked as closing or closed in the metastore.
    fn close_shards(&mut self, close_shards_response: &MetastoreCloseShardsResponse) {
        // TODO: This is pretty inefficient. Rework when data model is more certain.
        for close_shards_success in &close_shards_response.successes {
            let key = (
                close_shards_success.index_uid.clone().into(),
                close_shards_success.source_id.clone(),
            );
            if let Some(shards) = self.shards.get_mut(&key) {
                shards.retain(|shard| shard.shard_id != close_shards_success.shard_id);
            }
        }
    }
}

pub struct IngestController {
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    shard_table: ShardTable,
    replication_factor: usize,
}

impl IngestController {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        ingester_pool: IngesterPool,
        replication_factor: usize,
    ) -> Self {
        Self {
            metastore,
            ingester_pool,
            shard_table: ShardTable::default(),
            replication_factor,
        }
    }

    async fn load_shard_table(&mut self, ctx: &ActorContext<Self>) {
        let indexes = ctx
            .protect_future(self.metastore.list_indexes_metadatas())
            .await
            .expect("FIXME");

        let mut subrequests = Vec::new();

        for index in indexes {
            for source_id in index.sources.into_keys() {
                if source_id != INGEST_SOURCE_ID {
                    continue;
                }
                let request = ListShardsSubrequest {
                    index_uid: index.index_uid.clone().into(),
                    source_id,
                    shard_state: Some(ShardState::Open as i32),
                };
                subrequests.push(request);
            }
        }
        let list_shards_request = ListShardsRequest { subrequests };
        let list_shard_response = ctx
            .protect_future(self.metastore.list_shards(list_shards_request))
            .await
            .expect("FIXME");

        let mut shards = HashMap::with_capacity(list_shard_response.subresponses.len());

        for list_shards_subresponse in list_shard_response.subresponses {
            let key = (
                list_shards_subresponse.index_uid.into(),
                list_shards_subresponse.source_id,
            );
            shards.insert(key, list_shards_subresponse.shards);
        }
        self.shard_table.shards = shards;
    }

    /// Pings an ingester that is eligible to become a leader for a shard. If a follower ID is
    /// provided, it will also ping the follower.
    async fn ping_leader(
        &mut self,
        ctx: &ActorContext<Self>,
        leader_id: &NodeId,
        follower_id: &Option<NodeId>,
    ) -> Option<PingResponse> {
        let mut leader_ingester = self.ingester_pool.get(leader_id).await.expect("FIXME");

        let ping_request = PingRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id.clone().map(|follower_id| follower_id.into()),
        };
        ctx.protect_future(timeout(
            Duration::from_secs(1),
            leader_ingester.ping(ping_request),
        ))
        .await
        .ok()?
        .ok()
    }

    async fn find_leader(&mut self, ctx: &ActorContext<Self>) -> Option<(NodeId, Option<NodeId>)> {
        let mut excluded_candidates = HashSet::new();

        loop {
            let leader_id_candidate = self
                .ingester_pool
                .find(|ingester_id| !excluded_candidates.contains(ingester_id))
                .await?;
            let follower_id_candidate = if self.replication_factor == 1 {
                None
            } else {
                let follower_id_candidate = self
                    .ingester_pool
                    .find(|ingester_id| {
                        *ingester_id != leader_id_candidate
                            && !excluded_candidates.contains(ingester_id)
                    })
                    .await?;
                Some(follower_id_candidate)
            };
            if self
                .ping_leader(ctx, &leader_id_candidate, &follower_id_candidate)
                .await
                .is_some()
            {
                return Some((leader_id_candidate, follower_id_candidate));
            };
            excluded_candidates.insert(leader_id_candidate);
        }
    }

    async fn get_open_shards(
        &mut self,
        ctx: &ActorContext<Self>,
        get_open_shards_request: GetOpenShardsRequest,
    ) -> crate::Result<GetOpenShardsResponse> {
        let mut get_open_shards_subresponses =
            Vec::with_capacity(get_open_shards_request.subrequests.len());

        let unavailable_ingesters: HashSet<String> = get_open_shards_request
            .unavailable_ingesters
            .into_iter()
            .collect();

        let mut open_shards_subrequests = Vec::new();

        for get_open_shards_subrequest in get_open_shards_request.subrequests {
            if let Some(get_open_shards_subresponse) = self
                .shard_table
                .find_open_shards(&get_open_shards_subrequest, &unavailable_ingesters)
            {
                get_open_shards_subresponses.push(get_open_shards_subresponse);
            } else {
                // TODO: Find leaders in batches.
                let (leader_id, follower_id) = self.find_leader(ctx).await.expect("FIXME");

                let open_shards_subrequest = OpenShardsSubrequest {
                    index_uid: get_open_shards_subrequest.index_uid,
                    source_id: get_open_shards_subrequest.source_id,
                    shard_ids: vec![0], // FIXME
                    leader_id: leader_id.into(),
                    follower_id: follower_id.map(|follower_id| follower_id.into()),
                };
            }
        }
        if !open_shards_subrequests.is_empty() {
            let open_shards_request = OpenShardsRequest {
                subrequests: open_shards_subrequests,
            };
            let open_shards_response = ctx
                .protect_future(self.metastore.open_shards(open_shards_request))
                .await
                .expect("FIXME");

            self.shard_table.update_open_shards(&open_shards_response);

            for open_shards_subresponse in open_shards_response.subresponses {
                let get_open_shards_subresponse = GetOpenShardsSubresponse {
                    index_uid: open_shards_subresponse.index_uid,
                    source_id: open_shards_subresponse.source_id,
                    open_shards: open_shards_subresponse.open_shards,
                };
                get_open_shards_subresponses.push(get_open_shards_subresponse);
            }
        }
        let get_open_shards_response = GetOpenShardsResponse {
            subresponses: get_open_shards_subresponses,
        };
        Ok(get_open_shards_response)
    }

    async fn close_shards(
        &mut self,
        close_shards_request: CloseShardsRequest,
        ctx: &ActorContext<Self>,
    ) -> crate::Result<CloseShardsResponse> {
        let mut metastore_close_shards_subrequests =
            Vec::with_capacity(close_shards_request.subrequests.len());
        for close_shards_subrequest in close_shards_request.subrequests {
            let metastore_close_shards_subrequest = MetastoreCloseShardsSubrequest {
                index_uid: close_shards_subrequest.index_uid.into(),
                source_id: close_shards_subrequest.source_id,
                shard_id: close_shards_subrequest.shard_id,
                shard_state: close_shards_subrequest.shard_state,
                replication_position_inclusive: close_shards_subrequest
                    .replication_position_inclusive,
            };
            metastore_close_shards_subrequests.push(metastore_close_shards_subrequest);
        }
        let metastore_close_shards_request = MetastoreCloseShardsRequest {
            subrequests: metastore_close_shards_subrequests,
        };
        let metastore_close_shards_response = ctx
            .protect_future(self.metastore.close_shards(metastore_close_shards_request))
            .await
            .expect("FIXME");
        for metastore_close_shards_success in metastore_close_shards_response.successes {
            // self.shard_table
            //     .close_shards(&metastore_close_shards_success);
        }
        // self.indexing_controller_mailbox.send_message(metastore_close_shards_success);

        let close_shards_response = CloseShardsResponse {};
        Ok(close_shards_response)
    }
}

#[async_trait]
impl Actor for IngestController {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IngestController".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.load_shard_table(ctx).await;
        Ok(())
    }
}

#[async_trait]
impl Handler<GetOpenShardsRequest> for IngestController {
    type Reply = crate::Result<GetOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOpenShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let subresponses = Vec::new();
        let response = GetOpenShardsResponse { subresponses };
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<CloseShardsRequest> for IngestController {
    type Reply = crate::Result<CloseShardsResponse>;

    async fn handle(
        &mut self,
        request: CloseShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = CloseShardsResponse {};
        Ok(Ok(response))
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::OpenShardsSubresponse;

    use super::*;
    use crate::proto::control_plane::GetOpenShardsSubrequest;

    #[test]
    fn test_shard_table_get_open_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
        let index_uid_1: IndexUid = "test-index:1".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        let mut unavailable_ingesters = HashSet::new();

        let get_open_shards_subrequest = GetOpenShardsSubrequest {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
        };
        assert!(shard_table
            .find_open_shards(&get_open_shards_subrequest, &unavailable_ingesters)
            .is_none());

        let shard_0 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closing as i32,
            ..Default::default()
        };
        let shard_2 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 2,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_3 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 3,
            leader_id: "test-leader-1".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let key = (index_uid_0.clone(), source_id.clone());

        shard_table.shards.insert(
            key,
            vec![
                shard_0.clone(),
                shard_1.clone(),
                shard_2.clone(),
                shard_3.clone(),
            ],
        );
        let get_open_shards_subrequest = GetOpenShardsSubrequest {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
        };
        let open_shards = shard_table
            .find_open_shards(&get_open_shards_subrequest, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.open_shards.len(), 2);
        assert_eq!(open_shards.open_shards[0], shard_2);
        assert_eq!(open_shards.open_shards[1], shard_3);

        unavailable_ingesters.insert("test-leader-0".to_string());

        let open_shards = shard_table
            .find_open_shards(&get_open_shards_subrequest, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.open_shards.len(), 1);
        assert_eq!(open_shards.open_shards[0], shard_3);
    }

    #[test]
    fn test_shard_table_update_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
        let index_uid_1: IndexUid = "test-index:1".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_0 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_2 = Shard {
            index_uid: index_uid_1.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let key = (index_uid_0.clone(), source_id.clone());
        shard_table.shards.insert(key, vec![shard_0.clone()]);

        let open_shards_response = CloseShardsResponse {
            subresponses: vec![
                OpenShardsSubresponse {
                    index_uid: index_uid_0.clone().into(),
                    source_id: source_id.clone(),
                    open_shards: vec![shard_0.clone(), shard_1.clone()],
                },
                OpenShardsSubresponse {
                    index_uid: index_uid_1.clone().into(),
                    source_id: source_id.clone(),
                    open_shards: vec![shard_2.clone()],
                },
            ],
        };
        shard_table.update_open_shards(&open_shards_response);
        assert_eq!(shard_table.shards.len(), 2);

        let key = (index_uid_0.clone(), source_id.clone());
        assert_eq!(shard_table.shards[&key].len(), 2);
        assert_eq!(shard_table.shards[&key][0], shard_0);
        assert_eq!(shard_table.shards[&key][1], shard_1);

        let key = (index_uid_1.clone(), source_id.clone());
        assert_eq!(shard_table.shards[&key].len(), 1);
        assert_eq!(shard_table.shards[&key][0], shard_2);
    }

    #[test]
    fn test_shard_table_close_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
        let index_uid_1: IndexUid = "test-index:1".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_0 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let key = (index_uid_0.clone(), source_id.clone());
        shard_table
            .shards
            .insert(key, vec![shard_0.clone(), shard_1.clone()]);

        // let close_shards_response = MetastoreCloseShardsResponse {
        //     success: vec![
        //         MetastoreCloseShardsSucces {
        //             index_uid: index_uid_0.clone().into(),
        //             source_id: source_id.clone(),
        //             shard_id: 0,
        //         },
        //     ],
        //     failures: Vec::new(),
        // };
        // shard_table.update_open_shards(&close_shards_response);
        // assert_eq!(shard_table.shards.len(), 2);

        // let key = (index_uid_0.clone(), source_id.clone());
        // assert_eq!(shard_table.shards[&key].len(), 2);
        // assert_eq!(shard_table.shards[&key][0], shard_0);
        // assert_eq!(shard_table.shards[&key][1], shard_1);

        // let key = (index_uid_1.clone(), source_id.clone());
        // assert_eq!(shard_table.shards[&key].len(), 1);
        // assert_eq!(shard_table.shards[&key][0], shard_2);
    }
}
