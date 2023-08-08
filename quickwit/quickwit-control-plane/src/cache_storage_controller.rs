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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_cache_storage::CacheStorageService;
use quickwit_cluster::ClusterChange;
use quickwit_common::rendezvous_hasher::sort_by_rendez_vous_hash;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::service::QuickwitService;
use quickwit_metastore::{IndexMetadata, ListSplitsQuery, Metastore};
use quickwit_proto::cache_storage::{
    CacheStorageServiceClient, NotifySplitsChangeRequest, SplitsChangeNotification,
};
use serde::Serialize;
use tokio::sync::RwLock;
use tower::timeout::Timeout;
use tracing::debug;

#[derive(Debug, Clone, Default, Serialize)]
pub struct CacheStorageControllerState {}

/// Resides in the control plane. Responsible for recieving notification about the changes in
/// published splits and forwarding these notification to CacheStorageServices that resides on
/// search nodes, that in turn are responsible for maintaining the local cache.
pub struct CacheStorageController {
    metastore: Arc<dyn Metastore>,
    state: CacheStorageControllerState,
    split_to_node_map: HashMap<String, HashSet<String>>,
    pool: CacheStorageServicePool,
}

impl fmt::Debug for CacheStorageController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheStorageController")
            .field("metastore_uri", &self.metastore.uri())
            .finish()
    }
}

#[async_trait]
impl Actor for CacheStorageController {
    type ObservableState = CacheStorageControllerState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    fn name(&self) -> String {
        "CacheStorageController".to_string()
    }
}

impl CacheStorageController {
    pub fn new(metastore: Arc<dyn Metastore>, pool: CacheStorageServicePool) -> Self {
        Self {
            metastore,
            state: CacheStorageControllerState::default(),
            split_to_node_map: HashMap::new(),
            pool,
        }
    }

    // async fn list_splits(
    //     &self,
    //     ctx: &ActorContext<Self>,
    //     index_uid: String,
    // ) -> Result<IndexMetadata, CacheStorageServiceError> {
    //     let _protect_guard = ctx.protect_zone();
    //     let index_uid = IndexUid::from(index_uid);
    //     let splits_query =
    // ListSplitsQuery::for_index(index_uid).
    // with_split_state(quickwit_metastore::SplitState::Published);     let splits =
    // self.metastore.list_splits(splits_query).await?;     Ok(index_metadata)
    // }

    fn find_nodes(
        split_id: &str,
        _index_metadata: &IndexMetadata,
        nodes: &Vec<String>,
    ) -> Vec<String> {
        // This is a temporary implementation that always returns 1 node and doesn't support weights
        // for now
        let mut node_ids = nodes.clone();
        sort_by_rendez_vous_hash(&mut node_ids, split_id);
        node_ids.truncate(1);
        node_ids
    }

    async fn update_cache(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _protect_guard = ctx.protect_zone();
        //        let current_splits_vec = self.split_to_node_map.keys().collect_vec();
        let mut current_splits: HashSet<String> =
            HashSet::from_iter(self.split_to_node_map.keys().cloned());
        let mut updated_nodes = HashSet::new();
        let available_nodes = self.pool.available_nodes().await;
        let mut node_to_split_map: HashMap<String, Vec<(String, Uri)>> = HashMap::new();
        for index_metadata in self.metastore.list_indexes_metadatas().await? {
            let index_uid = index_metadata.index_uid.clone();
            let storage_uri = index_metadata.index_uri();
            if storage_uri.protocol() != Protocol::Cache {
                continue;
            }
            let splits_query = ListSplitsQuery::for_index(index_uid)
                .with_split_state(quickwit_metastore::SplitState::Published);
            for split in self.metastore.list_splits(splits_query).await? {
                let split_id = split.split_id().to_string();
                let allocated_nodes = HashSet::from_iter(
                    Self::find_nodes(&split_id, &index_metadata, &available_nodes)
                        .iter()
                        .cloned(),
                );
                current_splits.remove(&split_id);
                // Figure out whine nodes were updated and have to be notified
                if let Some(current_allocation) = self.split_to_node_map.get(&split_id) {
                    let diff = current_allocation.difference(&allocated_nodes);
                    for node in diff {
                        if available_nodes.contains(node) {
                            updated_nodes.insert(node.clone());
                        }
                    }
                } else {
                    for node in allocated_nodes.iter() {
                        if available_nodes.contains(node) {
                            updated_nodes.insert(node.clone());
                        }
                    }
                }
                for node in allocated_nodes.iter() {
                    if let Some(splits) = node_to_split_map.get_mut(node) {
                        splits.push((split_id.clone(), storage_uri.clone()));
                    } else {
                        node_to_split_map
                            .insert(node.clone(), vec![(split_id.clone(), storage_uri.clone())]);
                    }
                }
                self.split_to_node_map.insert(split_id, allocated_nodes);
            }
        }
        for node in updated_nodes {
            let splits = node_to_split_map.get(&node).expect("Not possible.");
            if let Some(mut service) = self.pool.service(&node).await {
                let request = NotifySplitsChangeRequest {
                    splits_change: splits
                        .iter()
                        .map(|(split_id, storage_uri)| SplitsChangeNotification {
                            storage_uri: storage_uri.to_string(),
                            split_id: split_id.clone(),
                        })
                        .collect(),
                };
                // TODO: Batch await
                quickwit_proto::cache_storage::CacheStorageService::notify_split_change(
                    &mut service,
                    request,
                )
                .await?;
            }
        }
        // let index_id = IndexUid::index_id(index_uid);
        // let incarnation_id = IndexUid::incarnation_id(index_uid);
        // let metadata = self.index_metadata(ctx, index_id).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CacheUpdateRequest {}

#[async_trait]
impl Handler<CacheUpdateRequest> for CacheStorageController {
    type Reply = quickwit_proto::control_plane::Result<()>;

    async fn handle(
        &mut self,
        _request: CacheUpdateRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Splits or nodes changed: updating storage cache.");
        self.update_cache(ctx)
            .await
            .context("Error when updating storage cache")?;
        Ok(Ok(()))
    }
}

#[derive(Clone)]

pub struct CacheStorageServicePool {
    local_cache_storage_service: Option<Mailbox<CacheStorageService>>,
    services: Arc<RwLock<HashMap<String, CacheStorageServiceClient>>>,
}

impl CacheStorageServicePool {
    pub fn new(local_cache_storage_service: Option<Mailbox<CacheStorageService>>) -> Self {
        Self {
            local_cache_storage_service,
            services: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn available_nodes(&self) -> Vec<String> {
        self.services.read().await.keys().cloned().collect()
    }

    async fn service(&self, node: &String) -> Option<CacheStorageServiceClient> {
        self.services.read().await.get(node).cloned()
    }

    pub fn listen_for_changes(
        &self,
        cache_storage_contoller: Mailbox<CacheStorageController>,
        cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    ) {
        let services = self.services.clone();
        let local_cache_storage_service = self.local_cache_storage_service.clone();
        let future = async move {
            cluster_change_stream
                .for_each(|change| async {
                    match change {
                        ClusterChange::Add(node) | ClusterChange::Update(node)
                            if node.enabled_services().contains(&QuickwitService::Searcher) =>
                        {
                            let node_id = node.node_id().to_string();
                            if node.is_self_node() {
                                if let Some(local_cache_storage_service_clone) =
                                    local_cache_storage_service.clone()
                                {
                                    let client = CacheStorageServiceClient::from_mailbox(
                                        local_cache_storage_service_clone,
                                    );
                                    if services.write().await.insert(node_id, client).is_none() {
                                        cache_storage_contoller
                                            .send_message(CacheUpdateRequest {})
                                            .await;
                                    }
                                }
                            } else {
                                let timeout_channel =
                                    Timeout::new(node.channel(), Duration::from_secs(30));
                                let client =
                                    CacheStorageServiceClient::from_channel(timeout_channel);
                                if services.write().await.insert(node_id, client).is_none() {
                                    cache_storage_contoller
                                        .send_message(CacheUpdateRequest {})
                                        .await;
                                }
                            }
                        }
                        ClusterChange::Remove(node) => {
                            let node_id = node.node_id().to_string();
                            services.write().await.remove(&node_id);
                            cache_storage_contoller
                                .send_message(CacheUpdateRequest {})
                                .await;
                        }
                        _ => {}
                    };
                })
                .await;
        };
        tokio::spawn(future);
    }
}

#[cfg(test)]
mod tests {}
