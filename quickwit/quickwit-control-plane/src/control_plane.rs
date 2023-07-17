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

use anyhow::{bail, Context};
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, AskError, Handler, Mailbox, Universe,
};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::Metastore;
use quickwit_proto::control_plane::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};
use quickwit_proto::types::NodeId;
use tracing::{debug, error};

use crate::proto::control_plane::{
    CloseShardsRequest, CloseShardsResponse, GetOpenShardsRequest, GetOpenShardsResponse,
    NotifyIndexChangeRequest, NotifyIndexChangeResponse,
};
use crate::scheduler::IndexingScheduler;
use crate::{IndexerPool, IngestController};

#[derive(Debug)]
pub struct ControlPlane {
    ingest_controller_mailbox: Mailbox<IngestController>,
    ingest_controller_handle: ActorHandle<IngestController>,
    indexing_scheduler_mailbox: Mailbox<IndexingScheduler>,
    indexing_scheduler_handle: ActorHandle<IndexingScheduler>,
}

impl ControlPlane {
    pub fn spawn(
        universe: &Universe,
        cluster_id: String,
        self_node_id: NodeId,
        metastore: Arc<dyn Metastore>,
        ingester_pool: IngesterPool,
        indexer_pool: IndexerPool,
        replication_factor: usize,
    ) -> (Mailbox<Self>, ActorHandle<Self>) {
        let ingester_controller =
            IngestController::new(metastore.clone(), ingester_pool, replication_factor);
        let (ingest_controller_mailbox, ingest_controller_handle) =
            universe.spawn_builder().spawn(ingester_controller);
        let indexing_scheduler =
            IndexingScheduler::new(cluster_id, self_node_id, metastore, indexer_pool);
        let (indexing_scheduler_mailbox, indexing_scheduler_handle) =
            universe.spawn_builder().spawn(indexing_scheduler);
        let control_plane = Self {
            ingest_controller_mailbox,
            ingest_controller_handle,
            indexing_scheduler_mailbox,
            indexing_scheduler_handle,
        };
        universe.spawn_builder().spawn(control_plane)
    }
}

#[async_trait]
impl Actor for ControlPlane {
    type ObservableState = (
        <IngestController as Actor>::ObservableState,
        <IndexingScheduler as Actor>::ObservableState,
    );

    fn name(&self) -> String {
        "ControlPlane".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        (
            self.ingest_controller_handle.last_observation(),
            self.indexing_scheduler_handle.last_observation(),
        )
    }
}

#[async_trait]
impl Handler<GetOpenShardsRequest> for ControlPlane {
    type Reply = crate::Result<GetOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOpenShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        match self.ingest_controller_mailbox.ask_for_res(request).await {
            Ok(response) => Ok(Ok(response)),
            Err(AskError::ErrorReply(error)) => Ok(Err(error)),
            Err(error) => Err(ActorExitStatus::Failure(anyhow::anyhow!(error).into())),
        }
    }
}

#[async_trait]
impl Handler<CloseShardsRequest> for ControlPlane {
    type Reply = crate::Result<CloseShardsResponse>;

    async fn handle(
        &mut self,
        request: CloseShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        match self.ingest_controller_mailbox.ask_for_res(request).await {
            Ok(response) => Ok(Ok(response)),
            Err(AskError::ErrorReply(error)) => Ok(Err(error)),
            Err(error) => Err(ActorExitStatus::Failure(anyhow::anyhow!(error).into())),
        }
    }
}

#[async_trait]
impl Handler<NotifyIndexChangeRequest> for ControlPlane {
    type Reply = quickwit_proto::control_plane::Result<NotifyIndexChangeResponse>;

    async fn handle(
        &mut self,
        request: NotifyIndexChangeRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Index change notification: schedule indexing plan.");
        self.indexing_scheduler_mailbox
            .send_message(request)
            .await
            .context("Error sending index change notification to index scheduler.")?;
        Ok(Ok(NotifyIndexChangeResponse {}))
    }
}
