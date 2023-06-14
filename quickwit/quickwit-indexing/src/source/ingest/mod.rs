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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::tower::Pool;
use quickwit_common::ServiceStream;
use quickwit_config::KafkaSourceParams;
use quickwit_ingest::{
    queue_id, FetchMessage, FetchPayload, FetchStreamCombiner, IngestErrorV2,
    IngestRouterServiceClient, IngesterPool, IngesterServiceClient, LeaseRenewalOutcome,
    ListShardsRequest, ListShardsSubrequest, RenewShardLeasesRequest, RenewShardLeasesSubrequest,
    ShardState, TruncateRequest, TruncateSubrequest,
};
use quickwit_metastore::checkpoint::{self, PartitionId, Position, SourceCheckpoint};
use quickwit_metastore::Metastore;
use quickwit_proto::IndexUid;
use quickwit_types::NodeId;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::StreamMap;
use tracing::log::warn;
use tracing::{debug, error, info};

use super::{BatchBuilder, Source, SourceContext, SourceRuntimeArgs, TypedSourceFactory};
use crate::actors::DocProcessor;
use crate::models::NewPublishToken;

pub type FetchStreamId = String;

type QueueId = String;

pub struct IngesterSourceFactory;

#[async_trait]
impl TypedSourceFactory for IngesterSourceFactory {
    type Source = IngesterSource;
    type Params = ();

    async fn typed_create_source(
        ctx: Arc<SourceRuntimeArgs>,
        _: (),
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        IngesterSource::try_new(ctx, checkpoint).await
    }
}

/// Commands sent to the ingester source by the ingester source controller.
enum IngesterSourceCommand {
    Start {
        leader_id: NodeId,
        follower_id: Option<NodeId>,
        index_uid: IndexUid,
        source_id: String,
        shard_id: u64,
        publish_position_inclusive: Position,
    },
    Stop {
        index_uid: IndexUid,
        source_id: String,
        shard_id: u64,
    },
}

#[derive(Debug, Clone)]
struct ClientId {
    node_id: NodeId,
    index_uid: IndexUid,
    source_id: String,
    pipeline_ord: usize,
}

impl ClientId {
    fn new(node_id: NodeId, index_uid: IndexUid, source_id: String, pipeline_ord: usize) -> Self {
        Self {
            node_id,
            index_uid,
            source_id,
            pipeline_ord,
        }
    }

    fn client_id(&self) -> String {
        format!(
            "{}/ingester-source/{}/{}/{}",
            self.node_id, self.index_uid, self.source_id, self.pipeline_ord
        )
    }
}

struct LeasedShard {
    leader_id: NodeId,
    follower_id: Option<NodeId>,
    partition_id: PartitionId,
    current_position_inclusive: Position,
}

/// Streams documents from a set of shards.
pub struct IngesterSource {
    client_id: ClientId,
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    command_rx: mpsc::UnboundedReceiver<IngesterSourceCommand>,
    fetch_stream: FetchStreamCombiner,
    leased_shards: HashMap<QueueId, LeasedShard>,
}

impl fmt::Debug for IngesterSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.debug_struct("IngesterSource").finish()
    }
}

impl IngesterSource {
    pub async fn try_new(
        ctx: Arc<SourceRuntimeArgs>,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let client_id = ClientId::new(
            ctx.node_id().into(),
            ctx.index_uid().clone(),
            ctx.source_id().to_string(),
            ctx.pipeline_ord(),
        );
        let metastore = ctx.metastore.clone();
        let ingester_pool = ctx.ingester_pool.clone();
        let command_rx = spawn_leaser_manager_task(metastore.clone(), client_id.clone());
        let fetch_stream = FetchStreamCombiner::new(client_id.client_id(), ingester_pool.clone());
        let leased_shards = HashMap::new();

        Ok(Self {
            client_id,
            metastore,
            ingester_pool,
            command_rx,
            fetch_stream,
            leased_shards,
        })
    }

    fn process_fetch_payload(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_payload: FetchPayload,
    ) {
        let queue_id = fetch_payload.queue_id();
        let leased_shard = self.leased_shards.get_mut(&queue_id).expect("FIXME");
        let partition_id = leased_shard.partition_id.clone();

        let doc_batch = fetch_payload.doc_batch.expect("");
        for doc in doc_batch.docs() {
            batch_builder.add_doc(doc);
        }
        let from_position = leased_shard.current_position_inclusive.clone();
        let to_position =
            Position::from(fetch_payload.from_position_inclusive + doc_batch.num_docs() as u64 - 1);

        batch_builder.checkpoint_delta.record_partition_delta(
            partition_id,
            from_position,
            to_position.clone(),
        );
        leased_shard.current_position_inclusive = to_position;
    }
}

#[async_trait]
impl Source for IngesterSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let publish_token = self.client_id.client_id();
        ctx.send_message(doc_processor_mailbox, NewPublishToken(publish_token))
            .await?;
        Ok(())
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::default();
        let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                fetch_command_opt = self.command_rx.recv() => {
                    match fetch_command_opt {
                        Some(IngesterSourceCommand::Start { leader_id, follower_id, index_uid, source_id, shard_id, publish_position_inclusive }) => {
                            let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
                            let partition_id = PartitionId::from(queue_id.clone());
                            let from_position_inclusive = publish_position_inclusive.as_u64().unwrap_or(0) + 1;
                            let leased_shard = LeasedShard {
                                leader_id: leader_id.clone(),
                                follower_id: follower_id.clone(),
                                partition_id,
                                current_position_inclusive: publish_position_inclusive
                            };
                            self.leased_shards.insert(queue_id, leased_shard);
                            self.fetch_stream.subscribe_to_shard(leader_id, follower_id, index_uid, source_id, shard_id, from_position_inclusive).await;
                        },
                        Some(IngesterSourceCommand::Stop { index_uid, source_id, shard_id }) => {
                            let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
                            self.leased_shards.remove(&queue_id);
                            self.fetch_stream.unsubscribe_from_shard(index_uid.as_str(), &source_id, shard_id).await;
                        },
                        _ => panic!("Lease manager terminated unexpectedly."), // FIXME
                    }
                }
                fetch_payload_opt = self.fetch_stream.next() => {
                    match fetch_payload_opt {
                        Some(Ok(fetch_payload)) => {
                            self.process_fetch_payload(&mut batch_builder, fetch_payload)
                        },
                        Some(Err(error)) => {
                            error!(error=?error, "Failed to fetch payload.")
                            // TODO handle error
                        },
                        _ => panic!("Fetch stream terminated unexpectedly."), // FIXME
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }
        if !batch_builder.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "Sending doc batch to indexer."
            );
            let message = batch_builder.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let mut subrequests = Vec::with_capacity(checkpoint.num_partitions());

        for (partition_id, position) in checkpoint.iter() {
            let index_uid = self.client_id.index_uid.clone();
            let source_id = self.client_id.source_id.clone();
            let shard_id = partition_id.as_u64().expect("");
            let to_position_inclusive = position.as_u64().expect("");

            let truncate_subrequest = TruncateSubrequest {
                index_uid: index_uid.to_string(),
                source_id: source_id.to_string(),
                shard_id,
                to_position_inclusive,
            };
        }
        let truncate_request = TruncateRequest { subrequests };
        // self.router.truncate(truncate_request).await?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("IngesterSource")
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({})
    }
}

fn spawn_leaser_manager_task(
    metastore: Arc<dyn Metastore>,
    client_id: ClientId,
) -> mpsc::UnboundedReceiver<IngesterSourceCommand> {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let future = async move {
        let lessee_id = client_id.client_id();
        let mut leased_shards = HashSet::new();
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            let subrequest = RenewShardLeasesSubrequest {
                lessee_id: lessee_id.clone(),
                index_uid: client_id.index_uid.to_string(),
                source_id: client_id.source_id.clone(),
                shard_ids: leased_shards.iter().cloned().collect(),
            };
            let request = RenewShardLeasesRequest {
                subrequests: vec![subrequest],
            };
            let response = match metastore.renew_shard_leases(request).await {
                Ok(response) => response,
                Err(error) => {
                    error!(error=?error, "Failed to renew shard leases.");
                    continue;
                }
            };
            for subresponse in response.subresponses {
                if subresponse.lessee_id != lessee_id {
                    continue;
                }
                // assert_eq!(subresponse.index_uid, client_id.index_uid);
                // assert_eq!(subresponse.source_id, client_id.source_id);

                for lease_renewal_result in subresponse.lease_renewal_results {
                    // FIXME
                    match lease_renewal_result.renewal_outcome {
                        0 => {
                            info!(
                                node_id=%client_id.node_id,
                                index_id=%client_id.index_uid.index_id(),
                                source_id=%client_id.source_id,
                                pipeline_ord=%client_id.pipeline_ord,
                                shard_id=%lease_renewal_result.shard_id,
                                "Acquired new shard lease."
                            );
                            leased_shards.insert(lease_renewal_result.shard_id);
                            let shard = lease_renewal_result
                                .shard
                                .expect("The acquired lease should have a shard.");
                            let publish_position_inclusive =
                                Position::from(shard.publish_position_inclusive);
                            let command = IngesterSourceCommand::Start {
                                leader_id: shard.leader_id.into(),
                                follower_id: shard
                                    .follower_id
                                    .map(|follower_id| follower_id.into()),
                                index_uid: shard.index_uid.into(),
                                source_id: shard.source_id,
                                shard_id: shard.shard_id,
                                publish_position_inclusive,
                            };
                            command_tx.send(command).unwrap();
                        }
                        1 => {
                            debug!(
                                node_id=%client_id.node_id,
                                index_id=%client_id.index_uid.index_id(),
                                source_id=%client_id.source_id,
                                pipeline_ord=%client_id.pipeline_ord,
                                shard_id=%lease_renewal_result.shard_id,
                                "Renewed shard lease."
                            );
                        }
                        _ => {
                            warn!("Unknown lease renewal outcome.")
                        }
                    }
                }
            }
        }
    };
    tokio::spawn(future);
    command_rx
}

// impl KafkaSource {
//     /// Instantiates a new `KafkaSource`.
//     pub async fn try_new(
//         ctx: Arc<SourceExecutionContext>,
//         params: KafkaSourceParams,
//         _ignored_checkpoint: SourceCheckpoint,
//     ) -> anyhow::Result<Self> { let topic = params.topic.clone(); let backfill_mode_enabled =
//       params.enable_backfill_mode;

//         let (events_tx, events_rx) = mpsc::channel(100);
//         let (client_config, consumer) = create_consumer(
//             &ctx.index_uid,
//             &ctx.source_config.source_id,
//             params,
//             events_tx.clone(),
//         )?;
//         let native_client_config = client_config.create_native_config()?;
//         let group_id = native_client_config.get("group.id")?;
//         let session_timeout_ms = native_client_config
//             .get("session.timeout.ms")?
//             .parse::<u64>()?;
//         let max_poll_interval_ms = native_client_config
//             .get("max.poll.interval.ms")?
//             .parse::<u64>()?;

//         let poll_loop_jh = spawn_consumer_poll_loop(consumer, topic.clone(), events_tx);
//         let publish_lock = PublishLock::default();

//         info!(
//             index_id=%ctx.index_uid.index_id(),
//             source_id=%ctx.source_config.source_id,
//             topic=%topic,
//             group_id=%group_id,
//             max_poll_interval_ms=%max_poll_interval_ms,
//             session_timeout_ms=%session_timeout_ms,
//             "Starting Kafka source."
//         );
//         if max_poll_interval_ms <= 60_000 {
//             warn!(
//                 "`max.poll.interval.ms` is set to a short duration that may cause the source to \
//                  crash when back pressure from the indexer occurs. The recommended value is \
//                  `300000` (5 minutes)."
//             );
//         }
//         Ok(KafkaSource {
//             ctx,
//             topic,
//             state: KafkaSourceState::default(),
//             backfill_mode_enabled,
//             events_rx,
//             poll_loop_jh,
//             publish_lock,
//         })
//     }

// #[async_trait]
// impl Source for IngesterSource {
//     async fn emit_batches(
//         &mut self,
//         doc_processor_mailbox: &Mailbox<DocProcessor>,
//         ctx: &SourceContext,
//     ) -> Result<Duration, ActorExitStatus> { let now = Instant::now(); let mut batch =
//       BatchBuilder::default(); let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
//       tokio::pin!(deadline);

//         loop {
//             tokio::select! {
//                 // event_opt = self.events_rx.recv() => {
//                 //     let event = event_opt.ok_or_else(||
// ActorExitStatus::from(anyhow!("Consumer was dropped.")))?;                 //     match event {
//                 //         KafkaEvent::Message(message) => self.process_message(message, &mut
// batch).await?,                 //         KafkaEvent::AssignPartitions { partitions,
// assignment_tx} => self.process_assign_partitions(ctx, &partitions, assignment_tx).await?,
//                 //         KafkaEvent::RevokePartitions { ack_tx } =>
// self.process_revoke_partitions(ctx, doc_processor_mailbox, &mut batch, ack_tx).await?,
//                 //         KafkaEvent::PartitionEOF(partition) =>
// self.process_partition_eof(partition),                 //         KafkaEvent::Error(error) =>
// Err(ActorExitStatus::from(error))?,                 //     }
//                 //     if batch.num_bytes >= BATCH_NUM_BYTES_LIMIT {
//                 //         break;
//                 //     }
//                 // }
//                 _ = &mut deadline => {
//                     break;
//                 }
//             }
//             ctx.record_progress();
//         }
//         if !batch.checkpoint_delta.is_empty() {
//             debug!(
//                 num_docs=%batch.docs.len(),
//                 num_bytes=%batch.num_bytes,
//                 num_millis=%now.elapsed().as_millis(),
//                 "Sending doc batch to indexer.");
//             let message = batch.build();
//             ctx.send_message(doc_processor_mailbox, message).await?;
//         }
//         // if self.should_exit() {
//         //     info!(topic = %self.topic, "Reached end of topic.");
//         //     ctx.send_exit_with_success(doc_processor_mailbox).await?;
//         //     return Err(ActorExitStatus::Success);
//         // }
//         Ok(Duration::default())
//     }

//     fn name(&self) -> String {
//         format!("IngesterSource")
//     }

//     fn observable_state(&self) -> serde_json::Value {
//         json!({})
//     }
// }
