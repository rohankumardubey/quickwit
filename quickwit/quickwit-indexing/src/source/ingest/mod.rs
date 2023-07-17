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
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_ingest::{FetchResponseV2, IngesterPool, MultiFetchStream};
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use quickwit_metastore::Metastore;
use quickwit_proto::types::NodeId;
use quickwit_proto::IndexUid;
use serde_json::json;
use tokio::time;
use tracing::{debug, error};

use super::{
    Assignment, BatchBuilder, Source, SourceContext, SourceRuntimeArgs, TypedSourceFactory,
    UnassignShards,
};
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
        runtime_args: Arc<SourceRuntimeArgs>,
        _: (),
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        IngesterSource::try_new(runtime_args, checkpoint).await
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

struct AssignedShard {
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
    assigned_shards: HashMap<QueueId, AssignedShard>,
    fetch_stream: MultiFetchStream,
}

impl fmt::Debug for IngesterSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.debug_struct("IngesterSource").finish()
    }
}

impl IngesterSource {
    pub async fn try_new(
        ctx: Arc<SourceRuntimeArgs>,
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let node_id: NodeId = ctx.node_id().into();
        let client_id = ClientId::new(
            node_id.clone(),
            ctx.index_uid().clone(),
            ctx.source_id().to_string(),
            ctx.pipeline_ord(),
        );
        let metastore = ctx.metastore.clone();
        let ingester_pool = ctx.ingester_pool.clone();
        let fetch_stream =
            MultiFetchStream::new(node_id, client_id.client_id(), ingester_pool.clone());
        let assigned_shards = HashMap::new();

        Ok(Self {
            client_id,
            metastore,
            ingester_pool,
            assigned_shards,
            fetch_stream,
        })
    }

    fn process_fetch_response(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_response: FetchResponseV2,
    ) {
        let queue_id = fetch_response.queue_id();
        let assigned_shard = self.assigned_shards.get_mut(&queue_id).expect("FIXME");
        let partition_id = assigned_shard.partition_id.clone();

        for doc in fetch_response.docs() {
            batch_builder.add_doc(doc);
        }
        let from_position_exclusive = assigned_shard.current_position_inclusive.clone();
        let to_position_inclusive = Position::from(fetch_response.to_position_inclusive());

        batch_builder.checkpoint_delta.record_partition_delta(
            partition_id,
            from_position_exclusive,
            to_position_inclusive.clone(),
        );
        assigned_shard.current_position_inclusive = to_position_inclusive;
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
                fetch_payload_opt = self.fetch_stream.next() => {
                    match fetch_payload_opt {
                        Some(Ok(fetch_payload)) => {
                            self.process_fetch_response(&mut batch_builder, fetch_payload);

                            if batch_builder.num_bytes >= 5 * 1024 * 1024 {
                                break;
                            }
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

    async fn assign_shards(
        &mut self,
        _assignement: Assignment,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // let assigned_shard_ids = self
        //     .assigned_shards
        //     .values()
        //     .map(|shard| shard.shard_id)
        //     .collect::<HashSet<_>>();
        // let assign_shards_request = AssignShardsRequest {
        //     client_id: self.client_id.client_id(),
        //     index_uid: self.client_id.index_uid.clone().into(),
        //     source_id: assignement.source_id.clone(),
        //     shard_ids: assignement.shard_ids.clone(),
        // };
        // let assign_shard_response = self.metastore.assign_shards(assign_shards_request).await?;
        // for assigned_shard in assign_shard_response.assigned_shards {
        //     self.fetch_stream.subscribe(
        //         assigned_shard.leader_id,
        //         assigned_shard.follower_id,
        //         assigned_shard.index_uid,
        //         assigned_shard.source_id,
        //         assigned_shard.shard_id,
        //         assigned_shard.from_position_exclusive,
        //         None,
        //     );
        //     let queue_id = assigned_shard.queue_id();
        //     self.assigned_shards.insert(queue_id, assigned_shard);
        // }
        Ok(())
    }

    async fn unassign_shards(
        &mut self,
        _unassignment: UnassignShards,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn suggest_truncate(
        &mut self,
        _checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // let mut subrequests = Vec::with_capacity(checkpoint.num_partitions());

        // for (partition_id, position) in checkpoint.iter() {
        //     let index_uid = self.client_id.index_uid.clone();
        //     let source_id = self.client_id.source_id.clone();
        //     let shard_id = partition_id.as_u64().expect("");
        //     let to_position_inclusive = position.as_u64().expect("");

        //     let truncate_subrequest = TruncateSubrequest {
        //         index_uid: index_uid.to_string(),
        //         source_id: source_id.to_string(),
        //         shard_id,
        //         to_position_inclusive,
        //     };
        // }
        // let truncate_request = TruncateRequest { subrequests };
        // self.router.truncate(truncate_request).await?;
        Ok(())
    }

    fn name(&self) -> String {
        "IngesterSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({})
    }
}

// fn spawn_leaser_manager_task(
//     metastore: Arc<dyn Metastore>,
//     client_id: ClientId,
// ) -> mpsc::UnboundedReceiver<IngesterSourceCommand> { let (command_tx, command_rx) =
//   mpsc::unbounded_channel(); let future = async move { let lessee_id = client_id.client_id(); let
//   mut assigned_shards = HashSet::new(); let mut interval =
//   time::interval(Duration::from_secs(10));

//         loop {
//             interval.tick().await;

//             let subrequest = RenewShardLeasesSubrequest {
//                 lessee_id: lessee_id.clone(),
//                 index_uid: client_id.index_uid.to_string(),
//                 source_id: client_id.source_id.clone(),
//                 shard_ids: assigned_shards.iter().cloned().collect(),
//             };
//             let request = RenewShardLeasesRequest {
//                 subrequests: vec![subrequest],
//             };
//             let response = match metastore.renew_shard_leases(request).await {
//                 Ok(response) => response,
//                 Err(error) => {
//                     error!(error=?error, "Failed to renew shard leases.");
//                     continue;
//                 }
//             };
//             for subresponse in response.subresponses {
//                 if subresponse.lessee_id != lessee_id {
//                     continue;
//                 }
//                 // assert_eq!(subresponse.index_uid, client_id.index_uid);
//                 // assert_eq!(subresponse.source_id, client_id.source_id);

//                 for lease_renewal_result in subresponse.lease_renewal_results {
//                     // FIXME
//                     match lease_renewal_result.renewal_outcome {
//                         0 => {
//                             info!(
//                                 node_id=%client_id.node_id,
//                                 index_id=%client_id.index_uid.index_id(),
//                                 source_id=%client_id.source_id,
//                                 pipeline_ord=%client_id.pipeline_ord,
//                                 shard_id=%lease_renewal_result.shard_id,
//                                 "Acquired new shard lease."
//                             );
//                             assigned_shards.insert(lease_renewal_result.shard_id);
//                             let shard = lease_renewal_result
//                                 .shard
//                                 .expect("The acquired lease should have a shard.");
//                             let publish_position_inclusive =
//                                 Position::from(shard.publish_position_inclusive);
//                             let command = IngesterSourceCommand::Start {
//                                 leader_id: shard.leader_id.into(),
//                                 follower_id: shard
//                                     .follower_id
//                                     .map(|follower_id| follower_id.into()),
//                                 index_uid: shard.index_uid.into(),
//                                 source_id: shard.source_id,
//                                 shard_id: shard.shard_id,
//                                 publish_position_inclusive,
//                             };
//                             command_tx.send(command).unwrap();
//                         }
//                         1 => {
//                             debug!(
//                                 node_id=%client_id.node_id,
//                                 index_id=%client_id.index_uid.index_id(),
//                                 source_id=%client_id.source_id,
//                                 pipeline_ord=%client_id.pipeline_ord,
//                                 shard_id=%lease_renewal_result.shard_id,
//                                 "Renewed shard lease."
//                             );
//                         }
//                         _ => {
//                             warn!("Unknown lease renewal outcome.")
//                         }
//                     }
//                 }
//             }
//         }
//     };
//     tokio::spawn(future);
//     command_rx
// }

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
