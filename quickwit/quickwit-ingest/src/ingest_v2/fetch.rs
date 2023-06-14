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

use core::fmt;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use mrecordlog::MultiRecordLog;
use quickwit_common::ServiceStream;
use quickwit_proto::IndexUid;
use quickwit_types::{NodeId, ShardId, SourceId};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;

use super::ingester::{QueueId, ShardStatus};
use crate::{
    DocBatchBuilderV2, FetchMessage, FetchPayload, IngestErrorV2, IngesterPool, IngesterService,
    OpenFetchStreamRequest, OpenFetchStreamResponse, SubscribeToShard, UnsubscribeFromShard,
    UpdateFetchStreamRequest,
};

pub(super) struct FetchTaskHandle {
    join_handle: JoinHandle<()>,
}

impl FetchTaskHandle {
    pub fn abort(&self) {
        self.join_handle.abort();
    }
}

pub(super) struct FetchTask {
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    from_position_inclusive: u64,
    to_position_inclusive: u64,
    fetch_message_tx: mpsc::Sender<super::Result<FetchMessage>>,
    shard_status_rx: watch::Receiver<ShardStatus>,
    batch_num_bytes: usize,
}

impl FetchTask {
    const DEFAULT_BATCH_NUM_BYTES: usize = 1024 * 1024; // 1 MiB

    pub fn spawn(
        mrecordlog: Arc<RwLock<MultiRecordLog>>,
        subscription: SubscribeToShard,
        fetch_message_tx: mpsc::Sender<super::Result<FetchMessage>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
    ) -> FetchTaskHandle {
        let from_position_inclusive = subscription
            .from_position_exclusive
            .map(|position| position + 1)
            .unwrap_or(0);
        let to_position_inclusive = subscription.to_position_inclusive.unwrap_or(u64::MAX);
        let mut fetch_task = Self {
            mrecordlog,
            queue_id: subscription.queue_id(),
            index_uid: subscription.index_uid.into(),
            source_id: subscription.source_id,
            shard_id: subscription.shard_id,
            from_position_inclusive,
            to_position_inclusive,
            fetch_message_tx,
            shard_status_rx,
            batch_num_bytes,
        };
        let future = async move { fetch_task.run().await };
        let join_handle = tokio::spawn(future);

        FetchTaskHandle { join_handle }
    }

    async fn run(&mut self) {
        while self.from_position_inclusive <= self.to_position_inclusive {
            while self
                .shard_status_rx
                .borrow()
                .replication_position_inclusive()
                < self.from_position_inclusive
            {
                if self.shard_status_rx.changed().await.is_err() {
                    // The ingester was dropped.
                    return;
                }
            }
            let fetch_range = self.from_position_inclusive..=self.to_position_inclusive;
            let mrecordlog_guard = self.mrecordlog.read().await;

            let Ok(docs) = mrecordlog_guard.range(&self.queue_id, fetch_range) else {
                // The queue no longer exists.
                break;
            };
            let mut doc_batch_builder = DocBatchBuilderV2::with_capacity(self.batch_num_bytes);

            for (_position, doc) in docs {
                if doc_batch_builder.num_bytes() + doc.len() > doc_batch_builder.capacity() {
                    break;
                }
                doc_batch_builder.add_doc(doc.borrow());
            }
            // Drop the lock while we send the message.
            drop(mrecordlog_guard);

            let doc_batch = doc_batch_builder.build();
            let num_docs = doc_batch.num_docs() as u64;

            let fetch_payload = FetchPayload {
                index_uid: self.index_uid.clone().into(),
                source_id: self.source_id.clone(),
                shard_id: self.shard_id.clone(),
                doc_batch: Some(doc_batch),
                from_position_inclusive: self.from_position_inclusive,
            };
            let fetch_message = FetchMessage::new_payload(fetch_payload);
            self.from_position_inclusive += num_docs;

            if self.fetch_message_tx.send(Ok(fetch_message)).await.is_err() {
                // The ingester was dropped.
                break;
            }
        }
    }
}

pub(super) type FetchStreamId = String;

pub(super) struct FetchStreamManager {
    node_id: NodeId,
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    fetch_stream_id_sequence: usize,
    fetch_streams: HashMap<FetchStreamId, FetchStream>,
}

impl FetchStreamManager {
    pub fn new(node_id: NodeId, mrecordlog: Arc<RwLock<MultiRecordLog>>) -> Self {
        Self {
            node_id,
            mrecordlog,
            fetch_stream_id_sequence: 0,
            fetch_streams: HashMap::new(),
        }
    }

    pub fn get_fetch_stream_mut(&mut self, fetch_stream_id: &str) -> Option<&mut FetchStream> {
        self.fetch_streams.get_mut(fetch_stream_id)
    }

    pub fn open_fetch_stream(
        &mut self,
        client_id: &str,
    ) -> (&mut FetchStream, ServiceStream<super::Result<FetchMessage>>) {
        let fetch_stream_id = format!("{}/{}", client_id, self.fetch_stream_id_sequence);
        self.fetch_stream_id_sequence += 1;

        let (fetch_message_tx, service_stream) = ServiceStream::new_bounded(5);

        let open_fetch_stream_response = OpenFetchStreamResponse {
            ingester_id: self.node_id.clone().into(),
            fetch_stream_id: fetch_stream_id.clone(),
        };
        let fetch_message = FetchMessage::new_response(open_fetch_stream_response);
        fetch_message_tx
            .try_send(Ok(fetch_message))
            .expect("Channel should be open and have enough capacity.");

        let fetch_stream = FetchStream {
            mrecordlog: self.mrecordlog.clone(),
            fetch_stream_id: fetch_stream_id.clone(),
            fetch_task_handles: HashMap::new(),
            fetch_message_tx,
        };
        let fetch_stream_ref = self
            .fetch_streams
            .entry(fetch_stream_id.clone())
            .or_insert_with(|| fetch_stream);

        (fetch_stream_ref, service_stream)
    }
}

pub(super) struct FetchStream {
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    fetch_stream_id: FetchStreamId,
    fetch_task_handles: HashMap<QueueId, FetchTaskHandle>,
    fetch_message_tx: mpsc::Sender<super::Result<FetchMessage>>,
}

impl fmt::Debug for FetchStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchStream")
            .field("fetch_stream_id", &self.fetch_stream_id)
            .finish()
    }
}

impl FetchStream {
    pub async fn subscribe(
        &mut self,
        subscription: SubscribeToShard,
        shard_status_rx: watch::Receiver<ShardStatus>,
    ) -> super::Result<()> {
        let mrecordlog = self.mrecordlog.clone();
        let queue_id = subscription.queue_id();
        let fetch_message_tx = self.fetch_message_tx.clone();
        let fetch_task_handle = FetchTask::spawn(
            mrecordlog,
            subscription,
            fetch_message_tx,
            shard_status_rx,
            FetchTask::DEFAULT_BATCH_NUM_BYTES,
        );
        if let Some(fetch_task_handle) = self.fetch_task_handles.insert(queue_id, fetch_task_handle)
        {
            fetch_task_handle.abort();
        }
        Ok(())
    }

    pub fn unsubscribe(&mut self, unsubscription: UnsubscribeFromShard) {
        let queue_id = unsubscription.queue_id();

        if let Some(fetch_task_handle) = self.fetch_task_handles.remove(&queue_id) {
            fetch_task_handle.abort();
        }
    }
}

// A handle to a fetch stream.
struct MultiFetchStreamHandle {
    fetch_stream_id: FetchStreamId,
    leader_id: NodeId,
    follower_id: Option<NodeId>,
}

/// Combines multiple fetch streams originating from different ingesters into a single one.
pub struct MultiFetchStream {
    client_id: String,
    ingester_pool: IngesterPool,
    fetch_streams: HashMap<QueueId, (NodeId, FetchStreamId)>,
    fetch_payload_rx: mpsc::Receiver<super::Result<FetchPayload>>,
    fetch_payload_tx: mpsc::Sender<super::Result<FetchPayload>>,
}

impl MultiFetchStream {
    pub fn new(client_id: String, ingester_pool: IngesterPool) -> Self {
        let (fetch_payload_tx, fetch_payload_rx) = mpsc::channel(5);
        Self {
            client_id,
            ingester_pool,
            fetch_streams: HashMap::new(),
            fetch_payload_rx,
            fetch_payload_tx,
        }
    }

    pub async fn subscribe(
        &mut self,
        leader_id: NodeId,
        _follower_id: Option<NodeId>,
        subscriptions: Vec<SubscribeToShard>,
    ) -> super::Result<()> {
        let subscription = subscriptions
            .first()
            .ok_or_else(|| IngestErrorV2::Internal("Subscription list is empty.".to_string()))?;
        let queue_id = subscription.queue_id();
        let entry = self.fetch_streams.entry(queue_id);

        match entry {
            Entry::Occupied(entry) => {
                let (node_id, fetch_stream_id) = entry.get();
                let update_fetch_stream_request = UpdateFetchStreamRequest {
                    fetch_stream_id: fetch_stream_id.clone(),
                    subscriptions,
                    unsubscriptions: Vec::new(),
                };
                self.ingester_pool
                    .get(node_id)
                    .await
                    .expect("FIXME")
                    .update_fetch_stream(update_fetch_stream_request)
                    .await?;
            }
            Entry::Vacant(entry) => {
                let open_fetch_stream_request = OpenFetchStreamRequest {
                    client_id: self.client_id.clone(),
                    subscriptions,
                };
                let mut fetch_stream = self
                    .ingester_pool
                    .get(&leader_id)
                    .await
                    .expect("FIXME")
                    .open_fetch_stream(open_fetch_stream_request)
                    .await?;

                let open_fetch_stream_response = fetch_stream
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_response()
                    .expect("");
                let fetch_stream_id = open_fetch_stream_response.fetch_stream_id;

                let fetch_payload_tx = self.fetch_payload_tx.clone();
                let future = async move {
                    while let Some(fetch_message_res) = fetch_stream.next().await {
                        let fetch_payload_res = fetch_message_res.map(|fetch_message| {
                            fetch_message
                                .into_payload()
                                .expect("The message should be a payload.")
                        });
                        if fetch_payload_tx.send(fetch_payload_res).await.is_err() {
                            // TODO: Attempt to stream from replica.
                            break;
                        }
                    }
                };
                tokio::spawn(future);
                entry.insert((leader_id, fetch_stream_id));
            }
        }
        Ok(())
    }
}

/// Combines multiple fetch streams into a single one.
// pub struct MultiFetchStream {
//     client_id: String,
//     ingester_pool: IngesterPool,
//     fetch_streams: HashMap<QueueId, (NodeId, FetchStreamId)>,
//     fetch_payload_rx: mpsc::Receiver<Result<FetchPayload, IngestErrorV2>>,
//     fetch_payload_tx: mpsc::Sender<Result<FetchPayload, IngestErrorV2>>,

// impl FetchStreamCombiner {
//     pub fn new(client_id: String, ingester_pool: IngesterPool) -> Self {
//         let fetch_streams = HashMap::new();
//         let (fetch_payload_tx, fetch_payload_rx) = mpsc::channel(5);
//         Self {
//             client_id,
//             ingester_pool,
//             fetch_streams,
//             fetch_payload_rx,
//             fetch_payload_tx,
//         }
//     }

//     pub async fn subscribe_to_shard(
//         &mut self,
//         leader_id: NodeId,
//         _follower_id: Option<NodeId>,
//         index_uid: IndexUid,
//         source_id: String,
//         shard_id: u64,
//         from_position_exclusive: Option<u64>,
//     ) -> super::Result<()> {
//         let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
//         if self.fetch_streams.contains_key(&queue_id) {
//             unimplemented!("Update fetch stream is not implemented.")
//         }
//         let node_id: NodeId = leader_id.into();
//         let mut ingester = self.ingester_pool.get(&node_id).await.expect("FIXME");
//         let subscribe_request = SubscribeToShard {
//             index_uid: index_uid.into(),
//             source_id,
//             shard_id,
//             from_position_exclusive,
//             to_position_inclusive: None,
//         };
//         let open_fetch_stream_request = OpenFetchStreamRequest {
//             client_id: self.client_id.clone(),
//             subscriptions: vec![subscribe_request],
//         };
//         let mut fetch_stream = ingester
//             .open_fetch_stream(open_fetch_stream_request)
//             .await?;

//         let fetch_response = fetch_stream
//             .next()
//             .await
//             .expect("FIXME")?
//             .into_response()
//             .expect("The first message should be a response.");

//         let fetch_stream_id = fetch_response.fetch_stream_id;
//         self.fetch_streams
//             .insert(queue_id, (node_id, fetch_stream_id));

//         let fetch_payload_tx = self.fetch_payload_tx.clone();
//         let future = async move {
//             while let Some(fetch_message_res) = fetch_stream.next().await {
//                 let fetch_payload_res = fetch_message_res.map(|fetch_message| {
//                     fetch_message
//                         .into_payload()
//                         .expect("The message should be a payload.")
//                 });
//                 if let Err(_error) = fetch_payload_tx.send(fetch_payload_res).await {
//                     panic!("FIXME");
//                     break;
//                 }
//             }
//         };
//         tokio::spawn(future);
//         Ok(())
//     }

//     pub async fn unsubscribe_from_shard(
//         &mut self,
//         _index_uid: &str,
//         _source_id: &str,
//         _shard_id: u64,
//     ) {
//         unimplemented!("Update fetch stream is not implemented.")
//     }

//     pub async fn truncate_shard(
//         &mut self,
//         _index_uid: &str,
//         _source_id: &str,
//         _shard_id: u64,
//         _position: u64,
//     ) {
//         unimplemented!("Update fetch stream is not implemented.")
//     }

// }
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use mrecordlog::MultiRecordLog;
    use tokio::time::timeout;

    use super::*;
    use crate::queue_id;

    #[tokio::test]
    async fn test_fetch_task() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let queue_id = queue_id(&index_uid, &source_id, 0);
        let (fetch_message_tx, mut fetch_message_rx) = mpsc::channel(5);
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let subscription = SubscribeToShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        let fetch_task_handle = FetchTask::spawn(
            mrecordlog.clone(),
            subscription,
            fetch_message_tx,
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        shard_status_tx.send(ShardStatus::Open(0.into())).unwrap();
        drop(mrecordlog_guard);

        let fetch_payload = timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 0);
        assert_eq!(fetch_payload.from_position_inclusive, 0);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000"
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-001"))
            .await
            .unwrap();
        drop(mrecordlog_guard);

        timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap_err();

        shard_status_tx.send(ShardStatus::Open(1.into())).unwrap();

        let fetch_payload = timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.from_position_inclusive, 1);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-001"
        );
        fetch_task_handle.abort();

        let join_error = fetch_task_handle.join_handle.await.unwrap_err();
        assert!(join_error.is_cancelled());
    }

    #[tokio::test]
    async fn test_fetch_task_to_position() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let queue_id = queue_id(&index_uid, &source_id, 0);
        let (fetch_message_tx, mut fetch_message_rx) = mpsc::channel(5);
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let subscription = SubscribeToShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: Some(0),
        };
        let fetch_task_handle = FetchTask::spawn(
            mrecordlog.clone(),
            subscription,
            fetch_message_tx,
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        shard_status_tx.send(ShardStatus::Open(0.into())).unwrap();
        drop(mrecordlog_guard);

        let fetch_payload = timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 0);
        assert_eq!(fetch_payload.from_position_inclusive, 0);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000"
        );
        fetch_task_handle.join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let queue_id = queue_id(&index_uid, &source_id, 0);
        let (fetch_message_tx, mut fetch_message_rx) = mpsc::channel(5);
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let subscription = SubscribeToShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: Some(2),
        };
        let fetch_task_handle = FetchTask::spawn(
            mrecordlog.clone(),
            subscription,
            fetch_message_tx,
            shard_status_rx,
            30,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-001"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-002"))
            .await
            .unwrap();
        shard_status_tx.send(ShardStatus::Open(2.into())).unwrap();
        drop(mrecordlog_guard);

        let fetch_payload = timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 0);
        assert_eq!(fetch_payload.from_position_inclusive, 0);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000test-doc-001"
        );
        let fetch_payload = timeout(Duration::from_millis(100), fetch_message_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 0);
        assert_eq!(fetch_payload.from_position_inclusive, 2);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-002"
        );
        fetch_task_handle.join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_stream() {
        let node_id: NodeId = "test-node".into();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let mut fetch_stream_manager = FetchStreamManager::new(node_id.clone(), mrecordlog.clone());

        let client_id = "test-client".to_string();
        let (fetch_stream, mut service_stream) = fetch_stream_manager.open_fetch_stream(&client_id);
        let fetch_stream_id = fetch_stream.fetch_stream_id.clone();

        let open_fetch_stream_response = timeout(Duration::from_millis(100), service_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_response()
            .unwrap();
        assert_eq!(open_fetch_stream_response.ingester_id, node_id);
        assert_eq!(open_fetch_stream_response.fetch_stream_id, fetch_stream_id,);

        let mut mrecordlog_guard = mrecordlog.write().await;

        let queue_id_00 = queue_id(&index_uid, &source_id, 0);
        mrecordlog_guard.create_queue(&queue_id_00).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id_00, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id_00, None, Bytes::from_static(b"test-doc-001"))
            .await
            .unwrap();
        let (shard_status_tx_00, shard_status_rx_00) = watch::channel(ShardStatus::default());

        let subscription = SubscribeToShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        fetch_stream
            .subscribe(subscription, shard_status_rx_00)
            .await
            .unwrap();

        let queue_id_01 = queue_id(&index_uid, &source_id, 1);
        mrecordlog_guard.create_queue(&queue_id_01).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id_01, None, Bytes::from_static(b"test-doc-010"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id_01, None, Bytes::from_static(b"test-doc-011"))
            .await
            .unwrap();
        let (shard_status_tx_01, shard_status_rx_01) = watch::channel(ShardStatus::default());

        let subscription = SubscribeToShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 1,
            from_position_exclusive: Some(0),
            to_position_inclusive: None,
        };
        fetch_stream
            .subscribe(subscription, shard_status_rx_01)
            .await
            .unwrap();

        drop(mrecordlog_guard);

        shard_status_tx_00
            .send(ShardStatus::Open(1.into()))
            .unwrap();
        let fetch_payload = timeout(Duration::from_millis(100), service_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 0);
        assert_eq!(fetch_payload.from_position_inclusive, 0);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000test-doc-001"
        );
        shard_status_tx_01
            .send(ShardStatus::Open(1.into()))
            .unwrap();
        let fetch_payload = timeout(Duration::from_millis(100), service_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 1);
        assert_eq!(fetch_payload.from_position_inclusive, 1);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-011"
        );

        let mut mrecordlog_guard = mrecordlog.write().await;
        mrecordlog_guard
            .append_record(&queue_id_00, None, Bytes::from_static(b"test-doc-002"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id_01, None, Bytes::from_static(b"test-doc-012"))
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let unsubscription = UnsubscribeFromShard {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
        };
        fetch_stream.unsubscribe(unsubscription);

        shard_status_tx_00
            .send(ShardStatus::Open(2.into()))
            .unwrap();
        shard_status_tx_01
            .send(ShardStatus::Open(2.into()))
            .unwrap();

        let fetch_payload = timeout(Duration::from_millis(100), service_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_payload()
            .unwrap();
        assert_eq!(fetch_payload.index_uid, "test-index:0");
        assert_eq!(fetch_payload.source_id, "test-source");
        assert_eq!(fetch_payload.shard_id, 1);
        assert_eq!(fetch_payload.from_position_inclusive, 2);
        assert_eq!(fetch_payload.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_payload.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-012"
        );
        timeout(Duration::from_millis(100), service_stream.next())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {}
}
