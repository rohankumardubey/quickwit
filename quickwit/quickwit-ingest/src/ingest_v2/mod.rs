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

mod codegen;
mod error;
mod fetch;
mod ingester;
mod replication;
mod router;
mod shard_table;
#[cfg(test)]
mod test_utils;

use bytes::{BufMut, Bytes, BytesMut};
use quickwit_common::tower::Pool;
use quickwit_types::NodeId;

pub use self::codegen::*;
pub use self::error::IngestErrorV2;
pub use self::fetch::MultiFetchStream;
pub use self::ingester::Ingester;
use self::replication::ReplicationResponse;
pub use self::router::IngestRouter;

pub type Result<T> = std::result::Result<T, IngestErrorV2>;

pub type IngesterPool = Pool<NodeId, IngesterServiceClient>;

pub fn queue_id(index_uid: &str, source_id: &str, shard_id: u64) -> String {
    format!("{}/{}/{}", index_uid, source_id, shard_id)
}

impl DocBatchV2 {
    pub fn docs(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_lengths.iter().scan(0, |start_offset, doc_length| {
            let start = *start_offset;
            let end = start + *doc_length as usize;
            *start_offset = end;
            Some(self.doc_buffer.slice(start..end))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    fn num_bytes(&self) -> usize {
        self.doc_buffer.len()
    }

    fn num_docs(&self) -> usize {
        self.doc_lengths.len()
    }
}

#[derive(Default)]
pub(crate) struct DocBatchBuilderV2 {
    doc_buffer: BytesMut,
    doc_lengths: Vec<u32>,
}

impl DocBatchBuilderV2 {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            doc_buffer: BytesMut::with_capacity(capacity),
            doc_lengths: Vec::new(),
        }
    }

    pub fn add_doc(&mut self, doc: &[u8]) {
        self.doc_lengths.push(doc.len() as u32);
        self.doc_buffer.put(doc);
    }

    pub fn build(self) -> DocBatchV2 {
        DocBatchV2 {
            doc_buffer: self.doc_buffer.freeze(),
            doc_lengths: self.doc_lengths,
        }
    }

    pub fn capacity(&self) -> usize {
        self.doc_buffer.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    fn num_bytes(&self) -> usize {
        self.doc_buffer.len()
    }
}

impl FetchMessage {
    fn into_response(self) -> Option<OpenFetchStreamResponse> {
        match self.message {
            Some(fetch_message::Message::Response(fetch_response)) => Some(fetch_response),
            _ => None,
        }
    }

    fn into_payload(self) -> Option<FetchPayload> {
        match self.message {
            Some(fetch_message::Message::Payload(fetch_payload)) => Some(fetch_payload),
            _ => None,
        }
    }

    fn new_response(response: OpenFetchStreamResponse) -> Self {
        Self {
            message: Some(fetch_message::Message::Response(response)),
        }
    }

    fn new_payload(payload: FetchPayload) -> Self {
        Self {
            message: Some(fetch_message::Message::Payload(payload)),
        }
    }
}

impl FetchPayload {
    pub fn docs(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_batch.iter().flat_map(|doc_batch| doc_batch.docs())
    }

    pub fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl IngestSubrequest {
    pub fn docs(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_batch.iter().flat_map(|doc_batch| doc_batch.docs())
    }
}

impl Shard {
    pub fn is_open(&self) -> bool {
        self.shard_state == ShardState::Open as i32
    }

    pub fn is_closed(&self) -> bool {
        self.shard_state == ShardState::Closed as i32
    }

    /// Returns true if the shard has no lessee or the lease has expired.
    pub fn is_lease_available(&self, now_millis: u64) -> bool {
        self.lessee_id.is_none() || self.lease_expiration_timestamp_millis < now_millis
    }

    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl ShardState {
    pub fn is_closed(&self) -> bool {
        *self == ShardState::Closed
    }
}

impl PersistSubrequest {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl PersistSuccess {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl SynReplicationMessage {
    fn into_open_request(self) -> Option<OpenReplicationStreamRequest> {
        match self.message {
            Some(syn_replication_message::Message::OpenRequest(open_request)) => Some(open_request),
            _ => None,
        }
    }

    fn into_replicate_request(self) -> Option<ReplicateRequest> {
        match self.message {
            Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) => {
                Some(replicate_request)
            }
            _ => None,
        }
    }

    fn new_open_request(open_request: OpenReplicationStreamRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::OpenRequest(open_request)),
        }
    }

    fn new_replicate_request(replicate_request: ReplicateRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::ReplicateRequest(
                replicate_request,
            )),
        }
    }

    fn new_truncate_request(truncate_request: TruncateRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::TruncateRequest(
                truncate_request,
            )),
        }
    }
}

impl AckReplicationMessage {
    fn into_open_response(self) -> Option<OpenReplicationStreamResponse> {
        match self.message {
            Some(ack_replication_message::Message::OpenResponse(open_response)) => {
                Some(open_response)
            }
            _ => None,
        }
    }

    fn into_replication_response(self) -> Option<ReplicationResponse> {
        match self.message {
            Some(ack_replication_message::Message::ReplicateResponse(replicate_response)) => {
                Some(ReplicationResponse::Replicate(replicate_response))
            }
            Some(ack_replication_message::Message::TruncateResponse(truncate_response)) => {
                Some(ReplicationResponse::Truncate(truncate_response))
            }
            _ => None,
        }
    }

    fn new_open_response(open_response: OpenReplicationStreamResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::OpenResponse(
                open_response,
            )),
        }
    }

    fn new_replicate_response(replicate_response: ReplicateResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::ReplicateResponse(
                replicate_response,
            )),
        }
    }

    fn new_truncate_response(truncate_response: TruncateResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::TruncateResponse(
                truncate_response,
            )),
        }
    }
}

impl ReplicateSubrequest {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    fn to_position_inclusive(&self) -> Option<u64> {
        let Some(doc_batch) = &self.doc_batch else {
            return self.from_position_exclusive;
        };
        let num_docs = doc_batch.num_docs() as u64;

        match self.from_position_exclusive {
            Some(from_position_exclusive) => Some(from_position_exclusive + num_docs),
            None => Some(num_docs - 1),
        }
    }
}

impl SubscribeToShard {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl UnsubscribeFromShard {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl TruncateSubrequest {
    fn queue_id(&self) -> String {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicate_subrequest_to_position_inclusive() {
        let mut subrequest = ReplicateSubrequest {
            index_uid: "test-index".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 0,
            from_position_exclusive: None,
            doc_batch: None,
        };
        assert_eq!(subrequest.to_position_inclusive(), None);

        subrequest.from_position_exclusive = Some(0);
        assert_eq!(subrequest.to_position_inclusive(), Some(0));

        subrequest.doc_batch = Some(DocBatchV2 {
            doc_buffer: Bytes::from_static(b"test-doc"),
            doc_lengths: vec![8],
        });
        assert_eq!(subrequest.to_position_inclusive(), Some(1));

        subrequest.from_position_exclusive = None;
        assert_eq!(subrequest.to_position_inclusive(), Some(0));
    }
}
