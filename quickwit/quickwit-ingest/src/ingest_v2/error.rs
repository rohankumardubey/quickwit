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

use quickwit_proto::{IndexUid, ServiceError, ServiceErrorCode};
use quickwit_types::{NodeId, ShardId, SourceId};

#[derive(Debug, Clone, thiserror::Error)]
pub enum IngestErrorV2 {
    #[error("An internal error occurred: {0}.")]
    Internal(String),
    #[error("Failed to connect to ingester `{ingester_id}`.")]
    IngesterUnavailable { ingester_id: NodeId },
    #[error(
        "Ingest service is currently unavailable with {num_ingesters} in the cluster and a \
         replication factor of {replication_factor}."
    )]
    ServiceUnavailable {
        num_ingesters: usize,
        replication_factor: usize,
    },
    // #[error("Could not find shard.")]
    // ShardNotFound {
    //     index_uid: IndexUid,
    //     source_id: SourceId,
    //     shard_id: ShardId,
    // },
    #[error("Failed to open or write to shard.")]
    ShardUnavailable {
        leader_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
    },
}

impl From<IngestErrorV2> for tonic::Status {
    fn from(error: IngestErrorV2) -> tonic::Status {
        let code = match &error {
            IngestErrorV2::Internal(_) => tonic::Code::Internal,
            IngestErrorV2::IngesterUnavailable { .. } => tonic::Code::Unavailable,
            IngestErrorV2::ShardUnavailable { .. } => tonic::Code::Unavailable,
            IngestErrorV2::ServiceUnavailable { .. } => tonic::Code::Unavailable,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

impl From<tonic::Status> for IngestErrorV2 {
    fn from(status: tonic::Status) -> Self {
        // TODO: Use status.details() #2859.
        match status.code() {
            // FIXME
            _ => IngestErrorV2::Internal(status.message().to_string()),
        }
    }
}

impl ServiceError for IngestErrorV2 {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal { .. } => ServiceErrorCode::Internal,
            Self::IngesterUnavailable { .. } => ServiceErrorCode::Unavailable,
            Self::ShardUnavailable { .. } => ServiceErrorCode::Unavailable,
            Self::ServiceUnavailable { .. } => ServiceErrorCode::Unavailable,
        }
    }
}
