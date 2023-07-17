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

use std::fmt;

use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

use crate::checkpoint::IncompatibleCheckpointDelta;

/// The different kinds of objects managed by the metastore.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntityKind {
    /// Index entity.
    Index {
        /// Index ID.
        index_id: String,
    },
    /// Source entity.
    Source {
        /// Source ID.
        source_id: String,
    },
    /// Shard entity.
    Shard {
        /// Shard UID: <index_uid>/<source_id>/<shard_id>
        queue_id: String,
    },
    /// Split entity.
    Split {
        /// Split ID.
        split_id: String,
    },
    /// A set of splits.
    Splits {
        /// Comma-separated split IDs.
        split_ids: Vec<String>,
    },
}

impl EntityKind {
    /// Returns the entity ID, for instance the index ID for an index or the source ID for a source.
    pub fn entity_id(&self) -> String {
        match self {
            EntityKind::Index { index_id } => index_id.clone(),
            EntityKind::Shard { queue_id } => queue_id.clone(),
            EntityKind::Source { source_id } => source_id.clone(),
            EntityKind::Split { split_id } => split_id.clone(),
            EntityKind::Splits { split_ids } => split_ids.join(","),
        }
    }
}

impl fmt::Display for EntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityKind::Index { index_id } => write!(f, "Index `{}`", index_id),
            EntityKind::Shard { queue_id: shard_id } => write!(f, "Shard `{}`", shard_id),
            EntityKind::Source { source_id } => write!(f, "Source `{}`", source_id),
            EntityKind::Split { split_id } => write!(f, "Split `{}`", split_id),
            EntityKind::Splits { split_ids } => write!(f, "Splits `{}`", split_ids.join(", ")),
        }
    }
}

/// Metastore error variants.
#[allow(missing_docs)]
#[derive(Clone, Debug, ThisError, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetastoreError {
    #[error("Connection error: `{message}`.")]
    ConnectionError { message: String },

    #[error("Invalid argument: `{0}`")]
    InvalidArgument(String),

    #[error("{0} not found.")]
    NotFound(EntityKind),

    #[error("{0} already exists.")]
    AlreadyExists(EntityKind),

    #[error("Access forbidden: `{message}`.")]
    Forbidden { message: String },

    /// Any generic internal error.
    /// The message can be helpful to users, but the detail of the error
    /// are judged uncoverable and not useful for error handling.
    #[error("Internal error: `{message}` Cause: `{cause}`.")]
    InternalError { message: String, cause: String },

    #[error("Failed to deserialize index metadata: `{message}`")]
    InvalidManifest { message: String },

    #[error("IO error: `{message}`")]
    Io { message: String },

    #[error("Splits `{split_ids:?}` do not exist.")]
    SplitsDoNotExist { split_ids: Vec<String> },

    #[error("Splits `{split_ids:?}` are not in a deletable state.")]
    SplitsNotDeletable { split_ids: Vec<String> },

    #[error("Splits `{split_ids:?}` are not staged.")]
    SplitsNotStaged { split_ids: Vec<String> },

    #[error("Publish checkpoint delta overlaps with current checkpoint: {0:?}.")]
    IncompatibleCheckpointDelta(#[from] IncompatibleCheckpointDelta),

    #[error("Database error: `{message}`.")]
    DbError { message: String },

    #[error("Failed to deserialize `{struct_name}` from JSON: `{message}`.")]
    JsonDeserializeError {
        struct_name: String,
        message: String,
    },

    #[error("Failed to serialize `{struct_name}` to JSON: `{message}`.")]
    JsonSerializeError {
        struct_name: String,
        message: String,
    },
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for MetastoreError {
    fn from(error: sqlx::Error) -> Self {
        MetastoreError::DbError {
            message: error.to_string(),
        }
    }
}

impl From<MetastoreError> for quickwit_proto::tonic::Status {
    fn from(metastore_error: MetastoreError) -> Self {
        let grpc_code = metastore_error.status_code().to_grpc_status_code();
        let error_msg = serde_json::to_string(&metastore_error)
            .unwrap_or_else(|_| format!("Raw metastore error: {metastore_error}"));
        quickwit_proto::tonic::Status::new(grpc_code, error_msg)
    }
}

impl ServiceError for MetastoreError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::AlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::ConnectionError { .. } => ServiceErrorCode::Internal,
            Self::DbError { .. } => ServiceErrorCode::Internal,
            Self::Forbidden { .. } => ServiceErrorCode::MethodNotAllowed,
            Self::IncompatibleCheckpointDelta(_) => ServiceErrorCode::BadRequest,
            Self::InternalError { .. } => ServiceErrorCode::Internal,
            Self::InvalidArgument { .. } => ServiceErrorCode::Internal,
            Self::InvalidManifest { .. } => ServiceErrorCode::Internal,
            Self::Io { .. } => ServiceErrorCode::Internal,
            Self::JsonDeserializeError { .. } => ServiceErrorCode::Internal,
            Self::JsonSerializeError { .. } => ServiceErrorCode::Internal,
            Self::NotFound { .. } => ServiceErrorCode::NotFound,
            Self::SplitsDoNotExist { .. } => ServiceErrorCode::BadRequest,
            Self::SplitsNotDeletable { .. } => ServiceErrorCode::BadRequest,
            Self::SplitsNotStaged { .. } => ServiceErrorCode::BadRequest,
        }
    }
}

/// Generic Result type for metastore operations.
pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Generic Storage Resolver Error.
#[derive(Debug, ThisError)]
pub enum MetastoreResolverError {
    /// The metastore config is invalid.
    #[error("Invalid metastore config: `{0}`")]
    InvalidConfig(String),

    /// The URI does not contain sufficient information to connect to the metastore.
    #[error("Invalid metastore URI: `{0}`")]
    InvalidUri(String),

    /// The requested backend is unsupported or unavailable.
    #[error("Unsupported metastore backend: `{0}`")]
    UnsupportedBackend(String),

    /// The config and URI are valid, and are meant to be handled by this resolver, but the
    /// resolver failed to actually connect to the backend. e.g. connection error, credentials
    /// error, incompatible version, internal error in a third party, etc.
    #[error("Failed to connect to metastore: `{0}`")]
    FailedToOpenMetastore(MetastoreError),
}
