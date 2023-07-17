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

use std::collections::hash_map::{Entry, IntoValues};
use std::collections::HashMap;
use std::fmt;

use itertools::Either;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    CloseShardsFailure, CloseShardsSubrequest, CloseShardsSuccess, DeleteShardsSubrequest,
    ListShardsSubrequest, ListShardsSubresponse, OpenShardsSubrequest, OpenShardsSubresponse,
};
use quickwit_proto::tonic::Code;
use quickwit_proto::types::ShardId;
use quickwit_proto::IndexUid;
use tracing::info;

use crate::checkpoint::{PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta};
use crate::error::EntityKind;
use crate::file_backed_metastore::MutationOccurred;
use crate::{MetastoreError, MetastoreResult};

#[derive(Clone)]
pub(super) struct Shards {
    index_uid: IndexUid,
    source_id: String,
    checkpoint: SourceCheckpoint,
    shards: HashMap<ShardId, Shard>,
    shard_id_sequence: ShardId,
}

impl fmt::Debug for Shards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shards")
            .field("num_shards", &self.shards.len())
            .finish()
    }
}

impl Shards {
    pub(super) fn new(index_uid: IndexUid, source_id: String) -> Self {
        Self {
            index_uid,
            source_id,
            checkpoint: SourceCheckpoint::default(),
            shards: HashMap::new(),
            shard_id_sequence: 0,
        }
    }

    fn get_shard(&self, shard_id: ShardId) -> MetastoreResult<&Shard> {
        self.shards.get(&shard_id).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Shard {
                queue_id: "FIXME".to_string(),
            })
        })
    }

    fn get_shard_mut(&mut self, shard_id: ShardId) -> MetastoreResult<&mut Shard> {
        self.shards.get_mut(&shard_id).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Shard {
                queue_id: "FIXME".to_string(),
            })
        })
    }

    pub(super) fn add_shard(&mut self, shard: Shard) {
        assert_eq!(self.index_uid, shard.index_uid);
        assert_eq!(self.source_id, shard.source_id);

        self.checkpoint.add_partition(
            PartitionId::from(shard.shard_id),
            Position::from(shard.publish_position_inclusive.clone()),
        );
        self.shards.insert(shard.shard_id, shard);
    }

    pub(super) fn into_shards(self) -> IntoValues<ShardId, Shard> {
        self.shards.into_values()
    }

    pub(super) fn open_shards(
        &mut self,
        subrequest: OpenShardsSubrequest,
    ) -> MetastoreResult<MutationOccurred<OpenShardsSubresponse>> {
        let mut mutation_occurred = false;

        let mut shard_ids = subrequest.shard_ids;
        shard_ids.sort_unstable();

        let mut open_shards = Vec::with_capacity(shard_ids.len());

        for shard_id in shard_ids {
            let entry = self.shards.entry(shard_id);
            let shard = match entry {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    if shard_id < self.shard_id_sequence {
                        continue;
                    }
                    mutation_occurred = true;
                    self.shard_id_sequence += 1;

                    let shard = Shard {
                        index_uid: self.index_uid.clone().into(),
                        source_id: self.source_id.clone(),
                        shard_id,
                        leader_id: subrequest.leader_id.clone(),
                        follower_id: subrequest.follower_id.clone(),
                        ..Default::default()
                    };
                    entry.insert(shard.clone());
                    info!(
                        index_id=%self.index_uid.index_id(),
                        source_id=%self.source_id,
                        shard_id=%shard.shard_id,
                        leader_id=%shard.leader_id,
                        follower_id=?shard.follower_id,
                        "Opened shard."
                    );
                    shard
                }
            };
            open_shards.push(shard);
        }
        let response = OpenShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            open_shards,
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
    }

    pub(super) fn close_shards(
        &mut self,
        subrequest: CloseShardsSubrequest,
    ) -> MetastoreResult<MutationOccurred<Either<CloseShardsSuccess, CloseShardsFailure>>> {
        let Some(shard) = self.shards.get_mut(&subrequest.shard_id) else {
            let failure = CloseShardsFailure {
                index_uid: subrequest.index_uid,
                source_id: subrequest.source_id,
                shard_id: subrequest.shard_id,
                error_code: Code::NotFound as u32,
                error_message: "Shard not found.".to_string(),
            };
            return Ok(MutationOccurred::No(Either::Right(failure)));
        };
        match subrequest.shard_state() {
            ShardState::Closing => {
                shard.shard_state = ShardState::Closing as i32;
                info!(
                    index_id=%self.index_uid.index_id(),
                    source_id=%shard.source_id,
                    shard_id=%shard.shard_id,
                    "Closing shard.",
                );
            }
            ShardState::Closed => {
                shard.shard_state = ShardState::Closed as i32;
                shard.replication_position_inclusive = shard
                    .replication_position_inclusive
                    .min(subrequest.replication_position_inclusive);
                info!(
                    index_id=%self.index_uid.index_id(),
                    source_id=%shard.source_id,
                    shard_id=%shard.shard_id,
                    "Closed shard.",
                );
            }
            other => {
                let error_message = format!(
                    "Invalid `shard_state` argument: expected `Closing` or `Closed` state, got \
                     `{other:?}`.",
                );
                let failure = CloseShardsFailure {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    error_code: Code::InvalidArgument as u32,
                    error_message,
                };
                return Ok(MutationOccurred::No(Either::Right(failure)));
            }
        }
        let success = CloseShardsSuccess {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shard_id: subrequest.shard_id,
        };
        Ok(MutationOccurred::Yes(Either::Left(success)))
    }

    pub(super) fn delete_shards(
        &mut self,
        subrequest: DeleteShardsSubrequest,
        force: bool,
    ) -> MetastoreResult<MutationOccurred<()>> {
        let mut mutation_occurred = false;
        for shard_id in subrequest.shard_ids {
            if let Entry::Occupied(entry) = self.shards.entry(shard_id) {
                let shard = entry.get();
                if force || shard.is_deletable() {
                    mutation_occurred = true;
                    info!(
                        index_id=%self.index_uid.index_id(),
                        source_id=%self.source_id,
                        shard_id=%shard.shard_id,
                        "Deleted shard.",
                    );
                    entry.remove();
                    continue;
                }
                return Err(MetastoreError::InvalidArgument(format!(
                    "Shard `{}` is not deletable.",
                    shard_id
                )));
            }
        }
        if mutation_occurred {
            Ok(MutationOccurred::Yes(()))
        } else {
            Ok(MutationOccurred::No(()))
        }
    }

    pub(super) fn list_shards(
        &self,
        subrequest: ListShardsSubrequest,
    ) -> MetastoreResult<MutationOccurred<ListShardsSubresponse>> {
        let shards = self._list_shards(subrequest.shard_state);
        let response = ListShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shards,
        };
        Ok(MutationOccurred::No(response))
    }

    pub(super) fn try_apply_delta(
        &mut self,
        checkpoint_delta: SourceCheckpointDelta,
        publish_token: String,
    ) -> MetastoreResult<MutationOccurred<()>> {
        if checkpoint_delta.is_empty() {
            return Ok(MutationOccurred::No(()));
        }
        self.checkpoint.check_compatibility(&checkpoint_delta)?;

        let mut shard_ids = Vec::with_capacity(checkpoint_delta.num_partitions());

        for (partition_id, partition_delta) in checkpoint_delta.iter() {
            let shard_id = partition_id.as_u64().expect("");
            let shard = self.get_shard(shard_id)?;

            if shard.publish_token() != publish_token {
                return Err(MetastoreError::InvalidArgument("FIXME".to_string()));
            }
            let publish_position_inclusive =
                partition_delta.to_position_inclusive.as_str().to_string();
            shard_ids.push((shard_id, publish_position_inclusive))
        }
        self.checkpoint.try_apply_delta_unchecked(checkpoint_delta);

        for (shard_id, publish_position_inclusive) in shard_ids {
            let shard = self.get_shard_mut(shard_id)?;
            shard.publish_position_inclusive = publish_position_inclusive
        }
        Ok(MutationOccurred::Yes(()))
    }

    fn _list_shards(&self, shard_state: Option<i32>) -> Vec<Shard> {
        if let Some(shard_state) = shard_state {
            self.shards
                .values()
                .filter(|shard| shard.shard_state == shard_state)
                .cloned()
                .collect()
        } else {
            self.shards.values().cloned().collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let mut shards = Shards::new(index_uid.clone(), source_id.clone());

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: None,
            shard_ids: Vec::new(),
        };
        let MutationOccurred::No(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 0);
        assert!(shards.shards.is_empty());

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: None,
            shard_ids: vec![0],
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shards(subrequest.clone()).unwrap()
        else {
            panic!("Expected `MutationOccured::Yes`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 1);

        let shard = &subresponse.shards[0];
        assert_eq!(shard.index_uid, index_uid.as_str());
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id, 0);
        assert_eq!(shard.shard_state, 0);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id, None);
        assert_eq!(shard.replication_position_inclusive, None);
        assert_eq!(shard.publish_position_inclusive, "");

        assert_eq!(shards.shards.get(&0).unwrap(), shard);

        let MutationOccurred::No(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.shards.len(), 1);

        let shard = &subresponse.shards[0];
        assert_eq!(shards.shards.get(&0).unwrap(), shard);

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: Some("follower_id".to_string()),
            shard_ids: vec![0, 1],
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 2);

        let shard = &subresponse.shards[0];
        assert_eq!(shard.index_uid, index_uid.as_str());
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id, 0);
        assert_eq!(shard.shard_state, 0);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id, None);
        assert_eq!(shard.replication_position_inclusive, None);
        assert_eq!(shard.publish_position_inclusive, "");

        assert_eq!(shards.shards.get(&0).unwrap(), shard);

        let shard = &subresponse.shards[1];
        assert_eq!(shard.index_uid, index_uid.as_str());
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id, 1);
        assert_eq!(shard.shard_state, 0);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id.as_ref().unwrap(), "follower_id");
        assert_eq!(shard.replication_position_inclusive, None);
        assert_eq!(shard.publish_position_inclusive, "");

        assert_eq!(shards.shards.get(&1).unwrap(), shard);

        shards.shards.remove(&1);

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: Some("follower_id".to_string()),
            shard_ids: vec![0, 1, 2],
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 2);

        let shard = &subresponse.shards[0];
        assert_eq!(shard.shard_id, 0);

        let shard = &subresponse.shards[1];
        assert_eq!(shard.shard_id, 2);
    }

    #[test]
    fn test_close_shard() {}

    #[test]
    fn test_list_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let mut shards = Shards::new(index_uid.clone(), source_id.clone());

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let MutationOccurred::No(subresponse) = shards.list_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 0);

        let shard_0 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        shards.shards.insert(0, shard_0);
        shards.shards.insert(1, shard_1);

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let MutationOccurred::No(mut subresponse) = shards.list_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        subresponse
            .shards
            .sort_unstable_by_key(|shard| shard.shard_id);
        assert_eq!(subresponse.shards.len(), 2);
        assert_eq!(subresponse.shards[0].shard_id, 0);
        assert_eq!(subresponse.shards[1].shard_id, 1);

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_state: Some(ShardState::Closed as i32),
        };
        let MutationOccurred::No(subresponse) = shards.list_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.shards.len(), 1);
        assert_eq!(subresponse.shards[0].shard_id, 1);
    }
}
