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

use std::collections::hash_map::IntoValues;
use std::collections::HashMap;
use std::fmt;

use quickwit_ingest::{
    GetOrCreateOpenShardsSubrequest, GetOrCreateOpenShardsSubresponse, LeaseRenewalOutcome,
    LeaseRenewalResult, ListShardsSubrequest, ListShardsSubresponse, RenewShardLeasesSubrequest,
    RenewShardLeasesSubresponse, Shard, ShardState,
};
use quickwit_proto::IndexUid;
use time::OffsetDateTime;
use tracing::info;

use crate::checkpoint::{PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta};
use crate::error::EntityKind;
use crate::file_backed_metastore::MutationOccurred;
use crate::{MetastoreError, MetastoreResult};

type ShardId = u64;

#[derive(Clone)]
pub(super) struct Shards {
    index_uid: IndexUid,
    source_id: String,
    checkpoint: SourceCheckpoint,
    shards: HashMap<ShardId, Shard>,
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
            shards: HashMap::new(),
            checkpoint: SourceCheckpoint::default(),
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

    fn create_shard(&mut self, leader_id: String, follower_id: Option<String>) -> Shard {
        let shard = Shard {
            index_uid: self.index_uid.clone().into(),
            source_id: self.source_id.clone(),
            leader_id,
            follower_id,
            start_hash_key_inclusive: 0,
            end_hash_key_exclusive: u64::MAX,
            ..Default::default() // FIXME
        };
        self.checkpoint = SourceCheckpoint::default();
        self.shards.insert(shard.shard_id, shard.clone());

        info!(
            index_id=%self.index_uid.index_id(),
            source_id=%self.source_id,
            shard_id=%shard.shard_id,
            leader_id=%shard.leader_id,
            follower_id=?shard.follower_id,
            "Creating shard."
        );
        shard
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

    pub(super) fn get_or_create_open_shards(
        &mut self,
        subrequest: GetOrCreateOpenShardsSubrequest,
    ) -> MutationOccurred<GetOrCreateOpenShardsSubresponse> {
        let mut mutation_occurred = false;
        let mut shards = self._list_shards(Some(ShardState::Open as i32));

        if shards.is_empty() {
            mutation_occurred = true;

            let shard = self.create_shard(subrequest.leader_id, subrequest.follower_id);
            shards.push(shard);
        }
        // TODO: Check for closed shards.
        //
        let response = GetOrCreateOpenShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shards,
        };
        if mutation_occurred {
            MutationOccurred::Yes(response)
        } else {
            MutationOccurred::No(response)
        }
    }

    pub(super) fn list_shards(&self, subrequest: ListShardsSubrequest) -> ListShardsSubresponse {
        let shards = self._list_shards(subrequest.shard_state);

        ListShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shards,
        }
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

    pub(super) fn renew_shard_leases(
        &mut self,
        subrequest: RenewShardLeasesSubrequest,
    ) -> MetastoreResult<MutationOccurred<RenewShardLeasesSubresponse>> {
        let mut mutation_occurred = false;

        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let now_millis = (now / 1_000_000) as u64;
        let lease_expiration_timestamp_millis = now_millis + 20_000; // now + 20 seconds

        let mut lease_renewal_results = Vec::with_capacity(subrequest.shard_ids.len());

        for shard_id in subrequest.shard_ids {
            let shard = self.get_shard_mut(shard_id)?;

            if let Some(lessee_id) = &shard.lessee_id {
                if *lessee_id != subrequest.lessee_id {
                    continue;
                }
                mutation_occurred = true;
                shard.lease_expiration_timestamp_millis = lease_expiration_timestamp_millis;

                let renewal_result = LeaseRenewalResult {
                    shard_id,
                    renewal_outcome: LeaseRenewalOutcome::Renewed as i32,
                    shard: None,
                    lease_expiration_timestamp_millis: Some(lease_expiration_timestamp_millis),
                };
                lease_renewal_results.push(renewal_result);
            };
        }
        for shard in self.shards.values_mut() {
            if shard.is_lease_available(now_millis) {
                mutation_occurred = true;
                shard.lessee_id = Some(subrequest.lessee_id.clone());
                shard.lease_expiration_timestamp_millis = lease_expiration_timestamp_millis;

                let renewal_result = LeaseRenewalResult {
                    shard_id: shard.shard_id,
                    renewal_outcome: LeaseRenewalOutcome::Acquired as i32,
                    shard: Some(shard.clone()),
                    lease_expiration_timestamp_millis: Some(lease_expiration_timestamp_millis),
                };
                lease_renewal_results.push(renewal_result);
                // Bound the number of leases that can be acquired to at most one at a time. TODO:
                // Make this configurable.
                break;
            }
        }
        let response = RenewShardLeasesSubresponse {
            lessee_id: subrequest.lessee_id,
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            lease_renewal_results,
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
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

            if shard.lessee_id() != publish_token {
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
}
