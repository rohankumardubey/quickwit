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
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_ingest::{
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, IngestErrorV2,
    IngestMetastoreService, ListShardsRequest, ListShardsResponse, RenewShardLeasesRequest,
    RenewShardLeasesResponse,
};

use crate::{Metastore, MetastoreError};

/// Exposes a [`Metastore`] as an [`IngestMetastoreService`].
#[derive(Clone)]
pub struct IngestMetastoreAdapter {
    metastore: Arc<dyn Metastore>,
}

impl IngestMetastoreAdapter {
    /// Creates a new [`IngestMetastoreAdapter`] from a [`Metastore`] instance.
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self { metastore }
    }
}

impl fmt::Debug for IngestMetastoreAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestMetastoreService").finish()
    }
}

#[async_trait]
impl IngestMetastoreService for IngestMetastoreAdapter {
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> Result<GetOrCreateOpenShardsResponse, IngestErrorV2> {
        self.metastore
            .get_or_create_open_shards(request)
            .await
            .map_err(|error| error.into())
    }

    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> Result<ListShardsResponse, IngestErrorV2> {
        self.metastore
            .list_shards(request)
            .await
            .map_err(|error| error.into())
    }

    async fn renew_shard_leases(
        &mut self,
        request: RenewShardLeasesRequest,
    ) -> Result<RenewShardLeasesResponse, IngestErrorV2> {
        self.metastore
            .renew_shard_leases(request)
            .await
            .map_err(|error| error.into())
    }
}

impl From<MetastoreError> for IngestErrorV2 {
    fn from(val: MetastoreError) -> Self {
        // TODO: add new variants to IngestErrorV2 to cover all cases.
        // match self {

        // }
        IngestErrorV2::Internal(val.to_string())
    }
}
