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

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_proto::indexing::ApplyIndexingPlanRequest;

pub struct IndexingControllerAgent {}

#[async_trait]
impl Actor for IndexingControllerAgent {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn name(&self) -> String {
        "IndexingControllerAgent".to_string()
    }
}

#[async_trait]
impl Handler<ApplyIndexingPlanRequest> for IndexingControllerAgent {
    type Reply = ();

    async fn handle(
        &mut self,
        request: ApplyIndexingPlanRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        println!("Received indexing plan: {:?}", request);
        Ok(())
    }
}
