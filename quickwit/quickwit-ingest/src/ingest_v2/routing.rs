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

use crate::Shard;

/// A set of open shards covering the entire hash key space and ordered by their start hash key to
/// allow for binary search.
#[derive(Debug, Default)]
pub(crate) struct RoutingEntry {
    shards: Vec<Shard>,
}

impl RoutingEntry {
    /// Creates a new routing entry and ensures that the invariants are respected.
    ///
    /// # Panics
    pub fn new(shards: Vec<Shard>) -> Self {
        let mut routing_entry = Self { shards };
        routing_entry.sort();
        routing_entry.check_invariants();
        routing_entry
    }

    /// Returns the number of shards that make up the routing entry.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Finds the shard whose hash key range contains the hash key.
    pub fn route_doc(&self, hash_key: u64) -> usize {
        self.shards
            .binary_search_by(|shard| shard.cmp_hash_key(hash_key))
            .expect("The routing entry should be complete.")
    }

    /// Returns the shards that make up the routing entry.
    pub fn shards(&self) -> &[Shard] {
        &self.shards
    }

    /// Checks that the routing entry is valid.
    fn check_invariants(&self) {
        let num_shards = self.shards.len();
        assert!(num_shards > 0, "Routing entry is empty.");

        assert_eq!(
            self.shards[0].start_hash_key_inclusive, 0,
            "Routing entry is incomplete."
        );
        assert_eq!(
            self.shards[num_shards - 1].end_hash_key_exclusive,
            u64::MAX,
            "Routing entry is incomplete."
        );
        assert!(
            self.shards.windows(2).all(|shards| {
                let left = &shards[0];
                let right = &shards[1];
                left.end_hash_key_exclusive == right.start_hash_key_inclusive
            }),
            "Routing entry is incomplete."
        );
        assert!(
            self.shards.iter().all(|shard| shard.is_open()),
            "Routing entry contains closed shards."
        );
    }

    /// Sorts the shards by their start hash key.
    fn sort(&mut self) {
        self.shards
            .sort_unstable_by_key(|shard| shard.start_hash_key_inclusive);
    }
}

/// A table of routing entries indexed by index UID and source ID.
#[derive(Debug, Default)]
pub(crate) struct RoutingTable {
    table: HashMap<(String, String), RoutingEntry>,
}

impl RoutingTable {
    pub fn contains_entry(
        &self,
        index_uid: impl Into<String>,
        source_id: impl Into<String>,
    ) -> bool {
        let key = (index_uid.into(), source_id.into());
        self.table.contains_key(&key)
    }

    pub fn find_entry(
        &self,
        index_uid: impl Into<String>,
        source_id: impl Into<String>,
    ) -> Option<&RoutingEntry> {
        let key = (index_uid.into(), source_id.into());
        self.table.get(&key)
    }

    pub fn insert_shards(
        &mut self,
        index_uid: impl Into<String>,
        source_id: impl Into<String>,
        shards: Vec<Shard>,
    ) {
        let key = (index_uid.into(), source_id.into());
        self.table.insert(key, RoutingEntry::new(shards));
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_entry() {
        let routing_entry = RoutingEntry::new(vec![Shard {
            start_hash_key_inclusive: 0,
            end_hash_key_exclusive: u64::MAX,
            ..Default::default()
        }]);
        assert_eq!(routing_entry.route_doc(0), 0);
        assert_eq!(routing_entry.route_doc(u64::MAX), 0);

        let routing_entry = RoutingEntry::new(vec![
            Shard {
                start_hash_key_inclusive: u64::MAX / 2,
                end_hash_key_exclusive: u64::MAX,
                ..Default::default()
            },
            Shard {
                start_hash_key_inclusive: 0,
                end_hash_key_exclusive: u64::MAX / 2,
                ..Default::default()
            },
        ]);
        assert_eq!(routing_entry.route_doc(0), 0);
        assert_eq!(routing_entry.route_doc(u64::MAX / 2 - 1), 0);
        assert_eq!(routing_entry.route_doc(u64::MAX / 2), 1);
        assert_eq!(routing_entry.route_doc(u64::MAX), 1);
    }

    #[test]
    fn test_routing_table() {
        let mut routing_table = RoutingTable::default();
        assert!(!routing_table.contains_entry("test-index", "test-source"));

        routing_table.insert_shards(
            "test-index",
            "test-source",
            vec![Shard {
                start_hash_key_inclusive: 0,
                end_hash_key_exclusive: u64::MAX,
                ..Default::default()
            }],
        );
        assert!(routing_table.contains_entry("test-index", "test-source"));
        assert_eq!(
            routing_table
                .find_entry("test-index", "test-source")
                .unwrap()
                .route_doc(0),
            0
        );
    }
}
