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

use anyhow::{Context};
use quickwit_proto::{SortBy, SortOrder};
use tantivy::collector::Collector;
use tantivy::fastfield::Column;
use tantivy::query::Query;
use tantivy::{DateTime, DocId, Score, Searcher, SegmentReader, TantivyError};
use tracing::warn;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct DynamicSortingValues {
    values: Box<[u64]>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct StaticSortingValues<const N: usize> {
    values: [u64; N],
}

pub(crate) trait SortByComputer: Send + Sync + 'static {
    type SortingValues: Ord + Send + Sync;

    fn compute_sorting_values(&self, doc_id: DocId, score: Score) -> Self::SortingValues;
}

#[derive(Debug, Default)]
pub(crate) enum SortingField {
    #[default]
    DocId, // Default
    FastField(Column<u64>, SortOrder),
    Score,                       // `_score`
    Timestamp(Column<DateTime>), // `_timestamp`
}

impl SortingField {
    #[inline]
    fn get_sorting_value(&self, doc_id: DocId, score: Score) -> u64 {
        match self {
            SortingField::DocId => doc_id as u64,
            SortingField::FastField(column, sort_order) => match sort_order {
                SortOrder::Asc => column.first(doc_id).unwrap_or(u64::MAX),
                SortOrder::Desc => u64::MAX - column.first(doc_id).unwrap_or(0),
            },
            SortingField::Score => u64::MAX - f32_to_u64(score),
            SortingField::Timestamp(column) => {
                let timestamp = column
                    .first(doc_id)
                    .map(|datetime| datetime.into_timestamp_micros() as u64)
                    .unwrap_or(0);
                u64::MAX - timestamp
            }
        }
    }
}

pub(crate) struct DynamicSortByComputer {
    sorting_fields: Vec<SortingField>,
}

impl SortByComputer for DynamicSortByComputer {
    type SortingValues = DynamicSortingValues;

    #[inline]
    fn compute_sorting_values(&self, doc_id: DocId, score: Score) -> Self::SortingValues {
        let mut values = Vec::with_capacity(self.sorting_fields.len());
        for (i, sorting_field) in self.sorting_fields.iter().enumerate() {
            // TODO: unroll loop?
            values[i] = sorting_field.get_sorting_value(doc_id, score);
        }
        Self::SortingValues {
            values: values.into_boxed_slice(),
        }
    }
}

impl TryFrom<Vec<SortingField>> for DynamicSortByComputer {
    type Error = anyhow::Error;

    fn try_from(sorting_fields: Vec<SortingField>) -> Result<Self, Self::Error> {
        Ok(DynamicSortByComputer { sorting_fields })
    }
}

pub(crate) struct StaticSortByComputer<const N: usize> {
    sorting_fields: [SortingField; N],
}

impl<const N: usize> SortByComputer for StaticSortByComputer<N> {
    type SortingValues = StaticSortingValues<N>;

    fn compute_sorting_values(&self, doc_id: DocId, score: Score) -> Self::SortingValues {
        let mut values = [0u64; N];
        for (i, sorting_field) in self.sorting_fields.iter().enumerate() {
            // TODO: unroll loop?
            values[i] = sorting_field.get_sorting_value(doc_id, score);
        }
        Self::SortingValues { values }
    }
}

impl<const N: usize> TryFrom<Vec<SortingField>> for StaticSortByComputer<N> {
    type Error = anyhow::Error;

    fn try_from(sorting_fields: Vec<SortingField>) -> Result<Self, Self::Error> {
        let sorting_fields = sorting_fields.try_into().map_err(|rejected: Vec<_>| {
            anyhow::anyhow!(
                "Invalid number of sort fields. Expected `{}`, got `{}`.",
                N,
                rejected.len()
            )
        })?;
        Ok(StaticSortByComputer { sorting_fields })
    }
}

pub(crate) fn resolve_sort_by<T>(
    sort_by: &SortBy,
    timestamp_column_opt: &Option<Column<DateTime>>,
    segment_reader: &SegmentReader,
) -> anyhow::Result<T>
where
    T: SortByComputer + TryFrom<Vec<SortingField>, Error = anyhow::Error>,
{
    let mut sorting_fields = Vec::with_capacity(sort_by.sort_fields.len() + 1);

    for (i, sort_field) in sort_by.sort_fields.iter().enumerate() {
        let computer = match sort_field.field_name.as_str() {
            "_score" => SortingField::Score,
            "_timestamp" => {
                let timestamp_column = timestamp_column_opt
                    .clone()
                    .unwrap_or_else(|| Column::build_empty_column(segment_reader.max_doc()));
                SortingField::Timestamp(timestamp_column)
            }
            field_name => {
                let column = segment_reader
                    .fast_fields()
                    .u64_lenient(field_name)?
                    .map(|(column, _)| column)
                    .unwrap_or_else(|| Column::build_empty_column(segment_reader.max_doc()));
                let sort_order =
                    SortOrder::from_i32(sort_field.sort_order).with_context(|| format!(""))?;
                SortingField::FastField(column, sort_order)
            }
        };
        sorting_fields[i] = computer;
    }
    T::try_from(sorting_fields)
}

pub(crate) trait SortByCollectorFactory {
    type SortByCollector: Collector;

    fn sort_by(&self) -> &SortBy;

    fn make<T>(self) -> Self::SortByCollector
    where T: SortByComputer;
}

pub(crate) fn search_and_collect<F>(
    searcher: &Searcher,
    query: &dyn Query,
    collector_factory: F,
    sort_by: &SortBy,
) -> Result<<<F as SortByCollectorFactory>::SortByCollector as Collector>::Fruit, TantivyError>
where
    F: SortByCollectorFactory,
{
    match collector_factory.sort_by().sort_fields.len() {
        1 => {
            let collector = collector_factory.make::<StaticSortByComputer<1>>();
            searcher.search(query, &collector)
        }
        2 => {
            let collector = collector_factory.make::<StaticSortByComputer<2>>();
            searcher.search(query, &collector)
        }
        3 => {
            let collector = collector_factory.make::<StaticSortByComputer<3>>();
            searcher.search(query, &collector)
        }
        _ => {
            warn!(sort_fields=?sort_by.sort_fields, "Using dynamic sort by computer to sort by {} fields", sort_by.sort_fields.len());
            let collector = collector_factory.make::<DynamicSortByComputer>();
            searcher.search(query, &collector)
        }
    }
}

/// Converts a float to an unsigned integer while preserving order.
/// See `<https://lemire.me/blog/2020/12/14/converting-floating-point-numbers-to-integers-while-preserving-order/>`
fn f32_to_u64(value: Score) -> u64 {
    let value_u32 = u32::from_le_bytes(value.to_le_bytes());
    let mut mask = (value_u32 as i32 >> 31) as u32;
    mask |= 0x80000000;
    (value_u32 ^ mask) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;
    use std::marker::PhantomData;

    use itertools::Itertools;
    use quickwit_proto::SortField;
    use tantivy::collector::{Collector, SegmentCollector};
    use tantivy::query::Query;
    use tantivy::{Searcher, SegmentReader};

    use super::*;

    struct TestSortByCollector<T: SortByComputer> {
        max_hits: usize,
        sort_by: SortBy,
        sort_by_marker: PhantomData<T>,
    }

    impl<T> TestSortByCollector<T>
    where T: SortByComputer
    {
        pub fn new(sort_by: SortBy, max_hits: usize) -> Self {
            Self {
                max_hits,
                sort_by,
                sort_by_marker: PhantomData,
            }
        }
    }

    impl<T> Collector for TestSortByCollector<T>
    where T: SortByComputer + TryFrom<Vec<SortingField>, Error = anyhow::Error>
    {
        type Fruit = Vec<T::SortingValues>;
        type Child = TestSortBySegmentCollector<T>;

        fn for_segment(
            &self,
            _segment_local_id: u32,
            segment_reader: &SegmentReader,
        ) -> tantivy::Result<Self::Child> {
            let max_hits = self.max_hits;
            let sort_by = resolve_sort_by(&self.sort_by, &None, segment_reader).unwrap();
            let segment_collector = TestSortBySegmentCollector {
                max_hits,
                sort_by,
                heap: BinaryHeap::with_capacity(self.max_hits),
            };
            Ok(segment_collector)
        }

        fn requires_scoring(&self) -> bool {
            false
        }

        fn merge_fruits(
            &self,
            child_fruits: Vec<<Self as Collector>::Fruit>,
        ) -> tantivy::Result<Self::Fruit> {
            Ok(child_fruits
                .into_iter()
                .kmerge()
                .take(self.max_hits)
                .collect())
        }
    }

    struct TestSortBySegmentCollector<T: SortByComputer> {
        max_hits: usize,
        sort_by: T,
        heap: BinaryHeap<T::SortingValues>,
    }

    impl<T> SegmentCollector for TestSortBySegmentCollector<T>
    where T: SortByComputer
    {
        type Fruit = Vec<T::SortingValues>;

        fn collect(&mut self, doc_id: DocId, score: Score) {
            let sorting_values = self.sort_by.compute_sorting_values(doc_id, score);

            if self.heap.len() < self.max_hits {
                self.heap.push(sorting_values);
            } else if let Some(mut max_sorting_values) = self.heap.peek_mut() {
                if sorting_values < *max_sorting_values {
                    *max_sorting_values = sorting_values;
                }
            }
        }

        fn harvest(self) -> Self::Fruit {
            self.heap.into_sorted_vec()
        }
    }

    struct TestSortByCollectorFactory {
        sort_by: SortBy,
        max_hits: usize,
    }

    impl TestSortByCollectorFactory {
        pub fn new(sort_by: SortBy, max_hits: usize) -> Self {
            Self { sort_by, max_hits }
        }
    }

    impl SortByCollectorFactory for TestSortByCollectorFactory {
        type SortByCollector = TestSortByCollector<DynamicSortByComputer>;

        fn sort_by(&self) -> &SortBy {
            &self.sort_by
        }

        fn make<T>(self) -> Self::SortByCollector {
            TestSortByCollector::new(self.sort_by.clone(), self.max_hits)
        }
    }


    #[test]
    fn test_dynamic_sort_by() {
        let searcher: Searcher = unimplemented!();
        let query: Box<dyn Query> = unimplemented!();

        let sort_by = SortBy {
            sort_fields: vec![
                SortField {
                    field_name: "severity".to_string(),
                    sort_order: SortOrder::Desc as i32,
                },
                SortField {
                    field_name: "_timestamp".to_string(),
                    sort_order: SortOrder::Desc as i32,
                },
            ],
        };
        let collector = TestSortByCollector::<DynamicSortByComputer>::new(sort_by, 3);
        searcher.search(&query, &collector).unwrap();
    }

    #[test]
    fn test_static_sort_by() {
        let searcher: Searcher = unimplemented!();
        let query: Box<dyn Query> = unimplemented!();

        let sort_by = SortBy {
            sort_fields: vec![
                SortField {
                    field_name: "severity".to_string(),
                    sort_order: SortOrder::Desc as i32,
                },
                SortField {
                    field_name: "_timestamp".to_string(),
                    sort_order: SortOrder::Desc as i32,
                },
            ],
        };
        let collector = TestSortByCollector::<StaticSortByComputer<2>>::new(sort_by, 3);
        searcher.search(&query, &collector).unwrap();
    }
}
