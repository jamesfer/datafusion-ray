use crate::proto::generated::streaming as proto;
use crate::streaming::serialisation::proto_context_serialization::ProtoSerializer;
use crate::streaming::serialisation::proto_serialisation::SerialiseToProto;
use arrow::compute::take_arrays;
use arrow_array::builder::UInt32Builder;
use arrow_array::{RecordBatch, RecordBatchOptions};
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct PartitionRange {
    start: u64,
    end: u64,
}

impl PartitionRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end: end.max(start) }
    }

    pub fn with_max_partitions(start: usize, end: usize, partitions: usize) -> Self {
        Self {
            start: ((start as f64 / partitions as f64) * u64::MAX as f64) as u64,
            end: ((end as f64 / partitions as f64) * u64::MAX as f64) as u64,
        }
    }

    pub fn empty() -> Self {
        Self {
            start: 0,
            end: 0,
        }
    }

    pub fn unit() -> Self {
        Self {
            start: 0,
            end: u64::MAX,
        }
    }

    pub fn full() -> Self {
        Self {
            start: 0,
            end: u64::MAX,
        }
    }

    pub fn new_from_index(index: usize, partitions: usize) -> Self {
        Self::with_max_partitions(index, index + 1, partitions)
    }

    pub fn intersection(&self, other: &PartitionRange) -> Self {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        if start < end {
            Self {
                start,
                end,
            }
        } else {
            Self::empty()
        }
    }

    pub fn intersects(&self, other: &PartitionRange) -> bool {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        start < end
    }

    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }

    pub fn size(&self) -> u64 {
        self.end - self.start
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn partitions(&self) -> u64 {
        u64::MAX
    }

    pub fn contains_hash(&self, hash: u64) -> bool {
        hash >= self.start && hash < self.end
    }

    pub fn find_covering_partitions(
        &self,
        available_partitions: &[PartitionRange],
    ) -> Option<Vec<OverlappingPartition>> {
        // Easy implementation, not quite optimal, but good enough for now.

        // All partition ranges will be converted to the same partition count for comparison
        // TODO this loop is probably not needed when all PartitionRanges are based on the same partition count
        let search_start = self.start();
        let search_end = self.end();

        let mut intersecting_ranges: Vec<(usize, &PartitionRange)> = available_partitions
            .iter()
            .enumerate()
            .filter(|(_index, partition_range)| partition_range.intersects(self))
            .collect();

        // Sort by start position
        intersecting_ranges.sort_by_key(|(_, partition_range)| partition_range.start);

        let mut current_search_start = search_start;
        let mut found_partitions = Vec::new();
        for (index, range) in intersecting_ranges {
            if range.end > current_search_start {
                // If the partition starts after the current search start, we have a gap
                if range.start > current_search_start {
                    // No overlapping partitions found
                    return None;
                }

                let overlap_end = search_end.min(range.end);
                found_partitions.push(OverlappingPartition {
                    index,
                    selected_partition_range: PartitionRange::new(current_search_start, overlap_end),
                });

                // Update the current search start to the end of this partition
                current_search_start = overlap_end;
                if current_search_start >= search_end {
                    // We have finished searching
                    return Some(found_partitions);
                }
            }
        }

        // If we reach here, it means we didn't cover the entire search range
        assert!(current_search_start < search_end, "Search algorithm finished without returning the success case");
        None
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct OverlappingPartition {
    pub index: usize,
    pub selected_partition_range: PartitionRange,
}

impl SerialiseToProto for PartitionRange {
    type ProtoType = proto::PartitionRange;

    fn to_proto(&self) -> Self::ProtoType {
        Self::ProtoType {
            start: self.start,
            end: self.end,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> Self {
        Self {
            start: proto.start,
            end: proto.end,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartitioningSpec {
    #[serde(with = "crate::streaming::serialisation::serde_serialization::physical_expr_refs")]
    pub expressions: Vec<Arc<dyn PhysicalExpr>>,
}

// Filters a record batch, selecting only the rows that fall within a specified partition range.
// This is probably a fairly inefficient approach, as partitioning a stream into 10 partitions
// would require calculating the hashes of the record batch 10 times.
pub fn filter_by_partition_range(
    batch: &RecordBatch,
    partition_range: &PartitionRange,
    expressions: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch, DataFusionError> {
    let arrays = expressions
        .iter()
        .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let mut hash_buffer = vec![0; batch.num_rows()];
    let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    create_hashes(&arrays, &random_state, &mut hash_buffer)?;

    let mut indices: Vec<_> = Vec::with_capacity(batch.num_rows());
    for (index, hash) in hash_buffer.iter().enumerate() {
        if partition_range.contains_hash(*hash) {
            indices.push(index as u32);
        }
    }

    let mut indices_array = UInt32Builder::new();
    indices_array.append_slice(indices.as_slice());
    let indices_array = indices_array.finish();

    // Produce batches based on indices
    let columns = take_arrays(batch.columns(), &indices_array, None)?;
    let mut options = RecordBatchOptions::new();
    options = options.with_row_count(Some(indices.len()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

#[cfg(test)]
mod tests {
    use crate::streaming::partitioning::{OverlappingPartition, PartitionRange};

    #[test]
    fn test_find_covering_partitions() {
        // Test case 1: Perfect coverage with non-overlapping partitions
        let search_range = PartitionRange::with_max_partitions(0, 4, 4); // Covers partitions 0,1,2,3 out of 4
        let available_partitions = vec![
            PartitionRange::with_max_partitions(0, 2, 4), // Covers 0,1
            PartitionRange::with_max_partitions(2, 4, 4), // Covers 2,3
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::with_max_partitions(0, 2, 4),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::with_max_partitions(2, 4, 4),
            },
        ]));

        // Test case 2: No coverage possible due to gap
        let search_range = PartitionRange::with_max_partitions(0, 4, 4);
        let available_partitions = vec![
            PartitionRange::with_max_partitions(0, 1, 4), // Covers 0 only
            PartitionRange::with_max_partitions(3, 4, 4), // Covers 3 only (gap at 1,2)
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert!(result.is_none());

        // Test case 3: Different partition counts (normalised comparison)
        let search_range = PartitionRange::with_max_partitions(0, 1, 2); // Half of 2 partitions = equivalent to 0-5 of 10
        let available_partitions = vec![
            PartitionRange::with_max_partitions(0, 5, 10), // First half in 10-partition system
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::with_max_partitions(0, 5, 10),
            },
        ]));

        // Test case 4: Search range has fewer base partitions, and available partitions don't
        // divide evenly
        let search_range = PartitionRange::with_max_partitions(0, 1, 2); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::with_max_partitions(0, 3, 8), // 8 partitions total, covers first half
            PartitionRange::with_max_partitions(3, 4, 8), // 8 partitions total, covers second half
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::with_max_partitions(0, 3, 8),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::with_max_partitions(3, 4, 8),
            },
        ]));

        // Test case 5: Ranges that extend outside search range and overlap with each other
        let search_range = PartitionRange::with_max_partitions(2, 6, 8); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::with_max_partitions(3, 8, 16),
            PartitionRange::with_max_partitions(0, 3, 16), // Should be excluded
            PartitionRange::with_max_partitions(6, 16, 16),
            PartitionRange::with_max_partitions(7, 16, 16),
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::with_max_partitions(4, 8, 16),
            },
            OverlappingPartition {
                index: 2,
                selected_partition_range: PartitionRange::with_max_partitions(8, 12, 16),
            },
        ]))
    }

}
