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
    start: usize,
    end: usize,
    // TODO always use max u64?
    partitions: usize,
}

impl PartitionRange {
    pub fn new(start: usize, end: usize, partitions: usize) -> Self {
        Self { start, end, partitions }
    }

    pub fn empty() -> Self {
        Self {
            start: 0,
            end: 0,
            partitions: 0,
        }
    }

    pub fn unit() -> Self {
        Self {
            start: 0,
            end: 1,
            partitions: 1,
        }
    }

    pub fn full() -> Self {
        Self::new(0, 2^32, 2^32)
    }

    pub fn new_from_index(index: usize, partitions: usize) -> Self {
        Self {
            start: index,
            end: index + 1,
            partitions
        }
    }

    pub fn intersection(&self, other: &PartitionRange) -> Self {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        if start < end {
            Self {
                start,
                end,
                partitions: self.partitions,
            }
        } else {
            Self::empty()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn size(&self) -> usize {
        self.end - self.start
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }

    pub fn partitions(&self) -> usize {
        self.partitions
    }

    pub fn contains_hash(&self, hash: u64) -> bool {
        let partition = (hash % self.partitions as u64) as usize;
        partition >= self.start && partition < self.end
    }

    pub fn contains_partition_indices(&self, partition_indices: &[usize]) -> bool {
        partition_indices.iter().any(|&index| {
            index >= self.start && index < self.end
        })
    }

    pub fn find_covering_partitions(
        &self,
        available_partitions: &[PartitionRange],
    ) -> Option<Vec<OverlappingPartition>> {
        // Easy implementation, not quite optimal, but good enough for now.

        // All partition ranges will be converted to the same partition count for comparison
        let target_partitions = available_partitions.iter()
            .map(|partition_range| partition_range.partitions())
            .chain([self.partitions()])
            .max()?;
        let search_start = self.start() * target_partitions / self.partitions();
        let search_end = self.end() * target_partitions / self.partitions();

        // Normalize all available partitions to the target partition count
        // (original index, normalised start, normalised end)
        let mut normalized_partitions: Vec<(usize, usize, usize)> = available_partitions
            .iter()
            .enumerate()
            .map(|(index, partition_range)| {
                let normalized_start = partition_range.start() * target_partitions / partition_range.partitions();
                let normalized_end = partition_range.end() * target_partitions / partition_range.partitions();
                (index, normalized_start, normalized_end)
            })
            .filter(|(_index, start, end)| {
                // Check if the partition overlaps with the search range
                *start < search_end && *end > search_start
            })
            .collect();

        // Sort by start position
        normalized_partitions.sort_by_key(|(start, _, _)| *start);

        let mut current_search_start = search_start;
        let mut found_partitions = Vec::new();
        for (index, start, end) in normalized_partitions {
            if end > current_search_start {
                // If the partition starts after the current search start, we have a gap
                if start > current_search_start {
                    // No overlapping partitions found
                    return None;
                }

                // Update the current search start to the end of this partition
                let overlap_end = search_end.min(end);
                found_partitions.push(OverlappingPartition {
                    index,
                    selected_partition_range: PartitionRange::new(
                        current_search_start,
                        overlap_end,
                        target_partitions,
                    ),
                });

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
            start: self.start as u64,
            end: self.end as u64,
            partitions: self.partitions as u64,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> Self {
        Self {
            start: proto.start as usize,
            end: proto.end as usize,
            partitions: proto.partitions as usize,
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
        let search_range = PartitionRange::new(0, 4, 4); // Covers partitions 0,1,2,3 out of 4
        let available_partitions = vec![
            PartitionRange::new(0, 2, 4), // Covers 0,1
            PartitionRange::new(2, 4, 4), // Covers 2,3
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 2, 4),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(2, 4, 4),
            },
        ]));

        // Test case 2: No coverage possible due to gap
        let search_range = PartitionRange::new(0, 4, 4);
        let available_partitions = vec![
            PartitionRange::new(0, 1, 4), // Covers 0 only
            PartitionRange::new(3, 4, 4), // Covers 3 only (gap at 1,2)
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert!(result.is_none());

        // Test case 3: Different partition counts (normalised comparison)
        let search_range = PartitionRange::new(0, 1, 2); // Half of 2 partitions = equivalent to 0-5 of 10
        let available_partitions = vec![
            PartitionRange::new(0, 5, 10), // First half in 10-partition system
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 5, 10),
            },
        ]));

        // Test case 4: Search range has fewer base partitions, and available partitions don't
        // divide evenly
        let search_range = PartitionRange::new(0, 1, 2); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(0, 3, 8), // 8 partitions total, covers first half
            PartitionRange::new(3, 4, 8), // 8 partitions total, covers second half
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 3, 8),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(3, 4, 8),
            },
        ]));

        // Test case 5: Ranges that extend outside search range and overlap with each other
        let search_range = PartitionRange::new(2, 6, 8); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(3, 8, 16),
            PartitionRange::new(0, 3, 16), // Should be excluded
            PartitionRange::new(6, 16, 16),
            PartitionRange::new(7, 16, 16),
        ];

        let result = search_range.find_covering_partitions(&available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(4, 8, 16),
            },
            OverlappingPartition {
                index: 2,
                selected_partition_range: PartitionRange::new(8, 12, 16),
            },
        ]))
    }

}
