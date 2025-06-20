use arrow::compute::take_arrays;
use arrow_array::builder::UInt32Builder;
use arrow_array::{RecordBatch, RecordBatchOptions};
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::proto::generated::streaming as proto;
use crate::streaming::serialisation::proto_context_serialization::ProtoSerializer;
use crate::streaming::serialisation::proto_serialisation::SerialiseToProto;

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
