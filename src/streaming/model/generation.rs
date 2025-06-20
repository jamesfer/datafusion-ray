// Generation can be updated when:
// - an upstream operator moves to another node, therefore changing its address
// - autoscaling causing each operator to need to use a different set of partitions

use serde::{Deserialize, Serialize};
use crate::streaming::partitioning::PartitionRange;

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteStreamLocation {
    pub address: String,
    pub offset_range: (usize, usize),
    pub partitions: PartitionRange,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteStreamDetails {
    pub stream_id: String,
    pub locations: Vec<RemoteStreamLocation>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum GenerationStartOffset {
    AnyTimeAfter(usize),
    // Exactly(usize),
    // Immediately,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GenerationSpec {
    pub id: String,
    pub partitions: PartitionRange,
    pub start_conditions: Vec<(String, GenerationStartOffset)>,
}

pub struct TaskSchedulingDetailsUpdate {
    pub generation: Option<GenerationSpec>,
    pub input_details: Option<Vec<RemoteStreamDetails>>,
}
