use arrow_array::RecordBatch;
use datafusion::common::DataFusionError;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Marker {
    pub checkpoint_number: u64,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum StreamItem {
    Marker(Marker),
    RecordBatch(
        #[serde(with = "crate::streaming::serialisation::serde_serialization::record_batch")]
        RecordBatch
    ),
}

impl From<RecordBatch> for StreamItem {
    fn from(batch: RecordBatch) -> Self {
        StreamItem::RecordBatch(batch)
    }
}

impl From<Marker> for StreamItem {
    fn from(marker: Marker) -> Self {
        StreamItem::Marker(marker)
    }
}

pub type StreamResult = Result<StreamItem, DataFusionError>;