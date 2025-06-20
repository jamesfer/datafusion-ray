use arrow_array::RecordBatch;
use crate::streaming::model::stream_item::{Marker, StreamItem};

#[derive(PartialEq, Debug)]
pub enum SItem {
    RecordBatch(RecordBatch),
    Marker(Marker),
    Generation(usize),
}

impl From<StreamItem> for SItem {
    fn from(item: StreamItem) -> Self {
        match item {
            StreamItem::RecordBatch(batch) => SItem::RecordBatch(batch),
            StreamItem::Marker(marker) => SItem::Marker(marker),
        }
    }
}