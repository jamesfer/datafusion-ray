use serde::{Deserialize, Serialize};
use flexbuffers;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;

pub trait SerialiseToFlexbuffers {
    fn to_flexbuffers_bytes(&self) -> Result<Vec<u8>, DataFusionError>;
}

impl<T: Serialize> SerialiseToFlexbuffers for T {
    fn to_flexbuffers_bytes(&self) -> Result<Vec<u8>, DataFusionError> {
        flexbuffers::to_vec(self)
            .map_err(|e| internal_datafusion_err!("Failed to serialize to flexbuffers: {}", e))
    }
}

pub trait DeserialiseFromFlexbuffers<'de> {
    fn from_flexbuffers_bytes(bytes: &'de [u8]) -> Result<Self, DataFusionError>
    where Self: Sized;
}

impl<'de, T: Deserialize<'de>> DeserialiseFromFlexbuffers<'de> for T {
    fn from_flexbuffers_bytes(bytes: &'de [u8]) -> Result<Self, DataFusionError>
    where Self: Sized
    {
        flexbuffers::from_slice(bytes)
            .map_err(|e| internal_datafusion_err!("Failed to deserialize from flexbuffers: {}", e))
    }
}
