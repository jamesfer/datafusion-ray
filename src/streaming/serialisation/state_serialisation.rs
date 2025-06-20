use datafusion::common::DataFusionError;

pub trait SerialiseToStateBytes {
    fn into_state_bytes(self) -> Result<Vec<u8>, DataFusionError>;
    fn from_state_bytes(bytes: Vec<u8>) -> Result<Self, DataFusionError>
    where Self: Sized;
}

impl SerialiseToStateBytes for u64 {
    fn into_state_bytes(self) -> Result<Vec<u8>, DataFusionError> {
        Ok(Vec::from(self.to_be_bytes()))
    }

    fn from_state_bytes(bytes: Vec<u8>) -> Result<Self, DataFusionError>
    where Self: Sized {
        bytes.try_into()
            .map_err(|_| DataFusionError::Execution("Failed to convert bytes to u64".to_string()))
            .map(u64::from_be_bytes)
    }
}
