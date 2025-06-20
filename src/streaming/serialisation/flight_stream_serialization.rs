use std::error::Error;
use std::ops::Deref;
use crate::streaming::serialisation::flight_data_encoder::{FlightDataEncoderBuilder, FlightDataItem};
use arrow::error::ArrowError;
use arrow_flight::decode::{DecodedPayload, FlightRecordBatchStream};
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use bytes::Bytes;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use tonic::Status;
use crate::streaming::model::stream_item::{Marker, StreamItem, StreamResult};
use crate::streaming::serialisation::flexbuffers_serialisation::{DeserialiseFromFlexbuffers, SerialiseToFlexbuffers};

pub fn encode_stream_to_flight(
    stream: impl Stream<Item=Result<StreamItem, impl Error + Send + Sync + 'static>> + Send + 'static
) -> impl Stream<Item=Result<FlightData, Status>> {
    let flight_data_items = stream
        // Convert datafusion errors to arrow errors
        .map_err(|e| FlightError::from_external_error(Box::new(e) as Box<dyn Error + Send + Sync>))
        // Convert stream items to flight items
        .map(|stream_result| {
            match stream_result {
                Ok(StreamItem::RecordBatch(record_batch)) => Ok(FlightDataItem::RecordBatch(record_batch)),
                Ok(StreamItem::Marker(marker)) => {
                    let marker_bytes = marker.to_flexbuffers_bytes()
                        .map_err(|e| FlightError::Arrow(ArrowError::ExternalError(Box::new(e))))?;
                    Ok(FlightDataItem::AppMetadata(Bytes::from(marker_bytes)))
                },
                Err(err) => Err(err),
            }
        });

    FlightDataEncoderBuilder::new()
        .build(Box::pin(flight_data_items))
        .map_err(|flight_error| {
            Status::internal(format!("Error encoding flight data: {}", flight_error))
        })
}

pub fn decode_flight_to_stream(
    flight_stream: FlightRecordBatchStream
) -> impl Stream<Item=StreamResult> + Send {
    flight_stream.into_inner()
        .map_err(|flight_error| match flight_error {
            FlightError::Arrow(arrow_error) => DataFusionError::ArrowError(arrow_error, None),
            FlightError::NotYetImplemented(_) => internal_datafusion_err!("Not yet implemented"),
            FlightError::Tonic(status) => internal_datafusion_err!("Tonic error {}", status),
            FlightError::ProtocolError(protocol_err) => internal_datafusion_err!("Protocol error: {}", protocol_err),
            FlightError::DecodeError(decode_err) => internal_datafusion_err!("Decode error: {}", decode_err),
            FlightError::ExternalError(external_err) => internal_datafusion_err!("External error: {}", external_err),
        })
        .try_filter_map(|decoded_flight_data| {
            let r = match decoded_flight_data.payload {
                DecodedPayload::RecordBatch(record_batch) => Ok(Some(StreamItem::RecordBatch(record_batch))),
                // Ignore schema items
                DecodedPayload::Schema(_) => Ok(None),
                DecodedPayload::None => {
                    // When the decoded payload is none, we check the app metadata for a marker
                    Marker::from_flexbuffers_bytes(decoded_flight_data.inner.app_metadata.deref())
                        .map(|marker| Some(StreamItem::Marker(marker)))
                },
            };
            futures::future::ready(r)
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_encoding_decoding_flight() {
        // TODO
    }
}
