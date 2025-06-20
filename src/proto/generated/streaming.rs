#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingFlightTicketData {
    /// output stream id
    #[prost(string, tag = "1")]
    pub stream_id: ::prost::alloc::string::String,
    /// partitions of the stream
    #[prost(message, optional, tag = "2")]
    pub partitions: ::core::option::Option<PartitionRange>,
    #[prost(uint64, tag = "3")]
    pub checkpoint_number: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionRange {
    #[prost(uint64, tag = "1")]
    pub start: u64,
    #[prost(uint64, tag = "2")]
    pub end: u64,
    #[prost(uint64, tag = "3")]
    pub partitions: u64,
}
