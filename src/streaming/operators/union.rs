use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::serialisation::proto_context_serialization::ProtoSerializer;
use datafusion::common::DataFusionError;

#[derive(Clone)]
pub struct UnionOperator;

impl ProtoSerializer for UnionOperator {
    type ProtoType = proto::UnionOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(proto::UnionOperator {})
    }

    fn try_from_proto(_proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(UnionOperator)
    }
}
