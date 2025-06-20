use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::functions::all_default_functions;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;

pub trait ProtoSerializer: Sized {
    type ProtoType;
    type SerializerContext<'a>;
    type DeserializerContext<'a>;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError>;
    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError>;
}

// To make converting from proto back to rust representation easier
pub trait P<D> {
    type Context<'a>;

    fn try_from_proto(self, context: &Self::Context<'_>) -> Result<D, DataFusionError>;
}

impl <T, D> P<D> for T
where D: ProtoSerializer<ProtoType=T>
{
    type Context<'a> = D::DeserializerContext<'a>;

    fn try_from_proto(self, context: &Self::Context<'_>) -> Result<D, DataFusionError> {
        D::try_from_proto(self, context)
    }
}

impl <T> ProtoSerializer for Vec<T>
where
    T: ProtoSerializer
{
    type ProtoType = Vec<T::ProtoType>;
    type SerializerContext<'a> = T::SerializerContext<'a>;
    type DeserializerContext<'a> = T::DeserializerContext<'a>;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        self.into_iter()
            .map(|item| item.try_into_proto(context))
            .collect()
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        proto.into_iter()
            .map(|item| T::try_from_proto(item, context))
            .collect()
    }
}

impl ProtoSerializer for PhysicalExprRef {
    type ProtoType = PhysicalExprNode;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = (&'a SessionContext, &'a Schema);

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        // TODO
        serialize_physical_expr(&self, &DefaultPhysicalExtensionCodec {})
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        parse_physical_expr(
            &proto,
            context.0,
            context.1,
            // TODO
            &DefaultPhysicalExtensionCodec {},
        )
    }
}

impl ProtoSerializer for SchemaRef {
    type ProtoType = datafusion_proto::protobuf::Schema;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(self.as_ref().try_into()?)
    }

    fn try_from_proto(proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(SchemaRef::new((&proto).try_into()?))
    }
}
