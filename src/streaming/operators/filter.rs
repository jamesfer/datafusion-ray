use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::serialisation::proto_context_serialization::{ProtoSerializer, P};
use arrow::datatypes::SchemaRef;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::prelude::SessionContext;

#[derive(Clone)]
pub struct FilterOperator {
    input_schema: SchemaRef,
    expression: PhysicalExprRef,
}

impl FilterOperator {
    pub fn new(input_schema: SchemaRef, expression: PhysicalExprRef) -> Self {
        Self {
            input_schema,
            expression,
        }
    }

    pub fn into_function(self) -> FilterTask {
        FilterTask::new(self.expression)
    }
}

impl ProtoSerializer for FilterOperator {
    type ProtoType = proto::FilterOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = SessionContext;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            input_schema: Some(self.input_schema.try_into_proto(&())?),
            expression: Some(self.expression.try_into_proto(context)?),
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let input_schema = proto.input_schema
            .ok_or(internal_datafusion_err!("Input schema is required for FilterOperator"))?
            .try_from_proto(&())?;
        Ok(Self {
            expression: proto.expression
                .ok_or(internal_datafusion_err!("Expression is required for FilterOperator"))?
                .try_from_proto(&(context, SchemaRef::as_ref(&input_schema)))?,
            input_schema,
        })
    }
}

pub struct FilterTask {
    expression: PhysicalExprRef,
}

impl FilterTask {
    pub fn new(expression: PhysicalExprRef) -> Self {
        Self {
            expression,
        }
    }
}
