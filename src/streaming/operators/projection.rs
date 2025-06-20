use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::model::stream_item::StreamItem;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::streaming_operators::projection::ProjectionStreamingTask;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use async_trait::async_trait;
use crate::streaming::serialisation::proto_context_serialization::{ProtoSerializer, P};

#[derive(Clone)]
pub struct ProjectionExpression {
    pub expression: PhysicalExprRef,
    pub alias: String,
}

impl ProtoSerializer for ProjectionExpression {
    type ProtoType = proto::ProjectionExpression;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = (&'a SessionContext, &'a Schema);

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            expression: Some(self.expression.try_into_proto(context)?),
            alias: self.alias,
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(Self {
            expression: proto.expression
                .ok_or(internal_datafusion_err!("Expression is required for ProjectionExpression"))?
                .try_from_proto(context)?,
            alias: proto.alias,
        })
    }
}

#[derive(Clone)]
pub struct ProjectionOperator {
    pub schema: SchemaRef,
    pub expressions: Vec<ProjectionExpression>,
}

impl ProjectionOperator {
    pub fn into_function(self) -> ProjectionTask {
        ProjectionTask::new(
            self.expressions.into_iter().map(|expr| (expr.expression, expr.alias)).collect(),
            self.schema,
        )
    }
}

impl ProtoSerializer for ProjectionOperator {
    type ProtoType = proto::ProjectionOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = SessionContext;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            input_schema: Some(self.schema.try_into_proto(context)?),
            expressions: self.expressions.try_into_proto(context)?,
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let input_schema: SchemaRef = proto.input_schema
            .ok_or(internal_datafusion_err!("Schema is required for ProjectionOperator"))?
            .try_from_proto(&())?;
        let context = (context, input_schema.as_ref());
        let expressions = proto.expressions.try_from_proto(&context)?;
        Ok(Self {
            schema: input_schema,
            expressions,
        })
    }
}

pub struct ProjectionTask {
    inner: ProjectionStreamingTask,
}

impl ProjectionTask {
    pub fn new(expressions: Vec<(PhysicalExprRef, String)>, input_schema: SchemaRef) -> Self {
        Self {
            inner: ProjectionStreamingTask::new(expressions, input_schema).unwrap(),
        }
    }
}
