use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::serialisation::proto_context_serialization::ProtoSerializer;
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::Runtime;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::streaming::model::sitem::SItem;

#[derive(Clone, Serialize, Deserialize)]
pub struct IdentityOperator;

impl IdentityOperator {
    pub fn into_function(self) -> IdentityTask {
        IdentityTask
    }
}

impl CreateOperatorFunction for IdentityOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(IdentityOperatorFunction)
    }
}

impl ProtoSerializer for IdentityOperator {
    type ProtoType = proto::IdentityOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(proto::IdentityOperator {})
    }

    fn try_from_proto(_proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(IdentityOperator)
    }
}

pub struct IdentityTask;

struct IdentityOperatorFunction;

#[async_trait]
impl OperatorFunction for IdentityOperatorFunction {
    async fn init(
        &mut self,
        _runtime: Arc<Runtime>,
        _scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        _state_id: &str,
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn load(&mut self, _checkpoint: usize) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        Ok(inputs)
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
