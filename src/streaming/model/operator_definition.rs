use std::sync::Arc;
use async_trait::async_trait;
use eyeball::{AsyncLock, SharedObservable};
use serde::{Deserialize, Serialize};
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::operators::count_by_key::CountByKeyOperator;
use crate::streaming::operators::count_star::CountStarOperator;
use crate::streaming::operators::identity::IdentityOperator;
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::runtime::Runtime;

#[derive(Clone, Serialize, Deserialize)]
pub enum OperatorSpec {
    Identity(IdentityOperator),
    Source(SourceOperator),
    CountStar(CountStarOperator),
    CountByKey(CountByKeyOperator),
    Nested(NestedOperator),
    RemoteExchangeOutput(RemoteExchangeOperator),
    RemoteExchangeInput(RemoteSourceOperator),
}

#[async_trait]
impl CreateOperatorFunction for OperatorSpec {
    async fn create_operator_function(
        &self,
        operator_id: &str,
        state_id: &str,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send> {
        match self {
            OperatorSpec::Identity(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::Source(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::CountStar(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::CountByKey(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::Nested(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::RemoteExchangeOutput(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
            OperatorSpec::RemoteExchangeInput(op) => op.create_operator_function(operator_id, state_id, runtime, scheduling_details).await,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorInput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorOutput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorDefinition {
    pub id: String,
    pub state_id: String,
    pub spec: OperatorSpec,
    pub inputs: Vec<OperatorInput>,
    pub outputs: Vec<OperatorOutput>,
}
