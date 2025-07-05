use crate::streaming::model::generation::{RemoteStreamDetails, GenerationSpec};
use crate::streaming::runtime::Runtime;
use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::model::sitem::SItem;
use crate::streaming::operators::remote_source::fibres::RunningStream;
use crate::streaming::utils::fiber_stream::FiberStream;

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteSourceOperator {
    stream_ids: Vec<String>,
}

impl RemoteSourceOperator {
    pub fn new(stream_ids: Vec<String>) -> Self {
        Self { stream_ids }
    }

    pub fn get_stream_ids(&self) -> &[String] {
        &self.stream_ids
    }
}

#[async_trait]
impl CreateOperatorFunction for RemoteSourceOperator {
    async fn create_operator_function(
        &self,
        _operator_id: &str,
        _state_id: &str,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(RemoteSourceOperatorFunction::new(
            self.stream_ids.clone(),
            runtime,
            scheduling_details,
        ))
    }
}

struct RemoteSourceOperatorFunction {
    stream_ids: Vec<String>,
    runtime: Arc<Runtime>,
    scheduling_details_state: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    loaded_checkpoint: usize,
}

impl RemoteSourceOperatorFunction {
    fn new(
        stream_ids: Vec<String>,
        runtime: Arc<Runtime>,
        scheduling_details_state: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>
    ) -> Self {
        Self {
            stream_ids,
            runtime,
            scheduling_details_state,
            loaded_checkpoint: 0,
        }
    }
}

#[async_trait]
impl OperatorFunction for RemoteSourceOperatorFunction {
    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        self.loaded_checkpoint = checkpoint;
        Ok(())
    }

    // Since this is an input operator, it takes no inputs, instead just reading from the remote
    // sources.
    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        assert_eq!(inputs.len(), 0);

        // Create RunningStream directly in the run method
        let running_stream = RunningStream::new(
            self.runtime.clone(),
            self.stream_ids.clone(),
            self.scheduling_details_state.clone(),
            self.loaded_checkpoint,
        );

        Ok(vec![(0, Box::new(running_stream) as Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync>)])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
