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

impl CreateOperatorFunction for RemoteSourceOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(RemoteSourceOperatorFunction::new(self.stream_ids.clone()))
    }
}

struct RemoteSourceOperatorFunction {
    stream_ids: Vec<String>,
    runtime: Option<Arc<Runtime>>,
    scheduling_details_state: Option<SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>>,
    loaded_checkpoint: usize,
}

impl RemoteSourceOperatorFunction {
    fn new(stream_ids: Vec<String>) -> Self {
        Self {
            stream_ids,
            runtime: None,
            scheduling_details_state: None,
            loaded_checkpoint: 0,
        }
    }
}

#[async_trait]
impl OperatorFunction for RemoteSourceOperatorFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        _state_id: &str,
    ) -> Result<(), DataFusionError> {
        self.runtime = Some(runtime);
        self.scheduling_details_state = Some(scheduling_details);
        Ok(())
    }

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

        let scheduling_details_state = self.scheduling_details_state.as_ref().ok_or_else(|| {
            internal_datafusion_err!("Scheduling details state not initialized. Did you call init()?")
        })?;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            internal_datafusion_err!("Runtime not initialized. Did you call init()?")
        })?;

        // Create RunningStream directly in the run method
        let running_stream = RunningStream::new(
            runtime.clone(),
            self.stream_ids.clone(),
            scheduling_details_state.clone(),
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
