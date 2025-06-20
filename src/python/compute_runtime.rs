use std::sync::Arc;
use pyo3::PyResult;
use crate::python::remote_processor::RemoteProcessor;
use crate::python::python_resources::PythonResources;
use crate::streaming::model::task_definition::TaskDefinition;
use crate::streaming::worker_process::InitialSchedulingDetails;

pub struct ComputeRuntime {
    python_resources: Arc<PythonResources>,
}

impl ComputeRuntime {
    pub fn new(python_resources: Arc<PythonResources>) -> Self {
        ComputeRuntime { python_resources }
    }

    pub async fn start_processor_and_run(
        &self,
        task_definition: TaskDefinition,
        initial_scheduling_details: InitialSchedulingDetails,
    ) -> PyResult<RemoteProcessor> {
        let processor = RemoteProcessor::start(self.python_resources.clone(), None).await?;
        processor.update_plan(&task_definition, &initial_scheduling_details).await?;
        Ok(processor)
    }

    pub async fn start_many_processors(
        &self,
        num_processors: usize,
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Vec<RemoteProcessor>> {
        RemoteProcessor::start_many(self.python_resources.clone(), num_processors, remote_checkpoint_dir).await
    }
}
