use std::borrow::Cow;
use crate::streaming::model::task_definition::TaskDefinition;
use crate::streaming::worker_process::InitialSchedulingDetails;
use crate::streaming::serialisation::flexbuffers_serialisation::SerialiseToFlexbuffers;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use std::sync::Arc;
use crate::python::datafusion_error_to_python::{DataFusionErrorToPython, DataFusionResultToPython};
use crate::python::python_resources::PythonResources;

// A rust wrapper of a python ray actor class. Makes it easier to call python methods.
pub struct RemoteProcessor {
    processor_object: Arc<Py<PyAny>>,
    python_resources: Arc<PythonResources>,
    addr: String,
}

impl RemoteProcessor {
    pub async fn start(
        python_resources: Arc<PythonResources>,
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Self> {
        let resources = python_resources.clone();
        let (processor_object, addr) = resources.with_python(|processor_class, ray_get| {
            let processor_object = Self::init_remote(processor_class, remote_checkpoint_dir)?;
            let addr = ray_get.call1((Self::start_up_remote(&processor_object)?, ))?
                .extract::<String>()?;
            
            Ok((processor_object.unbind(), addr))
        }).await?;

        Ok(RemoteProcessor {
            processor_object: Arc::new(processor_object),
            python_resources,
            addr,
        })
    }

    pub async fn start_many(
        python_resources: Arc<PythonResources>,
        num_processors: usize,
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Vec<Self>> {
        let processors = python_resources.with_python({
            let python_resources = python_resources.clone();
            move |processor_class, ray_get| {
                println!("About to start {} processors", num_processors);
                // Initialize all processors
                let processor_objects = (0..num_processors)
                    .map(|_| Self::init_remote(processor_class, remote_checkpoint_dir.clone()))
                    .collect::<PyResult<Vec<_>>>()?;

                // Wait for them to be ready
                let addr = ray_get.call1((
                    processor_objects.iter()
                        .map(|processor| Self::start_up_remote(processor))
                        .collect::<PyResult<Vec<_>>>()?,
                ))?.extract::<Vec<String>>()?;

                let processors = processor_objects.into_iter()
                    .zip(addr.into_iter())
                    .map(|(processor_object, addr)| Self::new(processor_object.unbind(), python_resources.clone(), addr))
                    .collect();
                Ok(processors)
            }
        }).await?;
        
        Ok(processors)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    fn init_remote<'py>(processor_class: &'py Bound<'py, PyAny>, remote_checkpoint_dir: Option<String>) -> PyResult<Bound<'py, PyAny>> {
        processor_class.getattr("remote")?.call1((remote_checkpoint_dir, ))
    }

    fn start_up_remote<'py>(processor: &'py Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        processor.getattr("start_up")?.getattr("remote")?.call0()
    }

    fn new(
        processor_object: Py<PyAny>,
        python_pool: Arc<PythonResources>,
        addr: String,
    ) -> Self {
        RemoteProcessor {
            processor_object: Arc::new(processor_object),
            python_resources: python_pool,
            addr,
        }
    }

    async fn start_up(&self) -> PyResult<String> {
        let processor_object = Python::with_gil(|py| self.processor_object.clone_ref(py));
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<String> {
                let processor_bound = processor_object.bind(py);
                processor_bound.getattr("start_up")?.call0()?.extract()
            })
        }).await.unwrap()
    }

    pub async fn update_plan(
        &self,
        task_definition2: &TaskDefinition,
        initial_scheduling_details: &InitialSchedulingDetails,
    ) -> PyResult<()> {
        let task_bytes = task_definition2.to_flexbuffers_bytes().to_python_result()?;
        let detail_bytes = initial_scheduling_details.to_flexbuffers_bytes().to_python_result()?;

        let processor_object = self.processor_object.clone();
        self.python_resources.with_python(move |_processor_class, _ray_get| {
            let task_bytes_cow: Cow<[u8]> = Cow::Owned(task_bytes);
            let detail_bytes_cow: Cow<[u8]> = Cow::Owned(detail_bytes);
            Python::with_gil(|py| {
                processor_object.bind(py)
                    .getattr("update_plan")?
                    .getattr("remote")?
                    .call1((task_bytes_cow, detail_bytes_cow))?;
                Ok(())
            })
        }).await
    }

    pub async fn update_plan_with_checkpoint(
        &self,
        task_definition2: &TaskDefinition,
        initial_scheduling_details: &InitialSchedulingDetails,
        checkpoint: usize,
    ) -> PyResult<()> {
        let task_bytes = task_definition2.to_flexbuffers_bytes().to_python_result()?;
        let detail_bytes = initial_scheduling_details.to_flexbuffers_bytes().to_python_result()?;

        let processor_object = self.processor_object.clone();
        self.python_resources.with_python(move |_processor_class, _ray_get| {
            let task_bytes_cow: Cow<[u8]> = Cow::Owned(task_bytes);
            let detail_bytes_cow: Cow<[u8]> = Cow::Owned(detail_bytes);
            Python::with_gil(|py| {
                processor_object.bind(py)
                    .getattr("update_plan_with_checkpoint")?
                    .getattr("remote")?
                    .call1((task_bytes_cow, detail_bytes_cow, checkpoint))?;
                Ok(())
            })
        }).await
    }
}
