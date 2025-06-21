// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::pin::Pin;
use std::sync::Arc;

use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion_python::utils::wait_for_future;
use futures::{StreamExt, TryStreamExt};

use futures_util::FutureExt;
use itertools::Itertools;
use pyo3::prelude::*;

use crate::streaming::model::task_definition::TaskDefinition;
use crate::streaming::worker_process::{InitialSchedulingDetails, WorkerProcess};
use crate::util::ResultExt;
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::streaming::state::checkpoint_storage::FileSystemStateStorage;
use crate::streaming::state::file_system::PrefixedLocalFileSystemStorage;
use object_store::local::LocalFileSystem;

enum WorkerProcessState {
    // The future needs to be sync and send because it is used by pyclass structs, which need to be
    Starting {
        future: Pin<Box<dyn Future<Output = Result<WorkerProcess, DataFusionError>> + Send + Sync>>,
        still_starting_error: DataFusionError,
    },
    Running(WorkerProcess),
    Errored(DataFusionError),
}

impl WorkerProcessState {
    pub fn new(future: Pin<Box<dyn Future<Output = Result<WorkerProcess, DataFusionError>> + Send + Sync>>) -> Self {
        Self::Starting {
            future,
            still_starting_error: internal_datafusion_err!("Worker process is still starting"),
        }
    }

    pub fn expect_started(&self) -> Result<&WorkerProcess, &DataFusionError> {
        match self {
            WorkerProcessState::Starting { still_starting_error, .. } => Err(still_starting_error),
            WorkerProcessState::Running(worker_process) => Ok(worker_process),
            WorkerProcessState::Errored(error) => Err(error),
        }
    }

    pub async fn wait_for_startup(&mut self) -> Result<&mut WorkerProcess, &mut DataFusionError> {
        match self {
            WorkerProcessState::Starting { future, .. } => {
                match future.await {
                    Ok(worker_process) => {
                        *self = WorkerProcessState::Running(worker_process);
                        match self {
                            WorkerProcessState::Running(process) => Ok(process),
                            _ => unreachable!(),
                        }
                    }
                    Err(error) => {
                        *self = WorkerProcessState::Errored(error);
                        match self {
                            WorkerProcessState::Errored(error) => Err(error),
                            _ => unreachable!(),
                        }
                    }
                }
            }
            WorkerProcessState::Running(worker_process) => Ok(worker_process),
            WorkerProcessState::Errored(error) => Err(error),
        }
    }
}

// Rust interface used by python to start tasks
#[pyclass]
pub struct DFRayStreamingProcessorService {
    worker_process: WorkerProcessState,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

#[pymethods]
impl DFRayStreamingProcessorService {
    #[new]
    pub fn new(name: String, remote_storage_url: Option<String>) -> PyResult<Self> {
        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let worker_process_fut = match remote_storage_url {
            None => Box::pin(WorkerProcess::start(name.clone()))
                as Pin<Box<dyn Future<Output = Result<WorkerProcess, DataFusionError>> + Send + Sync>>,
            Some(url) => {
                let object_store = Arc::new(
                    LocalFileSystem::new_with_prefix(&url)
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create LocalFileSystem: {}", e)))?
                );
                let remote_state_store = FileSystemStateStorage::new(object_store);
                Box::pin(WorkerProcess::start_with_remote_file_system(name.clone(), Arc::new(remote_state_store)))
                    as Pin<Box<dyn Future<Output = Result<WorkerProcess, DataFusionError>> + Send + Sync>>
            },
        };

        let worker_process_state = WorkerProcessState::new(worker_process_fut);

        Ok(Self {
            worker_process: worker_process_state,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// Explicitly wait for the underlying worker process to start up. This is separate from new()
    /// because Ray does not let you wait (AFAICT) on Actor init methods to complete, and we will
    /// want to wait on this with ray.get()
    pub fn start_up(&mut self, py: Python) -> PyResult<()> {
        wait_for_future(py, self.worker_process.wait_for_startup())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Worker process failed to start {e}"),
            ))?;
        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> PyResult<String> {
        Ok(self.worker_process.expect_started()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Worker process is still starting {e}"),
            ))?
            .data_exchange_address()
            .to_string())
    }

    /// signal to the service that we can shutdown
    ///
    /// returns a python coroutine that should be awaited
    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    pub fn start_task(
        &self,
        py: Python,
        task_bytes: &[u8],
        initial_scheduling_details_bytes: &[u8],
    ) -> PyResult<()> {
        let worker_process = self.worker_process.expect_started()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Worker process is still starting {e}"),
            ))?;

        let reader = flexbuffers::Reader::get_root(task_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to parse task bytes: {e}"),
            ))?;
        let task_definition = TaskDefinition::deserialize(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to deserialize task definition: {e}"),
            ))?;

        let reader = flexbuffers::Reader::get_root(initial_scheduling_details_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to parse scheduling details bytes: {e}"),
            ))?;
        let initial_scheduling_details = InitialSchedulingDetails::deserialize(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to deserialize task definition: {e}"),
            ))?;

        wait_for_future(py, worker_process.start_task(task_definition, initial_scheduling_details))
            .to_py_err()?;

        Ok(())
    }

    pub fn start_task_from(
        &self,
        py: Python,
        task_bytes: &[u8],
        initial_scheduling_details_bytes: &[u8],
        checkpoint_number: usize,
    ) -> PyResult<()> {
        let worker_process = self.worker_process.expect_started()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Worker process is still starting {e}"),
            ))?;

        let reader = flexbuffers::Reader::get_root(task_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to parse task bytes: {e}"),
            ))?;
        let task_definition = TaskDefinition::deserialize(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to deserialize task definition: {e}"),
            ))?;

        let reader = flexbuffers::Reader::get_root(initial_scheduling_details_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to parse scheduling details bytes: {e}"),
            ))?;
        let initial_scheduling_details = InitialSchedulingDetails::deserialize(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(
                format!("Failed to deserialize task definition: {e}"),
            ))?;

        wait_for_future(py, worker_process.start_task_from(
            task_definition,
            initial_scheduling_details,
            checkpoint_number,
        ))
            .to_py_err()?;

        Ok(())
    }

    // /// replace the plan that this service was providing, we will do this when we want
    // /// to reuse the DFRayProcessorService for a subsequent query
    // ///
    // /// returns a python coroutine that should be awaited
    // pub fn update_plan<'a>(
    //     &self,
    //     py: Python<'a>,
    //     stage_id: usize,
    //     output_stream_address_map: HashMap<String, String>,
    //     plan_bytes: &[u8],
    // ) -> PyResult<Bound<'a, PyAny>> {
    //     let task_def = self.worker_process.parse_task_definition(plan_bytes).to_py_err()?;
    // 
    //     debug!(
    //         "{} Received New Plan: Stage:{} my addr: {}, output address map:\n{:?}\nplan:\nTODO",
    //         self.name,
    //         stage_id,
    //         self.addr()?,
    //         output_stream_address_map,
    //         // display_plan_with_partition_counts(&plan)
    //     );
    // 
    //     let worker = self.worker_process.clone();
    //     let name = self.name.clone();
    //     let fut = async move {
    //         worker
    //             // TODO add input spec
    //             .start_task(stage_id, output_stream_address_map, task_def)
    //             .await
    //             .to_py_err()?;
    //         info!(
    //             "{} [stage: {}] updated plan",
    //             name, stage_id
    //         );
    //         Ok(())
    //     };
    // 
    //     pyo3_async_runtimes::tokio::future_into_py(py, fut)
    // }

    // /// start the service
    // /// returns a python coroutine that should be awaited
    // pub fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
    //     let mut all_done_rx = self.all_done_rx.take().unwrap();
    // 
    //     let signal = async move {
    //         all_done_rx
    //             .recv()
    //             .await
    //             .expect("problem receiving shutdown signal");
    //     };
    // 
    //     let service = FlightServ {
    //         handler: self.worker_process.make_flight_handler(),
    //     };
    //     let svc = FlightServiceServer::new(service);
    // 
    //     let listener = self.listener.take().unwrap();
    //     let name = self.name.clone();
    // 
    //     let serv = async move {
    //         Server::builder()
    //             .add_service(svc)
    //             .serve_with_incoming_shutdown(
    //                 tokio_stream::wrappers::TcpListenerStream::new(listener),
    //                 signal,
    //             )
    //             .await
    //             .inspect_err(|e| error!("{}, ERROR serving {e}", name))
    //             .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
    //         Ok::<(), Box<dyn Error + Send + Sync>>(())
    //     };
    // 
    //     let fut = async move {
    //         serv.await.to_py_err()?;
    //         Ok(())
    //     };
    // 
    //     pyo3_async_runtimes::tokio::future_into_py(py, fut)
    // }
}
