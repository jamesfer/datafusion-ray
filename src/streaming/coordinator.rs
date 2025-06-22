use crate::python::compute_runtime::ComputeRuntime;
use crate::python::remote_processor::RemoteProcessor;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails, RemoteStreamLocation};
use crate::streaming::model::sitem::SItem;
use crate::streaming::model::task_definition::TaskDefinition;
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::runtime::create_remote_stream::create_remote_stream_no_runtime;
use crate::streaming::state::checkpoint_storage::FileSystemStateStorage;
use crate::streaming::state::file_system::PrefixedLocalFileSystemStorage;
use crate::streaming::utils::retry::retry_future;
use crate::streaming::worker_process::InitialSchedulingDetails;
use arrow_array::RecordBatch;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures::stream::{StreamExt, TryStreamExt};
use futures::Stream;
use futures_util::stream::FuturesOrdered;
use pyo3::{PyResult, PyErr};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub struct StaticCoordinator {
    compute_runtime: Arc<ComputeRuntime>,
}

impl StaticCoordinator {
    pub fn new(compute_runtime: Arc<ComputeRuntime>) -> Self {
        Self { compute_runtime }
    }

    pub async fn schedule_together<'a>(
        &'_ self,
        tasks: &'a [TaskDefinition],
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Vec<(&'a TaskDefinition, String, RemoteProcessor)>> {
        let processors = self.compute_runtime.start_many_processors(tasks.len(), remote_checkpoint_dir).await?;
        println!("Processors started: {}", processors.len());

        let assigned_tasks = tasks.into_iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_without_partitions_inner(&assigned_tasks);

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        println!("Tasks deployed");

        Ok(assigned_tasks.into_iter()
            .zip(processors.into_iter())
            .map(|((task, addr), processor)| (task, addr, processor))
            .collect())
    }
}

pub struct ActiveCoordinator {
    compute_runtime: Arc<ComputeRuntime>,
    remote_checkpoint_storage: Arc<FileSystemStateStorage>,
    remote_checkpoint_dir: String,
    tasks: Vec<(TaskDefinition, Vec<(String, RemoteProcessor, PartitionRange)>)>,
}

impl ActiveCoordinator {
    pub async fn start_single_copies(
        compute_runtime: Arc<ComputeRuntime>,
        tasks: Vec<TaskDefinition>,
        remote_checkpoint_dir: String,
    ) -> PyResult<Self> {
        let local_fs = object_store::local::LocalFileSystem::new_with_prefix(&remote_checkpoint_dir)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create LocalFileSystem: {}", e)))?;
        let remote_checkpoint_storage = Arc::new(FileSystemStateStorage::new(
            Arc::new(local_fs),
        ));
        let processors = compute_runtime.start_many_processors(tasks.len(), Some(remote_checkpoint_dir.clone())).await?;

        let assigned_tasks = tasks.iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_with_constant_partitions(&assigned_tasks, PartitionRange::full());

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        Ok(Self {
            compute_runtime,
            remote_checkpoint_storage,
            remote_checkpoint_dir,
            tasks: assigned_tasks.into_iter()
                .zip(processors.into_iter())
                .map(|((task, addr), processor)| (task.clone(), vec![(addr, processor, PartitionRange::full())]))
                .collect()
        })
    }

    pub async fn stream_last_output(&self) -> PyResult<impl Stream<Item=Result<RecordBatch, DataFusionError>> + use<>> {
        let (output_task, copies) = self.tasks.last()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("No tasks assigned"))?;
        if copies.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err("Expected exactly one copy of the output task"));
        }

        let (output_addr, _processor, _partition_range) = copies.first().unwrap();
        let outputs = output_task.exchange_outputs();
        let last_output = outputs.last().unwrap();
        let stream = stream_results(&output_addr, last_output)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Error streaming results: {}", e)))?;
        Ok(stream)
    }

    pub async fn scale_up(&mut self, task_id: &str, new_number: usize) -> PyResult<()> {
        // Find the task to scale up
        let (task_def, copies) = self.tasks.iter()
            .find(|(task, _)| task.task_id == task_id)
            .ok_or(pyo3::exceptions::PyValueError::new_err(format!("Task with ID {} not found", task_id)))?;

        // Create n copies of the task with the new partitioning
        // When scaling we always use 2^32 partitions as maybe there is no reason to use any other
        // number
        let new_partition_cap = 2usize^32;
        let partition_size_floor = new_partition_cap / new_number;
        let remaining_size = new_partition_cap % new_number;
        let partitions = (0..new_number).map(|i| {
            let size = partition_size_floor + if i < remaining_size { 1 } else { 0 };
            let start = i * partition_size_floor + if i < remaining_size { i } else { remaining_size };
            PartitionRange::new(start, start + size, new_partition_cap)
        }).collect::<Vec<_>>();

        // Find the most recently completed checkpoint for each partition
        let state_ids = task_def.used_state_ids();
        // TODO support multiple stats here
        if state_ids.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Scaling up is only supported for tasks with a single state ID",
            ));
        }
        let task_state_id = &state_ids[0];
        let partitions = partitions.into_iter()
            .map(|partition| {
                let remote_checkpoint_storage = self.remote_checkpoint_storage.clone();
                async move {
                    let (checkpoint_id, _) = remote_checkpoint_storage.get_latest_state_checkpoint_parts(
                        task_state_id,
                        &partition,
                    ).await?;
                    // TODO the checkpoint_id should eventually be a string everywhere
                    // Parse checkpoint id to usize
                    let checkpoint_id = checkpoint_id.parse::<usize>()
                        .map_err(|e| internal_datafusion_err!("Failed to parse checkpoint ID: {}", e))?;
                    Ok::<(PartitionRange, usize), DataFusionError>((partition, checkpoint_id))
                }
            })
            .collect::<FuturesOrdered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        let processors = self.compute_runtime.start_many_processors(new_number, Some(self.remote_checkpoint_dir.clone())).await?;
        let assigned_tasks = partitions.into_iter()
            .zip(processors.into_iter())
            .map(|((partition, checkpoint), processor)| {
                (processor.addr().to_string(), processor, partition, checkpoint)
            })
            .collect::<Vec<_>>();

        // Start each of the tasks
        let input_locations = self.tasks.iter()
            .filter(|(task, _)| task.task_id != task_id)
            .flat_map(|(task, copies)| {
                // For each task output, create a generation input detail
                task.exchange_outputs()
                    .iter()
                    .map(|stream_id| {
                        RemoteStreamDetails {
                            stream_id: stream_id.clone(),
                            locations: copies.iter().map(|(addr, _, partition_range)| RemoteStreamLocation {
                                address: addr.clone(),
                                offset_range: (0, 2 << 31),
                                partitions: partition_range.clone(),
                            }).collect(),
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for (addr, processor, partition, checkpoint) in assigned_tasks.iter() {
            println!("Scaling up task {} to processor {} with partition {:?}", task_def.task_id, addr, partition);
            let generation = GenerationSpec {
                id: format!("{}-{}", task_def.task_id, addr),
                partitions: partition.clone(),
                start_conditions: vec![],
            };
            processor.update_plan_with_checkpoint(
                task_def,
                &InitialSchedulingDetails {
                    generations: vec![generation],
                    input_locations: input_locations.clone(),
                },
                *checkpoint,
            ).await?;
        }

        Ok(())
    }
}

struct CoordinatorLogger {
    active_coordinator: Arc<ActiveCoordinator>,
    background_handle: Option<JoinHandle<()>>,
}

impl CoordinatorLogger {
    pub fn start(active_coordinator: Arc<ActiveCoordinator>) -> Self {
        let background_handle = tokio::spawn({
            let active_coordinator = active_coordinator.clone();
            async move {
                Self::print_loop(active_coordinator).await;
            }
        });
        Self {
            active_coordinator,
            background_handle: Some(background_handle),
        }
    }

    async fn print_loop(active_coordinator: Arc<ActiveCoordinator>) {
        // Loop forever until the tokio task is cancelled
        loop {
            // Log the tasks and their copies
            Self::log_tasks(active_coordinator.clone());

            // Sleep for a while before logging again
            sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    pub fn log_tasks(active_coordinator: Arc<ActiveCoordinator>) {
        for (task, copies) in &active_coordinator.tasks {
            println!("Task ID: {}", task.task_id);
            for (addr, _, partition_range) in copies {
                println!("  Processor: {}, Partition Range: {:?}", addr, partition_range);
            }
        }
    }
}

impl Drop for CoordinatorLogger {
    fn drop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

pub async fn stream_results(address: &str, stream_id: &str) -> Result<impl Stream<Item=Result<RecordBatch, DataFusionError>> + use<>, DataFusionError> {
    let stream = retry_future(5, || {
        create_remote_stream_no_runtime(
            &stream_id,
            &address,
            PartitionRange::empty(),
        )
    }).await?;
    let stream = Box::into_pin(stream);
    let stream = stream.map(|result| {
        // Use a match statement to print each value of result
        match result {
            Ok(SItem::RecordBatch(record_batch)) => {
                println!("Collect (py utils) Received record batch: {:?}", record_batch);
                Ok(SItem::RecordBatch(record_batch))
            },
            Ok(SItem::Marker(marker)) => {
                println!("Collect (py utils) Received marker: {}", marker.checkpoint_number);
                Ok(SItem::Marker(marker))
            },
            Ok(SItem::Generation(usize)) => {
                println!("Collect (py utils) Received generation item");
                Ok(SItem::Generation(usize))
            },
            Err(err) => {
                println!("Collect (py utils) Error in stream: {}", err);
                Err(err)
            },
        }
    });

    let results = stream
        .filter_map(|item| async move {
            match item {
                Err(e) => Some(Err(e)),
                Ok(SItem::RecordBatch(record_batch)) => Some(Ok(record_batch)),
                _ => None,
            }
        });
    Ok(results)
}

pub fn schedule_without_partitions_inner(assigned_tasks: &[(&TaskDefinition, String)]) -> InitialSchedulingDetails {
    let initial_generation = GenerationSpec {
        id: "initial_generation".to_string(),
        partitions: PartitionRange::empty(),
        start_conditions: vec![],
    };
    let input_details = assigned_tasks.iter()
        .flat_map(|(task, address)| {
            task.exchange_outputs()
                .into_iter()
                .map(|stream_id| {
                    RemoteStreamDetails {
                        stream_id,
                        locations: vec![RemoteStreamLocation {
                            address: address.clone(),
                            offset_range: (0, 2 << 31),
                            partitions: PartitionRange::empty(),
                        }],
                    }
                })
        })
        .collect::<Vec<_>>();
    InitialSchedulingDetails {
        input_locations: input_details,
        generations: vec![initial_generation],
    }
}

pub fn schedule_with_constant_partitions(assigned_tasks: &[(&TaskDefinition, String)], partition: PartitionRange) -> InitialSchedulingDetails {
    let initial_generation = GenerationSpec {
        id: "initial_generation".to_string(),
        partitions: partition.clone(),
        start_conditions: vec![],
    };
    let input_details = assigned_tasks.iter()
        .flat_map(|(task, address)| {
            task.exchange_outputs()
                .into_iter()
                .map(|stream_id| {
                    RemoteStreamDetails {
                        stream_id,
                        locations: vec![RemoteStreamLocation {
                            address: address.clone(),
                            offset_range: (0, 2 << 31),
                            partitions: partition.clone(),
                        }],
                    }
                })
        })
        .collect::<Vec<_>>();
    InitialSchedulingDetails {
        input_locations: input_details,
        generations: vec![initial_generation],
    }
}
