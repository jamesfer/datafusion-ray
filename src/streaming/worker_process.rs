use crate::flight::FlightHandler;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::runtime::Runtime;
use crate::streaming::model::task_definition::TaskDefinition;
use crate::streaming::task_main_3::RunningTask;
use crate::streaming::utils::test_utils::make_temp_dir;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use local_ip_address::local_ip;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use crate::streaming::state::remote_checkpoint_storage::RemoteCheckpointStorage;
use crate::streaming::state::file_system::{FileSystemStorage, TempdirFileSystemStorage};
use object_store::local::LocalFileSystem;
use crate::streaming::model::scheduling_details::SchedulingDetailsUpdate;

#[derive(Clone, Serialize, Deserialize)]
pub struct InitialSchedulingDetails {
    pub generations: Vec<GenerationSpec>,
    pub input_locations: Vec<RemoteStreamDetails>,
}

pub struct WorkerProcess {
    name: String,
    runtime: Arc<Runtime>,
    running_tasks: RwLock<HashMap<String, RunningTask>>,
}

impl WorkerProcess {
    pub async fn start(name: String) -> Result<Self, DataFusionError> {
        let temp_dir = tempfile::tempdir()?;
        let remote_object_store = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path())
                .map_err(|e| internal_datafusion_err!("Failed to create LocalFileSystem: {}", e))?
        );
        let remote_state_store = Arc::new(RemoteCheckpointStorage::new(
            remote_object_store,
        ));
        Self::start_with_remote_file_system(
            name,
            remote_state_store,
        ).await
    }

    pub async fn start_with_remote_file_system(name: String, remote_state_store: Arc<RemoteCheckpointStorage>) -> Result<Self, DataFusionError> {
        let name = format!("[{}]", name);
        let local_ip_addr = local_ip()
            .map_err(|err| internal_datafusion_err!("Failed to get worker local ip: {}", err))?;

        // File system stores are fixed for now
        let dir = make_temp_dir(format!("{}-local", name))?;
        println!("Worker starting with local state: {}", dir.path().display());
        let local_file_system = Arc::new(TempdirFileSystemStorage::from_tempdir(dir));

        let runtime = Arc::new(Runtime::start(
            local_ip_addr,
            local_file_system,
            remote_state_store,
        ).await?);

        Ok(Self {
            name,
            runtime,
            running_tasks: RwLock::new(HashMap::new()),
        })
    }

    pub fn data_exchange_address(&self) -> &str {
        self.runtime.data_exchange_manager().get_exchange_address()
    }

    pub async fn start_task(
        &self,
        task_definition: TaskDefinition,
        initial_scheduling_details: InitialSchedulingDetails,
    ) -> Result<(), DataFusionError> {
        self.start_task_from(
            task_definition,
            initial_scheduling_details,
            0, // initial checkpoint
        ).await
    }

    pub async fn start_task_from(
        &self,
        task_definition: TaskDefinition,
        initial_scheduling_details: InitialSchedulingDetails,
        checkpoint: usize
    ) -> Result<(), DataFusionError> {
        let running_task = RunningTask::start(
            task_definition.task_id.clone(),
            task_definition.operator,
            self.runtime.clone(),
            checkpoint,
            initial_scheduling_details.input_locations,
            initial_scheduling_details.generations,
        ).await?;

        let mut running_tasks = self.running_tasks.write();
        match running_tasks.entry(task_definition.task_id.clone()) {
            Entry::Occupied(_) => {
                running_task.cancel();
                Err(internal_datafusion_err!(
                    "Task with ID {} is already running",
                    task_definition.task_id
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(running_task);
                Ok(())
            },
        }
    }

    pub async fn update_input(&self, task_id: String, task_scheduling_details_update: SchedulingDetailsUpdate) -> Result<(), DataFusionError> {
        let running_tasks = self.running_tasks.read();
        match running_tasks.get(&task_id) {
            None => {
                Err(internal_datafusion_err!(
                    "No running task found with ID {}",
                    task_id
                ))
            }
            Some(running_task) => {
                running_task.update_scheduling_details(task_scheduling_details_update).await;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails, RemoteStreamLocation};
    use crate::streaming::operators::count_star::CountStarOperator;
    use crate::streaming::operators::nested::NestedOperator;
    use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
    use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
    use crate::streaming::operators::source::SourceOperator;
    use crate::streaming::partitioning::{PartitionRange, PartitioningSpec};
    use crate::streaming::model::task_definition::TaskDefinition;
    use crate::streaming::utils::retry::retry_future;
    use crate::streaming::utils::test_utils::make_temp_dir;
    use crate::streaming::worker_process::{InitialSchedulingDetails, WorkerProcess};
    use datafusion::common::{record_batch, DataFusionError};
    use datafusion::physical_expr::expressions::col;
    use futures::{Stream, StreamExt};
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::{join, try_join};
    use crate::streaming::model::operator_definition::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
    use crate::streaming::model::sitem::SItem;
    use crate::streaming::model::stream_item::{Marker, StreamItem};
    use crate::streaming::runtime::create_remote_stream::create_remote_stream_no_runtime;
    use crate::streaming::state::remote_checkpoint_storage::RemoteCheckpointStorage;

    #[tokio::test]
    pub async fn single_output_task() {
        let worker = WorkerProcess::start("worker1".to_string()).await.unwrap();

        let test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let task_definition = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new(vec![test_batch.clone()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        worker.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let address = worker.data_exchange_address();

        let results_stream = retry_future(5, || {
            create_remote_stream_no_runtime(
                "exchange_output1",
                address,
                PartitionRange::empty(),
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).take(1).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::RecordBatch(test_batch));
    }

    #[tokio::test]
    pub async fn two_linked_tasks() {
        let worker1 = WorkerProcess::start("worker1".to_string()).await.unwrap();
        let worker2 = WorkerProcess::start("worker2".to_string()).await.unwrap();

        let test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let source_task = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::Marker(Marker { checkpoint_number: 1 }),
                                StreamItem::RecordBatch(test_batch.clone()),
                                StreamItem::Marker(Marker { checkpoint_number: 2 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        let exchange_task = TaskDefinition {
            task_id: "task2".to_string(),
            operator: OperatorDefinition {
                id: "nested2".to_string(),
                state_id: "4".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "exchange2".to_string(),
                            state_id: "5".to_string(),
                            spec: OperatorSpec::RemoteExchangeInput(RemoteSourceOperator::new(vec!["exchange_output1".to_string()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange3".to_string(),
                            state_id: "6".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output2".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        let address1 = worker1.data_exchange_address();
        let address2 = worker2.data_exchange_address();
        worker1.start_task(
            source_task,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();
        worker2.start_task(
            exchange_task,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![RemoteStreamDetails {
                    stream_id: "exchange_output1".to_string(),
                    locations: vec![RemoteStreamLocation {
                        address: address1.to_string(),
                        offset_range: (0, 2 << 31),
                        partitions: PartitionRange::empty(),
                    }],
                }],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let results_stream = retry_future(5, || {
            create_remote_stream_no_runtime(
                "exchange_output2",
                address2,
                PartitionRange::empty(),
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).take(3).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
        assert_eq!(results[1].as_ref().unwrap(), &SItem::RecordBatch(test_batch));
        assert_eq!(results[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
    }

    #[tokio::test]
    pub async fn partitioned_output() {
        let worker1 = WorkerProcess::start("worker1".to_string()).await.unwrap();
        let worker2 = WorkerProcess::start("worker2".to_string()).await.unwrap();
        let worker3 = WorkerProcess::start("worker3".to_string()).await.unwrap();

        let test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3]),
            ("b", Int32, vec![111i32, 222, 333])
        ).unwrap();
        let source_task = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::Marker(Marker { checkpoint_number: 1 }),
                                StreamItem::RecordBatch(test_batch.clone()),
                                StreamItem::Marker(Marker { checkpoint_number: 2 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new_with_partitioning(
                                "exchange_output1".to_string(),
                                test_batch.schema(),
                                PartitioningSpec {
                                    expressions: vec![col("a", test_batch.schema_ref().as_ref()).unwrap()],
                                },
                            )),
                            inputs: vec![OperatorInput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        let exchange_task1 = TaskDefinition {
            task_id: "task2".to_string(),
            operator: OperatorDefinition {
                id: "nested2".to_string(),
                state_id: "4".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "exchange2".to_string(),
                            state_id: "5".to_string(),
                            spec: OperatorSpec::RemoteExchangeInput(RemoteSourceOperator::new(vec!["exchange_output1".to_string()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange3".to_string(),
                            state_id: "6".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output2".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        let exchange_task2 = TaskDefinition {
            task_id: "task3".to_string(),
            operator: OperatorDefinition {
                id: "nested3".to_string(),
                state_id: "7".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "exchange4".to_string(),
                            state_id: "8".to_string(),
                            spec: OperatorSpec::RemoteExchangeInput(RemoteSourceOperator::new(vec!["exchange_output1".to_string()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output3".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange5".to_string(),
                            state_id: "9".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output3".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output3".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        let address1 = worker1.data_exchange_address();
        let address2 = worker2.data_exchange_address();
        let address3 = worker3.data_exchange_address();

        worker1.start_task(
            source_task,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::with_max_partitions(0, 2, 2),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        let input_detail = RemoteStreamDetails {
            stream_id: "exchange_output1".to_string(),
            locations: vec![RemoteStreamLocation {
                address: address1.to_string(),
                offset_range: (0, 2 << 31),
                partitions: PartitionRange::with_max_partitions(0, 2, 2),
            }],
        };
        // This task takes 1 half of the partitions
        worker2.start_task(
            exchange_task1,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    // The generation dictates which partitions this task should process
                    partitions: PartitionRange::new_from_index(0, 2),
                    start_conditions: vec![],
                }],
                input_locations: vec![input_detail.clone()],
            },
        ).await.unwrap();
        // This task takes the other half of the partitions
        worker3.start_task(
            exchange_task2,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    // The generation dictates which partitions this task should process
                    partitions: PartitionRange::new_from_index(1, 2),
                    start_conditions: vec![],
                }],
                input_locations: vec![input_detail],
            },
        ).await.unwrap();

        // Pull the output each of the exchange operators. We don't need to specify a
        // partition range here, as they are independent streams
        let results_stream1_fut = retry_future(5, || async {
            create_remote_stream_no_runtime(
                "exchange_output2",
                address2,
                PartitionRange::empty(),
            ).await
                .map(Box::into_pin)
                .map(|stream| {
                    Box::pin(stream.map(|result| {
                        match &result {
                            Ok(SItem::RecordBatch(record_batch)) => {
                                println!("Stream1 Received record batch: {:?}", record_batch);
                            },
                            Ok(SItem::Marker(marker)) => {
                                println!("Stream1 Received marker: {:?}", marker);
                            },
                            _ => {}
                        }
                        result
                    })) as Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>>
                })
        });
        let results_stream2_fut = retry_future(5, || async {
            create_remote_stream_no_runtime(
                "exchange_output3",
                address3,
                PartitionRange::empty(),
            ).await
                .map(Box::into_pin)
                .map(|stream| {
                    Box::pin(stream.map(|result| {
                        match &result {
                            Ok(SItem::RecordBatch(record_batch)) => {
                                println!("Stream2 Received record batch: {:?}", record_batch);
                            },
                            Ok(SItem::Marker(marker)) => {
                                println!("Stream2 Received marker: {:?}", marker);
                            },
                            _ => {}
                        }
                        result
                    })) as Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>>
                })
        });

        // Wait for two futures to complete in parallel
        let (results_stream1, results_stream2) = try_join!(results_stream1_fut, results_stream2_fut).unwrap();
        let (results1, results2) = join!(results_stream1.collect::<Vec<_>>(), results_stream2.collect::<Vec<_>>());

        println!("Received results 1: {:?}", results1);
        println!("Received results 2: {:?}", results2);

        assert_eq!(results1.len(), 3);
        assert_eq!(results1[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
        assert_eq!(results1[1].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
            ("a", Int32, vec![3i32]),
            ("b", Int32, vec![333i32])
        ).unwrap()));
        assert_eq!(results1[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));

        assert_eq!(results2.len(), 3);
        assert_eq!(results2[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
        assert_eq!(results2[1].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
            ("a", Int32, vec![1i32, 2]),
            ("b", Int32, vec![111i32, 222])
        ).unwrap()));
        assert_eq!(results2[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
    }

    #[tokio::test]
    pub async fn count_task() {
        let worker = WorkerProcess::start("worker1".to_string()).await.unwrap();

        let first_test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let second_test_batch = record_batch!(
            ("a", Int32, vec![4i32, 5, 6])
        ).unwrap();

        let task_definition = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::from(Marker { checkpoint_number: 1 }),
                                StreamItem::from(first_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 2 }),
                                StreamItem::from(second_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 3 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "source_output".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "count1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::CountStar(CountStarOperator::new()),
                            inputs: vec![OperatorInput {
                                stream_id: "source_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![OperatorOutput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "4".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        worker.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let address = worker.data_exchange_address();

        let results_stream = retry_future(5, || {
            create_remote_stream_no_runtime(
                "exchange_output1",
                address,
                PartitionRange::empty(),
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
        assert_eq!(results[1].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
        assert_eq!(results[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 3 }));
        assert_eq!(results[3].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
            ("count", UInt64, vec![6])
        ).unwrap()));
    }

    #[tokio::test]
    pub async fn restarting_with_state() {
        let worker = WorkerProcess::start("worker1".to_string()).await.unwrap();
        let address = worker.data_exchange_address();

        let first_test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let second_test_batch = record_batch!(
            ("a", Int32, vec![4i32, 5, 6])
        ).unwrap();

        let task_definition = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::from(Marker { checkpoint_number: 1 }),
                                StreamItem::from(first_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 2 }),
                                StreamItem::from(second_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 3 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "source_output".to_string(), ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "count1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::CountStar(CountStarOperator::new()),
                            inputs: vec![OperatorInput {
                                stream_id: "source_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![OperatorOutput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "4".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        // Run the task for the first time
        worker.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        {
            let results_stream = retry_future(5, || {
                create_remote_stream_no_runtime(
                    "exchange_output1",
                    address,
                    PartitionRange::empty(),
                )
            }).await.unwrap();
            let results = Box::into_pin(results_stream).collect::<Vec<_>>().await;

            assert_eq!(results.len(), 4);
            assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
            assert_eq!(results[1].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
            assert_eq!(results[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 3 }));
            assert_eq!(results[3].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
                ("count", UInt64, vec![6])
            ).unwrap()));
        }


        let task_definition2 = TaskDefinition {
            task_id: "task1_2".to_string(),
            operator: OperatorDefinition {
                id: "nested1_2".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1_2".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::from(Marker { checkpoint_number: 1 }),
                                StreamItem::from(first_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 2 }),
                                StreamItem::from(second_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 3 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "source_output_2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "count1_2".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::CountStar(CountStarOperator::new()),
                            inputs: vec![OperatorInput {
                                stream_id: "source_output_2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![OperatorOutput {
                                stream_id: "count_output_2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1_2".to_string(),
                            state_id: "4".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1_2".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "count_output_2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        // Re-run the task from a checkpoint
        worker.start_task_from(
            task_definition2,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::empty(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
            2, // Restart from checkpoint 2
        ).await.unwrap();

        {
            let results_stream = retry_future(5, || {
                create_remote_stream_no_runtime(
                    "exchange_output1_2",
                    address,
                    PartitionRange::empty(),
                )
            }).await.unwrap();
            let results = Box::into_pin(results_stream).collect::<Vec<_>>().await;

            assert_eq!(results.len(), 3);
            assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
            assert_eq!(results[1].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 3 }));
            assert_eq!(results[2].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
                ("count", UInt64, vec![6])
            ).unwrap()));
        }
    }

    #[tokio::test]
    pub async fn restarting_with_remote_state() {
        let temp_dir = make_temp_dir("shared-remote").unwrap();
        let remote_file_system = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let remote_state_store = Arc::new(RemoteCheckpointStorage::new(
            remote_file_system.clone(),
        ));
        let worker1 = WorkerProcess::start_with_remote_file_system(format!("worker1-{}", uuid::Uuid::new_v4()), remote_state_store.clone()).await.unwrap();
        let worker2 = WorkerProcess::start_with_remote_file_system(format!("worker2-{}", uuid::Uuid::new_v4()), remote_state_store).await.unwrap();
        let address1 = worker1.data_exchange_address();
        let address2 = worker2.data_exchange_address();

        let first_test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let second_test_batch = record_batch!(
            ("a", Int32, vec![4i32, 5, 6])
        ).unwrap();

        let task_definition = TaskDefinition {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::from(Marker { checkpoint_number: 1 }),
                                StreamItem::from(first_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 2 }),
                                StreamItem::from(second_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 3 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "source_output".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "count1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::CountStar(CountStarOperator::new()),
                            inputs: vec![OperatorInput {
                                stream_id: "source_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![OperatorOutput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "4".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "count_output".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        // Run the task for the first time
        worker1.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::unit(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        {
            let results_stream = retry_future(5, || {
                create_remote_stream_no_runtime(
                    "exchange_output1",
                    address1,
                    PartitionRange::empty(),
                )
            }).await.unwrap();
            let results = Box::into_pin(results_stream).collect::<Vec<_>>().await;

            assert_eq!(results.len(), 4);
            assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
            assert_eq!(results[1].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
            assert_eq!(results[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 3 }));
            assert_eq!(results[3].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
                ("count", UInt64, vec![6])
            ).unwrap()));
        }


        let task_definition2 = TaskDefinition {
            task_id: "task1_2".to_string(),
            operator: OperatorDefinition {
                id: "nested1_2".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1_2".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::from(Marker { checkpoint_number: 1 }),
                                StreamItem::from(first_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 2 }),
                                StreamItem::from(second_test_batch.clone()),
                                StreamItem::from(Marker { checkpoint_number: 3 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "source_output_2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "count1_2".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::CountStar(CountStarOperator::new()),
                            inputs: vec![OperatorInput {
                                stream_id: "source_output_2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![OperatorOutput {
                                stream_id: "count_output_2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1_2".to_string(),
                            state_id: "4".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1_2".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "count_output_2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        // Re-run the task from a checkpoint
        worker2.start_task_from(
            task_definition2,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: PartitionRange::unit(),
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
            2, // Restart from checkpoint 2
        ).await.unwrap();

        {
            let results_stream = retry_future(5, || {
                create_remote_stream_no_runtime(
                    "exchange_output1_2",
                    address2,
                    PartitionRange::empty(),
                )
            }).await.unwrap();
            let results = Box::into_pin(results_stream).collect::<Vec<_>>().await;

            assert_eq!(results.len(), 3);
            assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
            assert_eq!(results[1].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 3 }));
            assert_eq!(results[2].as_ref().unwrap(), &SItem::RecordBatch(record_batch!(
                ("count", UInt64, vec![6])
            ).unwrap()));
        }
    }
}
