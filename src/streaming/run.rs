use std::ops::Deref;
use std::sync::Arc;
use futures_util::StreamExt;
use pyo3::PyResult;
use datafusion::common::record_batch;
use crate::python::compute_runtime::ComputeRuntime;
use crate::streaming::coordinator::ActiveCoordinator;
use crate::streaming::model::stream_item::{Marker, StreamItem};
use crate::streaming::operators::count_by_key::CountByKeyOperator;
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::model::operator_definition::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::model::task_definition::TaskDefinition;

// Called by the py_entrypoint
pub async fn run(compute_runtime: Arc<ComputeRuntime>) -> PyResult<()> {
    println!("Running program");
    let remote_checkpoint_tempdir = tempfile::tempdir()?;
    assert!(remote_checkpoint_tempdir.path().to_str().is_some(), "Temporary directory path is not valid UTF-8");

    let tasks = infinite_stream_tasks();
    let coordinator = ActiveCoordinator::start_single_copies(
        compute_runtime.clone(),
        tasks,
        remote_checkpoint_tempdir.path().to_string_lossy().deref().to_string(),
    ).await?;

    // let coordinator = StaticCoordinator::new(runtime_env.clone());
    // let running_tasks = coordinator.schedule_together(&tasks).await?;

    // let (output_task, output_addr, _processor) = running_tasks.last()
    //     .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("No tasks assigned"))?;
    // let outputs = output_task.exchange_outputs();
    // let last_output = outputs.last().unwrap();
    // let stream = stream_results(output_addr, last_output)
    //     .await
    //     .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Error streaming results: {}", e)))?;
    // let mut stream = Box::pin(stream.enumerate());

    let mut stream = Box::pin(coordinator.stream_last_output().await?.enumerate());
    while let Some((index, result)) = stream.next().await {
        println!("Received item {}: {:?}", index, result);
    }

    println!("Finished processing all items in the stream");

    Ok(())
}

pub fn infinite_stream_tasks() -> Vec<TaskDefinition> {
    let test_batch = record_batch!(
        ("a", UInt64, vec![1u64, 2, 3, 1, 2, 1])
    ).unwrap();
    let task = TaskDefinition {
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
                        spec: OperatorSpec::Source(SourceOperator::new_with_iterations(
                            vec![
                                StreamItem::Marker(Marker { checkpoint_number: 1 }),
                                StreamItem::RecordBatch(test_batch.clone()),
                                StreamItem::Marker(Marker { checkpoint_number: 2 }),
                            ],
                            10,
                            std::time::Duration::from_secs(3),
                        )),
                        inputs: vec![],
                        outputs: vec![OperatorOutput {
                            stream_id: "output1".to_string(),
                            ordinal: 0,
                        }],
                    },
                    OperatorDefinition {
                        id: "count1".to_string(),
                        state_id: "4".to_string(),
                        spec: OperatorSpec::CountByKey(CountByKeyOperator::new("a".to_string())),
                        inputs: vec![OperatorInput {
                            stream_id: "output1".to_string(),
                            ordinal: 0,
                        }],
                        outputs: vec![OperatorOutput {
                            stream_id: "output2".to_string(),
                            ordinal: 0,
                        }],
                    },
                    OperatorDefinition {
                        id: "exchange1".to_string(),
                        state_id: "3".to_string(),
                        spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
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
        }
    };
    vec![task]
}

pub fn get_tasks_inner() -> Vec<TaskDefinition> {
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
    vec![source_task, exchange_task]
}
