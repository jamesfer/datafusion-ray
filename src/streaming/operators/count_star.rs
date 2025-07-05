use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::model::sitem::SItem;
use crate::streaming::model::stream_item::Marker;
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::runtime::Runtime;
use crate::streaming::serialisation::state_serialisation::SerialiseToStateBytes;
use crate::streaming::state::latest_checkpoint_model_2::ParentState;
use crate::streaming::state::rocksdb_state_backend::RocksDBStateBackend;
use crate::streaming::utils::fiber_stream::{FiberStream, SingleFiberStream};
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, record_batch, DataFusionError};
use eyeball::{AsyncLock, SharedObservable};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct CountStarOperator {}

impl CountStarOperator {
    pub fn new() -> Self {
        CountStarOperator {}
    }
}

#[async_trait]
impl CreateOperatorFunction for CountStarOperator {
    async fn create_operator_function(
        &self,
        operator_id: &str,
        state_id: &str,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send> {
        let (generation, _) = scheduling_details.get().await;
        let initial_generation = generation.as_ref()
            .ok_or_else(|| DataFusionError::Execution("No generation provided".to_string()))?
            .first()
            .ok_or_else(|| DataFusionError::Execution("No initial generation provided".to_string()))?;

        // Create the rocksdb database that will hold the incremental state
        let state = RocksDBStateBackend::open_new(
            operator_id.to_string(),
            state_id.to_string(),
            initial_generation.partitions.clone(),
            runtime.remote_checkpoint_file_system().clone(),
            runtime.local_file_system().clone(),
        ).await?;
        Box::new(CountStarFunction::new(runtime, Arc::new(Mutex::new(state)), initial_generation.partitions.clone()))
    }
}

struct CountStarFunction {
    runtime: Arc<Runtime>,
    state: Arc<Mutex<RocksDBStateBackend>>,
    local_count: u64,
    current_partition_range: PartitionRange,
}

impl CountStarFunction {
    pub fn new(
        runtime: Arc<Runtime>,
        state: Arc<Mutex<RocksDBStateBackend>>,
        current_partition_range: PartitionRange
    ) -> Self {
        CountStarFunction {
            runtime,
            state,
            current_partition_range,
            local_count: 0,
        }
    }
}

#[async_trait]
impl OperatorFunction for CountStarFunction {
    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        if checkpoint > 0 {
            let mut state = self.state.lock().await;
            let partition_range = self.current_partition_range.clone();
            state.move_to_checkpoint(checkpoint, partition_range).await?;

            self.local_count = match state.get(COUNT_STATE_KEY)? {
                None => 0,
                Some(bytes) => u64::from_state_bytes(bytes)?,
            };
            println!("Loaded count from state: {}", self.local_count);
        }

        Ok(())
    }

    async fn run<'a>(&'a mut self, mut inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        let (_, mut fiber_stream) = inputs.pop()
            .ok_or(internal_datafusion_err!("CountStarOperator expects exactly one input stream"))?;

        let state = self.state.as_ref()
            .ok_or_else(|| internal_datafusion_err!("State backend not initialized"))?
            .clone();
        let output_stream = Box::into_pin(fiber_stream.combined()?)
            .map_ok(|item| Some(item))
            // Append a marker to the stream to indicate the end of processing
            .chain(futures::stream::iter([Ok(None)]))
            .try_filter_map(move |item| {
                let result = match item {
                    Some(SItem::Generation(_generation)) => {
                        // TODO update the current partition range
                        Err(internal_datafusion_err!("CountStarOperator does not support generation items"))
                    },
                    Some(SItem::RecordBatch(record_batch)) => {
                        // Increment the local count. We can use a reference to self here as the
                        // stream is allowed to borrow from 'a.
                        self.local_count += record_batch.num_rows() as u64;
                        println!("Incrementing count to {}", self.local_count);
                        Ok(CountStreamAction::RecordBatch)
                    },
                    Some(SItem::Marker(marker)) => {
                        Ok(CountStreamAction::Marker {
                            marker: marker.clone(),
                            local_count: self.local_count,
                            partition_range: self.current_partition_range.clone(),
                        })
                    },
                    None => Ok(CountStreamAction::EndOfStream {
                        local_count: self.local_count,
                    }),
                };

                // Now state manipulations are done in an async block to make the lifetimes
                // easier to manage
                let state = state.clone();
                async move {
                    match result? {
                        // Don't emit anything for RecordBatch
                        CountStreamAction::RecordBatch => Ok(None),
                        CountStreamAction::Marker { marker, local_count, partition_range } => {
                            // Store the count in the state backend
                            let mut state = state.lock().await;
                            state.put(COUNT_STATE_KEY, local_count.into_state_bytes()?)?;
                            state.checkpoint(marker.checkpoint_number as usize, partition_range, vec![(0, ParentState::SamePoint)]).await?;
                            // Pass the marker downstream
                            Ok(Some(SItem::Marker(marker)))
                        },
                        CountStreamAction::EndOfStream { local_count } => {
                            // Emit the final count as a single item
                            let record_batch = record_batch!(
                                ("count", UInt64, vec![local_count])
                            )?;
                            println!("Returning final count: {:?}", record_batch);
                            Ok(Some(SItem::RecordBatch(record_batch)))
                        },
                    }
                }
            });

        Ok(vec![(0, Box::new(SingleFiberStream::new(output_stream)))])
    }

    async fn last_checkpoint(&self) -> usize {
        // TODO
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}

enum CountStreamAction {
    RecordBatch,
    Marker{
        marker: Marker,
        local_count: u64,
        partition_range: PartitionRange,
    },
    EndOfStream {
        local_count: u64,
    },
}

const COUNT_STATE_KEY: &str = "count";

// struct CountState {
//     local_count: u64,
//     count_state: RocksDBStateBackend,
// }
//
// impl CountState {
//     pub async fn init(
//         state_id: String,
//         partitions: PartitionRange,
//         remote_checkpoint_storage: Arc<RemoteCheckpointStorage>,
//         local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
//     ) -> Self {
//         CountState {
//             count_state: RocksDBStateBackend::open_new(
//                 format!("{}-count-state", state_id),
//                 partitions,
//                 remote_checkpoint_storage,
//                 local_file_system,
//             ).await.unwrap_or_else(|e| {
//                 panic!("Failed to open CountState: {}", e);
//             })?,
//         }
//     }
//
//     pub fn add(&mut self, amount: u64) {
//         self.local_count += amount;
//     }
//
//     pub async fn checkpoint(&mut self, checkpoint_number: usize, partition_range: PartitionRange) -> Result<(), DataFusionError> {
//         // Update the state with the most recent local count
//         self.count_state.put(COUNT_STATE_KEY, self.local_count.into_state_bytes()?)?;
//
//         // Create a local checkpoint
//         let local_checkpoint_dir = self.count_state.local_checkpoint(checkpoint_number, partition_range).await?;
//
//         // Upload the checkpoint to the remote storage in a background task
//     }
// }
