use crate::streaming::model::stream_item::Marker;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::utils::fiber_stream::{FiberStream, SingleFiberStream};
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::runtime::Runtime;
use crate::streaming::state::rocksdb_state_backend::RocksDBStateBackend;
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, record_batch, DataFusionError};
use eyeball::{AsyncLock, SharedObservable};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::streaming::model::sitem::SItem;
use crate::streaming::serialisation::state_serialisation::SerialiseToStateBytes;

#[derive(Clone, Serialize, Deserialize)]
pub struct CountStarOperator {}

impl CountStarOperator {
    pub fn new() -> Self {
        CountStarOperator {}
    }
}

impl CreateOperatorFunction for CountStarOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(CountStarFunction::new())
    }
}

struct CountStarFunction {
    runtime: Option<Arc<Runtime>>,
    state: Option<Arc<Mutex<RocksDBStateBackend>>>,
    local_count: u64,
    current_partition_range: Option<PartitionRange>,
}

impl CountStarFunction {
    pub fn new() -> Self {
        CountStarFunction {
            runtime: None,
            state: None,
            current_partition_range: None,
            local_count: 0,
        }
    }
}

const COUNT_STATE_KEY: &str = "count";

#[async_trait]
impl OperatorFunction for CountStarFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        state_id: &str,
    ) -> Result<(), DataFusionError> {
        let (generation, _) = scheduling_details.get().await;
        let initial_generation = generation.as_ref()
            .ok_or_else(|| DataFusionError::Execution("No generation provided".to_string()))?
            .first()
            .ok_or_else(|| DataFusionError::Execution("No initial generation provided".to_string()))?;

        // Create the rocksdb database that will hold the incremental state
        let state = RocksDBStateBackend::open_new(
            format!("{}-count-star", state_id),
            initial_generation.partitions.clone(),
            runtime.remote_checkpoint_file_system().clone(),
            runtime.local_file_system().clone(),
        ).await?;

        self.runtime = Some(runtime);
        self.state = Some(Arc::new(Mutex::new(state)));
        self.current_partition_range = Some(initial_generation.partitions.clone());
        Ok(())
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        if checkpoint > 0 {
            let mut state = self.state
                .as_ref()
                .ok_or(internal_datafusion_err!("State backend not initialized"))?
                .lock().await;
            let partition_range = self.current_partition_range
                .clone()
                .ok_or(internal_datafusion_err!("Current partition range not set"))?;
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
                        self.current_partition_range.clone()
                            .ok_or(internal_datafusion_err!("Current partition range not set"))
                            .map(|partition_range| {
                                CountStreamAction::Marker {
                                    marker: marker.clone(),
                                    local_count: self.local_count,
                                    partition_range,
                                }
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
                            state.put("count", local_count.into_state_bytes()?)?;
                            state.checkpoint(marker.checkpoint_number as usize, partition_range).await?;
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
