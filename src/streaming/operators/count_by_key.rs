use crate::streaming::model::stream_item::Marker;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::runtime::Runtime;
use crate::streaming::state::state::RocksDBStateBackend;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, record_batch, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::col;
use eyeball::{AsyncLock, SharedObservable};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::RecordBatch;
use tokio::sync::Mutex;
use crate::streaming::model::operator_function::{CreateOperatorFunction2, OperatorFunction2};
use crate::streaming::model::sitem::SItem;
use crate::streaming::utils::fiber_stream::{FiberStream, SingleFiberStream};

#[derive(Clone, Serialize, Deserialize)]
pub struct CountByKeyOperator {
    key_col: String,
}

impl CountByKeyOperator {
    pub fn new(key_col: String) -> Self {
        CountByKeyOperator { key_col }
    }
}

impl CreateOperatorFunction2 for CountByKeyOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(CountByKeyFunction::new(self.key_col.clone()))
    }
}

struct CountByKeyFunction {
    key_col: String,
    runtime: Option<Arc<Runtime>>,
    state: Option<Arc<Mutex<RocksDBStateBackend>>>,
    local_counts: HashMap<u64, u64>,
    current_partition_range: Option<PartitionRange>,
}

impl CountByKeyFunction {
    pub fn new(key_col: String) -> Self {
        CountByKeyFunction {
            key_col,
            runtime: None,
            state: None,
            current_partition_range: None,
            local_counts: HashMap::new(),
        }
    }

    fn update_local_counts(&mut self, record_batch: &RecordBatch) -> Result<CountStreamAction, DataFusionError> {
        // Increment the local count. We can use a reference to self here as the
        // stream is allowed to borrow from 'a.
        let expr = col(&self.key_col, record_batch.schema_ref())?;
        let result = expr.evaluate(&record_batch)?;
        if result.data_type() != arrow_schema::DataType::UInt64 {
            return Err(internal_datafusion_err!("Key column must be of type UInt64"));
        }
        
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_primitive::<UInt64Type>();
                for value in int_arr.values() {
                    let count = self.local_counts.entry(*value).or_insert(0);
                    *count += 1;
                }
            },
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(value))) => {
                self.local_counts
                    .entry(value)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            },
            unsupported => {
                return Err(internal_datafusion_err!("Unsupported columnar value {:?}", unsupported));
            },
        }
        
        Ok(CountStreamAction::RecordBatch)
    }
}

#[async_trait]
impl OperatorFunction2 for CountByKeyFunction {
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

            // Update the in memory view of the state from the db
            self.local_counts = HashMap::new();

            for entry in state.iterate() {
                let (key, value) = entry?;
                let key = u64::from_be_bytes(key.into_vec().try_into()
                    .map_err(|e| internal_datafusion_err!("State key is not a valid u64: {:?}", e))?);
                let value = u64::from_be_bytes(value.into_vec().try_into()
                    .map_err(|e| internal_datafusion_err!("State value is not a valid u64: {:?}", e))?);
                self.local_counts.insert(key, value);
            }
            println!("Loaded count from state: {:?}", self.local_counts);
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
                let action = match item {
                    Some(SItem::Generation(_generation)) => {
                        // TODO update the current partition range
                        Err(internal_datafusion_err!("CountStarOperator does not support generation items"))
                    },
                    Some(SItem::RecordBatch(record_batch)) => {
                        self.update_local_counts(&record_batch)
                    },
                    Some(SItem::Marker(marker)) => {
                        self.current_partition_range.clone()
                            .ok_or(internal_datafusion_err!("Current partition range not set"))
                            .map(|partition_range| {
                                CountStreamAction::Marker {
                                    marker: marker.clone(),
                                    local_counts: self.local_counts.clone(),
                                    partition_range,
                                }
                            })
                    },
                    None => Ok(CountStreamAction::EndOfStream {
                        local_counts: self.local_counts.clone()
                    }),
                };

                // Now state manipulations are done in an async block to make the lifetimes
                // easier to manage
                let state = state.clone();
                async move {
                    match action? {
                        // Don't emit anything for RecordBatch
                        CountStreamAction::RecordBatch => Ok(None),
                        CountStreamAction::Marker { marker, local_counts, partition_range } => {
                            // Store the count in the state backend
                            let mut state = state.lock().await;
                            for (key, local_count) in local_counts.iter() {
                                // Store each count for the key
                                state.put(key.to_be_bytes(), local_count.to_be_bytes())?;
                            }
                            state.checkpoint(marker.checkpoint_number as usize, partition_range).await?;

                            // Pass the marker downstream
                            Ok(Some(SItem::Marker(marker)))
                        },
                        CountStreamAction::EndOfStream { local_counts } => {
                            let mut key_builder = Vec::with_capacity(local_counts.len());
                            let mut value_builder = Vec::with_capacity(local_counts.len());
                            for (key, count) in local_counts.iter() {
                                key_builder.push(*key);
                                value_builder.push(*count);
                            }

                            // Emit the final count as a single item
                            let record_batch = record_batch!(
                                ("key", UInt64, key_builder),
                                ("count", UInt64, value_builder)
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
        local_counts: HashMap<u64, u64>,
        partition_range: PartitionRange,
    },
    EndOfStream {
        local_counts: HashMap<u64, u64>,
    },
}
