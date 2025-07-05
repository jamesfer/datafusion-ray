use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::utils::fiber_stream::{FiberStream, SingleFiberStream};
use crate::streaming::runtime::Runtime;
use arrow::array::{Array, RecordBatch};
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use eyeball::{AsyncLock, SharedObservable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use futures::StreamExt;
use crate::streaming::model::sitem::SItem;
use crate::streaming::model::stream_item::{Marker, StreamItem};

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceOperator {
    items: Vec<StreamItem>,
    num_iterations: usize,
    delay_between_iterations: std::time::Duration,
}

impl SourceOperator {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self::new_with_markers(data.into_iter().map(|batch| StreamItem::RecordBatch(batch)).collect())
    }

    pub fn new_with_markers(items: Vec<StreamItem>) -> Self {
        Self {
            items,
            num_iterations: 1,
            delay_between_iterations: std::time::Duration::from_secs(0),
        }
    }

    pub fn new_with_iterations(items: Vec<StreamItem>, num_iterations: usize, delay_between_iterations: std::time::Duration) -> Self {
        Self {
            items,
            num_iterations,
            delay_between_iterations,
        }
    }
}

#[async_trait]
impl CreateOperatorFunction for SourceOperator {
    async fn create_operator_function(
        &self,
        _operator_id: &str,
        _state_id: &str,
        _runtime: Arc<Runtime>,
        _scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(SourceOperatorFunction::new(self.items.clone(), self.num_iterations, self.delay_between_iterations))
    }
}

struct SourceOperatorFunction {
    items: Vec<StreamItem>,
    offset: usize,
    current_iteration: usize,
    num_iterations: usize,
    delay_between_iterations: std::time::Duration,
}

impl SourceOperatorFunction {
    pub fn new(items: Vec<StreamItem>, num_iterations: usize, delay_between_iterations: std::time::Duration) -> Self {
        Self {
            items,
            offset: 0,
            current_iteration: 0,
            num_iterations,
            delay_between_iterations,
        }
    }
}

#[async_trait]
impl OperatorFunction for SourceOperatorFunction {
    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // Move the offset to the matching marker in the static list
        if checkpoint > 0 {
            // Make the checkpoint relative to the batches in the source
            let max_marker = self.items.iter()
                .filter_map(|item| match item {
                    StreamItem::Marker(marker) => Some(marker.checkpoint_number as usize),
                    _ => None,
                })
                .max()
                .ok_or(internal_datafusion_err!("No markers found in source operator items"))?;
            let search_marker = 1 + ((checkpoint - 1) % max_marker);

            // Find the index of the search marker in the source item list
            let index = self.items.iter().enumerate()
                .find_map(|(index, item)| match item {
                    StreamItem::Marker(marker) if marker.checkpoint_number as usize == search_marker => Some(index),
                    _ => None,
                })
                .ok_or_else(|| DataFusionError::Execution(format!("No marker found for checkpoint {}", checkpoint)))?;
            self.offset = index;
            self.current_iteration = (checkpoint - 1) / max_marker;
            println!("SourceOperatorFunction: Loaded checkpoint {} at offset {}, iteration {}", checkpoint, self.offset, self.current_iteration);
        } else {
            self.offset = 0;
            self.current_iteration = 0;
        }

        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        assert_eq!(inputs.len(), 0, "Source operator should not have any inputs");

        let items = self.items.clone();
        let max_marker = self.items.iter()
            .filter_map(|item| match item {
                StreamItem::Marker(marker) => Some(marker.checkpoint_number as usize),
                _ => None,
            })
            .max()
            .ok_or(internal_datafusion_err!("No markers found in source operator items"))?;
        let num_iterations = self.num_iterations;
        let delay_between_iterations = self.delay_between_iterations;
        let stream =
            // Prefix the initial stream to avoid the first delay. The first batch may be partial
            // if we are already part way through it
            futures::stream::iter([self.items[self.offset..].to_vec()])
                // Then append all the other iterations
                .chain(futures::stream::unfold(self.current_iteration + 1, move |iteration| {
                    let items = items.clone();
                    async move {
                        if iteration >= num_iterations {
                            return None; // No more iterations to process
                        }

                        // Sleep inbetween each iteration if specified
                        if !delay_between_iterations.is_zero() {
                            tokio::time::sleep(delay_between_iterations).await;
                        }

                        let items = items.into_iter()
                            .map(|item| {
                                match item {
                                    StreamItem::Marker(Marker { checkpoint_number }) => {
                                        // Update the checkpoint number to the current iteration
                                        StreamItem::Marker(Marker {
                                            checkpoint_number: (iteration * max_marker + checkpoint_number as usize) as u64,
                                        })
                                    },
                                    item => item
                                }
                            })
                            .collect::<Vec<_>>();
                        Some((items, iteration + 1))
                    }
                }))
                // .flat_map(|x| futures::stream::iter(x))
                // .map(|x|)
                // Flatten the items
                .flat_map(futures::stream::iter)
                .map(|item| -> SItem { item.into() })
                .map(Ok);
        Ok(vec![(0, Box::new(SingleFiberStream::new(stream)))])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
