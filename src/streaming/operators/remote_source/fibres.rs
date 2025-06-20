use crate::streaming::model::stream_item::Marker;
use crate::streaming::model::generation::{RemoteStreamDetails, GenerationSpec};
use crate::streaming::operators::remote_source::shared_fibre_state::{FiberState, SharedFiberState};
use crate::streaming::operators::remote_source::utils::{find_current_generation, get_addresses};
use crate::streaming::model::sitem::SItem;
use crate::streaming::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::{create_remote_stream, Runtime};
use crate::streaming::utils::retry::retry_future;
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use eyeball::{AsyncLock, SharedObservable};
use futures::Stream;
use futures_util::stream::{iter, FuturesUnordered};
use futures_util::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use create_remote_stream::create_remote_stream;
use crate::streaming::partitioning::PartitionRange;

pub struct RunningStream {
    runtime: Arc<Runtime>,
    stream_ids: Vec<String>,
    scheduling_details_state: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    current_marker: Arc<std::sync::Mutex<usize>>,
    existing_fibers: Option<HashMap<(String, String), RunningFiber>>,
    current_generation: Option<GenerationSpec>,
    pull_called: bool,
}

impl RunningStream {
    pub fn new(
        runtime: Arc<Runtime>,
        stream_ids: Vec<String>,
        scheduling_details_state: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        current_marker: usize,
    ) -> Self {
        Self {
            runtime,
            stream_ids,
            scheduling_details_state,
            current_marker: Arc::new(std::sync::Mutex::new(current_marker)),
            existing_fibers: None,
            current_generation: None,
            pull_called: false,
        }
    }

    fn make_fibers_for_all_addresses<'a>(
        runtime: &'a Arc<Runtime>,
        stored_existing_fibers: &'a mut Option<HashMap<(String, String), RunningFiber>>,
        addresses: Vec<(String, String, PartitionRange)>,
    ) -> &'a mut HashMap<(String, String), RunningFiber> {
        // Reuse the existing fibres where possible
        let (mut existing_fibers, new_addresses) = match stored_existing_fibers.take() {
            Some(existing_fibers) => intersect_addresses_and_fibers(
                &addresses,
                existing_fibers,
            ),
            None => (Default::default(), addresses),
        };

        // Create new fibres for the new addresses
        for (stream_id, address, partitions) in new_addresses {
            let fiber = RunningFiber::new(runtime.clone(), stream_id.clone(), address, partitions.clone());
            existing_fibers.push((stream_id, fiber, partitions));
        }

        // Update the current partitions for each fibre
        for (_stream_id, fiber, partitions) in &mut existing_fibers {
            fiber.load(partitions.clone());
        }

        // Insert the new set of fibres back into the map
        stored_existing_fibers.insert(existing_fibers.into_iter()
            .map(|(stream_id, fiber, _partitions)| ((stream_id, fiber.address.clone()), fiber))
            .collect())
    }
}

#[async_trait]
impl FiberStream for RunningStream {
    type Item = Result<SItem, DataFusionError>;

    async fn pull<'a>(&'a mut self) -> Result<Vec<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Sync + Send + 'a>>, DataFusionError> {
        // As a temporary solution, we only allow the pull method to be called once.
        if self.pull_called {
            return Ok(vec![]);
        }
        self.pull_called = true;

        let (generations, input_details) = self.scheduling_details_state.get().await;
        let generations = generations.ok_or(internal_datafusion_err!("No initial generations were provided"))?;
        let input_details = input_details.ok_or(internal_datafusion_err!("No initial input_details were provided"))?;
        // We unwrap the lock error here because we should be the only thread trying to access this
        // variable right now, so it should never fail. I wonder if this should use a RefCell
        // or similar to make this intention clearer.
        let current_marker = { self.current_marker.lock().unwrap().clone() };

        // Lookup current generation
        // Get starting partitions
        let current_generation = find_current_generation(&self.stream_ids, &generations, current_marker)?;
        let current_partitions = &current_generation.partitions;

        // Get current input details
        // Find all addresses that cover the current partitions at our current checkpoint
        let addresses = get_addresses(current_partitions, current_marker, &self.stream_ids, input_details.as_slice())?;
        println!("Exchange source using addresses: {:?}, from marker: {}", addresses, current_marker);

        // Create streams for all addresses
        let existing_fibers = Self::make_fibers_for_all_addresses(
            &self.runtime,
            &mut self.existing_fibers,
            addresses.clone(),
        );
        let shared_state = Arc::new(SharedFiberState::new(
            self.current_marker.clone(),
            &self.stream_ids,
            &self.scheduling_details_state,
            current_generation.id.clone(),
            current_partitions.clone(),
            addresses,
        ));
        let streams = existing_fibers.values_mut()
            .map(|fiber| {
                async {
                    run_fiber_with_lifetimes(fiber, shared_state.clone()).await
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        Ok(streams)
    }
}

async fn run_fiber_with_lifetimes<'a>(
    fiber: &'a mut RunningFiber,
    shared_state: Arc<SharedFiberState<'a>>,
) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Sync + Send + 'a>, DataFusionError> {
    fiber.run(shared_state.clone()).await
}

fn intersect_addresses_and_fibers(
    addresses: &[(String, String, PartitionRange)], // (stream_id, address, partitions)
    mut fibers: HashMap<(String, String), RunningFiber>,
) -> (Vec<(String, RunningFiber, PartitionRange)>, Vec<(String, String, PartitionRange)>) {
    let mut existing_fibers = Vec::new();
    let mut new_addresses = Vec::new();
    for (stream_id, address, partitions) in addresses {
        match fibers.remove(&(stream_id.clone(), address.clone())) {
            None => new_addresses.push((stream_id.clone(), address.clone(), partitions.clone())),
            Some(fiber) => existing_fibers.push((stream_id.clone(), fiber, partitions.clone())),
        };
    }

    (existing_fibers, new_addresses)
}

pub enum RunningFiberCheckMarker<T> {
    Item(T),
    Check(Marker),
    Finished,
}

struct RunningFiber {
    runtime: Arc<Runtime>,
    stream_id: String,
    address: String,
    current_partitions: PartitionRange,
    // TODO generation markers shouldn't be here.
    remote_stream: Option<Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>>>,
}

impl RunningFiber {
    pub fn new(runtime: Arc<Runtime>, stream_id: String, address: String, range: PartitionRange) -> Self {
        Self {
            runtime,
            stream_id,
            address,
            current_partitions: range,
            remote_stream: None,
        }
    }

    pub fn load(&mut self, partitions: PartitionRange) {
        if self.current_partitions != partitions {
            // Clear the remote stream since we need to recreate it
            self.remote_stream = None;
            self.current_partitions = partitions;
        }
    }

    pub async fn run<'a, 'b>(
        &'a mut self,
        shared_state: Arc<SharedFiberState<'b>>,
    ) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'b>, DataFusionError>
    where 'a: 'b
    {
        let current_partitions = self.current_partitions.clone();
        let remote_stream = if self.remote_stream.is_none() {
            let stream = retry_future(10, || create_remote_stream(
                &self.runtime,
                &self.stream_id,
                &self.address,
                current_partitions.clone(),
            )).await?;
            println!("RunningFiber: Created remote stream for {} at {}", self.stream_id, self.address);
            self.remote_stream.get_or_insert(Box::into_pin(stream))
        } else {
            // unwrap is safe here because we check if remote_stream is None above
            self.remote_stream.as_mut().unwrap()
        };

        // TODO also watch for abrupt end of this address, and throw an error if we pass it by
        //  accident
        Ok(Box::new(Self::stream_until_shared_state_interrupts(shared_state, remote_stream)))
    }

    fn stream_until_shared_state_interrupts<'a>(
        shared_state: Arc<SharedFiberState<'a>>,
        stream: impl Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a ,
    ) -> impl Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a {
        let stream = Self::insert_stream_instructions(stream)
            // We need to wrap the value in another result because we want to use try_take_while
            // to return an error, but we want to keep the original result value as input.
            // Yes, this is quite messy.
            .map(|value| -> Result<_, DataFusionError> { Ok(value) })
            // Consume the result until the shared state tell us we need to pause
            .try_take_while(move |maybe_check| {
                let maybe_check_marker = match maybe_check {
                    RunningFiberCheckMarker::Check(marker) => Some(Ok((marker.clone(), shared_state.clone()))),
                    RunningFiberCheckMarker::Finished => Some(Err(shared_state.clone())),
                    _ => None,
                };
                async move {
                    match maybe_check_marker {
                        // If there is no action to take, just continue
                        None => Ok(true),
                        // Stop to poll the shared state when we receive a marker
                        Some(Ok((marker, shared_state))) => {
                            shared_state.poll_fiber_state(marker.checkpoint_number as usize).await
                                .map(|fiber_state| fiber_state == FiberState::Continue)
                        },
                        // Tell the shared state that we are finished after the end of the stream
                        Some(Err(shared_state)) => {
                            shared_state.report_finished().await;
                            Ok(false)
                        },
                    }
                }
            })
            // Flatten errors back into the stream
            .map(|result| result.unwrap_or_else(|err| RunningFiberCheckMarker::Item(Err(err))));
        Self::strip_stream_instructions(stream)
            .map(|item| {
                println!("Exchange source received item: {:?}", item);
                return item;
            })
    }

    fn insert_stream_instructions<'a>(
        stream: impl Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a
    ) -> impl Stream<Item=RunningFiberCheckMarker<Result<SItem, DataFusionError>>> + Send + Sync + 'a {
        stream.flat_map(|item| {
            // Inserts a check instruction after each marker
            iter(match item {
                // This double wraps the error
                Ok(SItem::Marker(marker)) => vec![
                    RunningFiberCheckMarker::Item(Ok(SItem::Marker(marker.clone()))),
                    RunningFiberCheckMarker::Check(marker),
                ],
                other => vec![RunningFiberCheckMarker::Item(other)]
            })
        })
            .chain(iter(vec![RunningFiberCheckMarker::Finished]))
    }

    fn strip_stream_instructions<'a>(
        stream: impl Stream<Item=RunningFiberCheckMarker<Result<SItem, DataFusionError>>> + Send + Sync + 'a
    ) -> impl Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a {
        stream.flat_map(|item| {
            match item {
                RunningFiberCheckMarker::Item(inner_result) => iter(vec![inner_result]),
                // Ignore the check markers
                RunningFiberCheckMarker::Check(_) => iter(vec![]),
                RunningFiberCheckMarker::Finished => iter(vec![]),
            }
        })
    }
}
