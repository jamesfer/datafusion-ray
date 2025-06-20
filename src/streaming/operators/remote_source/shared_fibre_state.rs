use crate::streaming::model::generation::{RemoteStreamDetails, GenerationSpec};
use crate::streaming::operators::remote_source::utils::{find_current_generation, get_addresses};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use eyeball::{AsyncLock, SharedObservable};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::streaming::operators::remote_source::barrier_with_result::BarrierWithResult;
use crate::streaming::partitioning::PartitionRange;

#[derive(Clone, PartialEq)]
pub enum FiberState {
    Continue,
    Pause,
}

struct SyncState {
    waiting_for_marker: Option<usize>,

}

pub struct SharedFiberState<'a> {
    stream_ids: &'a [String],
    scheduling_details_state: &'a SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    current_generation_id: String,
    current_partitions: PartitionRange,
    current_addresses: Vec<(String, String, PartitionRange)>,
    shared_trigger_state: BarrierWithResult<FiberState>,
    // Even though we have a mutable reference to the last_completed_marker, we need to use a Mutex
    // so we can change it through a shared reference. Some finagling could avoid this.
    last_completed_marker: Arc<std::sync::Mutex<usize>>,
    currently_waiting_on_marker: Arc<std::sync::Mutex<Option<usize>>>
}

impl <'a> SharedFiberState<'a> {
    pub fn new(
        last_completed_marker: Arc<std::sync::Mutex<usize>>,
        stream_ids: &'a [String],
        scheduling_details_state: &'a SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        current_generation_id: String,
        current_partitions: PartitionRange,
        current_addresses: Vec<(String, String, PartitionRange)>,
    ) -> Self {
        let shared_trigger_state = BarrierWithResult::new(current_addresses.len());
        Self {
            stream_ids,
            scheduling_details_state,
            current_generation_id,
            current_partitions,
            current_addresses,
            shared_trigger_state,
            last_completed_marker,
            currently_waiting_on_marker: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub async fn poll_fiber_state(&self, marker: usize) -> Result<FiberState, DataFusionError> {
        // Update the marker we are currently waiting for
        {
            let mut guard = self.currently_waiting_on_marker.lock().unwrap();
            match *guard {
                None => {
                    *guard = Some(marker);
                },
                Some(existing) => {
                    if existing != marker {
                        // TODO This is a case where one of the fibres returns an error early, which
                        //  would cause all other participants of the barrier to wait forever, as
                        //  the barrier doesn't know how to handle dropped participants.
                        return Err(internal_datafusion_err!("Already waiting on a different marker: {}. Current marker: {}", existing, marker));
                    }
                },
            }
        }

        self.shared_trigger_state.compute_result({
            // This function requires a bunch of unideal copies because the computation function is
            // temporarily stored in the shared state trigger. Some refactoring could remove this
            // need.
            let scheduling_details_state = self.scheduling_details_state.clone();
            let stream_ids = self.stream_ids.to_vec();
            let current_generation_id = self.current_generation_id.clone();
            let current_addresses = self.current_addresses.clone();
            let last_completed_marker = self.last_completed_marker.clone();
            let currently_waiting_on_marker = self.currently_waiting_on_marker.clone();

            move || async move {
                {
                    // We lock the marker here, but we are guaranteed to be the only thread attempting
                    // to do so.
                    let mut guard = last_completed_marker.try_lock().unwrap();
                    *guard = marker;
                    println!("Updated last completed marker: {}", marker);
                }

                {
                    // Same story here, we should be the only thread trying to update this marker.
                    let mut guard = currently_waiting_on_marker.try_lock().unwrap();
                    // Reset the current marker to none
                    *guard = None;
                }

                Self::check_generation_and_addresses(
                    scheduling_details_state,
                    stream_ids,
                    current_generation_id,
                    current_addresses,
                    marker,
                ).await
            }
        }).await
    }

    pub async fn report_finished(&self) {
        self.shared_trigger_state.remove_participant().await;
    }

    async fn check_generation_and_addresses(
        scheduling_details_state: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        stream_ids: Vec<String>,
        current_generation_id: String,
        current_addresses: Vec<(String, String, PartitionRange)>,
        marker: usize,
    ) -> Result<FiberState, DataFusionError> {
        let (generations, input_details) = scheduling_details_state.get().await;
        let generations = generations.ok_or(internal_datafusion_err!("No initial generations were provided"))?;
        let input_details = input_details.ok_or(internal_datafusion_err!("No initial input_details were provided"))?;

        // Look up the generation for this marker
        let current_generation = find_current_generation(
            &stream_ids,
            generations.as_slice(),
            marker,
        )?.clone();

        // If we have moved to the next generation, we need to stop streaming
        if current_generation.id != current_generation_id {
            return Ok(FiberState::Pause);
        }

        // Check that all the fibres are still valid
        let new_addresses = get_addresses(
            &current_generation.partitions,
            marker,
            &stream_ids,
            input_details.as_slice(),
        )?;
        if new_addresses.iter().collect::<HashSet<_>>() != current_addresses.iter().collect::<HashSet<_>>() {
            // Addresses have changed
            return Ok(FiberState::Pause);
        }

        // All fibres are allowed to continue streaming without interruption
        Ok(FiberState::Continue)
    }
}
