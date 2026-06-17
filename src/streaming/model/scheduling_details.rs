use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use eyeball::{AsyncLock, SharedObservable, Subscriber};
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};

#[derive(Clone)]
pub struct ConsumerDetails {
    consumer_id: String,
    starting_checkpoint: usize,
}

#[derive(Clone)]
pub struct ConsumerGroups {
    stream_id: String,
    consumers: Vec<ConsumerDetails>,
}

#[derive(Clone)]
pub struct SchedulingDetails {
    generations: Vec<GenerationSpec>,
    input_details: Vec<RemoteStreamDetails>,
    consumer_groups: HashMap<String, ConsumerGroups>
}

impl SchedulingDetails {
    pub fn new(
        generations: Vec<GenerationSpec>,
        input_details: Vec<RemoteStreamDetails>,
        consumer_groups: HashMap<String, ConsumerGroups>,
    ) -> Self {
        Self {
            generations,
            input_details,
            consumer_groups,
        }
    }

    pub fn merge(&self, update: SchedulingDetailsUpdate) -> Self {
        let mut result = self.clone();

        if let Some(generation) = update.generation {
            result.generations.push(generation);
        }

        if let Some(input_details) = update.input_details {
            result.input_details = input_details;
        }

        if let Some(consumer_groups) = update.consumer_groups {
            result.consumer_groups = consumer_groups;
        }

        result
    }
}

pub struct SchedulingDetailsUpdate {
    pub generation: Option<GenerationSpec>,
    pub input_details: Option<Vec<RemoteStreamDetails>>,
    pub consumer_groups: Option<HashMap<String, ConsumerGroups>>,
}

pub type SchedulingDetailsSubscriber = Subscriber<SchedulingDetails, AsyncLock>;

pub struct SchedulingDetailsObservable(SharedObservable<SchedulingDetails, AsyncLock>);

impl SchedulingDetailsObservable {
    pub fn new(scheduling_details: SchedulingDetails) -> Self {
        Self(SharedObservable::new_async(scheduling_details))
    }

    pub async fn merge_update(&self, update: SchedulingDetailsUpdate) {
        self.set(self.get().await.merge(update)).await;
    }
}

impl AsRef<SharedObservable<SchedulingDetails, AsyncLock>> for SchedulingDetailsObservable {
    fn as_ref(&self) -> &SharedObservable<SchedulingDetails, AsyncLock> {
        &self.0
    }
}

impl Deref for SchedulingDetailsObservable {
    type Target = SharedObservable<SchedulingDetails, AsyncLock>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SchedulingDetailsObservable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct OperatorStatusReporter {

}

impl OperatorStatusReporter {

}
