use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::runtime::Runtime;
use crate::streaming::utils::fiber_stream::FiberStream;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use futures::Stream;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use crate::streaming::model::sitem::SItem;

pub enum UpdateGenerationError {
    // A stream id or partition the operator is using didn't appear in the generation spec.
    // This is probably not recoverable
    InvalidGeneration { reason: String },
    // The operator has already passed the where the next generation is required to start from.
    // The operator should probably be cancelled and restarted from the new generation
    IncompatibleStartOffsets,
}

#[async_trait]
pub trait CreateOperatorFunction {
    async fn create_operator_function(
        &self,
        operator_id: &str,
        state_id: &str,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send>;
}

#[async_trait]
pub trait OperatorFunction {
    // Called strictly once before any other function
    // async fn init(
    //     &mut self,
    //     runtime: Arc<Runtime>,
    //     scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    //     state_id: &str,
    // ) -> Result<(), DataFusionError>;
    
    // Called each time, the operator needs to jump to a checkpoint, including when the operator
    // first starts, even if the operator is starting from the beginning.
    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError>;
    
    // Source operators would have no inputs defined, so inputs would be an empty slice
    // The majority of operators would only take a single input stream per ordinal, and only return
    // a single output per ordinal (and the majority of those would only return one output stream).
    // The vectors allow for operators to make use of the preserved ordered-ness of the inputs
    // steams when they come from multiple different operators.
    // Output operators would have no outputs defined, as they would publish their results over the
    // network. They would return a future that eventually completes with an empty array.
    // When a marker arrives in the input stream, it's the operators responsibility to take
    // checkpoints at appropriate intervals. This functionality could be wrapped in some kind of
    // common class so the majority of operators don't need to worry about it.
    // When a generation marker is received, the operator should take care to download the necessary
    // state partitions.
    // Regular markers can be consumed in any order, as long as all the streams are at a defined
    // marker when checkpoints are taken. However, generation markers can only be consumed when all
    // streams are at the same marker. Once a generation marker appears in one stream, other streams
    // must be consumed until they area also at the same generation point, then the new state can be
    // downloaded and the markers passed downstream.
    // Sometimes, and operator will need to jump to a different marker and or partition offset
    // immediately, such as when an upstream task fails, and the coordinator tells this task to
    // reset to the previous checkpoint and consume from the replacement task. When this occurs,
    // the runtime will drop the returned future and streams and start the run call again with a
    // different marker.
    // It is legal for tasks to only consume some of their inputs at a time, such as a join operator
    // that needs to consume the build side entirely, before consuming the probe side. The runtime
    // ensures that upstream operators are run lazily, so they won't start consuming or producing
    // results until all of their output streams are being awaited. This is necessary so that when
    // the probe side task starts being consumed in the example above, it uses the most recent
    // generation marker, rather than the marker that the task started with. In turn, the operator
    // agrees that once it polls a stream, it must support consuming its contents at some point
    // before processing a generation marker.
    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError>;
    // Lists the most recently completed checkpoint. Used to know where the operator should resume
    // from when it needs to restart or reset back in time.
    // TODO what about stateless operators?
    async fn last_checkpoint(&self) -> usize;
    async fn close(self: Box<Self>);
}

#[async_trait]
impl OperatorFunction for Box<dyn OperatorFunction + Sync + Send> {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        state_id: &str,
    ) -> Result<(), DataFusionError> {
        self.as_mut().init(runtime, scheduling_details, state_id).await
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        self.as_mut().load(checkpoint).await
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        self.as_mut().run(inputs).await
    }

    async fn last_checkpoint(&self) -> usize {
        self.as_ref().last_checkpoint().await
    }

    async fn close(self: Box<Self>) {
        (*self).close().await;
    }
}





// Idea to separate the runtime part of the operator, that could use mutable data, from the part
// that needs shared access. This could be returned from the init operator
pub trait OperatorFunctionRun {
    async fn load(&mut self, checkpoint: usize, partitions: Vec<usize>) -> Result<(), DataFusionError>;
    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Vec<Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>>>)>,
    ) -> Result<Vec<(usize, Vec<Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>>>)>, DataFusionError>;
}

// Task lifecycle
// init()
// configure()
// load_state()
// process() (repeating)
// get_state()
// configure() when certain task parameters change
// load_state()
// process()
// get_state()
// finish()

// Task reads non-partitioned input, there is only one task instance, and one state
// Task reads hash-partitioned input, has state tied to that key
// Task reads round-robin input, uses fake partitions based on a counter

// Round-robin input creates fake partitions based on an auto-incrementing counter.
// OR
// Mergeable state isn't tied to a particular partition of the input, it can be used by any task,
// as long as it is only used once. Sorting is a good example of an operator that would use
// mergeable state. There would be many task instances trying to sort parts of the data in parallel,
// storing their partially sorted results in the state. If the number of tasks change, the state can
// be used as the starting point of any new task, as long as it is only used once.



// pub trait TaskFactory {
//     async fn init(&mut self) -> Box<dyn TaskRuntimeFunction>;
// }
//
// pub trait TaskRuntimeFunction {
//     async fn process(self, data: RecordBatch, output: OutputChannel) -> TaskRuntimeFunctionProcessOutput<Self>;
//     async fn get_state(&mut self) -> RecordBatch;
//     async fn load_state(&mut self, state: RecordBatch);
// }
//
// pub trait TaskSourceFunction {
//     async fn poll(self, output: OutputChannel) -> TaskRuntimeFunctionProcessOutput<Self>;
// }
//
// pub enum TaskRuntimeFunctionProcessOutput<T> {
//     Done,
//     Continue(T),
// }
