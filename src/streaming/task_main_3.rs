use crate::streaming::model::generation::{RemoteStreamDetails, GenerationSpec, TaskSchedulingDetailsUpdate};
use crate::streaming::model::operator_definition::OperatorDefinition;
use crate::streaming::model::operator_function::{CreateOperatorFunction2, OperatorFunction2};
use crate::streaming::runtime::Runtime;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug)]
struct OutOfOrderGenerationsError {
    generation_id: String,
}

impl Display for OutOfOrderGenerationsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OutOfOrderGenerationsError(generation_id: {})", self.generation_id)
    }
}

impl Error for OutOfOrderGenerationsError {}

struct RunningTaskState {
    generations: Vec<GenerationSpec>,
    input_locations: Vec<RemoteStreamDetails>,
}

pub struct RunningTask {
    // generations: Arc<Mutex<Vec<GenerationSpec>>>,
    // function: Arc<dyn OperatorFunction2 + Sync + Send>,
    handle: JoinHandle<Result<(), DataFusionError>>,
    state: Arc<Mutex<RunningTaskState>>,
    scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>
}

impl RunningTask {
    pub async fn start(
        task_id: String,
        operator: OperatorDefinition,
        runtime: Arc<Runtime>,
        initial_checkpoint: usize,
        initial_input_locations: Vec<RemoteStreamDetails>,
        generations: Vec<GenerationSpec>,
    ) -> Result<Self, DataFusionError> {
        if operator.inputs.len() > 0 || operator.outputs.len() > 0 {
            return Err(internal_datafusion_err!("Task main doesn't support operators with inputs and outputs"));
        }

        let state = Arc::new(Mutex::new(RunningTaskState {
            generations: generations.clone(),
            input_locations: initial_input_locations.clone(),
        }));
        let scheduling_details = SharedObservable::new_async((Some(generations), Some(initial_input_locations)));

        // State the operator's main loop in a separate task
        let function = operator.spec.create_operator_function();
        let handle = spawn({
            let state = state.clone();
            let scheduling_details = scheduling_details.clone();
            async move {
                let result = operator_main_loop(
                    task_id.clone(),
                    function,
                    &operator.state_id,
                    runtime,
                    initial_checkpoint,
                    state,
                    scheduling_details,
                ).await;
                match &result {
                    Ok(_) => println!("Task {} completed successfully", task_id),
                    Err(err) => println!("Task {} failed: {}", task_id, err),
                }
                result
            }
        });



        Ok(Self {
            // function,
            handle,
            state,
            scheduling_details,
        })
    }

    pub async fn update_scheduling_details(&self, details: TaskSchedulingDetailsUpdate) {
        let generation = details.generation;
        let input_details = details.input_details;

        // Update the state on the task runner itself
        {
            let state = &mut *self.state.lock().await;
            generation.clone().map(|generation| {
                state.generations.push(generation);
            });
            input_details.clone().map(|input_details| {
                state.input_locations = input_details;
            });
        }

        // Pass the information down to the running task
        // self.function.update_scheduling_details(details.generation, details.input_details).await?;
        self.scheduling_details.set((
            generation.map(|g| vec![g]),
            input_details,
        )).await;
    }

    pub fn cancel(&self) {
        self.handle.abort();
    }
}


async fn operator_main_loop(
    task_id: String,
    mut function: Box<dyn OperatorFunction2 + Sync + Send>,
    state_id: &str,
    runtime: Arc<Runtime>,
    initial_checkpoint: usize,
    state: Arc<Mutex<RunningTaskState>>,
    scheduling_details_receiver: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>
) -> Result<(), DataFusionError> {
    // Initialisation
    println!("Starting task {}", task_id);
    function.init(runtime, scheduling_details_receiver, state_id).await?;
    // function.update_scheduling_details(Some(generations.clone()), Some(initial_input_locations.clone())).await?;

    // Main loop
    let mut checkpoint = initial_checkpoint;
    loop {
        println!("Task {}: Loading checkpoint {}", task_id, checkpoint);
        function.load(checkpoint).await?;

        // Since the root task doesn't have any outputs, the future should wait to resolve until the
        // entire task completes

        // Run the function in a block to make the lifetime explicit
        let error = {
            println!("Task {}: Running", task_id);
            match function.run(vec![]).await {
                Ok(outputs) => {
                    assert_eq!(outputs.len(), 0);
                    return Ok(());
                },
                Err(err) => err,
            }
        };

        match error {
            DataFusionError::External(external) => {
                // This specific error can be recovered by reloading an earlier checkpoint
                if let Some(out_of_order_error) = external.downcast_ref::<OutOfOrderGenerationsError>() {
                    let state = &* state.lock().await;
                    checkpoint = handle_out_of_order_error(
                        state.generations.as_slice(),
                        function.as_ref(),
                        out_of_order_error,
                    ).await;
                    continue;
                }
            }
            _ => return Err(error),
        }
    }
}

async fn handle_out_of_order_error(
    generations: &[GenerationSpec],
    function: &(dyn OperatorFunction2 + Sync + Send),
    out_of_order_error: &OutOfOrderGenerationsError,
) -> usize {
    // let generation_id = out_of_order_error.generation_id.as_str();
    // TODO throw real error
    // let generation = generations.iter().find(|g| g.id.as_str() == generation_id).unwrap();
    let last_checkpoint = function.last_checkpoint().await;

    // TODO Find the most recent checkpoint earlier than the out-of-order generation
    
    last_checkpoint
}
