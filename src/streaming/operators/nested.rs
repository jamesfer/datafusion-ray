use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::operators::utils::example_ref_storage::Store;
use crate::streaming::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::Runtime;
use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use futures::Stream;
use futures::StreamExt;
use futures_util::stream::FuturesUnordered;
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::streaming::model::operator_definition::OperatorDefinition;
use crate::streaming::model::sitem::SItem;

#[derive(Clone, Serialize, Deserialize)]
pub struct NestedOperator {
    inputs: Vec<(usize, String)>,
    operators: Vec<OperatorDefinition>,
    outputs: Vec<(usize, String)>,
}

impl NestedOperator {
    pub fn new(
        inputs: Vec<(usize, String)>,
        operators: Vec<OperatorDefinition>,
        outputs: Vec<(usize, String)>,
    ) -> Self {
        Self {
            inputs,
            operators,
            outputs,
        }
    }

    pub fn get_operators(&self) -> &[OperatorDefinition] {
        &self.operators
    }
}

impl CreateOperatorFunction for NestedOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(NestedOperatorFunction {
            input_stream_ids: self.inputs.clone(),
            operator_functions: self.operators.iter()
                .map(|op| (
                    op.clone(),
                    op.spec.create_operator_function()
                ))
                .collect(),
            output_stream_ids: self.outputs.clone(),
        })
    }
}

struct NestedOperatorFunction {
    input_stream_ids: Vec<(usize, String)>,
    operator_functions: Vec<(OperatorDefinition, Box<dyn OperatorFunction + Sync + Send>)>,
    output_stream_ids: Vec<(usize, String)>,
}

#[async_trait]
impl OperatorFunction for NestedOperatorFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        _state_id: &str,
    ) -> Result<(), DataFusionError> {
        self.operator_functions.iter_mut()
            .map(|(def, op)| {
                let runtime = runtime.clone();
                let scheduling_details = scheduling_details.clone();
                async move { op.init(runtime, scheduling_details, &def.state_id).await }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        self.operator_functions.iter_mut()
            .map(|(_, op)| async move { op.load(checkpoint).await })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        run_all_operators(inputs, &self.input_stream_ids, &mut self.operator_functions, &self.output_stream_ids).await
    }

    async fn last_checkpoint(&self) -> usize {
        todo!()
    }

    async fn close(mut self: Box<Self>) {
        self.operator_functions.into_iter()
            .map(|(_, op)| async { op.close().await })
            .collect::<FuturesUnordered<_>>()
            // Ignore all elements
            .collect::<()>()
            .await;
    }
}

async fn run_all_operators<'a>(
    external_input_streams: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    input_stream_ids: &[(usize, String)],
    operators: &'a mut [(OperatorDefinition, Box<dyn OperatorFunction + Sync + Send>)],
    output_stream_ids: &[(usize, String)]
) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
    let channels = Arc::new(Store::<Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>>::new());

    // Add all inputs to the channel map
    let input_stream_id_lookup = input_stream_ids.iter()
        .map(|(ordinal, stream_id)| (*ordinal, stream_id.clone()))
        .collect::<HashMap<_, _>>();
    for (ordinal, streams) in external_input_streams.into_iter() {
        let stream_id = input_stream_id_lookup.get(&ordinal)
            .ok_or(internal_datafusion_err!("No stream id found for ordinal {}", ordinal)).unwrap();
        channels.insert(stream_id.clone(), streams).await?;
    }

    // Start all nested operators. If this operator contains any output operators, this future
    // will block until they are finished. For that reason, this type of nested operator cannot
    // support a mix of some output operators and some output streams. It must either contain
    // all output operators or all output streams.
    operators.iter_mut()
        .map(|(operator_definition, operator_function)| {
            async {
                run_operator(operator_definition, operator_function.as_mut(), &channels).await
            }
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    // Extract the output streams from the channel map
    Ok(output_stream_ids.iter()
        .map(|(ordinal, stream_id)| {
            async {
                (ordinal.clone(), channels.take(stream_id.clone()).await.unwrap())
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await)
}

async fn run_operator<'a, 'b>(
    operator_definition: &OperatorDefinition,
    operator_function: &'a mut (dyn OperatorFunction + Sync + Send),
    channels: &'b Store<Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>>,
) -> Result<(), DataFusionError>
where 'a : 'b // a must live longer than b
{
    // Get all inputs. This future will be resolved once all upstream operators registered their inputs
    let input_streams = operator_definition.inputs.iter()
        .map(|input_spec| {
            async move {
                (input_spec.ordinal, channels.take(input_spec.stream_id.clone()).await.unwrap())
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    temp(operator_definition, operator_function, channels, input_streams).await?;

    // For all intermediate operators, the run function completes very quickly, as the streams just
    // need to be added to the channel map. However, the .run() method on output operators will not
    // complete until all streams are fully consumed.
    Ok(())
}

async fn temp<'a, 'b>(
    operator_definition: &OperatorDefinition,
    operator_function: &'a mut (dyn OperatorFunction + Sync + Send),
    channels: &'b Store<Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>>,
    input_streams: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
) -> Result<(), DataFusionError>
where 'a : 'b // a must live longer than b
{
    let outputs = operator_function.run(input_streams).await?;

    // Add the outputs to the channels
    let stream_id_lookup = operator_definition.outputs.iter()
        .map(|output_spec| (output_spec.ordinal, output_spec.stream_id.clone()))
        .collect::<HashMap<_, _>>();
    for (ordinal, streams) in outputs {
        let stream_id = stream_id_lookup.get(&ordinal)
            .ok_or(internal_datafusion_err!("No stream id found for ordinal {}", ordinal))?;
        channels.insert(stream_id.clone(), streams).await?;
    }

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use crate::streaming::operators::identity::IdentityOperator;
//     use crate::streaming::operators::nested::NestedOperator;
//     use crate::streaming::operators::operator_definition::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
//     use crate::streaming::operators::source::SourceOperator;
//     use crate::streaming::operators::operator_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
//     use arrow::array::{ArrayRef, UInt64Array};
//     use arrow::record_batch::RecordBatch;
//     use futures::StreamExt;
//     use futures_util::stream::{iter, FuturesUnordered};
//     use std::sync::Arc;
//     use eyeball::SharedObservable;
//     use crate::streaming::model::generation::GenerationSpec;
//     use crate::streaming::model::sitem::SItem;
//     use crate::streaming::operators::utils::fiber_stream::SingleFiberStream;
//     use crate::streaming::partitioning::PartitionRange;
//     use crate::streaming::utils::fiber_stream::SingleFiberStream;
//     use crate::streaming::utils::test_utils::make_test_runtime;
//
//     #[tokio::test]
//     async fn test_with_one_source_operator() {
//         let batch1 = RecordBatch::try_from_iter([
//             ("hash", Arc::new(UInt64Array::from(vec![0, 1, 2])) as ArrayRef),
//             ("offset", Arc::new(UInt64Array::from(vec![0, 10, 20])) as ArrayRef),
//         ]).unwrap();
//         let batch2 = RecordBatch::try_from_iter([
//             ("hash", Arc::new(UInt64Array::from(vec![3, 4, 5])) as ArrayRef),
//             ("offset", Arc::new(UInt64Array::from(vec![30, 40, 50])) as ArrayRef),
//         ]).unwrap();
//         let source = SourceOperator::new(vec![batch1.clone(), batch2.clone()]);
//
//         let source_def = OperatorDefinition {
//             id: "op1".to_string(),
//             state_id: "state1".to_string(),
//             spec: OperatorSpec::Source(source),
//             inputs: vec![],
//             outputs: vec![OperatorOutput {
//                 stream_id: "output".to_string(),
//                 ordinal: 0,
//             }],
//         };
//
//         let nested = NestedOperator::new(
//             vec![],
//             vec![source_def],
//             vec![(0, "output".to_string())],
//         );
//
//         let runtime = make_test_runtime().await.unwrap();
//         let scheduling_details = SharedObservable::new_async((
//             Some(vec![GenerationSpec {
//                 id: "1".to_string(),
//                 partitions: PartitionRange::unit(),
//                 start_conditions: vec![],
//             }]),
//             Some(vec![]),
//         ));
//
//         let mut nested_func = nested.create_operator_function();
//         nested_func.init(runtime.clone(), scheduling_details.clone(), "state_id").await.unwrap();
//         let outputs = nested_func.run(vec![]).await.unwrap();
//
//         let output_values = outputs.into_iter()
//             .map(|(ordinal, mut outputs)| {
//                 async move {
//                     (
//                         ordinal,
//                         Box::into_pin(outputs.combined().unwrap())
//                             // .map(|output| output.collect::<Vec<_>>())
//                             .collect::<Vec<_>>()
//                             .await
//                     )
//                 }
//             })
//             .collect::<FuturesUnordered<_>>()
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(output_values.len(), 1);
//         assert_eq!(output_values[0].0, 0);
//
//         let items = &output_values[0].1;
//         assert_eq!(items.len(), 2);
//         assert_eq!(items[0].as_ref().unwrap(), &SItem::RecordBatch(batch1));
//         assert_eq!(items[1].as_ref().unwrap(), &SItem::RecordBatch(batch2));
//     }
//
//     #[tokio::test]
//     async fn test_with_multiple_operators() {
//         let batch1 = RecordBatch::try_from_iter([
//             ("hash", Arc::new(UInt64Array::from(vec![0, 1, 2])) as ArrayRef),
//             ("offset", Arc::new(UInt64Array::from(vec![0, 10, 20])) as ArrayRef),
//         ]).unwrap();
//         let batch2 = RecordBatch::try_from_iter([
//             ("hash", Arc::new(UInt64Array::from(vec![3, 4, 5])) as ArrayRef),
//             ("offset", Arc::new(UInt64Array::from(vec![30, 40, 50])) as ArrayRef),
//         ]).unwrap();
//
//         let identity1 = OperatorDefinition {
//             id: "op1".to_string(),
//             state_id: "state".to_string(),
//             spec: OperatorSpec::Identity(IdentityOperator),
//             inputs: vec![OperatorInput {
//                 stream_id: "1".to_string(),
//                 ordinal: 0,
//             }],
//             outputs: vec![OperatorOutput {
//                 stream_id: "2".to_string(),
//                 ordinal: 0,
//             }],
//         };
//         let identity2 = OperatorDefinition {
//             id: "op2".to_string(),
//             state_id: "state".to_string(),
//             spec: OperatorSpec::Identity(IdentityOperator),
//             inputs: vec![OperatorInput {
//                 stream_id: "2".to_string(),
//                 ordinal: 0,
//             }],
//             outputs: vec![OperatorOutput {
//                 stream_id: "3".to_string(),
//                 ordinal: 0,
//             }],
//         };
//         let identity3 = OperatorDefinition {
//             id: "op3".to_string(),
//             state_id: "state".to_string(),
//             spec: OperatorSpec::Identity(IdentityOperator),
//             inputs: vec![OperatorInput {
//                 stream_id: "3".to_string(),
//                 ordinal: 0,
//             }],
//             outputs: vec![OperatorOutput {
//                 stream_id: "4".to_string(),
//                 ordinal: 0,
//             }],
//         };
//
//         let nested = NestedOperator::new(
//             vec![(0, "1".to_string())],
//             vec![identity1, identity2, identity3],
//             vec![(0, "4".to_string())],
//         );
//
//         let runtime = make_test_runtime().await.unwrap();
//         let scheduling_details = SharedObservable::new_async((
//             Some(vec![GenerationSpec {
//                 id: "1".to_string(),
//                 partitions: PartitionRange::unit(),
//                 start_conditions: vec![],
//             }]),
//             Some(vec![]),
//         ));
//
//         let mut nested_func = nested.create_operator_function();
//         nested_func.init(runtime.clone(), scheduling_details.clone(), "state_id").await.unwrap();
//
//         let input_stream = iter(vec![Ok(SItem::RecordBatch(batch1.clone())), Ok(SItem::RecordBatch(batch2.clone()))]);
//         let outputs = nested_func.run(vec![(0, Box::new(SingleFiberStream::new(input_stream)))]).await.unwrap();
//
//         let output_values = outputs.into_iter()
//             .map(|(ordinal, mut outputs)| {
//                 async move {
//                     (
//                         ordinal,
//                         Box::into_pin(outputs.combined().unwrap())
//                             .collect::<Vec<_>>()
//                             .await
//                     )
//                 }
//             })
//             .collect::<FuturesUnordered<_>>()
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(output_values.len(), 1);
//         assert_eq!(output_values[0].0, 0);
//         assert_eq!(output_values[0].1.len(), 1);
//
//         let items = &output_values[0].1;
//         assert_eq!(items.len(), 2);
//         assert_eq!(items[0].as_ref().unwrap(), &SItem::RecordBatch(batch1));
//         assert_eq!(items[1].as_ref().unwrap(), &SItem::RecordBatch(batch2));
//     }
// }
