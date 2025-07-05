use crate::streaming::model::stream_item::StreamItem;
use crate::streaming::model::generation::{GenerationSpec, RemoteStreamDetails};
use crate::streaming::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::{DataChannelSender, Runtime};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use eyeball::{AsyncLock, SharedObservable, Subscriber};
use futures_util::StreamExt;
use std::fmt;
use std::sync::Arc;
use arrow_schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize, Serializer};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use tokio::sync::Mutex;
use crate::streaming::model::operator_function::{CreateOperatorFunction, OperatorFunction};
use crate::streaming::model::sitem::SItem;
use crate::streaming::partitioning::PartitioningSpec;
use crate::streaming::runtime::exchange_manager::data_channels::ChannelPartitioningDetails;
use crate::streaming::serialisation::serde_serialization;

#[derive(Clone)]
pub struct RemoteExchangeOperator {
    output_stream_id: String,
    schema: SchemaRef,
    partitioning: Option<PartitioningSpec>,
}

#[derive(Serialize)]
struct InitialSerialization<'a> {
    output_stream_id: &'a str,
    #[serde(with = "crate::streaming::serialisation::serde_serialization::schema")]
    schema: &'a SchemaRef,
}

#[derive(Deserialize)]
struct InitialDeserialization {
    output_stream_id: String,
    #[serde(with = "crate::streaming::serialisation::serde_serialization::schema_ref")]
    schema: SchemaRef,
}

#[derive(Serialize)]
struct ContentSerialization<'a> {
    partitioning: &'a Option<PartitioningSpec>,
}

#[derive(Deserialize)]
struct ContentDeserialization {
    partitioning: Option<PartitioningSpec>,
}

impl Serialize for RemoteExchangeOperator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer,
    {
        let initial = InitialSerialization {
            output_stream_id: &self.output_stream_id,
            schema: &self.schema,
        };
        let content = ContentSerialization {
            partitioning: &self.partitioning,
        };
        let mut state = serializer.serialize_tuple(2)?;
        state.serialize_element(&initial)?;
        state.serialize_element(&content)?;
        state.end()
    }
}

impl <'de> Deserialize<'de> for RemoteExchangeOperator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, RemoteExchangeOperatorVisitor)
    }
}

struct RemoteExchangeOperatorVisitor;

impl <'de> Visitor<'de> for RemoteExchangeOperatorVisitor
{
    type Value = RemoteExchangeOperator;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("RemoteExchangeOperator")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
    where V: SeqAccess<'de>,
    {
        let pre = seq.next_element::<InitialDeserialization>()?
            .ok_or_else(|| Error::invalid_length(0, &self))?;

        // Deserialize the second element with the context active
        let post = {
            let _guard = serde_serialization::physical_expr_refs::set_context(pre.schema.clone())
                .map_err(|_| Error::custom("Failed to set context for deserialization"))?;
            seq.next_element::<ContentDeserialization>()?.ok_or_else(|| Error::invalid_length(1, &self))?
        };

        Ok(RemoteExchangeOperator {
            output_stream_id: pre.output_stream_id,
            schema: pre.schema,
            partitioning: post.partitioning,
        })
    }
}

impl RemoteExchangeOperator {
    pub fn new(output_stream_id: String) -> Self {
        Self {
            output_stream_id,
            schema: SchemaRef::new(Schema::empty()),
            partitioning: None,
        }
    }

    pub fn new_with_partitioning(output_stream_id: String, schema: SchemaRef, partitioning_spec: PartitioningSpec) -> Self {
        Self {
            output_stream_id,
            schema,
            partitioning: Some(partitioning_spec),
        }
    }

    pub fn get_stream_id(&self) -> &str {
        &self.output_stream_id
    }
}

#[async_trait]
impl CreateOperatorFunction for RemoteExchangeOperator {
    async fn create_operator_function(
        &self,
        _operator_id: &str,
        _state_id: &str,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    ) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(RemoteExchangeOperatorFunction::new(
            self.output_stream_id.clone(),
            self.partitioning.clone(),
            runtime,
            scheduling_details,
        ).await)
    }
}

struct RemoteExchangeOperatorFunction {
    output_stream_id: String,
    runtime: Arc<Runtime>,
    loaded_checkpoint: usize,
    data_channel: DataChannelSender,
    scheduling_details: Subscriber<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
    partitioning: Option<PartitioningSpec>,
}

impl RemoteExchangeOperatorFunction {
    async fn new(
        output_stream_id: String,
        partitioning: Option<PartitioningSpec>,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>
    ) -> Self {
        let scheduling_details_subscriber = scheduling_details.subscribe().await;
        let details = scheduling_details_subscriber.get().await;
        let generations = details.0.as_ref().unwrap();
        let generation = &generations[0];
        let partitions = generation.partitions.clone();
        let partitioning_details = partitioning.as_ref().map(|partitioning| {
            ChannelPartitioningDetails {
                partitioning_spec: partitioning.clone(),
                partitions: partitions.clone(),
            }
        });
        let data_channel = runtime.data_exchange_manager().create_channel(output_stream_id.clone(), partitioning_details).await;

        Self {
            output_stream_id,
            runtime,
            loaded_checkpoint: 0,
            data_channel,
            scheduling_details: scheduling_details_subscriber,
            partitioning,
        }
    }
}

#[async_trait]
impl OperatorFunction for RemoteExchangeOperatorFunction {
    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // No-op for now
        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        mut inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        let channel = &mut self.data_channel;

        assert_eq!(inputs.len(), 1, "RemoteExchangeOperatorFunction should only have one input");
        let mut input_stream = inputs.pop().unwrap().1;
        let mut combined_stream = Box::into_pin(input_stream.combined()?);
        while let Some(result) = combined_stream.next().await {
            match result {
                Ok(SItem::Generation(_)) => {
                    panic!("RemoteExchangeOperatorFunction does not know how to handle generations right now");
                },
                Ok(SItem::RecordBatch(record_batch)) => {
                    println!("Writing record to exchange channel: {}", record_batch.num_rows());
                    channel.send(Ok(StreamItem::RecordBatch(record_batch))).await;
                },
                Ok(SItem::Marker(marker)) => {
                    channel.send(Ok(StreamItem::Marker(marker))).await;
                },
                Err(err) => {
                    channel.send(Err(err)).await;
                },
            }
        }

        println!("RemoteExchange finished writing all results to stream {}", self.output_stream_id);
        self.data_channel.finish();
        // self.data_channel = None;

        Ok(vec![])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
