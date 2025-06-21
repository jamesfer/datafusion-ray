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

// struct TwoPart<T, U> {
//     preamble: T,
//     content: U,
// }
//
// trait EasyDe {
//     type PreSerialize<'a>: Serialize;
//     type PostSerialize<'a>: Serialize;
//     type PreDeserialize<'a>: Deserialize<'a>;
//     type PostDeserialize<'a>: Deserialize<'a>;
//     type Context: Drop;
//
//     fn split(&self) -> (Self::PreSerialize, Self::PostSerialize);
//     fn context(pre: &Self::PreDeserialize) -> Self::Context;
//     fn join(pre: Self::PreDeserialize, post: Self::PostDeserialize) -> Self;
// }
//
// impl <C, PD, PS, PRD, PRS> Serialize for dyn EasyDe<Context=C, PostDeserialize=PD, PostSerialize=PS, PreDeserialize=PRD, PreSerialize=PRS>
// where
//     C: Drop,
//     PD: Deserialize<'static>,
//     PS: Deserialize<'static>,
//     PRD: Deserialize<'static>,
//     PRS: Deserialize<'static>,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where S: Serializer
//     {
//         let (pre, post) = self.split();
//         let mut tuple_serializer = serializer.serialize_tuple(2)?;
//         tuple_serializer.serialize_element(pre)?;
//         tuple_serializer.serialize_element(post)?;
//         Ok(())
//     }
// }
//
// impl <'de, C, PD, PS, PRD, PRS> Deserialize<'de> for dyn EasyDe<Context=C, PostDeserialize=PD, PostSerialize=PS, PreDeserialize=PRD, PreSerialize=PRS>
// where
//     C: Drop,
//     PD: Deserialize<'de>,
//     PS: Deserialize<'de>,
//     PRD: Deserialize<'de>,
//     PRS: Deserialize<'de>,
// {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where D: Deserializer<'de>
//     {
//         deserializer.deserialize_tuple(2, EasyDeVisitor(PhantomData))
//     }
// }
//
// struct EasyDeVisitor<T>(PhantomData<T>);
//
// impl <'de, T> Visitor<'de> for EasyDeVisitor<T>
// where T: EasyDe
// {
//     type Value = T;
//
//     fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//         formatter.write_str("a tuple of two elements")
//     }
//
//     fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
//     where V: de::SeqAccess<'de>,
//     {
//         let pre = seq.next_element::<T::PreDeserialize>()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
//         // Deserialize the second element with the context active
//         let post = {
//             let _context = Self::Value::context(&pre);
//             seq.next_element::<T::PostDeserialize>()?.ok_or_else(|| de::Error::invalid_length(1, &self))?
//         };
//         Ok(Self::Value::join(pre, post))
//     }
// }
//
//
// impl Serialize for RemoteExchangeOperator {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let mut state = serializer.serialize_struct("RemoteExchangeOperator", 3)?;
//         state.serialize_field("output_stream_id", &self.output_stream_id)?;
//         state.serialize_field("schema", &self.schema)?;
//         state.serialize_field("partitioning", &self.partitioning)?;
//         state.end()
//     }
// }
//
// impl<'de> Deserialize<'de> for RemoteExchangeOperator {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         #[derive(Deserialize)]
//         #[serde(field_identifier, rename_all = "snake_case")]
//         enum Field {
//             OutputStreamId,
//             Schema,
//             Partitioning,
//         }
//
//         struct RemoteExchangeOperatorVisitor;
//
//         impl<'de> Visitor<'de> for RemoteExchangeOperatorVisitor {
//             type Value = RemoteExchangeOperator;
//
//             fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//                 formatter.write_str("struct RemoteExchangeOperator")
//             }
//
//             fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
//             where
//                 V: MapAccess<'de>,
//             {
//                 let mut output_stream_id = None;
//                 let mut schema = None;
//                 let mut partitioning = None;
//
//                 while let Some(key) = map.next_key()? {
//                     match key {
//                         Field::OutputStreamId => {
//                             if output_stream_id.is_some() {
//                                 return Err(de::Error::duplicate_field("output_stream_id"));
//                             }
//                             output_stream_id = Some(map.next_value()?);
//                         }
//                         Field::Schema => {
//                             if schema.is_some() {
//                                 return Err(de::Error::duplicate_field("schema"));
//                             }
//                             schema = Some(map.next_value()?);
//                         }
//                         Field::Partitioning => {
//                             if partitioning.is_some() {
//                                 return Err(de::Error::duplicate_field("partitioning"));
//                             }
//                             partitioning = Some(map.next_value()?);
//                         }
//                     }
//                 }
//
//                 let output_stream_id = output_stream_id.ok_or_else(|| de::Error::missing_field("output_stream_id"))?;
//                 let schema = schema.ok_or_else(|| de::Error::missing_field("schema"))?;
//                 Ok(RemoteExchangeOperator {
//                     output_stream_id,
//                     schema,
//                     partitioning,
//                 })
//             }
//         }
//
//         const FIELDS: &[&str] = &["output_stream_id", "schema", "partitioning"];
//         deserializer.deserialize_struct("RemoteExchangeOperator", FIELDS, RemoteExchangeOperatorVisitor)
//     }
// }

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

impl CreateOperatorFunction for RemoteExchangeOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction + Sync + Send> {
        Box::new(RemoteExchangeOperatorFunction::new(self.output_stream_id.clone(), self.partitioning.clone()))
    }
}

struct RemoteExchangeOperatorFunction {
    output_stream_id: String,
    runtime: Option<Arc<Runtime>>,
    scheduling_details_state: Option<Arc<Mutex<(Vec<GenerationSpec>, Vec<RemoteStreamDetails>)>>>,
    loaded_checkpoint: usize,
    data_channel: Option<DataChannelSender>,
    scheduling_details: Option<Subscriber<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>>,
    partitioning: Option<PartitioningSpec>,
}

impl RemoteExchangeOperatorFunction {
    fn new(output_stream_id: String, partitioning: Option<PartitioningSpec>) -> Self {
        Self {
            output_stream_id,
            runtime: None,
            scheduling_details_state: None,
            loaded_checkpoint: 0,
            data_channel: None,
            scheduling_details: None,
            partitioning,
        }
    }
}

#[async_trait]
impl OperatorFunction for RemoteExchangeOperatorFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<RemoteStreamDetails>>), AsyncLock>,
        _state_id: &str,
    ) -> Result<(), DataFusionError> {
        let scheduling_details_subscriber = scheduling_details.subscribe().await;
        let details = scheduling_details_subscriber.get().await;
        let generations = details.0.as_ref().unwrap();
        let generation = &generations[0];
        let partitions = generation.partitions.clone();
        let partitioning_details = self.partitioning.as_ref().map(|partitioning| {
            ChannelPartitioningDetails {
                partitioning_spec: partitioning.clone(),
                partitions: partitions.clone(),
            }
        });
        let channel = runtime.data_exchange_manager().create_channel(self.output_stream_id.clone(), partitioning_details).await;
        self.runtime = Some(runtime);
        self.data_channel = Some(channel);
        self.scheduling_details = Some(scheduling_details_subscriber);
        Ok(())
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // No-op for now
        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        mut inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        let channel = self.data_channel.as_mut().ok_or_else(|| {
            DataFusionError::Execution("Data channel not initialized".to_string())
        })?;

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
        self.data_channel = None;

        Ok(vec![])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
