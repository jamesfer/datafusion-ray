use serde::de::{DeserializeSeed, Error, Visitor};

pub mod record_batch {
    use arrow_array::RecordBatch;
    use serde::de::Error;
    use serde::{Deserializer, Serializer};
    use std::slice;
    use crate::streaming::serialisation::serde_serialization::record_batches;

    pub fn serialize<S: Serializer>(record_batches: &RecordBatch, serializer: S) -> Result<S::Ok, S::Error> {
        record_batches::serialize(slice::from_ref(record_batches), serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RecordBatch, D::Error> {
        let mut vec = record_batches::deserialize(deserializer)?;
        match vec.len() {
            1 => Ok(vec.pop().unwrap()),
            0 => Err(Error::custom("Cannot deserialize a record batch from an empty stream")),
            _ => Err(Error::custom("Found more than one record batch when deserializing the stream")),
        }
    }
}

pub mod record_batches {
    use arrow::array::RecordBatch;
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use serde::de::Error as DeError;
    use serde::ser::Error as SerError;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(record_batches: &[RecordBatch], serializer: S) -> Result<S::Ok, S::Error> {
        // Create a stream writer with the schema from the first batch
        match record_batches.first() {
            None => {
                serializer.serialize_bytes(&[])
            },
            Some(record_batch) => {
                let mut buffer = Vec::new();
                let mut writer = StreamWriter::try_new(&mut buffer, record_batch.schema_ref())
                    .map_err(|arrow_error| S::Error::custom(arrow_error))?;

                // Write each batch to the buffer
                for batch in record_batches {
                    writer.write(batch)
                        .map_err(|arrow_error| S::Error::custom(arrow_error))?;
                }

                // Finish writing to flush any remaining data
                writer.finish()
                    .map_err(|arrow_error| S::Error::custom(arrow_error))?;

                serializer.serialize_bytes(&buffer)
            },
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<RecordBatch>, D::Error> {
        let bytes: &[u8] = Deserialize::deserialize(deserializer)?;
        let mut reader = StreamReader::try_new(bytes, None)
            .map_err(|arrow_error| D::Error::custom(arrow_error))?;

        let mut record_batches = Vec::new();
        while let Some(result) = reader.next() {
            match result {
                Ok(record_batch) => record_batches.push(record_batch),
                Err(error) => {
                    return Err(D::Error::custom(format!("Failed to read record batch: {}", error)));
                }
            }
        }

        Ok(record_batches)
    }
}

pub mod schema {
    use arrow_schema::Schema;
    use datafusion_proto::protobuf;
    use prost::Message;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(schema: &Schema, serializer: S) -> Result<S::Ok, S::Error> {
        // datafusion_proto::protobuf::DfSchema::try_from(DFSchema::try_from())
        let proto_schema = protobuf::Schema::try_from(schema)
            .map_err(|e| serde::ser::Error::custom(format!("Failed to convert schema: {e}")))?;
        let bytes = proto_schema.encode_to_vec();
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Schema, D::Error> {
        let bytes: &[u8] = Deserialize::deserialize(deserializer)?;
        let proto_schema = protobuf::Schema::decode(bytes)
            .map_err(|e| serde::de::Error::custom(format!("Failed to decode schema: {e}")))?;

        let schema = (&proto_schema).try_into()
            .map_err(|e| serde::de::Error::custom(format!("Failed to convert schema: {e}")))?;
        Ok(schema)
    }
}

pub mod schema_ref {
    use arrow_schema::{Schema, SchemaRef};
    use serde::{Deserialize, Deserializer, Serializer};
    use std::sync::Arc;
    use crate::streaming::serialisation::serde_serialization::schema;

    pub fn serialize<S: Serializer>(schema: &Schema, serializer: S) -> Result<S::Ok, S::Error> {
        schema::serialize(schema, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SchemaRef, D::Error> {
        Ok(Arc::new(schema::deserialize(deserializer)?))
    }
}

pub mod physical_expr_refs {
    use crate::codec::RayCodec;
    use arrow_schema::{Schema, SchemaRef};
    use datafusion::common::{internal_datafusion_err, plan_err, DataFusionError};
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::planner::ExprPlanner;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_proto::physical_plan::from_proto::parse_physical_exprs;
    use datafusion_proto::physical_plan::to_proto::serialize_physical_exprs;
    use datafusion_proto::protobuf::PhysicalExprNode;
    use prost::Message;
    use serde::ser::SerializeStruct;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::sync::Arc;

    // #[derive(Serialize)]
    // #[serde(transparent)]
    // struct SerializeSchema<'a> {
    //     #[serde(serialize_with = "crate::streaming::serialisation::serde_serialization::schema::serialize")]
    //     schema: &'a Schema
    // }
    //
    // pub fn serialize_with_input_schema<S: Serializer>(expressions: &[PhysicalExprRef], input_schema: &Schema, serializer: S) -> Result<S::Ok, S::Error> {
    //     // TODO work towards supporting a custom codec, which would be quite complex with the serde
    //     //  approach we are taking
    //     let codec = RayCodec{};
    //
    //
    //
    //     // Serialize the input schema
    //     let mut state = serializer.serialize_struct("PhysicalExprRefs", 2)?;
    //     state.serialize_field("schema", &SerializeSchema { schema: input_schema })?;
    //
    //
    //     let proto_expressions = serialize_physical_exprs(expressions, &codec)
    //         .map_err(|err| serde::ser::Error::custom(format!("Failed to encode PhysicalExprNode: {err}")))?;
    //     let proto_expression_bytes = proto_expressions
    //         .into_iter()
    //         .map(|proto| proto.encode_to_vec())
    //         .collect::<Vec<_>>();
    //     state.serialize_field("expressions", &proto_expression_bytes)?;
    //     Ok(())
    // }

    // To be able to deserialise expressions, we need to have access to the input schema, that could
    // be stored in a completely different part of the struct. To pass this down to the correct
    // place, we use some thread-local storage which is written to when the input schema is decoded.

    // Thread-local storage to store the schema
    thread_local! {
        static INPUT_SCHEMA_CONTEXT: RefCell<Option<SchemaRef>> = RefCell::new(None);
    }

    // RAII-style guard that clears the context when dropped
    pub struct ContextGuard;

    impl Drop for ContextGuard {
        fn drop(&mut self) {
            INPUT_SCHEMA_CONTEXT.with(|context| {
                *context.borrow_mut() = None;
            });
        }
    }

    // Sets the value of the thread-local storage, and returns a guard that will clear it when dropped.
    pub fn set_context(schema: SchemaRef) -> Result<ContextGuard, DataFusionError> {
        INPUT_SCHEMA_CONTEXT.with(|context| {
            let mut borrowed = context.borrow_mut();
            if borrowed.is_some() {
                return Err(internal_datafusion_err!("Input schema already set in this thread"));
            }
            *borrowed = Some(schema);
            Ok(())
        })?;
        Ok(ContextGuard)
    }

    pub fn get_context() -> Result<SchemaRef, DataFusionError> {
        INPUT_SCHEMA_CONTEXT.with(|context| {
            context.borrow().clone()
                .ok_or_else(|| internal_datafusion_err!("No input schema set in this thread"))
        })
    }

    pub fn serialize<S: Serializer>(expressions: &[PhysicalExprRef], serializer: S) -> Result<S::Ok, S::Error> {
        // TODO work towards supporting a custom codec, which would be quite complex with the serde
        //  approach we are taking
        let codec = RayCodec{};

        let proto_expressions = serialize_physical_exprs(expressions, &codec)
            .map_err(|err| serde::ser::Error::custom(format!("Failed to encode PhysicalExprNode: {err}")))?;
        let proto_expression_bytes = proto_expressions
            .into_iter()
            .map(|proto| proto.encode_to_vec())
            .collect::<Vec<_>>();
        proto_expression_bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PhysicalExprRef>, D::Error> {
        let input_schema = get_context()
            .map_err(|err| serde::de::Error::custom(format!("Failed to get input schema: {err}")))?;
        deserialize_with_input_schema(&input_schema, deserializer)
    }

    pub fn deserialize_with_input_schema<'de, D: Deserializer<'de>>(input_schema: &Schema, deserializer: D) -> Result<Vec<PhysicalExprRef>, D::Error> {
        let codec = RayCodec{};

        let buffers: Vec<Vec<u8>> = Deserialize::deserialize(deserializer)?;
        let proto_expressions = buffers.into_iter()
            .map(|bytes| PhysicalExprNode::decode(&bytes[..]))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| serde::de::Error::custom(format!("Failed to decode PhysicalExprNode: {err}")))?;
        parse_physical_exprs(proto_expressions.iter(), &NoRegistry{}, input_schema, &codec)
            .map_err(|err| serde::de::Error::custom(format!("Failed to parse PhysicalExprNode: {err}")))
    }

    // Copied from datafusion private crate
    struct NoRegistry {}

    impl FunctionRegistry for NoRegistry {
        fn udfs(&self) -> HashSet<String> {
            HashSet::new()
        }

        fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
            plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Function '{name}'")
        }

        fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
            plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{name}'")
        }

        fn udwf(&self, name: &str) -> datafusion::common::Result<Arc<WindowUDF>> {
            plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Window Function '{name}'")
        }
        fn register_udaf(
            &mut self,
            udaf: Arc<AggregateUDF>,
        ) -> datafusion::common::Result<Option<Arc<AggregateUDF>>> {
            plan_err!("No function registry provided to deserialize, so can not register User Defined Aggregate Function '{}'", udaf.inner().name())
        }
        fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> datafusion::common::Result<Option<Arc<ScalarUDF>>> {
            plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Function '{}'", udf.inner().name())
        }
        fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> datafusion::common::Result<Option<Arc<WindowUDF>>> {
            plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Window Function '{}'", udwf.inner().name())
        }

        fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
            vec![]
        }
    }
}

// mod physical_expr {
//     use arrow::datatypes::SchemaRef;
//     use prost::Message;
//     use serde::de::{Error, Visitor};
//     use serde::{Deserializer, Serializer};
//     use std::fmt::Formatter;
//     use datafusion::physical_expr::PhysicalExprRef;
//
//     pub fn serialize<S: Serializer>(expr: &PhysicalExprRef, serializer: S) -> Result<S::Ok, S::Error> {
//         // The conversion is infallible
//         expr.
//         let proto_schema = datafusion_proto::protobuf::PhysicalExprNode::try_from()
//         let bytes = proto_schema.encode_to_vec();
//         serializer.serialize_bytes(&bytes)
//     }
//
//     pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SchemaRef, D::Error> {
//         let proto_schema = deserializer.deserialize_bytes(V)?;
//         proto_schema.try_into()
//             .map_err(|e| Error::custom(format!("failed to convert schema: {e}")))
//     }
//
//     struct V;
//
//     impl <'de> Visitor<'de> for V {
//         type Value = datafusion_proto::protobuf::Schema;
//
//         fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//             formatter.write_str("a byte array")
//         }
//
//         fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
//         where
//             E: Error
//         {
//             datafusion_proto::protobuf::Schema::decode(v)
//                 .map_err(|e| E::custom(format!("failed to decode schema: {e}")))
//         }
//     }
// }
