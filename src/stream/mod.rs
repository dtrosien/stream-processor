use crate::action_plan::convert::Convert;
use crate::action_plan::deserialize::Deserialize;
use crate::action_plan::transform::Transform;
use crate::action_plan::write::Write;
use crate::action_plan::ActionPlan;
use crate::data_sink::DataSink;
use crate::data_source::DataSource;
use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use crate::type_converter::TypeConverter;
use crate::type_mapper::TypeMapper;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

pub trait Stream {
    /// Apply Serialization
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream>;

    fn convert(
        self: Arc<Self>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<dyn Stream>;
    fn transform(
        self: Arc<Self>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream>;
    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream>;

    /// Get the Action
    fn action_plan(self: Arc<Self>) -> Arc<dyn ActionPlan>;
}

pub struct StreamImpl {
    pub plan: Option<Arc<dyn ActionPlan>>,
}

impl Stream for StreamImpl {
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Deserialize::new(self.plan.clone().expect("todo"), decoder)),
        })
    }

    fn convert(
        self: Arc<Self>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Convert::new(
                self.plan.clone().expect("todo"),
                mapper,
                converter,
            )),
        })
    }

    fn transform(
        self: Arc<Self>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Transform::new(
                self.plan.clone().expect("todo"),
                encoder,
                transformations,
            )),
        })
    }

    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Write::new(self.plan.clone().expect("todo"), data_sink)),
        })
    }

    fn action_plan(self: Arc<Self>) -> Arc<dyn ActionPlan> {
        self.plan.clone().expect("")
    }
}

#[cfg(test)]
mod test {
    use crate::container::{Batch, BatchContainer, GenericBatchContainer};
    use crate::data_sink::kafka_producer::KafkaProducer;
    use crate::decoder::avro_sr_decoder::AvroSRDecoder;
    use crate::encoder;
    use crate::encoder::avro_sr_encoder::AvroSREncoder;
    use crate::encoder::Encoder;
    use crate::execution::ExecutionContext;
    use crate::stream::Stream;
    use crate::transformation::Transformation;
    use crate::type_converter::avro_value_converter::AvroValueConverter;
    use crate::type_definitions::UniformBatch;
    use crate::type_mapper::MapperImpl;
    use apache_avro::types::Value;
    use apache_avro::AvroSchema;
    use mockito::Server;
    use parquet::data_type::AsBytes;
    use rdkafka::ClientConfig;
    use schema_registry_converter::blocking::avro::{AvroDecoder, AvroEncoder};
    use schema_registry_converter::blocking::schema_registry::SrSettings;
    use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use tracing_subscriber::fmt::format;

    #[test]
    fn registry() {
        let mut server = Server::new();
        let _m = server .mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(server.url());
        let decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6])).unwrap().value;
        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        );
    }

    #[test]
    fn build_stream() {
        // todo einen funktioniereneden stream bauen und wenn es mal laeuft die structs und methoden finalisieren

        let mut server = Server::new();
        let _m = server .mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"StringMessage\",\"namespace\":\"some.namespace\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}"}"#)
            .create();

        println!("{:?}", StringMessage::get_schema().canonical_form());

        let context = ExecutionContext::new(HashMap::default());
        let sr_settings = SrSettings::new(server.url());
        let decoder = AvroSRDecoder::new(sr_settings.clone());

        let avro_encoder = AvroEncoder::new(sr_settings);
        let s_n_strat = SubjectNameStrategy::TopicRecordNameStrategy(
            "topicA".to_string(),
            "recordA".to_string(),
        );

        let stream = context
            .dummy()
            .deserialize(decoder)
            .convert(MapperImpl::new(), AvroValueConverter::new())
            .transform(
                Some(Arc::new(Encoder::AvroSREncoder(AvroSREncoder::new(
                    avro_encoder,
                    s_n_strat,
                )))),
                vec![TestTransformation::new()],
            )
            .write(Arc::new(KafkaProducer {}));

        let action_plan = stream.action_plan();

        let _ = action_plan.execute();
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "some.namespace.StringMessage")]
    pub struct StringMessage {
        message: String,
    }

    struct TestTransformation;

    impl TestTransformation {
        pub fn new() -> Arc<Self> {
            Arc::new(TestTransformation {})
        }
    }

    impl Transformation for TestTransformation {
        fn execute(
            &self,
            input: Arc<dyn BatchContainer>,
            encoder: Option<Arc<Encoder>>,
        ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
            let batch = input.clone().get_batch();

            if let Some(batch_name) = input.clone().get_batch_name() {
                match batch_name.as_str() {
                    "test1" => {
                        if let Batch::Uniform(UniformBatch::Custom(custom)) = batch.as_ref() {
                            let transformed_data = custom
                                .into_iter()
                                .map(|a| {
                                    let val = a.downcast_ref::<String>().unwrap();
                                    let out = format!("{} xyz", val);

                                    if let Encoder::AvroSREncoder(encoder) =
                                        encoder.clone().unwrap().as_ref()
                                    {
                                        let bytes = encoder.encode(out).unwrap();
                                        Arc::new(bytes)
                                    } else {
                                        panic!("no encoder")
                                    }
                                })
                                .collect::<Vec<_>>();

                            let container = GenericBatchContainer::new(
                                Arc::new(Batch::Uniform(UniformBatch::Bytes(transformed_data))),
                                Some("topicA".to_string()),
                            )
                                as Arc<dyn BatchContainer>;

                            Box::new(std::iter::once(container))
                        } else {
                            panic!("wrong batch type")
                        }
                    }
                    &_ => {
                        panic!("not found")
                    }
                }
            } else {
                panic!("blabla")
            }
        }
    }
}
