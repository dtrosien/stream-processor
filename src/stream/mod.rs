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
            plan: Some(Deserialize::new(
                self.plan.clone().expect("todo").child().unwrap(),
                decoder,
            )),
        })
    }

    fn convert(
        self: Arc<Self>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Convert::new(
                self.plan.clone().expect("todo").child().unwrap(),
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
                self.plan.clone().expect("todo").child().unwrap(),
                encoder,
                transformations,
            )),
        })
    }

    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Write::new(
                self.plan.clone().expect("todo").child().unwrap(),
                data_sink,
            )),
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
    use parquet::data_type::AsBytes;
    use rdkafka::ClientConfig;
    use schema_registry_converter::blocking::avro::AvroEncoder;
    use schema_registry_converter::blocking::schema_registry::SrSettings;
    use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;

    #[test]
    fn build_stream() {
        // todo einen funktioniereneden stream bauen und wenn es mal laeuft die structs und methoden finalisieren

        let context = ExecutionContext::new(HashMap::default());
        let decoder = AvroSRDecoder::new();
        let sr_settings = SrSettings::new("url".to_string());
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
