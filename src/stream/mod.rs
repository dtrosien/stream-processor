use crate::action_plan::deserialize::Deserialize;
use crate::action_plan::transform::Transform;
use crate::action_plan::write::Write;
use crate::action_plan::ActionPlan;
use crate::data_sink::DataSink;
use crate::data_source::DataSource;
use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use crate::type_mapper::TypeMapper;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

pub trait Stream {
    /// Apply Serialization
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream>;

    fn transform(
        self: Arc<Self>,
        mapper: Option<Arc<dyn TypeMapper>>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream>;
    fn write(self: Arc<Self>, data_sinks: Vec<Arc<dyn DataSink>>) -> Arc<dyn Stream>;

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

    fn transform(
        self: Arc<Self>,
        mapper: Option<Arc<dyn TypeMapper>>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Transform::new(
                self.plan.clone().expect("todo"),
                mapper,
                encoder,
                transformations,
            )),
        })
    }

    fn write(self: Arc<Self>, data_sinks: Vec<Arc<dyn DataSink>>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Write::new(self.plan.clone().expect("todo"), data_sinks)),
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
    use crate::encoder::avro_sr_encoder::AvroSREncoder;
    use crate::encoder::Encoder;
    use crate::execution::ExecutionContext;
    use crate::stream::Stream;
    use crate::transformation::Transformation;
    use crate::type_definitions::UniformBatch;
    use crate::type_mapper::TypeMapper;
    use apache_avro::AvroSchema;
    use mockito::Server;

    use crate::data_source::dummy_source::TestDummy;
    use fake::Dummy;
    use schema_registry_converter::blocking::avro::AvroEncoder;
    use schema_registry_converter::blocking::schema_registry::SrSettings;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn build_stream() {
        // this test checks only if stream can be built, but does not execute it,
        // therefore empty transformation and no sr mocks are used
        let mut server = Server::new();
        let context = ExecutionContext::new(HashMap::default());
        let sr_settings = SrSettings::new(server.url());
        let decoder = AvroSRDecoder::new(sr_settings.clone());

        let avro_encoder = AvroEncoder::new(sr_settings);

        let stream = context
            .dummy::<StringMessage>(10, 1)
            .deserialize(decoder)
            .transform(
                None,
                Some(AvroSREncoder::new(avro_encoder)),
                vec![TestTransformation::new()],
            )
            .write(vec![Arc::new(KafkaProducer {})]);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Dummy)]
    #[serde(rename = "some.namespace.StringMessage")]
    pub struct StringMessage {
        pub message: String,
    }

    impl TestDummy for StringMessage {}
    struct TestTransformation {}

    impl TestTransformation {
        pub fn new() -> Arc<Self> {
            Arc::new(TestTransformation {})
        }
    }

    impl Transformation for TestTransformation {
        fn execute(
            &self,
            input: Arc<dyn BatchContainer>,
            mapper: Option<Arc<dyn TypeMapper>>,
            encoder: Option<Arc<Encoder>>,
        ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
            Box::new(
                vec![GenericBatchContainer::new(
                    Arc::new(Batch::Uniform(UniformBatch::Bytes(vec![]))),
                    None,
                    HashMap::default(),
                ) as Arc<dyn BatchContainer>]
                .into_iter(),
            )
        }
    }
}
