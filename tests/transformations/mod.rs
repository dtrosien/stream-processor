use crate::custom_types::test_struct::{BinaryRecord, BinaryType, FlatA, FlatB, FlatC, TestStruct};
use apache_avro::types::Value;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::iter;
use std::sync::Arc;
use stream_processor::container::{Batch, BatchContainer, GenericBatchContainer};
use stream_processor::encoder::Encoder;
use stream_processor::transformation::Transformation;
use stream_processor::type_definitions::{CustomType, UniformBatch};
use stream_processor::type_mapper::TypeMapper;

pub struct FlattenTestStruct {
    s_n_strategy_a: SubjectNameStrategy,
    s_n_strategy_b: SubjectNameStrategy,
    s_n_strategy_c: SubjectNameStrategy,
}

impl FlattenTestStruct {
    pub fn new() -> Arc<Self> {
        let s_n_strategy_a = SubjectNameStrategy::TopicRecordNameStrategy(
            "topicA".to_string(),
            "some.namespace.FlatA".to_string(),
        );

        let s_n_strategy_b = SubjectNameStrategy::TopicRecordNameStrategy(
            "topicA".to_string(),
            "some.namespace.FlatB".to_string(),
        );

        let s_n_strategy_c = SubjectNameStrategy::TopicRecordNameStrategy(
            "topicA".to_string(),
            "some.namespace.FlatC".to_string(),
        );

        Arc::new(FlattenTestStruct {
            s_n_strategy_a,
            s_n_strategy_b,
            s_n_strategy_c,
        })
    }

    fn create_a(
        test_struct: &TestStruct,
        r: &BinaryRecord,
        encoder: &Arc<Encoder>,
        s_n_strategy_a: &SubjectNameStrategy,
    ) -> Arc<Vec<u8>> {
        let a = FlatA {
            timestamp_ms: r.timestamp_ms,
            uuid: test_struct.uuid.clone(),
            source: test_struct.source.clone(),
            value: r.value.clone(),
        };
        let sr_encoder = encoder.get_avro_rs_encoder().expect("no decoder");
        let bytes = sr_encoder
            .encode(a, s_n_strategy_a)
            .expect("Failed to encode data");
        Arc::new(bytes)
    }

    fn create_b(
        test_struct: &TestStruct,
        r: &BinaryRecord,
        encoder: &Arc<Encoder>,
        s_n_strategy_b: &SubjectNameStrategy,
    ) -> Arc<Vec<u8>> {
        let b = FlatB {
            timestamp_ms: r.timestamp_ms,
            uuid: test_struct.uuid.clone(),
            source: test_struct.source.clone(),
            value: r.value.clone(),
        };
        let sr_encoder = encoder.get_avro_rs_encoder().expect("no decoder");
        let bytes = sr_encoder
            .encode(b, s_n_strategy_b)
            .expect("Failed to encode data");
        Arc::new(bytes)
    }

    fn create_c(
        test_struct: &TestStruct,
        r: &BinaryRecord,
        encoder: &Arc<Encoder>,
        s_n_strategy_c: &SubjectNameStrategy,
    ) -> Arc<Vec<u8>> {
        let c = FlatC {
            timestamp_ms: r.timestamp_ms,
            uuid: test_struct.uuid.clone(),
            source: test_struct.source.clone(),
            value: r.value.clone(),
        };
        let sr_encoder = encoder.get_avro_rs_encoder().expect("no decoder");
        let bytes = sr_encoder
            .encode(c, s_n_strategy_c)
            .expect("Failed to encode data");
        Arc::new(bytes)
    }

    fn flatten_test_struct(
        &self,
        encoder: &Arc<Encoder>,
        value: &Arc<Value>,
    ) -> Arc<dyn BatchContainer> {
        let test_struct =
            apache_avro::from_value::<TestStruct>(value).expect("Failed to deserialize TestStruct");

        let data = test_struct
            .records_binaries
            .iter()
            .map(|r| match r.binary_type {
                BinaryType::A => Self::create_a(&test_struct, r, encoder, &self.s_n_strategy_a),
                BinaryType::B => Self::create_b(&test_struct, r, encoder, &self.s_n_strategy_b),
                BinaryType::C => Self::create_c(&test_struct, r, encoder, &self.s_n_strategy_c),
            }) // todo use result in create_fn and then creat ErrorBatch in case of error
            .collect::<Vec<_>>();
        let container = GenericBatchContainer::new(
            Arc::new(Batch::Uniform(UniformBatch::Bytes(data))),
            Some("topicA".to_string()), // todo create another example where based on the type the topic is different
        ) as Arc<dyn BatchContainer>;
        container
    }

    fn split_by_input_type(
        &self,
        transform_type: &Arc<dyn CustomType>,
        encoder: &Arc<Encoder>,
        value: &Arc<Value>,
    ) -> Arc<dyn BatchContainer> {
        match transform_type.get_type_id() {
            0 => self.flatten_test_struct(encoder, value),
            _ => panic!("no id match for custom data"),
        }
    }
}
// todo further clean up (error handling) and generalize
impl Transformation for FlattenTestStruct {
    fn execute(
        &self,
        input: Arc<dyn BatchContainer>,
        mapper: Option<Arc<dyn TypeMapper>>, // todo maybe as struct in Transformation, force implementation with trait fn get_mapper(&self) -> Option<Mapper>
        encoder: Option<Arc<Encoder>>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let batch_name = input.to_owned().get_type_name().unwrap_or_default();
        let transform_type = mapper
            .as_ref()
            .expect("no mapper found")
            .map_name_to_type(&batch_name);
        let batch = input.to_owned().get_batch();

        match batch.as_ref() {
            Batch::Uniform(UniformBatch::AvroValue(value_batch)) => {
                let encoder = encoder.expect("no encoder found");
                let result = value_batch
                    .into_iter()
                    .map(move |value| self.split_by_input_type(&transform_type, &encoder, &value))
                    .collect::<Vec<_>>();
                Box::new(result.into_iter())
            }
            _ => Box::new(iter::once(input)),
        }
    }
}
