use std::sync::Arc;
use stream_processor::container::{Batch, BatchContainer, GenericBatchContainer};
use stream_processor::data_source::dummy_source::StringMessage;
use stream_processor::encoder::Encoder;
use stream_processor::transformation::Transformation;
use stream_processor::type_definitions::UniformBatch;

// pub mod example_transformations;
pub struct FlattenStructTransformation;

impl FlattenStructTransformation {
    pub fn new() -> Arc<Self> {
        Arc::new(FlattenStructTransformation {})
    }
}

impl Transformation for FlattenStructTransformation {
    fn execute(
        &self,
        input: Arc<dyn BatchContainer>,
        encoder: Option<Arc<Encoder>>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let batch = input.clone().get_batch();

        if let Some(batch_name) = input.clone().get_batch_name() {
            match batch_name.as_str() {
                "StringMessage" => {
                    if let Batch::Uniform(UniformBatch::Custom(custom)) = batch.as_ref() {
                        let transformed_data = custom
                            .iter()
                            .map(|a| {
                                let val = a.downcast_ref::<StringMessage>().unwrap();

                                if let Encoder::AvroSREncoder(encoder) =
                                    encoder.clone().unwrap().as_ref()
                                {
                                    let bytes = encoder.encode("out").unwrap();
                                    Arc::new(bytes)
                                } else {
                                    panic!("no encoder")
                                }
                            })
                            .collect::<Vec<_>>();

                        let container = GenericBatchContainer::new(
                            Arc::new(Batch::Uniform(UniformBatch::Bytes(transformed_data))),
                            Some("topicA".to_string()),
                        ) as Arc<dyn BatchContainer>;

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
