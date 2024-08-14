use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::dummy_consumer::StringMessage;
use crate::type_converter::TypeConverter;
use crate::type_definitions::{CustomType, CustomTypes, UniformBatch};
use crate::type_mapper::TypeMapper;
use std::any::Any;
use std::sync::Arc;

pub struct AvroValueConverter;

impl AvroValueConverter {
    pub fn new() -> Arc<Self> {
        Arc::new(AvroValueConverter {})
    }
}

// todo remove class completely if new integration test is worling with mapper in transformation

/// converts avro values to custom types
impl TypeConverter for AvroValueConverter {
    fn convert(
        &self,
        msg: Arc<dyn BatchContainer>,
        mapper: Arc<dyn TypeMapper>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let msg_name = msg.clone().get_batch_name().unwrap_or_default();

        // todo eventuell kann man die trafo sein lassen... man kann ja auch einfach in der travo von Value auf concreten type casten. andererseits muss dann der mapper in die transformation parameter
        // todo alles sauber implementiern und testen
        let transform_type = mapper.map_name_to_type(&msg_name);

        let batch = msg.get_batch();
        let mut batch_name = None;
        if let Batch::Uniform(UniformBatch::AvroValue(value_batch)) = batch.as_ref() {
            let converted_values = value_batch
                .iter()
                .map(|value| {
                    let data = match transform_type.get_type_id() {
                        0 => {
                            let a =
                                Arc::new(apache_avro::from_value::<StringMessage>(value).unwrap());
                            println!("got {}", a.as_ref().message);
                            batch_name = Some("StringMessage".to_string());
                            a as Arc<dyn Any>
                        }
                        1 => {
                            let b = Arc::new(apache_avro::from_value::<String>(value).unwrap());
                            b as Arc<dyn Any>
                        }
                        _ => {
                            panic!("no id match for custom data")
                        }
                    };
                    data
                })
                .collect::<Vec<_>>();

            let container = GenericBatchContainer::new(
                Arc::new(Batch::Uniform(UniformBatch::Custom(converted_values))),
                batch_name,
            ) as Arc<dyn BatchContainer>;

            Box::new(std::iter::once(container))
        } else {
            panic!("not supported")
        }
    }
}
