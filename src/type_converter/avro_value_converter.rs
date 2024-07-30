use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::type_converter::TypeConverter;
use crate::type_definitions::{CustomTypes, GenericTypes, MsgType};
use crate::type_mapper::TypeMapper;
use apache_avro::types::Value;
use arrow::compute::or;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct AvroValueConverter;
impl TypeConverter for AvroValueConverter {
    fn convert(
        &self,
        msg: Arc<dyn BatchContainer>,
        mapper: Arc<dyn TypeMapper>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let msg_type = msg.clone().get_batch_type();

        if let MsgType::Generic(GenericTypes::AvroValue) = msg_type.as_ref() {
            let msg_name = msg.clone().get_batch_name().unwrap_or_default();

            // todo hier wird auf gesammten batch operiert ... ueberlegen wie das besser geht.. unten muss eher zum deserializer weil hier eigentlich nur noch homogene batches ankommen sollen
            // todo aber man kann den code hier nutzen ... nur noch ueber legen wo heterogene batches auftreten koennen .. evtl datatypes anpassen damit klar ist was homogen ist. zb das datatype Optional ist und nur wenn gestezt, ist es homogen
            let transform_type = mapper.map_name_to_type(&msg_name);

            let batch = msg.get_batch();

            if let Batch::AnyBatch(any_batch) = batch.as_ref() {
                let converted_values = any_batch
                    .iter()
                    .map(|item| {
                        let value = item.downcast_ref::<Value>().unwrap();

                        let data = match transform_type {
                            CustomTypes::A => {
                                let a = Arc::new(apache_avro::from_value::<u64>(value).unwrap());
                                a as Arc<dyn Any>
                            }
                            CustomTypes::B => {
                                let b = Arc::new(apache_avro::from_value::<String>(value).unwrap());
                                b as Arc<dyn Any>
                            }
                        };
                        data
                    })
                    .collect::<Vec<_>>();

                let container = GenericBatchContainer::new(
                    Arc::new(Batch::AnyBatch(converted_values)),
                    None,
                    MsgType::Custom(transform_type),
                ) as Arc<dyn BatchContainer>;

                Box::new(std::iter::once(container))
            } else {
                panic!("not supported")
            }
        } else {
            panic!("not Supported")
        }
    }
}
