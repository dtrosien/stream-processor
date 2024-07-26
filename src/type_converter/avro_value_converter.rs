use crate::container::{GenericMsgContainer, MsgContainer};
use crate::type_converter::TypeConverter;
use crate::type_definitions::{CustomTypes, GenericTypes, MsgType};
use crate::type_mapper::TypeMapper;
use apache_avro::types::Value;
use std::any::Any;
use std::sync::Arc;

pub struct AvroValueConverter;
impl TypeConverter for AvroValueConverter {
    fn convert(
        &self,
        msg: Arc<dyn MsgContainer>,
        mapper: Arc<dyn TypeMapper>,
    ) -> Arc<dyn MsgContainer> {
        let msg_type = msg.clone().get_msg_type();

        if let MsgType::Generic(GenericTypes::AvroValue) = msg_type.as_ref() {
            let msg_name = msg.clone().get_msg_name().unwrap_or_default();

            let transform_type = mapper.map_name_to_type(&msg_name);

            let msg = msg.get_msg();
            let value = msg.downcast_ref::<Value>().unwrap();

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

            GenericMsgContainer::new(data, Some(msg_name), MsgType::Custom(transform_type))
        } else {
            panic!("not Supported")
        }
    }
}
