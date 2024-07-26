use crate::container::{GenericMsgContainer, MsgContainer};
use crate::decoder::Decoder;
use crate::type_definitions::{GenericTypes, MsgType, RawTypes};
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use std::any::Any;
use std::sync::Arc;

struct AvroSRDecoder {}

impl Decoder for AvroSRDecoder {
    fn decode(&self, msg: Arc<dyn MsgContainer>) -> Arc<dyn MsgContainer> {
        let msg_type = msg.clone().get_msg_type();

        if let MsgType::Raw(RawTypes::Bytes) = msg_type.as_ref() {
            let sr_settings = SrSettings::new("some_url".to_string());
            let decoder = AvroDecoder::new(sr_settings);

            let msg = msg.get_msg();
            let payload = msg.downcast_ref::<Vec<u8>>().unwrap();

            let result = decoder.decode(Some(&payload[..])).unwrap();

            let name = result.name;
            let value = result.value;

            let container = GenericMsgContainer::new(
                Arc::new(value) as Arc<dyn Any>,
                name.and_then(|n| Some(n.name)),
                MsgType::Generic(GenericTypes::AvroValue),
            );
            container
        } else {
            panic!("not supported")
        }
    }
}
