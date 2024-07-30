use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::decoder::Decoder;
use crate::type_definitions::{CustomTypes, GenericTypes, MsgType, RawTypes};
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tracing_subscriber::fmt::format;

struct AvroSRDecoder {}

impl Decoder for AvroSRDecoder {
    fn decode(
        &self,
        msg: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let msg_type = msg.clone().get_batch_type();

        if let MsgType::Raw(RawTypes::Bytes) = msg_type.as_ref() {
            let sr_settings = SrSettings::new("some_url".to_string());
            let decoder = AvroDecoder::new(sr_settings);

            // takes a batch decodes each bytes based on its schemaid and regroup them back
            // in batches with the same schema name (so no regrouping is needed afterwards like in the typematcher).
            let batch = msg.get_batch();
            if let Batch::AnyBatch(any_batch) = batch.as_ref() {
                let mut batch_map: HashMap<String, Vec<Arc<dyn Any>>> = HashMap::new();
                any_batch.iter().for_each(|item| {
                    let payload = item.downcast_ref::<Vec<u8>>().unwrap();
                    let result = decoder.decode(Some(&payload[..])).unwrap();

                    let value = Arc::new(result.value) as Arc<dyn Any>;
                    if let Some(name) = result.name {
                        let full_name =
                            format!("{}.{}", name.namespace.unwrap_or_default(), name.name);
                        batch_map
                            .entry(full_name)
                            .or_insert_with(Vec::new)
                            .push(value);
                    }
                });

                let collected: Vec<Arc<dyn BatchContainer>> = batch_map
                    .iter()
                    .flat_map(move |(k, v)| {
                        std::iter::once(GenericBatchContainer::new(
                            Arc::new(Batch::AnyBatch(v.clone())),
                            Some(k.clone()),
                            MsgType::Generic(GenericTypes::AvroValue),
                        ) as Arc<dyn BatchContainer>)
                    })
                    .collect();

                Box::new(collected.into_iter())
            } else {
                panic!("not supported")
            }
        } else {
            panic!("not supported")
        }
    }
}
