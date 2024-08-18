use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::decoder::Decoder;
use crate::type_definitions::{MixedBatch, UniformBatch};
use apache_avro::types::Value;
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;

use std::collections::HashMap;
use std::sync::Arc;

pub struct AvroSRDecoder {
    sr_settings: SrSettings,
}

impl AvroSRDecoder {
    pub fn new(sr_settings: SrSettings) -> Arc<Self> {
        Arc::new(AvroSRDecoder { sr_settings })
    }
}

impl Decoder for AvroSRDecoder {
    fn decode(
        &self,
        msg: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let decoder = AvroDecoder::new(self.sr_settings.clone());

        // takes a batch decodes each bytes based on its schema_id and regroup them back
        // in batches with the same schema name (so no regrouping is needed afterward).
        let batch = msg.get_batch();
        if let Batch::Mixed(MixedBatch::Bytes(bytes)) = batch.as_ref() {
            let mut batch_map: HashMap<String, Vec<Arc<Value>>> = HashMap::new();
            bytes.iter().for_each(|item| {
                let item = item.as_ref();
                let result = decoder.decode(Some(&item[..])).unwrap();

                let value = Arc::new(result.value);
                if let Some(name) = result.name {
                    let full_name = format!("{}.{}", name.namespace.unwrap_or_default(), name.name);
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
                        Arc::new(Batch::Uniform(UniformBatch::AvroValue(v.clone()))),
                        Some(k.clone()),
                    ) as Arc<dyn BatchContainer>)
                })
                .collect();

            Box::new(collected.into_iter())
        } else {
            panic!("not supported")
        }
    }
}
