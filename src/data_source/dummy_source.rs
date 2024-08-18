use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use apache_avro::{to_avro_datum, to_value, AvroSchema};
use fake::{Dummy, Faker};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct DummySource<T: Serialize + Dummy<Faker> + AvroSchema> {
    pub dummy_data: PhantomData<T>, // todo should be able to have really mixed data in the source... so multiple phantomdata
    pub batch_size: u64,
}

impl<T: Serialize + Dummy<Faker> + AvroSchema> DummySource<T> {
    pub fn new(batch_size: u64) -> Arc<Self> {
        Arc::new(DummySource::<T> {
            dummy_data: PhantomData,
            batch_size,
        })
    }
}

impl<T: Serialize + Dummy<Faker> + AvroSchema> DataSource for DummySource<T> {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let magic_byte = 0u8;
        let id_bytes = 1_u32.to_be_bytes();
        let schema = T::get_schema();
        let msg = (0..self.batch_size)
            .into_iter()
            .map(|_| {
                let a = T::dummy(&Faker);
                let avro_binary = to_avro_datum(&schema, to_value(a).unwrap()).unwrap();
                let mut byte_vector = Vec::new();
                byte_vector.push(magic_byte);
                byte_vector.extend(id_bytes);
                byte_vector.extend_from_slice(&avro_binary);
                Arc::new(byte_vector)
            })
            .collect::<Vec<_>>();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> = GenericBatchContainer::new(batch, None);
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {}
}
