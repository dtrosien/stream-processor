use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use apache_avro::{to_avro_datum, to_value, AvroSchema};
use fake::{Dummy, Faker};
use rand::Rng;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

/// creates up to three different input structs
/// if using only two different structs, the probability is not equally distributed
pub struct DummySource<T1, T2, T3>
where
    T1: Serialize + Dummy<Faker> + AvroSchema,
    T2: Serialize + Dummy<Faker> + AvroSchema,
    T3: Serialize + Dummy<Faker> + AvroSchema,
{
    pub dummy_data_1: PhantomData<T1>,
    pub dummy_data_2: PhantomData<T2>,
    pub dummy_data_3: PhantomData<T3>,
    pub batch_size: u64,
    pub s_id1: u32,
    pub s_id2: u32,
    pub s_id3: u32,
}

impl<T1, T2, T3> DummySource<T1, T2, T3>
where
    T1: Serialize + Dummy<Faker> + AvroSchema,
    T2: Serialize + Dummy<Faker> + AvroSchema,
    T3: Serialize + Dummy<Faker> + AvroSchema,
{
    pub fn new(batch_size: u64, s_id1: u32, s_id2: u32, s_id3: u32) -> Arc<Self> {
        Arc::new(DummySource::<T1, T2, T3> {
            dummy_data_1: PhantomData,
            dummy_data_2: PhantomData,
            dummy_data_3: PhantomData,
            s_id1,
            s_id2,
            s_id3,
            batch_size,
        })
    }

    fn create_msgs<T: Serialize + Dummy<Faker> + AvroSchema>(
        &self,
        num: u64,
        schema_id: u32,
    ) -> Vec<Arc<Vec<u8>>> {
        let magic_byte = 0u8;
        let id_bytes = schema_id.to_be_bytes();
        let schema = T::get_schema();
        (0..num)
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
            .collect::<Vec<_>>()
    }
}

impl<T1, T2, T3> DataSource for DummySource<T1, T2, T3>
where
    T1: Serialize + Dummy<Faker> + AvroSchema,
    T2: Serialize + Dummy<Faker> + AvroSchema,
    T3: Serialize + Dummy<Faker> + AvroSchema,
{
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let mut rng = rand::thread_rng();
        let split1 = rng.gen_range(0..self.batch_size);
        let remaining = self.batch_size - split1;
        let split2 = rng.gen_range(0..=remaining);
        let split3 = remaining - split2;

        let msg1 = self.create_msgs::<T1>(split1, self.s_id1);
        let msg2 = self.create_msgs::<T2>(split2, self.s_id2);
        let msg3 = self.create_msgs::<T3>(split3, self.s_id3);

        let msg = msg1
            .into_iter()
            .chain(msg2.into_iter())
            .chain(msg3.into_iter())
            .collect::<Vec<_>>();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(batch, None, HashMap::default());
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {}
}
