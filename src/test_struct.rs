use apache_avro::AvroSchema;
use chrono::Utc;
use fake::{Dummy, Fake, Faker};
use serde::{Deserialize, Serialize};

#[derive(Debug, AvroSchema, Serialize, Deserialize)]
pub struct TestStruct {
    pub timestamp_ms: i64,
    pub uuid: String, // avro does not support uuid as type
    pub source: String,
    pub records_binaries: Vec<BinaryRecord>,
}

#[derive(Debug, AvroSchema, Serialize, Deserialize)]
pub struct BinaryRecord {
    pub id: i64,
    pub name: String,
    pub timestamp_ms: i64,
    pub binary_type: BinaryType,
    pub value: Vec<u8>,
}
#[derive(Debug, Dummy, AvroSchema, Serialize, Deserialize)]
pub enum BinaryType {
    A,
    B,
    C,
}

impl Dummy<Faker> for TestStruct {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &Faker, rng: &mut R) -> Self {
        Self {
            timestamp_ms: Utc::now().timestamp_millis(),
            uuid: Faker.fake(),
            source: Faker.fake_with_rng(rng),
            records_binaries: fake::vec![BinaryRecord; 1..400],
        }
    }
}

impl Dummy<Faker> for BinaryRecord {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &Faker, rng: &mut R) -> Self {
        Self {
            id: Faker.fake_with_rng(rng),
            name: Faker.fake_with_rng(rng),
            timestamp_ms: Utc::now().timestamp_millis(),
            binary_type: Faker.fake_with_rng(rng),
            value: fake::vec![u8; 1000..10000],
        }
    }
}
//////////////////////////////////////////////////////////////////////

#[derive(Debug, AvroSchema, Serialize)]
pub struct FlatA {
    pub timestamp_ms: i64,
    pub uuid: String, // avro does not support uuid as type
    pub source: String,
    pub value: Vec<u8>,
}

#[derive(Debug, AvroSchema, Serialize)]
pub struct FlatB {
    pub timestamp_ms: i64,
    pub uuid: String, // avro does not support uuid as type
    pub source: String,
    pub value: Vec<u8>,
}

#[derive(Debug, AvroSchema, Serialize)]
pub struct FlatC {
    pub timestamp_ms: i64,
    pub uuid: String, // avro does not support uuid as type
    pub source: String,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod test {
    use crate::test_struct::TestStruct;
    use fake::{Fake, Faker};

    #[test]
    fn build_test_struct() {
        let test_struct: TestStruct = Faker::fake(&Faker);

        println!("num records: {}", test_struct.records_binaries.len());

        test_struct
            .records_binaries
            .iter()
            .enumerate()
            .for_each(|(i, r)| println!("record {}, num bytes: {}", i, r.value.len()));
    }
}
