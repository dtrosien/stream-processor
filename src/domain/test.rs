// todo check if possible to pass avro namespace via annotations

use crate::object_store_sink::general::ToArrow;
use apache_avro::AvroSchema;
use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use fake::{faker, Fake};
use rand::random;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Person {
    pub id: u32,
    pub name: String,
    pub address: Option<Address>,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Address {
    pub street: String,
    pub number: u32,
}

pub fn get_test_person() -> Person {
    let address = Some(Address {
        street: faker::address::en::StreetName().fake(),
        number: random(),
    });

    Person {
        id: random(),
        name: faker::name::en::Name().fake(),
        address,
    }
}

impl ToArrow for Person {
    fn schema() -> Schema {
        let fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::UInt32, false),
        ];
        Schema::new(fields)
    }

    fn create_record_batch(data: &[Self]) -> RecordBatch {
        let names: Vec<String> = data.iter().map(|p| p.name.clone()).collect();
        let ids: Vec<u32> = data.iter().map(|p| p.id).collect();

        let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
        let id_array = Arc::new(UInt32Array::from(ids)) as ArrayRef;

        RecordBatch::try_from_iter(vec![("name", name_array), ("id", id_array)]).unwrap()
    }
}
