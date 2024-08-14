use crate::type_definitions::{CustomType, CustomTypes};
use std::sync::Arc;

pub trait TypeMapper: Send {
    fn map_name_to_type(&self, input: &str) -> Arc<dyn CustomType>;
}

// todo move example impl into integration tests
pub struct MapperImpl;

impl MapperImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(MapperImpl {})
    }
}
impl TypeMapper for MapperImpl {
    fn map_name_to_type(&self, input: &str) -> Arc<dyn CustomType> {
        match input {
            "some.namespace.StringMessage" => Arc::new(CustomTypes::StringMessage),
            "banana" => Arc::new(CustomTypes::B),
            _ => panic!("It's something else!{}", input),
        }
    }
}
