use crate::type_definitions::CustomTypes;
use std::sync::Arc;

pub trait TypeMapper: Send {
    fn map_name_to_type(&self, input: &str) -> CustomTypes;
}

pub struct MapperImpl;

impl MapperImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(MapperImpl {})
    }
}
impl TypeMapper for MapperImpl {
    fn map_name_to_type(&self, input: &str) -> CustomTypes {
        match input {
            "some.namespace.StringMessage" => CustomTypes::StringMessage,
            "banana" => CustomTypes::B,
            _ => panic!("It's something else!{}", input),
        }
    }
}
