use crate::custom_types::CustomTypes;
use std::sync::Arc;
use stream_processor::type_definitions::CustomType;
use stream_processor::type_mapper::TypeMapper;

pub struct MapperTestImpl;

impl MapperTestImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(MapperTestImpl {})
    }
}
impl TypeMapper for MapperTestImpl {
    fn map_name_to_type(&self, input: &str) -> Arc<dyn CustomType> {
        match input {
            "some.namespace.TestStruct" => Arc::new(CustomTypes::TestStruct),
            _ => panic!("It's something else!{}", input),
        }
    }
}
