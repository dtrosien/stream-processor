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
        // todo check if this can be improve : currently her and in trafo are strings or ints required for mapping ... maye this is redundant -> go through whole pipeline and think about the concept once more
        // todo looks like the custom_type impl is enough, then ther is only the mapping in trafo .. on the otherhand: how do implement static strct deserialization in deserializer... this class was also ment for that
        match input {
            "some.namespace.TestStruct" => Arc::new(CustomTypes::TestStruct),
            "some.namespace.AnotherTestStruct" => Arc::new(CustomTypes::AnotherTestStruct),
            _ => panic!("It's something else!{}", input),
        }
    }
}
