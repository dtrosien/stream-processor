use stream_processor::type_definitions::CustomType;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CustomTypes {
    TestStruct,
}

impl CustomType for CustomTypes {
    fn get_type_name(&self) -> String {
        match self {
            CustomTypes::TestStruct => "TestStruct".to_string(),
        }
    }

    fn get_type_id(&self) -> u32 {
        match self {
            CustomTypes::TestStruct => 0,
        }
    }
}
