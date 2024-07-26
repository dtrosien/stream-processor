use crate::type_definitions::CustomTypes;

pub trait TypeMapper: Send {
    fn map_name_to_type(&self, input: &str) -> CustomTypes;
}

pub struct MapperImpl;
impl TypeMapper for MapperImpl {
    fn map_name_to_type(&self, input: &str) -> CustomTypes {
        match input {
            "apple" => CustomTypes::A,
            "banana" => CustomTypes::B,
            _ => panic!("It's something else!"),
        }
    }
}
