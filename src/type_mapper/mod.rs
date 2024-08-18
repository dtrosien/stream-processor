use crate::type_definitions::CustomType;
use std::sync::Arc;

pub trait TypeMapper: Send {
    fn map_name_to_type(&self, input: &str) -> Arc<dyn CustomType>;
}
