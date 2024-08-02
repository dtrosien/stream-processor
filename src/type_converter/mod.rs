pub mod avro_value_converter;

use crate::container::BatchContainer;
use crate::type_mapper::TypeMapper;
use std::sync::Arc;

pub trait TypeConverter {
    fn convert(
        &self,
        msg: Arc<dyn BatchContainer>,
        mapper: Arc<dyn TypeMapper>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
