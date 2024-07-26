mod avro_value_converter;

use crate::container::MsgContainer;
use crate::type_mapper::TypeMapper;
use std::sync::Arc;

pub trait TypeConverter {
    fn convert(
        &self,
        msg: Arc<dyn MsgContainer>,
        mapper: Arc<dyn TypeMapper>,
    ) -> Arc<dyn MsgContainer>;
}
