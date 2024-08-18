use crate::container::BatchContainer;
use crate::encoder::Encoder;
use crate::type_mapper::TypeMapper;
use std::sync::Arc;

pub trait Transformation {
    fn execute(
        &self,
        input: Arc<dyn BatchContainer>,
        mapper: Option<Arc<dyn TypeMapper>>,
        encoder: Option<Arc<Encoder>>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
