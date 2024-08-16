use crate::container::BatchContainer;
use crate::encoder::Encoder;
use std::sync::Arc;

pub trait Transformation {
    fn execute(
        &self,
        input: Arc<dyn BatchContainer>,
        encoder: Option<Arc<Encoder>>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
