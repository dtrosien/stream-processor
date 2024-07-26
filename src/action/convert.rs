use crate::action::Action;
use crate::container::MsgContainer;
use crate::type_converter::TypeConverter;
use crate::type_mapper::TypeMapper;
use std::sync::Arc;

pub struct Convert {
    input: Arc<dyn Action>,
    mapper: Arc<dyn TypeMapper>,
    converter: Arc<dyn TypeConverter>,
}
impl Action for Convert {
    fn execute(&self) -> Arc<dyn MsgContainer> {
        let result = self.input.execute();
        self.converter.convert(result, self.mapper.clone())
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
