use crate::action::Action;
use crate::container::MsgContainer;
use crate::data_sink::DataSink;
use std::sync::Arc;

pub struct Write {
    input: Arc<dyn Action>,
    data_sink: Arc<dyn DataSink>,
}

impl Action for Write {
    fn execute(&self) -> Arc<dyn MsgContainer> {
        let result = self.input.execute();
        self.data_sink.write(result);
        todo!()
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
