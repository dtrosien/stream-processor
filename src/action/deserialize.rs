use crate::action::Action;
use crate::container::MsgContainer;
use crate::decoder::Decoder;
use std::sync::Arc;

struct Deserialize {
    input: Arc<dyn Action>,
    decoder: Arc<dyn Decoder>,
}
impl Action for Deserialize {
    fn execute(&self) -> Arc<dyn MsgContainer> {
        let result = self.input.execute();
        self.decoder.decode(result)
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
