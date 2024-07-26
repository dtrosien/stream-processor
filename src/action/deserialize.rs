use crate::action::Action;
use crate::container::MsgContainer;
use crate::decoder::Decoder;
use std::sync::Arc;

pub struct Deserialize {
    input: Arc<dyn Action>,
    decoder: Arc<dyn Decoder>,
}

impl Deserialize {
    pub fn new(input: Arc<dyn Action>, decoder: Arc<dyn Decoder>) -> Arc<Self> {
        Arc::new(Deserialize { input, decoder })
    }
}
impl Action for Deserialize {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        let input = self.input.execute();
        Box::new(input.map(move |container| self.decoder.decode(container)))
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
