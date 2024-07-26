use crate::action::Action;
use crate::container::MsgContainer;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use std::sync::Arc;

pub struct Transform {
    input: Arc<dyn Action>,
    encoder: Option<Arc<Encoder>>,
    transformations: Vec<Arc<dyn Transformation>>,
}

impl Transform {
    pub fn new(
        input: Arc<dyn Action>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<Self> {
        Arc::new(Transform {
            input,
            encoder,
            transformations,
        })
    }
}

impl Action for Transform {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        let input = self.input.execute();

        Box::new(input.flat_map(move |container| {
            self.transformations.iter().flat_map(move |t| {
                t.execute(container.clone(), self.encoder.clone())
                    .into_iter()
            })
        }))
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
