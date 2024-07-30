use crate::action_plan::ActionPlan;
use crate::container::BatchContainer;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use std::sync::Arc;

pub struct Transform {
    input: Arc<dyn ActionPlan>,
    encoder: Option<Arc<Encoder>>,
    transformations: Vec<Arc<dyn Transformation>>,
}

impl Transform {
    pub fn new(
        input: Arc<dyn ActionPlan>,
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

impl ActionPlan for Transform {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let input = self.input.execute();

        Box::new(input.flat_map(move |container| {
            self.transformations.iter().flat_map(move |t| {
                t.execute(container.clone(), self.encoder.clone())
                    .into_iter()
            })
        }))
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        Option::from(self.input.clone())
    }

    fn commit_batch(&self) {
        self.child().unwrap().commit_batch()
    }
}
