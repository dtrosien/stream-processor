use crate::action::Action;
use crate::container::MsgContainer;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use std::sync::Arc;

pub struct Transform
// todo noch eine variante mit encode ... und mal schaauen ob ich mit crates doch noch als trait object hinbekomme
{
    input: Arc<dyn Action>,
    //encoder: Option<Arc<dyn Encoder<T>>>,
    transformations: Vec<Arc<dyn Transformation>>,
}
impl Action for Transform {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        let input = self.input.execute();

        //self.transformations.execute(result, self.encoder.clone())
        Box::new(input.flat_map(move |container| {
            self.transformations
                .iter()
                .flat_map(move |t| t.execute(container.clone()).into_iter())
        }))
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
