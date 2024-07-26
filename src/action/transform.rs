use crate::action::Action;
use crate::container::MsgContainer;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use std::sync::Arc;

pub struct Transform<T>
// todo noch eine variante mit encode ... und mal schaauen ob ich mit crates doch noch als trait object hinbekomme
where
    T: Encoder,
{
    input: Arc<dyn Action>,
    encoder: Option<Arc<T>>,
    transformations: Arc<dyn Transformation>, // todo als Vec, aber dann muessen execute Vec<Arc<Container>> returnen (muss sowieso da sont kein 1:n mapping moeglich)
}
impl<T> Action for Transform<T>
where
    T: Encoder,
{
    fn execute(&self) -> Arc<dyn MsgContainer> {
        let result = self.input.execute();
        //self.transformations.execute(result, self.encoder.clone())
        self.transformations.execute(result)
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        Option::from(self.input.clone())
    }
}
