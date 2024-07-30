use crate::action_plan::ActionPlan;
use crate::container::BatchContainer;
use crate::type_converter::TypeConverter;
use crate::type_mapper::TypeMapper;
use std::sync::Arc;

pub struct Convert {
    input: Arc<dyn ActionPlan>,
    mapper: Arc<dyn TypeMapper>,
    converter: Arc<dyn TypeConverter>,
}

impl Convert {
    pub fn new(
        input: Arc<dyn ActionPlan>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<Self> {
        Arc::new(Convert {
            input,
            mapper,
            converter,
        })
    }
}

impl ActionPlan for Convert {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let input = self.input.execute();

        Box::new(
            input
                .flat_map(move |container| self.converter.convert(container, self.mapper.clone()))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        Option::from(self.input.clone())
    }

    fn commit_batch(&self) {
        self.child().unwrap().commit_batch()
    }
}
