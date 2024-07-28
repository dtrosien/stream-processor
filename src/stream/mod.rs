use crate::action_plan::convert::Convert;
use crate::action_plan::deserialize::Deserialize;
use crate::action_plan::transform::Transform;
use crate::action_plan::write::Write;
use crate::action_plan::ActionPlan;
use crate::data_sink::DataSink;
use crate::data_source::DataSource;
use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use crate::type_converter::TypeConverter;
use crate::type_mapper::TypeMapper;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

pub trait Stream {
    /// Apply Serialization
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream>;

    fn convert(
        self: Arc<Self>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<dyn Stream>;
    fn transform(
        self: Arc<Self>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream>;
    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream>;

    /// Get the Action
    fn action_plan(self: Arc<Self>) -> Arc<dyn ActionPlan>;
}

pub struct StreamImpl {
    pub plan: Option<Arc<dyn ActionPlan>>,
}

impl Stream for StreamImpl {
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Deserialize::new(
                self.plan.clone().expect("todo").child().unwrap(),
                decoder,
            )),
        })
    }

    fn convert(
        self: Arc<Self>,
        mapper: Arc<dyn TypeMapper>,
        converter: Arc<dyn TypeConverter>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Convert::new(
                self.plan.clone().expect("todo").child().unwrap(),
                mapper,
                converter,
            )),
        })
    }

    fn transform(
        self: Arc<Self>,
        encoder: Option<Arc<Encoder>>,
        transformations: Vec<Arc<dyn Transformation>>,
    ) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Transform::new(
                self.plan.clone().expect("todo").child().unwrap(),
                encoder,
                transformations,
            )),
        })
    }

    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            plan: Some(Write::new(
                self.plan.clone().expect("todo").child().unwrap(),
                data_sink,
            )),
        })
    }

    fn action_plan(self: Arc<Self>) -> Arc<dyn ActionPlan> {
        self.plan.clone().expect("")
    }
}

#[cfg(test)]
mod test {
    use crate::execution::ExecutionContext;
    use crate::stream::Stream;
    use rdkafka::ClientConfig;
    use std::collections::HashMap;

    fn build_stream() {
        let client_config = ClientConfig::new();

        let context = ExecutionContext::new(HashMap::default());
        let stream = context
            .kafka("topic".to_string(), client_config)
            .deserialize(todo!())
            .convert(todo!(), todo!())
            .transform(todo!(), todo!())
            .write(todo!());

        let action_plan = stream.action_plan();

        action_plan.execute();
    }
}
