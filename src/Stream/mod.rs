use crate::action::convert::Convert;
use crate::action::deserialize::Deserialize;
use crate::action::transform::Transform;
use crate::action::write::Write;
use crate::action::Action;
use crate::data_sink::DataSink;
use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::transformation::Transformation;
use crate::type_converter::TypeConverter;
use crate::type_mapper::TypeMapper;
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
    fn action(self: Arc<Self>) -> Arc<dyn Action>;
}

pub struct StreamImpl {
    pub action: Option<Arc<dyn Action>>,
}

impl Stream for StreamImpl {
    fn deserialize(self: Arc<Self>, decoder: Arc<dyn Decoder>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            action: Some(Deserialize::new(
                self.action.clone().expect("todo").child().unwrap(),
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
            action: Some(Convert::new(
                self.action.clone().expect("todo").child().unwrap(),
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
            action: Some(Transform::new(
                self.action.clone().expect("todo").child().unwrap(),
                encoder,
                transformations,
            )),
        })
    }

    fn write(self: Arc<Self>, data_sink: Arc<dyn DataSink>) -> Arc<dyn Stream> {
        Arc::new(StreamImpl {
            action: Some(Write::new(
                self.action.clone().expect("todo").child().unwrap(),
                data_sink,
            )),
        })
    }

    fn action(self: Arc<Self>) -> Arc<dyn Action> {
        self.action.clone().expect("")
    }
}
