use crate::container::MsgContainer;
use crate::encoder::Encoder;
use std::sync::Arc;

pub trait Transformation {
    fn execute(
        &self,
        input: Arc<dyn MsgContainer>,
        //encoder: Option<Arc<dyn Encoder>>,
    ) -> Arc<dyn MsgContainer>;
}

// old example
// async fn transform(
//     input: Arc<dyn MsgContainer>,
//     encoder: Option<impl Encoder>,
// ) -> Arc<dyn MsgContainer> {
//     let msg_type = input.clone().get_msg_type();
//
//     match msg_type.as_ref() {
//         MsgType::Custom(c) => match c {
//             CustomTypes::A => {
//                 let msg = input.get_msg();
//                 let payload = msg.downcast_ref::<u64>().unwrap();
//                 encoder.unwrap().encode(payload).await.unwrap()
//             }
//
//             CustomTypes::B => {
//                 todo!()
//             }
//         },
//         _ => panic!("not supportet type"),
//     }
// }
