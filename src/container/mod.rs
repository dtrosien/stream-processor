use crate::type_definitions::MsgType;
use std::any::Any;
use std::sync::Arc;

pub trait MsgContainer {
    fn get_sink(self: Arc<Self>) -> Option<String>;
    fn get_msg_type(self: Arc<Self>) -> Arc<MsgType>;
    fn get_msg(self: Arc<Self>) -> Arc<dyn Any>; // todo evtl als batch? Dann waere es ein BatchContainer
    fn get_msg_name(self: Arc<Self>) -> Option<String>;
}

// todo maybe include batch infos for commit

pub struct GenericMsgContainer {
    msg: Arc<dyn Any>,
    msg_name: Option<String>,
    msg_type: Arc<MsgType>,
}

impl GenericMsgContainer {
    pub fn new(msg: Arc<dyn Any>, msg_name: Option<String>, msg_type: MsgType) -> Arc<Self> {
        Arc::new(GenericMsgContainer {
            msg,
            msg_name,
            msg_type: Arc::new(msg_type),
        })
    }
}

impl MsgContainer for GenericMsgContainer {
    fn get_sink(self: Arc<Self>) -> Option<String> {
        None
    }

    fn get_msg_type(self: Arc<Self>) -> Arc<MsgType> {
        self.msg_type.clone()
    }

    fn get_msg(self: Arc<Self>) -> Arc<dyn Any> {
        self.msg.clone()
    }

    fn get_msg_name(self: Arc<Self>) -> Option<String> {
        self.msg_name.clone()
    }
}
