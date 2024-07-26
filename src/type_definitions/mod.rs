#[derive(Clone, Debug)]
pub enum RawTypes {
    Bytes,
    RecordBatch,
}

#[derive(Clone, Debug)]
pub enum CustomTypes {
    A,
    B,
}

#[derive(Clone, Debug)]
pub enum GenericTypes {
    AvroValue,
    RecordBatch,
}

#[derive(Clone, Debug)]
pub enum MsgType {
    Raw(RawTypes),
    Custom(CustomTypes),
    Generic(GenericTypes),
}
