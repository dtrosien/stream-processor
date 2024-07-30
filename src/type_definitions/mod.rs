// todo I guess most of the types can just be included in the Batch Enum?! Whcih would then make the GenercBatch clearer since MsgType is not needed. Think about how to group best (also keep in mind homogenious and hetro batches)

#[derive(Clone, Debug)]
pub enum RawTypes {
    Bytes,
    RecordBatch,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
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
