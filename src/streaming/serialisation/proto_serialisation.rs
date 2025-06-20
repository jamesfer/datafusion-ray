pub trait SerialiseToProto {
    type ProtoType: prost::Message;
    
    fn to_proto(&self) -> Self::ProtoType;
    fn from_proto(proto: Self::ProtoType) -> Self;
}