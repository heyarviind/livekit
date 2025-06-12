package mesh

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

// RelayMessage wraps RTP or RTCP data for forwarding via psrpc.
type RelayMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Packet  []byte   `protobuf:"bytes,1,opt,name=packet,proto3"`
	Packets [][]byte `protobuf:"bytes,2,rep,name=packets,proto3"`
}

func (x *RelayMessage) Reset()         { *x = RelayMessage{} }
func (x *RelayMessage) String() string { return protoimpl.X.MessageStringOf(x) }
func (*RelayMessage) ProtoMessage()    {}
func (x *RelayMessage) ProtoReflect() protoreflect.Message {
	mi := &file_livekit_sfu_mesh_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

var file_livekit_sfu_mesh_proto_msgTypes = make([]protoimpl.MessageInfo, 1)

func init() {
	file_livekit_sfu_mesh_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
		switch v := v.(*RelayMessage); i {
		case 0:
			return &v.state
		case 1:
			return &v.sizeCache
		case 2:
			return &v.unknownFields
		default:
			return nil
		}
	}
}
