package mesh

import "google.golang.org/protobuf/reflect/protoreflect"

// MediaMessage is used to forward media data over psrpc streams.
type MediaMessage struct {
	Packet  []byte   // RTP packet data
	Packets [][]byte // batched RTCP packets
}

func (*MediaMessage) Reset()                             {}
func (*MediaMessage) String() string                     { return "MediaMessage" }
func (*MediaMessage) ProtoMessage()                      {}
func (*MediaMessage) ProtoReflect() protoreflect.Message { return nil }
