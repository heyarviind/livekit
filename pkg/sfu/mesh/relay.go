package mesh

import (
	"sync"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc"
)

// Relay forwards media to a remote SFU node using psrpc streams.
type Relay struct {
	stream psrpc.Stream[*MediaMessage, *MediaMessage]
	logger logger.Logger
	mu     sync.Mutex
}

func NewRelay(stream psrpc.Stream[*MediaMessage, *MediaMessage], l logger.Logger) *Relay {
	return &Relay{stream: stream, logger: l}
}

// ForwardRTP forwards an RTP packet to the remote node.
func (r *Relay) ForwardRTP(pkt *rtp.Packet) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	b, err := pkt.Marshal()
	if err != nil {
		return err
	}
	return r.stream.Send(&MediaMessage{Packet: b})
}

// ForwardRTCP forwards RTCP packets to the remote node.
func (r *Relay) ForwardRTCP(pkts []rtcp.Packet) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	data := make([][]byte, 0, len(pkts))
	for _, p := range pkts {
		b, err := p.Marshal()
		if err != nil {
			return err
		}
		data = append(data, b)
	}
	return r.stream.Send(&MediaMessage{Packets: data})
}

// Close terminates the underlying psrpc stream.
func (r *Relay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stream.Close(nil)
}
