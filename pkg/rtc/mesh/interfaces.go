// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mesh

import (
	"context"
	"time"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"
)

// MeshNode represents a node in the mesh network
type MeshNode struct {
	ID       string    `json:"id"`
	Region   string    `json:"region"`
	Address  string    `json:"address"`
	Port     int       `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	Capacity float32   `json:"capacity"` // 0.0 to 1.0
}

// RemoteParticipant represents a participant connected to another node
type RemoteParticipant struct {
	ParticipantInfo *livekit.ParticipantInfo
	NodeID          string
	TimedVersion    utils.TimedVersion
	JoinedAt        time.Time
}

// Implement the base Participant interface
func (r *RemoteParticipant) ID() livekit.ParticipantID {
	return livekit.ParticipantID(r.ParticipantInfo.Sid)
}

func (r *RemoteParticipant) Identity() livekit.ParticipantIdentity {
	return livekit.ParticipantIdentity(r.ParticipantInfo.Identity)
}

func (r *RemoteParticipant) State() livekit.ParticipantInfo_State {
	return r.ParticipantInfo.State
}

func (r *RemoteParticipant) ConnectedAt() time.Time {
	return r.JoinedAt
}

func (r *RemoteParticipant) CloseReason() types.ParticipantCloseReason {
	return types.ParticipantCloseReasonNone
}

func (r *RemoteParticipant) Kind() livekit.ParticipantInfo_Kind {
	return r.ParticipantInfo.Kind
}

func (r *RemoteParticipant) IsRecorder() bool {
	return r.ParticipantInfo.Kind == livekit.ParticipantInfo_EGRESS
}

func (r *RemoteParticipant) IsDependent() bool {
	return r.IsRecorder()
}

func (r *RemoteParticipant) IsAgent() bool {
	return r.ParticipantInfo.Kind == livekit.ParticipantInfo_AGENT
}

func (r *RemoteParticipant) CanSkipBroadcast() bool {
	return false
}

func (r *RemoteParticipant) Version() utils.TimedVersion {
	return r.TimedVersion
}

func (r *RemoteParticipant) ToProto() *livekit.ParticipantInfo {
	return r.ParticipantInfo
}

func (r *RemoteParticipant) IsPublisher() bool {
	return len(r.ParticipantInfo.Tracks) > 0
}

func (r *RemoteParticipant) GetPublishedTrack(trackID livekit.TrackID) types.MediaTrack {
	// Remote participants don't have direct access to media tracks
	return nil
}

func (r *RemoteParticipant) GetPublishedTracks() []types.MediaTrack {
	return nil
}

func (r *RemoteParticipant) RemovePublishedTrack(track types.MediaTrack, isExpectedToResume bool, shouldClose bool) {
	// No-op for remote participants
}

func (r *RemoteParticipant) GetAudioLevel() (smoothedLevel float64, active bool) {
	return 0, false
}

func (r *RemoteParticipant) HasPermission(trackID livekit.TrackID, subIdentity livekit.ParticipantIdentity) bool {
	return true // Permissions are handled by the source node
}

func (r *RemoteParticipant) Hidden() bool {
	return r.ParticipantInfo.Permission != nil && r.ParticipantInfo.Permission.Hidden
}

func (r *RemoteParticipant) Close(sendLeave bool, reason types.ParticipantCloseReason, isExpectedToResume bool) error {
	// Remote participants are closed via mesh coordination
	return nil
}

func (r *RemoteParticipant) SubscriptionPermission() (*livekit.SubscriptionPermission, utils.TimedVersion) {
	// Convert ParticipantPermission to SubscriptionPermission for remote participants
	if r.ParticipantInfo.Permission == nil {
		return nil, r.TimedVersion
	}
	return &livekit.SubscriptionPermission{
		AllParticipants:  r.ParticipantInfo.Permission.CanSubscribe,
		TrackPermissions: nil, // Remote participants don't have track-level permissions
	}, r.TimedVersion
}

func (r *RemoteParticipant) UpdateSubscriptionPermission(
	subscriptionPermission *livekit.SubscriptionPermission,
	timedVersion utils.TimedVersion,
	resolverBySid func(participantID livekit.ParticipantID) types.LocalParticipant,
) error {
	if timedVersion.After(r.TimedVersion) {
		// For remote participants, we only update basic permission state
		if r.ParticipantInfo.Permission == nil {
			r.ParticipantInfo.Permission = &livekit.ParticipantPermission{}
		}
		r.ParticipantInfo.Permission.CanSubscribe = subscriptionPermission.AllParticipants
		r.TimedVersion = timedVersion
	}
	return nil
}

func (r *RemoteParticipant) DebugInfo() map[string]interface{} {
	return map[string]interface{}{
		"type":        "remote",
		"nodeID":      r.NodeID,
		"identity":    string(r.Identity()),
		"state":       r.State().String(),
		"connectedAt": r.ConnectedAt,
	}
}

func (r *RemoteParticipant) OnMetrics(callback func(types.Participant, *livekit.DataPacket)) {
	// Remote participants don't generate metrics directly
}

// MessageBus handles state synchronization across mesh nodes
type MessageBus interface {
	// Publish a state update to all nodes hosting this room
	PublishParticipantUpdate(ctx context.Context, roomName livekit.RoomName, nodeID string, participant *livekit.ParticipantInfo) error

	// Subscribe to state updates for a room
	SubscribeToRoom(ctx context.Context, roomName livekit.RoomName, handler StateUpdateHandler) error

	// Unsubscribe from a room
	UnsubscribeFromRoom(ctx context.Context, roomName livekit.RoomName) error

	// Request current state from other nodes
	RequestStateSync(ctx context.Context, roomName livekit.RoomName) error

	// Close the message bus
	Close() error
}

// StateUpdateHandler processes incoming state updates
type StateUpdateHandler interface {
	OnParticipantUpdate(nodeID string, participant *livekit.ParticipantInfo)
	OnParticipantLeft(nodeID string, participantID livekit.ParticipantID)
	OnStateSyncRequest(nodeID string)
}

// MediaRelay handles media forwarding between SFU nodes
type MediaRelay interface {
	// Start the relay server
	Start(ctx context.Context) error

	// Stop the relay server
	Stop() error

	// Request media tracks from another node
	RequestTracks(ctx context.Context, nodeID string, trackIDs []livekit.TrackID) error

	// Stop requesting tracks from a node
	StopTracks(ctx context.Context, nodeID string, trackIDs []livekit.TrackID) error

	// Send media packets to subscribers
	SendPackets(packets []RelayPacket) error
}

// RelayPacket represents a media packet being relayed
type RelayPacket struct {
	TrackID    livekit.TrackID
	RTPPacket  []byte
	Timestamp  int64
	SourceNode string
}

// NodeDiscovery handles finding and tracking mesh nodes
type NodeDiscovery interface {
	// Start discovery service
	Start(ctx context.Context) error

	// Stop discovery service
	Stop() error

	// Register this node as hosting a room
	RegisterRoom(ctx context.Context, roomName livekit.RoomName) error

	// Unregister this node from hosting a room
	UnregisterRoom(ctx context.Context, roomName livekit.RoomName) error

	// Get all nodes hosting a room
	GetNodesForRoom(ctx context.Context, roomName livekit.RoomName) ([]*MeshNode, error)

	// Update node capacity
	UpdateCapacity(capacity float32) error

	// Get node latency map
	GetNodeLatencies() map[string]time.Duration
}

// MeshSession manages a distributed session across multiple nodes
type MeshSession interface {
	// Add a remote participant to the session
	AddRemoteParticipant(participant *RemoteParticipant) error

	// Remove a remote participant from the session
	RemoveRemoteParticipant(participantID livekit.ParticipantID) error

	// Get all remote participants
	GetRemoteParticipants() []*RemoteParticipant

	// Broadcast participant update to mesh
	BroadcastParticipantUpdate(participant types.LocalParticipant) error

	// Handle incoming state updates
	HandleStateUpdate(nodeID string, participant *livekit.ParticipantInfo) error

	// Request media tracks from remote nodes
	RequestRemoteTracks(participantID livekit.ParticipantID, trackIDs []livekit.TrackID) error

	// Stop requesting remote tracks
	StopRemoteTracks(participantID livekit.ParticipantID, trackIDs []livekit.TrackID) error

	// Close the mesh session
	Close() error
}
