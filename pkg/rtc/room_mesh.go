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

package rtc

import (
	"context"
	"fmt"
	"sync"

	"github.com/livekit/livekit-server/pkg/agent"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc/mesh"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/protocol/livekit"
)

// MeshRoom extends Room with mesh networking capabilities
type MeshRoom struct {
	*Room

	// Mesh components
	meshSession mesh.MeshSession
	meshEnabled bool
	meshConfig  config.MeshConfig

	// Remote participants from other nodes
	remoteParticipants map[livekit.ParticipantID]*mesh.RemoteParticipant
	remotePartsMutex   sync.RWMutex
}

// NewMeshRoom creates a room with optional mesh support
func NewMeshRoom(
	room *livekit.Room,
	internal *livekit.RoomInternal,
	config WebRTCConfig,
	roomConfig config.RoomConfig,
	audioConfig *sfu.AudioConfig,
	serverInfo *livekit.ServerInfo,
	telemetry telemetry.TelemetryService,
	agentClient agent.Client,
	agentStore AgentStore,
	egressLauncher EgressLauncher,
	meshConfig *config.MeshConfig,
) *MeshRoom {
	baseRoom := NewRoom(room, internal, config, roomConfig, audioConfig, serverInfo, telemetry, agentClient, agentStore, egressLauncher)

	meshRoom := &MeshRoom{
		Room:               baseRoom,
		meshEnabled:        meshConfig != nil && meshConfig.Enabled,
		remoteParticipants: make(map[livekit.ParticipantID]*mesh.RemoteParticipant),
	}

	if meshConfig != nil {
		meshRoom.meshConfig = *meshConfig
	}

	// Initialize mesh session if enabled
	if meshRoom.meshEnabled {
		if err := meshRoom.initializeMesh(); err != nil {
			baseRoom.logger.Warnw("failed to initialize mesh", err)
			meshRoom.meshEnabled = false
		}
	}

	return meshRoom
}

// initializeMesh sets up the mesh networking components
func (r *MeshRoom) initializeMesh() error {
	roomName := r.Name()
	nodeID := r.meshConfig.NodeID

	// Create message bus
	messageBus, err := mesh.NewRedisMessageBus(nodeID, r.meshConfig.MessageBus, r.logger)
	if err != nil {
		return fmt.Errorf("failed to create message bus: %w", err)
	}

	// Create media relay (stub implementation for now)
	var mediaRelay mesh.MediaRelay = &stubMediaRelay{}

	// Create mesh session
	meshSession := mesh.NewMeshSession(roomName, nodeID, messageBus, mediaRelay, r.logger)
	r.meshSession = meshSession

	// Set up callbacks
	meshSession.OnRemoteParticipantAdded(r.onRemoteParticipantAdded)
	meshSession.OnRemoteParticipantRemoved(r.onRemoteParticipantRemoved)
	meshSession.OnRemoteParticipantUpdated(r.onRemoteParticipantUpdated)

	// Initialize the mesh session
	if err := meshSession.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize mesh session: %w", err)
	}

	r.logger.Infow("mesh networking enabled",
		"nodeID", nodeID,
		"room", roomName)

	return nil
}

// GetAllParticipants returns both local and remote participants
func (r *MeshRoom) GetAllParticipants() []types.Participant {
	var allParticipants []types.Participant

	// Add local participants
	localParticipants := r.GetLocalParticipants()
	for _, p := range localParticipants {
		allParticipants = append(allParticipants, p)
	}

	// Add remote participants if mesh is enabled
	if r.meshEnabled {
		r.remotePartsMutex.RLock()
		for _, p := range r.remoteParticipants {
			allParticipants = append(allParticipants, p)
		}
		r.remotePartsMutex.RUnlock()
	}

	return allParticipants
}

// GetAllParticipantCount returns total participant count including remote
func (r *MeshRoom) GetAllParticipantCount() int {
	count := r.GetParticipantCount()

	if r.meshEnabled {
		r.remotePartsMutex.RLock()
		count += len(r.remoteParticipants)
		r.remotePartsMutex.RUnlock()
	}

	return count
}

// GetRemoteParticipants returns all remote participants
func (r *MeshRoom) GetRemoteParticipants() []*mesh.RemoteParticipant {
	if !r.meshEnabled {
		return nil
	}

	r.remotePartsMutex.RLock()
	defer r.remotePartsMutex.RUnlock()

	participants := make([]*mesh.RemoteParticipant, 0, len(r.remoteParticipants))
	for _, p := range r.remoteParticipants {
		participants = append(participants, p)
	}

	return participants
}

// BroadcastLocalParticipantUpdate broadcasts a local participant update to the mesh
func (r *MeshRoom) BroadcastLocalParticipantUpdate(participant types.LocalParticipant) {
	if !r.meshEnabled || r.meshSession == nil {
		return
	}

	if err := r.meshSession.BroadcastParticipantUpdate(participant); err != nil {
		r.logger.Warnw("failed to broadcast participant update", err,
			"participantID", participant.ID(),
			"identity", participant.Identity())
	}
}

// Override Join to broadcast new participant to mesh
func (r *MeshRoom) Join(participant types.LocalParticipant, requestSource routing.MessageSource, opts *ParticipantOptions, iceServers []*livekit.ICEServer) error {
	// Call base room join
	err := r.Room.Join(participant, requestSource, opts, iceServers)
	if err != nil {
		return err
	}

	// Broadcast to mesh
	r.BroadcastLocalParticipantUpdate(participant)

	return nil
}

// Override RemoveParticipant to notify mesh
func (r *MeshRoom) RemoveParticipant(identity livekit.ParticipantIdentity, pID livekit.ParticipantID, reason types.ParticipantCloseReason) {
	// Call base room remove
	r.Room.RemoveParticipant(identity, pID, reason)

	// TODO: Notify mesh about participant removal
	r.logger.Debugw("participant removed from local room",
		"identity", identity,
		"participantID", pID,
		"reason", reason.String())
}

// Close the mesh room
func (r *MeshRoom) Close(reason types.ParticipantCloseReason) {
	// Close mesh session first
	if r.meshEnabled && r.meshSession != nil {
		if err := r.meshSession.Close(); err != nil {
			r.logger.Warnw("failed to close mesh session", err)
		}
	}

	// Close base room
	r.Room.Close(reason)
}

// Mesh event handlers

func (r *MeshRoom) onRemoteParticipantAdded(participant *mesh.RemoteParticipant) {
	r.remotePartsMutex.Lock()
	r.remoteParticipants[participant.ID()] = participant
	r.remotePartsMutex.Unlock()

	r.logger.Infow("remote participant added",
		"participantID", participant.ID(),
		"identity", participant.Identity(),
		"nodeID", participant.NodeID)

	// Notify local participants about the new remote participant
	r.broadcastRemoteParticipantToLocal(participant)
}

func (r *MeshRoom) onRemoteParticipantRemoved(participantID livekit.ParticipantID) {
	r.remotePartsMutex.Lock()
	participant, exists := r.remoteParticipants[participantID]
	if exists {
		delete(r.remoteParticipants, participantID)
	}
	r.remotePartsMutex.Unlock()

	if exists {
		r.logger.Infow("remote participant removed",
			"participantID", participantID,
			"identity", participant.Identity(),
			"nodeID", participant.NodeID)

		// Notify local participants about the removed remote participant
		r.notifyRemoteParticipantLeft(participant)
	}
}

func (r *MeshRoom) onRemoteParticipantUpdated(participant *mesh.RemoteParticipant) {
	r.remotePartsMutex.Lock()
	r.remoteParticipants[participant.ID()] = participant
	r.remotePartsMutex.Unlock()

	r.logger.Debugw("remote participant updated",
		"participantID", participant.ID(),
		"identity", participant.Identity(),
		"nodeID", participant.NodeID)

	// Notify local participants about the updated remote participant
	r.broadcastRemoteParticipantToLocal(participant)
}

// broadcastRemoteParticipantToLocal notifies local participants about remote participant changes
func (r *MeshRoom) broadcastRemoteParticipantToLocal(remoteParticipant *mesh.RemoteParticipant) {
	participantInfo := remoteParticipant.ToProto()
	updates := []*ParticipantUpdate{{
		ParticipantInfo: participantInfo,
	}}

	localParticipants := r.GetLocalParticipants()
	SendParticipantUpdates(updates, localParticipants)
}

// notifyRemoteParticipantLeft notifies local participants that a remote participant left
func (r *MeshRoom) notifyRemoteParticipantLeft(remoteParticipant *mesh.RemoteParticipant) {
	participantInfo := remoteParticipant.ToProto()
	participantInfo.State = livekit.ParticipantInfo_DISCONNECTED

	updates := []*ParticipantUpdate{{
		ParticipantInfo: participantInfo,
		CloseReason:     types.ParticipantCloseReasonNone,
	}}

	localParticipants := r.GetLocalParticipants()
	SendParticipantUpdates(updates, localParticipants)
}

// Stub media relay implementation
type stubMediaRelay struct{}

func (s *stubMediaRelay) Start(ctx context.Context) error { return nil }
func (s *stubMediaRelay) Stop() error                     { return nil }
func (s *stubMediaRelay) RequestTracks(ctx context.Context, nodeID string, trackIDs []livekit.TrackID) error {
	return nil
}
func (s *stubMediaRelay) StopTracks(ctx context.Context, nodeID string, trackIDs []livekit.TrackID) error {
	return nil
}
func (s *stubMediaRelay) SendPackets(packets []mesh.RelayPacket) error { return nil }
