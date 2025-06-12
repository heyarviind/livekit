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
	"fmt"
	"sync"
	"time"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
)

// MeshSessionImpl implements MeshSession
type MeshSessionImpl struct {
	roomName livekit.RoomName
	nodeID   string
	logger   logger.Logger

	// State synchronization
	messageBus MessageBus

	// Participants
	remoteParticipants map[livekit.ParticipantID]*RemoteParticipant
	participantsMutex  sync.RWMutex

	// Media relay
	mediaRelay MediaRelay

	// Callbacks
	onRemoteParticipantAdded   func(*RemoteParticipant)
	onRemoteParticipantRemoved func(livekit.ParticipantID)
	onRemoteParticipantUpdated func(*RemoteParticipant)

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMeshSession creates a new mesh session
func NewMeshSession(
	roomName livekit.RoomName,
	nodeID string,
	messageBus MessageBus,
	mediaRelay MediaRelay,
	logger logger.Logger,
) *MeshSessionImpl {
	ctx, cancel := context.WithCancel(context.Background())

	return &MeshSessionImpl{
		roomName:           roomName,
		nodeID:             nodeID,
		messageBus:         messageBus,
		mediaRelay:         mediaRelay,
		logger:             logger,
		remoteParticipants: make(map[livekit.ParticipantID]*RemoteParticipant),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// AddRemoteParticipant adds a remote participant to the session
func (m *MeshSessionImpl) AddRemoteParticipant(participant *RemoteParticipant) error {
	m.participantsMutex.Lock()
	defer m.participantsMutex.Unlock()

	participantID := participant.ID()
	m.remoteParticipants[participantID] = participant

	m.logger.Infow("added remote participant",
		"participantID", participantID,
		"identity", participant.Identity(),
		"nodeID", participant.NodeID)

	if m.onRemoteParticipantAdded != nil {
		m.onRemoteParticipantAdded(participant)
	}

	return nil
}

// RemoveRemoteParticipant removes a remote participant from the session
func (m *MeshSessionImpl) RemoveRemoteParticipant(participantID livekit.ParticipantID) error {
	m.participantsMutex.Lock()
	defer m.participantsMutex.Unlock()

	participant, exists := m.remoteParticipants[participantID]
	if !exists {
		return nil
	}

	delete(m.remoteParticipants, participantID)

	m.logger.Infow("removed remote participant",
		"participantID", participantID,
		"identity", participant.Identity(),
		"nodeID", participant.NodeID)

	if m.onRemoteParticipantRemoved != nil {
		m.onRemoteParticipantRemoved(participantID)
	}

	return nil
}

// GetRemoteParticipants returns all remote participants
func (m *MeshSessionImpl) GetRemoteParticipants() []*RemoteParticipant {
	m.participantsMutex.RLock()
	defer m.participantsMutex.RUnlock()

	participants := make([]*RemoteParticipant, 0, len(m.remoteParticipants))
	for _, p := range m.remoteParticipants {
		participants = append(participants, p)
	}

	return participants
}

// BroadcastParticipantUpdate broadcasts a local participant update to the mesh
func (m *MeshSessionImpl) BroadcastParticipantUpdate(participant types.LocalParticipant) error {
	participantInfo := participant.ToProto()

	if err := m.messageBus.PublishParticipantUpdate(m.ctx, m.roomName, m.nodeID, participantInfo); err != nil {
		return fmt.Errorf("failed to broadcast participant update: %w", err)
	}

	return nil
}

// HandleStateUpdate processes incoming state updates from other nodes
func (m *MeshSessionImpl) HandleStateUpdate(nodeID string, participant *livekit.ParticipantInfo) error {
	if nodeID == m.nodeID {
		return nil
	}

	remoteParticipant := &RemoteParticipant{
		ParticipantInfo: participant,
		NodeID:          nodeID,
		TimedVersion:    utils.TimedVersion(0),
		JoinedAt:        time.Now(),
	}

	return m.AddRemoteParticipant(remoteParticipant)
}

// RequestRemoteTracks requests media tracks from remote nodes
func (m *MeshSessionImpl) RequestRemoteTracks(participantID livekit.ParticipantID, trackIDs []livekit.TrackID) error {
	m.participantsMutex.RLock()
	participant, exists := m.remoteParticipants[participantID]
	m.participantsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("remote participant not found: %s", participantID)
	}

	if err := m.mediaRelay.RequestTracks(m.ctx, participant.NodeID, trackIDs); err != nil {
		return fmt.Errorf("failed to request tracks from node %s: %w", participant.NodeID, err)
	}

	return nil
}

// StopRemoteTracks stops requesting tracks from remote nodes
func (m *MeshSessionImpl) StopRemoteTracks(participantID livekit.ParticipantID, trackIDs []livekit.TrackID) error {
	m.participantsMutex.RLock()
	participant, exists := m.remoteParticipants[participantID]
	m.participantsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("remote participant not found: %s", participantID)
	}

	if err := m.mediaRelay.StopTracks(m.ctx, participant.NodeID, trackIDs); err != nil {
		return fmt.Errorf("failed to stop tracks from node %s: %w", participant.NodeID, err)
	}

	return nil
}

// Close closes the mesh session
func (m *MeshSessionImpl) Close() error {
	m.cancel()

	if err := m.messageBus.UnsubscribeFromRoom(m.ctx, m.roomName); err != nil {
		m.logger.Warnw("failed to unsubscribe from room", err)
	}

	m.participantsMutex.Lock()
	m.remoteParticipants = make(map[livekit.ParticipantID]*RemoteParticipant)
	m.participantsMutex.Unlock()

	return nil
}

// StateUpdateHandler implementation
func (m *MeshSessionImpl) OnParticipantUpdate(nodeID string, participant *livekit.ParticipantInfo) {
	if err := m.HandleStateUpdate(nodeID, participant); err != nil {
		m.logger.Warnw("failed to handle participant update", err,
			"nodeID", nodeID,
			"participantID", participant.Sid)
	}
}

func (m *MeshSessionImpl) OnParticipantLeft(nodeID string, participantID livekit.ParticipantID) {
	if err := m.RemoveRemoteParticipant(participantID); err != nil {
		m.logger.Warnw("failed to remove participant", err,
			"nodeID", nodeID,
			"participantID", participantID)
	}
}

func (m *MeshSessionImpl) OnStateSyncRequest(nodeID string) {
	m.logger.Debugw("received state sync request", "fromNode", nodeID)
}

// Initialize starts the mesh session
func (m *MeshSessionImpl) Initialize() error {
	// Subscribe to mesh updates for this room
	if err := m.messageBus.SubscribeToRoom(m.ctx, m.roomName, m); err != nil {
		return fmt.Errorf("failed to subscribe to room: %w", err)
	}

	// Request current state from other nodes
	if err := m.messageBus.RequestStateSync(m.ctx, m.roomName); err != nil {
		m.logger.Warnw("failed to request initial state sync", err)
	}

	m.logger.Infow("mesh session initialized", "room", m.roomName, "nodeID", m.nodeID)

	return nil
}

// Callback setters
func (m *MeshSessionImpl) OnRemoteParticipantAdded(callback func(*RemoteParticipant)) {
	m.onRemoteParticipantAdded = callback
}

func (m *MeshSessionImpl) OnRemoteParticipantRemoved(callback func(livekit.ParticipantID)) {
	m.onRemoteParticipantRemoved = callback
}

func (m *MeshSessionImpl) OnRemoteParticipantUpdated(callback func(*RemoteParticipant)) {
	m.onRemoteParticipantUpdated = callback
}
