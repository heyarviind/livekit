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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

const (
	meshChannelPrefix = "livekit:mesh:"
	meshStatePrefix   = "livekit:state:"
	meshLockPrefix    = "livekit:lock:"
)

// StateUpdate represents a mesh state update message
type StateUpdate struct {
	Type          string                   `json:"type"`
	RoomName      string                   `json:"room_name"`
	NodeID        string                   `json:"node_id"`
	Participant   *livekit.ParticipantInfo `json:"participant,omitempty"`
	ParticipantID string                   `json:"participant_id,omitempty"`
	Timestamp     time.Time                `json:"timestamp"`
}

// RedisMessageBus implements MessageBus using Redis pub/sub
type RedisMessageBus struct {
	client redis.UniversalClient
	nodeID string
	logger logger.Logger

	// Subscriptions
	subscriptions map[livekit.RoomName]*subscription
	subsMutex     sync.RWMutex

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
}

type subscription struct {
	roomName livekit.RoomName
	handler  StateUpdateHandler
	pubsub   *redis.PubSub
	cancel   context.CancelFunc
}

// NewRedisMessageBus creates a new Redis-based message bus
func NewRedisMessageBus(nodeID string, cfg config.MessageBusConfig, logger logger.Logger) (*RedisMessageBus, error) {
	opts := &redis.UniversalOptions{
		Addrs:    []string{cfg.Address},
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewUniversalClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	return &RedisMessageBus{
		client:        client,
		nodeID:        nodeID,
		logger:        logger,
		subscriptions: make(map[livekit.RoomName]*subscription),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// PublishParticipantUpdate publishes a participant update to the mesh
func (r *RedisMessageBus) PublishParticipantUpdate(ctx context.Context, roomName livekit.RoomName, nodeID string, participant *livekit.ParticipantInfo) error {
	update := StateUpdate{
		Type:        "participant_update",
		RoomName:    string(roomName),
		NodeID:      nodeID,
		Participant: participant,
		Timestamp:   time.Now(),
	}

	data, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal state update: %w", err)
	}

	channel := meshChannelPrefix + string(roomName)
	if err := r.client.Publish(ctx, channel, data).Err(); err != nil {
		return fmt.Errorf("failed to publish to %s: %w", channel, err)
	}

	// Also store current state for new nodes
	stateKey := meshStatePrefix + string(roomName) + ":" + nodeID + ":" + participant.Sid
	participantData, err := proto.Marshal(participant)
	if err != nil {
		return fmt.Errorf("failed to marshal participant: %w", err)
	}

	if err := r.client.Set(ctx, stateKey, participantData, 30*time.Minute).Err(); err != nil {
		r.logger.Warnw("failed to store participant state", err, "key", stateKey)
	}

	return nil
}

// SubscribeToRoom subscribes to state updates for a room
func (r *RedisMessageBus) SubscribeToRoom(ctx context.Context, roomName livekit.RoomName, handler StateUpdateHandler) error {
	r.subsMutex.Lock()
	defer r.subsMutex.Unlock()

	// Check if already subscribed
	if _, exists := r.subscriptions[roomName]; exists {
		return fmt.Errorf("already subscribed to room %s", roomName)
	}

	channel := meshChannelPrefix + string(roomName)
	pubsub := r.client.Subscribe(ctx, channel)

	subCtx, subCancel := context.WithCancel(r.ctx)

	sub := &subscription{
		roomName: roomName,
		handler:  handler,
		pubsub:   pubsub,
		cancel:   subCancel,
	}

	r.subscriptions[roomName] = sub

	// Start processing messages
	go r.processMessages(subCtx, sub)

	r.logger.Infow("subscribed to mesh room", "room", roomName, "channel", channel)

	return nil
}

// UnsubscribeFromRoom unsubscribes from room updates
func (r *RedisMessageBus) UnsubscribeFromRoom(ctx context.Context, roomName livekit.RoomName) error {
	r.subsMutex.Lock()
	defer r.subsMutex.Unlock()

	sub, exists := r.subscriptions[roomName]
	if !exists {
		return nil
	}

	sub.cancel()
	sub.pubsub.Close()
	delete(r.subscriptions, roomName)

	// Clean up state keys for this node
	pattern := meshStatePrefix + string(roomName) + ":" + r.nodeID + ":*"
	keys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil {
		r.logger.Warnw("failed to get state keys for cleanup", err, "pattern", pattern)
	} else if len(keys) > 0 {
		if err := r.client.Del(ctx, keys...).Err(); err != nil {
			r.logger.Warnw("failed to clean up state keys", err, "keys", keys)
		}
	}

	r.logger.Infow("unsubscribed from mesh room", "room", roomName)

	return nil
}

// RequestStateSync requests current state from other nodes
func (r *RedisMessageBus) RequestStateSync(ctx context.Context, roomName livekit.RoomName) error {
	update := StateUpdate{
		Type:      "state_sync_request",
		RoomName:  string(roomName),
		NodeID:    r.nodeID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal sync request: %w", err)
	}

	channel := meshChannelPrefix + string(roomName)
	if err := r.client.Publish(ctx, channel, data).Err(); err != nil {
		return fmt.Errorf("failed to publish sync request: %w", err)
	}

	// Also load existing state from Redis
	go r.loadExistingState(ctx, roomName)

	return nil
}

// Close closes the message bus
func (r *RedisMessageBus) Close() error {
	r.cancel()

	r.subsMutex.Lock()
	defer r.subsMutex.Unlock()

	for _, sub := range r.subscriptions {
		sub.cancel()
		sub.pubsub.Close()
	}
	r.subscriptions = make(map[livekit.RoomName]*subscription)

	return r.client.Close()
}

// processMessages processes incoming messages for a subscription
func (r *RedisMessageBus) processMessages(ctx context.Context, sub *subscription) {
	defer sub.pubsub.Close()

	ch := sub.pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg == nil {
				continue
			}

			var update StateUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
				r.logger.Warnw("failed to unmarshal state update", err, "payload", msg.Payload)
				continue
			}

			// Skip updates from ourselves
			if update.NodeID == r.nodeID {
				continue
			}

			r.handleStateUpdate(sub, &update)
		}
	}
}

// handleStateUpdate processes a state update message
func (r *RedisMessageBus) handleStateUpdate(sub *subscription, update *StateUpdate) {
	switch update.Type {
	case "participant_update":
		if update.Participant != nil {
			sub.handler.OnParticipantUpdate(update.NodeID, update.Participant)
		}
	case "participant_left":
		if update.ParticipantID != "" {
			sub.handler.OnParticipantLeft(update.NodeID, livekit.ParticipantID(update.ParticipantID))
		}
	case "state_sync_request":
		sub.handler.OnStateSyncRequest(update.NodeID)
	default:
		r.logger.Warnw("unknown state update type", nil, "updateType", update.Type)
	}
}

// loadExistingState loads existing participant state from Redis
func (r *RedisMessageBus) loadExistingState(ctx context.Context, roomName livekit.RoomName) {
	pattern := meshStatePrefix + string(roomName) + ":*"
	keys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil {
		r.logger.Warnw("failed to get existing state keys", err, "pattern", pattern)
		return
	}

	for _, key := range keys {
		// Skip our own state
		if contains(key, ":"+r.nodeID+":") {
			continue
		}

		data, err := r.client.Get(ctx, key).Bytes()
		if err != nil {
			r.logger.Warnw("failed to get state", err, "key", key)
			continue
		}

		var participant livekit.ParticipantInfo
		if err := proto.Unmarshal(data, &participant); err != nil {
			r.logger.Warnw("failed to unmarshal participant", err, "key", key)
			continue
		}

		// Extract node ID from key
		// Key format: "livekit:state:{room}:{nodeID}:{participantID}"
		parts := splitKey(key)
		if len(parts) >= 4 {
			nodeID := parts[3]

			r.subsMutex.RLock()
			if sub, exists := r.subscriptions[roomName]; exists {
				sub.handler.OnParticipantUpdate(nodeID, &participant)
			}
			r.subsMutex.RUnlock()
		}
	}
}

// Helper functions
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		(len(s) > len(substr) && s[:len(substr)] == substr) ||
		(len(s) > len(substr) && contains(s[1:], substr))
}

func splitKey(key string) []string {
	result := []string{}
	current := ""

	for _, c := range key {
		if c == ':' {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}

	return result
}
