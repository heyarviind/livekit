# LiveKit Mesh Networking Setup Guide

This guide explains how to set up and use LiveKit's mesh networking feature, which allows participants connected to different servers to communicate in the same room.

## Overview

The mesh system implements a **multi-home architecture** where:

- Sessions span multiple servers instead of being tied to a single server
- Participants are automatically synchronized across nodes via a message bus
- Media can be relayed between SFUs using a custom protocol
- Fault tolerance and live migrations are supported

## Architecture Components

### 1. Mesh Session Manager

- Manages both local and remote participants in a room
- Handles state synchronization across nodes
- Coordinates media relay requests

### 2. Message Bus (Redis)

- Uses Redis pub/sub for real-time state synchronization
- Stores participant state for new nodes joining
- Handles state sync requests and responses

### 3. Media Relay Protocol

- Custom protocol for relaying media between SFU nodes
- Encrypted media packet forwarding
- Bandwidth-aware relay routing

### 4. Node Discovery

- Tracks which nodes are hosting each room
- Health monitoring and automatic failover
- Latency-based routing decisions

## Setup Instructions

### 1. Prerequisites

- Redis server (for state synchronization)
- Multiple LiveKit server instances
- Network connectivity between servers

### 2. Configuration

Create a mesh configuration file for each node (see `config-mesh-sample.yaml`):

```yaml
mesh:
  enabled: true
  node_id: "node-1" # Must be unique per node
  node_region: "us-west"

  message_bus:
    type: "redis"
    address: "your-redis-server:6379"
    db: 1

  media_relay:
    listen_port: 7882
    max_relay_bitrate: 10000000
    encryption_key: "shared-encryption-key"

  discovery:
    heartbeat_interval: "30s"
    node_timeout: "90s"
    latency_threshold: "100ms"
```

### 3. Start Multiple Nodes

Start each LiveKit server with its respective configuration:

```bash
# Node 1 (us-west)
./livekit-server --config config-node1.yaml

# Node 2 (us-east)
./livekit-server --config config-node2.yaml

# Node 3 (eu-west)
./livekit-server --config config-node3.yaml
```

### 4. Create Rooms with Mesh Support

When creating rooms programmatically, the mesh system will automatically:

- Detect other nodes hosting the same room
- Synchronize participant states
- Enable cross-node communication

## How It Works

### Participant Join Flow

1. **Local Join**: Participant connects to nearest node (Node A)
2. **State Broadcast**: Node A broadcasts participant info to mesh
3. **Remote Sync**: Other nodes (Node B, C) receive participant update
4. **Cross-Node Visibility**: All participants see each other regardless of node

### Media Relay Flow

1. **Subscription Request**: Participant on Node A wants to subscribe to participant on Node B
2. **Relay Setup**: Node A requests media tracks from Node B
3. **Media Forward**: Node B forwards media packets to Node A
4. **Local Delivery**: Node A delivers media to local participant

### State Synchronization

- **Real-time Updates**: Participant state changes are broadcast immediately
- **Eventual Consistency**: All nodes converge to the same state
- **Conflict Resolution**: Latest timestamp wins for conflicting updates

## Configuration Options

### Mesh Config

```yaml
mesh:
  enabled: true # Enable/disable mesh networking
  node_id: "unique-id" # Unique identifier for this node
  node_region: "region" # Geographic region
```

### Message Bus Config

```yaml
message_bus:
  type: "redis" # Currently only Redis supported
  address: "host:port" # Redis connection string
  password: "password" # Redis auth (optional)
  db: 1 # Redis database number
```

### Media Relay Config

```yaml
media_relay:
  listen_port: 7882 # Port for relay connections
  max_relay_bitrate: 10000000 # Max bitrate per relay (bytes/sec)
  relay_buffer_size: 500 # Packet buffer size
  encryption_key: "key" # Shared encryption key
```

### Discovery Config

```yaml
discovery:
  heartbeat_interval: "30s" # Node presence announcement frequency
  node_timeout: "90s" # Time before marking node as dead
  latency_threshold: "100ms" # Max acceptable relay latency
```

## Monitoring and Debugging

### Logs

Monitor mesh activity in the logs:

```
INFO mesh session initialized room=myroom nodeID=node-1
INFO remote participant added participantID=abc123 nodeID=node-2
```

### Health Checks

- Monitor Redis connectivity
- Check node discovery heartbeats
- Verify media relay connections

### Metrics

- Cross-node participant count
- Media relay bandwidth usage
- State synchronization latency

## Troubleshooting

### Common Issues

**Participants not seeing each other across nodes:**

- Check Redis connectivity and configuration
- Verify mesh is enabled on all nodes
- Ensure node_id is unique per node

**Media not flowing between nodes:**

- Check media relay port accessibility
- Verify encryption keys match
- Monitor relay bandwidth limits

**High latency or poor quality:**

- Check network connectivity between nodes
- Adjust latency thresholds
- Consider adding nodes closer to users

### Debug Commands

```bash
# Check Redis mesh state
redis-cli -n 1 KEYS "livekit:mesh:*"

# Monitor mesh messages
redis-cli -n 1 MONITOR

# View participant state
redis-cli -n 1 KEYS "livekit:state:*"
```

## Limitations

- Media relay introduces additional latency
- Requires shared Redis infrastructure
- Currently limited to Redis message bus
- Beta feature - not recommended for production without testing

## Next Steps

1. **Test Setup**: Start with 2 nodes in the same region
2. **Monitor Performance**: Track latency and bandwidth usage
3. **Scale Gradually**: Add more nodes and regions as needed
4. **Optimize Configuration**: Tune based on your specific use case

For questions or issues, please refer to the LiveKit documentation or open an issue on GitHub.
