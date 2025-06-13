# Shared Secret Authentication

This guide shows how to configure and use shared secret authentication with LiveKit instead of multiple API key pairs.

## Configuration

### LiveKit Server Configuration

Update your `config.yaml` to use a shared secret:

```yaml
# Authentication configuration
auth:
  shared_secret: "your-shared-secret-at-least-32-characters-long"
# Comment out or remove the traditional keys section
# keys:
#   key1: secret1
#   key2: secret2
```

### Environment Variable

You can also set the shared secret via environment variable:

```bash
export LIVEKIT_AUTH_SHARED_SECRET="your-shared-secret-at-least-32-characters-long"
```

## Backend Integration

### Go Backend Example

```go
package main

import (
    "time"
    "github.com/livekit/protocol/auth"
)

const SHARED_SECRET = "your-shared-secret-at-least-32-characters-long"

// Create a room access token
func createRoomToken(userID, roomName string, canPublish bool) (string, error) {
    grant := &auth.VideoGrant{
        RoomJoin:     true,
        Room:         roomName,
        CanPublish:   &canPublish,
        CanSubscribe: true,
    }

    // API key can be any string since we use shared secret
    token := auth.NewAccessToken("backend", SHARED_SECRET).
        AddGrant(grant).
        SetIdentity(userID).
        SetValidFor(24 * time.Hour)

    return token.ToJWT()
}

// Create an admin token for server API calls
func createAdminToken() (string, error) {
    grant := &auth.VideoGrant{
        RoomAdmin:   true,
        RoomList:    true,
        RoomCreate:  true,
        RoomRecord:  true,
    }

    token := auth.NewAccessToken("admin", SHARED_SECRET).
        AddGrant(grant).
        SetIdentity("admin").
        SetValidFor(time.Hour)

    return token.ToJWT()
}

// Create a token with custom permissions
func createCustomToken(userID string, permissions map[string]interface{}) (string, error) {
    grant := &auth.VideoGrant{}

    // Set permissions based on your requirements
    if roomJoin, ok := permissions["room_join"].(bool); ok {
        grant.RoomJoin = roomJoin
    }
    if room, ok := permissions["room"].(string); ok {
        grant.Room = room
    }
    if canPublish, ok := permissions["can_publish"].(bool); ok {
        grant.SetCanPublish(canPublish)
    }
    if canSubscribe, ok := permissions["can_subscribe"].(bool); ok {
        grant.SetCanSubscribe(canSubscribe)
    }

    token := auth.NewAccessToken("backend", SHARED_SECRET).
        AddGrant(grant).
        SetIdentity(userID).
        SetValidFor(24 * time.Hour)

    return token.ToJWT()
}
```

### Node.js Backend Example

```javascript
const { AccessToken } = require("livekit-server-sdk");

const SHARED_SECRET = "your-shared-secret-at-least-32-characters-long";

// Create a room access token
function createRoomToken(userID, roomName, canPublish = true) {
  const token = new AccessToken("backend", SHARED_SECRET, {
    identity: userID,
    ttl: "24h",
  });

  token.addGrant({
    room: roomName,
    roomJoin: true,
    canPublish: canPublish,
    canSubscribe: true,
  });

  return token.toJwt();
}

// Create an admin token
function createAdminToken() {
  const token = new AccessToken("admin", SHARED_SECRET, {
    identity: "admin",
    ttl: "1h",
  });

  token.addGrant({
    roomAdmin: true,
    roomList: true,
    roomCreate: true,
    roomRecord: true,
  });

  return token.toJwt();
}
```

### Python Backend Example

```python
from livekit import api

SHARED_SECRET = "your-shared-secret-at-least-32-characters-long"

def create_room_token(user_id: str, room_name: str, can_publish: bool = True) -> str:
    token = api.AccessToken("backend", SHARED_SECRET)
    token.with_identity(user_id)
    token.with_ttl(timedelta(hours=24))
    token.with_grants(api.VideoGrants(
        room_join=True,
        room=room_name,
        can_publish=can_publish,
        can_subscribe=True
    ))
    return token.to_jwt()

def create_admin_token() -> str:
    token = api.AccessToken("admin", SHARED_SECRET)
    token.with_identity("admin")
    token.with_ttl(timedelta(hours=1))
    token.with_grants(api.VideoGrants(
        room_admin=True,
        room_list=True,
        room_create=True,
        room_record=True
    ))
    return token.to_jwt()
```

## Benefits

1. **Simplified Key Management**: Only one secret to manage instead of multiple key-value pairs
2. **Backend Control**: Your backend controls all authentication and permissions
3. **Dynamic Permissions**: Easy to implement per-user or per-room permissions
4. **Backward Compatible**: Can still use traditional multi-key approach if needed
5. **Security**: Single point of secret management reduces attack surface

## Security Considerations

1. **Secret Length**: Use at least 32 characters for the shared secret
2. **Secret Storage**: Store the secret securely (environment variables, secret management services)
3. **Network Security**: Ensure LiveKit is not publicly accessible - place behind your authenticated API
4. **Token Validation**: Your backend should validate all requests before calling LiveKit APIs
5. **Secret Rotation**: Plan for secret rotation by updating both backend and LiveKit configuration

## Migration from Multi-Key

To migrate from the traditional multi-key approach:

1. Add the `auth.shared_secret` configuration to your LiveKit config
2. Update your backend to use the shared secret for JWT signing
3. Test thoroughly with the new configuration
4. Once verified, comment out or remove the old `keys` section
5. Restart LiveKit server

The system supports both approaches simultaneously during migration.
