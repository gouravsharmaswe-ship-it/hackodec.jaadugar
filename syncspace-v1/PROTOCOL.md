# SyncSpace message protocol

All messages are JSON text frames over one WebSocket. Every message has a type field `t`.
Room ids: 1-32 chars of `A-Z a-z 0-9 _ -`. The server is the source of truth for a room's shared state.

## Client -> server
| Message | Fields | Meaning |
|---|---|---|
| `join` | `room`, `name`, `clientId` | First message. `clientId` stays the same across reconnects. |
| `set` | `key`, `value` | Set a shared key (omit `value` to delete it). Key max 64 chars, value max 10,000 chars as JSON. |
| `cursor` | `x`, `y` | Cursor position (the demo sends 0-1 fractions of the window). |
| `ping` | | Keep-alive, server answers `pong`. |

## Server -> client
| Message | Fields | Meaning |
|---|---|---|
| `welcome` | `id`, `room`, `version`, `state`, `peers` | Reply to `join`: the full latest state and who is online. Sent again after every reconnect. |
| `state` | `key`, `value`, `by`, `v` | Someone changed a key. |
| `peer-join` / `peer-leave` | `peer` / `id` | Presence changes. |
| `cursor` | `id`, `x`, `y` | Another peer's cursor. |
| `error` | `code`, `message` | `bad_json`, `bad_message`, `bad_room`, `not_joined`, `already_joined`, `room_full`, `bad_key`, `too_big`, `too_many_keys`, `rate_limited`, `unknown_type`. |

## Limits
25 peers/room, 100 keys/room, 30 `set`/s and 60 `cursor`/s per peer, 16 KB per frame. Empty rooms are deleted after 10 minutes.
Conflict rule: last write wins (server order).
