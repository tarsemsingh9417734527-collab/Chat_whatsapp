# Chatly - Real-Time Chat Application (WhatsApp-like)

A fully working implementation of the **System Design: real-time chat application** assignment.
No dependencies and no database to install: just Node.js.

## Run it

```bash
node server.js          # or: npm start
```

Open **http://localhost:3000** in two different browsers (or one normal + one private window).
Create two accounts (first login with a new username creates the account), press **+**, pick the other user and chat.

To try it with a phone on the same Wi-Fi, open `http://<your-computer-IP>:3000` on the phone.

Requires Node.js 18 or newer. Run the automated tests with `node test.js` (needs Node 22+, which has a built-in WebSocket client).

## Features and where they are in the code

| Requirement from the assignment | How it works | Code |
|---|---|---|
| 1:1 messaging | Direct conversation per pair of users, messages pushed live over WebSocket | `server.js` `onSend` |
| Group messaging | Groups up to 256 members; one stored copy, fan-out to online members | `create_group`, `onSend` |
| WebSocket connection management | Own RFC 6455 implementation (handshake, framing, masking, ping/pong, close); token auth on connect; connection registry `user -> sockets` (multi-device); heartbeat every 25 s drops dead peers; client reconnects with exponential backoff + jitter | `WSConn`, `registry`, heartbeat `setInterval`, client `connect()` |
| Message delivery and read receipts | Sent (1 tick, server stored it), delivered (2 grey ticks, client ACK), read (2 blue ticks). Stored as cursors per user per conversation, so groups scale | `onCursor`, client `ticks()` |
| Offline message storage | Everything is persisted; offline users get a push notification (stub) and on reconnect the client sends its cursors and the server returns only the missed messages | `sync` handler, `pushNotify`, `db.json` |
| Message ordering | Server assigns a strictly increasing sequence number per conversation; clients sort by it; gaps are detected and repaired with `fetch` | `++c.seq`, client `addMsg` / `checkGap` |
| Idempotency / no duplicates | Client message id; a retry returns the same sequence number and is not delivered twice | `seen` set |
| Presence, typing | Online / last seen to conversation partners; typing indicator | `broadcastPresence`, `typing` |
| Security and abuse | scrypt password hashing, HMAC-signed tokens, input validation, token-bucket rate limit (20 burst, 2/s), login rate limit, max frame size | `login`, `onSend` |

## Message protocol (JSON over WebSocket)

Client to server: `sync`, `fetch`, `send`, `ack`, `read`, `typing`, `users`, `open_dm`, `create_group`
Server to client: `ready`, `sync`, `msg`, `sent`, `receipt`, `conv`, `presence`, `typing`, `users`, `error`

Flow of one message: `send {cid, id, text}` -> server stores with `seq` -> `sent {seq}` to sender (1 tick) -> `msg` to online recipients -> `ack` back (2 ticks) -> `read` when the chat is open (blue ticks).

## How this prototype maps to the production design in the PDF

| Production design | In this prototype |
|---|---|
| Many chat servers behind a WebSocket-aware load balancer | One Node process |
| Redis connection registry + pub/sub between servers | In-memory `Map` (`registry`) |
| Cassandra (messages) + PostgreSQL (users, groups) | One JSON file with atomic writes (`data/db.json`) |
| Kafka for fan-out and async jobs | Direct loop over group members |
| FCM / APNs push service | Console log (`[push] ...`) and browser Notification API |
| End-to-end encryption | Not implemented (TLS would be added by the load balancer) |

To scale to several servers you would replace `registry` with Redis, the JSON file with the databases above, and forward messages to the recipient's server through pub/sub. The protocol and the ordering/receipt logic stay the same.

## Project structure

```
server.js        HTTP + WebSocket server, all backend logic
public/index.html  Chat client (HTML, CSS, JavaScript in one file)
test.js          End-to-end tests (28 checks)
data/db.json     Created on first run (users, conversations, messages)
```
