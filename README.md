
# msgmux — Type-Safe WebSocket Message Multiplexer

A Go package for multiplexing JSON-based request/response and one-way message patterns over a single JSON connection (e.g., WebSocket). It provides type-safe, generic message routing with concurrent request handling, automatic correlation, and configurable concurrency limits.

## Overview

`msgmux` manages a bidirectional JSON connection and dispatches incoming messages based on their type and correlation ID. It supports three communication patterns:

| Pattern | Description |
|---|---|
| **Request → Response** | Send a request and await a correlated response (`SendRequest`) |
| **Incoming Request** | Register a responder to handle requests from the remote end and automatically reply |
| **One-way Message** | Send or listen for fire-and-forget messages (`SendMessage` / `WithListener`) |

## Core Types

### `Message[T]`

The fundamental envelope for all communication:

```go
type MessageId   string
type MessageType string

type Message[T any] struct {
    MsgId MessageId
    Type  MessageType
    Error *string
    Body  T
}
```

- **`MsgId`** — Unique identifier used to correlate requests with responses. Auto-generated (UUID) if omitted.
- **`Type`** — Distinguishes message kinds; used for routing to the correct handler.
- **`Error`** — Non-nil when the message represents an error.
- **`Body`** — Generic payload, type-parameterized for type safety.

### `Request[I]`

Extends `Message[I]` — used for request/response exchanges. Sent via `SendRequest`, handled by `WithResponder`.

### `Response[O]`

Extends `Message[O]` — the reply side of a request/response exchange. Returned by `SendRequest` and auto-sent by responders.

### `MessageRouter`

The central multiplexer that owns the read loop, tracks pending requests, and dispatches incoming messages.

## Quick Start

### 1. Create a Router

```go
router := msgmux.NewMessageRouter(ctx, conn,
    msgmux.WithMaxParallelization(16),
    msgmux.WithResponder("get_status", getStatusHandler),
    msgmux.WithListener("notification", notificationHandler),
    msgmux.WithUnknownMsgTypeHandler(func(ctx context.Context, msg msgmux.Message[json.RawMessage]) {
        log.Printf("unknown message type: %s", msg.Type)
    }),
    msgmux.WithListenerErrorHandler(func(ctx context.Context, msg msgmux.Message[json.RawMessage], err error) {
        log.Printf("listener error for %s: %v", msg.Type, err)
    }),
)
```

- `conn` must implement `IJsonReadWriter` (e.g., a `*gorilla/websocket.Conn` wrapper).
- The read loop starts automatically in a background goroutine.

### 2. Send a Request (with response)

```go
type PingBody struct{}
type PongBody struct{ ServerTime int64 }

resp, err := msgmux.SendRequest[PingBody, PongBody](ctx, router, msgmux.Request[PingBody]{
    Type: "ping",
    Body: PingBody{},
})
if err != nil {
    // handle error (timeout, connection closed, unmarshal failure, etc.)
}
fmt.Println("Server time:", resp.Body.ServerTime)
```

- Blocks until a response with a matching `MsgId` arrives, or the context/router is cancelled.

### 3. Send a One-way Message

```go
err := msgmux.SendMessage(ctx, router, msgmux.Message[MyPayload]{
    Type: "log",
    Body: MyPayload{Level: "info", Text: "hello"},
})
```

### 4. Handle Incoming Requests (Responder)

```go
func getStatusHandler(ctx context.Context, req msgmux.Request[GetStatusReq]) (GetStatusResp, error) {
    return GetStatusResp{Online: true}, nil
}

// Register via option:
msgmux.WithResponder[GetStatusReq, GetStatusResp]("get_status", getStatusHandler)
```

- When a message of type `"get_status"` arrives, the body is unmarshaled into `GetStatusReq`, the handler runs, and a `Response[GetStatusResp]` is automatically written back with the same `MsgId`.

### 5. Listen for One-way Messages

```go
func notificationHandler(ctx context.Context, msg msgmux.Message[Notification]) error {
    fmt.Println("Notification:", msg.Body.Text)
    return nil
}

// Register via option:
msgmux.WithListener[Notification]("notification", notificationHandler)
```

## Configuration Options

| Option | Signature | Description |
|---|---|---|
| `WithMaxParallelization` | `(p int) func(*MessageRouter)` | Sets the max number of concurrent handler goroutines (default: **8**). Uses a semaphore channel internally. |
| `WithResponder[I, O]` | `(msgType MessageType, handler Responder[I, O]) func(*MessageRouter)` | Registers a typed responder for incoming requests. Auto-unmarshals body into `I`, calls handler, writes response with body type `O`. |
| `WithListener[I]` | `(msgType MessageType, handler Listener[I]) func(*MessageRouter)` | Registers a typed listener for one-way messages. Auto-unmarshals body into `I`. |
| `WithUnknownMsgTypeHandler` | `(fn func(...)) func(*MessageRouter)` | Callback for messages that match no pending request, responder, or listener. |
| `WithListenerErrorHandler` | `(fn func(...)) func(*MessageRouter)` | Callback when a listener returns an error. |
| `Context` | `(mr *MessageRouter) Context() context.Context` | Returns the router's lifecycle context. Cancelled when the connection ends or the parent context is cancelled. Use <-Context().Done() to wait for disconnection. |

## `IJsonReadWriter` Interface

Any connection type providing JSON read/write can be used:

```go
type IJsonReadWriter interface {
    ReadJSON(v any) error
    WriteJSON(v any) error
}
```

Compatible with [gorilla/websocket](https://github.com/gorilla/websocket) — just wrap `*websocket.Conn` (it already implements these methods).

## Lifecycle & Cancellation

- `NewMessageRouter` creates a derived context via `context.WithCancel`.
- When the **parent context** is cancelled, or the **read loop encounters an error**, the derived context is cancelled.
- On cancellation, all pending request channels are **closed**, causing every blocked `SendRequest` to return with a `"connection closed"` error.
- `SendRequest` also respects the caller's own context for per-request timeouts.

## Design Notes

- **Thread-safe**: All shared state (pending map, responder map, listener map) is protected by `sync.RWMutex`.
- **Concurrency-limited**: A buffered semaphore channel (`sem`) caps the number of concurrently running handlers to prevent resource exhaustion.
- **Generic & type-safe**: Go 1.18+ generics ensure that message bodies are typed at compile time — no runtime type assertions needed.
- **Best-effort response writes**: Responder errors are reported in the `Error` field of the response; write failures are silently ignored.

## Dependencies

- `github.com/google/uuid` — for auto-generating message IDs

## License
MIT
