# msgmux

A lightweight, generic Go library for bidirectional JSON message routing over any read/write interface (e.g. WebSockets, pipes, TCP connections).

`msgmux` supports three messaging patterns:

- **Request/Response** — send a typed request and await a typed response
- **Fire and Forget** — send a typed message without response
- **Listener** — fire-and-forget messages handled by a registered handler
- **Responder** — handle incoming requests and automatically send back a typed response

## Features

- ✅ Generic, type-safe message and response bodies
- ✅ Concurrent message handling with configurable parallelization
- ✅ Automatic message ID generation via UUID
- ✅ Graceful shutdown with `context` propagation
- ✅ Pluggable error hooks for listeners, responders, and unknown message types
- ✅ Works with any `IJsonReadWriter` implementation (WebSocket, net.Conn, etc.)

## Installation

```bash
go get github.com/uoul/go-msgmux
```

## Core Types

### `Message[T]`

The base envelope for all messages.

```go
type Message[T any] struct {
    MsgId MessageId   // Unique message identifier
    Type  MessageType // Logical message type/topic
    Error *string     // Optional error string (used in responses)
    Body  T           // Typed payload
}
```

### `Request[T]` / `Response[T]`

Typed wrappers for the request/response pattern, sharing the same envelope structure as `Message[T]`.

### `IJsonReadWriter`

The interface your transport must implement:

```go
type IJsonReadWriter interface {
    ReadJSON(v any) error
    WriteJSON(v any) error
}
```

## Usage

### 1. Create a `MessageRouter`

```go
router := msgmux.NewMessageRouter(ctx, myJsonReadWriter,
    msgmux.WithMaxParallelization(16),
    msgmux.WithListenerErrorHandler(func(ctx context.Context, msg msgmux.Message[json.RawMessage], err error) {
        log.Printf("listener error: %v", err)
    }),
    msgmux.WithUnknownMsgTypeHandler(func(ctx context.Context, msg msgmux.Message[json.RawMessage]) {
        log.Printf("unknown message type: %s", msg.Type)
    }),
    msgmux.WithResponder[RequestBody, ResponseBody]("my.request.type", func(ctx context.Context, req msgmux.Request[RequestBody]) (ResponseBody, error) {
        // handle request
        return ResponseBody{Result: "ok"}, nil
    }),
    msgmux.WithListener[EventBody]("my.event.type", func(ctx context.Context, msg msgmux.Message[EventBody]) error {
        // handle event
        return nil
    }),
)
defer router.Close()
```

### 4. Send a Request (and await a response)

```go
resp, err := msgmux.SendRequest[RequestBody, ResponseBody](ctx, router, msgmux.Request[RequestBody]{
    Type: "my.request.type",
    Body: RequestBody{Foo: "bar"},
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(resp.Body)
```

### 5. Send a Fire-and-Forget Message

```go
err := msgmux.SendMessage[EventBody](ctx, router, msgmux.Message[EventBody]{
    Type: "my.event.type",
    Body: EventBody{Value: 42},
})
```

## Options

| Option                                  | Description                                                     |
| --------------------------------------- | --------------------------------------------------------------- |
| `WithMaxParallelization(n int)`         | Max number of concurrent goroutines for handlers (default: `8`) |
| `WithResponder[I, O](msgType, handler)` | Register a request handler that sends a typed response          |
| `WithListener[I](msgType, handler)`     | Register a fire-and-forget message handler                      |
| `WithListenerErrorHandler(fn)`          | Called when a listener returns an error                         |
| `WithUnknownMsgTypeHandler(fn)`         | Called when a message type has no registered handler            |
| `WithResponseWriteErrorHandler(fn)`     | Called when writing a response back to the transport fails      |

## Lifecycle

- The router starts its internal read loop immediately upon creation via `NewMessageRouter`.
- Call `router.Close()` to cancel the context, stop the read loop, and wait for all in-flight handlers to finish.
- If the underlying transport closes (i.e. `ReadJSON` returns an error), the router cancels its context and closes all pending request channels gracefully.

## Concurrency Model

- All incoming messages are dispatched to goroutines via a semaphore-bounded pool.
- The default parallelization limit is **8** concurrent handlers, configurable via `WithMaxParallelization`.
- Writes are serialized via a mutex to prevent interleaved JSON output.


Made with ❤️ for the Go community
