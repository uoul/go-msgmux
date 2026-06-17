package msgmux

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// --------------------------------------------------------------------------------------------------
// Types
// --------------------------------------------------------------------------------------------------

type IJsonReadWriter interface {
	ReadJSON(v any) error
	WriteJSON(v any) error
}

type Responder[I any, O any] func(ctx context.Context, req Request[I]) (O, error)
type Listener[I any] func(ctx context.Context, req Message[I]) error

type MessageRouter struct {
	ctx    context.Context
	cancel context.CancelFunc
	iface  IJsonReadWriter

	sem chan struct{}

	pendingMux sync.RWMutex
	pending    map[MessageId]chan Response[json.RawMessage]

	responderMux sync.RWMutex
	responder    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) (any, error)

	listenerMux sync.RWMutex
	listener    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) error

	onUnknownMsgType func(ctx context.Context, msg Message[json.RawMessage])
	onListenerError  func(ctx context.Context, msg Message[json.RawMessage], err error)
}

// --------------------------------------------------------------------------------------------------
// Public
// --------------------------------------------------------------------------------------------------

func SendRequest[I any, O any](ctx context.Context, w *MessageRouter, req Request[I]) (Response[O], error) {
	// Create MsgId if not exists
	if len(req.MsgId) <= 0 {
		req.MsgId = MessageId(uuid.NewString())
	}
	// Create chanel for response
	respCh := make(chan Response[json.RawMessage], 1)
	// Check if api not closed already
	w.pendingMux.Lock()
	if err := w.ctx.Err(); err != nil {
		w.pendingMux.Unlock()
		return Response[O]{}, fmt.Errorf("device api closed: %w", err)
	}
	// Register pending request
	w.pending[MessageId(req.MsgId)] = respCh
	w.pendingMux.Unlock()
	// Ensure we cleanup
	defer func() {
		w.pendingMux.Lock()
		delete(w.pending, MessageId(req.MsgId))
		w.pendingMux.Unlock()
	}()
	// Send Request
	if err := w.iface.WriteJSON(req); err != nil {
		return Response[O]{}, fmt.Errorf("write request: %w", err)
	}
	// Wait for response or deadline exeeded
	select {
	case raw, ok := <-respCh:
		if !ok {
			// ReadLoop closed the channel — connection lost.
			return Response[O]{}, fmt.Errorf("connection closed while waiting for response")
		}
		// Parse body
		var body O
		if err := json.Unmarshal(raw.Body, &body); err != nil {
			return Response[O]{}, fmt.Errorf("unmarshal response body: %w", err)
		}
		return Response[O]{
			MsgId: raw.MsgId,
			Type:  raw.Type,
			Error: raw.Error,
			Body:  body,
		}, nil

	case <-ctx.Done():
		return Response[O]{}, ctx.Err()

	case <-w.ctx.Done():
		return Response[O]{}, fmt.Errorf("device api closed: %w", w.ctx.Err())
	}
}

func SendMessage[I any](ctx context.Context, w *MessageRouter, msg Message[I]) error {
	// Ensure message id
	if len(msg.MsgId) <= 0 {
		msg.MsgId = MessageId(uuid.NewString())
	}
	// Send Message
	if err := w.iface.WriteJSON(msg); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

func (mr *MessageRouter) Context() context.Context {
	return mr.ctx
}

// --------------------------------------------------------------------------------------------------
// Private
// --------------------------------------------------------------------------------------------------

func (w *MessageRouter) readLoop() {
	defer func() {
		w.pendingMux.Lock()
		for id, ch := range w.pending {
			close(ch)
			delete(w.pending, id)
		}
		w.pendingMux.Unlock()
	}()
	// Read from connection
	for {
		// Read incomming data
		var msg Message[json.RawMessage]
		if err := w.iface.ReadJSON(&msg); err != nil {
			w.cancel()
			return
		}
		// Check if message belongs to open request
		w.pendingMux.Lock()
		ch, isPending := w.pending[msg.MsgId]
		if isPending {
			delete(w.pending, msg.MsgId)
		}
		w.pendingMux.Unlock()
		// Get Responder
		w.responderMux.RLock()
		responder, hasResponder := w.responder[msg.Type]
		w.responderMux.RUnlock()
		// Get Listener
		w.listenerMux.RLock()
		listener, hasListener := w.listener[msg.Type]
		w.listenerMux.RUnlock()

		// Process message
		switch {
		// Response to ongoing request
		case isPending:
			ch <- Response[json.RawMessage]{
				MsgId: msg.MsgId,
				Type:  msg.Type,
				Error: msg.Error,
				Body:  msg.Body,
			}
		// Request from Device
		case hasResponder:
			// Aquire slot of sem to limit rate
			select {
			case w.sem <- struct{}{}:
			case <-w.ctx.Done():
				return
			}
			// Handle Request asynchronously
			go func() {
				// Dispose aquired slot
				defer func() { <-w.sem }()
				// Execute handler
				result, err := responder(w.ctx, msg)
				resp := Response[any]{
					MsgId: msg.MsgId,
					Type:  msg.Type,
				}
				if err != nil {
					errStr := err.Error()
					resp.Error = &errStr
				} else {
					resp.Body = result
				}
				// Best-effort response write; ignore error.
				_ = w.iface.WriteJSON(resp)
			}()

		case hasListener:
			// Aquire slot of sem to limit rate
			select {
			case w.sem <- struct{}{}:
			case <-w.ctx.Done():
				return
			}
			// Handle Request
			go func() {
				// Dispose aquired slot
				defer func() { <-w.sem }()
				// Call listener
				if err := listener(w.ctx, msg); err != nil && w.onListenerError != nil {
					w.onListenerError(w.ctx, msg, err)
				}
			}()

		default:
			if w.onUnknownMsgType != nil {
				// Aquire slot of sem to limit rate
				select {
				case w.sem <- struct{}{}:
				case <-w.ctx.Done():
					return
				}
				go func() {
					defer func() { <-w.sem }()
					w.onUnknownMsgType(w.ctx, msg)
				}()
			}
		}
	}
}

// --------------------------------------------------------------------------------------------------
// Constructor
// --------------------------------------------------------------------------------------------------

func NewMessageRouter(ctx context.Context, iface IJsonReadWriter, opts ...func(*MessageRouter)) *MessageRouter {
	wsCtx, cancel := context.WithCancel(ctx)
	m := &MessageRouter{
		ctx:    wsCtx,
		cancel: cancel,
		iface:  iface,

		sem: make(chan struct{}, 8),

		pendingMux: sync.RWMutex{},
		pending:    map[MessageId]chan Response[json.RawMessage]{},

		responderMux: sync.RWMutex{},
		responder:    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) (any, error){},

		listenerMux: sync.RWMutex{},
		listener:    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) error{},

		onUnknownMsgType: nil,
		onListenerError:  nil,
	}
	for _, o := range opts {
		o(m)
	}
	go m.readLoop()
	return m
}

// --------------------------------------------------------------------------------------------------
// Options
// --------------------------------------------------------------------------------------------------

func WithMaxParallelization(p int) func(*MessageRouter) {
	return func(da *MessageRouter) {
		da.sem = make(chan struct{}, p)
	}
}

func WithListenerErrorHandler(fn func(ctx context.Context, msg Message[json.RawMessage], err error)) func(*MessageRouter) {
	return func(wm *MessageRouter) {
		wm.onListenerError = fn
	}
}

func WithUnknownMsgTypeHandler(fn func(ctx context.Context, msg Message[json.RawMessage])) func(*MessageRouter) {
	return func(wm *MessageRouter) {
		wm.onUnknownMsgType = fn
	}
}

func WithResponder[I any, O any](msgType MessageType, handler Responder[I, O]) func(*MessageRouter) {
	return func(wm *MessageRouter) {
		wm.responderMux.Lock()
		defer wm.responderMux.Unlock()
		wm.responder[msgType] = func(ctx context.Context, msg Message[json.RawMessage]) (any, error) {
			var body I
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return nil, fmt.Errorf("unmarshal request body: %w", err)
			}
			return handler(ctx, Request[I]{
				MsgId: msg.MsgId,
				Type:  msg.Type,
				Body:  body,
			})
		}
	}
}

func WithListener[I any](msgType MessageType, handler Listener[I]) func(*MessageRouter) {
	return func(wm *MessageRouter) {
		wm.listenerMux.Lock()
		defer wm.listenerMux.Unlock()
		wm.listener[msgType] = func(ctx context.Context, msg Message[json.RawMessage]) error {
			var body I
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return fmt.Errorf("unmarshal message body: %w", err)
			}
			return handler(ctx, Message[I]{
				MsgId: msg.MsgId,
				Type:  msg.Type,
				Body:  body,
			})
		}
	}
}
