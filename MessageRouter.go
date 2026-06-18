package msgmux

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

const (
	DefaultParallelization = 8
)

// --------------------------------------------------------------------------------------------------
// Types
// --------------------------------------------------------------------------------------------------

type IJsonReadWriter interface {
	ReadJSON(v any) error
	WriteJSON(v any) error
}

type Responder[I any, O any] func(ctx context.Context, body I) (O, error)
type Listener[I any] func(ctx context.Context, body I) error

type MessageRouter struct {
	ctx    context.Context
	cancel context.CancelFunc
	iface  IJsonReadWriter

	sem             chan struct{}
	wg              sync.WaitGroup
	writeMux        sync.Mutex
	parallelization int

	pendingMux sync.RWMutex
	pending    map[MessageId]chan Response[json.RawMessage]

	responderMux sync.RWMutex
	responder    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) (any, error)

	listenerMux sync.RWMutex
	listener    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) error

	onUnknownMsgType     func(ctx context.Context, msg Message[json.RawMessage])
	onListenerError      func(ctx context.Context, msg Message[json.RawMessage], err error)
	onResponseWriteError func(ctx context.Context, err error)
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
		return Response[O]{}, fmt.Errorf("already closed: %w", err)
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
	if err := w.writeMessage(req); err != nil {
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
		return Response[O]{}, fmt.Errorf("already closed: %w", w.ctx.Err())
	}
}

func SendMessage[I any](ctx context.Context, w *MessageRouter, msg Message[I]) error {
	// Ensure message id
	if len(msg.MsgId) <= 0 {
		msg.MsgId = MessageId(uuid.NewString())
	}
	return w.writeMessage(msg)
}

func (mr *MessageRouter) Context() context.Context {
	return mr.ctx
}

func (mr *MessageRouter) Close() error {
	mr.cancel()
	mr.wg.Wait()
	return nil
}

// --------------------------------------------------------------------------------------------------
// Private
// --------------------------------------------------------------------------------------------------

func (w *MessageRouter) writeMessage(resp any) error {
	w.writeMux.Lock()
	defer w.writeMux.Unlock()
	if err := w.ctx.Err(); err != nil {
		return fmt.Errorf("already closed: %w", err)
	}
	return w.iface.WriteJSON(resp)
}

func (w *MessageRouter) dispatch(fn func()) {
	// Limit number of spawned go routines
	select {
	case w.sem <- struct{}{}:
	case <-w.ctx.Done():
		return
	}
	// Execute handler non blocking
	w.wg.Go(func() {
		defer func() { <-w.sem }()
		fn()
	})
}

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
			w.dispatch(func() {
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
				if err := w.writeMessage(resp); err != nil && w.onResponseWriteError != nil {
					w.onResponseWriteError(w.ctx, err)
				}
			})

		case hasListener:
			w.dispatch(func() {
				if err := listener(w.ctx, msg); err != nil && w.onListenerError != nil {
					w.onListenerError(w.ctx, msg, err)
				}
			})

		default:
			if w.onUnknownMsgType != nil {
				w.dispatch(func() {
					w.onUnknownMsgType(w.ctx, msg)
				})
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

		wg:              sync.WaitGroup{},
		writeMux:        sync.Mutex{},
		parallelization: DefaultParallelization,

		pendingMux: sync.RWMutex{},
		pending:    map[MessageId]chan Response[json.RawMessage]{},

		responderMux: sync.RWMutex{},
		responder:    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) (any, error){},

		listenerMux: sync.RWMutex{},
		listener:    map[MessageType]func(ctx context.Context, msg Message[json.RawMessage]) error{},

		onUnknownMsgType:     nil,
		onListenerError:      nil,
		onResponseWriteError: nil,
	}
	for _, o := range opts {
		o(m)
	}
	m.sem = make(chan struct{}, m.parallelization)
	go m.readLoop()
	return m
}

// --------------------------------------------------------------------------------------------------
// Options
// --------------------------------------------------------------------------------------------------

func WithMaxParallelization(p int) func(*MessageRouter) {
	return func(da *MessageRouter) {
		da.parallelization = p
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

func WithResponseWriteErrorHandler(fn func(ctx context.Context, err error)) func(*MessageRouter) {
	return func(wm *MessageRouter) {
		wm.onResponseWriteError = fn
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
			return handler(ctx, body)
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
			return handler(ctx, body)
		}
	}
}
