package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
)

// HTTPHandle is returned by RunHTTP so callers can shut down the
// server programmatically (tests, embedded scenarios) and discover
// the actual listening port (handy when port=0).
type HTTPHandle struct {
	server      *http.Server
	listener    net.Listener
	subscribers *subscriberSet
}

// Addr returns the actual TCP address the server is listening on.
func (h *HTTPHandle) Addr() string { return h.listener.Addr().String() }

// Close shuts down the HTTP server and disconnects every SSE subscriber.
func (h *HTTPHandle) Close(ctx context.Context) error {
	h.subscribers.closeAll()
	return h.server.Shutdown(ctx)
}

// RunHTTP drives `p` over the streamable-HTTP contract on host:port.
//
//   - POST / decodes a JSON-RPC request from the body, runs Dispatch,
//     returns the response (or stream ack) in the body.
//   - GET  / returns a long-lived Server-Sent-Events stream the
//     runtime pushes server notifications onto
//     ($/stream/data / $/stream/end).
//
// Same JSON-RPC semantics as Run — both go through Dispatch.
// Returns once the listener is bound; callers control shutdown via
// the returned HTTPHandle.
func RunHTTP(ctx context.Context, p BowirePlugin, host string, port int) (*HTTPHandle, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	subs := newSubscriberSet()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleSubscribe(w, r, subs)
		case http.MethodPost:
			handlePost(ctx, w, r, p, subs)
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()

	return &HTTPHandle{server: server, listener: listener, subscribers: subs}, nil
}

func handleSubscribe(w http.ResponseWriter, r *http.Request, subs *subscriberSet) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported by this responder", http.StatusInternalServerError)
		return
	}

	// Register before headers go out — once the client's Do() returns
	// it may immediately POST to start a stream, and the broadcaster
	// must already see this subscriber when the pump fires.
	sub := &subscriber{
		ch:   make(chan []byte, 64),
		done: make(chan struct{}),
	}
	subs.add(sub)
	defer subs.remove(sub)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case payload := <-sub.ch:
			if _, err := w.Write(payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func handlePost(ctx context.Context, w http.ResponseWriter, r *http.Request, p BowirePlugin, subs *subscriberSet) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	outcome := Dispatch(ctx, p, req)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	switch r := outcome.(type) {
	case ReplyResult:
		_ = json.NewEncoder(w).Encode(r.Response)
	case ShutdownResult:
		_ = json.NewEncoder(w).Encode(r.Response)
	case StreamResult:
		_ = json.NewEncoder(w).Encode(r.Ack)
		go pumpStreamHTTP(r.StreamID, r.Stream, subs)
	}
}

func pumpStreamHTTP(streamID string, stream <-chan string, subs *subscriberSet) {
	defer func() {
		end, _ := json.Marshal(Notification{
			JSONRPC: "2.0",
			Method:  "$/stream/end",
			Params:  map[string]string{"streamId": streamID},
		})
		subs.broadcast(append(append([]byte("data: "), end...), '\n', '\n'))
	}()
	for message := range stream {
		data, _ := json.Marshal(Notification{
			JSONRPC: "2.0",
			Method:  "$/stream/data",
			Params: map[string]string{
				"streamId": streamID,
				"message":  message,
			},
		})
		subs.broadcast(append(append([]byte("data: "), data...), '\n', '\n'))
	}
}

// subscriber is one open SSE connection. A bounded channel + non-
// blocking send means a slow consumer can't stall the broadcaster;
// dropped frames are tolerable for the workbench's diagnostic UX.
type subscriber struct {
	ch   chan []byte
	done chan struct{}
}

type subscriberSet struct {
	mu      sync.RWMutex
	members map[*subscriber]struct{}
}

func newSubscriberSet() *subscriberSet {
	return &subscriberSet{members: map[*subscriber]struct{}{}}
}

func (s *subscriberSet) add(sub *subscriber) {
	s.mu.Lock()
	s.members[sub] = struct{}{}
	s.mu.Unlock()
}

func (s *subscriberSet) remove(sub *subscriber) {
	s.mu.Lock()
	delete(s.members, sub)
	s.mu.Unlock()
}

func (s *subscriberSet) broadcast(payload []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.members {
		select {
		case sub.ch <- payload:
		default:
			// drop: slow consumer
		}
	}
}

func (s *subscriberSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.members {
		close(sub.done)
		delete(s.members, sub)
	}
}
