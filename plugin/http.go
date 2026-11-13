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
	dispatcher  *Dispatcher
}

// Addr returns the actual TCP address the server is listening on.
func (h *HTTPHandle) Addr() string { return h.listener.Addr().String() }

// Close stops the sidecar: it ends every session (channels close, streams
// stop), disconnects every SSE subscriber, shuts down the HTTP server and
// then runs the plugin's ShutdownHook. The hook runs once, however often
// Close is called, and only here: a host that leaves does not trigger it.
func (h *HTTPHandle) Close(ctx context.Context) error {
	h.dispatcher.EndAllSessions()
	h.subscribers.closeAll()
	err := h.server.Shutdown(ctx)
	h.dispatcher.RunShutdownHook(ctx)
	return err
}

// SessionHeader is the request header a host names itself with, on every
// POST and on the SSE GET. Its value is opaque. Requests without it form
// one unnamed session.
const SessionHeader = "Bowire-Session"

// RunHTTP drives `p` over the streamable-HTTP contract on host:port.
//
//   - POST / decodes a JSON-RPC request from the body, runs Dispatch,
//     returns the response (or stream ack) in the body.
//   - GET  / returns a long-lived Server-Sent-Events stream the
//     runtime pushes server notifications onto
//     ($/stream/data, $/stream/end, $/channel/data, $/channel/closed).
//
// One sidecar serves several hosts, told apart by the SessionHeader. A
// stream or channel belongs to the session of the request that started it,
// and its notifications go only to that session's SSE streams. A session
// ends when its host POSTs `shutdown` or when its last SSE stream closes;
// its channels close and its streams stop, and every other session carries
// on. The plugin's ShutdownHook runs when the handle is closed.
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
	d := NewSessionDispatcher(p, func(session string, n Notification) { sendNotification(subs, session, n) })

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleSubscribe(w, r, d, subs)
		case http.MethodPost:
			handlePost(ctx, w, r, d, subs)
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()

	return &HTTPHandle{server: server, listener: listener, subscribers: subs, dispatcher: d}, nil
}

func handleSubscribe(w http.ResponseWriter, r *http.Request, d *Dispatcher, subs *subscriberSet) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported by this responder", http.StatusInternalServerError)
		return
	}

	// Register before headers go out — once the client's Do() returns
	// it may immediately POST to start a stream, and the broadcaster
	// must already see this subscriber when the pump fires.
	sub := &subscriber{
		session: r.Header.Get(SessionHeader),
		ch:      make(chan []byte, 64),
		done:    make(chan struct{}),
	}
	subs.add(sub)
	defer func() {
		// The last stream of a session closing is how a host that crashed
		// or lost the network leaves.
		if subs.remove(sub) {
			d.EndSession(sub.session)
		}
	}()

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

func handlePost(ctx context.Context, w http.ResponseWriter, r *http.Request, d *Dispatcher, subs *subscriberSet) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	session := r.Header.Get(SessionHeader)
	outcome := d.DispatchFor(ctx, session, req)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// No ShutdownResult here: over http `shutdown` ends the caller's session
	// and is an ordinary reply.
	switch r := outcome.(type) {
	case ReplyResult:
		_ = json.NewEncoder(w).Encode(r.Response)
	case StreamResult:
		_ = json.NewEncoder(w).Encode(r.Ack)
		go pumpStreamHTTP(session, r.StreamID, r.Stream, subs)
	}
}

func pumpStreamHTTP(session, streamID string, stream <-chan string, subs *subscriberSet) {
	defer sendNotification(subs, session, Notification{
		JSONRPC: "2.0",
		Method:  "$/stream/end",
		Params:  map[string]string{"streamId": streamID},
	})
	for message := range stream {
		sendNotification(subs, session, Notification{
			JSONRPC: "2.0",
			Method:  "$/stream/data",
			Params: map[string]string{
				"streamId": streamID,
				"message":  message,
			},
		})
	}
}

// sendNotification puts one server-initiated frame on every open SSE
// stream of the session that owns it.
func sendNotification(subs *subscriberSet, session string, n Notification) {
	data, _ := json.Marshal(n)
	subs.send(session, append(append([]byte("data: "), data...), '\n', '\n'))
}

// subscriber is one open SSE connection of one session. A bounded channel
// + non-blocking send means a slow consumer can't stall the sender;
// dropped frames are tolerable for the workbench's diagnostic UX.
type subscriber struct {
	session string
	ch      chan []byte
	done    chan struct{}
}

// subscriberSet holds the open SSE connections by session. The unnamed
// session's key is the empty string.
type subscriberSet struct {
	mu       sync.RWMutex
	sessions map[string]map[*subscriber]struct{}
}

func newSubscriberSet() *subscriberSet {
	return &subscriberSet{sessions: map[string]map[*subscriber]struct{}{}}
}

func (s *subscriberSet) add(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.sessions[sub.session]
	if members == nil {
		members = map[*subscriber]struct{}{}
		s.sessions[sub.session] = members
	}
	members[sub] = struct{}{}
}

// remove drops one connection and reports whether it was the last of its
// session. A connection closeAll already dropped reports false.
func (s *subscriberSet) remove(sub *subscriber) (last bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.sessions[sub.session]
	if _, present := members[sub]; !present {
		return false
	}
	delete(members, sub)
	if len(members) > 0 {
		return false
	}
	delete(s.sessions, sub.session)
	return true
}

func (s *subscriberSet) send(session string, payload []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.sessions[session] {
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
	for session, members := range s.sessions {
		for sub := range members {
			close(sub.done)
		}
		delete(s.sessions, session)
	}
}
