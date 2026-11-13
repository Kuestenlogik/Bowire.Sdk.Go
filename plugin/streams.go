package plugin

import (
	"context"
	"sync"
)

// hostStream is one running `invokeStream`, owned by the session of the
// request that started it. Cancelling it ends the stream.
type hostStream struct {
	session string
	cancel  context.CancelFunc
}

// streamRegistry holds the running streams of a runtime that serves several
// hosts, so the streams of one session can be stopped when its host leaves.
type streamRegistry struct {
	mu      sync.Mutex
	streams map[*hostStream]struct{}
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{streams: map[*hostStream]struct{}{}}
}

// start opens a stream for session and returns the channel the transport
// pumps. open gets a context that is cancelled when the session ends, so a
// plugin that watches it stops producing. The returned channel closes when
// the plugin's channel closes or when the stream is stopped, whichever is
// first; either way the transport's pump emits its one `$/stream/end`.
func (r *streamRegistry) start(ctx context.Context, session string, open func(context.Context) (<-chan string, error)) (<-chan string, error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &hostStream{session: session, cancel: cancel}
	r.mu.Lock()
	r.streams[s] = struct{}{}
	r.mu.Unlock()

	in, err := open(ctx)
	if err != nil {
		r.forget(s)
		return nil, err
	}

	out := make(chan string)
	go func() {
		relay(ctx, in, out)
		close(out)
		r.forget(s)
		// A stopped stream is read to its end, so a plugin that is still
		// writing into it does not block forever on a frame nobody takes.
		for range in {
		}
	}()
	return out, nil
}

// relay passes frames from in to out until in closes or ctx is done.
func relay(ctx context.Context, in <-chan string, out chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-in:
			if !ok {
				return
			}
			select {
			case out <- message:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *streamRegistry) forget(s *hostStream) {
	r.mu.Lock()
	delete(r.streams, s)
	r.mu.Unlock()
	s.cancel()
}

// stopSession stops the streams of one session.
func (r *streamRegistry) stopSession(session string) {
	r.stop(func(s *hostStream) bool { return s.session == session })
}

// stopAll stops every stream.
func (r *streamRegistry) stopAll() {
	r.stop(func(*hostStream) bool { return true })
}

func (r *streamRegistry) stop(match func(*hostStream) bool) {
	r.mu.Lock()
	var stopping []*hostStream
	for s := range r.streams {
		if match(s) {
			stopping = append(stopping, s)
		}
	}
	r.mu.Unlock()
	for _, s := range stopping {
		s.cancel()
	}
}
