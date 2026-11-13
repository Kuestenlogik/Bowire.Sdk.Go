package plugin

import (
	"errors"
	"fmt"
	"sync"
)

// ErrChannelClosed is what ChannelHandle.Send returns once the channel has
// closed: the host has stopped reading it.
var ErrChannelClosed = errors.New("plugin: channel closed")

type channelState int

const (
	channelOpen channelState = iota
	channelClosing
	channelClosed
)

// hostChannel is one duplex channel, keyed by the id the host minted
// (Bowire.Sdk.Go#3). The host subscribes to that id before it sends
// `openChannel`, so every frame carries it — a self-minted one would
// publish into a channel nobody reads.
//
// session is the host that opened it: over http only requests of that
// session reach the channel, and its frames go only to that session.
type hostChannel struct {
	id      string
	session string
	notify  func(Notification)
	ended   func(*hostChannel)

	mu            sync.Mutex
	state         channelState
	onMessage     []func(string)
	onCloseChain  []func()
	deliverSerial sync.Mutex
}

func (c *hostChannel) ID() string { return c.id }

// Send is still accepted while the OnClose handlers run, so a plugin can
// answer a close with a last word. After that the host has stopped reading.
func (c *hostChannel) Send(message string) error {
	c.mu.Lock()
	closed := c.state == channelClosed
	c.mu.Unlock()
	if closed {
		return ErrChannelClosed
	}
	c.notify(Notification{
		JSONRPC: "2.0",
		Method:  "$/channel/data",
		Params:  map[string]string{"channelId": c.id, "message": message},
	})
	return nil
}

func (c *hostChannel) OnMessage(handler func(message string)) {
	c.mu.Lock()
	c.onMessage = append(c.onMessage, handler)
	c.mu.Unlock()
}

func (c *hostChannel) OnClose(handler func()) {
	c.mu.Lock()
	c.onCloseChain = append(c.onCloseChain, handler)
	c.mu.Unlock()
}

func (c *hostChannel) Close() { c.end() }

func (c *hostChannel) isOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == channelOpen
}

// deliver hands a `channel.send` from the host to the message handlers, in
// order. Deliveries never overlap, so frames reach the plugin in the order
// the host sent them even on the HTTP transport, where requests arrive on
// goroutines of their own.
func (c *hostChannel) deliver(message string) {
	c.deliverSerial.Lock()
	defer c.deliverSerial.Unlock()
	c.mu.Lock()
	handlers := append([]func(string){}, c.onMessage...)
	c.mu.Unlock()
	for _, h := range handlers {
		h(message)
	}
}

// end closes the channel from either side, exactly once: the OnClose
// handlers run, then `$/channel/closed` goes out. A second call while the
// first is under way returns at once — which also keeps a Close from
// inside an OnClose handler from waiting on itself.
func (c *hostChannel) end() {
	c.mu.Lock()
	if c.state != channelOpen {
		c.mu.Unlock()
		return
	}
	c.state = channelClosing
	handlers := append([]func(){}, c.onCloseChain...)
	c.mu.Unlock()

	// Deferred so it happens even when a handler panics: the host's read
	// stream must end, or it waits for a frame that never comes.
	defer func() {
		c.mu.Lock()
		c.state = channelClosed
		c.mu.Unlock()
		c.ended(c)
		c.notify(Notification{
			JSONRPC: "2.0",
			Method:  "$/channel/closed",
			Params:  map[string]string{"channelId": c.id},
		})
	}()
	for _, h := range handlers {
		h()
	}
}

// channelKey names a channel: the id its host minted, within that host's
// session. Two hosts that mint the same id do not collide, and one host
// cannot reach another's channel by naming its id.
type channelKey struct {
	session string
	id      string
}

// channelRegistry holds the open channels of one runtime. Each is reached
// only by its own session and id, so frames never cross.
type channelRegistry struct {
	notify func(session string, n Notification)

	mu       sync.Mutex
	channels map[channelKey]*hostChannel
}

func newChannelRegistry(notify func(session string, n Notification)) *channelRegistry {
	return &channelRegistry{notify: notify, channels: map[channelKey]*hostChannel{}}
}

// open registers a channel under the host's id. A second open with an id
// that is still open in the same session is refused rather than silently
// replacing the first.
func (r *channelRegistry) open(session, id string) (*hostChannel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := channelKey{session: session, id: id}
	if _, taken := r.channels[key]; taken {
		return nil, fmt.Errorf("channel %s is already open", id)
	}
	c := &hostChannel{
		id:      id,
		session: session,
		notify:  func(n Notification) { r.notify(session, n) },
		ended:   r.forget,
	}
	r.channels[key] = c
	return c, nil
}

// get finds a channel of the caller's own session. An id that belongs to
// another session is as absent as one that never existed.
func (r *channelRegistry) get(session, id string) *hostChannel {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channels[channelKey{session: session, id: id}]
}

func (r *channelRegistry) forget(c *hostChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := channelKey{session: c.session, id: c.id}
	if r.channels[key] == c {
		delete(r.channels, key)
	}
}

// discard drops a channel that never finished opening. No
// `$/channel/closed`: the host got an error for the open and has already
// let the id go.
func (r *channelRegistry) discard(c *hostChannel) { r.forget(c) }

func (r *channelRegistry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.channels)
}

// closeAll closes every open channel: the runtime is stopping, or the
// one host's wire is gone.
func (r *channelRegistry) closeAll() {
	r.close(func(*hostChannel) bool { return true })
}

// closeSession closes the channels of one session: its host has left.
func (r *channelRegistry) closeSession(session string) {
	r.close(func(c *hostChannel) bool { return c.session == session })
}

func (r *channelRegistry) close(match func(*hostChannel) bool) {
	r.mu.Lock()
	open := make([]*hostChannel, 0, len(r.channels))
	for _, c := range r.channels {
		if match(c) {
			open = append(open, c)
		}
	}
	r.mu.Unlock()
	for _, c := range open {
		c.end()
	}
}
