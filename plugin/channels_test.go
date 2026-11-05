package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// chatPlugin echoes every frame back with a prefix; a frame "close" closes
// the channel from the plugin's side, and the OnClose handler says goodbye.
type chatPlugin struct {
	mu      sync.Mutex
	handles map[string]ChannelHandle
	closed  map[string]int
	refuse  bool
}

func newChatPlugin() *chatPlugin {
	return &chatPlugin{handles: map[string]ChannelHandle{}, closed: map[string]int{}}
}

func (*chatPlugin) ID() string   { return "chat" }
func (*chatPlugin) Name() string { return "Chat" }
func (*chatPlugin) Discover(context.Context, DiscoverRequest) ([]ServiceInfo, error) {
	return nil, nil
}
func (*chatPlugin) Invoke(context.Context, InvokeRequest) (InvokeResult, error) {
	return NewOKResult("{}"), nil
}

func (p *chatPlugin) OpenChannel(_ context.Context, req ChannelRequest, ch ChannelHandle) error {
	if p.refuse {
		return errors.New("no channels today")
	}
	p.mu.Lock()
	p.handles[ch.ID()] = ch
	p.mu.Unlock()
	ch.OnMessage(func(message string) {
		if message == "close" {
			ch.Close()
			return
		}
		_ = ch.Send(req.Method + ":" + message)
	})
	ch.OnClose(func() {
		p.mu.Lock()
		p.closed[ch.ID()]++
		p.mu.Unlock()
		_ = ch.Send("bye")
	})
	return nil
}

func (p *chatPlugin) handle(id string) ChannelHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.handles[id]
}

func (p *chatPlugin) closeCount(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed[id]
}

// recorder collects the notifications a Dispatcher emits.
type recorder struct {
	mu    sync.Mutex
	notes []Notification
}

func (r *recorder) notify(n Notification) {
	r.mu.Lock()
	r.notes = append(r.notes, n)
	r.mu.Unlock()
}

// frames lists the notifications of one channel as "data:<message>" and
// "closed".
func (r *recorder) frames(channelID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, n := range r.notes {
		p := n.Params.(map[string]string)
		if p["channelId"] != channelID {
			continue
		}
		switch n.Method {
		case "$/channel/data":
			out = append(out, "data:"+p["message"])
		case "$/channel/closed":
			out = append(out, "closed")
		}
	}
	return out
}

func call(t *testing.T, d *Dispatcher, method string, params interface{}) Response {
	t.Helper()
	r := d.Dispatch(context.Background(), Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: method, Params: mustRaw(t, params)})
	switch x := r.(type) {
	case ReplyResult:
		return x.Response
	case ShutdownResult:
		return x.Response
	}
	t.Fatalf("%s: unexpected %T", method, r)
	return Response{}
}

func mustOK(t *testing.T, resp Response) {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("want a result, got error %+v", resp.Error)
	}
}

func open(t *testing.T, d *Dispatcher, id string) {
	t.Helper()
	resp := call(t, d, "openChannel", map[string]interface{}{"channelId": id, "service": "S", "method": "M"})
	mustOK(t, resp)
	if got := resp.Result.(map[string]string)["channelId"]; got != id {
		t.Fatalf("the reply names the host's id: want %s, got %s", id, got)
	}
}

func send(t *testing.T, d *Dispatcher, id string, message interface{}) Response {
	t.Helper()
	return call(t, d, "channel.send", map[string]interface{}{"channelId": id, "message": message})
}

func equal(a, b []string) bool { return strings.Join(a, "|") == strings.Join(b, "|") }

func TestChannels_CapabilityFollowsTheInterface(t *testing.T) {
	caps := capabilitiesOf(newChatPlugin())
	if !caps["channels"] {
		t.Errorf("a ChannelPlugin reports channels: true")
	}
	if capabilitiesOf(minimalPlugin{})["channels"] {
		t.Errorf("a plugin without ChannelPlugin reports channels: false")
	}
	resp := call(t, NewDispatcher(minimalPlugin{}, discardNotify), "openChannel", map[string]string{"channelId": "x"})
	if resp.Error == nil || resp.Error.Code != ErrMethodNotFound {
		t.Errorf("openChannel without the capability: want method-not-found, got %+v", resp.Error)
	}
}

func TestChannels_OpenSendBothWaysCloseFromTheHost(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	d := NewDispatcher(p, rec.notify)
	open(t, d, "ab12")

	mustOK(t, send(t, d, "ab12", "hello"))
	mustOK(t, send(t, d, "ab12", map[string]int{"n": 1}))
	if err := p.handle("ab12").Send("pushed"); err != nil {
		t.Fatalf("the plugin pushes on its own: %v", err)
	}
	mustOK(t, call(t, d, "channel.close", map[string]string{"channelId": "ab12"}))

	want := []string{"data:M:hello", `data:M:{"n":1}`, "data:pushed", "data:bye", "closed"}
	if got := rec.frames("ab12"); !equal(got, want) {
		t.Errorf("frames:\n got %v\nwant %v", got, want)
	}
	if p.closeCount("ab12") != 1 {
		t.Errorf("OnClose ran %d times", p.closeCount("ab12"))
	}

	// After the close the host's send is refused with the error it reads
	// as false, and the plugin's Send reports the closed channel.
	if resp := send(t, d, "ab12", "late"); resp.Error == nil || resp.Error.Code != ErrChannelGone {
		t.Errorf("send after close: want ErrChannelGone, got %+v", resp.Error)
	}
	if err := p.handle("ab12").Send("late"); !errors.Is(err, ErrChannelClosed) {
		t.Errorf("plugin send after close: want ErrChannelClosed, got %v", err)
	}
	if resp := call(t, d, "channel.close", map[string]string{"channelId": "ab12"}); resp.Error == nil || resp.Error.Code != ErrChannelGone {
		t.Errorf("a second close finds no channel: %+v", resp.Error)
	}
	if got := rec.frames("ab12"); !equal(got, want) {
		t.Errorf("nothing follows the closed notification: %v", got)
	}
}

func TestChannels_CloseFromThePluginEndsTheHostStreamOnce(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	d := NewDispatcher(p, rec.notify)
	open(t, d, "c1")

	mustOK(t, send(t, d, "c1", "ping"))
	mustOK(t, send(t, d, "c1", "close"))
	want := []string{"data:M:ping", "data:bye", "closed"}
	if got := rec.frames("c1"); !equal(got, want) {
		t.Errorf("frames:\n got %v\nwant %v", got, want)
	}
	if resp := send(t, d, "c1", "anyone?"); resp.Error == nil || resp.Error.Code != ErrChannelGone {
		t.Errorf("a send after the plugin closed: want ErrChannelGone, got %+v", resp.Error)
	}
	if p.closeCount("c1") != 1 {
		t.Errorf("OnClose ran %d times", p.closeCount("c1"))
	}
}

func TestChannels_SeveralOpenAtOnceDoNotCrossFrames(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	d := NewDispatcher(p, rec.notify)
	open(t, d, "one")
	open(t, d, "two")

	mustOK(t, send(t, d, "one", "a"))
	mustOK(t, send(t, d, "two", "b"))
	mustOK(t, call(t, d, "channel.close", map[string]string{"channelId": "one"}))
	mustOK(t, send(t, d, "two", "c"))

	if got := rec.frames("one"); !equal(got, []string{"data:M:a", "data:bye", "closed"}) {
		t.Errorf("one: %v", got)
	}
	if got := rec.frames("two"); !equal(got, []string{"data:M:b", "data:M:c"}) {
		t.Errorf("two: %v", got)
	}
}

// Both sides closing at the same moment still yields one round of OnClose
// handlers and one $/channel/closed. Run with -race.
func TestChannels_ConcurrentCloseFromBothSidesHappensOnce(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	d := NewDispatcher(p, rec.notify)
	open(t, d, "race")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.handle("race").Close()
	}()
	go func() {
		defer wg.Done()
		d.Dispatch(context.Background(), Request{JSONRPC: "2.0", ID: json.RawMessage("2"), Method: "channel.close",
			Params: json.RawMessage(`{"channelId":"race"}`)})
	}()
	wg.Wait()

	closed := 0
	for _, f := range rec.frames("race") {
		if f == "closed" {
			closed++
		}
	}
	if closed != 1 || p.closeCount("race") != 1 {
		t.Errorf("want one closed notification and one OnClose, got %d and %d", closed, p.closeCount("race"))
	}
}

func TestChannels_ARefusedOpenLeavesNothingBehind(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	p.refuse = true
	d := NewDispatcher(p, rec.notify)
	resp := call(t, d, "openChannel", map[string]string{"channelId": "no"})
	if resp.Error == nil || resp.Error.Code != ErrInternal {
		t.Fatalf("want the plugin's error, got %+v", resp.Error)
	}
	if d.channels.size() != 0 || len(rec.frames("no")) != 0 {
		t.Errorf("a refused open registers nothing and sends nothing")
	}
	p.refuse = false
	open(t, d, "no")
}

func TestChannels_AnIDStillOpenIsRefused(t *testing.T) {
	d := NewDispatcher(newChatPlugin(), discardNotify)
	open(t, d, "dup")
	resp := call(t, d, "openChannel", map[string]string{"channelId": "dup"})
	if resp.Error == nil || resp.Error.Code != ErrInvalidParams {
		t.Errorf("want invalid-params for a second open of a live id, got %+v", resp.Error)
	}
}

func TestChannels_ShutdownClosesWhatIsOpen(t *testing.T) {
	p, rec := newChatPlugin(), &recorder{}
	d := NewDispatcher(p, rec.notify)
	open(t, d, "s1")
	open(t, d, "s2")
	mustOK(t, call(t, d, "shutdown", map[string]string{}))
	for _, id := range []string{"s1", "s2"} {
		if got := rec.frames(id); !equal(got, []string{"data:bye", "closed"}) {
			t.Errorf("%s: %v", id, got)
		}
	}
}

// Over stdio: the frames interleave with the replies on stdout, and EOF on
// stdin ends what is still open.
func TestRun_ChannelOverStdio(t *testing.T) {
	in := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"openChannel","params":{"channelId":"io","method":"M"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"channel.send","params":{"channelId":"io","message":"hi"}}`,
		`{"jsonrpc":"2.0","id":3,"method":"openChannel","params":{"channelId":"left-open","method":"M"}}`,
	}, "\n") + "\n")
	var out bytes.Buffer
	p := newChatPlugin()
	if err := runWithIO(context.Background(), p, in, &out); err != nil {
		t.Fatalf("runWithIO: %v", err)
	}

	var lines []string
	dec := json.NewDecoder(&out)
	for {
		var m map[string]interface{}
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if method, ok := m["method"].(string); ok {
			params := m["params"].(map[string]interface{})
			lines = append(lines, method+" "+params["channelId"].(string))
		} else {
			lines = append(lines, "reply "+string(mustRaw(t, m["id"])))
		}
	}
	want := []string{
		"reply 1",
		"$/channel/data io",
		"reply 2",
		"reply 3",
	}
	if !equal(lines[:4], want) {
		t.Fatalf("wire:\n got %v\nwant %v...", lines, want)
	}
	// EOF: both channels end, each with its goodbye and one closed.
	rest := strings.Join(lines[4:], "|")
	for _, id := range []string{"io", "left-open"} {
		if strings.Count(rest, "$/channel/closed "+id) != 1 {
			t.Errorf("%s: want one closed after EOF, got %v", id, lines[4:])
		}
		if p.closeCount(id) != 1 {
			t.Errorf("%s: OnClose ran %d times", id, p.closeCount(id))
		}
	}
}
