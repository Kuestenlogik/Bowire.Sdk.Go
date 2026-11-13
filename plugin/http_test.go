package plugin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type httpTestPlugin struct{}

func (httpTestPlugin) ID() string   { return "http-test" }
func (httpTestPlugin) Name() string { return "HTTP Test" }

func (httpTestPlugin) Discover(_ context.Context, _ DiscoverRequest) ([]ServiceInfo, error) {
	return []ServiceInfo{NewServiceInfo("Svc")}, nil
}

func (httpTestPlugin) Invoke(_ context.Context, req InvokeRequest) (InvokeResult, error) {
	return NewOKResult(`{"method":"` + req.Method + `"}`), nil
}

func (httpTestPlugin) InvokeStream(_ context.Context, _ InvokeRequest) (<-chan string, error) {
	ch := make(chan string, 2)
	ch <- "a"
	ch <- "b"
	close(ch)
	return ch, nil
}

func startTestServer(t *testing.T) (*HTTPHandle, string) {
	t.Helper()
	handle, err := RunHTTP(context.Background(), httpTestPlugin{}, "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("RunHTTP: %v", err)
	}
	return handle, "http://" + handle.Addr()
}

func TestRunHTTP_POSTRunsUnaryInvokeThroughDispatch(t *testing.T) {
	handle, base := startTestServer(t)
	defer func() { _ = handle.Close(context.Background()) }()

	body := bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"invoke","params":{"serverUrl":"x://host","service":"Svc","method":"Echo","jsonMessages":[],"showInternalServices":false,"metadata":{}}}`))
	resp, err := http.Post(base+"/", "application/json", body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var parsed struct {
		Result InvokeResult `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Result.Status != "OK" {
		t.Errorf("want OK, got %s", parsed.Result.Status)
	}
	if parsed.Result.Response != `{"method":"Echo"}` {
		t.Errorf("response mismatch: %q", parsed.Result.Response)
	}
}

func TestRunHTTP_POSTWithMalformedBody_Returns400(t *testing.T) {
	handle, base := startTestServer(t)
	defer func() { _ = handle.Close(context.Background()) }()

	resp, err := http.Post(base+"/", "application/json", strings.NewReader("not-json"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestRunHTTP_PATCH_Returns405(t *testing.T) {
	handle, base := startTestServer(t)
	defer func() { _ = handle.Close(context.Background()) }()

	req, _ := http.NewRequest("PATCH", base+"/", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("want 405, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Allow") != "GET, POST" {
		t.Errorf("want Allow: GET, POST, got %q", resp.Header.Get("Allow"))
	}
}

func TestRunHTTP_GETStartsSSEThatCarriesInvokeStreamFrames(t *testing.T) {
	handle, base := startTestServer(t)
	defer func() { _ = handle.Close(context.Background()) }()

	// Subscribe first so the broadcast pump has somewhere to land.
	sseReq, _ := http.NewRequest("GET", base+"/", nil)
	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sseResp.Body.Close()
	if got := sseResp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("want text/event-stream, got %q", got)
	}

	// Kick off the stream — ack lands in POST body, frames on SSE.
	body := bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":2,"method":"invokeStream","params":{"method":"Watch"}}`))
	postResp, err := http.Post(base+"/", "application/json", body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer postResp.Body.Close()
	var ack struct {
		Result map[string]string `json:"result"`
	}
	if err := json.NewDecoder(postResp.Body).Decode(&ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Result["streamId"] == "" {
		t.Errorf("want streamId in ack")
	}

	// Collect SSE events until we've seen both $/stream/data and
	// $/stream/end, or the 5s budget expires.
	deadline := time.Now().Add(5 * time.Second)
	reader := bufio.NewReader(sseResp.Body)
	sawData := false
	sawEnd := false
	for time.Now().Before(deadline) && !(sawData && sawEnd) {
		line, err := readWithTimeout(reader, time.Until(deadline))
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		line = strings.TrimPrefix(strings.TrimSpace(line), "data: ")
		if line == "" {
			continue
		}
		var n Notification
		if err := json.Unmarshal([]byte(line), &n); err != nil {
			continue
		}
		switch n.Method {
		case "$/stream/data":
			sawData = true
		case "$/stream/end":
			sawEnd = true
		}
	}
	if !sawData {
		t.Errorf("never saw a $/stream/data notification")
	}
	if !sawEnd {
		t.Errorf("never saw a $/stream/end notification")
	}
}

// readWithTimeout reads a single line from r, bailing if remaining
// > 0 elapses with nothing on the wire. SSE has no built-in idle
// timeout so we layer our own to keep the test bounded.
func readWithTimeout(r *bufio.Reader, remaining time.Duration) (string, error) {
	type result struct {
		line string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		line, err := r.ReadString('\n')
		done <- result{line, err}
	}()
	select {
	case res := <-done:
		return res.line, res.err
	case <-time.After(remaining):
		return "", io.EOF
	}
}

// sessionPlugin serves the session tests: channels that echo, streams the
// test feeds frame by frame, and a shutdown hook that counts its calls.
// Whatever the runtime does to it is reported on a Go channel, so a test
// waits for the event itself.
type sessionPlugin struct {
	httpTestPlugin

	// closed gets the id of a channel when its OnClose handler runs.
	closed chan string
	// stopped gets the method of a stream when its context is done.
	stopped chan string
	// hookRuns counts the calls of the shutdown hook.
	hookRuns atomic.Int32

	mu    sync.Mutex
	feeds map[string]chan string
}

func newSessionPlugin() *sessionPlugin {
	return &sessionPlugin{
		closed:  make(chan string, 16),
		stopped: make(chan string, 16),
		feeds:   map[string]chan string{},
	}
}

func (p *sessionPlugin) OpenChannel(_ context.Context, _ ChannelRequest, ch ChannelHandle) error {
	ch.OnMessage(func(message string) { _ = ch.Send("echo:" + message) })
	ch.OnClose(func() { p.closed <- ch.ID() })
	return nil
}

// InvokeStream emits what push hands it, until the runtime stops the stream.
func (p *sessionPlugin) InvokeStream(ctx context.Context, req InvokeRequest) (<-chan string, error) {
	out := make(chan string)
	feed := make(chan string)
	p.mu.Lock()
	p.feeds[req.Method] = feed
	p.mu.Unlock()
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				p.stopped <- req.Method
				return
			case message := <-feed:
				select {
				case out <- message:
				case <-ctx.Done():
					p.stopped <- req.Method
					return
				}
			}
		}
	}()
	return out, nil
}

// push emits one frame on the stream started for method.
func (p *sessionPlugin) push(method, message string) {
	p.mu.Lock()
	feed := p.feeds[method]
	p.mu.Unlock()
	feed <- message
}

func (p *sessionPlugin) Shutdown(context.Context) error {
	p.hookRuns.Add(1)
	return nil
}

var (
	_ ChannelPlugin   = (*sessionPlugin)(nil)
	_ StreamingPlugin = (*sessionPlugin)(nil)
	_ ShutdownHook    = (*sessionPlugin)(nil)
)

func startSessionServer(t *testing.T) (*sessionPlugin, *HTTPHandle, string) {
	t.Helper()
	p := newSessionPlugin()
	handle, err := RunHTTP(context.Background(), p, "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("RunHTTP: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close(context.Background()) })
	return p, handle, "http://" + handle.Addr()
}

// testHost is one host of an http sidecar: a session id (empty for a host
// that sends no header) and the SSE stream it reads its notifications off.
type testHost struct {
	t       *testing.T
	base    string
	session string
	sse     *http.Response
	// frames carries every notification of the SSE stream, in order, and
	// closes when the stream ends.
	frames chan string
}

// connectHost opens the SSE stream of a session. The sidecar registers the
// stream before it answers, so the host is subscribed once this returns.
func connectHost(t *testing.T, base, session string) *testHost {
	t.Helper()
	h := &testHost{t: t, base: base, session: session, frames: make(chan string, 64)}
	h.sse = h.do(http.MethodGet, nil)
	t.Cleanup(h.disconnect)
	go func() {
		defer close(h.frames)
		scanner := bufio.NewScanner(h.sse.Body)
		for scanner.Scan() {
			data, isData := strings.CutPrefix(scanner.Text(), "data: ")
			if !isData {
				continue
			}
			var n struct {
				Method string            `json:"method"`
				Params map[string]string `json:"params"`
			}
			if err := json.Unmarshal([]byte(data), &n); err != nil {
				h.frames <- "undecodable: " + data
				continue
			}
			h.frames <- describeFrame(n.Method, n.Params)
		}
	}()
	return h
}

// describeFrame renders a notification as "<method> <id>" or
// "<method> <id> <message>".
func describeFrame(method string, params map[string]string) string {
	id := params["channelId"]
	if id == "" {
		id = params["streamId"]
	}
	if message, has := params["message"]; has {
		return method + " " + id + " " + message
	}
	return method + " " + id
}

func (h *testHost) do(method string, body io.Reader) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.base+"/", body)
	if err != nil {
		h.t.Fatalf("%s: %v", method, err)
	}
	if h.session != "" {
		req.Header.Set(SessionHeader, h.session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s: %v", method, err)
	}
	return resp
}

// disconnect closes the host's SSE stream, as a crashed host would.
func (h *testHost) disconnect() { _ = h.sse.Body.Close() }

// call POSTs one request in the host's session and returns the reply's
// error, nil for a result.
func (h *testHost) call(method string, params interface{}) *ErrorObject {
	h.t.Helper()
	body, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		h.t.Fatalf("%s: %v", method, err)
	}
	resp := h.do(http.MethodPost, bytes.NewReader(body))
	defer resp.Body.Close()
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *ErrorObject    `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		h.t.Fatalf("%s: decode the reply: %v", method, err)
	}
	if reply.Error == nil && reply.Result == nil {
		h.t.Fatalf("%s: the reply carries neither a result nor an error", method)
	}
	return reply.Error
}

// mustCall is call for a request that has to succeed.
func (h *testHost) mustCall(method string, params interface{}) {
	h.t.Helper()
	if rpcErr := h.call(method, params); rpcErr != nil {
		h.t.Fatalf("%s: want a result, got error %+v", method, rpcErr)
	}
}

func (h *testHost) openChannel(id string) {
	h.t.Helper()
	h.mustCall("openChannel", map[string]string{"channelId": id, "service": "S", "method": "M"})
}

func (h *testHost) send(channelID, message string) *ErrorObject {
	h.t.Helper()
	return h.call("channel.send", map[string]string{"channelId": channelID, "message": message})
}

// mustSend is send for a frame the channel has to take.
func (h *testHost) mustSend(channelID, message string) {
	h.t.Helper()
	if rpcErr := h.send(channelID, message); rpcErr != nil {
		h.t.Fatalf("channel.send to %s in session %q: %+v", channelID, h.session, rpcErr)
	}
}

// expectFrame waits for the host's next notification, which has to be want.
// A frame that was meant for another session would turn up here first.
func (h *testHost) expectFrame(want string) {
	h.t.Helper()
	got, open := <-h.frames
	if !open {
		h.t.Fatalf("the SSE stream ended, want frame %q", want)
	}
	if got != want {
		h.t.Fatalf("next frame of session %q:\n got %q\nwant %q", h.session, got, want)
	}
}

func wantChannelGone(t *testing.T, what string, rpcErr *ErrorObject, channelID string) {
	t.Helper()
	if rpcErr == nil || rpcErr.Code != ErrChannelGone || rpcErr.Message != "channel "+channelID+" is closed" {
		t.Errorf("%s: want the channel-is-closed error for %s, got %+v", what, channelID, rpcErr)
	}
}

// noneClosed holds that no OnClose handler has run. A request that ends a
// session runs the handlers before it is answered, so there is nothing to
// wait for.
func noneClosed(t *testing.T, p *sessionPlugin) {
	t.Helper()
	select {
	case id := <-p.closed:
		t.Errorf("channel %s was closed", id)
	default:
	}
}

func TestSessions_ShutdownEndsOnlyTheLeavingHostsChannels(t *testing.T) {
	t.Parallel()
	p, _, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	// Both hosts mint the same id: the session keeps them apart.
	a.openChannel("chat")
	b.openChannel("chat")
	a.mustSend("chat", "from a")
	a.expectFrame("$/channel/data chat echo:from a")

	a.mustCall("shutdown", nil)

	if got := <-p.closed; got != "chat" {
		t.Errorf("closed channel: got %s", got)
	}
	noneClosed(t, p)
	a.expectFrame("$/channel/closed chat")
	wantChannelGone(t, "the leaving host's send", a.send("chat", "late"), "chat")

	// The other host's channel is untouched, and the first frame on its SSE
	// stream is its own: it saw nothing of host a.
	b.mustSend("chat", "still here")
	b.expectFrame("$/channel/data chat echo:still here")

	// Ending the session a second time is harmless.
	a.mustCall("shutdown", nil)
	noneClosed(t, p)
}

func TestSessions_NotificationsGoOnlyToTheOwningSession(t *testing.T) {
	t.Parallel()
	_, _, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	// A second SSE stream of the same session reads the session's frames too.
	a2 := connectHost(t, base, "host-a")
	a.openChannel("a-chan")
	b.openChannel("b-chan")

	// A frame is on the SSE streams it goes to before the send is answered,
	// so host a's next frame being its own shows it never got host b's.
	b.mustSend("b-chan", "one")
	a.mustSend("a-chan", "two")
	a.expectFrame("$/channel/data a-chan echo:two")
	a2.expectFrame("$/channel/data a-chan echo:two")
	b.expectFrame("$/channel/data b-chan echo:one")
}

func TestSessions_StreamFramesGoOnlyToTheOwningSessionAndStopWithIt(t *testing.T) {
	t.Parallel()
	p, _, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	a.mustCall("invokeStream", map[string]string{"streamId": "a-stream", "method": "WatchA"})
	b.mustCall("invokeStream", map[string]string{"streamId": "b-stream", "method": "WatchB"})

	p.push("WatchB", "b1")
	b.expectFrame("$/stream/data b-stream b1")
	p.push("WatchA", "a1")
	a.expectFrame("$/stream/data a-stream a1")

	a.mustCall("shutdown", nil)

	if got := <-p.stopped; got != "WatchA" {
		t.Errorf("stopped stream: got %s", got)
	}
	a.expectFrame("$/stream/end a-stream")

	// Host b's stream runs on: its next frame is data, not an end.
	p.push("WatchB", "b2")
	b.expectFrame("$/stream/data b-stream b2")
}

func TestSessions_ClosingTheLastSSEStreamEndsTheSession(t *testing.T) {
	t.Parallel()
	p, _, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	a2 := connectHost(t, base, "host-a")
	a.openChannel("a-chan")
	b.openChannel("b-chan")
	a.mustCall("invokeStream", map[string]string{"streamId": "a-stream", "method": "WatchA"})

	// The last stream closing ends the session: the host said nothing, it
	// is just gone. Host a has two, so both have to close.
	a2.disconnect()
	a.disconnect()
	if got := <-p.closed; got != "a-chan" {
		t.Errorf("closed channel: got %s", got)
	}
	if got := <-p.stopped; got != "WatchA" {
		t.Errorf("stopped stream: got %s", got)
	}
	wantChannelGone(t, "send after the session ended", a.send("a-chan", "late"), "a-chan")

	b.mustSend("b-chan", "still here")
	b.expectFrame("$/channel/data b-chan echo:still here")
	noneClosed(t, p)
}

func TestSessions_ThePluginHookRunsWhenTheServerStopsNotWhenAHostLeaves(t *testing.T) {
	t.Parallel()
	p, handle, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	a.openChannel("a-chan")
	b.openChannel("b-chan")

	a.mustCall("shutdown", nil)
	if got := <-p.closed; got != "a-chan" {
		t.Errorf("closed channel: got %s", got)
	}
	if runs := p.hookRuns.Load(); runs != 0 {
		t.Errorf("a host leaving ran the shutdown hook %d times", runs)
	}

	// The server stopping ends what is still open and runs the hook, once.
	if err := handle.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := <-p.closed; got != "b-chan" {
		t.Errorf("closed channel: got %s", got)
	}
	if runs := p.hookRuns.Load(); runs != 1 {
		t.Errorf("the server stopping ran the shutdown hook %d times", runs)
	}
	if err := handle.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if runs := p.hookRuns.Load(); runs != 1 {
		t.Errorf("after a second Close the shutdown hook has run %d times", runs)
	}
}

func TestSessions_ShutdownWithoutTheHeaderEndsNothing(t *testing.T) {
	t.Parallel()
	p, _, base := startSessionServer(t)
	unnamed, named := connectHost(t, base, ""), connectHost(t, base, "host-b")
	unnamed.openChannel("old-chan")
	named.openChannel("b-chan")

	unnamed.mustCall("shutdown", nil)

	noneClosed(t, p)
	if runs := p.hookRuns.Load(); runs != 0 {
		t.Errorf("shutdown without the header ran the shutdown hook %d times", runs)
	}
	// Unnamed requests still reach the unnamed session's channel, and its
	// frames go to the SSE streams that sent no header, not to a named one.
	unnamed.mustSend("old-chan", "still here")
	named.mustSend("b-chan", "mine")
	unnamed.expectFrame("$/channel/data old-chan echo:still here")
	named.expectFrame("$/channel/data b-chan echo:mine")
}

func TestSessions_AChannelIsReachedOnlyFromItsOwnSession(t *testing.T) {
	t.Parallel()
	p, _, base := startSessionServer(t)
	a, b := connectHost(t, base, "host-a"), connectHost(t, base, "host-b")
	unnamed := connectHost(t, base, "")
	a.openChannel("a-chan")

	wantChannelGone(t, "another host's send", b.send("a-chan", "intruder"), "a-chan")
	wantChannelGone(t, "another host's close",
		b.call("channel.close", map[string]string{"channelId": "a-chan"}), "a-chan")
	wantChannelGone(t, "a send without the header", unnamed.send("a-chan", "intruder"), "a-chan")
	noneClosed(t, p)

	// The channel never saw any of it: the owner's frame is the first echo.
	a.mustSend("a-chan", "owner")
	a.expectFrame("$/channel/data a-chan echo:owner")
}
