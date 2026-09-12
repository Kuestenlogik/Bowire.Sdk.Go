package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type testPlugin struct {
	shutdownCalled bool
}

func (testPlugin) ID() string      { return "test" }
func (testPlugin) Name() string    { return "Test Plugin" }
func (testPlugin) IconSvg() string { return "<svg/>" }

func (testPlugin) Discover(_ context.Context, _ string, _ bool) ([]ServiceInfo, error) {
	return []ServiceInfo{NewServiceInfo("Svc").WithMethods(UnaryMethod("M"))}, nil
}

func (testPlugin) Invoke(_ context.Context, req InvokeRequest) (InvokeResult, error) {
	if req.Method == "fail" {
		return InvokeResult{}, errors.New("boom")
	}
	first := ""
	if len(req.JSONMessages) > 0 {
		first = req.JSONMessages[0]
	}
	return NewOKResult(`{"echoed":"` + first + `"}`), nil
}

func (testPlugin) InvokeStream(_ context.Context, _ InvokeRequest) (<-chan string, error) {
	ch := make(chan string, 2)
	ch <- "frame-1"
	ch <- "frame-2"
	close(ch)
	return ch, nil
}

func (p *testPlugin) Shutdown(_ context.Context) error {
	p.shutdownCalled = true
	return nil
}

// Compile-time interface checks.
var (
	_ BowirePlugin    = testPlugin{}
	_ StreamingPlugin = testPlugin{}
	_ IconPlugin      = testPlugin{}
	_ ShutdownHook    = (*testPlugin)(nil)
)

func mustRawID(t *testing.T, id int) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal id: %v", err)
	}
	return raw
}

func mustRaw(t *testing.T, v interface{}) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	return raw
}

func TestDispatch_Initialize_ReturnsIDNameSettings(t *testing.T) {
	p := &testPlugin{}
	r := Dispatch(context.Background(), p, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "initialize"})
	reply, ok := r.(ReplyResult)
	if !ok {
		t.Fatalf("want ReplyResult, got %T", r)
	}
	result := reply.Response.Result.(map[string]interface{})
	if result["id"] != "test" {
		t.Errorf("want id=test, got %v", result["id"])
	}
	if result["name"] != "Test Plugin" {
		t.Errorf("want name set, got %v", result["name"])
	}
	if result["iconSvg"] != "<svg/>" {
		t.Errorf("want the plugin icon, got %v", result["iconSvg"])
	}
	settings, ok := result["settings"].([]PluginSetting)
	if !ok || len(settings) != 0 {
		t.Errorf("want empty settings slice, got %v", result["settings"])
	}
}

// #416: without these the host treats the sidecar as legacy contract v1 -- a
// warning on every boot, and no way to refuse an incompatible sidecar at the
// handshake instead of at the first call.
func TestDispatch_Initialize_AdvertisesContractVersionAndCapabilities(t *testing.T) {
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "initialize"})
	result := r.(ReplyResult).Response.Result.(map[string]interface{})

	if result["protocolVersion"] != SidecarProtocolVersion {
		t.Errorf("want protocolVersion=%d, got %v", SidecarProtocolVersion, result["protocolVersion"])
	}
	caps, ok := result["capabilities"].(map[string]bool)
	if !ok {
		t.Fatalf("want a capabilities map, got %T", result["capabilities"])
	}
	// testPlugin implements StreamingPlugin; nothing routes openChannel.
	want := map[string]bool{"discover": true, "invoke": true, "invokeStream": true, "channels": false}
	for k, v := range want {
		if caps[k] != v {
			t.Errorf("capability %s: want %v, got %v", k, v, caps[k])
		}
	}
}

// A plugin without StreamingPlugin has to say so, or the host round-trips to
// a method-not-found for every server-streaming method in the tree.
func TestDispatch_Initialize_CapabilitiesFollowTheOptionalInterfaces(t *testing.T) {
	r := Dispatch(context.Background(), minimalPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "initialize"})
	result := r.(ReplyResult).Response.Result.(map[string]interface{})

	caps := result["capabilities"].(map[string]bool)
	if caps["invokeStream"] {
		t.Errorf("a plugin without StreamingPlugin must not advertise invokeStream")
	}
	if result["iconSvg"] != "" {
		t.Errorf("a plugin without IconPlugin reports no icon, got %v", result["iconSvg"])
	}
}

// The contract replies with the bare string; the map was this SDK's own
// invention, and a host using ping as a liveness probe reads it as malformed.
func TestDispatch_Ping_ReturnsTheBarePongString(t *testing.T) {
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "ping"})
	reply := r.(ReplyResult)
	if reply.Response.Result != "pong" {
		t.Errorf("want the bare pong string, got %v", reply.Response.Result)
	}
}

// The host sends serverUrl / showInternalServices and reads anything but a
// bare array as "no services, try the next plugin" -- so the old
// endpoint/refresh params plus the services envelope discovered nothing, ever.
func TestDispatch_Discover_ReturnsABareServicesArray(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"serverUrl": "x://host", "showInternalServices": false})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "discover", Params: params})
	reply := r.(ReplyResult)
	services, ok := reply.Response.Result.([]ServiceInfo)
	if !ok {
		t.Fatalf("want a bare []ServiceInfo, got %T", reply.Response.Result)
	}
	if len(services) != 1 {
		t.Errorf("want 1 service, got %d", len(services))
	}
}

// What the plugin receives has to be what the host sent: the params struct
// read endpoint, so every Discover ran against an empty URL.
func TestDispatch_Discover_PassesTheServerUrlThrough(t *testing.T) {
	seen := &urlCapturingPlugin{}
	params := mustRaw(t, map[string]interface{}{"serverUrl": "x://host", "showInternalServices": true})
	Dispatch(context.Background(), seen, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "discover", Params: params})

	if seen.serverURL != "x://host" {
		t.Errorf("want the URL the host sent, got %q", seen.serverURL)
	}
	if !seen.showInternal {
		t.Errorf("showInternalServices did not arrive")
	}
}

func TestDispatch_Invoke_EchoesBody(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"method": "Echo", "jsonMessages": []string{"hi"}})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "invoke", Params: params})
	reply := r.(ReplyResult)
	result := reply.Response.Result.(InvokeResult)
	if result.Status != "OK" {
		t.Errorf("want OK, got %s", result.Status)
	}
	if result.Response != `{"echoed":"hi"}` {
		t.Errorf("response mismatch: %q", result.Response)
	}
}

func TestDispatch_InvokeErrors_SurfaceAsInternal(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"method": "fail"})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "invoke", Params: params})
	reply := r.(ReplyResult)
	if reply.Response.Error == nil {
		t.Fatalf("want error response")
	}
	if reply.Response.Error.Code != ErrInternal {
		t.Errorf("want internal code, got %d", reply.Response.Error.Code)
	}
	if reply.Response.Error.Message != "boom" {
		t.Errorf("want message=boom, got %q", reply.Response.Error.Message)
	}
}

func TestDispatch_InvokeStream_KeepsTheHostsStreamID(t *testing.T) {
	// The host mints the id and subscribes to it before sending the request,
	// so a self-minted one publishes every frame where nobody is listening.
	params := mustRaw(t, map[string]interface{}{"streamId": "host-minted-42", "method": "Watch"})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "invokeStream", Params: params})
	stream, ok := r.(StreamResult)
	if !ok {
		t.Fatalf("want StreamResult, got %T", r)
	}
	if stream.StreamID != "host-minted-42" {
		t.Errorf("want the streamId the host minted, got %q", stream.StreamID)
	}
	ack := stream.Ack.Result.(map[string]string)
	if ack["streamId"] != stream.StreamID {
		t.Errorf("ack streamId mismatch")
	}
	frames := []string{}
	for f := range stream.Stream {
		frames = append(frames, f)
	}
	if len(frames) != 2 || frames[0] != "frame-1" || frames[1] != "frame-2" {
		t.Errorf("frames mismatch: %v", frames)
	}
}

func TestDispatch_Shutdown_CallsHookAndSignals(t *testing.T) {
	p := &testPlugin{}
	r := Dispatch(context.Background(), p, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "shutdown"})
	if _, ok := r.(ShutdownResult); !ok {
		t.Fatalf("want ShutdownResult, got %T", r)
	}
	if !p.shutdownCalled {
		t.Errorf("shutdown hook should have been called")
	}
}

func TestDispatch_UnknownMethod_ReturnsMethodNotFound(t *testing.T) {
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "wat"})
	reply := r.(ReplyResult)
	if reply.Response.Error == nil || reply.Response.Error.Code != ErrMethodNotFound {
		t.Errorf("want method-not-found error, got %+v", reply.Response.Error)
	}
}

// Records what Discover actually received.
type urlCapturingPlugin struct {
	serverURL    string
	showInternal bool
}

func (urlCapturingPlugin) ID() string   { return "cap" }
func (urlCapturingPlugin) Name() string { return "Cap" }

func (p *urlCapturingPlugin) Discover(_ context.Context, serverURL string, showInternal bool) ([]ServiceInfo, error) {
	p.serverURL = serverURL
	p.showInternal = showInternal
	return nil, nil
}

func (urlCapturingPlugin) Invoke(_ context.Context, _ InvokeRequest) (InvokeResult, error) {
	return InvokeResult{}, nil
}

// Minimal plugin without optional capabilities — proves InvokeStream
// 404s when the plugin doesn't implement StreamingPlugin.
type minimalPlugin struct{}

func (minimalPlugin) ID() string   { return "min" }
func (minimalPlugin) Name() string { return "Min" }
func (minimalPlugin) Discover(_ context.Context, _ string, _ bool) ([]ServiceInfo, error) {
	return nil, nil
}
func (minimalPlugin) Invoke(_ context.Context, _ InvokeRequest) (InvokeResult, error) {
	return NewOKResult("{}"), nil
}

func TestDispatch_InvokeStream_OnPluginWithoutCapability_Returns404(t *testing.T) {
	r := Dispatch(context.Background(), minimalPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "invokeStream"})
	reply := r.(ReplyResult)
	if reply.Response.Error == nil || reply.Response.Error.Code != ErrMethodNotFound {
		t.Errorf("want method-not-found, got %+v", reply.Response.Error)
	}
}
