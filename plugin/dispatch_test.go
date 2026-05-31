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

func (testPlugin) ID() string   { return "test" }
func (testPlugin) Name() string { return "Test Plugin" }

func (testPlugin) Discover(_ context.Context, _ string, _ bool) ([]ServiceInfo, error) {
	return []ServiceInfo{NewServiceInfo("Svc").WithMethods(UnaryMethod("M"))}, nil
}

func (testPlugin) Invoke(_ context.Context, req InvokeRequest) (InvokeResult, error) {
	if req.Method == "fail" {
		return InvokeResult{}, errors.New("boom")
	}
	first := ""
	if len(req.Body) > 0 {
		first = req.Body[0]
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
	settings, ok := result["settings"].([]PluginSetting)
	if !ok || len(settings) != 0 {
		t.Errorf("want empty settings slice, got %v", result["settings"])
	}
}

func TestDispatch_Ping_ReturnsPongTrue(t *testing.T) {
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "ping"})
	reply := r.(ReplyResult)
	result := reply.Response.Result.(map[string]bool)
	if !result["pong"] {
		t.Errorf("want pong=true")
	}
}

func TestDispatch_Discover_ReturnsServicesArray(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"endpoint": "x", "refresh": false})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "discover", Params: params})
	reply := r.(ReplyResult)
	result := reply.Response.Result.(map[string]interface{})
	services := result["services"].([]ServiceInfo)
	if len(services) != 1 {
		t.Errorf("want 1 service, got %d", len(services))
	}
}

func TestDispatch_Invoke_EchoesBody(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"method": "Echo", "body": []string{"hi"}})
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

func TestDispatch_InvokeStream_AckPlusIterableFrames(t *testing.T) {
	params := mustRaw(t, map[string]interface{}{"method": "Watch"})
	r := Dispatch(context.Background(), &testPlugin{}, Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "invokeStream", Params: params})
	stream, ok := r.(StreamResult)
	if !ok {
		t.Fatalf("want StreamResult, got %T", r)
	}
	if stream.StreamID == "" {
		t.Errorf("streamId should be non-empty")
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
