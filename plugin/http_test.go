package plugin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type httpTestPlugin struct{}

func (httpTestPlugin) ID() string   { return "http-test" }
func (httpTestPlugin) Name() string { return "HTTP Test" }

func (httpTestPlugin) Discover(_ context.Context, _ string, _ bool) ([]ServiceInfo, error) {
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

	body := bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"invoke","params":{"endpoint":"x","service":"Svc","method":"Echo","body":[],"streaming":false,"metadata":{}}}`))
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
