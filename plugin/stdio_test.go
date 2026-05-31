package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunStdio_HandlesPingThenShutdown(t *testing.T) {
	in := strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"shutdown"}` + "\n" +
			// This third line should never reach the plugin — shutdown stops the loop.
			`{"jsonrpc":"2.0","id":3,"method":"ping"}` + "\n",
	)
	var out bytes.Buffer

	if err := runWithIO(context.Background(), &testPlugin{}, in, &out); err != nil {
		t.Fatalf("runWithIO: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 reply lines, got %d: %q", len(lines), out.String())
	}

	var ping, shutdown Response
	if err := json.Unmarshal([]byte(lines[0]), &ping); err != nil {
		t.Fatalf("decode ping: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &shutdown); err != nil {
		t.Fatalf("decode shutdown: %v", err)
	}

	if ping.Result == nil {
		t.Errorf("ping reply missing result")
	}
	if shutdown.Result == nil {
		t.Errorf("shutdown reply missing result")
	}
}

func TestRunStdio_SkipsMalformedLines(t *testing.T) {
	in := strings.NewReader(
		"not-json\n" +
			`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"shutdown"}` + "\n",
	)
	var out bytes.Buffer

	if err := runWithIO(context.Background(), &testPlugin{}, in, &out); err != nil {
		t.Fatalf("runWithIO: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("want 2 lines (malformed dropped), got %d: %q", len(lines), out.String())
	}
}
