package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
)

// Run drives `p` over the NDJSON JSON-RPC contract on stdin/stdout
// and returns when the host sends `shutdown` or stdin closes.
//
// One JSON object per line, no framing headers (mirrors the Python,
// Rust and Node SDKs). Server-initiated frames are written as
// notifications; replies always carry the request id back.
func Run(ctx context.Context, p BowirePlugin) error {
	return runWithIO(ctx, p, os.Stdin, os.Stdout)
}

// runWithIO is the testable form of Run — used by the unit tests so
// they don't have to touch process stdin/stdout.
func runWithIO(ctx context.Context, p BowirePlugin, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	// Default token size is 64KB which may be too small for large
	// invoke bodies; allow up to 16MB per line. The host enforces
	// its own ceiling on outbound frames.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	writer := newFrameWriter(out)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			// Host violated the framing — skip the line and keep going.
			continue
		}

		outcome := Dispatch(ctx, p, req)

		switch r := outcome.(type) {
		case ReplyResult:
			if err := writer.write(r.Response); err != nil {
				return err
			}
		case ShutdownResult:
			if err := writer.write(r.Response); err != nil {
				return err
			}
			return nil
		case StreamResult:
			if err := writer.write(r.Ack); err != nil {
				return err
			}
			go pumpStream(r.StreamID, r.Stream, writer)
		}
	}

	return scanner.Err()
}

// frameWriter serialises writes to the shared output stream so the
// background stream-pump can't interleave bytes with the main loop's
// replies.
type frameWriter struct {
	out io.Writer
	enc *json.Encoder
}

func newFrameWriter(out io.Writer) *frameWriter {
	return &frameWriter{out: out, enc: json.NewEncoder(out)}
}

func (w *frameWriter) write(v interface{}) error {
	// json.Encoder.Encode appends '\n' on its own — perfect NDJSON.
	return w.enc.Encode(v)
}

func pumpStream(streamID string, stream <-chan string, w *frameWriter) {
	defer func() {
		_ = w.write(Notification{
			JSONRPC: "2.0",
			Method:  "$/stream/end",
			Params:  map[string]string{"streamId": streamID},
		})
	}()
	for message := range stream {
		_ = w.write(Notification{
			JSONRPC: "2.0",
			Method:  "$/stream/data",
			Params: map[string]string{
				"streamId": streamID,
				"message":  message,
			},
		})
	}
}
