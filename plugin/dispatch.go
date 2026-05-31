package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
)

// DispatchResult is the discriminated outcome of a single inbound
// JSON-RPC request. Transport runtimes (stdio, http) translate
// each variant into the right wire effect.
type DispatchResult interface{ isDispatchResult() }

// ReplyResult means: write Response back on the same wire and stop.
type ReplyResult struct{ Response Response }

// StreamResult means: write Ack back, then pump frames from Stream
// as `$/stream/data` notifications until the channel closes, then
// emit one `$/stream/end` notification.
type StreamResult struct {
	Ack      Response
	StreamID string
	Stream   <-chan string
}

// ShutdownResult means: write Response back and signal the runtime
// to stop accepting new requests.
type ShutdownResult struct{ Response Response }

func (ReplyResult) isDispatchResult()    {}
func (StreamResult) isDispatchResult()   {}
func (ShutdownResult) isDispatchResult() {}

type invokeParams struct {
	Endpoint  string            `json:"endpoint"`
	Service   string            `json:"service"`
	Method    string            `json:"method"`
	Body      []string          `json:"body"`
	Streaming bool              `json:"streaming"`
	Metadata  map[string]string `json:"metadata"`
}

type discoverParams struct {
	Endpoint string `json:"endpoint"`
	Refresh  bool   `json:"refresh"`
}

// Dispatch runs a single JSON-RPC request against `plugin` and
// returns the transport-agnostic outcome. Both Run (stdio) and
// RunHTTP route every inbound envelope through this helper, so
// contract semantics live in one place.
func Dispatch(ctx context.Context, p BowirePlugin, req Request) DispatchResult {
	id := req.ID

	switch req.Method {
	case "shutdown":
		if sh, ok := p.(ShutdownHook); ok {
			_ = sh.Shutdown(ctx)
		}
		return ShutdownResult{Response: okResponse(id, struct{}{})}

	case "initialize":
		var settings []PluginSetting
		if sp, ok := p.(SettingsPlugin); ok {
			settings = sp.Settings()
		}
		if settings == nil {
			settings = []PluginSetting{}
		}
		return ReplyResult{Response: okResponse(id, map[string]interface{}{
			"id":       p.ID(),
			"name":     p.Name(),
			"settings": settings,
		})}

	case "ping":
		return ReplyResult{Response: okResponse(id, map[string]bool{"pong": true})}

	case "discover":
		var dp discoverParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &dp); err != nil {
				return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
			}
		}
		services, err := p.Discover(ctx, dp.Endpoint, dp.Refresh)
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		if services == nil {
			services = []ServiceInfo{}
		}
		return ReplyResult{Response: okResponse(id, map[string]interface{}{"services": services})}

	case "invoke":
		ip, err := parseInvoke(req.Params)
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		result, err := p.Invoke(ctx, ip.toRequest())
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		return ReplyResult{Response: okResponse(id, result)}

	case "invokeStream":
		sp, ok := p.(StreamingPlugin)
		if !ok {
			return ReplyResult{Response: errResponse(id, ErrMethodNotFound, "invokeStream not supported")}
		}
		ip, err := parseInvoke(req.Params)
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		stream, err := sp.InvokeStream(ctx, ip.toRequest())
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		streamID := newStreamID()
		return StreamResult{
			Ack:      okResponse(id, map[string]string{"streamId": streamID}),
			StreamID: streamID,
			Stream:   stream,
		}

	default:
		return ReplyResult{Response: errResponse(id, ErrMethodNotFound, "unknown method: "+req.Method)}
	}
}

func parseInvoke(raw json.RawMessage) (invokeParams, error) {
	var ip invokeParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &ip); err != nil {
			return ip, err
		}
	}
	if ip.Body == nil {
		ip.Body = []string{}
	}
	if ip.Metadata == nil {
		ip.Metadata = map[string]string{}
	}
	return ip, nil
}

func (ip invokeParams) toRequest() InvokeRequest {
	return InvokeRequest{
		Endpoint:  ip.Endpoint,
		Service:   ip.Service,
		Method:    ip.Method,
		Body:      ip.Body,
		Streaming: ip.Streaming,
		Metadata:  ip.Metadata,
	}
}

func newStreamID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
