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

// The wire shapes the host actually sends. These used to read endpoint /
// refresh / body / streaming — names this SDK invented — so discover arrived
// with an empty URL and invoke with no messages, every time, silently.
type invokeParams struct {
	StreamID             string            `json:"streamId"`
	ServerURL            string            `json:"serverUrl"`
	Service              string            `json:"service"`
	Method               string            `json:"method"`
	JSONMessages         []string          `json:"jsonMessages"`
	ShowInternalServices bool              `json:"showInternalServices"`
	Metadata             map[string]string `json:"metadata"`
}

type discoverParams struct {
	ServerURL            string `json:"serverUrl"`
	ShowInternalServices bool   `json:"showInternalServices"`
}

// SidecarProtocolVersion is the sidecar wire-contract version this SDK
// speaks (#416). A sidecar that advertises none is tolerated as contract v1
// — with a warning in the host log on every boot; one outside the host's
// range is refused at the handshake rather than at the first call.
const SidecarProtocolVersion = 1

// capabilitiesOf reports what this plugin can answer, read off the optional
// interfaces it implements. The host skips a call whose flag is false.
//
// channels is always false: ChannelPlugin exists in this package, but the
// dispatcher has no openChannel case, so nothing routes to it. Advertising
// the capability would earn a method-not-found for every duplex method an
// operator opens. Routing it is separate work.
func capabilitiesOf(p BowirePlugin) map[string]bool {
	_, streams := p.(StreamingPlugin)
	return map[string]bool{
		"discover":     true,
		"invoke":       true,
		"invokeStream": streams,
		"channels":     false,
	}
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
		iconSvg := ""
		if ip, ok := p.(IconPlugin); ok {
			iconSvg = ip.IconSvg()
		}
		return ReplyResult{Response: okResponse(id, map[string]interface{}{
			"id":              p.ID(),
			"name":            p.Name(),
			"iconSvg":         iconSvg,
			"settings":        settings,
			"protocolVersion": SidecarProtocolVersion,
			"capabilities":    capabilitiesOf(p),
		})}

	case "ping":
		// The contract's reply is the bare string. `{"pong": true}` was this
		// SDK's own invention, and a host using ping as a liveness probe
		// reads it as a malformed answer.
		return ReplyResult{Response: okResponse(id, "pong")}

	case "discover":
		var dp discoverParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &dp); err != nil {
				return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
			}
		}
		services, err := p.Discover(ctx, dp.ServerURL, dp.ShowInternalServices)
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		if services == nil {
			services = []ServiceInfo{}
		}
		// A bare array: the host reads anything else as "no services, try the
		// next plugin" and moves on without a word.
		return ReplyResult{Response: okResponse(id, services)}

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
		// The host mints the id and subscribes to it *before* sending the
		// request, so a self-minted one publishes every frame into a channel
		// nobody reads — which is how streaming looked empty rather than
		// broken. newStreamID stays for a caller that omits it.
		streamID := ip.StreamID
		if streamID == "" {
			streamID = newStreamID()
		}
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
	if ip.JSONMessages == nil {
		ip.JSONMessages = []string{}
	}
	if ip.Metadata == nil {
		ip.Metadata = map[string]string{}
	}
	return ip, nil
}

func (ip invokeParams) toRequest() InvokeRequest {
	return InvokeRequest{
		ServerURL:            ip.ServerURL,
		Service:              ip.Service,
		Method:               ip.Method,
		JSONMessages:         ip.JSONMessages,
		ShowInternalServices: ip.ShowInternalServices,
		Metadata:             ip.Metadata,
	}
}

func newStreamID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
