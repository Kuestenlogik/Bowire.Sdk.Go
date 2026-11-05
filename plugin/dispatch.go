package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
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

// The wire shapes the host actually sends. See the Bowire repo's
// docs/architecture/sidecar-plugins.md.
type invokeParams struct {
	StreamID             string            `json:"streamId"`
	ServerURL            string            `json:"serverUrl"`
	Service              string            `json:"service"`
	Method               string            `json:"method"`
	JSONMessages         []string          `json:"jsonMessages"`
	ShowInternalServices bool              `json:"showInternalServices"`
	Metadata             map[string]string `json:"metadata"`
	Settings             json.RawMessage   `json:"settings"`
}

type discoverParams struct {
	ServerURL            string          `json:"serverUrl"`
	ShowInternalServices bool            `json:"showInternalServices"`
	Settings             json.RawMessage `json:"settings"`
}

type openChannelParams struct {
	ChannelID            string            `json:"channelId"`
	ServerURL            string            `json:"serverUrl"`
	Service              string            `json:"service"`
	Method               string            `json:"method"`
	ShowInternalServices bool              `json:"showInternalServices"`
	Metadata             map[string]string `json:"metadata"`
	Settings             json.RawMessage   `json:"settings"`
}

// channelParams are the params of channel.send and channel.close. The
// message is kept raw: the host sends a string, and anything else is
// passed on as its JSON text.
type channelParams struct {
	ChannelID string          `json:"channelId"`
	Message   json.RawMessage `json:"message"`
}

// SidecarProtocolVersion is the sidecar wire-contract version this SDK
// speaks (#416). A sidecar that advertises none is tolerated as contract v1
// — with a warning in the host log on every boot; one outside the host's
// range is refused at the handshake rather than at the first call.
const SidecarProtocolVersion = 1

// capabilitiesOf reports what this plugin can answer, read off the optional
// interfaces it implements. The host skips a call whose flag is false:
// `channels: false` short-circuits openChannel without a round-trip.
func capabilitiesOf(p BowirePlugin) map[string]bool {
	_, streams := p.(StreamingPlugin)
	_, channels := p.(ChannelPlugin)
	return map[string]bool{
		"discover":     true,
		"invoke":       true,
		"invokeStream": streams,
		"channels":     channels,
	}
}

// Dispatcher runs JSON-RPC requests against one plugin and holds what
// outlives a single request: the open duplex channels. Both Run (stdio)
// and RunHTTP route every inbound envelope through one, so contract
// semantics live in one place.
type Dispatcher struct {
	plugin   BowirePlugin
	channels *channelRegistry
}

// NewDispatcher builds a Dispatcher for p. notify is where channel frames
// go — `$/channel/data` and `$/channel/closed` — and may be called from
// any goroutine; the transport serialises it onto its wire.
func NewDispatcher(p BowirePlugin, notify func(Notification)) *Dispatcher {
	return &Dispatcher{plugin: p, channels: newChannelRegistry(notify)}
}

// CloseChannels closes every open channel. The runtimes call it when the
// host's wire is gone; shutdown does it on its own.
func (d *Dispatcher) CloseChannels() { d.channels.closeAll() }

// Dispatch runs a single JSON-RPC request and returns the
// transport-agnostic outcome.
func (d *Dispatcher) Dispatch(ctx context.Context, req Request) DispatchResult {
	id := req.ID
	p := d.plugin

	switch req.Method {
	case "shutdown":
		// Every open channel ends first, so the host's read streams end
		// before the process does.
		d.channels.closeAll()
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
		reply := map[string]interface{}{
			"id":              p.ID(),
			"name":            p.Name(),
			"iconSvg":         iconSvg,
			"settings":        settings,
			"protocolVersion": SidecarProtocolVersion,
			"capabilities":    capabilitiesOf(p),
		}
		// Kuestenlogik/Bowire#1003 - a form the host would refuse fails the
		// handshake, naming why, rather than silently never appearing.
		if mp, ok := p.(ManualEntryPlugin); ok {
			if form := mp.ManualEntry(); form != nil {
				if problems := ManualEntryProblems(*form); len(problems) > 0 {
					return ReplyResult{Response: errResponse(id, ErrInternal,
						"manualEntry: "+strings.Join(problems, "; "))}
				}
				if form.Fields == nil {
					form.Fields = []ManualField{}
				}
				reply["manualEntry"] = form
			}
		}
		return ReplyResult{Response: okResponse(id, reply)}

	case "ping":
		// The contract's reply is the bare string; a host using ping as a
		// liveness probe reads anything else as a malformed answer.
		return ReplyResult{Response: okResponse(id, "pong")}

	case "discover":
		var dp discoverParams
		if err := unmarshalParams(req.Params, &dp); err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		services, err := p.Discover(ctx, DiscoverRequest{
			ServerURL:            dp.ServerURL,
			ShowInternalServices: dp.ShowInternalServices,
			Settings:             settingValuesOf(p, dp.Settings),
		})
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
		result, err := p.Invoke(ctx, ip.toRequest(p))
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
		stream, err := sp.InvokeStream(ctx, ip.toRequest(p))
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		// The host mints the id and subscribes to it *before* sending the
		// request, so a self-minted one publishes every frame into a channel
		// nobody reads. newStreamID stays for a caller that omits it.
		streamID := ip.StreamID
		if streamID == "" {
			streamID = newStreamID()
		}
		return StreamResult{
			Ack:      okResponse(id, map[string]string{"streamId": streamID}),
			StreamID: streamID,
			Stream:   stream,
		}

	case "openChannel":
		cp, ok := p.(ChannelPlugin)
		if !ok {
			return ReplyResult{Response: errResponse(id, ErrMethodNotFound, "openChannel not supported")}
		}
		var op openChannelParams
		if err := unmarshalParams(req.Params, &op); err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		// Host-minted, like the streamId: the host subscribed to this id
		// before sending the request.
		channelID := op.ChannelID
		if channelID == "" {
			channelID = newStreamID()
		}
		channel, err := d.channels.open(channelID)
		if err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		if op.Metadata == nil {
			op.Metadata = map[string]string{}
		}
		err = cp.OpenChannel(ctx, ChannelRequest{
			ServerURL:            op.ServerURL,
			Service:              op.Service,
			Method:               op.Method,
			ShowInternalServices: op.ShowInternalServices,
			Metadata:             op.Metadata,
			Settings:             settingValuesOf(p, op.Settings),
		}, channel)
		if err != nil {
			d.channels.discard(channel)
			return ReplyResult{Response: errResponse(id, ErrInternal, err.Error())}
		}
		return ReplyResult{Response: okResponse(id, map[string]string{"channelId": channelID})}

	case "channel.send":
		var cp channelParams
		if err := unmarshalParams(req.Params, &cp); err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		channel := d.channels.get(cp.ChannelID)
		if channel == nil || !channel.isOpen() {
			return channelGone(id, cp.ChannelID)
		}
		channel.deliver(messageText(cp.Message))
		return ReplyResult{Response: okResponse(id, struct{}{})}

	case "channel.close":
		var cp channelParams
		if err := unmarshalParams(req.Params, &cp); err != nil {
			return ReplyResult{Response: errResponse(id, ErrInvalidParams, err.Error())}
		}
		channel := d.channels.get(cp.ChannelID)
		if channel == nil {
			return channelGone(id, cp.ChannelID)
		}
		// A close that races one already under way from the plugin's side
		// succeeds without a second round: the channel is ending either way.
		channel.end()
		return ReplyResult{Response: okResponse(id, struct{}{})}

	default:
		return ReplyResult{Response: errResponse(id, ErrMethodNotFound, "unknown method: "+req.Method)}
	}
}

func unmarshalParams(raw json.RawMessage, v interface{}) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}

// channelGone is the error the host maps onto IBowireChannel.SendAsync
// returning false.
func channelGone(id json.RawMessage, channelID string) DispatchResult {
	if channelID == "" {
		channelID = "(none)"
	}
	return ReplyResult{Response: errResponse(id, ErrChannelGone, "channel "+channelID+" is closed")}
}

// messageText is the frame as the plugin sees it: the string the host
// sent, or the JSON text of anything else.
func messageText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// settingValuesOf reads the values the host sent with this call against
// what the plugin declares, so an absent key resolves to the declared
// default. The declarations are asked for per call rather than captured at
// initialize: a plugin is free to compute them.
func settingValuesOf(p BowirePlugin, raw json.RawMessage) SettingValues {
	var declared []PluginSetting
	if sp, ok := p.(SettingsPlugin); ok {
		declared = sp.Settings()
	}
	return settingValuesFromWire(raw, declared)
}

func parseInvoke(raw json.RawMessage) (invokeParams, error) {
	var ip invokeParams
	if err := unmarshalParams(raw, &ip); err != nil {
		return ip, err
	}
	if ip.JSONMessages == nil {
		ip.JSONMessages = []string{}
	}
	if ip.Metadata == nil {
		ip.Metadata = map[string]string{}
	}
	return ip, nil
}

func (ip invokeParams) toRequest(p BowirePlugin) InvokeRequest {
	return InvokeRequest{
		ServerURL:            ip.ServerURL,
		Service:              ip.Service,
		Method:               ip.Method,
		JSONMessages:         ip.JSONMessages,
		ShowInternalServices: ip.ShowInternalServices,
		Metadata:             ip.Metadata,
		Settings:             settingValuesOf(p, ip.Settings),
	}
}

func newStreamID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
