package plugin

import "context"

// DiscoverRequest carries the params of a `discover` JSON-RPC call.
type DiscoverRequest struct {
	ServerURL            string
	ShowInternalServices bool
	// Settings are the values of the plugin's declared settings, sent fresh
	// with this call (Kuestenlogik/Bowire#693).
	Settings SettingValues
}

// InvokeRequest carries the params of an `invoke` or `invokeStream`
// JSON-RPC call, bundled as a single struct rather than a six-argument
// method.
//
// The field names are the host's vocabulary: serverUrl, jsonMessages and
// showInternalServices on the wire. See the Bowire repo's
// docs/architecture/sidecar-plugins.md for the contract.
type InvokeRequest struct {
	ServerURL            string
	Service              string
	Method               string
	JSONMessages         []string
	ShowInternalServices bool
	Metadata             map[string]string
	// Settings are the values of the plugin's declared settings, sent fresh
	// with this call (Kuestenlogik/Bowire#693).
	Settings SettingValues
}

// ChannelRequest carries the params of an `openChannel` JSON-RPC call.
type ChannelRequest struct {
	ServerURL            string
	Service              string
	Method               string
	ShowInternalServices bool
	Metadata             map[string]string
	// Settings are the values of the plugin's declared settings, sent fresh
	// with this call (Kuestenlogik/Bowire#693).
	Settings SettingValues
}

// BowirePlugin is the minimum contract every Bowire sidecar implements.
//
// Optional capabilities (streaming, settings, shutdown hook, duplex
// channels) are exposed as additional interfaces — idiomatic Go.
// The dispatcher uses type assertions to discover them at runtime,
// so a plugin only implements what it actually supports, and the
// initialize reply's capabilities say exactly that.
type BowirePlugin interface {
	ID() string
	Name() string
	Discover(ctx context.Context, req DiscoverRequest) ([]ServiceInfo, error)
	Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error)
}

// IconPlugin is the optional capability for a protocol-tab icon. The host
// falls back to a generic plug glyph when a sidecar reports none.
type IconPlugin interface {
	IconSvg() string
}

// StreamingPlugin is the optional capability for server-streaming
// methods. The returned channel is owned by the plugin and must be
// closed when the stream ends — the runtime emits a single
// `$/stream/end` notification after the channel drains. Over http ctx is
// done when the host that started the stream has left; stop producing and
// close the channel then.
type StreamingPlugin interface {
	InvokeStream(ctx context.Context, req InvokeRequest) (<-chan string, error)
}

// SettingsPlugin is the optional capability for user-configurable
// settings. The declarations go to the host with the initialize reply and
// render in the workbench's Settings dialog; the values come back with
// every call as the Settings field of the request. The dispatcher asks for
// the declarations again per call, to resolve an unset key to its default.
type SettingsPlugin interface {
	Settings() []PluginSetting
}

// ShutdownHook is the optional capability called once when the sidecar
// itself stops, after every open channel has closed: over stdio when the
// host sends `shutdown` or closes stdin, over http when HTTPHandle.Close stops the server.
// A host that leaves an http sidecar does not trigger it. Use it to release
// resources (open connections, file handles).
type ShutdownHook interface {
	Shutdown(ctx context.Context) error
}

// ChannelHandle is one duplex channel between the plugin and the
// workbench. The runtime keys it by the id the host minted, so a plugin
// keeps the handle and nothing else; several can be open at once.
//
// Closing runs the same way whichever side starts it: the OnClose handlers
// run first (they may still Send a last frame), then `$/channel/closed`
// goes out and ends the host's read stream — exactly once, even when both
// sides close at the same time.
type ChannelHandle interface {
	// ID is the host's id for this channel.
	ID() string
	// Send pushes a frame to the workbench as `$/channel/data`. It returns
	// ErrChannelClosed once the channel has closed.
	Send(message string) error
	// OnMessage registers a handler for the frames the workbench sends.
	// Handlers run in order on the runtime's request loop, so frames
	// arrive in the order the host sent them; a handler that does slow
	// work should hand it to a goroutine.
	OnMessage(handler func(message string))
	// OnClose registers a handler run once when the channel closes, from
	// either side.
	OnClose(handler func())
	// Close closes the channel from the plugin's side. It returns once
	// `$/channel/closed` is out; a call while a close is already under
	// way returns at once.
	Close()
}

// ChannelPlugin is the optional capability for duplex / bidirectional
// methods (WebSocket, SignalR, MQTT and the like). OpenChannel sets the
// channel up — registers its handlers, starts whatever pumps frames — and
// returns; the handle lives on until either side closes it. An error
// refuses the open, and the host sees it as a failed open.
type ChannelPlugin interface {
	OpenChannel(ctx context.Context, req ChannelRequest, channel ChannelHandle) error
}
