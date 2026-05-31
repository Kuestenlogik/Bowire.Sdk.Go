package plugin

import "context"

// InvokeRequest carries the params of an `invoke` JSON-RPC call.
// Mirrors the Python/Rust/Node Invoke surface but bundled as a
// single struct — more idiomatic Go than a 6-arg method.
type InvokeRequest struct {
	Endpoint  string
	Service   string
	Method    string
	Body      []string
	Streaming bool
	Metadata  map[string]string
}

// BowirePlugin is the minimum contract every Bowire sidecar implements.
//
// Optional capabilities (streaming, settings, shutdown hook, duplex
// channels) are exposed as additional interfaces — idiomatic Go.
// The dispatcher uses type assertions to discover them at runtime,
// so a plugin only embeds what it actually supports.
type BowirePlugin interface {
	ID() string
	Name() string
	Discover(ctx context.Context, endpoint string, refresh bool) ([]ServiceInfo, error)
	Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error)
}

// StreamingPlugin is the optional capability for server-streaming
// methods. The returned channel is owned by the plugin and must be
// closed when the stream ends — the runtime emits a single
// `$/stream/end` notification after the channel drains.
type StreamingPlugin interface {
	InvokeStream(ctx context.Context, req InvokeRequest) (<-chan string, error)
}

// SettingsPlugin is the optional capability for user-configurable
// settings the host renders on the protocol tab. Called once per
// initialize cycle.
type SettingsPlugin interface {
	Settings() []PluginSetting
}

// ShutdownHook is the optional capability called once when the host
// sends a `shutdown` request. Use it to release resources (open
// connections, file handles).
type ShutdownHook interface {
	Shutdown(ctx context.Context) error
}

// ChannelHandle is the optional duplex pipe a plugin can use when
// implementing ChannelPlugin. `Send` pushes a message to the
// workbench; `OnMessage` registers a handler for inbound messages;
// `OnClose` registers a handler invoked when the workbench closes
// the channel.
type ChannelHandle interface {
	Send(message string) error
	OnMessage(handler func(message string) error)
	OnClose(handler func())
}

// ChannelPlugin is the optional capability for duplex / bidirectional
// transports (WebSocket, SignalR, MQTT). The handler is given a
// ChannelHandle wired to the workbench; the call returns when the
// channel closes.
type ChannelPlugin interface {
	OpenChannel(ctx context.Context, endpoint, service, method string, metadata map[string]string, channel ChannelHandle) error
}
