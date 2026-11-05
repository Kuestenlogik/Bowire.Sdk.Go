# Bowire.Sdk.Go

Go SDK for authoring [Bowire](https://bowire.io) sidecar plugins.

Bowire hosts protocol plugins as .NET assemblies by default. The
sidecar bridge widens that surface — any language that speaks
JSON-RPC over stdin/stdout (or HTTP/SSE) can ship a Bowire plugin.
This module is the Go side of that bridge.

## Install

```bash
go get github.com/Kuestenlogik/Bowire.Sdk.Go/plugin
```

Requires Go 1.22 or newer.

## Author a plugin

```go
package main

import (
    "context"

    "github.com/Kuestenlogik/Bowire.Sdk.Go/plugin"
)

type EchoPlugin struct{}

func (EchoPlugin) ID() string   { return "echo" }
func (EchoPlugin) Name() string { return "Echo" }

func (EchoPlugin) Discover(_ context.Context, _ plugin.DiscoverRequest) ([]plugin.ServiceInfo, error) {
    return []plugin.ServiceInfo{
        plugin.NewServiceInfo("EchoService").WithMethods(
            plugin.UnaryMethod("Echo"),
        ),
    }, nil
}

func (EchoPlugin) Invoke(_ context.Context, req plugin.InvokeRequest) (plugin.InvokeResult, error) {
    first := ""
    if len(req.JSONMessages) > 0 {
        first = req.JSONMessages[0]
    }
    return plugin.NewOKResult(`{"echoed":"` + first + `"}`), nil
}

func main() {
    _ = plugin.Run(context.Background(), EchoPlugin{})
}
```

The minimum contract is `ID`, `Name`, `Discover`, and `Invoke`.
Optional capabilities are declared by implementing additional
interfaces — idiomatic Go:

| Capability       | Interface         | When to implement                      |
| ---------------- | ----------------- | -------------------------------------- |
| Server streaming | `StreamingPlugin` | gRPC server-streaming, Watch, &c       |
| Duplex channels  | `ChannelPlugin`   | WebSocket, SignalR, MQTT, …            |
| User settings    | `SettingsPlugin`  | Connection URLs, credentials, options  |
| Shutdown hook    | `ShutdownHook`    | Releasing open connections cleanly     |

The dispatcher discovers capabilities at runtime via type
assertions — a plugin implements only what it actually supports, and
the `capabilities` in the initialize reply say exactly that, so the host
never asks a plugin for something it cannot do.

[`examples/echo`](examples/echo/main.go) implements all of them.

## Settings

A plugin declares its settings; the workbench renders them in its
Settings dialog, under the plugin's own section, and sends the values
back with every call.

```go
func (EchoPlugin) Settings() []plugin.PluginSetting {
    return []plugin.PluginSetting{
        plugin.NewSetting("transform", "Transform", "select").
            WithDefault("none").
            WithOptions(
                plugin.SettingOption{Value: "none", Label: "As sent"},
                plugin.SettingOption{Value: "upper", Label: "Upper case"},
            ),
        plugin.NewSetting("ticks", "Ticks", "number").WithDefault(3),
    }
}
```

`Type` is `string`, `bool`, `number` or `select`; a `select` needs its
`Options`.

The values arrive as `Settings` on `DiscoverRequest`, `InvokeRequest`
and `ChannelRequest`, read fresh with every call — a changed setting
takes effect without restarting the sidecar:

```go
func (EchoPlugin) Invoke(_ context.Context, req plugin.InvokeRequest) (plugin.InvokeResult, error) {
    if req.Settings.String("transform", "none") == "upper" {
        // ...
    }
    ticks := req.Settings.Int("ticks", 3)
    // ...
}
```

The host sends every value as a string and only the keys that are set.
The accessors (`String`, `Bool`, `Number`, `Int`) parse the value; when
the key is absent, or its value does not parse, they return the default
the plugin declared, then the fallback passed in. `Has` and `Raw` tell
a set value from a default.

## Duplex channels

A `ChannelPlugin` serves bidirectional methods. `OpenChannel` wires up
the handle and returns; the handle lives on until either side closes it.

```go
func (EchoPlugin) OpenChannel(_ context.Context, req plugin.ChannelRequest, ch plugin.ChannelHandle) error {
    ch.OnMessage(func(message string) {
        _ = ch.Send(message) // back to the workbench as $/channel/data
    })
    ch.OnClose(func() { /* release what the channel holds */ })
    return nil
}
```

- The SDK keys each channel by the id the host minted, so the handle is
  all a plugin keeps; several channels can be open at once.
- `OnMessage` handlers run in order on the request loop, so frames
  arrive in the order the host sent them. Hand slow work to a goroutine.
- `Send` may be called from any goroutine. It returns
  `plugin.ErrChannelClosed` once the channel has closed.
- `Close` closes from the plugin's side. Whichever side closes, the
  `OnClose` handlers run once (they may still `Send` a last frame), then
  `$/channel/closed` ends the host's read stream. Shutdown, and stdin
  closing, end every channel still open.
- An error from `OpenChannel` refuses the open.

## Transports

Pick the wire your sidecar runs over. Both routes go through the
shared `Dispatcher`, so contract semantics live in one place
regardless of how the wire is framed.

### stdio (`Run`)

```go
plugin.Run(ctx, EchoPlugin{})
```

NDJSON JSON-RPC on stdin/stdout, zero deps. The Bowire host spawns
the sidecar binary and pipes both wires. Recommended default for
local / single-tenant setups.

### HTTP/SSE (`RunHTTP`)

```go
handle, err := plugin.RunHTTP(ctx, EchoPlugin{}, "127.0.0.1", 8770)
if err != nil { log.Fatal(err) }
fmt.Println("listening on", handle.Addr())
defer handle.Close(context.Background())
```

Streamable-HTTP variant — `POST /` lands JSON-RPC requests, `GET /`
is a long-lived SSE stream the runtime pushes server notifications
onto (`$/stream/data`, `$/stream/end`, `$/channel/data`,
`$/channel/closed`). Fits hosted /
multi-tenant deployments where one sidecar serves many workbenches.

`net/http` standard-library only — zero runtime dependencies.

## Wire model

Service / method / message / field shapes serialise to camelCase on
the wire — matches what the Python, Rust, and Node SDKs emit.

```go
svc := plugin.NewServiceInfo("Demo").WithMethods(
    plugin.ServerStreamingMethod("Watch").
        WithInput(plugin.NewMessageInfo("WatchReq", "demo.WatchReq").WithFields(
            plugin.String("topic").MarkRequired(),
        )).
        WithSummary("Watch a topic"),
)
```

## Testing against the host

CI installs the published Bowire CLI, installs the example as a sidecar
plugin and asks it to discover. Then [`e2e`](e2e/main.go) starts the
workbench headless and goes through the endpoints its UI calls: the
settings are declared and their values reach each call, and the example's
`Chat` method is opened, sent over both ways and closed from each side.

```bash
go run ./e2e --bowire bowire --plugin-dir <dir with the example installed> --work-dir <scratch dir>
```

## Related

- [Bowire (host)](https://github.com/Kuestenlogik/Bowire)
- [Bowire.Sdk.Python](https://github.com/Kuestenlogik/Bowire.Sdk.Python) — Python sibling
- [Bowire.Sdk.Rust](https://github.com/Kuestenlogik/Bowire.Sdk.Rust) — Rust sibling
- [Bowire.Sdk.Node](https://github.com/Kuestenlogik/Bowire.Sdk.Node) — Node.js sibling

## License

Apache-2.0, see [LICENSE](LICENSE).
