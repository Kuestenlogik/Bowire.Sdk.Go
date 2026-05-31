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

func (EchoPlugin) Discover(_ context.Context, _ string, _ bool) ([]plugin.ServiceInfo, error) {
    return []plugin.ServiceInfo{
        plugin.NewServiceInfo("EchoService").WithMethods(
            plugin.UnaryMethod("Echo"),
        ),
    }, nil
}

func (EchoPlugin) Invoke(_ context.Context, req plugin.InvokeRequest) (plugin.InvokeResult, error) {
    first := ""
    if len(req.Body) > 0 {
        first = req.Body[0]
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
assertions — a plugin embeds only what it actually supports.

## Transports

Pick the wire your sidecar runs over. Both routes go through the
shared `Dispatch` helper, so contract semantics live in one place
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
onto (`$/stream/data`, `$/stream/end`). Fits hosted /
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

## Related

- [Bowire (host)](https://github.com/Kuestenlogik/Bowire)
- [Bowire.Sdk.Python](https://github.com/Kuestenlogik/Bowire.Sdk.Python) — Python sibling
- [Bowire.Sdk.Rust](https://github.com/Kuestenlogik/Bowire.Sdk.Rust) — Rust sibling
- [Bowire.Sdk.Node](https://github.com/Kuestenlogik/Bowire.Sdk.Node) — Node.js sibling

## License

MIT
