// Runnable Echo plugin demonstrating the Bowire Go SDK surface: a unary
// method, a server stream, a duplex channel, and settings that change what
// each of them does.
//
// stdio mode (default):
//
//	go run ./examples/echo
//
// HTTP/SSE mode:
//
//	BOWIRE_HTTP_PORT=8770 go run ./examples/echo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Kuestenlogik/Bowire.Sdk.Go/plugin"
)

type echoPlugin struct{}

func (echoPlugin) ID() string   { return "echo" }
func (echoPlugin) Name() string { return "Echo" }

// Settings are declared here and rendered by the workbench's Settings
// dialog; their values come back with every call.
func (echoPlugin) Settings() []plugin.PluginSetting {
	return []plugin.PluginSetting{
		plugin.NewSetting("transform", "Transform", "select").
			WithDescription("What Echo and Chat do to the text before sending it back").
			WithDefault("none").
			WithOptions(
				plugin.SettingOption{Value: "none", Label: "As sent"},
				plugin.SettingOption{Value: "upper", Label: "Upper case"},
				plugin.SettingOption{Value: "lower", Label: "Lower case"},
			),
		plugin.NewSetting("ticks", "Ticks", "number").
			WithDescription("How many events Watch streams").
			WithDefault(3),
	}
}

func transform(text string, settings plugin.SettingValues) string {
	switch settings.String("transform", "none") {
	case "upper":
		return strings.ToUpper(text)
	case "lower":
		return strings.ToLower(text)
	default:
		return text
	}
}

func (echoPlugin) Discover(_ context.Context, _ plugin.DiscoverRequest) ([]plugin.ServiceInfo, error) {
	return []plugin.ServiceInfo{
		plugin.NewServiceInfo("EchoService").
			WithDescription("A demo plugin").
			WithMethods(
				plugin.UnaryMethod("Echo").
					WithInput(plugin.NewMessageInfo("EchoRequest", "echo.EchoRequest").WithFields(
						plugin.String("message").MarkRequired(),
					)).
					WithOutput(plugin.NewMessageInfo("EchoResponse", "echo.EchoResponse").WithFields(
						plugin.String("echoed"),
					)).
					WithSummary("Echo a message back"),
				plugin.ServerStreamingMethod("Watch").WithSummary("Stream tick events"),
				plugin.BidirectionalMethod("Chat").
					WithSummary(`Echo every frame back; a frame of {"close":true} ends the channel`),
			),
	}, nil
}

func (echoPlugin) Invoke(_ context.Context, req plugin.InvokeRequest) (plugin.InvokeResult, error) {
	first := ""
	if len(req.JSONMessages) > 0 {
		first = req.JSONMessages[0]
	}
	body, _ := json.Marshal(map[string]string{"echoed": transform(first, req.Settings)})
	return plugin.NewOKResult(string(body)), nil
}

func (echoPlugin) InvokeStream(_ context.Context, req plugin.InvokeRequest) (<-chan string, error) {
	ticks := req.Settings.Int("ticks", 3)
	if ticks < 0 {
		ticks = 0
	}
	ch := make(chan string, ticks)
	for i := 0; i < ticks; i++ {
		ch <- fmt.Sprintf(`{"tick":%d}`, i)
	}
	close(ch)
	return ch, nil
}

// OpenChannel wires up a duplex channel and returns; the handle is all the
// plugin keeps — the SDK routes the host's frames to the right one.
func (echoPlugin) OpenChannel(_ context.Context, req plugin.ChannelRequest, channel plugin.ChannelHandle) error {
	channel.OnMessage(func(message string) {
		if wantsClose(message) {
			channel.Close()
			return
		}
		body, _ := json.Marshal(map[string]string{"echoed": transform(message, req.Settings)})
		_ = channel.Send(string(body))
	})
	return nil
}

func wantsClose(message string) bool {
	var frame struct {
		Close bool `json:"close"`
	}
	return json.Unmarshal([]byte(message), &frame) == nil && frame.Close
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if portStr := os.Getenv("BOWIRE_HTTP_PORT"); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("invalid BOWIRE_HTTP_PORT: %v", err)
		}
		handle, err := plugin.RunHTTP(ctx, echoPlugin{}, "127.0.0.1", port)
		if err != nil {
			log.Fatalf("RunHTTP: %v", err)
		}
		fmt.Fprintf(os.Stderr, "echo listening on http://%s\n", handle.Addr())
		<-ctx.Done()
		_ = handle.Close(context.Background())
		return
	}

	if err := plugin.Run(ctx, echoPlugin{}); err != nil {
		log.Fatalf("Run: %v", err)
	}
}
