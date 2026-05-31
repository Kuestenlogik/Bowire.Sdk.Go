// Runnable Echo plugin demonstrating the @bowire/plugin Go surface.
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
	"syscall"

	"github.com/Kuestenlogik/Bowire.Sdk.Go/plugin"
)

type echoPlugin struct{}

func (echoPlugin) ID() string   { return "echo" }
func (echoPlugin) Name() string { return "Echo" }

func (echoPlugin) Discover(_ context.Context, _ string, _ bool) ([]plugin.ServiceInfo, error) {
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
			),
	}, nil
}

func (echoPlugin) Invoke(_ context.Context, req plugin.InvokeRequest) (plugin.InvokeResult, error) {
	first := ""
	if len(req.Body) > 0 {
		first = req.Body[0]
	}
	body, _ := json.Marshal(map[string]string{"echoed": first})
	return plugin.NewOKResult(string(body)), nil
}

func (echoPlugin) InvokeStream(_ context.Context, _ plugin.InvokeRequest) (<-chan string, error) {
	ch := make(chan string, 3)
	for i := 0; i < 3; i++ {
		ch <- fmt.Sprintf(`{"tick":%d}`, i)
	}
	close(ch)
	return ch, nil
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
