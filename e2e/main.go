// Command e2e checks the echo example against the real Bowire workbench.
//
//	go run ./e2e --bowire bowire --plugin-dir <dir> --work-dir <dir>
//
// `bowire discover` proves a plugin installs and answers, but the CLI has no
// workspace, so it never sends plugin settings, and no command of it drives
// a duplex channel. The workbench does both, behind the REST endpoints its
// UI calls — so this goes through those: a setting declared and read back
// per call (Kuestenlogik/Bowire#693, this repo's #4), and the example's Chat
// method opened, sent over both ways and closed from each side (#3).
//
// The plugin must already be installed into --plugin-dir; the CI job does
// that with `bowire plugin install`. --work-dir is a scratch directory: the
// host's log lands there, and it holds the workspace's storage root, so the
// settings this writes stay out of the operator's ~/.bowire.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	protocol  = "echo"
	service   = "EchoService"
	serverURL = "echo@http://localhost:1"
)

// Loopback only; a proxy from the runner's environment has no business here.
var client = &http.Client{Transport: &http.Transport{Proxy: nil}}

func main() {
	bowire := flag.String("bowire", "bowire", "the Bowire executable")
	pluginDir := flag.String("plugin-dir", "", "the plugin directory the example is installed in")
	workDir := flag.String("work-dir", "", "a scratch directory")
	flag.Parse()
	if *pluginDir == "" || *workDir == "" {
		fail(errors.New("--plugin-dir and --work-dir are required"))
	}
	if err := run(*bowire, *pluginDir, *workDir); err != nil {
		fail(err)
	}
	fmt.Println("e2e: all checks passed")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "e2e:", err)
	os.Exit(1)
}

func run(bowire, pluginDir, workDir string) error {
	storage := filepath.Join(workDir, "workspace")
	if err := os.MkdirAll(storage, 0o755); err != nil {
		return err
	}
	wb, err := startWorkbench(bowire, pluginDir, workDir)
	if err != nil {
		return err
	}
	defer wb.stop()

	s := &session{base: wb.url, query: url.Values{
		"workspaceId": {"e2e"},
		"storageRoot": {storage},
		"serverUrl":   {serverURL},
	}.Encode()}
	if err := checkSettings(s); err != nil {
		return fmt.Errorf("%w (host log: %s)", err, wb.logFile)
	}
	if err := checkChannels(s); err != nil {
		return fmt.Errorf("%w (host log: %s)", err, wb.logFile)
	}
	return nil
}

// --- the workbench ---------------------------------------------------------

type workbench struct {
	url     string
	logFile string
	cmd     *exec.Cmd
	drained chan struct{}
}

var banner = regexp.MustCompile(`Bowire is running at:\s+(\S+)`)

// startWorkbench launches the workbench on an OS-picked port and returns
// once it listens. The host prints its banner only after Kestrel reports
// the socket bound, so reading stdout up to that line is the readiness
// handshake: no port is guessed and nothing is retried. Everything the host
// prints goes on to host.log in workDir.
func startWorkbench(bowire, pluginDir, workDir string) (*workbench, error) {
	logFile := filepath.Join(workDir, "host.log")
	log, err := os.Create(logFile)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bowire, "--plugin-dir", pluginDir, "--no-browser", "--port", "0")
	// Outside the checkout: the workbench takes its cwd as its content root.
	cmd.Dir = workDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	lines := bufio.NewScanner(stdout)
	for lines.Scan() {
		fmt.Fprintln(log, lines.Text())
		if m := banner.FindStringSubmatch(lines.Text()); m != nil {
			wb := &workbench{url: strings.TrimRight(m[1], "/"), logFile: logFile, cmd: cmd, drained: make(chan struct{})}
			// Keep draining, or a chatty host blocks on a full pipe.
			go func() {
				defer close(wb.drained)
				defer log.Close()
				_, _ = io.Copy(log, stdout)
			}()
			return wb, nil
		}
	}
	_ = cmd.Wait()
	log.Close()
	return nil, fmt.Errorf("the workbench exited (%v) before it was listening; see %s", cmd.ProcessState, logFile)
}

func (wb *workbench) stop() {
	_ = wb.cmd.Process.Kill()
	<-wb.drained
	_ = wb.cmd.Wait()
}

// --- the endpoints the UI calls ----------------------------------------------

type session struct {
	base  string
	query string
}

func (s *session) url(path string) string { return s.base + path + "?" + s.query }

func (s *session) json(method, path string, body, out interface{}) (int, error) {
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		payload = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, s.url(path), payload)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, fmt.Errorf("%s %s -> %d: %s", method, path, resp.StatusCode, text)
	}
	if out != nil {
		if err := json.Unmarshal(text, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: %w: %s", method, path, err, text)
		}
	}
	return resp.StatusCode, nil
}

func (s *session) echo(message string) (string, error) {
	var reply struct {
		Status   string `json:"status"`
		Response string `json:"response"`
	}
	if _, err := s.json("POST", "/api/invoke", map[string]interface{}{
		"protocol": protocol, "service": service, "method": "Echo", "messages": []string{message},
	}, &reply); err != nil {
		return "", err
	}
	if reply.Status != "OK" {
		return "", fmt.Errorf("Echo status %q", reply.Status)
	}
	var body struct {
		Echoed string `json:"echoed"`
	}
	if err := json.Unmarshal([]byte(reply.Response), &body); err != nil {
		return "", fmt.Errorf("Echo response %q: %w", reply.Response, err)
	}
	return body.Echoed, nil
}

// ticks counts the frames Watch streams, read off the SSE stream the
// workbench serves for a server-streaming call.
func (s *session) ticks() (int, error) {
	q := url.Values{
		"protocol": {protocol}, "service": {service}, "method": {"Watch"}, "messages": {`["{}"]`},
	}
	resp, err := client.Get(s.url("/api/invoke/stream") + "&" + q.Encode())
	if err != nil {
		return 0, err
	}
	n := 0
	err = readSSE(resp, func(event, _ string) error {
		switch event {
		case "message":
			n++
		case "error":
			return errors.New("the stream failed")
		}
		return nil
	})
	return n, err
}

func (s *session) setSetting(key string, value interface{}) error {
	_, err := s.json("PUT", "/api/plugins/settings/"+protocol+"/"+key, map[string]interface{}{"value": value}, nil)
	return err
}

// chat is one channel as the workbench drives it: frames in over POST
// .../send, frames out over the SSE stream at .../responses, which ends
// with a `done` event once the sidecar says $/channel/closed.
type chat struct {
	s      *session
	id     string
	echoed []string
	dones  int
	ended  chan error
}

func (s *session) openChat() (*chat, error) {
	var opened struct {
		ChannelID string `json:"channelId"`
	}
	if _, err := s.json("POST", "/api/channel/open", map[string]string{
		"protocol": protocol, "service": service, "method": "Chat",
	}, &opened); err != nil {
		return nil, err
	}
	if opened.ChannelID == "" {
		return nil, errors.New("the open named no channel")
	}
	c := &chat{s: s, id: opened.ChannelID, ended: make(chan error, 1)}
	// Subscribe before sending anything: the host subscribed to the id
	// before it opened the channel, so nothing sent from here on is lost.
	resp, err := client.Get(s.url("/api/channel/" + c.id + "/responses"))
	if err != nil {
		return nil, err
	}
	go func() {
		c.ended <- readSSE(resp, func(event, data string) error {
			switch event {
			case "message":
				var frame struct {
					Data string `json:"data"`
				}
				var body struct {
					Echoed string `json:"echoed"`
				}
				if err := json.Unmarshal([]byte(data), &frame); err != nil {
					return err
				}
				if err := json.Unmarshal([]byte(frame.Data), &body); err != nil {
					return err
				}
				c.echoed = append(c.echoed, body.Echoed)
			case "done":
				c.dones++
			}
			return nil
		})
	}()
	return c, nil
}

// send returns whether the host took the frame (2xx) and, if so, whether
// the sidecar accepted it.
func (c *chat) send(message string) (bool, error) {
	var reply struct {
		Sent bool `json:"sent"`
	}
	status, err := c.s.json("POST", "/api/channel/"+c.id+"/send", map[string]string{"message": message}, &reply)
	if status/100 == 4 || status/100 == 5 {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return reply.Sent, nil
}

func (c *chat) close() error {
	_, err := c.s.json("POST", "/api/channel/"+c.id+"/close", map[string]string{}, nil)
	return err
}

// done waits for the host to end the response stream — the signal that the
// channel is closed on both sides.
func (c *chat) done() error { return <-c.ended }

// readSSE reads a Server-Sent-Events response until the server ends it.
func readSSE(resp *http.Response, onEvent func(event, data string) error) error {
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("SSE %s -> %d", resp.Request.URL.Path, resp.StatusCode)
	}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	event, data := "message", []string{}
	for lines.Scan() {
		line := strings.TrimRight(lines.Text(), "\r")
		switch {
		case line == "":
			if len(data) > 0 {
				if err := onEvent(event, strings.Join(data, "\n")); err != nil {
					return err
				}
			}
			event, data = "message", data[:0]
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[6:])
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(line[5:], " "))
		}
	}
	return lines.Err()
}

// --- the checks ----------------------------------------------------------------

func expect(ok bool, format string, args ...interface{}) error {
	if ok {
		return nil
	}
	return fmt.Errorf("assertion failed: "+format, args...)
}

// Both halves of #693: the declaration reaches the Settings dialog (with
// the options a select needs), and a value set there reaches the plugin
// with the next call, from the same sidecar process.
func checkSettings(s *session) error {
	var protocols []struct {
		ID       string `json:"id"`
		Settings []struct {
			Key     string `json:"key"`
			Type    string `json:"type"`
			Options []struct {
				Value string `json:"value"`
			} `json:"options"`
		} `json:"settings"`
	}
	if _, err := s.json("GET", "/api/protocols", nil, &protocols); err != nil {
		return err
	}
	declared := false
	for _, p := range protocols {
		if p.ID != protocol {
			continue
		}
		for _, setting := range p.Settings {
			if setting.Key != "transform" {
				continue
			}
			values := []string{}
			for _, o := range setting.Options {
				values = append(values, o.Value)
			}
			if err := expect(setting.Type == "select" && strings.Join(values, ",") == "none,upper,lower",
				"transform is a select with its options, got %s %v", setting.Type, values); err != nil {
				return err
			}
			declared = true
		}
	}
	if err := expect(declared, "the echo protocol declares transform"); err != nil {
		return err
	}
	fmt.Println("settings: declared, with options")

	steps := []struct {
		key, value string
		set        bool
		echo       string
		ticks      int
	}{
		{echo: "Hi", ticks: 3},
		{key: "transform", value: "upper", set: true, echo: "HI", ticks: 3},
		{key: "ticks", value: "2", set: true, echo: "HI", ticks: 2},
		{key: "transform", echo: "Hi", ticks: 2},
		{key: "ticks", echo: "Hi", ticks: 3},
	}
	for i, step := range steps {
		if step.key != "" {
			var value interface{}
			if step.set {
				value = step.value
			}
			if err := s.setSetting(step.key, value); err != nil {
				return err
			}
		}
		got, err := s.echo("Hi")
		if err != nil {
			return err
		}
		if err := expect(got == step.echo, "step %d: Echo answered %q, want %q", i, got, step.echo); err != nil {
			return err
		}
		n, err := s.ticks()
		if err != nil {
			return err
		}
		if err := expect(n == step.ticks, "step %d: Watch streamed %d ticks, want %d", i, n, step.ticks); err != nil {
			return err
		}
	}
	fmt.Println("settings: values reach invoke and invokeStream per call, a cleared one falls back")
	return nil
}

// #3: a duplex method end to end, closed once from each side. The setting
// is set first, so this also proves openChannel receives the values.
func checkChannels(s *session) (err error) {
	if err := s.setSetting("transform", "upper"); err != nil {
		return err
	}
	defer func() {
		if clearErr := s.setSetting("transform", nil); err == nil {
			err = clearErr
		}
	}()

	byHost, err := s.openChat()
	if err != nil {
		return err
	}
	for _, text := range []string{"hello", "again"} {
		sent, err := byHost.send(text)
		if err != nil {
			return err
		}
		if err := expect(sent, "the sidecar accepted %q", text); err != nil {
			return err
		}
	}
	if err := byHost.close(); err != nil {
		return err
	}
	if err := byHost.done(); err != nil {
		return err
	}
	if err := expect(strings.Join(byHost.echoed, ",") == "HELLO,AGAIN",
		"frames came back in order, transformed: %v", byHost.echoed); err != nil {
		return err
	}
	if err := expect(byHost.dones == 1, "closed from the host: the stream ended once, got %d", byHost.dones); err != nil {
		return err
	}
	fmt.Println("channels: open, send both ways, close from the host")

	byPlugin, err := s.openChat()
	if err != nil {
		return err
	}
	if _, err := byPlugin.send("ping"); err != nil {
		return err
	}
	if _, err := byPlugin.send(`{"close":true}`); err != nil {
		return err
	}
	if err := byPlugin.done(); err != nil {
		return err
	}
	if err := expect(strings.Join(byPlugin.echoed, ",") == "PING",
		"the frame before the close came back: %v", byPlugin.echoed); err != nil {
		return err
	}
	if err := expect(byPlugin.dones == 1, "closed from the plugin: the stream ended once, got %d", byPlugin.dones); err != nil {
		return err
	}
	late, err := byPlugin.send("anyone?")
	if err != nil {
		return err
	}
	if err := expect(!late, "a send after the plugin closed is refused"); err != nil {
		return err
	}
	fmt.Println("channels: closed from the plugin, a later send refused")
	return nil
}
