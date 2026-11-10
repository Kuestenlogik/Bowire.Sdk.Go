package main

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
)

// httpSidecar is the example running on its own, as an http sidecar: the
// host connects to it rather than spawning it, so the e2e starts it and
// stops it.
type httpSidecar struct {
	url     string
	logFile string
	cmd     *exec.Cmd
	drained chan struct{}
}

var listening = regexp.MustCompile(`listening on (http://\S+)`)

// startHTTPSidecar starts the built example in HTTP mode on a port the OS
// picks. The example prints its address once the listener is bound, so
// reading its output up to that line is the readiness handshake, and it
// carries the port. Everything it prints goes on to sidecar.log.
func startHTTPSidecar(binary, workDir string) (*httpSidecar, error) {
	logFile := filepath.Join(workDir, "sidecar.log")
	log, err := os.Create(logFile)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "BOWIRE_HTTP_PORT=0")
	cmd.Stdout = log
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	lines := bufio.NewScanner(stderr)
	for lines.Scan() {
		fmt.Fprintln(log, lines.Text())
		if m := listening.FindStringSubmatch(lines.Text()); m != nil {
			sc := &httpSidecar{url: strings.TrimRight(m[1], "/") + "/", logFile: logFile, cmd: cmd, drained: make(chan struct{})}
			go func() {
				defer close(sc.drained)
				defer log.Close()
				_, _ = io.Copy(log, stderr)
			}()
			return sc, nil
		}
	}
	_ = cmd.Wait()
	log.Close()
	return nil, fmt.Errorf("the http sidecar exited (%v) before it was listening; see %s", cmd.ProcessState, logFile)
}

// stop tells the sidecar to stop the way a container manager does and
// returns how it exited. Windows has no signal a process can handle, so
// there it is killed.
func (sc *httpSidecar) stop() (exitCode int, err error) {
	if sc.cmd.ProcessState != nil {
		return sc.cmd.ProcessState.ExitCode(), nil
	}
	if runtime.GOOS == "windows" {
		err = sc.cmd.Process.Kill()
	} else {
		err = sc.cmd.Process.Signal(syscall.SIGTERM)
	}
	if err != nil {
		return -1, err
	}
	<-sc.drained
	_ = sc.cmd.Wait()
	return sc.cmd.ProcessState.ExitCode(), nil
}

// writeHTTPZip writes a sidecar zip that ships no code, only where to
// reach the sidecar: the example's manifest with "transport": "http" and
// the sidecar's url instead of an executable.
func writeHTTPZip(path, manifestPath, sidecarURL string) error {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var manifest map[string]interface{}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	for _, key := range []string{"executable", "args", "envPrefix"} {
		delete(manifest, key)
	}
	manifest["packageId"] = fmt.Sprint(manifest["packageId"]) + ".Http"
	manifest["transport"] = "http"
	manifest["url"] = sidecarURL
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	archive := zip.NewWriter(file)
	entry, err := archive.Create("sidecar.json")
	if err != nil {
		return err
	}
	if _, err := entry.Write(body); err != nil {
		return err
	}
	return archive.Close()
}

// bowireCLI runs one CLI command and returns what it printed, failing on a
// non-zero exit after showing the output.
func bowireCLI(bowire string, args ...string) (string, error) {
	out, err := exec.Command(bowire, args...).CombinedOutput()
	fmt.Print(string(out))
	if err != nil {
		return string(out), fmt.Errorf("bowire %s: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}
