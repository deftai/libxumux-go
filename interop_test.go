package xumux

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// childConn is a spawned process's stdio as a stream with half-close.
type childConn struct {
	io.Reader
	w   io.WriteCloser
	cmd *exec.Cmd
}

func (c *childConn) Write(b []byte) (int, error) { return c.w.Write(b) }
func (c *childConn) CloseWrite() error           { return c.w.Close() }
func (c *childConn) Close() error                { c.w.Close(); return nil }

// spawnLibxumux starts interop/libxumux-peer.mjs. It needs node and a built
// libxumux checkout in LIBXUMUX_DIR; otherwise the test is skipped.
func spawnLibxumux(t *testing.T, role string) *childConn {
	dir := os.Getenv("LIBXUMUX_DIR")
	if dir == "" {
		t.Skip("set LIBXUMUX_DIR to a built libxumux checkout to run interop tests")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	cmd := exec.Command(node, filepath.Join("..", "interop", "libxumux-peer.mjs"), role)
	cmd.Env = append(os.Environ(), "LIBXUMUX_DIR="+dir)
	cmd.Stderr = testWriter{t}
	w, err := cmd.StdinPipe()
	require.NoError(t, err)
	r, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return &childConn{Reader: r, w: w, cmd: cmd}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(string(bytes.TrimRight(b, "\n")))
	return len(b), nil
}

func waitExit(t *testing.T, c *childConn) {
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case err := <-done:
		assert.NoError(t, err, "libxumux peer reported a failure")
	case <-time.After(10 * time.Second):
		t.Fatal("libxumux peer did not exit")
	}
}

func echoHandler(ch *Channel) {
	for {
		m, err := ch.Receive(context.Background())
		if err != nil {
			return
		}
		if ch.Send(context.Background(), m.Type, m.Payload) != nil {
			return
		}
	}
}

// Go client <-> libxumux server.
func TestInteropGoClient(t *testing.T) {
	child := spawnLibxumux(t, "server")
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, res, err := Connect(c, NewStreamConn(child), ClientConfig{Channels: []ChannelSpec{data}}, acceptAll(echoHandler))
	require.NoError(t, err)
	require.Len(t, res.Channels, 1)
	ch := res.Channels[0]
	for _, size := range []int{0, 5, 16384, 16385, 200_000} {
		p := make([]byte, size)
		for i := range p {
			p[i] = byte(i * 31)
		}
		require.NoError(t, ch.Send(c, 4, p))
		m, err := ch.Receive(c)
		require.NoError(t, err)
		assert.Equal(t, Message{Type: 4, Payload: p}, m, "echo of %d bytes", size)
	}
	_, err = ch.Ping(c)
	require.NoError(t, err)
	_, err = s.Ping(c)
	require.NoError(t, err)
	dyn, err := s.OpenChannel(c, ChannelSpec{Name: "dyn", Reliable: true, Ordered: true})
	require.NoError(t, err)
	assert.Equal(t, uint16(3), dyn.ID())
	require.NoError(t, dyn.Close(c, CodeNormal, ""))
	time.Sleep(200 * time.Millisecond) // let the server-opened channel finish its echo check
	require.NoError(t, s.Close(c, CodeNormal, "done"))
	waitExit(t, child)
}

// libxumux client <-> Go server.
func TestInteropGoServer(t *testing.T) {
	child := spawnLibxumux(t, "client")
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, hello, err := Accept(c, NewStreamConn(child), ServerConfig{}, acceptAll(echoHandler))
	require.NoError(t, err)
	assert.Equal(t, Version, hello.Version)
	select {
	case <-s.Done():
	case <-c.Done():
		t.Fatal("session did not close")
	}
	assert.Equal(t, CloseError{Code: CodeNormal, Reason: "done", Remote: true}, s.CloseInfo())
	waitExit(t, child)
}
