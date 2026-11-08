package webrtc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	xumux "github.com/deftai/libxumux-go"
	"github.com/pion/ice/v4"
	pion "github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logWriter struct{ t *testing.T }

func (w logWriter) Write(b []byte) (int, error) {
	w.t.Log(string(bytes.TrimRight(b, "\n")))
	return len(b), nil
}

// signalMsg is one line of the interop peer's stdio signalling.
type signalMsg struct {
	SDP       *pion.SessionDescription `json:"sdp,omitempty"`
	Candidate *pion.ICECandidateInit   `json:"candidate,omitempty"`
}

// libxumux (TypeScript, node-datachannel's standard RTCPeerConnection) as the
// WebRTC client, offering; this module as the server, answering. Needs node
// and a built libxumux checkout in LIBXUMUX_DIR (npm ci installs
// node-datachannel); otherwise it is skipped.
func TestInteropLibxumuxClient(t *testing.T) {
	dir := os.Getenv("LIBXUMUX_DIR")
	if dir == "" {
		t.Skip("set LIBXUMUX_DIR to a built libxumux checkout to run interop tests")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	ctx := ctxT(t, 40*time.Second)
	cmd := exec.Command(node, filepath.Join("interop", "libxumux-webrtc-peer.mjs"))
	cmd.Env = append(os.Environ(), "LIBXUMUX_DIR="+dir)
	cmd.Stderr = logWriter{t}
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// The peer is not on loopback only, so gather every host candidate here.
	var se pion.SettingEngine
	se.SetNetworkTypes([]pion.NetworkType{pion.NetworkTypeUDP4})
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	se.SetSCTPMaxMessageSize(RecommendedMaxMessageSize)
	pc, err := pion.NewAPI(pion.WithSettingEngine(se)).NewPeerConnection(pion.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })
	dc, err := CreateDataChannel(pc)
	require.NoError(t, err)
	tr, err := NewTransport(dc, Config{})
	require.NoError(t, err)

	// Signalling: the offer, then trickled candidates; answer with ours bundled.
	lines := bufio.NewScanner(stdout)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	require.True(t, lines.Scan(), "no offer from the libxumux peer")
	var offer signalMsg
	require.NoError(t, json.Unmarshal(lines.Bytes(), &offer))
	require.NotNil(t, offer.SDP)
	require.NoError(t, pc.SetRemoteDescription(*offer.SDP))
	answer, err := pc.CreateAnswer(nil)
	require.NoError(t, err)
	gathered := pion.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(answer))
	<-gathered
	out, err := json.Marshal(signalMsg{SDP: pc.LocalDescription()})
	require.NoError(t, err)
	_, err = stdin.Write(append(out, '\n'))
	require.NoError(t, err)
	go func() {
		for lines.Scan() {
			var m signalMsg
			if json.Unmarshal(lines.Bytes(), &m) == nil && m.Candidate != nil {
				_ = pc.AddICECandidate(*m.Candidate)
			}
		}
	}()

	s, hello, err := Accept(ctx, tr, xumux.ServerConfig{}, xumux.Options{Acceptor: echoAcceptor(ctx)})
	require.NoError(t, err)
	assert.Equal(t, "webrtc-interop/1", hello.Application)
	select {
	case <-s.Done():
	case <-ctx.Done():
		t.Fatal("session did not close")
	}
	assert.Equal(t, xumux.CloseError{Code: xumux.CodeNormal, Reason: "done", Remote: true}, s.CloseInfo())

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		assert.NoError(t, err, "libxumux peer reported a failure")
	case <-time.After(10 * time.Second):
		t.Fatal("libxumux peer did not exit")
	}
}
