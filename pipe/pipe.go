// Package pipe implements the xumux pipe/1 profile: one bidirectional byte
// stream, with half-close, per channel.
//
// A pipe is a [net.Conn], so the standard library works over it: io.Copy,
// bufio, crypto/tls, net/http. [Dial] opens one, and a [Listener] is the
// [net.Listener] for the pipes peers open.
//
//	// Client
//	c, err := pipe.Dial(ctx, session, "web", "")
//	io.WriteString(c, "GET / HTTP/1.0\r\n\r\n")
//
//	// Server
//	l := pipe.NewListener()
//	s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
//	http.Serve(l, handler)
//
// pipe/1 lets the opener name a target for the responder to connect the
// pipe to. Responders that act on targets must check them against an
// allow-list (Listener.Check is the place); an unrestricted target makes
// the responder an open proxy.
package pipe

import (
	"context"
	"fmt"

	xumux "github.com/deftai/libxumux-go"
)

// Profile is the metadata.profile value of a pipe/1 channel.
const Profile = "pipe/1"

// Message types.
const (
	TypeData  byte = 0x01
	TypeEOF   byte = 0x02
	TypeReset byte = 0x03
)

// CodeTargetUnreachable refuses a channel whose target cannot be reached.
const CodeTargetUnreachable = 4500

// maxChunk caps DATA messages. They are never fragmented, so each one is
// also capped by the peer's maxFramePayload.
const maxChunk = 32 << 10

// Spec describes a pipe/1 channel. An empty target lets the responder choose.
func Spec(name, target string) xumux.ChannelSpec {
	md := map[string]any{"profile": Profile}
	if target != "" {
		md["target"] = target
	}
	return xumux.ChannelSpec{Name: name, Reliable: true, Ordered: true, Metadata: md}
}

// Dial opens a pipe/1 channel on s, asking the peer to connect it to target
// ("" lets the peer choose), and returns it as a Conn once accepted.
func Dial(ctx context.Context, s *xumux.Session, name, target string) (*Conn, error) {
	ch, err := s.OpenChannel(ctx, Spec(name, target))
	if err != nil {
		return nil, err
	}
	return NewConn(ch), nil
}

// IsPipe reports whether req asks for a pipe/1 channel, and if not, why.
func IsPipe(req xumux.ChannelRequest) error {
	if p, _ := req.Metadata["profile"].(string); p != Profile {
		return &xumux.Refusal{Code: xumux.CodeChannelRefused, Reason: fmt.Sprintf("unsupported profile %q", p)}
	}
	if !req.Reliable || !req.Ordered {
		return &xumux.Refusal{Code: xumux.CodeChannelRefused, Reason: "pipe/1 channels must be reliable and ordered"}
	}
	return nil
}

// Target returns the target a pipe/1 request names, or "".
func Target(req xumux.ChannelRequest) string {
	t, _ := req.Metadata["target"].(string)
	return t
}

// ResetError reports that the far end of the pipe failed (a RESET message).
type ResetError struct {
	Code   int
	Reason string
}

func (e *ResetError) Error() string {
	return fmt.Sprintf("pipe reset by peer (%d): %s", e.Code, e.Reason)
}

type resetMsg struct {
	Code   int    `json:"code"`
	Reason string `json:"reason,omitempty"`
}
