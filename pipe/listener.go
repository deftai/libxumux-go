package pipe

import (
	"context"
	"errors"
	"net"

	xumux "github.com/deftai/libxumux-go"
)

// Listener is a net.Listener for the pipe/1 channels peers open. Use its
// Acceptor in xumux.Options; one Listener can serve any number of sessions.
// It refuses channels of any other profile.
type Listener struct {
	// Check, if set, decides on each pipe/1 request before it is accepted;
	// an error refuses it (a *xumux.Refusal chooses the code, otherwise
	// 4010). Check the target here if it means anything to you. Set it
	// before the listener is used.
	Check func(ctx context.Context, req xumux.ChannelRequest) error

	l *xumux.ChannelListener
}

// NewListener returns an open Listener.
func NewListener() *Listener {
	pl := &Listener{l: xumux.NewChannelListener()}
	pl.l.Check = func(ctx context.Context, req xumux.ChannelRequest) error {
		if err := IsPipe(req); err != nil {
			return err
		}
		if pl.Check != nil {
			return pl.Check(ctx, req)
		}
		return nil
	}
	return pl
}

// Acceptor returns the xumux.Acceptor that feeds this listener.
func (l *Listener) Acceptor() xumux.Acceptor { return l.l.Acceptor() }

// Accept implements net.Listener. After Close it returns an error that
// matches net.ErrClosed, which is how net/http's Serve knows to stop.
func (l *Listener) Accept() (net.Conn, error) { return l.AcceptPipe(context.Background()) }

// AcceptPipe waits for the next pipe, or until ctx ends.
func (l *Listener) AcceptPipe(ctx context.Context) (*Conn, error) {
	ch, err := l.l.Accept(ctx)
	if errors.Is(err, xumux.ErrClosed) {
		return nil, &net.OpError{Op: "accept", Net: "xumux", Addr: l.Addr(), Err: net.ErrClosed}
	}
	if err != nil {
		return nil, err
	}
	return NewConn(ch), nil
}

// Close implements net.Listener. Sessions are not affected.
func (l *Listener) Close() error { return l.l.Close() }

// Addr implements net.Listener.
func (l *Listener) Addr() net.Addr { return Addr{Name: "listener"} }
