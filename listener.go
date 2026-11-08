package xumux

import (
	"context"
	"sync"
)

// ChannelListener hands the channels peers open to Accept, in the style of
// net.Listener, as an alternative to running a Handler per channel. Use its
// Acceptor in Options; one listener can serve any number of sessions.
//
//	l := xumux.NewChannelListener()
//	s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
//	for {
//		ch, err := l.Accept(ctx)
//		...
//	}
//
// A channel is acknowledged as soon as Check allows it, and waits, open,
// until Accept takes it. MaxChannels bounds how many can wait per session.
type ChannelListener struct {
	// Check, if set, decides on each request before it is accepted; an
	// error refuses it as an Acceptor's would. Set it before the listener
	// is used.
	Check func(ctx context.Context, req ChannelRequest) error

	conns  chan *Channel
	closed chan struct{}
	once   sync.Once
}

// NewChannelListener returns an open listener.
func NewChannelListener() *ChannelListener {
	return &ChannelListener{conns: make(chan *Channel), closed: make(chan struct{})}
}

// Acceptor returns the Acceptor that feeds this listener. Once the listener
// is closed it refuses every channel with 4010.
func (l *ChannelListener) Acceptor() Acceptor {
	return func(ctx context.Context, req ChannelRequest) (Handler, error) {
		select {
		case <-l.closed:
			return nil, &Refusal{Code: CodeChannelRefused, Reason: "not accepting channels"}
		default:
		}
		if l.Check != nil {
			if err := l.Check(ctx, req); err != nil {
				return nil, err
			}
		}
		return func(ch *Channel) {
			select {
			case l.conns <- ch:
			case <-ch.Done(): // closed before anyone accepted it
			case <-l.closed:
				_ = ch.Close(context.Background(), CodeGoingAway, "listener closed")
			}
		}, nil
	}
}

// Accept waits for the next open channel. It returns ErrClosed once the
// listener is closed, or ctx's error.
func (l *ChannelListener) Accept(ctx context.Context) (*Channel, error) {
	select {
	case ch := <-l.conns:
		return ch, nil
	case <-l.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops the listener. Pending and future Accepts return ErrClosed,
// channels waiting to be accepted are closed with 1001, and new requests
// are refused. Sessions are not affected.
func (l *ChannelListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
