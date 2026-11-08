package xumux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var plainSpec = ChannelSpec{Name: "x", Reliable: true, Ordered: true}

// SendPrefix sends what the credit allows as one whole message, never a
// fragment of a larger one.
func TestSendPrefixSendsWhatCreditAllowsUnfragmented(t *testing.T) {
	const window = 4096 // the smallest window the limits allow
	sopts := acceptAll(nil)
	sopts.Limits = Limits{MaxFramePayload: window, MaxMessageSize: window, InitialWindow: window}
	got := make(chan *Channel, 1)
	sopts.Acceptor = func(context.Context, ChannelRequest) (Handler, error) {
		return func(ch *Channel) { ch.SetManualCredit(true); got <- ch }, nil
	}
	client, _, _ := pair(t, testOpts, sopts)
	ch, err := client.OpenChannel(ctx(t), plainSpec)
	require.NoError(t, err)
	srv := <-got

	payload := make([]byte, 3000)
	n, err := ch.SendPrefix(ctx(t), 0x01, payload)
	require.NoError(t, err)
	assert.Equal(t, 3000, n)
	n, err = ch.SendPrefix(ctx(t), 0x01, payload)
	require.NoError(t, err)
	assert.Equal(t, window-3000, n, "only the remaining credit")

	// Each arrives as its own message: had the second been a fragment,
	// the receiver would still be waiting for the rest.
	for _, want := range []int{3000, window - 3000} {
		m, err := srv.Receive(ctx(t))
		require.NoError(t, err)
		assert.Len(t, m.Payload, want)
	}

	// With no credit left it waits, and a deadline ends the wait cleanly.
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	n, err = ch.SendPrefix(short, 0x01, payload)
	assert.Zero(t, n)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	srv.ReturnCredit(window)
	n, err = ch.SendPrefix(ctx(t), 0x01, payload)
	require.NoError(t, err)
	assert.Equal(t, 3000, n)

	// An empty payload is an empty message, and needs no credit.
	n, err = ch.SendPrefix(ctx(t), 0x02, nil)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestChannelListener(t *testing.T) {
	l := NewChannelListener()
	l.Check = func(_ context.Context, req ChannelRequest) error {
		if req.Name == "no" {
			return &Refusal{Code: 4011, Reason: "not this one"}
		}
		return nil
	}
	sopts := testOpts
	sopts.Acceptor = l.Acceptor()
	client, _, _ := pair(t, testOpts, sopts)

	accepted := make(chan *Channel)
	go func() {
		for {
			ch, err := l.Accept(context.Background())
			if err != nil {
				close(accepted)
				return
			}
			accepted <- ch
		}
	}()

	for _, name := range []string{"a", "b"} {
		spec := plainSpec
		spec.Name = name
		_, err := client.OpenChannel(ctx(t), spec)
		require.NoError(t, err)
		assert.Equal(t, name, (<-accepted).Name())
	}

	spec := plainSpec
	spec.Name = "no"
	_, err := client.OpenChannel(ctx(t), spec)
	assert.True(t, isClose(err, 4011), "Check's refusal code: %v", err)

	require.NoError(t, l.Close())
	_, ok := <-accepted
	assert.False(t, ok, "Accept ends after Close")
	_, err = l.Accept(ctx(t))
	assert.True(t, errors.Is(err, ErrClosed))
	_, err = client.OpenChannel(ctx(t), plainSpec)
	assert.True(t, isClose(err, CodeChannelRefused), "refused after Close: %v", err)
}

// A channel accepted but never taken by Accept is closed when the listener
// closes, instead of staying open.
func TestChannelListenerClosesWaitingChannels(t *testing.T) {
	l := NewChannelListener()
	sopts := testOpts
	sopts.Acceptor = l.Acceptor()
	client, _, _ := pair(t, testOpts, sopts)
	ch, err := client.OpenChannel(ctx(t), plainSpec)
	require.NoError(t, err)
	require.NoError(t, l.Close())
	select {
	case <-ch.Done():
		assert.Equal(t, CodeGoingAway, ch.CloseInfo().Code)
	case <-time.After(2 * time.Second):
		t.Fatal("waiting channel was not closed")
	}
}
