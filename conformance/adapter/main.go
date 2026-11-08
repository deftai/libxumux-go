// Command adapter runs libxumux-go as the server under test for the xumux
// behavioural conformance suite (deftai/xumux conformance/behaviour). It
// speaks xumux on stdin/stdout; its one argument is a JSON config of limits.
// Channels behave according to their name: echo, sink, hold, refuse and
// pipe-echo (pipe/1 only).
//
//	python3 -I run.py -- go run ./conformance/adapter
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

type config struct {
	MaxFramePayload    uint32 `json:"maxFramePayload"`
	MaxMessageSize     uint32 `json:"maxMessageSize"`
	InitialWindow      uint32 `json:"initialWindow"`
	MaxChannels        int    `json:"maxChannels"`
	HandshakeTimeoutMs int    `json:"handshakeTimeoutMs"`
}

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error                { _ = os.Stdin.Close(); return os.Stdout.Close() }
func (stdio) CloseWrite() error           { return os.Stdout.Close() }

func main() {
	var cfg config
	if len(os.Args) > 1 {
		if err := json.Unmarshal([]byte(os.Args[1]), &cfg); err != nil {
			fmt.Fprintln(os.Stderr, "adapter: bad config:", err)
			os.Exit(2)
		}
	}
	opts := xumux.Options{
		Limits:           xumux.Limits{MaxFramePayload: cfg.MaxFramePayload, MaxMessageSize: cfg.MaxMessageSize, InitialWindow: cfg.InitialWindow},
		PingInterval:     -1,
		MaxChannels:      cfg.MaxChannels,
		HandshakeTimeout: time.Duration(cfg.HandshakeTimeoutMs) * time.Millisecond,
		Acceptor:         accept,
	}
	s, _, err := xumux.Accept(context.Background(), xumux.NewStreamConn(stdio{}), xumux.ServerConfig{}, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "adapter: handshake:", err)
		return
	}
	<-s.Done()
}

func accept(ctx context.Context, req xumux.ChannelRequest) (xumux.Handler, error) {
	switch req.Name {
	case "echo":
		return echo, nil
	case "sink":
		return sink, nil
	case "hold":
		return func(ch *xumux.Channel) { <-ch.Done() }, nil
	case "pipe-echo":
		if err := pipe.IsPipe(req); err != nil {
			return nil, err
		}
		return pipeEcho, nil
	}
	return nil, &xumux.Refusal{Code: xumux.CodeChannelRefused, Reason: "refused"}
}

func echo(ch *xumux.Channel) {
	ctx := ch.Session().Context()
	for {
		m, err := ch.Receive(ctx)
		if err != nil {
			return
		}
		if err := ch.Send(ctx, m.Type, m.Payload); err != nil {
			return
		}
	}
}

func sink(ch *xumux.Channel) {
	for {
		if _, err := ch.Receive(ch.Session().Context()); err != nil {
			return
		}
	}
}

func pipeEcho(ch *xumux.Channel) {
	c := pipe.NewConn(ch)
	defer c.Close()
	if _, err := io.Copy(c, c); err != nil {
		var re *pipe.ResetError
		if !errors.As(err, &re) {
			_ = c.Reset(xumux.CodeGoingAway, err.Error())
		}
		return
	}
	_ = c.CloseWrite()
	<-ch.Done()
}
