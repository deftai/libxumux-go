# libxumux-go

A Go implementation of the [xumux](https://github.com/deftai/xumux) 0.3 core protocol (draft `v0.3.0-draft`): many channels, each with its own flow control, over one connection.

It was written from the spec independently of the TypeScript [libxumux](https://github.com/deftai/libxumux), passes the spec's [conformance vectors](https://github.com/deftai/xumux/tree/main/conformance) and behavioural suite, and is tested against libxumux in both directions. 0.x minor versions refuse each other (CLOSE 4006), so a 0.3 peer and a 0.2 peer, such as libxumux up to v0.4.0, cannot connect; library releases up to v0.3.0 implement protocol 0.2. [xumcat](https://github.com/deftai/xumcat), netcat for xumux, is built on it.

```sh
go get github.com/deftai/libxumux-go
```

The package name is `xumux`:

```go
import xumux "github.com/deftai/libxumux-go"
```

The only dependency is the standard library. Tests also use testify and `golang.org/x/net/nettest`. WebSocket support is in the separate [`ws`](#websocket-the-ws-module) module.

## Use

```go
// Client: any byte stream (TCP, TLS, a Unix socket, stdio) via NewStreamConn,
// or a message transport such as a WebSocket via NewMessageConn.
s, res, err := xumux.Connect(ctx, xumux.NewStreamConn(conn), xumux.ClientConfig{
	Application: "myapp/1",
	Channels:    []xumux.ChannelSpec{{Name: "data", Reliable: true, Ordered: true}},
}, xumux.Options{})
ch := res.Channels[0]                 // channels requested in HELLO
ch.Send(ctx, 0x01, []byte("hello"))   // blocks for flow-control credit
msg, err := ch.Receive(ctx)           // io.EOF after a normal close
rtt, err := ch.Ping(ctx)              // per-channel round-trip time
more, err := s.OpenChannel(ctx, xumux.ChannelSpec{Name: "more", Reliable: true, Ordered: true})

// Server: the Acceptor decides on every channel the peer opens.
s, hello, err := xumux.Accept(ctx, xumux.NewStreamConn(conn), xumux.ServerConfig{}, xumux.Options{
	Acceptor: func(ctx context.Context, req xumux.ChannelRequest) (xumux.Handler, error) {
		return func(ch *xumux.Channel) { /* serve ch */ }, nil
	},
})
```

See the [package example](example_test.go) for a complete echo server and client.

A server can take channels from a [`ChannelListener`](listener.go) instead, in the style of `net.Listener`:

```go
l := xumux.NewChannelListener()
s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
for {
	ch, err := l.Accept(ctx)
	...
}
```

## Extensions

xumux 0.3 lets HELLO and WELCOME carry `extensionParams`, parameters for each requested or accepted extension (core §3.8). The library negotiates them; what they mean is up to the extension:

```go
// Client: request ext/1 with parameters, and validate the server's.
s, res, err := xumux.Connect(ctx, fc, xumux.ClientConfig{
	Extensions:      []string{"ext/1"},
	ExtensionParams: map[string]json.RawMessage{"ext/1": json.RawMessage(`{"level":3}`)},
	CheckExtensionParams: func(name string, p json.RawMessage) error { ... }, // error: CLOSE 4001
}, xumux.Options{})

// Server: accept ext/1 and answer its parameters; an error leaves it out of WELCOME.
s, hello, err := xumux.Accept(ctx, fc, xumux.ServerConfig{
	Extensions: []string{"ext/1"},
	NegotiateExtension: func(ctx context.Context, name string, p json.RawMessage) (json.RawMessage, error) { ... },
}, xumux.Options{})

s.Extensions()                     // accepted extensions
local, peer := s.ExtensionParams("ext/1")
```

Parameters for extensions that are not accepted are ignored, and a server never sends any for them. `extensionParams` that is not a JSON object closes the connection with 4001.

An extension can also claim the first frame of a new transport, to continue an existing connection instead of starting one with HELLO (core §3.1). `ServerConfig.FirstFrames` maps a control type in 0x30–0x7F to a `FirstFrameHandler`, which answers in place of WELCOME and takes the transport; `Accept` then returns `ErrFirstFrameHandled`. Without a handler for its type, a first frame other than HELLO closes with 1002.

## Session resume

[`session-resume/1`](https://github.com/deftai/xumux/blob/main/spec/extensions/session-resume.md) lets a client that loses its transport (Wi-Fi to cellular, sleep and wake, a proxy restart) continue the **same** session on a new one: the same `*Session` and `*Channel` values, with no message lost or duplicated. Both sides opt in:

```go
// Server: one ResumeServer for every Accept. It answers RESUME on new
// transports, and Accept returns ErrFirstFrameHandled for those.
rs := xumux.NewResumeServer(xumux.ResumeServerOptions{Timeout: 2 * time.Minute})
s, hello, err := xumux.Accept(ctx, fc, xumux.ServerConfig{
	Resume: rs,
	// Required if the server authenticates clients: check that a RESUME
	// comes from the identity that s was authenticated as.
	AuthorizeResume: func(ctx context.Context, req *xumux.ResumeRequest, s *xumux.Session) error { ... },
}, opts)
if errors.Is(err, xumux.ErrFirstFrameHandled) {
	return // a resumed session took this transport
}

// Client: Redial opens a new transport to the same server.
s, res, err := xumux.Connect(ctx, fc, xumux.ClientConfig{
	Resume: &xumux.ResumeClient{
		Redial:     func(ctx context.Context) (xumux.FrameConn, error) { ... },
		OnFallback: func(s *xumux.Session, res *xumux.ConnectResult, err error) { ... }, // session lost: a new one
	},
}, opts)
```

- **While detached**, sends wait (flow control still applies), keepalive is suspended, and `Session.Detached()` is true. A keepalive timeout also detaches rather than closing with 4008. The client redials with backoff until the server's `resumeTimeout`, retrying after CLOSE 1001, 4004, 4007 or 4011 and giving up on any other refusal. A session that is not resumed in time ends on both sides with `CloseInfo()` code 4101 (RESUME_EXPIRED) and `Lost` set. The server keeps the session at least that long (`ResumeServerOptions.Timeout`, default 120 s), and at most `MaxDetached` detached sessions in all.
- **What carries over.** Each side keeps a copy of every frame it sent on channel 0 and on reliable, ordered channels until the peer's RECEIPT covers it, and retransmits what the peer lacks after a resume. Every channel in this library is on the base path, so all of them carry over. A message cut off by the drop completes from the retransmitted fragments.
- **Tokens** are 128-bit, single use, compared in constant time, and kept in memory only. Each resume issues a new one, and the server can rotate them (`RotateEvery`). They are bearer secrets: use an encrypted transport except on local bindings (session-resume §8).
- **Refusals.** If the server no longer has the session (4100, 4101), cannot replay it (4102, 4103), no longer implements the extension (1002) or speaks another version (4006), the session ends with that code. With `OnFallback`, the library then connects again with HELLO. Closing a session, from either side, ends it for good.

## Streams: `net.Conn` over xumux

A core channel carries typed messages. For byte streams, the [`pipe`](pipe) package implements the [`pipe/1`](https://github.com/deftai/xumux/blob/main/spec/profiles/pipe.md) profile, where each pipe is a `net.Conn` with `CloseWrite` for half-close. `io.Copy`, `bufio`, `crypto/tls` and `net/http` work over it unchanged:

```go
import "github.com/deftai/libxumux-go/pipe"

// Client: one pipe per HTTP connection.
hc := &http.Client{Transport: &http.Transport{
	DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return pipe.Dial(ctx, session, "http", "")
	},
}}

// Server: a net.Listener for the pipes peers open.
l := pipe.NewListener()
s, _, err := xumux.Accept(ctx, fc, xumux.ServerConfig{}, xumux.Options{Acceptor: l.Acceptor()})
http.Serve(l, handler)
```

- **Flow control is backpressure.** Credit goes back to the peer only as `Read` hands bytes to the caller, so a reader that stops reading stops the writer.
- **Deadlines** (`SetDeadline`, `SetReadDeadline`, `SetWriteDeadline`) work as on a socket, including on calls already waiting. A timeout is a `net.Error` with `Timeout()` true and matches `os.ErrDeadlineExceeded`.
- **Ending a pipe:** `CloseWrite` sends EOF and reading goes on. `Close` sends EOF if needed and closes the channel. `Reset` reports a failure to the peer, whose `Read` returns a `*pipe.ResetError`.
- **Targets:** `pipe.Dial` can name a target for the peer to connect the pipe to. A responder must check targets against an allow-list (`Listener.Check`), or it becomes an open proxy.

`pipe.Conn` passes `nettest.TestConn`, the conformance suite the Go team uses for `net.Conn` implementations.

## WebSocket: the `ws` module

[`ws`](ws) implements the xumux [WebSocket binding](https://github.com/deftai/xumux/blob/main/spec/bindings/websocket.md): subprotocol `xumux`, one frame per binary message, text messages refused with CLOSE 1002, and a WebSocket close of 1000 (1001 when going away) after the xumux CLOSE. It is a separate module, so the core keeps no dependencies; it uses [`github.com/coder/websocket`](https://github.com/coder/websocket).

```sh
go get github.com/deftai/libxumux-go/ws
```

```go
import "github.com/deftai/libxumux-go/ws"

// Client
fc, _, err := ws.Dial(ctx, "wss://example.com/xumux", nil)
s, res, err := xumux.Connect(ctx, fc, cfg, xumux.Options{})

// Server: Handler upgrades, refuses clients without the subprotocol (1008),
// and closes the connection when the callback returns. ws.Accept(w, r, opts)
// does the upgrade alone.
http.Handle("/xumux", ws.Handler(nil, func(fc xumux.FrameConn, r *http.Request) {
	s, hello, err := xumux.Accept(r.Context(), fc, xumux.ServerConfig{}, opts)
	if err == nil {
		<-s.Done()
	}
}))
```

Set `DialOptions.MaxFramePayload` / `AcceptOptions.MaxFramePayload` to the session's `Limits.MaxFramePayload` if you change it; it sizes the WebSocket read limit. Cross-origin browser requests are refused unless listed in `AcceptOptions.OriginPatterns`.

The module is versioned on its own, with tags of the form `ws/vX.Y.Z` (Go's rule for nested modules), and `ws/go.mod` requires a released core version. A change that needs new core API lands in two steps: release the core, then bump the requirement in `ws/go.mod` and tag `ws/v…`.

## WebRTC

The [`webrtc`](webrtc) module implements the [WebRTC binding](https://github.com/deftai/xumux/blob/main/spec/bindings/webrtc.md): the whole connection over one reliable, ordered DataChannel labelled `xumux`, one frame per binary message. It is a separate module, so only programs that use it depend on [pion](https://github.com/pion/webrtc):

```sh
go get github.com/deftai/libxumux-go/webrtc
```

xumux does not do signalling. Create the PeerConnection and exchange SDP and ICE as the application likes, and hand the DataChannel over:

```go
import "github.com/deftai/libxumux-go/webrtc"

dc, err := webrtc.CreateDataChannel(pc)            // negotiated, id 0: both sides, before the offer
t, err := webrtc.NewTransport(dc, webrtc.Config{}) // straight away, before it can receive
// ... signalling ...
s, res, err := webrtc.Connect(ctx, t, cfg, opts)   // waits for the DataChannel to open; webrtc.Accept on the server
```

- **Backpressure:** `WriteMessage` waits while the DataChannel's `BufferedAmount` is above `Config.HighWaterMark` (1 MiB), until `OnBufferedAmountLow` reports it drained to half. Received messages pass through a small bounded queue from pion's read loop, so a session that stops reading stops SCTP rather than buffering.
- **Message size:** frames this side sends stay within the peer's `a=max-message-size`. pion accepts messages of up to 1 GiB by default; `SettingEngine.SetSCTPMaxMessageSize(webrtc.RecommendedMaxMessageSize)` limits that to one default-sized frame.
- **Closing:** the xumux CLOSE exchange runs first, then the DataChannel closes. The PeerConnection is the application's to close, since it may also carry media tracks. If the DataChannel closes unexpectedly, the session ends as lost.
- A DataChannel created in-band works too: call `NewTransport` inside `OnDataChannel`. `NewTransport` checks the label, protocol and reliability, and takes over the DataChannel's event handlers.

## What's implemented

- Framing over byte streams (with the `XMUX` magic) and message transports.
- The HELLO/WELCOME handshake: version negotiation, limits, auth, channels opened in HELLO, `extensionParams`, and a server hook for extension-defined first frames.
- Channels: open, accept or refuse (per channel, or through a `ChannelListener`), reliable/ordered flags, metadata, fragmentation and reassembly, ID reuse after close (§5.4).
- The `pipe/1` profile as `net.Conn` and `net.Listener` (package `pipe`).
- Flow control: per-channel credit with receive-window auto-tuning, bounded per connection.
- The WebRTC binding over one DataChannel (module `webrtc`).
- Probes and keepalive: PING/PONG per channel and per connection.
- The `session-resume/1` extension: detach on transport loss, resume on a new transport with retransmission, token rotation and takeover.
- Close codes and errors as in the spec.

**Not implemented yet:** the `webrtc-multichannel/1` extension and the QUIC binding.

## Limits

A peer can't make this side use memory without limit. The `Options` defaults are:

| Option | Default | |
|---|---|---|
| `MaxChannels` | 1024 | Channels open or being opened, HELLO channels included. Past it, opens are refused with 4002 |
| `Limits.InitialWindow` | 256 KiB | Receive credit granted per channel |
| `MaxChannelWindow` | 16 MiB | How far auto-tuning may grow one channel's window |
| `ConnectionWindow` | 64 MiB | Stop growing windows once their sum reaches this |
| `MaxControlQueue` | 1 MiB | Queued control and probe replies. A peer that never reads them is closed with 4004 (negative: off) |

## Development

```sh
make check                                   # fmt, vet, race tests, coverage, ws module
make test/ws                                 # the nested ws module only
make test/interop LIBXUMUX_DIR=../libxumux   # against a built libxumux checkout
make test/fuzz FUZZTIME=30s                  # every fuzz target
```

`testdata/vectors.json` is copied from [`deftai/xumux` `conformance/vectors.json`](https://github.com/deftai/xumux/tree/main/conformance). Update it when the spec changes.

This code was developed in the xumcat repository, and its history came along when it moved here.

## License

MIT
