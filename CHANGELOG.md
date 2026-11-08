# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.2] - 2026-11-08

Also the first releases of the nested modules `ws` (`ws/v0.1.0`) and `webrtc` (`webrtc/v0.1.0`), which require core v0.2.1 or later.

### Changed

- Requires Go 1.25 (was 1.24). The test-only dependency `golang.org/x/net` moves from v0.30.0 to v0.55.0, which clears three advisories (HTML parser DoS, XSS, proxy bypass via IPv6 zone IDs). The library never used the affected packages, but `x/net` v0.55.0 itself needs Go 1.25.

### Fixed

- A `pipe.Conn` that failed (the peer sent RESET, or a message type pipe/1 doesn't define) could close its channel with 1000 and send EOF when the application called `Close` straight after the failed `Read`, as `io.Copy`-style code does. The failure's close started in the background and the `Close` usually won. `Close` now closes with the failure's code (1001 after RESET, 1002 after a protocol error) and sends no EOF, and `Reset` on a failed pipe sends no RESET. Found by the xumux behavioural conformance suite.

### Added

- `conformance/adapter`: runs this library as the server under test for the xumux behavioural conformance suite (deftai/xumux `conformance/behaviour`), via `make test/conformance XUMUX_DIR=...`.
- Module `github.com/deftai/libxumux-go/ws` (nested, tagged `ws/vX.Y.Z`): the xumux WebSocket binding on `github.com/coder/websocket`. `ws.Dial` returns an `xumux.FrameConn`; `ws.Accept` and `ws.Handler` serve one. It negotiates the `xumux` subprotocol (Dial fails and Accept closes with 1008 without it), carries one frame per binary message, refuses text messages (xumux CLOSE 1002, then WebSocket close 1002), sizes the read limit from `MaxFramePayload` (channel 0's 64 KiB always fits), and closes the WebSocket with 1000, or 1001 after a xumux CLOSE 1001. A peer's 1000/1001 close reads as `io.EOF`. The core module still has no dependencies. First release: `ws/v0.1.0`, requiring core v0.2.1.
- Module `github.com/deftai/libxumux-go/webrtc`: the WebRTC binding, the whole connection over one reliable, ordered DataChannel, using pion. `CreateDataChannel` makes the negotiated `xumux` channel; `NewTransport` wraps it as a `MessageTransport`, with send backpressure from `BufferedAmount` and `OnBufferedAmountLow`; `Connect` and `Accept` wait for it to open and keep sent frames within the peer's maximum message size. The core module still depends on the standard library only.

## [0.2.1] - 2026-11-08

### Added

- `pipe.Conn.WriteTo`, so `io.Copy` from a pipe writes each DATA payload straight to the destination and returns credit only once the destination has taken it. pipe/1 requires credit to follow writes to the local end; with only `Read`, `io.Copy` returned credit as bytes reached its 32 KiB buffer. It also saves a copy.

## [0.2.0] - 2026-11-08

### Added

- Package `pipe`: the `pipe/1` profile as standard Go streams.
  - `pipe.Conn` is a `net.Conn` with `CloseWrite`, plus `Reset` to report a failure. It has read and write deadlines, and credit is returned only as `Read` hands bytes over. It passes `nettest.TestConn`.
  - `pipe.Dial` opens a pipe; `pipe.Listener` is a `net.Listener` for the pipes peers open, with a `Check` hook for target allow-lists. `net/http` runs over them unchanged.
  - `Spec`, `IsPipe`, `Target`, `ResetError` and the message-type constants.
- `ChannelListener`: take peer-opened channels from `Accept`, `net.Listener` style, instead of a `Handler` per channel. One listener can serve many sessions.
- `Channel.SendPrefix`: sends as much of a payload as fits in one unfragmented frame and the available credit, and reports how much. Byte streams use it, since pipe/1 DATA must not be fragmented; `Send` fragments a message when credit runs short.

## [0.1.0] - 2026-11-08

### Added

- First standalone release. The package was developed inside [xumcat](https://github.com/deftai/xumcat), where it last shipped in xumcat v0.3.1, and moved here with its history. The API is unchanged apart from the import path, now `github.com/deftai/libxumux-go` (package `xumux`).
- A runnable package example: an echo server and client over TCP.

[Unreleased]: https://github.com/deftai/libxumux-go/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/deftai/libxumux-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/deftai/libxumux-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/deftai/libxumux-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/deftai/libxumux-go/releases/tag/v0.1.0
