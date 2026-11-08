# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/deftai/libxumux-go/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/deftai/libxumux-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/deftai/libxumux-go/releases/tag/v0.1.0
