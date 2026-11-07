package xumux

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"unicode/utf8"
)

// ChannelRequest asks for a channel (HELLO channels and OPEN_CHANNEL).
type ChannelRequest struct {
	ID                uint16         `json:"id"`
	Name              string         `json:"name"`
	Reliable          bool           `json:"reliable"`
	Ordered           bool           `json:"ordered"`
	MaxRetransmits    *int           `json:"maxRetransmits,omitempty"`
	MaxPacketLifeTime *int           `json:"maxPacketLifeTime,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

// Properties are a channel's effective delivery properties.
type Properties struct {
	Reliable          bool `json:"reliable"`
	Ordered           bool `json:"ordered"`
	MaxRetransmits    *int `json:"maxRetransmits,omitempty"`
	MaxPacketLifeTime *int `json:"maxPacketLifeTime,omitempty"`
}

// ChannelAccept answers a request (WELCOME channels and CHANNEL_ACK).
type ChannelAccept struct {
	ID uint16 `json:"id"`
	Properties
}

// ChannelRefusal refuses a request (WELCOME rejected and CHANNEL_REJECT).
type ChannelRefusal struct {
	ID     uint16 `json:"id"`
	Code   int    `json:"code"`
	Reason string `json:"reason,omitempty"`
}

// Limits are the limits a receiver declares for what it accepts (core §3.4).
type Limits struct {
	MaxFramePayload uint32 `json:"maxFramePayload"`
	MaxMessageSize  uint32 `json:"maxMessageSize"`
	InitialWindow   uint32 `json:"initialWindow"`
}

// Hello is the client's HELLO.
type Hello struct {
	Version      [3]int           `json:"version"`
	Application  string           `json:"application,omitempty"`
	Extensions   []string         `json:"extensions,omitempty"`
	Limits                        // flattened
	PingInterval float64          `json:"pingInterval"`
	PingTimeout  float64          `json:"pingTimeout"`
	Channels     []ChannelRequest `json:"channels,omitempty"`
	Auth         map[string]any   `json:"auth,omitempty"`
}

// Welcome is the server's WELCOME.
type Welcome struct {
	Version      [3]int           `json:"version"`
	Extensions   []string         `json:"extensions,omitempty"`
	Limits                        // flattened
	PingInterval float64          `json:"pingInterval"`
	PingTimeout  float64          `json:"pingTimeout"`
	Channels     []ChannelAccept  `json:"channels,omitempty"`
	Rejected     []ChannelRefusal `json:"rejected,omitempty"`
}

type closeChannelMsg struct {
	ID     uint16 `json:"id"`
	Code   int    `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type closeMsg struct {
	Code   int    `json:"code"`
	Reason string `json:"reason,omitempty"`
}

// ErrorMessage is a received ERROR (core §9.2). It is informational.
type ErrorMessage struct {
	Code    int     `json:"code"`
	Channel *uint16 `json:"channel,omitempty"`
	Reason  string  `json:"reason,omitempty"`
}

type windowEntry struct {
	Channel   uint16
	Increment uint32
}

func controlFrame(typ byte, v any) Frame {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only our own types are marshalled
	}
	return Frame{Channel: ControlChannel, Type: typ, Payload: b}
}

func windowUpdateFrame(entries []windowEntry) Frame {
	b := make([]byte, 6*len(entries))
	for i, e := range entries {
		binary.BigEndian.PutUint16(b[6*i:], e.Channel)
		binary.BigEndian.PutUint32(b[6*i+2:], e.Increment)
	}
	return Frame{Channel: ControlChannel, Type: TypeWindowUpdate, Payload: b}
}

func probeFrame(ch uint16, typ byte, token uint32) Frame {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, token)
	return Frame{Channel: ch, Type: typ, Flags: FlagProbe, Payload: b}
}

// ─── Decoding (core §4.1) ───────────────────────────────────────────────────

// obj parses a JSON object, returning its raw fields. Anything else is 4001.
func obj(payload []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(payload) {
		return nil, protoErr(CodeInvalidMessage, "control payload is not UTF-8")
	}
	t := bytes.TrimLeft(payload, " \t\r\n")
	if len(t) == 0 || t[0] != '{' {
		return nil, protoErr(CodeInvalidMessage, "control payload is not a JSON object")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, protoErr(CodeInvalidMessage, "control payload: %v", err)
	}
	return m, nil
}

// field decodes m[k] into dst. Missing fields are an error only if required.
// JSON null counts as missing.
func field(m map[string]json.RawMessage, k string, dst any, required bool) error {
	raw, ok := m[k]
	if !ok || string(raw) == "null" {
		if required {
			return protoErr(CodeInvalidMessage, "missing field %q", k)
		}
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return protoErr(CodeInvalidMessage, "field %q: %v", k, err)
	}
	return nil
}

func fields(m map[string]json.RawMessage, specs ...fieldSpec) error {
	for _, s := range specs {
		if err := field(m, s.k, s.dst, s.req); err != nil {
			return err
		}
	}
	return nil
}

type fieldSpec struct {
	k   string
	dst any
	req bool
}

func req(k string, dst any) fieldSpec { return fieldSpec{k, dst, true} }
func opt(k string, dst any) fieldSpec { return fieldSpec{k, dst, false} }

func decodeVersion(m map[string]json.RawMessage) ([3]int, error) {
	var v []int
	if err := field(m, "version", &v, true); err != nil {
		return [3]int{}, err
	}
	if len(v) != 3 || v[0] < 0 || v[1] < 0 || v[2] < 0 {
		return [3]int{}, protoErr(CodeInvalidMessage, "version must be [major, minor, patch]")
	}
	return [3]int{v[0], v[1], v[2]}, nil
}

func decodeLimits(m map[string]json.RawMessage) (Limits, error) {
	l := Limits{DefaultMaxFramePayload, DefaultMaxMessageSize, DefaultInitialWindow}
	err := fields(m, opt("maxFramePayload", &l.MaxFramePayload), opt("maxMessageSize", &l.MaxMessageSize), opt("initialWindow", &l.InitialWindow))
	return l, err
}

// checkLimits applies the bounds of core §3.4.
func checkLimits(l Limits) error {
	switch {
	case l.MaxFramePayload < MinMaxFramePayload:
		return protoErr(CodeInvalidMessage, "maxFramePayload must be >= %d", MinMaxFramePayload)
	case l.MaxMessageSize < l.MaxFramePayload:
		return protoErr(CodeInvalidMessage, "maxMessageSize must be >= maxFramePayload")
	case l.InitialWindow < l.MaxMessageSize:
		return protoErr(CodeInvalidMessage, "initialWindow must be >= maxMessageSize")
	case l.InitialWindow > MaxWindow:
		return protoErr(CodeInvalidMessage, "initialWindow must be <= 2^31-1")
	}
	return nil
}

func decodeSeconds(m map[string]json.RawMessage, k string, def float64) (float64, error) {
	v := def
	if err := field(m, k, &v, false); err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, protoErr(CodeInvalidMessage, "%s must be >= 0", k)
	}
	return v, nil
}

func decodeChannelRequest(raw json.RawMessage) (ChannelRequest, error) {
	m, err := obj(raw)
	if err != nil {
		return ChannelRequest{}, err
	}
	var r ChannelRequest
	err = fields(m, req("id", &r.ID), req("name", &r.Name), req("reliable", &r.Reliable), req("ordered", &r.Ordered),
		opt("maxRetransmits", &r.MaxRetransmits), opt("maxPacketLifeTime", &r.MaxPacketLifeTime), opt("metadata", &r.Metadata))
	return r, err
}

func decodeChannelAccept(raw json.RawMessage) (ChannelAccept, error) {
	m, err := obj(raw)
	if err != nil {
		return ChannelAccept{}, err
	}
	var a ChannelAccept
	err = fields(m, req("id", &a.ID), req("reliable", &a.Reliable), req("ordered", &a.Ordered),
		opt("maxRetransmits", &a.MaxRetransmits), opt("maxPacketLifeTime", &a.MaxPacketLifeTime))
	return a, err
}

func decodeChannelRefusal(raw json.RawMessage) (ChannelRefusal, error) {
	m, err := obj(raw)
	if err != nil {
		return ChannelRefusal{}, err
	}
	var r ChannelRefusal
	err = fields(m, req("id", &r.ID), req("code", &r.Code), opt("reason", &r.Reason))
	return r, err
}

func decodeList[T any](m map[string]json.RawMessage, k string, dec func(json.RawMessage) (T, error)) ([]T, error) {
	var raws []json.RawMessage
	if err := field(m, k, &raws, false); err != nil {
		return nil, err
	}
	out := make([]T, 0, len(raws))
	for _, r := range raws {
		v, err := dec(r)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func decodeHello(p []byte) (Hello, error) {
	var h Hello
	m, err := obj(p)
	if err != nil {
		return h, err
	}
	if h.Version, err = decodeVersion(m); err != nil {
		return h, err
	}
	if h.Limits, err = decodeLimits(m); err != nil {
		return h, err
	}
	if h.PingInterval, err = decodeSeconds(m, "pingInterval", 30); err != nil {
		return h, err
	}
	if h.PingTimeout, err = decodeSeconds(m, "pingTimeout", 10); err != nil {
		return h, err
	}
	if err = fields(m, opt("application", &h.Application), opt("extensions", &h.Extensions), opt("auth", &h.Auth)); err != nil {
		return h, err
	}
	h.Channels, err = decodeList(m, "channels", decodeChannelRequest)
	return h, err
}

func decodeWelcome(p []byte) (Welcome, error) {
	var w Welcome
	m, err := obj(p)
	if err != nil {
		return w, err
	}
	if w.Version, err = decodeVersion(m); err != nil {
		return w, err
	}
	if w.Limits, err = decodeLimits(m); err != nil {
		return w, err
	}
	if w.PingInterval, err = decodeSeconds(m, "pingInterval", 30); err != nil {
		return w, err
	}
	if w.PingTimeout, err = decodeSeconds(m, "pingTimeout", 10); err != nil {
		return w, err
	}
	if err = field(m, "extensions", &w.Extensions, false); err != nil {
		return w, err
	}
	if w.Channels, err = decodeList(m, "channels", decodeChannelAccept); err != nil {
		return w, err
	}
	w.Rejected, err = decodeList(m, "rejected", decodeChannelRefusal)
	return w, err
}

func decodeCloseChannel(p []byte) (closeChannelMsg, error) {
	m, err := obj(p)
	if err != nil {
		return closeChannelMsg{}, err
	}
	c := closeChannelMsg{Code: CodeNormal}
	err = fields(m, req("id", &c.ID), opt("code", &c.Code), opt("reason", &c.Reason))
	return c, err
}

func decodeClose(p []byte) (closeMsg, error) {
	m, err := obj(p)
	if err != nil {
		return closeMsg{}, err
	}
	var c closeMsg
	err = fields(m, req("code", &c.Code), opt("reason", &c.Reason))
	return c, err
}

func decodeError(p []byte) (ErrorMessage, error) {
	m, err := obj(p)
	if err != nil {
		return ErrorMessage{}, err
	}
	var e ErrorMessage
	err = fields(m, req("code", &e.Code), opt("channel", &e.Channel), opt("reason", &e.Reason))
	return e, err
}

func decodeWindowUpdate(p []byte) ([]windowEntry, error) {
	if len(p) == 0 || len(p)%6 != 0 {
		return nil, protoErr(CodeInvalidMessage, "WINDOW_UPDATE length %d is not a positive multiple of 6", len(p))
	}
	out := make([]windowEntry, len(p)/6)
	for i := range out {
		out[i] = windowEntry{binary.BigEndian.Uint16(p[6*i:]), binary.BigEndian.Uint32(p[6*i+2:])}
		if out[i].Increment == 0 || out[i].Increment > MaxWindow {
			return nil, protoErr(CodeInvalidMessage, "WINDOW_UPDATE increment %d out of range", out[i].Increment)
		}
	}
	return out, nil
}

// probeToken returns the token of a well-formed probe payload.
func probeToken(p []byte) (uint32, bool) {
	if len(p) != 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(p), true
}

// VersionsCompatible applies core §3.5.
func VersionsCompatible(a, b [3]int) bool {
	if a[0] != b[0] {
		return false
	}
	return a[0] != 0 || a[1] == b[1]
}
