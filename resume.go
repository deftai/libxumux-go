package xumux

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"
)

// This file implements the session-resume/1 extension: a client that loses
// its transport continues the same connection on a new one, with no frame
// lost or duplicated on reliable, ordered channels. Each side counts the
// replayed frames it sends and receives on every replayed channel, keeps a
// copy of each sent one until the peer's RECEIPT covers it, and after a
// resume retransmits what the peer has not received, in the original order.
// It sits on the base-path seam of pathHooks (core §11.1).

// ExtensionSessionResume is the name of the session-resume/1 extension.
const ExtensionSessionResume = "session-resume/1"

// Control types of session-resume/1 (registry).
const (
	TypeResumeToken byte = 0x30 // S→C: the token to resume with
	TypeResume      byte = 0x31 // C→S: first frame on a new transport
	TypeResumed     byte = 0x32 // S→C: the answer to RESUME
	TypeReceipt     byte = 0x33 // both: replayed frames received
)

// Codes of session-resume/1 (§9).
const (
	CodeResumeNotFound      = 4100 // unknown or invalid token, or the session is gone
	CodeResumeExpired       = 4101 // the session was discarded after resumeTimeout
	CodeResumeDataLost      = 4102 // a frame needed for the resume is no longer retained
	CodeResumeStateMismatch = 4103 // receive counts that cannot be right
)

// Defaults for session-resume/1.
const (
	// DefaultResumeTimeout is how long a server keeps a detached session.
	DefaultResumeTimeout = 120 * time.Second
	// DefaultResumeFrameLimit caps the unacknowledged frames per channel
	// (§5); MinResumeFrameLimit is the least the spec allows.
	DefaultResumeFrameLimit = 4096
	MinResumeFrameLimit     = 128
	// DefaultMaxControlRetention caps unacknowledged channel-0 bytes; past
	// it the connection closes with 4004 (§5).
	DefaultMaxControlRetention = 1 << 20
	// DefaultMaxDetached caps the detached sessions a ResumeServer keeps.
	DefaultMaxDetached = 1024
)

const (
	maxResumeTimeout = 3600 * time.Second
	maxTokenLen      = 512
	// receiptEntriesMax keeps one RECEIPT within the channel-0 limit.
	receiptEntriesMax = ControlMaxPayload / 6
)

// ResumeClient configures session-resume/1 for a client
// (ClientConfig.Resume). Only Redial is required.
type ResumeClient struct {
	// Redial opens a new transport to the same server with the same
	// binding, for a resume (and for the fallback HELLO). ctx ends when the
	// server's resumeTimeout has passed since the transport was lost.
	Redial func(ctx context.Context) (FrameConn, error)
	// Auth is sent in RESUME (nil: ClientConfig.Auth), for servers that
	// authenticate it as they did HELLO (§8).
	Auth map[string]any
	// RetryDelay is the wait after a failed attempt, doubling up to
	// MaxRetryDelay (defaults 250 ms and 5 s).
	RetryDelay, MaxRetryDelay time.Duration
	// FrameLimit caps unacknowledged frames per channel (default 4096, at
	// least 128). MaxControlRetention caps unacknowledged channel-0 bytes
	// (default 1 MiB; past it the connection closes with 4004).
	FrameLimit          int
	MaxControlRetention int64
	// OnDetach is called (in its own goroutine) when the transport is lost
	// and the session detaches; OnResume when it has resumed.
	OnDetach func(err error)
	OnResume func()
	// OnFallback, if set, is called (in its own goroutine) after the server
	// refused the resume because the session is gone (41xx), no longer
	// implements the extension (1002) or the version (4006), or when RESUME
	// would not fit in 64 KiB. The old session has ended by then; the
	// library has connected anew with HELLO on a transport from Redial, with
	// the same ClientConfig and Options, and passes the new session, or the
	// error if that failed (§9). If nil, the session just ends.
	OnFallback func(s *Session, res *ConnectResult, err error)
}

// ResumeServerOptions configure a ResumeServer. Zero values select the
// defaults.
type ResumeServerOptions struct {
	// Timeout is resumeTimeout: how long a detached session is kept
	// (default 120 s, between 1 s and 3600 s, whole seconds).
	Timeout time.Duration
	// MaxDetached caps the detached sessions kept at once (default 1024).
	// Past it the one detached longest is discarded; resuming it then
	// fails with 4100 (§8).
	MaxDetached int
	// RotateEvery, if positive, sends a new RESUME_TOKEN that often (§3.1).
	RotateEvery time.Duration
	// FrameLimit and MaxControlRetention: as in ResumeClient.
	FrameLimit          int
	MaxControlRetention int64
}

// ResumeServer holds the server side of session-resume/1: the tokens of
// sessions that negotiated it, and the sessions that are detached. Use one
// for every Accept of a server (ServerConfig.Resume). The zero value is
// not usable; call NewResumeServer.
type ResumeServer struct {
	opts ResumeServerOptions

	mu       sync.Mutex
	byToken  map[[32]byte]*resumeState // by SHA-256 of the token
	expired  map[[32]byte]time.Time    // tokens of sessions that timed out
	expOrder [][32]byte
	detached map[*resumeState]time.Time
}

// NewResumeServer returns a ResumeServer.
func NewResumeServer(opts ResumeServerOptions) *ResumeServer {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultResumeTimeout
	}
	opts.Timeout = min(max(opts.Timeout.Round(time.Second), time.Second), maxResumeTimeout)
	if opts.MaxDetached <= 0 {
		opts.MaxDetached = DefaultMaxDetached
	}
	return &ResumeServer{
		opts: opts, byToken: map[[32]byte]*resumeState{}, expired: map[[32]byte]time.Time{},
		detached: map[*resumeState]time.Time{},
	}
}

// Detached returns the number of detached sessions the server holds.
func (rs *ResumeServer) Detached() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.detached)
}

// ResumeRequest is what AuthorizeResume sees of a RESUME. The token is not
// included: it is a bearer secret that must not be logged (§8).
type ResumeRequest struct {
	Version [3]int
	Auth    map[string]any
}

// ─── State ──────────────────────────────────────────────────────────────────

type retainedFrame struct {
	seq uint64 // order of first sending, across channels
	f   Frame
}

// issuedToken is a token the server has sent (or is sending). need is the
// channel-0 count that covers the frame that carried it.
type issuedToken struct {
	tok         string
	hash        [32]byte
	need        uint32
	sent        bool
	dropOnFirst bool // the token a resume presented: valid until the client's first frame
}

// resumeState is one session's session-resume/1 state, guarded by s.mu.
type resumeState struct {
	s          *Session
	frameLimit uint32
	maxCtl     int64

	seq                       uint64
	tx0, rx0, acked0, report0 uint32
	retained0                 []retainedFrame
	retained0Bytes            int64
	dirty                     bool // a RECEIPT is due
	timeout                   time.Duration

	// Client.
	cl        *ResumeClient
	clientCfg ClientConfig
	opts      Options
	token     string
	gotFirst  bool

	// Server.
	srv            *ResumeServer
	tokens         []*issuedToken
	confirmPending bool
	expired        bool
	detachGen      uint64
	detachTimer    *time.Timer
	rotateTimer    *time.Timer
}

func frameLimit(n int) uint32 {
	if n <= 0 {
		return DefaultResumeFrameLimit
	}
	return uint32(max(n, MinResumeFrameLimit))
}

func controlRetention(n int64) int64 {
	if n <= 0 {
		return DefaultMaxControlRetention
	}
	return n
}

func newClientResume(s *Session, cfg ClientConfig, opts Options) *resumeState {
	return &resumeState{
		s: s, cl: cfg.Resume, clientCfg: cfg, opts: opts,
		frameLimit: frameLimit(cfg.Resume.FrameLimit), maxCtl: controlRetention(cfg.Resume.MaxControlRetention),
	}
}

func newServerResume(s *Session, rs *ResumeServer) *resumeState {
	r := &resumeState{
		s: s, srv: rs, timeout: rs.opts.Timeout,
		frameLimit: frameLimit(rs.opts.FrameLimit), maxCtl: controlRetention(rs.opts.MaxControlRetention),
	}
	if rs.opts.RotateEvery > 0 {
		r.rotateTimer = time.AfterFunc(rs.opts.RotateEvery, r.rotate)
	}
	return r
}

// serialGE reports a >= b in serial-number arithmetic (RFC 1982).
func serialGE(a, b uint32) bool { return int32(a-b) >= 0 }

// replayed reports whether c's frames are replayed (§4). Every channel of
// this library uses the base path, so that is reliable and ordered.
func replayed(c *Channel) bool { return c.props.Reliable && c.props.Ordered }

// listed reports whether c belongs in a received list or a RECEIPT: a
// replayed channel in Opening or Open state (§7).
func listed(c *Channel) bool {
	return replayed(c) && (c.state == chanOpening || c.state == chanOpen) && !c.sentClose && !c.recvClose
}

// ─── pathHooks ──────────────────────────────────────────────────────────────

func (r *resumeState) pathLost(err error) bool {
	s := r.s
	if r.cl != nil {
		if r.token == "" {
			return false // no token yet: nothing to resume with
		}
		go r.clientLoop(time.Now())
		if cb := r.cl.OnDetach; cb != nil {
			go cb(err)
		}
		return true
	}
	r.detachGen++
	gen := r.detachGen
	r.detachTimer = time.AfterFunc(r.timeout, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.state == stateOpen && s.path == nil && r.detachGen == gen {
			r.expired = true
			s.finishLocked(CloseError{Code: CodeResumeExpired, Reason: "not resumed within resumeTimeout", Lost: true})
		}
	})
	if victim := r.srv.addDetached(r); victim != nil {
		go victim.discard()
	}
	return true
}

// writing counts and keeps every replayed frame as it is sent (§4, §5).
// Frames that are not replayed (probes, RECEIPT, unreplayed channels) are
// not sent again if the path fails under them.
func (r *resumeState) writing(it *outItem) bool {
	s, f := r.s, it.f
	if f.IsProbe() {
		return true
	}
	if f.Channel == ControlChannel {
		if f.Type == TypeReceipt || f.Type == TypeResume || f.Type == TypeResumed {
			return true
		}
		r.tx0++
		r.seq++
		r.retained0 = append(r.retained0, retainedFrame{r.seq, f})
		r.retained0Bytes += int64(HeaderSize + len(f.Payload))
		if c := it.closeOf; c != nil {
			c.closeCounted, c.closeSentAt = true, r.tx0
		}
		if t := it.tokenOf; t != nil {
			t.sent, t.need = true, r.tx0
		}
		if r.retained0Bytes > r.maxCtl && s.state == stateOpen {
			s.beginCloseLocked(CodeRateLimited, fmt.Sprintf("over %d bytes of channel-0 frames unacknowledged", r.maxCtl), false)
		}
		return true
	}
	c := it.ch
	if c == nil || !replayed(c) {
		return true
	}
	c.txCount++
	r.seq++
	// Send returns once the frame is written and the caller may reuse its
	// buffer, so keep a copy.
	c.retained = append(c.retained, retainedFrame{r.seq, Frame{Channel: f.Channel, Type: f.Type, Flags: f.Flags, Payload: slices.Clone(f.Payload)}})
	return true
}

// canSendLocked reports whether channel id is below its unacknowledged-
// frame limit (§5).
func (r *resumeState) canSendLocked(id uint16) bool {
	c := r.s.chans[id]
	return c == nil || !replayed(c) || c.txCount-c.txAcked < r.frameLimit
}

// receivedLocked counts a received frame and handles this extension's
// control messages. handled means the core does not see the frame.
func (r *resumeState) receivedLocked(f Frame) (handled bool, err error) {
	s := r.s
	if r.confirmPending {
		// Any frame on the transport that carried RESUMED confirms the new
		// token (§3.3).
		r.confirmLocked()
	}
	if r.cl != nil && !r.gotFirst && !(f.IsControl() && f.Type == TypeResumeToken) {
		return true, protoErr(CodeProtocolError, "the server's first frame after WELCOME must be RESUME_TOKEN")
	}
	if f.IsProbe() {
		return false, nil
	}
	if f.Channel == ControlChannel {
		switch f.Type {
		case TypeReceipt:
			return true, r.onReceiptLocked(f.Payload)
		case TypeResume, TypeResumed:
			return true, protoErr(CodeProtocolError, "RESUME or RESUMED during the connection")
		}
		r.rx0++
		r.markDirtyLocked()
		if f.Type == TypeResumeToken {
			if r.cl == nil {
				return true, protoErr(CodeProtocolError, "RESUME_TOKEN from the client")
			}
			return true, r.onTokenLocked(f.Payload)
		}
		return false, nil
	}
	if c := s.chans[f.Channel]; c != nil && listed(c) && c.state == chanOpen {
		c.rxCount++
		r.markDirtyLocked()
	}
	return false, nil
}

// countHeldLocked counts a frame held for c before its answer arrived
// (core §5.2), as it is processed. Such frames are never counted on arrival:
// they come before WELCOME, or (with paths of their own) overtake the ACK.
func (r *resumeState) countHeldLocked(c *Channel) {
	if listed(c) {
		c.rxCount++
		r.markDirtyLocked()
	}
}

func (r *resumeState) markDirtyLocked() {
	r.dirty = true
	r.s.signalWriter()
}

// receiptLocked builds a RECEIPT for every replayed channel with frames
// received since the last one, or nil. force always includes channel 0.
func (r *resumeState) receiptLocked(force bool) *Frame {
	var entries []windowEntry
	if r.rx0 != r.report0 || force {
		entries = append(entries, windowEntry{ControlChannel, r.rx0})
		r.report0 = r.rx0
	}
	r.dirty = false
	for id, c := range r.s.chans {
		if c.rxCount == c.rxReported || !listed(c) {
			continue
		}
		if len(entries) == receiptEntriesMax {
			r.dirty = true // the rest go in the next one
			break
		}
		entries = append(entries, windowEntry{id, c.rxCount})
		c.rxReported = c.rxCount
	}
	if len(entries) == 0 {
		return nil
	}
	f := windowUpdateFrame(entries)
	f.Type = TypeReceipt
	return &f
}

// reportAllLocked records that the peer knows every receive count, as
// after a received list in RESUME or RESUMED.
func (r *resumeState) reportAllLocked() {
	r.report0 = r.rx0
	for _, c := range r.s.chans {
		c.rxReported = c.rxCount
	}
	r.dirty = false
}

func (r *resumeState) onReceiptLocked(p []byte) error {
	if len(p) == 0 || len(p)%6 != 0 {
		return protoErr(CodeInvalidMessage, "RECEIPT length %d is not a positive multiple of 6", len(p))
	}
	s := r.s
	for i := 0; i < len(p); i += 6 {
		id, count := binary.BigEndian.Uint16(p[i:]), binary.BigEndian.Uint32(p[i+2:])
		if id == ControlChannel {
			if err := r.ack0Locked(count, false); err != nil {
				return err
			}
			continue
		}
		c := s.chans[id]
		if c == nil || !listed(c) {
			continue // not replayed, closing or unknown: ignore (§3.4)
		}
		if err := ackChannel(c, count, false); err != nil {
			return err
		}
	}
	s.signalWriter() // a channel at its frame limit may go on
	return nil
}

// ack drops the retained frames a count covers. A count lower than what is
// already acknowledged is stale (ignored) in a RECEIPT, and in a resume
// means a frame the peer needs is gone (4102). One above what was sent is
// 4103.
func ack(acked *uint32, sent, count uint32, retained *[]retainedFrame, resume bool) (dropped []retainedFrame, err error) {
	d := count - *acked
	if int32(d) < 0 {
		if resume {
			return nil, protoErr(CodeResumeDataLost, "the peer needs frames that are no longer retained")
		}
		return nil, nil
	}
	if d > sent-*acked {
		return nil, protoErr(CodeResumeStateMismatch, "the peer acknowledged %d frames, more than were sent", count)
	}
	dropped = (*retained)[:d]
	*retained = slices.Clone((*retained)[d:])
	*acked = count
	return dropped, nil
}

func ackChannel(c *Channel, count uint32, resume bool) error {
	_, err := ack(&c.txAcked, c.txCount, count, &c.retained, resume)
	return err
}

// ack0Locked acknowledges channel-0 frames. A RECEIPT also retires
// replaced tokens; a resume's received list does not (reissueLocked does).
func (r *resumeState) ack0Locked(count uint32, resume bool) error {
	dropped, err := ack(&r.acked0, r.tx0, count, &r.retained0, resume)
	if err != nil {
		return err
	}
	for _, d := range dropped {
		r.retained0Bytes -= int64(HeaderSize + len(d.f.Payload))
	}
	if r.srv != nil && !resume {
		r.tokensAckedLocked(count)
	}
	return nil
}

// ─── Messages ───────────────────────────────────────────────────────────────

type resumeTokenMsg struct {
	Token         string `json:"token"`
	ResumeTimeout int    `json:"resumeTimeout"`
}

type resumeMsg struct {
	Version  [3]int         `json:"version"`
	Token    string         `json:"token"`
	Received [][2]uint32    `json:"received"`
	Auth     map[string]any `json:"auth,omitempty"`
}

type resumedMsg struct {
	Token         string      `json:"token"`
	ResumeTimeout int         `json:"resumeTimeout"`
	Received      [][2]uint32 `json:"received"`
}

func decodeTokenFields(m map[string]json.RawMessage) (string, time.Duration, error) {
	var tok string
	var secs float64
	if err := fields(m, req("token", &tok), req("resumeTimeout", &secs)); err != nil {
		return "", 0, err
	}
	if len(tok) == 0 || len(tok) > maxTokenLen {
		return "", 0, protoErr(CodeInvalidMessage, "token must be 1 to %d bytes", maxTokenLen)
	}
	if secs < 1 || secs > 3600 {
		return "", 0, protoErr(CodeInvalidMessage, "resumeTimeout must be between 1 and 3600")
	}
	return tok, time.Duration(secs * float64(time.Second)), nil
}

// decodeReceived decodes a `received` list: [id, count] pairs with distinct
// IDs (§3.2).
func decodeReceived(m map[string]json.RawMessage) (map[uint16]uint32, error) {
	var raw [][]uint64
	if err := field(m, "received", &raw, true); err != nil {
		return nil, err
	}
	out := make(map[uint16]uint32, len(raw))
	for _, e := range raw {
		if len(e) != 2 || e[0] >= ReservedChannel || e[1] > math.MaxUint32 {
			return nil, protoErr(CodeInvalidMessage, "received entries must be [id, count]")
		}
		id := uint16(e[0])
		if _, dup := out[id]; dup {
			return nil, protoErr(CodeInvalidMessage, "received lists channel %d twice", id)
		}
		out[id] = uint32(e[1])
	}
	if _, ok := out[ControlChannel]; !ok {
		return nil, protoErr(CodeInvalidMessage, "received has no entry for channel 0")
	}
	return out, nil
}

func decodeResume(p []byte) (resumeMsg, map[uint16]uint32, error) {
	var msg resumeMsg
	m, err := obj(p)
	if err != nil {
		return msg, nil, err
	}
	if msg.Version, err = decodeVersion(m); err != nil {
		return msg, nil, err
	}
	if err := fields(m, req("token", &msg.Token), opt("auth", &msg.Auth)); err != nil {
		return msg, nil, err
	}
	rec, err := decodeReceived(m)
	return msg, rec, err
}

func (r *resumeState) receivedListLocked() [][2]uint32 {
	out := [][2]uint32{{ControlChannel, r.rx0}}
	for id, c := range r.s.chans {
		if listed(c) {
			out = append(out, [2]uint32{uint32(id), c.rxCount})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// ─── Replay (§7) ────────────────────────────────────────────────────────────

type replayPlan struct {
	c0    uint32
	count map[*Channel]uint32 // retransmit from the count
	drop  []*Channel          // discard retained frames
}

// planLocked checks the peer's received list against what this side sent,
// and decides, per channel instance, what to retransmit (§7). It changes
// nothing, so a refused resume leaves the session as it was.
func (r *resumeState) planLocked(rec map[uint16]uint32) (*replayPlan, *ProtocolError) {
	s := r.s
	p := &replayPlan{c0: rec[ControlChannel], count: map[*Channel]uint32{}}
	if d := p.c0 - r.acked0; int32(d) < 0 {
		return nil, protoErr(CodeResumeDataLost, "channel-0 frames the peer needs are no longer retained")
	} else if d > r.tx0-r.acked0 {
		return nil, protoErr(CodeResumeStateMismatch, "the peer received %d channel-0 frames, more than were sent", p.c0)
	}
	for id, c := range s.chans {
		if !replayed(c) {
			continue
		}
		// 2. The peer has processed our CLOSE_CHANNEL: an entry for the ID
		// can only be a later instance.
		if c.closeCounted && serialGE(p.c0, c.closeSentAt) {
			p.drop = append(p.drop, c)
			continue
		}
		n, ok := rec[id]
		if !ok {
			// 4. The peer has closed the channel or never heard of it.
			p.drop = append(p.drop, c)
			continue
		}
		// 3. The entry is for this instance.
		if d := n - c.txAcked; int32(d) < 0 {
			return nil, protoErr(CodeResumeDataLost, "frames the peer needs on channel %d are no longer retained", id)
		} else if d > c.txCount-c.txAcked {
			return nil, protoErr(CodeResumeStateMismatch, "the peer received %d frames on channel %d, more than were sent", n, id)
		}
		p.count[c] = n
	}
	for id, n := range rec {
		if id != ControlChannel && s.chans[id] == nil && n != 0 {
			return nil, protoErr(CodeResumeStateMismatch, "the peer received %d frames on unknown channel %d", n, id)
		}
	}
	return p, nil
}

// applyLocked carries out a plan and returns the frames to retransmit, in
// the order they were first sent.
func (r *resumeState) applyLocked(p *replayPlan) []Frame {
	_ = r.ack0Locked(p.c0, true) // checked by planLocked
	all := slices.Clone(r.retained0)
	for c, n := range p.count {
		_ = ackChannel(c, n, true)
		all = append(all, c.retained...)
	}
	for _, c := range p.drop {
		c.retained, c.txAcked = nil, c.txCount
	}
	slices.SortFunc(all, func(a, b retainedFrame) int {
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		}
		return 0
	})
	out := make([]Frame, len(all))
	for i, rf := range all {
		out[i] = rf.f
	}
	return out
}

// resumedLocked finishes a resume on either side, after the new path is
// attached: RTT smoothing starts again (§7), and channels on a path of
// their own, which the transport took with it, close with 1001. Every
// channel of this library is on the base path, so only a channel a peer
// acknowledged as reliable but unordered is affected.
func (r *resumeState) resumedLocked() {
	s := r.s
	reset := func(st *statsRecorder) { st.RTT, st.SRTT, st.RTTVar = 0, 0, 0 }
	reset(&s.stats)
	for _, c := range s.chans {
		reset(&c.stats)
		if c.state == chanOpen && !c.sentClose && c.props.Reliable && !c.props.Ordered {
			s.sendCloseChannelLocked(c, CodeGoingAway, "path lost on resume")
		}
	}
}

func (r *resumeState) endedLocked() {
	if r.detachTimer != nil {
		r.detachTimer.Stop()
	}
	if r.rotateTimer != nil {
		r.rotateTimer.Stop()
	}
	r.retained0 = nil
	if r.srv != nil {
		r.srv.forget(r, r.tokens, r.expired)
		r.tokens = nil
	}
}

// ─── Client ─────────────────────────────────────────────────────────────────

func (r *resumeState) onTokenLocked(p []byte) error {
	m, err := obj(p)
	if err != nil {
		return err
	}
	tok, timeout, err := decodeTokenFields(m)
	if err != nil {
		return err
	}
	r.token, r.timeout, r.gotFirst = tok, timeout, true
	return nil
}

func (r *resumeState) retryDelays() (time.Duration, time.Duration) {
	d, m := r.cl.RetryDelay, r.cl.MaxRetryDelay
	if d <= 0 {
		d = 250 * time.Millisecond
	}
	if m <= 0 {
		m = 5 * time.Second
	}
	return d, max(d, m)
}

// clientLoop redials and resumes until it succeeds, the server refuses, or
// resumeTimeout passes (§6).
func (r *resumeState) clientLoop(lost time.Time) {
	s := r.s
	s.mu.Lock()
	timeout := r.timeout
	s.mu.Unlock()
	ctx, cancel := context.WithDeadline(s.ctx, lost.Add(timeout))
	defer cancel()
	delay, maxDelay := r.retryDelays()
	for {
		if conn, err := r.cl.Redial(ctx); err == nil {
			if r.attempt(ctx, conn) {
				return
			}
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			s.mu.Lock()
			if s.state == stateOpen && s.path == nil {
				s.finishLocked(CloseError{Code: CodeResumeExpired, Reason: "not resumed within resumeTimeout", Lost: true})
			}
			s.mu.Unlock()
			return
		case <-t.C:
		}
		delay = min(2*delay, maxDelay)
	}
}

// resumeOutcome classifies a server's CLOSE in answer to RESUME.
func resumeRetryable(code int) bool {
	switch code {
	case CodeGoingAway, CodeBusy, CodeHelloTimeout, CodeRateLimited:
		return true
	}
	return false
}

func resumeFallback(code int) bool {
	return code == CodeProtocolError || code == CodeVersionMismatch || (code >= CodeResumeNotFound && code <= CodeResumeStateMismatch)
}

// attempt resumes on conn. It reports true once the outcome is final:
// resumed, or the session ended.
func (r *resumeState) attempt(ctx context.Context, conn FrameConn) bool {
	s := r.s
	s.mu.Lock()
	if s.state != stateOpen || s.path != nil {
		s.mu.Unlock()
		conn.Close()
		return true
	}
	auth := r.cl.Auth
	if auth == nil {
		auth = r.clientCfg.Auth
	}
	msg := resumeMsg{Version: Version, Token: r.token, Received: r.receivedListLocked(), Auth: auth}
	s.mu.Unlock()
	frame, err := encodeControl(TypeResume, msg)
	if err != nil {
		// Too large to send: the session is lost; fall back to HELLO (§7).
		conn.Close()
		r.fail(CloseError{Code: CodeGoingAway, Reason: "RESUME does not fit in a control frame", Lost: true}, true)
		return true
	}
	hctx, cancel := context.WithTimeout(ctx, s.opts.HandshakeTimeout)
	defer cancel()
	stop := context.AfterFunc(hctx, func() { conn.Close() })
	defer stop()
	if err := conn.WriteFrame(frame); err != nil {
		conn.Close()
		return false
	}
	f, err := conn.ReadFrame(s.local.MaxFramePayload)
	if err == nil {
		err = validateFrame(f)
	}
	if err != nil {
		conn.Close()
		return false
	}
	if f.IsControl() && f.Type == TypeClose {
		m, err := decodeClose(f.Payload)
		if err != nil {
			closeConn(conn, CodeInvalidMessage, "invalid CLOSE", s.opts.CloseTimeout)
			r.fail(CloseError{Code: CodeInvalidMessage, Reason: "invalid CLOSE in answer to RESUME"}, false)
			return true
		}
		_ = conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: m.Code}))
		_ = conn.CloseWrite()
		conn.Close()
		if resumeRetryable(m.Code) {
			return false
		}
		r.fail(CloseError{Code: m.Code, Reason: m.Reason, Remote: true}, resumeFallback(m.Code))
		return true
	}
	if !f.IsControl() || f.Type != TypeResumed {
		closeConn(conn, CodeProtocolError, "expected RESUMED", s.opts.CloseTimeout)
		r.fail(CloseError{Code: CodeProtocolError, Reason: "the server answered RESUME with something other than RESUMED or CLOSE"}, false)
		return true
	}
	var rm resumedMsg
	var timeout time.Duration
	var rec map[uint16]uint32
	m, err := obj(f.Payload)
	if err == nil {
		rm.Token, timeout, err = decodeTokenFields(m)
	}
	if err == nil {
		rec, err = decodeReceived(m)
	}
	if err != nil {
		var pe *ProtocolError
		errors.As(err, &pe)
		closeConn(conn, pe.Code, pe.Message, s.opts.CloseTimeout)
		r.fail(CloseError{Code: pe.Code, Reason: pe.Message}, false)
		return true
	}
	if !stop() {
		return false // the answer came too late; the transport is closed
	}
	s.mu.Lock()
	if s.state != stateOpen || s.path != nil {
		s.mu.Unlock()
		conn.Close()
		return true
	}
	plan, perr := r.planLocked(rec)
	if perr != nil {
		s.mu.Unlock()
		closeConn(conn, perr.Code, perr.Message, s.opts.CloseTimeout)
		r.fail(CloseError{Code: perr.Code, Reason: perr.Message}, false)
		return true
	}
	r.token, r.timeout = rm.Token, timeout
	replay := r.applyLocked(plan)
	r.reportAllLocked()
	// The confirming RECEIPT for channel 0 goes first (§3.3), then the
	// retransmissions, then anything new (§7).
	preamble := append([]Frame{*r.receiptLocked(true)}, replay...)
	_ = s.attachLocked(conn, preamble)
	r.resumedLocked()
	cb := r.cl.OnResume
	s.mu.Unlock()
	if cb != nil {
		go cb()
	}
	return true
}

// fail ends the session after a resume failed and, if fallback and the
// application asked for it, connects anew with HELLO (§9).
func (r *resumeState) fail(info CloseError, fallback bool) {
	s := r.s
	s.mu.Lock()
	s.finishLocked(info)
	s.mu.Unlock()
	if !fallback || r.cl.OnFallback == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), r.opts.withDefaults().HandshakeTimeout)
		defer cancel()
		conn, err := r.cl.Redial(ctx)
		if err != nil {
			r.cl.OnFallback(nil, nil, err)
			return
		}
		ns, res, err := Connect(ctx, conn, r.clientCfg, r.opts)
		r.cl.OnFallback(ns, res, err)
	}()
}

// closeConn refuses on a transport that no session runs on: CLOSE, then a
// wait for the peer's CLOSE or EOF (core §9.1). The timeout bounds it all,
// including a write to a peer that does not read.
func closeConn(conn FrameConn, code int, reason string, timeout time.Duration) {
	t := time.AfterFunc(timeout, func() { conn.Close() })
	defer t.Stop()
	_ = conn.WriteFrame(controlFrame(TypeClose, closeMsg{Code: code, Reason: reason}))
	_ = conn.CloseWrite()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			f, err := conn.ReadFrame(DefaultMaxFramePayload)
			if err != nil || (f.IsControl() && f.Type == TypeClose) {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	conn.Close()
}

// ─── Server ─────────────────────────────────────────────────────────────────

func newIssuedToken() *issuedToken {
	var b [16]byte // 128 bits from the CSPRNG (§8)
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	tok := base64.RawURLEncoding.EncodeToString(b[:])
	return &issuedToken{tok: tok, hash: sha256.Sum256([]byte(tok))}
}

func (r *resumeState) timeoutSeconds() int { return int(r.timeout / time.Second) }

// sendTokenLocked issues a new token and queues RESUME_TOKEN with it.
func (r *resumeState) sendTokenLocked() {
	t := newIssuedToken()
	it, err := r.s.enqueueLocked(controlFrame(TypeResumeToken, resumeTokenMsg{Token: t.tok, ResumeTimeout: r.timeoutSeconds()}), nil, false)
	if err != nil {
		return
	}
	it.tokenOf = t
	r.tokens = append(r.tokens, t)
	r.srv.register(t.hash, r)
}

// rotate sends a new RESUME_TOKEN (§3.1), then schedules the next one.
func (r *resumeState) rotate() {
	s := r.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != stateOpen {
		return
	}
	if s.path != nil {
		r.sendTokenLocked()
	}
	r.rotateTimer.Reset(r.srv.opts.RotateEvery)
}

// tokensAckedLocked applies a channel-0 acknowledgement to the tokens: once
// a replacement token's frame is covered, earlier tokens stop working.
func (r *resumeState) tokensAckedLocked(c0 uint32) {
	last := -1
	for i, t := range r.tokens {
		if t.sent && serialGE(c0, t.need) {
			last = i
		}
	}
	if last > 0 {
		r.dropTokensLocked(func(i int, _ *issuedToken) bool { return i < last })
	}
}

func (r *resumeState) dropTokensLocked(drop func(int, *issuedToken) bool) {
	var keep []*issuedToken
	for i, t := range r.tokens {
		if drop(i, t) {
			r.srv.unregister(t.hash)
		} else {
			keep = append(keep, t)
		}
	}
	r.tokens = keep
}

// confirmLocked: the client has used the transport that carried RESUMED,
// so it has the new token, and the one it resumed with stops working.
func (r *resumeState) confirmLocked() {
	r.confirmPending = false
	r.dropTokensLocked(func(_ int, t *issuedToken) bool { return t.dropOnFirst })
}

// matchLocked finds tok among the valid tokens, in constant time (§8).
func (r *resumeState) matchLocked(tok string) int {
	found := -1
	for i, t := range r.tokens {
		if subtle.ConstantTimeCompare([]byte(t.tok), []byte(tok)) == 1 {
			found = i
		}
	}
	return found
}

// reissueLocked replaces the tokens after a resume with tokens[idx]: that
// one stays valid until the client's first frame, the new one from RESUMED
// is valid, and so are tokens in RESUME_TOKENs that will be retransmitted
// (the client will receive them after RESUMED, so the latest of them is the
// one it will hold). Everything else stops working.
func (r *resumeState) reissueLocked(idx int, peer0 uint32) *issuedToken {
	presented := r.tokens[idx]
	nt := newIssuedToken()
	nt.sent, nt.need = true, peer0
	keep := []*issuedToken{presented, nt}
	for i, t := range r.tokens {
		if i == idx {
			continue
		}
		if i > idx && !t.dropOnFirst && (!t.sent || !serialGE(peer0, t.need)) {
			keep = append(keep, t)
			continue
		}
		r.srv.unregister(t.hash)
	}
	presented.dropOnFirst = true
	r.tokens = keep
	r.srv.register(nt.hash, r)
	return nt
}

// discard ends a detached session early (MaxDetached).
func (r *resumeState) discard() {
	s := r.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateOpen && s.path == nil {
		s.finishLocked(CloseError{Code: CodeGoingAway, Reason: "discarded: too many detached sessions", Lost: true})
	}
}

func (rs *ResumeServer) register(h [32]byte, r *resumeState) {
	rs.mu.Lock()
	rs.byToken[h] = r
	rs.mu.Unlock()
}

func (rs *ResumeServer) unregister(h [32]byte) {
	rs.mu.Lock()
	delete(rs.byToken, h)
	rs.mu.Unlock()
}

// addDetached records a detached session and returns one to discard if
// there are now too many.
func (rs *ResumeServer) addDetached(r *resumeState) (victim *resumeState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.detached[r] = time.Now()
	if len(rs.detached) <= rs.opts.MaxDetached {
		return nil
	}
	var oldest time.Time
	for d, at := range rs.detached {
		if d != r && (victim == nil || at.Before(oldest)) {
			victim, oldest = d, at
		}
	}
	delete(rs.detached, victim)
	return victim
}

func (rs *ResumeServer) attached(r *resumeState) {
	rs.mu.Lock()
	delete(rs.detached, r)
	rs.mu.Unlock()
}

// forget removes an ended session's tokens. Those of a session that timed
// out are remembered for another resumeTimeout, so a late RESUME gets 4101
// rather than 4100.
func (rs *ResumeServer) forget(r *resumeState, tokens []*issuedToken, expired bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.detached, r)
	now := time.Now()
	for _, t := range tokens {
		delete(rs.byToken, t.hash)
		if expired {
			rs.expired[t.hash] = now
			rs.expOrder = append(rs.expOrder, t.hash)
		}
	}
	for len(rs.expOrder) > 0 && (len(rs.expOrder) > 4*rs.opts.MaxDetached || now.Sub(rs.expired[rs.expOrder[0]]) > rs.opts.Timeout) {
		delete(rs.expired, rs.expOrder[0])
		rs.expOrder = rs.expOrder[1:]
	}
}

func (rs *ResumeServer) lookup(tok string) (r *resumeState, expired bool) {
	h := sha256.Sum256([]byte(tok))
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r = rs.byToken[h]; r != nil {
		return r, false
	}
	_, expired = rs.expired[h]
	return nil, expired
}

func refusal(err error) error {
	var pe *ProtocolError
	if errors.As(err, &pe) {
		return &Refusal{Code: pe.Code, Reason: pe.Message}
	}
	return &Refusal{Code: CodeInvalidMessage, Reason: err.Error()}
}

// handler answers RESUME, the extension's first frame (core §3.1), for
// Accept with cfg.
func (rs *ResumeServer) handler(cfg ServerConfig) FirstFrameHandler {
	return func(ctx context.Context, conn FrameConn, f Frame) error {
		// The checks go in the order of §7: version, token, authentication,
		// counts.
		msg, rec, err := decodeResume(f.Payload)
		if err != nil {
			return refusal(err)
		}
		if !VersionsCompatible(msg.Version, Version) {
			return &Refusal{Code: CodeVersionMismatch, Reason: fmt.Sprintf("unsupported version %v", msg.Version)}
		}
		r, expired := rs.lookup(msg.Token)
		if r == nil {
			if expired {
				return &Refusal{Code: CodeResumeExpired, Reason: "the session expired"}
			}
			return &Refusal{Code: CodeResumeNotFound, Reason: "unknown session"}
		}
		s := r.s
		switch {
		case cfg.AuthorizeResume != nil:
			if err := cfg.AuthorizeResume(ctx, &ResumeRequest{Version: msg.Version, Auth: msg.Auth}, s); err != nil {
				var ref *Refusal
				if errors.As(err, &ref) {
					return ref
				}
				return &Refusal{Code: CodeAuthFailed, Reason: err.Error()}
			}
		case cfg.Authorize != nil:
			return &Refusal{Code: CodeAuthFailed, Reason: "resume is not authorized"}
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		idx := r.matchLocked(msg.Token)
		if s.state != stateOpen || idx < 0 {
			return &Refusal{Code: CodeResumeNotFound, Reason: "unknown session"}
		}
		plan, perr := r.planLocked(rec)
		if perr != nil {
			// 4102 and 4103 end the session (§7).
			s.finishLocked(CloseError{Code: perr.Code, Reason: perr.Message})
			return &Refusal{Code: perr.Code, Reason: perr.Message}
		}
		nt := r.reissueLocked(idx, plan.c0)
		resumed, err := encodeControl(TypeResumed, resumedMsg{Token: nt.tok, ResumeTimeout: r.timeoutSeconds(), Received: r.receivedListLocked()})
		if err != nil {
			s.finishLocked(CloseError{Code: CodeResumeDataLost, Reason: "RESUMED does not fit in a control frame"})
			return &Refusal{Code: CodeResumeDataLost, Reason: "RESUMED does not fit in a control frame"}
		}
		// Take over: a session that still appears attached stops reading
		// its old transport, which is closed without CLOSE (§6).
		replay := r.applyLocked(plan)
		r.reportAllLocked()
		r.detachGen++
		if r.detachTimer != nil {
			r.detachTimer.Stop()
		}
		rs.attached(r)
		r.confirmPending = true
		_ = s.attachLocked(conn, append([]Frame{resumed}, replay...))
		r.resumedLocked()
		return nil
	}
}
