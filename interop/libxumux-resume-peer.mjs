// Interop peer for session-resume/1: runs libxumux over TCP as a server or a
// client, driven by the Go test over this process's stdio.
// Usage: node libxumux-resume-peer.mjs   (LIBXUMUX_DIR = built libxumux checkout)
//
// stdin: one JSON command per line, {id, op, ...}. stdout: one JSON line per
// answer ({id, ok, ...} or {id, error, code, sessionLost}) and per event
// ({event, ...}). Progress goes to stderr.
//
// Both roles run the same "bulk" exchange on any channel named bulk:<bytes>
// (opened by either side): each side sends <bytes> random bytes in messages of
// random sizes (type 7, some empty, many fragmented), then an empty type 8.
// On the peer's type 8 it answers with the hex SHA-256 of what it received
// (type 9). On the peer's type 9 it compares that with the hash of what it
// sent and answers "ok" or "mismatch" (type 10). Done when it has both the
// peer's verdict and its own.
import path from 'node:path';
import net from 'node:net';
import readline from 'node:readline';
import { createHash, randomBytes, randomInt } from 'node:crypto';

const dir = process.env.LIBXUMUX_DIR;
const { XumuxClient, XumuxServer, SessionResumeStore, XumuxResumeError } = await import(
  path.join(dir, 'dist/index.js')
);
const { TcpAdapter } = await import(path.join(dir, 'dist/transports/tcp-adapter.js'));
const RESUME = 'session-resume/1';
const log = (...a) => console.error('[libxumux]', ...a);
const emit = (o) => process.stdout.write(JSON.stringify(o) + '\n');
const opts = { pingInterval: 0, onChannelRequest: () => true };
const enc = new TextEncoder();
const dec = new TextDecoder();

/** The bulk exchange on ch; resolves with byte counts, rejects on a mismatch. */
async function bulk(ch) {
  const total = Number(ch.name.split(':')[1]);
  const sentHash = createHash('sha256');
  const recvHash = createHash('sha256');
  let received = 0;
  let mine;
  let theirs;
  const sending = (async () => {
    for (let sent = 0; sent < total; ) {
      let n = Math.min(1 + randomInt(70_000), total - sent);
      if (randomInt(10) === 0) n = Math.min(randomInt(64), total - sent);
      const b = randomBytes(n);
      sentHash.update(b);
      await ch.send(7, new Uint8Array(b.buffer, b.byteOffset, n));
      sent += n;
    }
    await ch.send(8, new Uint8Array(0));
    return sentHash.digest('hex');
  })();
  while (mine === undefined || theirs === undefined) {
    const m = await ch.receive();
    if (!m) throw new Error(`${ch.name}: channel ended`);
    switch (m.type) {
      case 7:
        recvHash.update(m.payload);
        received += m.payload.length;
        break;
      case 8:
        await ch.send(9, enc.encode(recvHash.digest('hex')));
        break;
      case 9:
        mine = dec.decode(m.payload) === (await sending) ? 'ok' : 'mismatch';
        await ch.send(10, enc.encode(mine));
        break;
      case 10:
        theirs = dec.decode(m.payload);
        break;
      default:
        throw new Error(`${ch.name}: unexpected type ${m.type}`);
    }
  }
  if (mine !== 'ok' || theirs !== 'ok' || received !== total)
    throw new Error(`${ch.name}: mine=${mine} theirs=${theirs} received=${received}/${total}`);
  return { sent: total, received };
}

function onChannel(ch) {
  if (!ch.name.startsWith('bulk:')) return;
  bulk(ch).then(
    (r) => emit({ event: 'bulk', name: ch.name, ...r }),
    (e) => emit({ event: 'bulk', name: ch.name, error: e.message }),
  );
}

function dial(port) {
  return new Promise((resolve, reject) => {
    const s = net.createConnection({ port, host: '127.0.0.1' }, () => resolve(s));
    s.once('error', reject);
  });
}

// Channels kept open after their bulk exchange, by handle, for closech.
const kept = new Map();
let nextHandle = 0;

/** Run the bulk exchange on ch and, with keep, return a handle to close it later. */
async function bulkKept(ch, keep) {
  const r = await bulk(ch);
  if (!keep) return r;
  kept.set(++nextHandle, ch);
  return { ...r, handle: nextHandle };
}

const errorReply = (e) => ({
  error: e.message,
  code: e.code,
  sessionLost: e instanceof XumuxResumeError ? e.sessionLost : undefined,
});

// ─── Server ────────────────────────────────────────────────────────────────

let latest = null; // the newest session (from HELLO)

async function startServer(resumeTimeout) {
  const store = new SessionResumeStore({ resumeTimeout });
  const tcp = net.createServer((socket) => {
    socket.on('error', () => undefined);
    log('tcp connection');
    const server = new XumuxServer(new TcpAdapter(socket), opts);
    server.accept({ sessionResume: store }).then(
      (r) => {
        if (r.resumed) {
          emit({ event: 'resumed', timeout: resumeTimeout });
          return;
        }
        latest = server;
        server.addEventListener('channel', (e) => onChannel(e.detail));
        for (const ch of r.channels) onChannel(ch);
        server.addEventListener('detach', () => emit({ event: 'detached', timeout: resumeTimeout }));
        server.addEventListener('close', (e) =>
          emit({ event: 'closed', code: e.detail.code, timeout: resumeTimeout }),
        );
        emit({ event: 'session', extensions: r.extensions, timeout: resumeTimeout });
      },
      (e) => emit({ event: 'refused', code: e.code, timeout: resumeTimeout }),
    );
  });
  await new Promise((r) => tcp.listen(0, '127.0.0.1', r));
  return tcp.address().port;
}

// ─── Client ────────────────────────────────────────────────────────────────

let client = null;
let port = 0;
let socket = null;
let autoResume = true;

async function connect(channels = []) {
  socket = await dial(port);
  const c = new XumuxClient(new TcpAdapter(socket), opts);
  const r = await c.connect({ extensions: [RESUME], channels });
  c.addEventListener('channel', (e) => onChannel(e.detail));
  for (const ch of r.channels) onChannel(ch);
  c.addEventListener('detach', () => {
    emit({ event: 'detached' });
    if (autoResume) void resumeLoop(c);
  });
  c.addEventListener('close', (e) => emit({ event: 'closed', code: e.detail.code }));
  client = c;
  return r.extensions;
}

/** Resume until it works or the session is lost; then fall back to HELLO (§9). */
async function resumeLoop(c) {
  for (;;) {
    if (client !== c || c.state === 'closed') return;
    try {
      socket = await dial(port);
      await c.resume(new TcpAdapter(socket));
      emit({ event: 'resumed' });
      return;
    } catch (e) {
      if (e instanceof XumuxResumeError && e.sessionLost) {
        emit({ event: 'lost', code: e.code });
        try {
          emit({ event: 'fallback', extensions: await connect() });
        } catch (e2) {
          emit({ event: 'fallback', error: e2.message });
        }
        return;
      }
      if (!c.resumable) {
        emit({ event: 'lost', code: e.code ?? 0, error: e.message });
        return;
      }
      await new Promise((r) => setTimeout(r, 20));
    }
  }
}

async function waitResumable() {
  for (let i = 0; !client.resumable; i++) {
    if (i > 500) throw new Error('no RESUME_TOKEN');
    await new Promise((r) => setTimeout(r, 5));
  }
}

const ops = {
  // Server
  async listen({ timeouts }) {
    const ports = [];
    for (const t of timeouts) ports.push(await startServer(t));
    return { ports };
  },
  rotate() {
    latest.rotateResumeToken();
    return {};
  },
  async open({ bytes, keep }) {
    const ch = await latest.openChannel({ name: `bulk:${bytes}`, reliable: true, ordered: true });
    return bulkKept(ch, keep);
  },
  async closech({ handle }) {
    const ch = kept.get(handle);
    kept.delete(handle);
    const info = await ch.close(1000, 'bye');
    return { code: info.code, state: ch.state };
  },
  // Client
  async connect(cmd) {
    port = cmd.port;
    const extensions = await connect(cmd.channels);
    await waitResumable();
    return { extensions };
  },
  async bulk({ bytes, reliable = true, ordered = true, keep }) {
    const ch = await client.openChannel({ name: `bulk:${bytes}`, reliable, ordered });
    return bulkKept(ch, keep);
  },
  /** The transport to the proxy at local `port` was lost; make sure this side knows. */
  drop({ port }) {
    if (socket?.localPort === port) socket.destroy();
    return {};
  },
  autoresume({ on }) {
    autoResume = on;
    return {};
  },
  async resume() {
    socket = await dial(port);
    await client.resume(new TcpAdapter(socket));
    return {};
  },
  // The client's token is private state; read and set it only to test refusals.
  token() {
    return { token: client.rs.token };
  },
  settoken({ token }) {
    client.rs.token = token;
    return {};
  },
  state() {
    return { state: client.state, detached: client.detached, resumable: client.resumable };
  },
  async close() {
    const info = await client.close(1000, 'done');
    return { code: info.code };
  },
};

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const cmd = JSON.parse(line);
  Promise.resolve()
    .then(() => ops[cmd.op](cmd))
    .then(
      (r) => emit({ id: cmd.id, ok: true, ...r }),
      (e) => {
        log(cmd.op, 'failed:', e.message);
        emit({ id: cmd.id, ok: false, ...errorReply(e) });
      },
    );
});
rl.on('close', () => process.exit(0));
log('ready');
