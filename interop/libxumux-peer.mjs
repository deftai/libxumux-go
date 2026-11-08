// Interop peer: runs libxumux on this process's stdio, as server or client.
// Usage: node libxumux-peer.mjs server|client   (LIBXUMUX_DIR = built libxumux checkout)
// Progress goes to stderr; exit code 0 means every check passed.
import path from 'node:path';

const dir = process.env.LIBXUMUX_DIR;
const { XumuxClient, XumuxServer } = await import(path.join(dir, 'dist/index.js'));
const { StdioAdapter } = await import(path.join(dir, 'dist/transports/stdio-adapter.js'));
const log = (...a) => console.error('[libxumux]', ...a);
const transport = new StdioAdapter(process.stdin, process.stdout);
const opts = { pingInterval: 0, onChannelRequest: () => true };

const echo = async (ch) => {
  try {
    for await (const m of ch) await ch.send(m.type, m.payload);
  } catch (e) {
    log('echo ended', ch.name, e.message);
  }
};
const same = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);
const check = (ok, what) => {
  if (!ok) {
    log('FAIL', what);
    process.exit(1);
  }
  log('ok', what);
};

if (process.argv[2] === 'server') {
  const server = new XumuxServer(transport, opts);
  server.addEventListener('channel', (e) => void echo(e.detail));
  server.addEventListener('close', (e) => {
    log('closed', e.detail.code);
    process.exit(e.detail.code === 1000 ? 0 : 1);
  });
  const { channels } = await server.accept();
  for (const ch of channels) void echo(ch);
  // Open a channel to the Go client too: it must echo.
  const srv = await server.openChannel({ name: 'from-ts', reliable: true, ordered: true });
  check(srv.id % 2 === 0, 'server channel ID is even');
  const payload = new Uint8Array(100_000).map((_, i) => i * 7);
  await srv.send(5, payload);
  const back = await srv.receive();
  check(back.type === 5 && same(back.payload, payload), 'Go echoed 100 KB on a server-opened channel');
  await srv.close();
} else {
  const client = new XumuxClient(transport, opts);
  const { channels } = await client.connect({ channels: [{ name: 'echo', reliable: true, ordered: true }] });
  const [ch] = channels;
  check(ch?.id === 1, 'HELLO channel accepted with ID 1');
  for (const size of [0, 5, 16384, 16385, 200_000]) {
    const p = new Uint8Array(size).map((_, i) => (i * 13) & 0xff);
    await ch.send(9, p);
    const m = await ch.receive();
    check(m.type === 9 && same(m.payload, p), `echo ${size} bytes`);
  }
  check((await ch.ping()) >= 0, 'channel ping');
  check((await client.ping()) >= 0, 'connection ping');
  const dyn = await client.openChannel({ name: 'dyn', reliable: true, ordered: true });
  check(dyn.id === 3, 'dynamic channel ID 3');
  await dyn.send(1, new TextEncoder().encode('dyn'));
  check(new TextDecoder().decode((await dyn.receive()).payload) === 'dyn', 'dynamic channel echo');
  await dyn.close();
  check(dyn.state === 'closed', 'two-step channel close');
  const info = await client.close(1000, 'done');
  check(info.code === 1000, 'graceful close');
  process.exit(0);
}
