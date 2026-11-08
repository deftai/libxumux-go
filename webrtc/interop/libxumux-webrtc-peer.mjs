// Interop peer: libxumux as a WebRTC client, using the standard RTCPeerConnection
// API from node-datachannel/polyfill (as a browser would). Signalling is JSON
// lines on stdio: this side writes {"sdp"} then {"candidate"} lines, and reads
// the answer as one {"sdp"} line (candidates bundled).
// Usage: node libxumux-webrtc-peer.mjs   (LIBXUMUX_DIR = built libxumux checkout)
// Progress goes to stderr; exit code 0 means every check passed.
import path from "node:path";
import readline from "node:readline";
import { createRequire } from "node:module";

const dir = process.env.LIBXUMUX_DIR;
const require = createRequire(path.join(dir, "package.json"));
const { RTCPeerConnection } = await import(
  require.resolve("node-datachannel/polyfill")
);
const { XumuxClient } = await import(path.join(dir, "dist/index.js"));
const { WebRTCAdapter } = await import(
  path.join(dir, "dist/transports/webrtc-adapter.js")
);
const log = (...a) => console.error("[libxumux]", ...a);
const send = (o) => process.stdout.write(JSON.stringify(o) + "\n");
const check = (ok, what) => {
  if (!ok) {
    log("FAIL", what);
    process.exit(1);
  }
  log("ok", what);
};
const same = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);
setTimeout(() => (log("FAIL timeout"), process.exit(1)), 30_000).unref();

const pc = new RTCPeerConnection();
const dc = pc.createDataChannel("xumux", {
  negotiated: true,
  id: 0,
  ordered: true,
  protocol: "xumux",
});
pc.onicecandidate = (e) =>
  e.candidate && send({ candidate: e.candidate.toJSON() });
const offer = await pc.createOffer();
await pc.setLocalDescription(offer);
send({ sdp: pc.localDescription.toJSON() });
const lines = readline.createInterface({ input: process.stdin });
for await (const line of lines) {
  const m = JSON.parse(line);
  if (m.sdp) {
    await pc.setRemoteDescription(m.sdp);
    break;
  }
}
// Older adapters don't wait for the DataChannel to open themselves.
if (dc.readyState !== "open")
  await new Promise((r) => dc.addEventListener("open", r, { once: true }));
check(dc.readyState === "open", "DataChannel open");

const client = new XumuxClient(new WebRTCAdapter(dc), { pingInterval: 0 });
const { channels } = await client.connect({
  application: "webrtc-interop/1",
  channels: [{ name: "echo", reliable: true, ordered: true }],
});
const [ch] = channels;
check(ch?.id === 1, "HELLO channel accepted with ID 1");
for (const size of [0, 5, 16384, 16385, 200_000]) {
  const p = new Uint8Array(size).map((_, i) => (i * 13) & 0xff);
  await ch.send(9, p);
  const m = await ch.receive();
  check(m.type === 9 && same(m.payload, p), `echo ${size} bytes`);
}
check((await ch.ping()) >= 0, "channel ping");
check((await client.ping()) >= 0, "connection ping");
const bulk = await client.openChannel({
  name: "bulk",
  reliable: true,
  ordered: true,
});
const chunk = new Uint8Array(64 << 10).map((_, i) => i & 0xff);
const n = 64; // 4 MiB through the DataChannel's send buffer
const sending = (async () => {
  for (let i = 0; i < n; i++) await bulk.send(1, chunk);
})();
let got = 0;
while (got < n * chunk.length) {
  const m = await bulk.receive();
  if (!m || !same(m.payload, chunk)) check(false, "bulk chunk echoed intact");
  got += m.payload.length;
}
await sending;
check(true, "4 MiB echoed over the DataChannel");
const info = await client.close(1000, "done");
check(info.code === 1000, "normal close");
pc.close();
process.exit(0);
