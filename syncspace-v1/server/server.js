// SyncSpace backend: HTTP (static demo + /health) and WebSocket rooms.
// Protocol is documented in PROTOCOL.md. Run: npm start
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// Comma-separated list of allowed browser origins. Empty = allow all (fine for a demo).
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const LIMITS = { name: 24, key: 64, valueChars: 10000, keysPerRoom: 100, peersPerRoom: 25, cursorPerSec: 60, setPerSec: 30 };
const EMPTY_ROOM_TTL_MS = 10 * 60 * 1000;
const WEB_DIR = path.join(__dirname, '..', 'web');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };

const rooms = new Map(); // roomId -> { state: Map, version, peers: Map(id -> peer), emptySince }

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  }
  const file = url.pathname === '/' ? '/index.html' : url.pathname;
  const full = path.normalize(path.join(WEB_DIR, file));
  if (!full.startsWith(WEB_DIR)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(full)] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024,
  verifyClient: ({ origin }) => ALLOWED_ORIGINS.length === 0 || !origin || ALLOWED_ORIGINS.includes(origin),
});

const send = (ws, msg) => { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); };
const broadcast = (room, msg, exceptId) => {
  for (const p of room.peers.values()) if (p.id !== exceptId) send(p.ws, msg);
};
const publicPeer = p => ({ id: p.id, name: p.name, color: p.color });
const COLORS = ['#ef4444', '#f59e0b', '#10b981', '#06b6d4', '#6366f1', '#d946ef', '#ec4899', '#84cc16'];
const validId = s => typeof s === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(s);

function allow(peer, kind, max) {
  const now = Date.now();
  const b = peer.buckets[kind] || (peer.buckets[kind] = { t: now, n: 0 });
  if (now - b.t >= 1000) { b.t = now; b.n = 0; }
  return ++b.n <= max;
}

wss.on('connection', ws => {
  let peer = null, room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { t: 'error', code: 'bad_json', message: 'Message must be valid JSON' }); }
    if (!msg || typeof msg.t !== 'string') return send(ws, { t: 'error', code: 'bad_message', message: 'Missing message type "t"' });

    if (msg.t === 'join') {
      if (peer) return send(ws, { t: 'error', code: 'already_joined', message: 'Already joined a room' });
      if (!validId(msg.room)) return send(ws, { t: 'error', code: 'bad_room', message: 'Room must be 1-32 chars: letters, numbers, - or _' });
      const id = validId(msg.clientId) ? msg.clientId : Math.random().toString(36).slice(2, 10);
      room = rooms.get(msg.room);
      if (!room) { room = { state: new Map(), version: 0, peers: new Map(), emptySince: null }; rooms.set(msg.room, room); }
      const old = room.peers.get(id); // same client reconnecting before its old socket timed out
      if (!old && room.peers.size >= LIMITS.peersPerRoom) { room = null; return send(ws, { t: 'error', code: 'room_full', message: 'Room is full' }); }
      if (old) { old.ws.removeAllListeners('close'); old.ws.terminate(); room.peers.delete(id); }
      const name = String(msg.name || 'Guest').slice(0, LIMITS.name);
      const color = COLORS[[...id].reduce((a, c) => a + c.charCodeAt(0), 0) % COLORS.length];
      peer = { id, name, color, ws, buckets: {} };
      room.peers.set(id, peer); room.emptySince = null;
      send(ws, { t: 'welcome', id, room: msg.room, version: room.version, state: Object.fromEntries(room.state), peers: [...room.peers.values()].map(publicPeer) });
      broadcast(room, { t: 'peer-join', peer: publicPeer(peer) }, id);
      return;
    }
    if (!peer) return send(ws, { t: 'error', code: 'not_joined', message: 'Send "join" first' });

    if (msg.t === 'set') {
      if (typeof msg.key !== 'string' || !msg.key || msg.key.length > LIMITS.key) return send(ws, { t: 'error', code: 'bad_key', message: `Key must be 1-${LIMITS.key} chars` });
      if (!allow(peer, 'set', LIMITS.setPerSec)) return send(ws, { t: 'error', code: 'rate_limited', message: 'Too many updates' });
      if (msg.value === undefined) {
        room.state.delete(msg.key);
      } else {
        if (JSON.stringify(msg.value).length > LIMITS.valueChars) return send(ws, { t: 'error', code: 'too_big', message: 'Value too large' });
        if (!room.state.has(msg.key) && room.state.size >= LIMITS.keysPerRoom) return send(ws, { t: 'error', code: 'too_many_keys', message: 'Room key limit reached' });
        room.state.set(msg.key, msg.value);
      }
      room.version++;
      broadcast(room, { t: 'state', key: msg.key, value: msg.value, by: peer.id, v: room.version }, peer.id);
    } else if (msg.t === 'cursor') {
      if (!allow(peer, 'cursor', LIMITS.cursorPerSec)) return;
      if (typeof msg.x !== 'number' || typeof msg.y !== 'number') return;
      broadcast(room, { t: 'cursor', id: peer.id, x: msg.x, y: msg.y }, peer.id);
    } else if (msg.t === 'ping') {
      send(ws, { t: 'pong' });
    } else {
      send(ws, { t: 'error', code: 'unknown_type', message: `Unknown message type "${msg.t}"` });
    }
  });

  ws.on('close', () => {
    if (!peer || !room) return;
    if (room.peers.get(peer.id) === peer) {
      room.peers.delete(peer.id);
      broadcast(room, { t: 'peer-leave', id: peer.id });
      if (room.peers.size === 0) room.emptySince = Date.now();
    }
  });
  ws.on('error', () => {});
});

// Heartbeat drops dead connections; empty rooms are cleaned up after a TTL.
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
  for (const [id, r] of rooms) if (r.emptySince && Date.now() - r.emptySince > EMPTY_ROOM_TTL_MS) rooms.delete(id);
}, 30000).unref();

if (require.main === module) server.listen(PORT, () => console.log(`SyncSpace listening on :${PORT}`));
module.exports = { server, wss, rooms };
