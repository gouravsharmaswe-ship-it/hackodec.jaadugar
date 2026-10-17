/*!
 * SyncSpace client library (single file, no dependencies).
 *
 *   const room = SyncSpace.connect({ url: 'wss://your-server', room: 'demo', name: 'Asha' });
 *   room.on('state', (key, value) => console.log(key, value));
 *   room.set('score', 10);
 *
 * Public API: SyncSpace.connect(opts), SyncSpace.randomRoom()
 * Room object: set(key, value), get(key), getAll(), peers(), me(), cursor(x, y),
 *              on(event, fn), off(event, fn), close(), status
 * Events: open, close, reconnecting, state(key, value, byId), presence(peers),
 *         peer-join(peer), peer-leave(id), cursor(id, x, y, peer), error(err)
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SyncSpace = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  function randomRoom() {
    var c = 'abcdefghjkmnpqrstuvwxyz23456789', s = '';
    for (var i = 0; i < 6; i++) s += c[Math.floor(Math.random() * c.length)];
    return s;
  }

  function connect(opts) {
    opts = opts || {};
    if (!opts.url) throw new Error('SyncSpace.connect: "url" is required');
    if (!/^[A-Za-z0-9_-]{1,32}$/.test(opts.room || '')) throw new Error('SyncSpace.connect: "room" must be 1-32 chars (letters, numbers, - or _)');

    var WS = opts.WebSocket || (typeof WebSocket !== 'undefined' ? WebSocket : null);
    if (!WS) throw new Error('No WebSocket implementation available');

    var listeners = {};
    var state = {};           // latest known shared state
    var peers = {};           // id -> {id, name, color}
    var myId = opts.clientId || randomRoom() + randomRoom();
    var ws = null, closedByUser = false, attempts = 0, timer = null, hb = null;
    var queue = [];           // writes made while offline, flushed after reconnect
    var lastCursor = 0;
    var api = { status: 'connecting' };

    function emit(ev) {
      var args = Array.prototype.slice.call(arguments, 1);
      (listeners[ev] || []).slice().forEach(function (fn) { try { fn.apply(null, args); } catch (e) { console.error('[SyncSpace] listener error', e); } });
    }
    function rawSend(obj) { if (ws && ws.readyState === 1) { ws.send(JSON.stringify(obj)); return true; } return false; }
    function setStatus(s) { api.status = s; }
    function peerList() { return Object.keys(peers).map(function (k) { return peers[k]; }); }

    function open() {
      setStatus(attempts ? 'reconnecting' : 'connecting');
      var sock = ws = new WS(opts.url);
      var dropped = false;
      // If the handshake hangs (flaky network, sleeping server), give up on it and retry.
      var connectTimer = setTimeout(function () { if (sock.readyState !== 1) { try { sock.close(); } catch (e) {} drop(); } }, opts.connectTimeout || 8000);
      function drop() {
        if (dropped) return; dropped = true; clearTimeout(connectTimer); clearInterval(hb);
        sock.onmessage = sock.onopen = null;
        if (closedByUser) { setStatus('closed'); emit('close'); return; }
        setStatus('reconnecting');
        var delay = Math.min(10000, 500 * Math.pow(2, attempts)) + Math.random() * 300;
        attempts++;
        emit('reconnecting', attempts, delay);
        timer = setTimeout(open, delay);
      }
      sock.onopen = function () {
        clearTimeout(connectTimer);
        rawSend({ t: 'join', room: opts.room, name: opts.name || 'Guest', clientId: myId });
      };
      sock.onmessage = function (e) {
        var m; try { m = JSON.parse(e.data); } catch (err) { return; }
        switch (m.t) {
          case 'welcome': {
            var wasReconnect = attempts > 0;
            attempts = 0; myId = m.id; setStatus('open');
            // Server state is the source of truth: restore it, then replay offline writes.
            var old = state; state = m.state || {};
            Object.keys(state).forEach(function (k) { if (JSON.stringify(old[k]) !== JSON.stringify(state[k])) emit('state', k, state[k], null); });
            Object.keys(old).forEach(function (k) { if (!(k in state)) emit('state', k, undefined, null); });
            peers = {}; (m.peers || []).forEach(function (p) { peers[p.id] = p; });
            emit('open', { reconnected: wasReconnect });
            emit('presence', peerList());
            var q = queue; queue = [];
            q.forEach(function (w) { api.set(w.key, w.value); });
            clearInterval(hb);
            hb = setInterval(function () { rawSend({ t: 'ping' }); }, 20000);
            break;
          }
          case 'state':
            if (m.value === undefined) delete state[m.key]; else state[m.key] = m.value;
            emit('state', m.key, m.value, m.by); break;
          case 'peer-join': peers[m.peer.id] = m.peer; emit('peer-join', m.peer); emit('presence', peerList()); break;
          case 'peer-leave': var gone = peers[m.id]; delete peers[m.id]; emit('peer-leave', m.id, gone); emit('presence', peerList()); break;
          case 'cursor': emit('cursor', m.id, m.x, m.y, peers[m.id]); break;
          case 'error': emit('error', { code: m.code, message: m.message }); break;
        }
      };
      sock.onclose = drop;
      sock.onerror = function () { /* onclose follows and handles retry */ };
    }

    api.on = function (ev, fn) { (listeners[ev] = listeners[ev] || []).push(fn); return api; };
    api.off = function (ev, fn) { listeners[ev] = (listeners[ev] || []).filter(function (f) { return f !== fn; }); return api; };
    api.get = function (key) { return state[key]; };
    api.getAll = function () { return JSON.parse(JSON.stringify(state)); };
    api.peers = peerList;
    api.me = function () { return peers[myId] || { id: myId, name: opts.name || 'Guest' }; };
    api.set = function (key, value) {
      if (value === undefined) delete state[key]; else state[key] = value;   // optimistic local update
      if (!rawSend({ t: 'set', key: key, value: value })) queue.push({ key: key, value: value });
    };
    api.cursor = function (x, y) {
      var now = Date.now();
      if (now - lastCursor < 33) return;   // ~30 updates/sec max
      lastCursor = now; rawSend({ t: 'cursor', x: x, y: y });
    };
    api.close = function () { closedByUser = true; clearTimeout(timer); clearInterval(hb); if (ws) ws.close(); };

    open();
    return api;
  }

  return { connect: connect, randomRoom: randomRoom };
});
