// Integration tests: real server + real client library (Node 22 global WebSocket).
// A TCP proxy sits between clients and server so we can cut connections for real.
const net = require('net');
const assert = require('assert');
const { server } = require('../server/server');
const SyncSpace = require('../web/syncspace.js');

const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms = 5000, label = 'condition') => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (fn()) return; await sleep(25); }
  throw new Error('Timed out waiting for ' + label);
};

let passed = 0, failed = 0;
async function test(name, fn) {
  try { await fn(); passed++; console.log('  PASS', name); }
  catch (e) { failed++; console.log('  FAIL', name, '-', e.message); }
}

(async () => {
  await new Promise(r => server.listen(0, r));
  const serverPort = server.address().port;

  // proxy
  const conns = new Set(); let blocked = false;
  const proxy = net.createServer(c => {
    if (blocked) return c.destroy();
    const s = net.connect(serverPort, '127.0.0.1');
    conns.add(c); conns.add(s);
    c.pipe(s); s.pipe(c);
    const kill = () => { c.destroy(); s.destroy(); conns.delete(c); conns.delete(s); };
    c.on('error', kill); s.on('error', kill); c.on('close', kill); s.on('close', kill);
  });
  await new Promise(r => proxy.listen(0, r));
  const url = 'ws://127.0.0.1:' + proxy.address().port;
  const open = [];
  const join = (room, name) => { const c = SyncSpace.connect({ url, room, name, connectTimeout: 1500 }); open.push(c); return new Promise(r => c.on('open', () => r(c))); };

  console.log('SyncSpace tests');
  const [a, b, c] = await Promise.all([join('r1', 'A'), join('r1', 'B'), join('r1', 'C')]);
  const other = await join('r2', 'Other');

  await test('three clients in one room see each other (presence)', async () => {
    await until(() => a.peers().length === 3 && b.peers().length === 3 && c.peers().length === 3, 3000, 'presence');
    assert.deepStrictEqual(a.peers().map(p => p.name).sort(), ['A', 'B', 'C']);
  });

  await test('shared state syncs to all three tabs', async () => {
    a.set('x', { hello: 'world' });
    await until(() => b.get('x') && c.get('x'), 3000, 'state');
    assert.deepStrictEqual(b.get('x'), { hello: 'world' });
    assert.deepStrictEqual(c.get('x'), { hello: 'world' });
    c.set('n', 5); await until(() => a.get('n') === 5 && b.get('n') === 5, 3000, 'state from C');
  });

  await test('rooms are isolated', async () => {
    assert.strictEqual(other.get('x'), undefined);
    assert.strictEqual(other.peers().length, 1);
    other.set('secret', 1); await sleep(200);
    assert.strictEqual(a.get('secret'), undefined);
  });

  await test('live cursors reach other peers only in same room', async () => {
    let got = null, leaked = false;
    b.on('cursor', (id, x, y) => { got = { id, x, y }; });
    other.on('cursor', () => { leaked = true; });
    a.cursor(0.25, 0.75);
    await until(() => got, 3000, 'cursor');
    assert.strictEqual(got.x, 0.25); assert.strictEqual(got.y, 0.75); assert.strictEqual(leaked, false);
  });

  await test('late joiner receives latest state', async () => {
    const d = await join('r1', 'D');
    assert.deepStrictEqual(d.get('x'), { hello: 'world' }); assert.strictEqual(d.get('n'), 5);
    d.close();
    await until(() => a.peers().length === 3, 3000, 'peer-leave');
  });

  await test('invalid room name is rejected', () => {
    assert.throws(() => SyncSpace.connect({ url, room: 'bad room!' }), /room/);
  });

  await test('server rejects bad messages without crashing', async () => {
    const ws = new WebSocket(url);
    const msgs = [];
    ws.onmessage = e => msgs.push(JSON.parse(e.data));
    await new Promise(r => (ws.onopen = r));
    ws.send('not json'); ws.send(JSON.stringify({ t: 'set', key: 'k', value: 1 })); ws.send(JSON.stringify({ t: 'join', room: '../etc' }));
    await until(() => msgs.length >= 3, 3000, 'errors');
    assert.deepStrictEqual(msgs.map(m => m.code), ['bad_json', 'not_joined', 'bad_room']);
    ws.close();
  });

  await test('disconnect -> auto-reconnect -> latest state restored', async () => {
    let reconnecting = 0, reopened = null;
    c.on('reconnecting', () => reconnecting++);
    c.on('open', info => { reopened = info; });
    blocked = true; conns.forEach(s => s.destroy());        // real network cut, new connections refused
    await until(() => reconnecting > 0, 5000, 'reconnecting event');
    b.set('while-away', 'fresh');                              // state changes while C is offline (B also reconnects)
    c.set('offline-write', 'from-c');                          // queued by the library
    await sleep(800);
    blocked = false;                                           // network back
    await until(() => reopened && reopened.reconnected, 15000, 'reopen');
    await until(() => c.get('while-away') === 'fresh' || a.get('offline-write') === 'from-c', 15000, 'state restore');
    await until(() => a.get('offline-write') === 'from-c' && b.get('offline-write') === 'from-c', 15000, 'queued write delivered');
    assert.strictEqual(c.get('x').hello, 'world');
    assert.strictEqual(c.peers().length, 3);
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  open.forEach(o => o.close()); proxy.close(); server.close();
  setTimeout(() => process.exit(failed ? 1 : 0), 100);
})();
