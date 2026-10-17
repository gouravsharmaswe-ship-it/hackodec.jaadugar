# SyncSpace - multiplayer in a few lines

SyncSpace gives any web page shared state, live cursors and an online list using one small JavaScript file and a tiny Node.js WebSocket server.

## Quick start (local)
```bash
npm install
npm start            # http://localhost:3000
```
Open `http://localhost:3000` in three tabs, then open the same room link (`?room=abc123`) in each. Type in the notepad, click the counter, move the mouse.

## Use it in your own page (integration example)
```html
<script src="syncspace.js"></script>
<script>
  const room = SyncSpace.connect({ url: 'wss://YOUR-SERVER', room: 'my-room', name: 'Asha' });
  room.on('state', (key, value) => console.log(key, '=', value));   // anything anyone sets
  room.set('score', 10);                                            // shared with everyone in the room
  room.on('presence', people => console.log(people.map(p => p.name)));
  document.onmousemove = e => room.cursor(e.clientX, e.clientY);
  room.on('cursor', (id, x, y, peer) => { /* draw peer.name at x, y */ });
</script>
```
API: `connect({url, room, name})`, `set(key, value)`, `get(key)`, `getAll()`, `peers()`, `me()`, `cursor(x, y)`, `on/off(event, fn)`, `close()`.
Events: `open`, `close`, `reconnecting`, `state`, `presence`, `peer-join`, `peer-leave`, `cursor`, `error`.
If the connection drops the library retries automatically (exponential backoff), then restores the latest room state and re-sends changes you made while offline.

## Architecture
```
Browser tab --(syncspace.js)--WebSocket--> server/server.js (rooms in memory)
```
- `web/syncspace.js` - the one-file client library.
- `web/index.html` - demo page, uses only the library's public API.
- `web/config.js` - backend URL for the frontend (empty = same host).
- `server/server.js` - HTTP (`/health`, static files) + `ws` rooms, validation, rate limits, heartbeat.
- `PROTOCOL.md` - the message format shared by client and server.
- `test/test.js` - `npm test` runs the integration tests.

State is held in server memory: restarting the server clears rooms (free hosts also sleep when idle).

## Deploy
The server hosts the demo page too, so **one** free web service is enough.
1. Push this repo to GitHub.
2. On Render.com: New > Web Service > connect the repo. Build `npm install`, start `npm start` (a `render.yaml` is included). Render gives you `https://NAME.onrender.com`; the page and the `wss://` backend share that address.
3. Optional: host `web/` separately (GitHub Pages/Netlify), set `window.SYNCSPACE_URL = 'wss://NAME.onrender.com'` in `web/config.js`, and set the env var `ALLOWED_ORIGINS` on the server to your frontend's origin.

Free-tier limits to check before relying on it: Render free web services sleep after idle time (first visit is slow) and rooms are lost on restart.

## Configuration
`PORT` (default 3000), `ALLOWED_ORIGINS` (comma-separated; empty = any).
