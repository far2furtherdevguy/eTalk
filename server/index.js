const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const srv = http.createServer(app);
// Fast ping so dead mobile sockets are dropped in ~15s (prevents ghost "Room is full")
const io = new Server(srv, { cors: { origin: '*' }, pingInterval: 8000, pingTimeout: 6000 });

app.use((_, res, next) => { res.set('Access-Control-Allow-Origin', '*'); next(); });
app.use(express.static(path.join(__dirname, '..', 'www')));
app.get('/health', (_, res) => res.send('ok'));

// ---- ICE (STUN/TURN) -------------------------------------------------------
// Works with zero config using public servers. For reliable TURN, optionally set
// METERED_APP + METERED_KEY (free key from metered.ca/openrelay, 20GB/month) in Render env.
const FALLBACK_ICE = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun.relay.metered.ca:80' },
  { urls: ['turn:openrelay.metered.ca:80', 'turn:openrelay.metered.ca:443',
           'turn:openrelay.metered.ca:443?transport=tcp', 'turns:openrelay.metered.ca:443?transport=tcp'],
    username: 'openrelayproject', credential: 'openrelayproject' },
];
let iceCache = { t: 0, v: null };
app.get('/ice', async (_, res) => {
  const { METERED_APP, METERED_KEY } = process.env;
  if (METERED_APP && METERED_KEY) {
    if (iceCache.v && Date.now() - iceCache.t < 3600e3) return res.json(iceCache.v);
    try {
      const r = await fetch(`https://${METERED_APP}.metered.live/api/v1/turn/credentials?apiKey=${METERED_KEY}`);
      if (r.ok) { iceCache = { t: Date.now(), v: await r.json() }; return res.json(iceCache.v); }
    } catch (e) { console.error('metered fetch failed', e.message); }
  }
  res.json(FALLBACK_ICE);
});

// ---- Rooms: pin -> Map(clientId -> socket), max 2 --------------------------
const rooms = new Map();

function drop(s) {
  const r = rooms.get(s.pin);
  if (r && r.get(s.cid) === s) {
    r.delete(s.cid);
    if (!r.size) rooms.delete(s.pin);
    else r.forEach((p) => p.emit('peer-left'));
  }
  s.pin = null;
}

io.on('connection', (s) => {
  s.on('join', (pin, cid) => {
    pin = String(pin);
    if (!/^\d{4}$/.test(pin) || !cid) return s.emit('err', 'PIN must be 4 digits');
    if (s.pin) drop(s);
    let r = rooms.get(pin);
    if (!r) rooms.set(pin, (r = new Map()));
    // Same client rejoining (reconnect / ghost socket): replace the old socket
    const old = r.get(cid);
    if (old && old !== s) { r.delete(cid); old.pin = null; old.emit('err', 'Joined from another tab/device'); old.disconnect(true); }
    if (r.size >= 2) { if (!r.size) rooms.delete(pin); return s.emit('err', 'Room is full. If you were just disconnected, retry in 15 seconds.'); }
    s.pin = pin; s.cid = cid; r.set(cid, s);
    if (r.size === 1) return s.emit('waiting');
    r.forEach((p) => p.emit('matched', { initiator: p !== s })); // the one already waiting makes the offer
  });
  s.on('signal', (d) => {
    const r = rooms.get(s.pin);
    if (r) r.forEach((p) => p !== s && p.emit('signal', d));
  });
  s.on('leave', () => drop(s));
  s.on('disconnect', () => drop(s));
});

const port = process.env.PORT || 3000;
srv.listen(port, () => console.log('eTalk on ' + port));
