// Last Fire multiplayer relay + lobby server.
// Zero game logic: it hands out room codes, lists public lobbies, and passes
// messages between a room's host and its players. The host runs the game.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const MAX_ROOMS = 500, MAX_CLIENTS = 7;                 // host + 7 = 8 players
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';        // same alphabet as the game
// Optional: ALLOWED_ORIGINS="itch.zone,itch.io,localhost" (suffix match). Empty = allow all.
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

const rooms = new Map();                                 // code -> room

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('last-fire relay ok, rooms: ' + rooms.size);
});
const wss = new WebSocketServer({ server, maxPayload: 32 * 1024, perMessageDeflate: false });

const send = (ws, o) => { try { if (ws.readyState === 1 && ws.bufferedAmount < 262144) ws.send(JSON.stringify(o)); } catch (e) {} };
const str = (v, n) => String(v == null ? '' : v).replace(/[^\p{L}\p{N}_ .'\-]/gu, '').slice(0, n);
const cleanInfo = i => ({ m: str(i && i.m, 8) || 'normal', n: (i && i.n | 0) || 0, h: str(i && i.h, 12) || 'Host', g: i && i.g ? 1 : 0 });
function newCode() {
  for (let k = 0; k < 50; k++) {
    let c = ''; for (let i = 0; i < 5; i++) c += ALPHA[Math.random() * ALPHA.length | 0];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function closeRoom(room, why) {
  rooms.delete(room.code);
  for (const [, c] of room.clients) { send(c, { t: 'hostgone', why }); c.room = null; try { c.close(1000, 'host left'); } catch (e) {} }
  room.clients.clear();
}
function leave(ws) {
  const room = ws.room; if (!room) return; ws.room = null;
  if (ws.role === 'host') closeRoom(room, 'left');
  else if (ws.role === 'client') { room.clients.delete(ws.id); send(room.host, { t: 'gone', id: ws.id }); }
}

wss.on('connection', (ws, req) => {
  if (ALLOWED.length) {
    let host = ''; try { host = new URL(req.headers.origin || '').hostname; } catch (e) {}
    if (!ALLOWED.some(a => host === a || host.endsWith('.' + a))) { ws.close(1008, 'origin'); return; }
  }
  ws.alive = true; ws.role = null; ws.room = null; ws.n = 0;
  const rl = setInterval(() => { ws.n = 0; }, 1000);
  ws.on('pong', () => { ws.alive = true; });
  ws.on('close', () => { clearInterval(rl); leave(ws); });
  ws.on('error', () => {});

  ws.on('message', raw => {
    ws.alive = true;
    if (++ws.n > (ws.role === 'host' ? 400 : 90)) { ws.close(1008, 'rate'); return; }
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    switch (m.t) {
      case 'create': {
        if (ws.room || rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', e: 'busy' });
        const code = newCode(); if (!code) return send(ws, { t: 'err', e: 'busy' });
        const room = { code, host: ws, clients: new Map(), seq: 0, pub: m.pub ? 1 : 0, info: cleanInfo(m.info) };
        rooms.set(code, room); ws.room = room; ws.role = 'host';
        return send(ws, { t: 'created', code });
      }
      case 'info':
        if (ws.role === 'host') ws.room.info = cleanInfo(m.info);
        return;
      case 'list': {
        const l = [];
        for (const r of rooms.values()) {
          if (!r.pub || r.info.g) continue;
          l.push({ c: r.code, m: r.info.m, n: r.info.n, h: r.info.h, g: 0, p: 1 + r.clients.size });
          if (l.length >= 30) break;
        }
        return send(ws, { t: 'lobbies', l });
      }
      case 'join': {
        if (ws.room) return;
        const room = rooms.get(str(m.code, 5).toUpperCase());
        if (!room) return send(ws, { t: 'err', e: 'nolobby' });
        if (room.info.g) return send(ws, { t: 'err', e: 'started' });
        if (room.clients.size >= MAX_CLIENTS) return send(ws, { t: 'err', e: 'full' });
        ws.id = 'p' + (++room.seq); ws.room = room; ws.role = 'client';
        room.clients.set(ws.id, ws);
        send(ws, { t: 'joined', id: ws.id });
        return send(room.host, { t: 'peer', id: ws.id });
      }
      case 'up':      // player -> host
        if (ws.role === 'client' && m.d && typeof m.d === 'object') send(ws.room.host, { t: 'from', id: ws.id, d: m.d });
        return;
      case 'to': {    // host -> one player
        if (ws.role !== 'host' || !m.d || typeof m.d !== 'object') return;
        const c = ws.room.clients.get(m.id); if (c) send(c, { t: 'from', d: m.d });
        return;
      }
      case 'kick': {
        if (ws.role !== 'host') return;
        const c = ws.room.clients.get(m.id); if (c) { ws.room.clients.delete(m.id); c.room = null; try { c.close(1000, 'kicked'); } catch (e) {} }
        return;
      }
      case 'ping': return send(ws, { t: 'pong' });
    }
  });
});

// Drop dead sockets (phones that vanished) so lobbies don't linger.
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} }
}, 25000);

server.listen(PORT, () => console.log('Last Fire relay listening on ' + PORT));
