'use strict';
/**
 * Chatly - real-time chat server (WhatsApp-like). Zero dependencies: only Node built-ins.
 * Implements: WebSocket (RFC 6455) server, auth, 1:1 + group chat, connection registry,
 * heartbeat, sent/delivered/read receipts, offline storage + sync, per-conversation ordering,
 * idempotent sends (dedupe), rate limiting, presence, typing.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const PORT = Number(process.env.PORT) || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS) || 25000;
const MAX_FRAME = 64 * 1024;
const GROUP_LIMIT = 256;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ------------------------------ persistence ------------------------------ */
// Production design: Cassandra (messages), PostgreSQL (users/groups). Here: one JSON file, atomic writes.
fs.mkdirSync(DATA_DIR, { recursive: true });
let db = { secret: crypto.randomBytes(32).toString('hex'), users: {}, convs: {}, msgs: {}, cursors: {} };
try { Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) { /* first run */ }
let saveTimer = null;
function save() { if (!saveTimer) saveTimer = setTimeout(flush, 250); }
function flush() {
  clearTimeout(saveTimer); saveTimer = null;
  const tmp = DB_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db));
  fs.renameSync(tmp, DB_FILE);
}
flush();
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { flush(); process.exit(0); });

// Idempotency index: conversation|sender|clientMsgId
const seen = new Set();
for (const [cid, list] of Object.entries(db.msgs)) for (const m of list) seen.add(cid + '|' + m.from + '|' + m.id);

/* --------------------------------- auth ---------------------------------- */
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
function sign(payload) {
  const b = Buffer.from(JSON.stringify(payload)).toString('base64url');
  return b + '.' + crypto.createHmac('sha256', db.secret).update(b).digest('base64url');
}
function verify(tok) {
  if (typeof tok !== 'string') return null;
  const [b, s] = tok.split('.');
  if (!b || !s) return null;
  const expect = crypto.createHmac('sha256', db.secret).update(b).digest('base64url');
  const A = Buffer.from(s), B = Buffer.from(expect);
  if (A.length !== B.length || !crypto.timingSafeEqual(A, B)) return null;
  try { const p = JSON.parse(Buffer.from(b, 'base64url').toString()); return p.exp > Date.now() ? p.u : null; }
  catch (e) { return null; }
}
const loginAttempts = new Map();
function tooManyLogins(ip) {
  const now = Date.now();
  const arr = (loginAttempts.get(ip) || []).filter(t => now - t < 60000);
  arr.push(now); loginAttempts.set(ip, arr);
  return arr.length > 30;
}

/* ------------------------- minimal WebSocket server ----------------------- */
class WSConn extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket; this.buf = Buffer.alloc(0); this.alive = true; this.open = true;
    this.frag = []; this.fragSize = 0; this.fragOp = 0;
    socket.setNoDelay(true);
    socket.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this._parse(); });
    socket.on('close', () => this._closed());
    socket.on('error', () => this._closed());
  }
  _closed() { if (!this.open) return; this.open = false; this.emit('close'); }
  _parse() {
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (!masked) return this.terminate();               // clients must mask (RFC 6455)
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (this.buf.length < 10) return;
        if (this.buf.readUInt32BE(2)) return this.terminate();
        len = this.buf.readUInt32BE(6); off = 10;
      }
      if (len > MAX_FRAME) return this.terminate();
      if (this.buf.length < off + 4 + len) return;        // wait for the rest of the frame
      const mask = this.buf.subarray(off, off + 4);
      const payload = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      this._frame(fin, op, payload);
      if (!this.open) return;
    }
  }
  _frame(fin, op, p) {
    this.alive = true;                                    // any traffic proves the peer is alive
    if (op === 0x8) { this._send(0x8, p.subarray(0, 2)); this.socket.end(); return this._closed(); }
    if (op === 0x9) return this._send(0xA, p);            // ping -> pong
    if (op === 0xA) return;                               // pong
    if (op === 0x1 || op === 0x2 || op === 0x0) {
      if (op !== 0x0) { this.fragOp = op; this.frag = []; this.fragSize = 0; }
      this.fragSize += p.length;
      if (this.fragSize > MAX_FRAME) return this.terminate();
      this.frag.push(p);
      if (fin) {
        const data = Buffer.concat(this.frag); this.frag = []; this.fragSize = 0;
        if (this.fragOp === 0x1) this.emit('message', data.toString('utf8'));
      }
    }
  }
  _send(op, payload) {
    if (!this.open) return;
    const len = payload.length; let h;
    if (len < 126) h = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(len, 6); }
    this.socket.write(Buffer.concat([h, payload]));
  }
  send(obj) { this._send(0x1, Buffer.from(JSON.stringify(obj))); }
  ping() { this._send(0x9, Buffer.alloc(0)); }
  terminate() { this.socket.destroy(); this._closed(); }
}

/* ----------------------- connection registry + presence ------------------- */
// Production design: Redis hash user_id -> {server_id, device}. Here: in-memory Map (single node).
const registry = new Map(); // user -> Set<WSConn>
const isOnline = u => !!(registry.get(u) && registry.get(u).size);
function sendTo(user, obj) { const s = registry.get(user); if (s) for (const c of s) c.send(obj); }
function peersOf(user) {
  const set = new Set();
  for (const c of Object.values(db.convs)) if (c.members.includes(user)) c.members.forEach(m => m !== user && set.add(m));
  return set;
}
function presenceOf(users) {
  const out = {};
  for (const u of users) if (db.users[u]) out[u] = { online: isOnline(u), lastSeen: db.users[u].lastSeen };
  return out;
}
function broadcastPresence(user, online) {
  const lastSeen = db.users[user].lastSeen;
  for (const p of peersOf(user)) sendTo(p, { t: 'presence', user, online, lastSeen });
}

/* ------------------------------ conversations ----------------------------- */
const dmId = (a, b) => 'd:' + [a, b].sort().join('|');
function convView(c, me) {
  const cur = db.cursors[c.id] || {};
  const list = db.msgs[c.id] || [];
  const myRead = (cur[me] && cur[me].read) || 0;
  let unread = 0;
  for (let i = list.length - 1; i >= 0 && list[i].seq > myRead; i--) if (list[i].from !== me) unread++;
  return { id: c.id, type: c.type, name: c.name, members: c.members, lastSeq: c.seq, unread, receipts: cur, created: c.created };
}
const visibleTo = (c, user) => c.members.includes(user) &&
  (c.type === 'group' || c.creator === user || (db.msgs[c.id] || []).length > 0);
function announceConv(c, openedBy) {
  const peers = presenceOf(c.members);
  for (const m of c.members) sendTo(m, { t: 'conv', conv: convView(c, m), peers, open: m === openedBy });
}
function pushNotify(user, c, msg) {
  // Production: FCM / APNs via Push Service (payload carries no message text, to preserve E2E privacy).
  console.log(`[push] notification queued for ${user}: new message in ${c.id} (#${msg.seq})`);
}

/* ------------------------------- ws handlers ------------------------------ */
function onConnect(ws, user) {
  ws.user = user; ws.bucket = { tokens: 20, ts: Date.now() };
  let set = registry.get(user);
  const wasOffline = !set || set.size === 0;
  if (!set) { set = new Set(); registry.set(user, set); }
  set.add(ws);
  const convs = Object.values(db.convs).filter(c => visibleTo(c, user));
  const peerNames = new Set(); convs.forEach(c => c.members.forEach(m => peerNames.add(m)));
  ws.send({ t: 'ready', me: user, convs: convs.map(c => convView(c, user)), peers: presenceOf(peerNames) });
  if (wasOffline) broadcastPresence(user, true);
  ws.on('message', raw => handle(ws, raw));
  ws.on('close', () => {
    set.delete(ws);
    if (set.size === 0) {
      registry.delete(user);
      db.users[user].lastSeen = Date.now(); save();
      broadcastPresence(user, false);
    }
  });
}

function sendHistory(ws, c, from) {
  const msgs = (db.msgs[c.id] || []).filter(m => m.seq > from).slice(0, 500);
  if (msgs.length) ws.send({ t: 'sync', cid: c.id, msgs });
}

function handle(ws, raw) {
  if (raw.length > 20000) return;
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  const user = ws.user;
  switch (m.t) {
    case 'sync': {                                   // client sends cursors; server returns what it missed
      const cursors = m.cursors && typeof m.cursors === 'object' ? m.cursors : {};
      for (const c of Object.values(db.convs)) {
        if (!visibleTo(c, user)) continue;
        const cur = cursors[c.id];
        sendHistory(ws, c, typeof cur === 'number' ? cur : Math.max(0, c.seq - 50));
      }
      return;
    }
    case 'fetch': {                                  // gap repair
      const c = db.convs[m.cid];
      if (c && c.members.includes(user)) sendHistory(ws, c, Math.max(0, Number(m.from) - 1 || 0));
      return;
    }
    case 'send': return onSend(ws, m);
    case 'ack': return onCursor(ws, m, 'delivered');
    case 'read': return onCursor(ws, m, 'read');
    case 'typing': {
      const c = db.convs[m.cid];
      if (!c || !c.members.includes(user)) return;
      for (const mem of c.members) if (mem !== user) sendTo(mem, { t: 'typing', cid: c.id, user });
      return;
    }
    case 'users':
      return ws.send({ t: 'users', users: Object.keys(db.users).filter(u => u !== user)
        .map(u => ({ u, online: isOnline(u), lastSeen: db.users[u].lastSeen })) });
    case 'open_dm': {
      const other = String(m.with || '').toLowerCase();
      if (other === user || !db.users[other]) return ws.send({ t: 'error', code: 'no_user' });
      const id = dmId(user, other);
      let c = db.convs[id];
      if (!c) {
        c = db.convs[id] = { id, type: 'direct', name: null, members: [user, other].sort(), seq: 0, created: Date.now(), creator: user };
        db.msgs[id] = []; save();
      }
      return sendTo(user, { t: 'conv', conv: convView(c, user), peers: presenceOf(c.members), open: true });
    }
    case 'create_group': {
      const name = String(m.name || '').trim().slice(0, 40);
      const others = [...new Set((Array.isArray(m.members) ? m.members : []).map(x => String(x).toLowerCase()))]
        .filter(u => u !== user && db.users[u]);
      if (!name) return ws.send({ t: 'error', code: 'group_name' });
      if (!others.length || others.length + 1 > GROUP_LIMIT) return ws.send({ t: 'error', code: 'group_size' });
      const id = 'g:' + crypto.randomUUID();
      const c = db.convs[id] = { id, type: 'group', name, members: [user, ...others], seq: 0, created: Date.now(), creator: user };
      db.msgs[id] = []; save();
      return announceConv(c, user);
    }
  }
}

function onSend(ws, m) {
  const user = ws.user, c = db.convs[m.cid];
  const id = typeof m.id === 'string' && m.id.length > 0 && m.id.length <= 64 ? m.id : null;
  if (!c || !c.members.includes(user)) return ws.send({ t: 'error', code: 'no_conversation', id: m.id });
  const text = typeof m.text === 'string' ? m.text.trim() : '';
  if (!id || !text || text.length > 4000) return ws.send({ t: 'error', code: 'bad_message', id: m.id });
  // rate limit: token bucket, 20 burst, +2 per second
  const now = Date.now(), b = ws.bucket;
  b.tokens = Math.min(20, b.tokens + (now - b.ts) / 500); b.ts = now;
  if (b.tokens < 1) return ws.send({ t: 'error', code: 'rate_limited', id });
  b.tokens -= 1;

  const key = c.id + '|' + user + '|' + id;
  const list = db.msgs[c.id] || (db.msgs[c.id] = []);
  if (seen.has(key)) {                               // retry of an already stored message: just re-ACK
    const ex = list.find(x => x.from === user && x.id === id);
    if (ex) ws.send({ t: 'sent', cid: c.id, id, seq: ex.seq, ts: ex.ts });
    return;
  }
  seen.add(key);
  const msg = { seq: ++c.seq, id, from: user, text, ts: now };   // per-conversation sequence number
  list.push(msg); save();
  ws.send({ t: 'sent', cid: c.id, id, seq: msg.seq, ts: msg.ts });   // ACK 1: stored => "sent"

  for (const member of c.members) {
    if (member === user) {                           // sender's other devices
      for (const s of registry.get(user) || []) if (s !== ws) s.send({ t: 'msg', cid: c.id, m: msg });
      continue;
    }
    if (isOnline(member)) {
      if (c.type === 'direct' && list.length === 1) sendTo(member, { t: 'conv', conv: convView(c, member), peers: presenceOf(c.members), open: false });
      sendTo(member, { t: 'msg', cid: c.id, m: msg });
    } else pushNotify(member, c, msg);               // offline: stays stored, push notification
  }
}

function onCursor(ws, m, kind) {
  const user = ws.user, c = db.convs[m.cid];
  if (!c || !c.members.includes(user)) return;
  const seq = Math.min(Number(m.seq) | 0, c.seq);
  if (seq <= 0) return;
  const cur = (db.cursors[c.id] = db.cursors[c.id] || {});
  const mine = (cur[user] = cur[user] || { delivered: 0, read: 0 });
  let changed = false;
  if (kind === 'read' && seq > mine.read) { mine.read = seq; if (mine.delivered < seq) mine.delivered = seq; changed = true; }
  if (kind === 'delivered' && seq > mine.delivered) { mine.delivered = seq; changed = true; }
  if (!changed) return;
  save();
  for (const mem of c.members) sendTo(mem, { t: 'receipt', cid: c.id, user, delivered: mine.delivered, read: mine.read });
}

// Heartbeat: ping every interval; a peer that did not answer since the last ping is dropped.
setInterval(() => {
  for (const set of registry.values()) for (const ws of set) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, HEARTBEAT_MS);

/* ---------------------------------- HTTP ---------------------------------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.ico': 'image/x-icon' };
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
function readBody(req, limit = 2048) {
  return new Promise((ok, no) => {
    let b = '';
    req.on('data', d => { b += d; if (b.length > limit) { no(new Error('too big')); req.destroy(); } });
    req.on('end', () => ok(b)); req.on('error', no);
  });
}
async function login(req, res) {
  try {
    if (tooManyLogins(req.socket.remoteAddress)) return json(res, 429, { error: 'Too many attempts, try again in a minute' });
    const { username, password } = JSON.parse(await readBody(req));
    const u = String(username || '').toLowerCase();
    if (!/^[a-z0-9_]{3,20}$/.test(u)) return json(res, 400, { error: 'Username: 3-20 letters, numbers or underscore' });
    if (typeof password !== 'string' || password.length < 4 || password.length > 100) return json(res, 400, { error: 'Password must be at least 4 characters' });
    let rec = db.users[u];
    if (!rec) {                                      // first login creates the account
      const salt = crypto.randomBytes(16).toString('hex');
      rec = db.users[u] = { salt, hash: hashPw(password, salt), created: Date.now(), lastSeen: null };
      save();
    } else if (!crypto.timingSafeEqual(Buffer.from(hashPw(password, rec.salt)), Buffer.from(rec.hash))) {
      return json(res, 401, { error: 'Wrong password for this username' });
    }
    json(res, 200, { token: sign({ u, exp: Date.now() + 7 * 864e5 }), username: u });
  } catch (e) { json(res, 400, { error: 'Bad request' }); }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'POST' && url.pathname === '/api/login') return login(req, res);
  if (req.method === 'GET' && url.pathname === '/api/stats') {
    return json(res, 200, {
      onlineUsers: registry.size, connections: [...registry.values()].reduce((n, s) => n + s.size, 0),
      users: Object.keys(db.users).length, conversations: Object.keys(db.convs).length,
      messages: Object.values(db.msgs).reduce((n, l) => n + l.length, 0)
    });
  }
  if (req.method !== 'GET') { res.writeHead(405); return res.end(); }
  let rel = url.pathname === '/' ? '/index.html' : decodeURIComponent(url.pathname);
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  const user = url.pathname === '/ws' ? verify(url.searchParams.get('token')) : null;
  const key = req.headers['sec-websocket-key'];
  if (!user || !key || !db.users[user]) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  onConnect(new WSConn(socket), user);
});

server.listen(PORT, () => console.log(`Chatly running at http://localhost:${PORT}  (data: ${DB_FILE})`));
