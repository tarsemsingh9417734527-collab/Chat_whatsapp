'use strict';
// End-to-end tests: starts the real server and drives it with real WebSocket clients.
// Run: node test.js   (needs Node 22+ for the built-in WebSocket client)
const { spawn } = require('child_process');
const net = require('net');
const fs = require('fs');
const os = require('os');
const path = require('path');

const PORT = 3999, BASE = `http://localhost:${PORT}`;
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chatly-test-'));
let server, passed = 0, failed = 0;
const sleep = ms => new Promise(r => setTimeout(r, ms));
function ok(cond, name) { if (cond) { passed++; console.log('  PASS', name); } else { failed++; console.log('  FAIL', name); } }

function start() {
  server = spawn(process.execPath, ['server.js'], { cwd: __dirname, env: { ...process.env, PORT, DATA_DIR: dataDir, HEARTBEAT_MS: 300 }, stdio: ['ignore', 'pipe', 'pipe'] });
  server.stdout.on('data', d => { if (process.env.VERBOSE) process.stdout.write(d); });
  return sleep(600);
}
const stop = async () => { server.kill('SIGTERM'); await sleep(300); };
async function login(u, p = 'secret1') {
  const r = await fetch(BASE + '/api/login', { method: 'POST', body: JSON.stringify({ username: u, password: p }) });
  return { status: r.status, ...(await r.json()) };
}
function client(token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${PORT}/ws?token=${encodeURIComponent(token)}`);
    const c = { ws, events: [] };
    ws.onmessage = e => c.events.push(JSON.parse(e.data));
    c.send = o => ws.send(JSON.stringify(o));
    c.wait = async (pred, ms = 2000) => {
      const t0 = Date.now();
      while (Date.now() - t0 < ms) { const f = c.events.find(pred); if (f) return f; await sleep(20); }
      return null;
    };
    c.count = pred => c.events.filter(pred).length;
    ws.onopen = () => resolve(c);
    ws.onerror = () => reject(new Error('ws error'));
  });
}

(async () => {
  await start();
  console.log('Auth');
  const a = await login('alice'), b = await login('bob'), cc = await login('carol');
  ok(a.status === 200 && a.token, 'new account created on first login');
  ok((await login('alice', 'wrongpw')).status === 401, 'wrong password rejected');
  ok((await login('x')).status === 400, 'invalid username rejected');
  let rejected = false;
  try { await client('bad.token'); } catch (e) { rejected = true; }
  ok(rejected, 'WebSocket with invalid token is refused');

  console.log('1:1 messaging, receipts, ordering');
  const A = await client(a.token), B = await client(b.token);
  ok(!!(await A.wait(e => e.t === 'ready')) && !!(await B.wait(e => e.t === 'ready')), 'both clients connect and get ready');
  A.send({ t: 'open_dm', with: 'bob' });
  const conv = (await A.wait(e => e.t === 'conv')).conv;
  A.send({ t: 'send', cid: conv.id, id: 'm1', text: 'hello bob' });
  const sent = await A.wait(e => e.t === 'sent' && e.id === 'm1');
  ok(sent && sent.seq === 1, 'sender gets ACK "sent" with seq 1');
  ok(!!(await B.wait(e => e.t === 'conv')), 'recipient is told about the new conversation');
  const got = await B.wait(e => e.t === 'msg');
  ok(got && got.m.text === 'hello bob' && got.m.seq === 1, 'online recipient receives the message live');
  B.send({ t: 'ack', cid: conv.id, seq: 1 });
  const r1 = await A.wait(e => e.t === 'receipt' && e.user === 'bob' && e.delivered === 1);
  ok(!!r1 && r1.read === 0, 'delivered receipt reaches sender');
  B.send({ t: 'read', cid: conv.id, seq: 1 });
  ok(!!(await A.wait(e => e.t === 'receipt' && e.user === 'bob' && e.read === 1)), 'read receipt reaches sender');

  A.send({ t: 'send', cid: conv.id, id: 'm1', text: 'hello bob' });          // retry with same client id
  const again = await A.wait(e => e.t === 'sent' && e.id === 'm1' && e !== sent);
  await sleep(200);
  ok(again && again.seq === 1 && B.count(e => e.t === 'msg') === 1, 'duplicate send is de-duplicated (same seq, not delivered twice)');

  for (let i = 2; i <= 6; i++) A.send({ t: 'send', cid: conv.id, id: 'o' + i, text: 'msg ' + i });
  await sleep(400);
  const seqs = B.events.filter(e => e.t === 'msg').map(e => e.m.seq);
  ok(JSON.stringify(seqs) === '[1,2,3,4,5,6]', 'burst of 5 messages arrives in strict sequence order (' + seqs.join(',') + ')');

  console.log('Offline storage and sync');
  B.ws.close(); await sleep(300);
  const pres = await A.wait(e => e.t === 'presence' && e.user === 'bob' && e.online === false);
  ok(!!pres && !!pres.lastSeen, 'presence: sender sees bob go offline with last seen');
  for (let i = 7; i <= 9; i++) A.send({ t: 'send', cid: conv.id, id: 'q' + i, text: 'while offline ' + i });
  await sleep(300);
  const B2 = await client(b.token);
  const ready = await B2.wait(e => e.t === 'ready');
  ok(ready.convs[0].unread === 8, 'unread counter after reconnect is 8 (seq 2-9 not yet read)');
  B2.send({ t: 'sync', cursors: { [conv.id]: 6 } });                           // client says it has up to seq 6
  const sync = await B2.wait(e => e.t === 'sync');
  ok(sync && JSON.stringify(sync.msgs.map(m => m.seq)) === '[7,8,9]', 'sync returns exactly the missed messages [7,8,9]');
  B2.send({ t: 'fetch', cid: conv.id, from: 8 });
  await sleep(200);
  ok(B2.events.filter(e => e.t === 'sync').length === 2, 'gap repair (fetch) works');

  console.log('Group chat');
  const C = await client(cc.token); await C.wait(e => e.t === 'ready'); C.ws.close(); await sleep(200);
  A.send({ t: 'create_group', name: 'Team', members: ['bob', 'carol', 'nobody'] });
  const gconv = (await A.wait(e => e.t === 'conv' && e.conv.type === 'group')).conv;
  ok(gconv.members.length === 3, 'group created with 3 valid members (unknown user ignored)');
  A.send({ t: 'send', cid: gconv.id, id: 'g1', text: 'hi team' });
  ok(!!(await B2.wait(e => e.t === 'msg' && e.cid === gconv.id)), 'online member receives group message');
  const C2 = await client(cc.token);
  const cready = await C2.wait(e => e.t === 'ready');
  const gc = cready.convs.find(x => x.id === gconv.id);
  ok(gc && gc.unread === 1, 'offline member sees the group with 1 unread after connecting');
  C2.send({ t: 'sync', cursors: {} });
  ok(!!(await C2.wait(e => e.t === 'sync' && e.cid === gconv.id)), 'offline member syncs the group message');
  C2.send({ t: 'ack', cid: gconv.id, seq: 1 });
  ok(!!(await A.wait(e => e.t === 'receipt' && e.cid === gconv.id && e.user === 'carol')), 'group receipts tracked per member');
  A.send({ t: 'send', cid: gconv.id, id: 'g2', text: '' });
  ok(!!(await A.wait(e => e.t === 'error' && e.code === 'bad_message')), 'empty message rejected');
  const stranger = await login('eve');
  const E = await client(stranger.token); await E.wait(e => e.t === 'ready');
  E.send({ t: 'send', cid: gconv.id, id: 'x1', text: 'let me in' });
  ok(!!(await E.wait(e => e.t === 'error' && e.code === 'no_conversation')), 'non-member cannot post to a group');

  console.log('Rate limiting');
  for (let i = 0; i < 40; i++) A.send({ t: 'send', cid: conv.id, id: 'r' + i, text: 'spam' });
  ok(!!(await A.wait(e => e.t === 'error' && e.code === 'rate_limited')), 'flooding is rate limited');

  console.log('Heartbeat');
  await sleep(1200);
  ok(A.ws.readyState === 1, 'responsive client survives several heartbeat cycles (pong works)');
  const dead = await new Promise(res => {
    const s = net.connect(PORT, 'localhost', () => {
      s.write(`GET /ws?token=${encodeURIComponent(cc.token)} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n`);
    });
    s.on('data', () => {}); s.on('close', () => res(true)); s.on('error', () => res(true));
    setTimeout(() => res(false), 2500);
  });
  ok(dead, 'silent (dead) connection is detected and closed by the server');

  console.log('Persistence');
  [A, B2, C2, E].forEach(x => x.ws.close());
  await stop(); await start();
  const A3 = await client((await login('alice')).token);
  const r = await A3.wait(e => e.t === 'ready');
  ok(r.convs.length === 2 && r.convs.find(x => x.id === conv.id).lastSeq >= 9, 'conversations and sequence counters survive a server restart');
  A3.send({ t: 'sync', cursors: {} });
  const hist = await A3.wait(e => e.t === 'sync' && e.cid === conv.id);
  ok(hist && hist.msgs.some(m => m.text === 'hello bob'), 'message history survives a server restart');
  A3.ws.close();
  await stop();
  fs.rmSync(dataDir, { recursive: true, force: true });
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); try { server.kill(); } catch (x) {} process.exit(1); });
