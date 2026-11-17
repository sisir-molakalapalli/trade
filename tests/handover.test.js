'use strict';
// v6.15: a new build takes over the open trades of the build it replaces. The handover itself runs between two live
// servers (checked in a trial with a stand-in for Render's public URL); these checks keep its safety rules in place.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const fnHead = (name) => {
  const i = src.search(new RegExp(`(async )?function ${name}\\(`));
  assert.ok(i >= 0, `${name} not found`);
  return src.slice(i, src.indexOf('\n', src.indexOf('{', i) + 1) + 200);
};

test('nothing opens, closes or changes a trade while paused (starting, or handed over)', () => {
  for (const f of ['openLong', 'openShort', 'realizeExit', 'runPredictiveSentry', 'evaluateAutopilotEntries', 'settleFunding', 'closeManual']) {
    assert.ok(/tradingPaused\(\)/.test(fnHead(f)), `${f} must check tradingPaused()`);
  }
  assert.ok(/const tradingPaused = \(\) => !Handover\.ready \|\| Handover\.frozen;/.test(src));
});

test('a paused build refuses every change over HTTP and WebSocket, but still answers reads', () => {
  assert.ok(/if \(req\.method === 'GET' \|\| req\.path === '\/api\/handoff' \|\| !tradingPaused\(\)\) return next\(\);\s*res\.status\(503\)/.test(src));
  assert.ok(/if \(tradingPaused\(\) && msg\.type !== 'SYNC_STATE'\) return;/.test(src));
  // the 503 guard comes after the CORS headers, so the app can read its message
  assert.ok(src.indexOf("res.header('Access-Control-Allow-Origin', '*');") < src.indexOf('res.status(503)'));
});

test('a new build takes over before it opens its port and starts its timers', () => {
  const boot = src.slice(src.indexOf('async function boot()'), src.indexOf('function startServerAndTimers()'));
  assert.ok(boot.indexOf('loadDiskState()') < boot.indexOf('await takeOverFromPreviousBuild()'));
  assert.ok(boot.indexOf('await takeOverFromPreviousBuild()') < boot.indexOf('Handover.ready = true'));
  assert.ok(boot.indexOf('Handover.ready = true') < boot.indexOf('startServerAndTimers()'));
  const timers = src.slice(src.indexOf('function startServerAndTimers()'));
  assert.ok(timers.indexOf('server.listen(') >= 0 && timers.indexOf('setInterval(') > timers.indexOf('server.listen('));
  assert.strictEqual((src.match(/server\.listen\(/g) || []).length, 1);
});

test('the handover only gives an account out; nothing takes an account from outside', () => {
  const ep = src.slice(src.indexOf("app.post('/api/handoff'"), src.indexOf('function watchHandover()'));
  assert.ok(!/applyPersistedState/.test(ep));
  assert.ok(/persistableState\(Infinity, true\)/.test(ep));
  // the account is applied only from what this build fetched from the public URL at boot
  const users = src.split('applyPersistedState(').length - 1;
  assert.strictEqual(users, 4);   // the definition, the disk file, the v6.15 handover, an older build's /api/state
});

test('an older build is used only when it has no handover (its state carries no handover field)', () => {
  assert.ok(/st && !st\.handover && st\.portfolio && st\.activePositions && st\.recoveryBags/.test(src));
  assert.ok(/capitalResetId: CAPITAL_RESET_ID/.test(src.slice(src.indexOf('function legacyHandover('))));
});
