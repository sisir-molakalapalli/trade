'use strict';
// v6.14: matching a row read back from the Google Sheet to the engine's own trade record (ledgerMatch).
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const S = require('../strategy');

// The two PUMP shorts of 15 Nov 2026 (live): 20:00 at 2x (about $43.33) and 23:50 at 1x ($21.90). The list is newest first,
// as the engine keeps it.
const late = { id: 1794766809732, pair: 'PUMP/USDT', date: '15/11/2026', time: '23:50:09', entryPriceUSDT: 0.002828, notionalUSDT: 21.9, leverage: 1 };
const early = { id: 1794753005999, pair: 'PUMP/USDT', date: '15/11/2026', time: '20:00:05', entryPriceUSDT: 0.002903, notionalUSDT: 43.33, leverage: 2 };
const local = [late, early];
const row = (t, extra = {}) => ({ id: t.id, pair: t.pair, date: t.date, time: t.time, entryPriceUSDT: t.entryPriceUSDT, ...extra });

test('a row finds its own record by id, not another trade of the same coin that day', () => {
  assert.strictEqual(S.ledgerMatch(local, row(early)), early);
  assert.strictEqual(S.ledgerMatch(local, row(late)), late);
  // the old rule: first record of the coin that day with an entry within $0.0001 (0.002903 - 0.002828 = 0.000075)
  const old = local.find((x) => x.id === early.id || (x.pair === early.pair && x.date === early.date && Math.abs(x.entryPriceUSDT - early.entryPriceUSDT) < 0.0001));
  assert.strictEqual(old, late);   // the bug: the 20:00 trade took the 23:50 trade's $21.90
});

test('an id match needs the same coin', () => {
  assert.strictEqual(S.ledgerMatch(local, row(early, { pair: 'DOGE/USDT' })), null);
});

test('a row whose id the engine does not hold matches only the same coin, day, minute and entry price', () => {
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: 1 })), early);
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: undefined, time: '20:00:41' })), early);   // same minute
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: 1, time: '20:01:05' })), null);            // another minute
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: 1, date: '16/11/2026' })), null);          // another day
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: 1, entryPriceUSDT: 0.002828 })), null);    // another trade's entry
  assert.strictEqual(S.ledgerMatch(local, row(early, { id: 1, time: '' })), null);
});

test('the entry price window is 0.001% of the price, at any price', () => {
  const btc = { id: 5, pair: 'BTC/USDT', date: '16/11/2026', time: '10:00:00', entryPriceUSDT: 65000 };
  assert.strictEqual(S.ledgerMatch([btc], { ...btc, id: 0, entryPriceUSDT: 65000.6 }), btc);   // 0.6 <= 0.65
  assert.strictEqual(S.ledgerMatch([btc], { ...btc, id: 0, entryPriceUSDT: 65001 }), null);
  const cheap = { ...early, id: 6 };
  assert.strictEqual(S.ledgerMatch([cheap], { ...cheap, id: 0, entryPriceUSDT: 0.002903 * (1 + 0.5e-5) }), cheap);
  assert.strictEqual(S.ledgerMatch([cheap], { ...cheap, id: 0, entryPriceUSDT: 0.002904 }), null);
});

test('no list or no row gives null', () => {
  assert.strictEqual(S.ledgerMatch(undefined, row(early)), null);
  assert.strictEqual(S.ledgerMatch([], row(early)), null);
  assert.strictEqual(S.ledgerMatch(local, null), null);
});

test('server.js uses ledgerMatch when it reads the sheet back', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.ok(src.includes('Strategy.ledgerMatch(State.tradeLogs'));
  assert.ok(!/entryPriceUSDT \|\| 0\) - entry\) < 0\.0001/.test(src));
});
