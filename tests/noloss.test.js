'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const S = require('../strategy');
const T0 = 1e12, min = (m) => T0 + m * 60000;
const pos = () => ({ pair: 'A/USDT', entryPriceUSDT: 100, openedAt: T0, qty: 0.05, notionalUSDT: 5, ratchetStage: 'INITIAL', sl: 97.2, peakHighPrice: 100, peakGainPct: 0, tp1Secured: false });
const run = (p, seq) => seq.map(([px, m = 1, sig = {}]) => S.manageNoLoss(p, px, sig, min(m)));

test('a losing position is never EXITed, at any depth or age', () => {
  const p = pos();
  for (const [px, m] of [[99.9, 1], [99, 10], [98.6, 500], [97.5, 5000]]) assert.equal(S.manageNoLoss(p, px, { bearishPattern: 'X', peakExhausted: true }, min(m)).action, 'HOLD');
});
test('moves to a bag at -3.50% and not before, whatever the order flow', () => {
  assert.equal(S.manageNoLoss(pos(), 96.5, {}, min(1)).action, 'DEMOTE');
  assert.equal(S.manageNoLoss(pos(), 96.6, { upsideDead: true }, min(120)).action, 'HOLD');
});
test('below the floor a trailing stop does not sell', () => {
  const p = pos(); run(p, [[100.8]]); assert.ok(p.sl >= 100.35);
  assert.equal(S.manageNoLoss(p, 100.2, {}, min(2)).action, 'HOLD');
});
test('ratchets, trail exit and candle harvest stay at or above the +0.30% minimum', () => {
  const p = pos(); run(p, [[100.4], [100.75], [101.2]]);
  // v6.13: 0.20% behind the +1.20% peak (101.0), tighter than the old 0.35% trail (100.85)
  assert.equal(p.ratchetStage, 'DYNAMIC_TRAIL'); assert.ok(Math.abs(p.sl - 101.0) < 1e-9);
  assert.equal(S.manageNoLoss(p, 100.8, {}, min(5)).action, 'EXIT');
  const q = pos(); assert.equal(S.manageNoLoss(q, 100.36, { bearishPattern: 'BEARISH ENGULFING' }, min(1)).action, 'EXIT');
  const r = pos(); assert.equal(S.manageNoLoss(r, 100.29, { bearishPattern: 'BEARISH ENGULFING' }, min(1)).action, 'HOLD');
});
test('45 minute scratch only in profit; +1.8% takes the partial', () => {
  assert.equal(S.manageNoLoss(pos(), 100.4, {}, min(46)).action, 'EXIT');
  assert.equal(S.manageNoLoss(pos(), 100.1, {}, min(46)).action, 'HOLD');
  assert.equal(S.manageNoLoss(pos(), 101.9, {}, min(1)).action, 'PARTIAL');
});
test('bag closes at exactly +0.30% and never below, however long or deep', () => {
  const b = { entryPriceUSDT: 100 };
  for (const px of [50, 90, 99.99, 100, 100.29]) assert.equal(S.manageBag(b, px).action, 'HOLD');
  assert.equal(S.manageBag(b, 100.3).action, 'EXIT');
  assert.ok(Math.abs(b.targetExitPriceUSDT - 100.3) < 1e-9);
  assert.ok(S.netPnl(0.05, 100, 100.3) > 0, '+0.30% is still net positive after both fees');
});
