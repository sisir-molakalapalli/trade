'use strict';
// v6.13: from +0.50% the stop trails 0.20% behind the peak, at every level, long and short.
const test = require('node:test');
const assert = require('node:assert/strict');
const S = require('../strategy');
const T0 = 1e12, min = (m) => T0 + m * 60000;
const FUT = { ...S.EXIT_RULES, floorPct: S.EXIT_RULES.shortFloorPct };   // what the server passes for a futures long
const long = () => ({ pair: 'A/USDT', entryPriceUSDT: 100, openedAt: T0, qty: 0.2, ratchetStage: 'INITIAL', sl: 96.5, peakHighPrice: 100, peakGainPct: 0, tp1Secured: false });
const short = () => ({ side: 'SHORT', entryPriceUSDT: 100, openedAt: T0, qty: 0.2, sl: 103.5, peakLowPrice: 100, peakGainPct: 0, ratchetStage: 'INITIAL', tp1Secured: false });
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-9, `${a} vs ${b}`);

test('the rule is +0.50% and 0.20% behind the peak', () => {
  assert.equal(S.EXIT_RULES.earlyTrailAtPct, 0.5);
  assert.equal(S.EXIT_RULES.earlyTrailPct, 0.2);
});

test('below +0.50% nothing changes: the minimum exit is locked, no trail yet', () => {
  const p = long();
  assert.equal(S.manageNoLoss(p, 100.45, {}, min(2), FUT).action, 'HOLD');
  assert.equal(p.ratchetStage, 'BREAKEVEN_LOCKED');
  near(p.sl, 100.2);
});

test('the first surge is kept: +0.60% then back to +0.40% closes there, not at +0.20%', () => {
  const p = long();
  assert.equal(S.manageNoLoss(p, 100.6, {}, min(2), FUT).action, 'HOLD');
  assert.equal(p.ratchetStage, 'DYNAMIC_TRAIL');
  near(p.sl, 100.4);
  assert.equal(S.manageNoLoss(p, 100.45, {}, min(3), FUT).action, 'HOLD');
  const r = S.manageNoLoss(p, 100.4, {}, min(4), FUT);
  assert.deepEqual(r, { action: 'EXIT', reason: 'TRAILING STOP' });
});

test('it follows the peak up at every level, tighter than the old trails, and through TP1', () => {
  const p = long();
  S.manageNoLoss(p, 101.2, {}, min(2), FUT);
  near(p.sl, 101.0);                 // old 0.35% trail would be 100.846
  const r = S.manageNoLoss(p, 101.9, {}, min(3), FUT);
  assert.equal(r.action, 'PARTIAL', 'TP1 still banks half at +1.80%');
  assert.equal(p.ratchetStage, 'RUNNER_TRAIL');
  near(p.sl, 101.7);                 // old runner trail 0.50% would be 101.39
  assert.equal(S.manageNoLoss(p, 101.71, {}, min(4), FUT).action, 'HOLD');
  assert.deepEqual(S.manageNoLoss(p, 101.7, {}, min(5), FUT), { action: 'EXIT', reason: 'RUNNER EXIT' });
});

test('the stop only moves up', () => {
  const p = long();
  S.manageNoLoss(p, 100.8, {}, min(2), FUT);
  near(p.sl, 100.6);
  S.manageNoLoss(p, 100.65, {}, min(3), FUT);
  near(p.sl, 100.6);
});

test('a short gets the mirror image', () => {
  const p = short();
  assert.equal(S.manageShort(p, 99.4, {}, min(2)).action, 'HOLD');
  assert.equal(p.ratchetStage, 'DYNAMIC_TRAIL');
  near(p.sl, 99.6);
  assert.equal(S.manageShort(p, 99.59, {}, min(3)).action, 'HOLD');
  assert.deepEqual(S.manageShort(p, 99.6, {}, min(4)), { action: 'EXIT', reason: 'TRAILING STOP' });
});

test('never below the minimum exit, and a losing trade is still never closed', () => {
  const spot = long();   // spot long: +0.30% minimum
  S.manageNoLoss(spot, 100.5, {}, min(2));
  near(spot.sl, 100.3);
  const q = long();
  assert.equal(S.manageNoLoss(q, 99.0, { bearishPattern: 'X', peakExhausted: true }, min(500), FUT).action, 'HOLD');
});
