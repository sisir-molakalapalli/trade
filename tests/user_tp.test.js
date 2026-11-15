'use strict';
// v6.12: your take profit on autopilot positions and recovery bags.
const test = require('node:test');
const assert = require('node:assert/strict');
const S = require('../strategy');
const near = (a, b, eps = 1e-9) => assert.ok(Math.abs(a - b) < eps * Math.max(1, Math.abs(b)), `${a} vs ${b}`);

test('a take profit is 0.1% to 100%; empty, null or 0 clears it', () => {
  assert.deepEqual(S.userTakeProfit(0.3), { ok: true, pct: 0.3 });
  assert.deepEqual(S.userTakeProfit('1.256'), { ok: true, pct: 1.26 });
  assert.deepEqual(S.userTakeProfit(null), { ok: true, pct: null });
  assert.deepEqual(S.userTakeProfit(''), { ok: true, pct: null });
  assert.deepEqual(S.userTakeProfit(0), { ok: true, pct: null });
  assert.equal(S.userTakeProfit(0.05).ok, false);
  assert.equal(S.userTakeProfit(-1).ok, false, 'never a stop loss');
  assert.equal(S.userTakeProfit(101).ok, false);
  assert.equal(S.userTakeProfit('abc').ok, false);
});

test('the take-profit price is above entry for a long, below for a short', () => {
  near(S.userTpPrice({ entryPriceUSDT: 100, userTpPct: 0.5, side: 'LONG' }), 100.5);
  near(S.userTpPrice({ entryPriceUSDT: 100, userTpPct: 0.5, side: 'SHORT' }), 99.5);
  near(S.userTpPrice({ entryPriceUSDT: 100, userTpPct: 0.5 }), 100.5);   // an old spot long has no side
  assert.equal(S.userTpPrice({ entryPriceUSDT: 100 }), null);
});

test('an open position closes only at the take profit: not at the minimum exit, not on the 45-minute scratch', () => {
  const t0 = Date.now() - 3 * 3600e3;
  const pos = { entryPriceUSDT: 100, side: 'LONG', openedAt: t0, userTpPct: 1 };
  assert.equal(S.manageUserTp(pos, 100.5).action, 'HOLD', '+0.5% is past the minimum exit, but the take profit is +1%');
  // the autopilot alone would have closed it here (held 3 hours, above the +0.30% minimum)
  assert.equal(S.manageNoLoss({ entryPriceUSDT: 100, openedAt: t0 }, 100.5).action, 'EXIT');
  const r = S.manageUserTp(pos, 101);
  assert.equal(r.action, 'EXIT');
  assert.equal(r.reason, 'YOUR TP');
  near(pos.peakGainPct, 1);
  near(pos.peakHighPrice, 101);
});

test('a short with a take profit: exit below entry, bag at -3.50%, peak tracked for the autopilot', () => {
  const pos = { entryPriceUSDT: 3764.51, side: 'SHORT', openedAt: Date.now(), userTpPct: 0.1 };
  assert.equal(S.manageUserTp(pos, 3818.53).action, 'HOLD', '-1.43%: wait');
  assert.equal(S.manageUserTp(pos, 3760.9).action, 'HOLD', '+0.096%: not yet');
  near(pos.peakLowPrice, 3760.9);
  assert.equal(S.manageUserTp(pos, 3760.7).action, 'EXIT', '+0.101%');
  const bagging = { entryPriceUSDT: 100, side: 'SHORT', userTpPct: 0.5 };
  assert.deepEqual(S.manageUserTp(bagging, 103.5), { action: 'DEMOTE', reason: '-3.50%' });
});

test('in a bag the take profit replaces +0.30%, and a bag is never demoted again', () => {
  const bag = { entryPriceUSDT: 1.533, side: 'SHORT', userTpPct: 0.1, market: 'FUTURES', qty: 16.2 };
  assert.equal(S.manageUserTp(bag, 1.62, { inBag: true }).action, 'HOLD', '-5.7% in a bag: hold, no demote');
  near(bag.targetExitPriceUSDT, 1.533 * 0.999);
  assert.equal(bag.targetStep, 'YOUR TP +0.1%');
  near(bag.troughPrice, 1.62);
  assert.equal(S.manageShortBag({ ...bag }, 1.5310).action, 'HOLD', 'the bag rule alone needs +0.30%');
  const r = S.manageUserTp(bag, 1.5310, { inBag: true });
  assert.deepEqual(r, { action: 'EXIT', reason: 'BAG YOUR TP' });
});

test('with no take profit it does nothing (the autopilot manages the trade)', () => {
  assert.equal(S.manageUserTp({ entryPriceUSDT: 100, side: 'LONG' }, 150).action, 'HOLD');
  assert.equal(S.manageUserTp({ entryPriceUSDT: 0, userTpPct: 1 }, 150).action, 'HOLD');
});
