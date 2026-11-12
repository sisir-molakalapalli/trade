'use strict';
// v6.5: Settings > High vol breakdown > Long. A breakdown long closes only at +0.30%, or moves to a recovery bag at -3.50%.
const test = require('node:test');
const assert = require('node:assert');
const S = require('../strategy');

const pos = (over = {}) => ({ entryPriceUSDT: 100, qty: 0.1, fundingUSDT: 0, openedAt: Date.now(), targetOnlyPct: 0.30, ...over });

test('breakdown long target is +0.30%', () => {
  assert.strictEqual(S.BREAKDOWN_LONG.targetPct, 0.30);
});

test('holds below +0.30%, closes at +0.30%', () => {
  const p = pos();
  assert.deepStrictEqual(S.manageTargetLong(p, 100.29), { action: 'HOLD' });
  assert.deepStrictEqual(S.manageTargetLong(p, 100.30), { action: 'EXIT', reason: 'TARGET +0.30%' });
  assert.ok(Math.abs(p.targetPrice - 100.3) < 1e-9);
});

test('no early exits: a normal long would time-scratch at +0.25% after 45 minutes, this one holds', () => {
  const opened = Date.now() - 60 * 60000;
  const normal = { entryPriceUSDT: 100, qty: 0.1, openedAt: opened, ratchetStage: 'INITIAL' };
  const r = S.manageNoLoss(normal, 100.25, { peakExhausted: true }, Date.now(), { ...S.EXIT_RULES, floorPct: S.EXIT_RULES.shortFloorPct });
  assert.strictEqual(r.action, 'EXIT');
  assert.deepStrictEqual(S.manageTargetLong(pos({ openedAt: opened }), 100.25), { action: 'HOLD' });
});

test('moves to a recovery bag at -3.50%, holds at -3.49%', () => {
  assert.deepStrictEqual(S.manageTargetLong(pos(), 96.51), { action: 'HOLD' });
  assert.deepStrictEqual(S.manageTargetLong(pos(), 96.5), { action: 'DEMOTE', reason: '-3.50%' });
});

test('funding it has paid moves the target up, so the close still makes +0.30%', () => {
  const p = pos({ fundingUSDT: -0.01 });   // paid $0.01 on a $10 position = 0.10%
  assert.deepStrictEqual(S.manageTargetLong(p, 100.30), { action: 'HOLD' });
  assert.deepStrictEqual(S.manageTargetLong(p, 100.40), { action: 'EXIT', reason: 'TARGET +0.30%' });
  assert.deepStrictEqual(S.manageTargetLong(pos({ fundingUSDT: 0.01 }), 100.30).action, 'EXIT');   // funding received does not lower it
});

test('closing at the target is a profit after both fees (about +0.20%)', () => {
  const qty = 10 / 100, pnl = S.netPnlLongFutures(qty, 100, 100.3);
  assert.ok(pnl > 0);
  assert.ok(Math.abs(pnl / 10 * 100 - 0.19985) < 0.0001, `${pnl / 10 * 100}`);   // 0.30% - fees 0.05% x (1 + 1.003)
});

test('tracks the peak for the position card', () => {
  const p = pos();
  S.manageTargetLong(p, 100.2); S.manageTargetLong(p, 99.5);
  assert.strictEqual(p.peakGainPct, 0.2);
  assert.strictEqual(p.peakHighPrice, 100.2);
});
