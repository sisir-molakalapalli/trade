'use strict';
// v6.8: Settings > Leverage. Auto leverage from volatility, isolated-margin liquidation, bag top-up to 1x.
const test = require('node:test');
const assert = require('node:assert');
const S = require('../strategy');
const sig = { ok: true, stopPrice: 99, stopPct: 1 };
const at = (pct, L) => ({ ...S.FUTURES, positionPct: pct, leverage: L });

test('auto leverage: floor(1.5 / 15m ATR%), 1x to 3x', () => {
  assert.strictEqual(S.autoLeverage({ atrPct: 0.3 }), 3);
  assert.strictEqual(S.autoLeverage({ atrPct: 0.499 }), 3);
  assert.strictEqual(S.autoLeverage({ atrPct: 0.5 }), 3);   // 1.5 / 0.5 = 3
  assert.strictEqual(S.autoLeverage({ atrPct: 0.51 }), 2);
  assert.strictEqual(S.autoLeverage({ atrPct: 0.75 }), 2);
  assert.strictEqual(S.autoLeverage({ atrPct: 0.76 }), 1);
  assert.strictEqual(S.autoLeverage({ atrPct: 2 }), 1);
  assert.strictEqual(S.autoLeverage({ atrPct: 0.01 }), 3);  // never above the 3x cap
  for (const bad of [0, -1, NaN, undefined, null, 'x']) assert.strictEqual(S.autoLeverage({ atrPct: bad }), 1, String(bad));
  assert.strictEqual(S.autoLeverage(), 1);
});

test('20% of $110 as margin: 1x is unchanged, 3x is a $66 position on $22 margin', () => {
  const one = S.planCompoundEntry(sig, 100, 110, 110, at(20, 1));
  assert.strictEqual(one.notional, 22); assert.strictEqual(one.margin, 22); assert.strictEqual(one.leverage, 1);
  const three = S.planCompoundEntry(sig, 100, 110, 110, at(20, 3));
  assert.strictEqual(three.margin, 22); assert.strictEqual(three.notional, 66); assert.strictEqual(three.leverage, 3);
  assert.ok(Math.abs(three.qty - 0.66) < 1e-12);
});

test('free cash must cover the margin plus the opening fee on the whole position', () => {
  const p = S.planCompoundEntry(sig, 100, 110, 10, at(20, 3));   // only $10 free
  assert.ok(p.margin + p.notional * 0.0005 <= 10 + 1e-9, JSON.stringify(p));
  assert.strictEqual(p.margin, 9.98);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 1.6, at(20, 3)).ok, false);   // under the $5 minimum position
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 1.7, at(20, 3)).notional, 5.07); // $1.69 margin (fee kept back) x 3 clears it
});

test('isolated liquidation price (1% maintenance); 1x keeps the old behaviour', () => {
  assert.ok(Math.abs(S.liquidationPrice(100, 'LONG', 3) - 100 * (2 / 3) / 0.99) < 1e-9);
  assert.ok(Math.abs(S.liquidationPrice(100, 'SHORT', 3) - 100 * (4 / 3) / 1.01) < 1e-9);
  assert.ok(Math.abs(S.liquidationPrice(100, 'LONG', 2) - 50 / 0.99) < 1e-9);
  assert.strictEqual(S.liquidationPrice(100, 'LONG', 1), 0);                              // a 1x long is never liquidated
  assert.strictEqual(S.liquidationPrice(100, 'SHORT', 1), S.shortLiquidationPrice(100)); // same as before v6.8
  // a 3x trade moves to a bag (-3.50%) long before liquidation (about -32.7% / +32.0%)
  assert.ok(S.liquidationPrice(100, 'LONG', 3) < 96.5 && S.liquidationPrice(100, 'SHORT', 3) > 103.5);
});

test('bag top-up: all that is needed for 1x, or what free cash allows', () => {
  assert.strictEqual(S.bagTopUp(22, 66, 100), 44);
  assert.strictEqual(S.bagTopUp(22, 66, 20.5), 20.49);   // keeps a cent
  assert.strictEqual(S.bagTopUp(22, 66, 0), 0);
  assert.strictEqual(S.bagTopUp(66, 66, 100), 0);         // already 1x
  // after a partial top-up the effective leverage is lower and liquidation is further away
  const L = 66 / (22 + 20.49);
  assert.ok(S.liquidationPrice(100, 'LONG', L) < S.liquidationPrice(100, 'LONG', 3));
});

test('leverage defaults', () => {
  assert.deepStrictEqual(S.LEVERAGE, { maxLeverage: 3, atrTargetPct: 1.5, mmrPct: 1.0 });
  assert.strictEqual(S.FUTURES.leverage, 1);
});
