'use strict';
const test = require('node:test');
const assert = require('node:assert');
const S = require('../strategy');
const C = require('../candles');

const hourly = (n, f, t0 = 1700000000000) => Array.from({ length: n }, (_, i) => {
  const c = f(i), o = f(Math.max(0, i - 1));
  return [t0 + i * 3600000, String(o), String(Math.max(o, c) * 1.001), String(Math.min(o, c) * 0.999), String(c), '100', t0 + (i + 1) * 3600000 - 1, '0', '0', '50'];
});
const up = (i) => 100 * Math.pow(1.002, i), down = (i) => 100 * Math.pow(0.998, i);
const shortPos = (over = {}) => ({ side: 'SHORT', entryPriceUSDT: 100, qty: 0.05, openedAt: 1000, sl: 102.8, peakLowPrice: 100, peakGainPct: 0, ratchetStage: 'INITIAL', tp1Secured: false, ...over });

test('market mode: bull needs BTC uptrend and breadth', () => {
  const coins = Array.from({ length: 12 }, () => hourly(90, up));
  assert.strictEqual(S.evaluateMarketMode(hourly(90, up), coins).side, 'LONG');
  assert.strictEqual(S.evaluateMarketMode(hourly(90, up), Array.from({ length: 12 }, () => hourly(90, down))).side, 'BOTH');
});
test('market mode: bear needs BTC downtrend and weak breadth', () => {
  const coins = Array.from({ length: 12 }, () => hourly(90, down));
  assert.strictEqual(S.evaluateMarketMode(hourly(90, down), coins).side, 'SHORT');
});
test('market mode: missing data opens nothing', () => {
  assert.strictEqual(S.evaluateMarketMode(null, []).side, 'NONE');
  assert.strictEqual(S.evaluateMarketMode(hourly(90, down), []).side, 'NONE');
});
test('market mode: one fresh hour is not enough', () => {
  const flipLast = (i) => (i === 89 ? up(40) : down(i));
  const coins = Array.from({ length: 12 }, () => hourly(90, down));
  assert.strictEqual(S.evaluateMarketMode(hourly(90, flipLast), coins).side, 'BOTH');
});
test('short P&L: price down is profit, both fees at 0.05%', () => {
  const pnl = S.netPnlShort(1, 100, 99);
  assert.ok(Math.abs(pnl - (1 - 0.0995)) < 1e-9);
  assert.ok(S.netPnlShort(1, 100, 101) < 0);
});
test('short never exits in loss, and moves to a bag at -3.50%', () => {
  const p = shortPos();
  assert.strictEqual(S.manageShort(p, 101, {}, 2000).action, 'HOLD');
  assert.strictEqual(S.manageShort(p, 103.5, {}, 3000).action, 'DEMOTE');
});
test('short locks profit below entry and trails down', () => {
  const p = shortPos();
  S.manageShort(p, 99.3, {}, 2000);
  assert.strictEqual(p.ratchetStage, 'PROFIT_LOCKED');
  assert.ok(Math.abs(p.sl - 99.5) < 1e-6);
  assert.strictEqual(S.manageShort(p, 99.51, {}, 3000).action, 'EXIT');
  const q = shortPos();
  S.manageShort(q, 98.5, {}, 2000);
  assert.strictEqual(q.ratchetStage, 'DYNAMIC_TRAIL');
  assert.ok(q.sl < 99.65 && q.sl > 98.5);
  assert.strictEqual(q.peakLowPrice, 98.5);
});
test('short candle harvest needs the +0.20% minimum', () => {
  const sig = { bearishPattern: 'HAMMER' };
  assert.strictEqual(S.manageShort(shortPos(), 99.9, sig, 2000).action, 'HOLD');
  assert.strictEqual(S.manageShort(shortPos(), 99.8, sig, 2000).action, 'EXIT');
  assert.strictEqual(S.manageShort(shortPos(), 99.6, sig, 2000).action, 'EXIT');
});
test('short bag closes only at 0.30% below entry', () => {
  const b = { side: 'SHORT', entryPriceUSDT: 100, qty: 0.05 };
  assert.strictEqual(S.manageShortBag(b, 100, undefined).action, 'HOLD');
  assert.strictEqual(S.manageShortBag(b, 99.71).action, 'HOLD');
  assert.strictEqual(S.manageShortBag(b, 99.7).action, 'EXIT');
});
test('funding paid moves the short bag target so the close is not a loss', () => {
  const b = { side: 'SHORT', entryPriceUSDT: 100, qty: 0.05, fundingUSDT: -0.05 };
  assert.strictEqual(S.manageShortBag(b, 99.7).action, 'HOLD');
  assert.ok(Math.abs(b.targetExitPriceUSDT - 98.7) < 1e-6);
});
test('1x short liquidation is near double the entry', () => {
  const l = S.shortLiquidationPrice(100);
  assert.ok(l > 198 && l < 200);
});
test('assessShort: a bearish pattern in an up major trend is rejected', () => {
  const r = C.assessShort(hourly(60, down), { major: 'up' });
  assert.ok(r.vetoes.includes('major trend up'));
  assert.strictEqual(r.sell, false);
});

test('short exclusions: volume, funding, listing age', () => {
  const ok = { volumeUSD: 2e7, fundingRate: 0.0001, listedAt: 1, now: 1e12 };
  assert.strictEqual(S.shortExclusion(ok), null);
  assert.match(S.shortExclusion({ ...ok, volumeUSD: 9e6 }), /volume/);
  assert.match(S.shortExclusion({ ...ok, fundingRate: -0.0003 }), /crowded/);
  assert.strictEqual(S.shortExclusion({ ...ok, fundingRate: -0.00029 }), null);
  assert.match(S.shortExclusion({ ...ok, fundingRate: undefined }), /unknown/);
  assert.match(S.shortExclusion({ ...ok, listedAt: 1e12 - 86400000 }), /listed/);
});

const closesFrom = (n, f) => Array.from({ length: n }, (_, i) => f(i));
test('neutral market: a coin trades only with its own clear 1h trend', () => {
  assert.strictEqual(S.coinTrend(closesFrom(90, (i) => 100 + i)), 'LONG');
  assert.strictEqual(S.coinTrend(closesFrom(90, (i) => 200 - i)), 'SHORT');
  assert.strictEqual(S.coinTrend(closesFrom(90, () => 100)), null);
  assert.strictEqual(S.coinTrend(closesFrom(20, (i) => 100 + i)), null);
});

test('every trade is 20% of current account value by default, so size compounds', () => {
  const sig = { ok: true, stopPrice: 99, stopPct: 1 };
  assert.strictEqual(S.FUTURES.positionPct, 20);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 110).notional, 22);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 150, 150).notional, 30);
});

test('the size set in Settings is used (1% to 100%)', () => {
  const sig = { ok: true, stopPrice: 99, stopPct: 1 }, at = (pct) => ({ ...S.FUTURES, positionPct: pct });
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 110, at(10)).notional, 11);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 110, at(35)).notional, 38.5);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 110, at(1)).notional, 5);          // never under the $5 minimum
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 110, at(100)).notional, 109.94);   // all the cash, less the opening fee
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 30, at(50)).notional, 29.98);      // only what cash is free
});

test('only 1% to 100% is accepted as a position size', () => {
  assert.strictEqual(S.validPositionPct(20), 20);
  assert.strictEqual(S.validPositionPct('12.5'), 12.5);
  assert.strictEqual(S.validPositionPct(1), 1);
  assert.strictEqual(S.validPositionPct(100), 100);
  assert.strictEqual(S.validPositionPct(33.333), 33.33);
  for (const bad of [0, 0.5, 100.01, 150, -5, 'abc', '', null, undefined, NaN, Infinity]) assert.strictEqual(S.validPositionPct(bad), null, String(bad));
});

test('compounding uses what cash is left, and refuses under $5', () => {
  const sig = { ok: true, stopPrice: 99, stopPct: 1 };
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 7).notional, 6.99);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 110, 4.9).ok, false);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 30, 30).notional, 6);
  assert.strictEqual(S.planCompoundEntry(sig, 100, 20, 20).notional, 5);   // 20% of $20 is $4: the $5 minimum applies
});

test('futures limits apply in both directions, funding against the crowded side', () => {
  const ok = { volumeUSD: 2e7, fundingRate: 0.0001 };
  assert.strictEqual(S.futuresExclusion({ ...ok, side: 'LONG' }), null);
  assert.match(S.futuresExclusion({ ...ok, side: 'LONG', fundingRate: 0.0003 }), /crowded longs/);
  assert.match(S.futuresExclusion({ ...ok, side: 'SHORT', fundingRate: -0.0003 }), /crowded shorts/);
  assert.match(S.futuresExclusion({ side: 'LONG', volumeUSD: 5e6, fundingRate: 0 }), /\$10M/);
  assert.match(S.futuresExclusion({ side: 'LONG', volumeUSD: 2e7 }), /funding unknown/);
});

test('futures long P&L pays 0.05% per side', () => {
  assert.ok(Math.abs(S.netPnlLongFutures(1, 100, 101) - (1 - 0.1005)) < 1e-9);
});

test('futures long bag also earns back funding it paid; spot bag is unchanged', () => {
  const b = { market: 'FUTURES', entryPriceUSDT: 100, qty: 0.05, fundingUSDT: -0.05 };
  assert.strictEqual(S.manageBag(b, 101.29).action, 'HOLD');
  assert.strictEqual(S.manageBag(b, 101.31).action, 'EXIT');
  assert.strictEqual(S.manageBag({ entryPriceUSDT: 100, qty: 0.05, fundingUSDT: -0.05 }, 100.3).action, 'EXIT');
});
