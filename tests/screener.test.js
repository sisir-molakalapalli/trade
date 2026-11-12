'use strict';
// v6.7: the radar is built from USD-M futures tickers, not spot.
const test = require('node:test');
const assert = require('node:assert');
const S = require('../strategy');

const tk = (symbol, quoteVolume, lastPrice = 1) => ({ symbol, quoteVolume: String(quoteVolume), lastPrice: String(lastPrice) });
const perps = new Set(['BTCUSDT', 'ETHUSDT', 'ZECUSDT', '1000PEPEUSDT', 'SOLUSDT', 'BTCDOMUSDT', 'USDCUSDT', 'TUSDT', '牛来USDT', 'SMALLUSDT', 'NEWUSDT']);

test('only trading USDT perpetuals with $10M+ futures volume, biggest first', () => {
  const list = S.futuresUniverse([
    tk('SOLUSDT', 300e6, 80), tk('BTCUSDT', 9000e6, 72000), tk('ETHUSDT', 4000e6, 2000), tk('SMALLUSDT', 9.99e6),
    tk('SPOTONLYUSDT', 500e6),              // no perpetual
    tk('ETHBTC', 900e6),                    // not a USDT pair
  ], perps);
  assert.deepStrictEqual(list.map((t) => t.symbol), ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']);
});

test('no price cap, and 1000x contracts are kept under their own name', () => {
  const list = S.futuresUniverse([tk('ZECUSDT', 175e6, 944), tk('1000PEPEUSDT', 300e6, 0.009)], perps);
  assert.deepStrictEqual(list.map((t) => t.symbol), ['1000PEPEUSDT', 'ZECUSDT']);
});

test('indexes, stablecoins and non-letter names are left out; the T token is not TUSD', () => {
  const list = S.futuresUniverse([tk('BTCDOMUSDT', 50e6), tk('USDCUSDT', 900e6), tk('牛来USDT', 80e6), tk('TUSDT', 20e6)], perps);
  assert.deepStrictEqual(list.map((t) => t.symbol), ['TUSDT']);
});

test('the cap keeps the biggest by volume', () => {
  const many = Array.from({ length: 30 }, (_, i) => tk(`C${i}USDT`, (i + 1) * 20e6));
  const p = new Set(many.map((t) => t.symbol));
  const list = S.futuresUniverse(many, p, { ...S.SCREENER, maxCoins: 5 });
  assert.deepStrictEqual(list.map((t) => t.symbol), ['C29USDT', 'C28USDT', 'C27USDT', 'C26USDT', 'C25USDT']);
  assert.strictEqual(S.futuresUniverse(many, p).length, 30);
});

test('defaults: $10M floor (same as the trade floor), 150 coins', () => {
  assert.strictEqual(S.SCREENER.minVolumeUSD, 10000000);
  assert.strictEqual(S.SCREENER.maxCoins, 150);
});

test('bad input gives an empty list', () => {
  assert.deepStrictEqual(S.futuresUniverse(null, perps), []);
  assert.deepStrictEqual(S.futuresUniverse([tk('BTCUSDT', 1e9)], null), []);
  assert.deepStrictEqual(S.futuresUniverse([null, {}, tk('ETHUSDT', 'x'), tk('BTCUSDT', 1e9, 0)], perps), []);
});
