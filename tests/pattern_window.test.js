'use strict';
// v6.10: a candle-pattern trade opens only within 5 minutes of the close of the candle that completed or confirmed it.
const test = require('node:test');
const assert = require('node:assert');
const Candles = require('../candles.js');
const Strategy = require('../strategy.js');

const T0 = Date.UTC(2026, 10, 1);
const bar = (i, o, h, l, c) => [T0 + i * 900000, o.toFixed(2), h.toFixed(2), l.toFixed(2), c.toFixed(2), '1000', T0 + (i + 1) * 900000 - 1, '0', 0, '500'];
// 60 rising candles, then a shooting star at the top
function rally() {
  const k = [];
  for (let i = 0; i < 60; i++) { const o = 100 + i * 0.1, c = o + 0.08; k.push(bar(i, o, c + 0.03, o - 0.03, c)); }
  k.push(bar(60, 106.0, 106.6, 105.98, 106.05));
  return k;
}
const star = (k) => Candles.scan(k).patterns.find((p) => p.name === 'SHOOTING STAR');

test('shooting star confirmed by the last candle: signal is fresh', () => {
  const k = rally(); k.push(bar(61, 106.0, 106.05, 105.65, 105.7));
  const s = star(k);
  assert.ok(s, 'shooting star found');
  assert.strictEqual(s.status, 'confirmed');
  assert.strictEqual(s.barsAgo, 1);
  assert.strictEqual(s.signalBarsAgo, 0);
  assert.strictEqual(Candles.assessShort(k).signalAge, 0);
});

test('confirmed a candle ago and again now: signal is one candle old', () => {
  const k = rally(); k.push(bar(61, 106.0, 106.05, 105.65, 105.7)); k.push(bar(62, 105.7, 105.75, 105.45, 105.5));
  const s = star(k);
  assert.strictEqual(s.status, 'confirmed');
  assert.strictEqual(s.barsAgo, 2);
  assert.strictEqual(s.signalBarsAgo, 1);
  assert.strictEqual(Candles.assessShort(k).signalAge, 1);
});

test('first confirmed two candles after the star: signal is fresh', () => {
  const k = rally(); k.push(bar(61, 106.0, 106.1, 105.95, 106.02)); k.push(bar(62, 106.02, 106.05, 105.65, 105.7));
  const s = star(k);
  assert.strictEqual(s.status, 'confirmed');
  assert.strictEqual(s.barsAgo, 2);
  assert.strictEqual(s.signalBarsAgo, 0);
});

test('entry window: open for 5 minutes after the signal candle closes', () => {
  const close = T0 + 61 * 900000;
  assert.strictEqual(Strategy.PATTERN_ENTRY.windowMin, 5);
  assert.strictEqual(Strategy.patternEntryOpen(close, 0, close + 1000), true);
  assert.strictEqual(Strategy.patternEntryOpen(close, 0, close + 5 * 60000), true);
  assert.strictEqual(Strategy.patternEntryOpen(close, 0, close + 5 * 60000 + 1000), false);
  assert.strictEqual(Strategy.patternEntryOpen(close, 0, close + 7 * 60000), false);
  assert.strictEqual(Strategy.patternEntryOpen(close, 1, close + 60000), false, 'a one-candle-old signal is 16 min old');
  assert.strictEqual(Math.round(Strategy.patternSignalAgeMin(close, 1, close + 60000)), 16);
  assert.strictEqual(Strategy.patternEntryOpen(undefined, 0, close), false, 'unknown close time: no pattern trade');
});
