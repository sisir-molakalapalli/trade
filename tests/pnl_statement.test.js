'use strict';
// v6.11: the P&L statement behind Realized P&L, Take-home after tax and the statement sheet.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const Strategy = require('../strategy.js');

const NOW = Date.parse('2026-11-13T15:40:00Z');   // 21:10 IST, 13 Nov
// What the engine records (net of fees, with side / market / exact size) ...
const eng = (id, pair, type, entry, exit, notional, pnl, extra = {}) => ({ id, date: '13/11/2026', time: '19:08:43', pair, type, side: /^SHORT/.test(type) ? 'SHORT' : 'LONG', market: 'FUTURES', entryPriceUSDT: entry, exitPriceUSDT: exit, notionalUSDT: notional, pnlUSDT: pnl, feesIncluded: true, fundingUSDT: 0, ...extra });
// ... and what the sheet sends back for the same trade: no side / market / fees flag, size worked back from net P&L.
const sheetOf = (t) => ({ id: t.id, date: t.date, time: t.time, pair: t.pair, type: t.type, entryPriceUSDT: t.entryPriceUSDT, exitPriceUSDT: t.exitPriceUSDT,
  notionalUSDT: Number((Math.abs(t.pnlUSDT) / Math.abs((t.exitPriceUSDT - t.entryPriceUSDT) / t.entryPriceUSDT)).toFixed(2)), pnlUSDT: t.pnlUSDT, reason: '' });

// P&L as the engine books it: size x move less 0.05% a side, rounded to 4 decimals
const net = (side, size, entry, exit) => Number((side === 'SHORT' ? Strategy.netPnlShort(size / entry, entry, exit) : Strategy.netPnlLongFutures(size / entry, entry, exit)).toFixed(4));
const UNI = eng(1794583577208, 'UNI/USDT', 'SHORT BAG RECOVERED +0.30%', 5.795, 5.777, 22, net('SHORT', 22, 5.795, 5.777));
const ZRO = eng(1794577123563, 'ZRO/USDT', 'LONG TRAILING STOP', 1.6107, 1.6272, 10.8, net('LONG', 10.8, 1.6107, 1.6272));
const JUP = eng(1794577123563, 'JUP/USDT', 'LONG PEAK HARVEST', 0.2665, 0.2682, 10.8, net('LONG', 10.8, 0.2665, 0.2682));   // same millisecond, another coin
const LOSS = eng(1794590000000, 'PONS/USDT', 'LONG BAG MANUAL EXIT', 0.1567, 0.1453, 10.79, -0.7958, { date: '13/11/2026' });
const OLD = eng(1794470000000, 'QNT/USDT', 'SHORT TRAILING STOP', 157.55, 156.77, 22, 0.087, { date: '12/11/2026', time: '13:48:18', fundingUSDT: -0.002 });
const TP1 = eng(1794480000000, 'SOL/USDT', 'LONG TP1 1.8%', 100, 101.8, 10, 0.17, { date: '12/11/2026' });
const TEST = { id: 1794585000000, date: '13/11/2026', time: '21:00:00', pair: 'TEST/USDT', type: 'DIAGNOSTIC TEST WRITE', entryPriceUSDT: 1, exitPriceUSDT: 1.005, notionalUSDT: 10, pnlUSDT: 0.05 };

const near = (a, b, msg) => assert.ok(Math.abs(a - b) < 1e-6, `${msg}: ${a} vs ${b}`);

test('each trade counts once: the engine copy and the sheet copy share an id', () => {
  const engine = [UNI, ZRO, JUP];
  const s = Strategy.pnlStatement([...engine, ...engine.map(sheetOf), sheetOf(UNI)], NOW);
  assert.strictEqual(s.records, 3);
  assert.strictEqual(s.trades, 3);
  near(s.netUSDT, UNI.pnlUSDT + ZRO.pnlUSDT + JUP.pnlUSDT, 'net');
});

test('tax is 31.2% of each winning trade; a loss does not reduce it', () => {
  const s = Strategy.pnlStatement([UNI, ZRO, LOSS], NOW);
  const w = UNI.pnlUSDT + ZRO.pnlUSDT;
  near(s.winUSDT, w, 'wins');
  near(s.lossUSDT, -0.7958, 'losses');
  near(s.netUSDT, w - 0.7958, 'net');
  near(s.taxUSDT, w * 0.312, 'tax');
  near(s.takeHomeUSDT, w - 0.7958 - w * 0.312, 'take-home');
  assert.strictEqual(s.wins, 2); assert.strictEqual(s.losses, 1);
  assert.deepStrictEqual(s.worst, { pair: 'PONS/USDT', pnlUSDT: -0.7958, date: '13/11/2026' });
});

test('fees are the same from the engine copy and from the sheet copy (whose size is too small)', () => {
  const a = Strategy.pnlStatement([UNI, ZRO], NOW), b = Strategy.pnlStatement([sheetOf(UNI), sheetOf(ZRO)], NOW);
  const exact = (22 / 5.795) * (5.795 + 5.777) * 0.0005 + (10.8 / 1.6107) * (1.6107 + 1.6272) * 0.0005;
  near(a.feesUSDT, exact, 'engine fees');
  assert.ok(Math.abs(b.feesUSDT - exact) < 0.0002, `sheet fees ${b.feesUSDT} vs ${exact}`);
  near(a.movesUSDT, a.netUSDT + a.feesUSDT, 'moves = net + fees');
});

test('days, today, sides, exits; a partial close is money but not another trade; test writes are left out', () => {
  const s = Strategy.pnlStatement([UNI, ZRO, LOSS, OLD, TP1, TEST], NOW);
  assert.strictEqual(s.records, 5);
  assert.strictEqual(s.trades, 4, 'TP1 is not a separate trade');
  assert.strictEqual(s.firstDate, '12/11/2026'); assert.strictEqual(s.lastDate, '13/11/2026'); assert.strictEqual(s.todayKey, '13/11/2026');
  assert.deepStrictEqual(s.days.map((d) => d.date), ['13/11/2026', '12/11/2026']);
  assert.strictEqual(s.today.trades, 3);
  near(s.today.netUSDT, UNI.pnlUSDT + ZRO.pnlUSDT - 0.7958, 'today net');
  near(s.days[1].netUSDT, 0.087 + 0.17, '12 Nov net');
  near(s.fundingUSDT, -0.002, 'funding');
  assert.strictEqual(s.sides.SHORT.trades, 2); assert.strictEqual(s.sides.LONG.trades, 2);
  assert.ok(s.exits.some((x) => x.type === 'TRAILING STOP' && x.trades === 2));
  assert.ok(!JSON.stringify(s).includes('TEST/USDT'));
  assert.strictEqual(Strategy.isTestTrade(TEST), true);
  assert.strictEqual(Strategy.isTestTrade(UNI), false);
});

test('no trades, bad rows', () => {
  const s = Strategy.pnlStatement([null, {}, { pair: 'X/USDT', pnlUSDT: 'abc' }, { pair: 'Y/USDT', pnlUSDT: null }], NOW);
  assert.strictEqual(s.records, 0); assert.strictEqual(s.netUSDT, 0); assert.strictEqual(s.takeHomeUSDT, 0);
  assert.strictEqual(s.best, null); assert.deepStrictEqual(s.days, []);
});

test('the app runs the same statement code as the engine', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'strategy.js'), 'utf8');
  const block = (s) => s.slice(s.indexOf('/* PNL_STATEMENT_BEGIN'), s.indexOf('/* PNL_STATEMENT_END */'));
  assert.ok(block(html).length > 1000, 'statement block found in index.html');
  assert.strictEqual(block(html), block(js), 'the two copies differ');
  const appFn = new Function(`${block(html)}; return pnlStatement;`)();
  const rows = [UNI, ZRO, JUP, LOSS, OLD, TP1, TEST, ...[UNI, OLD].map(sheetOf)];
  assert.deepStrictEqual(appFn(rows, NOW), Strategy.pnlStatement(rows, NOW));
});
