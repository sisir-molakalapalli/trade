/**
 * =========================================================================
 * SISIR QUANT - BACKTESTER (runs the exact strategy.js the engine trades)
 * =========================================================================
 * Usage (on your PC / Render shell — needs internet to reach Binance):
 *   node backtest.js                      top 25 USDT pairs, last 30 days
 *   node backtest.js --days 60 --top 40
 *   node backtest.js --symbols SOLUSDT,AVAXUSDT,LINKUSDT --days 45
 *   node backtest.js --synthetic          offline self-test on generated data
 *
 * Fill model (deliberately pessimistic):
 *   - signal on a CLOSED 15m bar, entry at the NEXT bar's open + 0.05% slippage
 *   - if a bar's low touches the stop, the stop fills at min(stop, open)
 *   - stop is checked before any profit target inside the same bar
 *   - fees 0.10% per side on every fill
 * Each symbol is simulated independently (max-concurrent slots are ignored).
 * The 5m bearish veto is not applied here (it only removes trades live).
 * =========================================================================
 */
'use strict';
const https = require('https');
const S = require('./strategy');

const args = process.argv.slice(2);
const arg = (name, def) => { const i = args.indexOf(`--${name}`); return i >= 0 ? args[i + 1] : def; };
const DAYS = Number(arg('days', 30));
const TOP = Number(arg('top', 25));
const SYMBOLS = arg('symbols', '') ? arg('symbols').split(',').map((s) => s.trim().toUpperCase()) : null;
const SYNTHETIC = args.includes('--synthetic');
const SLIPPAGE = 0.0005;
const M15 = 15 * 60000, H1 = 3600000;

// ------------------------------------------------------------------ data
function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'SisirQuant-Backtest/5.0' }, timeout: 15000 }, (res) => {
      let d = '';
      res.on('data', (c) => (d += c));
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch (e) { reject(new Error(`Bad JSON from ${url}`)); } });
    }).on('error', reject).on('timeout', function () { this.destroy(new Error('timeout')); });
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchKlines(symbol, interval, startMs, endMs) {
  const out = [];
  let from = startMs;
  while (from < endMs) {
    const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&startTime=${from}&endTime=${endMs}&limit=1000`;
    const rows = await getJson(url);
    if (!Array.isArray(rows)) throw new Error(`${symbol} ${interval}: ${JSON.stringify(rows).slice(0, 120)}`);
    if (!rows.length) break;
    out.push(...rows);
    from = Number(rows[rows.length - 1][0]) + 1;
    if (rows.length < 1000) break;
    await sleep(120);
  }
  return out;
}

async function pickSymbols() {
  if (SYMBOLS) return SYMBOLS;
  const t = await getJson('https://api.binance.com/api/v3/ticker/24hr');
  const bad = /(UP|DOWN|BULL|BEAR)USDT$|^(USDC|FDUSD|TUSD|EUR|BUSD|DAI|USDP|PAXG|BTC)USDT$/;
  return t.filter((x) => x.symbol.endsWith('USDT') && !bad.test(x.symbol) && Number(x.lastPrice) < 350)
    .sort((a, b) => Number(b.quoteVolume) - Number(a.quoteVolume))
    .slice(0, TOP).map((x) => x.symbol);
}

// ------------------------------------------------------------- synthetic
function syntheticSeries(seed, n15, drift) {
  let s = seed;
  const rnd = () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
  const g = () => Math.sqrt(-2 * Math.log(Math.max(rnd(), 1e-12))) * Math.cos(2 * Math.PI * rnd());
  const start = Date.UTC(2026, 6, 1);
  const k15 = [];
  let p = 50, regime = 0;
  for (let i = 0; i < n15; i++) {
    if (i % 192 === 0) regime = (rnd() - 0.45) * 0.0012 + drift;    // new 2-day regime
    const o = p, c = p * Math.exp(regime + 0.0045 * g());
    const h = Math.max(o, c) * (1 + Math.abs(g()) * 0.0018), l = Math.min(o, c) * (1 - Math.abs(g()) * 0.0018);
    const v = 1000 * (0.6 + rnd()) * (1 + Math.max(0, (c - o) / o) * 150);
    const t = start + i * M15;
    k15.push([t, o, h, l, c, v, t + M15 - 1, v * c, 100, v * (0.45 + rnd() * 0.15), 0, 0]);
    p = c;
  }
  const k1h = [];
  for (let i = 0; i + 4 <= k15.length; i += 4) {
    const g4 = k15.slice(i, i + 4);
    k1h.push([g4[0][0], g4[0][1], Math.max(...g4.map((b) => b[2])), Math.min(...g4.map((b) => b[3])), g4[3][4],
      g4.reduce((a, b) => a + b[5], 0), g4[0][0] + H1 - 1, 0, 0, g4.reduce((a, b) => a + b[9], 0), 0, 0]);
  }
  return { k15, k1h };
}

// ------------------------------------------------------------ simulation
function lastBefore(arr, closeTimeLimit, count, fromIdx) {
  // arr sorted; returns last `count` rows with closeTime <= limit, and the index reached
  let i = fromIdx;
  while (i + 1 < arr.length && Number(arr[i + 1][6]) <= closeTimeLimit) i++;
  return { rows: arr.slice(Math.max(0, i - count + 1), i + 1), idx: i };
}

function simulateSymbol(symbol, k15, k1h, btc15, btc1h) {
  const trades = [];
  let pos = null, cooldownUntil = 0;
  let i1h = 0, ib1h = 0, ib15 = 0;
  const cfg = S.CONFIG;

  for (let i = 100; i < k15.length - 1; i++) {
    const bar = { t: Number(k15[i][0]), o: Number(k15[i][1]), h: Number(k15[i][2]), l: Number(k15[i][3]), c: Number(k15[i][4]), ct: Number(k15[i][6]) };

    // ---- manage open position on this bar ----
    if (pos && bar.t >= pos.openedAt) {
      const exit = (price, reason) => {
        const qty = pos.qty;
        const pnl = S.netPnl(qty, pos.entryPriceUSDT, price) + pos.realized;
        trades.push({ symbol, open: pos.openedAt, close: bar.ct, entry: pos.entryPriceUSDT, exit: price, reason,
          pnl, pnlPct: (pnl / pos.initialNotional) * 100, R: pnl / pos.riskUSDT });
        if (pnl < 0) cooldownUntil = bar.ct + cfg.symbolCooldownMin * 60000;
        pos = null;
      };
      if (bar.l <= pos.sl) { exit(Math.min(pos.sl, bar.o), pos.ratchetStage === 'INITIAL' ? 'STOP LOSS' : 'BE/TRAIL STOP'); }
      else {
        const rH = S.managePosition(pos, bar.h, bar.t + M15 / 2);
        if (rH.action === 'PARTIAL') {
          const fillP = pos.tp1Price, q = pos.qty * rH.fraction;
          pos.realized += S.netPnl(q, pos.entryPriceUSDT, fillP); pos.qty -= q;
          if (bar.l <= pos.sl) exit(pos.sl, 'TP1 then STOP (same bar)');
        } else if (rH.action === 'EXIT' && /TARGET/.test(rH.reason)) {
          exit(pos.targetPrice, rH.reason);
        }
        if (pos) {
          const rC = S.managePosition(pos, bar.c, bar.ct);
          if (rC.action === 'EXIT') exit(bar.c, rC.reason);
          else if (rC.action === 'PARTIAL') { const q = pos.qty * rC.fraction; pos.realized += S.netPnl(q, pos.entryPriceUSDT, bar.c); pos.qty -= q; }
        }
      }
      continue;
    }
    if (pos || bar.ct < cooldownUntil) continue;

    // ---- look for a new signal on this CLOSED bar ----
    const now = bar.ct + 1;
    const h = lastBefore(k1h, bar.ct, 100, i1h); i1h = h.idx;
    if (btc1h) {
      const b1 = lastBefore(btc1h, bar.ct, 100, ib1h); ib1h = b1.idx;
      const b15 = lastBefore(btc15, bar.ct, 10, ib15); ib15 = b15.idx;
      if (!S.evaluateBtcRegime(b1.rows, b15.rows, now).ok) continue;
    }
    const sig = S.evaluateEntry({ k15: k15.slice(i - 99, i + 1), k1h: h.rows, now });
    if (!sig.ok) continue;
    const nextOpen = Number(k15[i + 1][1]) * (1 + SLIPPAGE);
    const plan = S.planEntry(sig, nextOpen, 1000, 1000);
    if (!plan.ok) continue;
    const R = plan.entryPrice - plan.stopPrice;
    pos = {
      openedAt: Number(k15[i + 1][0]), entryPriceUSDT: plan.entryPrice, initialStopPrice: plan.stopPrice, sl: plan.stopPrice,
      qty: plan.qty, initialNotional: plan.notional, riskUSDT: plan.riskUSDT, atrPct: sig.atrPct,
      ratchetStage: 'INITIAL', tp1Secured: false, realized: 0, pattern: sig.pattern,
      tp1Price: plan.entryPrice + cfg.tp1AtR * R, targetPrice: plan.entryPrice + cfg.finalTargetR * R
    };
  }
  return trades;
}

// ------------------------------------------------------------- reporting
function report(all) {
  if (!all.length) { console.log('\nNo trades. Filters found no qualifying setups in this period.'); return; }
  all.sort((a, b) => a.close - b.close);
  const wins = all.filter((t) => t.pnl > 0), losses = all.filter((t) => t.pnl <= 0);
  const sum = (a, f) => a.reduce((s, t) => s + f(t), 0);
  const gp = sum(wins, (t) => t.pnl), gl = -sum(losses, (t) => t.pnl);
  let eq = 0, peak = 0, dd = 0;
  all.forEach((t) => { eq += t.R; peak = Math.max(peak, eq); dd = Math.min(dd, eq - peak); });
  const reasons = {};
  all.forEach((t) => { reasons[t.reason] = (reasons[t.reason] || 0) + 1; });

  console.log('\n================ BACKTEST RESULT ================');
  console.log(`trades              ${all.length}`);
  console.log(`win rate            ${(100 * wins.length / all.length).toFixed(1)}%`);
  console.log(`avg win / avg loss  +${(sum(wins, (t) => t.pnlPct) / (wins.length || 1)).toFixed(2)}% / ${(sum(losses, (t) => t.pnlPct) / (losses.length || 1)).toFixed(2)}%   (of position, net of fees)`);
  console.log(`expectancy          ${(sum(all, (t) => t.R) / all.length).toFixed(3)} R per trade`);
  console.log(`profit factor       ${gl > 0 ? (gp / gl).toFixed(2) : '∞'}`);
  console.log(`total               ${sum(all, (t) => t.R).toFixed(2)} R   (1R = 0.5% of equity)`);
  console.log(`max drawdown        ${dd.toFixed(2)} R`);
  console.log(`worst trade         ${Math.min(...all.map((t) => t.pnlPct)).toFixed(2)}% of position`);
  console.log(`trades worse -2%    ${all.filter((t) => t.pnlPct <= -2).length}`);
  console.log(`exit reasons        ${Object.entries(reasons).map(([k, v]) => `${k}:${v}`).join('  ')}`);
  const bySym = {};
  all.forEach((t) => { (bySym[t.symbol] = bySym[t.symbol] || []).push(t); });
  console.log('\nper symbol (R):');
  Object.entries(bySym).sort((a, b) => sum(b[1], (t) => t.R) - sum(a[1], (t) => t.R))
    .forEach(([s, ts]) => console.log(`  ${s.padEnd(12)} ${String(ts.length).padStart(3)} trades  ${sum(ts, (t) => t.R).toFixed(2).padStart(7)} R`));
  console.log('\nRead this before going live: a positive expectancy over 100+ trades is the minimum bar.');
  console.log('If expectancy <= 0, do NOT trade real money. Tune entries, not stops.');
}

// ------------------------------------------------------------------ main
(async () => {
  const all = [];
  if (SYNTHETIC) {
    console.log('Synthetic self-test (random regimes, no real market data)...');
    const btc = syntheticSeries(99, 96 * DAYS + 400, 0.00015);
    for (let k = 0; k < 10; k++) {
      const d = syntheticSeries(1000 + k, 96 * DAYS + 400, 0);
      all.push(...simulateSymbol(`SYN${k}USDT`, d.k15, d.k1h, btc.k15, btc.k1h));
    }
    report(all);
    return;
  }
  const end = Date.now();
  const start15 = end - DAYS * 86400000 - 110 * M15;
  const start1h = end - DAYS * 86400000 - 110 * H1;
  console.log(`Backtesting ${DAYS} days...`);
  const btc15 = await fetchKlines('BTCUSDT', '15m', start15, end);
  const btc1h = await fetchKlines('BTCUSDT', '1h', start1h, end);
  const syms = await pickSymbols();
  for (const sym of syms) {
    try {
      const k15 = await fetchKlines(sym, '15m', start15, end);
      const k1h = await fetchKlines(sym, '1h', start1h, end);
      const t = simulateSymbol(sym, k15, k1h, btc15, btc1h);
      process.stdout.write(`  ${sym.padEnd(12)} ${t.length} trades\n`);
      all.push(...t);
    } catch (e) {
      console.log(`  ${sym} skipped: ${e.message}`);
    }
  }
  report(all);
})().catch((e) => { console.error('Backtest failed:', e.message); process.exit(1); });
