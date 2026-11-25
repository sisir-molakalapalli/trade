'use strict';
/**
 * v6.42: the BTC 2-HOUR PREDICTION, worked out inside the app (no AI, no key, no cost): the user's PDF of 26 Nov 2026.
 * (v6.41's Gemini forecast was removed: it would have cost too much.)
 *
 * The chance that BTC closes above its 1h EMA50 two hours from now:
 *   D_t   = price vs the 1h EMA50 now (%)
 *   S_lead = 0.30 F15m + 0.20 Fema + 0.25 Fflow + 0.15 Foi + 0.10 Ffund, five leading signals squashed into -1..+1 with tanh:
 *            15m EMA20 vs EMA50, RSI and the last hour's move; the 1h EMA20's 2h slope vs the EMA50's 3h slope; taker buy/sell
 *            ratios and the order book's bid share; open interest with or against the move; funding (crowding, contrarian)
 *   sigma_2h = 1.13 x the 1h ATR%,  r_2h = 0.65 x sigma_2h x S_lead (the expected 2-hour move)
 *   D_t+2 = 0.9231 D_t + 0.9419 r_2h (two steps of the EMA50 recursion),  z = D_t+2 / sigma_2h,  P = 100 / (1 + e^(-1.702 z))
 * Every 5 minutes (just after each 5m close), or sooner when BTC moves 0.35% from the last prediction's price.
 *
 * The user's rules: it steers ONLY the candlestick pattern path, and it never closes a trade (nothing below breakeven):
 *   62% or more  -> candlestick trades see a bullish market
 *   38% or less  -> a bearish market
 *   39 to 61     -> no new candlestick trades (BTC in the chop zone around its 1h EMA50)
 *   no prediction under 15 minutes old -> the app's own market mode, as before.
 * The Fibonacci path, open positions and bags are not affected. (The PDF's "tighten an open long's stop to -1.25%" is left
 * out: it would close trades below breakeven.)
 * Calibration: each prediction (one per 15 minutes) is checked 2 hours later: BTC's price then against the 1h EMA50 then.
 *
 * Changes from the PDF's code, both to measure what it means: 200 1h candles instead of 80, so the EMA50 has settled (the
 * app's market mode reads 200 too); the "1h return" is the move over the last 60 minutes (the PDF's code took the move
 * since the current hour began, 0-60 minutes depending on the clock).
 */

const RULES = {
  everyMs: 5 * 60000,         // one prediction per 5m candle
  afterCloseMs: 10000,        // 10 s after the 5m close
  shockPct: 0.35,             // BTC 0.35%+ away from the last prediction's price: a new one early
  shockGapMs: 60000,          // but not more than once a minute
  retryMs: 60000,             // a failed one (no Binance data) is tried again a minute later
  maxAgeMs: 15 * 60000,       // older than this, it no longer steers trades
  probHigh: 62, probLow: 38,  // the PDF's thresholds
  horizonMs: 2 * 3600000,
  logEveryMs: 15 * 60000,     // one prediction per 15 minutes kept for the 2-hour check
  keepLog: 400,               // about 4 days
  dropAfterMs: 14 * 3600000   // one that can't be checked by then (no data) is dropped
};
const num = (x) => { const n = Number(x); return Number.isFinite(n) ? n : NaN; };
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
function calcEMA(values, period) {
  if (!values.length) return [];
  const k = 2 / (period + 1), ema = [values[0]];
  for (let i = 1; i < values.length; i++) ema.push(values[i] * k + ema[i - 1] * (1 - k));
  return ema;
}
// the PDF's RSI: simple averages of the last 14 moves
function calcRSI(closes, period = 14) {
  if (closes.length <= period) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) { const d = closes[i] - closes[i - 1]; if (d >= 0) gains += d; else losses -= d; }
  if (losses === 0) return 100;
  return 100 - 100 / (1 + (gains / period) / (losses / period));
}

/**
 * The PDF's inputs from Binance USD-M public data (oldest first; the last candle may still be open, as in the PDF).
 * d: { k1h (200), k15m (70): klines; prem: premiumIndex; oi: openInterestHist 15m rows (6); taker: takerlongshortRatio 15m
 * rows (4); depth: { bids, asks } (50 levels) }. Returns null without enough candles.
 */
function features(d, now = Date.now()) {
  const k1h = Array.isArray(d && d.k1h) ? d.k1h : [], k15m = Array.isArray(d && d.k15m) ? d.k15m : [];
  if (k1h.length < 60 || k15m.length < 50) return null;
  const c1 = k1h.map((c) => num(c[4])), h1 = k1h.map((c) => num(c[2])), l1 = k1h.map((c) => num(c[3]));
  if (!c1.every((x) => x > 0)) return null;
  const e50 = calcEMA(c1, 50), e20 = calcEMA(c1, 20), n = c1.length - 1, price = c1[n];
  let tr = 0; for (let i = n - 13; i <= n; i++) tr += Math.max(h1[i] - l1[i], Math.abs(h1[i] - c1[i - 1]), Math.abs(l1[i] - c1[i - 1]));
  const c15 = k15m.map((c) => num(c[4])), e20q = calcEMA(c15, 20), e50q = calcEMA(c15, 50);
  // the move over the last 60 minutes: from the close of the 15m candle that closed at least an hour ago
  let ago = NaN; for (let i = k15m.length - 1; i >= 0; i--) if (num(k15m[i][6]) <= now - 3600000) { ago = num(k15m[i][4]); break; }
  if (!(ago > 0)) ago = c1[n - 1];
  const oi = (Array.isArray(d.oi) ? d.oi : []).map((x) => num(x && x.sumOpenInterest)).filter((x) => x > 0);
  const tk = (Array.isArray(d.taker) ? d.taker : []).map((x) => num(x && x.buySellRatio)).filter((x) => x > 0);
  const side = (a) => (Array.isArray(a) ? a : []).reduce((s, [p, q]) => s + (num(p) * num(q) || 0), 0);
  const bid = side(d.depth && d.depth.bids), ask = side(d.depth && d.depth.asks);
  return {
    at: now,
    btcPrice: price,
    ema50_1h: e50[n], ema20_1h: e20[n],
    distFromEma50Pct: ((price - e50[n]) / e50[n]) * 100,
    ret1hPct: ((price - ago) / ago) * 100,
    ema50_3hSlopePct: ((e50[n] - e50[n - 3]) / e50[n - 3]) * 100,
    ema20_2hSlopePct: ((e20[n] - e20[n - 2]) / e20[n - 2]) * 100,
    atr1hPct: (tr / 14 / price) * 100,
    ema20_15m: e20q[e20q.length - 1], ema50_15m: e50q[e50q.length - 1], rsi14_15m: calcRSI(c15, 14),
    fundingRatePct: Number.isFinite(num(d.prem && d.prem.lastFundingRate)) ? num(d.prem.lastFundingRate) * 100 : 0,
    oiChange1hPct: oi.length >= 2 ? ((oi[oi.length - 1] - oi[Math.max(0, oi.length - 5)]) / oi[Math.max(0, oi.length - 5)]) * 100 : 0,
    takerBuySellRatio15m: tk.length ? tk[tk.length - 1] : 1,
    takerBuySellRatio1hAvg: tk.length ? tk.reduce((s, x) => s + x, 0) / tk.length : 1,
    top50BidShare: bid / Math.max(bid + ask, 1)
  };
}

/** The PDF's formula, unchanged (predictEma50FromFeatures). */
function predict(f) {
  const atrPct = Math.max(0.15, Number(f.atr1hPct) || 0.5);
  const sigma2h = 1.13 * atrPct;                       // 2-hour return standard deviation (%)
  const distPct = Number(f.distFromEma50Pct) || 0;
  // 1. 15m micro-trend and RSI
  const ema15SpreadAtr = ((f.ema20_15m - f.ema50_15m) / f.btcPrice * 100) / atrPct;
  const rsi15Norm = ((Number(f.rsi14_15m) || 50) - 50) / 25;
  const ret1hAtr = (Number(f.ret1hPct) || 0) / atrPct;
  const f15m = Math.tanh(0.8 * ema15SpreadAtr + 0.6 * rsi15Norm + 0.6 * ret1hAtr);
  // 2. 1h EMA20 curvature vs the EMA50 slope
  const slopeSpread = (Number(f.ema20_2hSlopePct) || 0) - (Number(f.ema50_3hSlopePct) || 0);
  const fEma = Math.tanh(slopeSpread / (0.35 * atrPct));
  // 3. taker flow and order book
  const tr15 = Math.max(0.2, Number(f.takerBuySellRatio15m) || 1);
  const tr1h = Math.max(0.2, Number(f.takerBuySellRatio1hAvg) || 1);
  const obi = clamp(Number(f.top50BidShare) || 0.5, 0.05, 0.95);
  const fFlow = Math.tanh(2.5 * Math.log(tr15) + 1.5 * Math.log(tr1h) + 1.5 * (2 * obi - 1));
  // 4. open interest: rising confirms the move, falling = covering / liquidation (fades it)
  const r1h = Number(f.ret1hPct) || 0, oi1h = Number(f.oiChange1hPct) || 0;
  const fOi = oi1h >= 0 ? Math.tanh(Math.sign(r1h) * (oi1h / 0.8)) : -Math.tanh(Math.sign(r1h) * (Math.abs(oi1h) / 0.8));
  // 5. funding crowding (contrarian)
  const fundingPct = Number(f.fundingRatePct) || 0;
  const fFund = -Math.tanh((fundingPct - 0.01) / 0.025);
  const sLead = 0.30 * f15m + 0.20 * fEma + 0.25 * fFlow + 0.15 * fOi + 0.10 * fFund;
  const expectedDrift2hPct = 0.65 * sigma2h * sLead;
  const projectedDist2hPct = 0.9231 * distPct + 0.9419 * expectedDrift2hPct;
  const z = projectedDist2hPct / sigma2h;
  const probAbove = clamp(100 / (1 + Math.exp(-1.702 * z)), 1, 99);
  const subScores = [f15m, fEma, fFlow, fOi, fFund];
  const dominantSign = Math.sign(sLead) || 1;
  const alignedCount = subScores.filter((s) => Math.sign(s) === dominantSign && Math.abs(s) > 0.15).length;
  const confidence = Math.round(clamp(45 + Math.abs(sLead) * 35 + alignedCount * 4, 30, 96));
  const currentlyAbove = distPct >= 0;
  let regime = 'CHOP_COMPRESSION', risk = 'LOW';
  if (currentlyAbove && probAbove <= 42) { regime = 'BEARISH_BREAKDOWN_2H'; risk = 'HIGH'; }
  else if (!currentlyAbove && probAbove >= 58) { regime = 'BULLISH_RECLAIM_2H'; risk = 'HIGH'; }
  else if (Math.abs(distPct) < 0.45 * sigma2h && probAbove > 42 && probAbove < 58) { regime = 'CHOP_COMPRESSION'; risk = 'HIGH'; }
  else if (probAbove >= 62) { regime = 'ABOVE_EMA50_TREND'; risk = probAbove < 72 ? 'MEDIUM' : 'LOW'; }
  else if (probAbove <= 38) { regime = 'BELOW_EMA50_TREND'; risk = probAbove > 28 ? 'MEDIUM' : 'LOW'; }
  else risk = 'MEDIUM';
  let mode = 'DEFENSIVE_PAUSE';
  if (probAbove >= 62 && confidence >= 55) mode = 'LONG_ONLY';
  else if (probAbove <= 38 && confidence >= 55) mode = 'SHORT_ONLY';
  const sg = (v, d = 2) => `${v >= 0 ? '+' : ''}${v.toFixed(d)}`;
  return {
    prob: Number(probAbove.toFixed(1)), regime, risk, mode, confidence,
    sLead: Number(sLead.toFixed(3)), projectedDist2hPct: Number(projectedDist2hPct.toFixed(3)), sigma2hPct: Number(sigma2h.toFixed(3)), z: Number(z.toFixed(2)),
    factors: { f15m: Number(f15m.toFixed(2)), fEma: Number(fEma.toFixed(2)), fFlow: Number(fFlow.toFixed(2)), fOi: Number(fOi.toFixed(2)), fFund: Number(fFund.toFixed(2)) },
    drivers: [
      `Gap ${sg(distPct)}% -> projected 2h gap ${sg(projectedDist2hPct)}% (z=${z.toFixed(2)})`,
      `Lead alpha ${sg(sLead)} (15m=${f15m.toFixed(2)}, flow=${fFlow.toFixed(2)}, OI=${fOi.toFixed(2)})`,
      `2h vol radius ±${sigma2h.toFixed(2)}% | taker 15m ${tr15.toFixed(2)} | funding ${fundingPct.toFixed(4)}%`
    ]
  };
}

/** What a prediction means for candlestick trades (the PDF's section 3): BULL, BEAR or PAUSE. */
function decide(p, R = RULES) {
  if (!p) return { act: 'NONE', why: 'no prediction yet' };
  if (p.prob >= R.probHigh) return { act: 'BULL', why: `${p.prob}% chance BTC is above its 1h EMA50 in 2 hours` };
  if (p.prob <= R.probLow) return { act: 'BEAR', why: `only ${p.prob}% chance BTC is above its 1h EMA50 in 2 hours` };
  return { act: 'PAUSE', why: `${p.prob}% chance BTC is above its 1h EMA50 in 2 hours: BTC is in the chop zone around it` };
}
const fresh = (p, now = Date.now(), R = RULES) => Boolean(p && Number(p.at) > 0 && now - p.at <= R.maxAgeMs);
/** The market mode the CANDLESTICK path uses: the app's own mode (own), changed by a fresh prediction. Fibonacci never calls this. */
function applyToMode(own, p, now = Date.now(), R = RULES) {
  const base = own || { mode: 'NEUTRAL', side: 'NONE', reason: 'Reading the market' };
  if (!fresh(p, now, R)) return { ...base, source: 'own' };
  const d = decide(p, R);
  if (d.act === 'BULL') return { ...base, mode: 'BULL', side: 'LONG', shortsOnly: false, source: 'predict', reason: `BTC 2h prediction: ${d.why}` };
  if (d.act === 'BEAR') return { ...base, mode: 'BEAR', side: 'SHORT', shortsOnly: false, source: 'predict', reason: `BTC 2h prediction: ${d.why}` };
  return { ...base, side: 'NONE', pause: true, source: 'predict', reason: `BTC 2h prediction: ${d.why}; no new candlestick trades` };
}

// closed candles only, for the 2-hour check
const closedBars = (k, now) => (Array.isArray(k) ? k : []).filter((b) => Array.isArray(b) && num(b[6]) < now)
  .map((b) => ({ c: num(b[4]), ct: num(b[6]) })).filter((b) => b.c > 0);
/**
 * Checks logged predictions that are 2+ hours old: BTC's 15m close at (or just after) T+2h against the 1h EMA50 of the last
 * 1h candle closed by then. Changes the log in place; returns how many it resolved.
 */
function resolveLog(log, k1h, k15m, now = Date.now(), R = RULES) {
  const h1 = closedBars(k1h, now), m15 = closedBars(k15m, now);
  if (h1.length < 60 || !m15.length) return 0;
  const e50 = calcEMA(h1.map((b) => b.c), 50);
  let done = 0;
  for (let i = log.length - 1; i >= 0; i--) {
    const x = log[i];
    if (x.above2h !== undefined) continue;
    const T = x.at + R.horizonMs;
    if (T > now) continue;
    const q = m15.find((b) => b.ct + 1 >= T && b.ct + 1 - T < 15 * 60000);
    let j = -1; for (let k = h1.length - 1; k >= 0; k--) if (h1[k].ct + 1 <= T) { j = k; break; }
    if (!q || j < 49) { if (now - x.at > R.dropAfterMs) log.splice(i, 1); continue; }
    x.above2h = q.c > e50[j];
    done++;
  }
  return done;
}
/** Hit rate and Brier score of the checked predictions, next to "BTC stays on the side it is on now"; how often it paused. */
function calibration(log, R = RULES) {
  const all = log || [], xs = all.filter((x) => typeof x.above2h === 'boolean');
  const pausedPct = all.length ? Math.round((100 * all.filter((x) => x.prob > R.probLow && x.prob < R.probHigh).length) / all.length) : null;
  if (!xs.length) return { n: 0, pausedPct, logged: all.length };
  const nowAbove = (x) => x.distPct >= 0, right = (x) => (x.prob >= 50) === x.above2h;
  const crosses = xs.filter((x) => nowAbove(x) !== x.above2h);
  const called = xs.filter((x) => x.prob >= R.probHigh || x.prob <= R.probLow);
  return {
    n: xs.length, logged: all.length, pausedPct,
    hitPct: Math.round((100 * xs.filter(right).length) / xs.length),
    stayPct: Math.round((100 * xs.filter((x) => nowAbove(x) === x.above2h).length) / xs.length),
    brier: Number((xs.reduce((a, x) => a + (x.prob / 100 - (x.above2h ? 1 : 0)) ** 2, 0) / xs.length).toFixed(3)),
    crosses: crosses.length, crossesSeen: crosses.filter(right).length,
    called: called.length, calledHitPct: called.length ? Math.round((100 * called.filter(right).length) / called.length) : null
  };
}

/** When the next prediction is due: 10 s after the next 5m close, or a retry a minute after a failure. */
function nextRunAt(lastOkAt, lastTryAt, now = Date.now(), R = RULES) {
  if (!lastOkAt) return lastTryAt ? lastTryAt + R.retryMs : now;
  const due = Math.floor((lastOkAt - R.afterCloseMs) / R.everyMs) * R.everyMs + R.afterCloseMs + R.everyMs;
  return lastTryAt > lastOkAt ? Math.min(due, lastTryAt + R.retryMs) : due;
}

function create({ log = () => {}, R = RULES } = {}) {
  const st = { p: null, log: [], lastOkAt: 0, lastTryAt: 0, lastShockAt: 0, lastError: '', busy: false };
  /**
   * Makes a prediction when one is due (each 5m close, a retry, or BTC 0.35%+ from the last prediction's price).
   * getData: async () => Binance data for features(), or null. opts.price: BTC's price now. Returns true on a new prediction.
   */
  async function tick(getData, now = Date.now(), { price = NaN, force = false } = {}) {
    if (st.busy) return false;
    const shock = st.p && Number.isFinite(num(price)) && st.p.price > 0 && Math.abs((num(price) - st.p.price) / st.p.price) * 100 >= R.shockPct && now - st.lastShockAt >= R.shockGapMs;
    if (!force && !shock && now < nextRunAt(st.lastOkAt, st.lastTryAt, now, R)) return false;
    st.busy = true; st.lastTryAt = now; if (shock) st.lastShockAt = now;
    try {
      const d = await getData();
      if (d) resolveLog(st.log, d.k1h, d.k15m, now, R);
      const f = d ? features(d, now) : null;
      if (!f) { if (!st.lastError) log('BTC 2h prediction: Binance BTC data unavailable; candlestick trades follow the app\'s own mode until it is back.', 'warning'); st.lastError = 'Binance BTC data unavailable'; return false; }
      const out = predict(f);
      st.p = { ...out, at: now, price: f.btcPrice, ema50: Number(f.ema50_1h.toFixed(2)), distPct: Number(f.distFromEma50Pct.toFixed(3)), shock: Boolean(shock) };
      st.lastOkAt = now; st.lastError = '';
      const lastLog = st.log[st.log.length - 1];
      if (!lastLog || Math.floor(lastLog.at / R.logEveryMs) !== Math.floor(now / R.logEveryMs)) {
        st.log.push({ at: now, prob: st.p.prob, distPct: st.p.distPct });
        if (st.log.length > R.keepLog) st.log.splice(0, st.log.length - R.keepLog);
      }
      return true;
    } catch (e) {
      st.lastError = String((e && e.message) || e).slice(0, 200);
      return false;
    } finally { st.busy = false; }
  }
  function load(saved) {
    if (!saved || typeof saved !== 'object') return;
    if (saved.p && Number.isFinite(Number(saved.p.prob))) { st.p = saved.p; st.lastOkAt = Number(saved.p.at) || 0; }
    if (Array.isArray(saved.log)) st.log = saved.log.filter((x) => x && Number(x.at) > 0 && Number.isFinite(Number(x.prob))).slice(-R.keepLog);
  }
  const toJSON = () => ({ p: st.p, log: st.log });
  const status = (now = Date.now()) => ({
    at: st.lastOkAt || null, fresh: fresh(st.p, now, R),
    prediction: st.p ? { prob: st.p.prob, regime: st.p.regime, risk: st.p.risk, mode: st.p.mode, confidence: st.p.confidence, sLead: st.p.sLead,
      projectedDist2hPct: st.p.projectedDist2hPct, sigma2hPct: st.p.sigma2hPct, z: st.p.z, factors: st.p.factors, drivers: st.p.drivers,
      price: st.p.price, ema50: st.p.ema50, distPct: st.p.distPct, shock: st.p.shock } : null,
    effect: decide(fresh(st.p, now, R) ? st.p : null, R),
    nextAt: nextRunAt(st.lastOkAt, st.lastTryAt, now, R), lastError: st.lastError || null,
    calibration: calibration(st.log, R), pending: st.log.filter((x) => x.above2h === undefined).length
  });
  return { tick, load, toJSON, status, prediction: () => st.p, _st: st };
}

module.exports = { create, features, predict, decide, applyToMode, fresh, resolveLog, calibration, nextRunAt, calcRSI, RULES };
