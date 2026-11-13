/**
 * =========================================================================
 * SISIR QUANT - STRATEGY CORE (v5.0)  —  pure functions, no network / no I/O
 * =========================================================================
 * Used by BOTH server.js (live/paper engine) and backtest.js, so whatever you
 * backtest is exactly what the engine trades.
 *
 * WHY v5 EXISTS
 *   v4.x never took a small loss ("never close below +0.35%"). Losers were
 *   parked as "recovery bags" until -3.80%. Winners were capped at ~+0.35..1.8%.
 *   That is +0.4% average win vs -3.8% average loss: you need >91% winners just
 *   to break even before fees, and every time the market moves against you the
 *   only exit left is the hard disaster cut. On top of that the "breakeven lock"
 *   could never fire (see managePosition notes), so even trades that were in
 *   profit rode all the way down into the disaster cut.
 *
 * v5 RULES
 *   - Every position gets a REAL stop the moment it opens (0.6% .. 1.5% away,
 *     based on 15m ATR and the recent swing low). The stop fires whenever price
 *     is at or below it. No window, no "only if still in profit" condition.
 *   - Size is set from the stop: a stop-out loses a fixed fraction of equity.
 *   - Profit is taken in multiples of that risk (R): breakeven at +1R,
 *     half off at +1.5R, trail the rest, full exit at +3R.
 *   - Entries only on CLOSED candles, only in a 1h uptrend, only while BTC is
 *     healthy, never when volatility is too wild for the stop, never chasing.
 *   - Any missing/failed market data => NO TRADE (fail closed).
 * =========================================================================
 */
'use strict';

const CONFIG = {
  feePct: 0.10,              // Binance spot taker fee per side, in %

  // ---- stop placement ----
  minStopPct: 0.60,          // never tighter than this (noise)
  maxStopPct: 1.50,          // setups needing a wider stop are skipped
  atrStopMult: 1.2,          // stop >= 1.2 x ATR(14, 15m)
  stopBufferPct: 0.10,       // extra room below the swing low

  // ---- profit management, in R (1R = entry - initial stop) ----
  breakevenAtR: 1.0,         // at +1R the stop moves to entry + fees
  tp1AtR: 1.5,               // at +1.5R sell tp1Fraction
  tp1Fraction: 0.5,
  lockAfterTp1R: 0.5,        // after TP1 the stop is at least entry + 0.5R
  trailAtrMult: 1.0,         // after TP1 trail 1 x ATR below the peak
  finalTargetR: 3.0,         // close everything at +3R
  staleMinutes: 90,          // if after 90m the trade never reached +0.3R ...
  staleMinR: 0.3,            // ... scratch it
  maxHoldMinutes: 240,       // hard time limit for a position that never hit TP1

  // ---- entry filters ----
  minAtrPct: 0.30,           // too quiet: fees eat the move
  maxAtrPct: 1.20,           // too wild: the stop would sit inside the noise
  maxExtensionPct: 3.0,      // max distance above the 1h EMA20 (no FOMO)
  minRvol: 1.3,              // breakout bar volume vs prior 20 bars
  breakoutLookback: 8,       // 8 x 15m = 2h range
  btcMax15mDropPct: -0.40,   // last closed BTC 15m bar must not be worse than this
  maxChaseR: 0.5,            // skip if live price already ran > 0.5R past signal close

  // ---- portfolio guard ----
  riskPerTradePct: 0.50,     // a full stop-out costs 0.5% of equity
  maxAllocPct: 15,           // never more than 15% of equity in one position
  maxConcurrent: 3,
  minNotional: 10.5,         // Binance min notional is $10
  dailyLossLimitPct: 2.0,    // stop trading for the day after -2% realized
  maxConsecutiveLosses: 3,   // ... or after 3 losses in a row
  lossStreakPauseMin: 120,   // pause length after the loss streak
  symbolCooldownMin: 240     // after a losing trade, leave that coin alone 4h
};

// ---------------------------------------------------------------- helpers
const num = (x) => Number(x);

function closedOnly(klines, now = Date.now()) {
  if (!Array.isArray(klines)) return [];
  // kline[6] = close time. A bar is closed once its close time has passed.
  return klines.filter((k) => Array.isArray(k) && num(k[6]) < now);
}

function toBars(klines) {
  return klines.map((k) => ({
    t: num(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]),
    v: num(k[5]), ct: num(k[6]), tb: num(k[9])
  }));
}

function emaSeries(values, period) {
  if (!values.length) return [];
  const k = 2 / (period + 1);
  const out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i - 1] * (1 - k));
  return out;
}

function atrPct(bars, period = 14) {
  if (bars.length < period + 1) return NaN;
  let sum = 0;
  for (let i = bars.length - period; i < bars.length; i++) {
    const b = bars[i], pc = bars[i - 1].c;
    sum += Math.max(b.h - b.l, Math.abs(b.h - pc), Math.abs(b.l - pc));
  }
  const last = bars[bars.length - 1].c;
  return last > 0 ? (sum / period / last) * 100 : NaN;
}

function vwap(bars) {
  let pv = 0, v = 0;
  bars.forEach((b) => { pv += ((b.h + b.l + b.c) / 3) * b.v; v += b.v; });
  return v > 0 ? pv / v : NaN;
}

const reject = (reason) => ({ ok: false, reason });

// ------------------------------------------------------------- BTC regime
/**
 * BTC must be above its 1h EMA50 and its last closed 15m bar must not be a dump.
 * Altcoin longs opened while BTC is falling are the #1 source of stop-outs.
 */
function evaluateBtcRegime(btc1h, btc15, now = Date.now(), cfg = CONFIG) {
  const h = toBars(closedOnly(btc1h, now));
  const m = toBars(closedOnly(btc15, now));
  if (h.length < 55 || m.length < 2) return { ok: false, reason: 'BTC data unavailable' };
  const closes = h.map((b) => b.c);
  const e50 = emaSeries(closes, 50);
  const e20 = emaSeries(closes, 20);
  const last = closes[closes.length - 1];
  const lastM = m[m.length - 1];
  const chg15 = lastM.o > 0 ? ((lastM.c - lastM.o) / lastM.o) * 100 : 0;
  if (last < e50[e50.length - 1]) {
    return { ok: false, reason: `BTC below 1h EMA50 (downtrend)`, chg15 };
  }
  if (chg15 <= cfg.btcMax15mDropPct) {
    return { ok: false, reason: `BTC 15m bar ${chg15.toFixed(2)}% <= ${cfg.btcMax15mDropPct}%`, chg15 };
  }
  return { ok: true, chg15, btcAboveEma20: last >= e20[e20.length - 1] };
}

// ------------------------------------------------------------ entry check
/**
 * @param {object} p
 *   k15  : 15m klines (>= 60, raw Binance arrays)  REQUIRED
 *   k1h  : 1h klines  (>= 60)                      REQUIRED
 *   k5   : 5m klines  (optional, bearish veto only)
 *   now  : ms timestamp used to decide which bars are closed
 * @returns {ok, reason, pattern, stopPrice, refPrice, stopPct, atrPct, rvol}
 */
function evaluateEntry({ k15, k1h, k5 = null, now = Date.now() }, cfg = CONFIG) {
  const raw15 = closedOnly(k15, now);
  const c15 = toBars(raw15);
  const c1h = toBars(closedOnly(k1h, now));
  if (c15.length < 40) return reject('insufficient 15m history');
  if (c1h.length < 55) return reject('insufficient 1h history');

  // 1) 1h trend: EMA20 > EMA50, price above EMA20, EMA20 rising
  const h = c1h.map((b) => b.c);
  const e20 = emaSeries(h, 20), e50 = emaSeries(h, 50);
  const n = h.length - 1;
  if (!(e20[n] > e50[n])) return reject('1h EMA20 below EMA50 (no uptrend)');
  if (h[n] < e20[n]) return reject('price below 1h EMA20');
  if (!(e20[n] > e20[n - 3])) return reject('1h EMA20 not rising');

  const last = c15[c15.length - 1];
  const ext = ((last.c - e20[n]) / e20[n]) * 100;
  if (ext > cfg.maxExtensionPct) return reject(`overextended +${ext.toFixed(2)}% above 1h EMA20`);

  // 2) volatility band
  const atr = atrPct(c15, 14);
  if (!(atr >= cfg.minAtrPct)) return reject(`ATR ${atr.toFixed(2)}% too low`);
  if (atr > cfg.maxAtrPct) return reject(`ATR ${atr.toFixed(2)}% too high for a safe stop`);

  // 3) trigger on the LAST CLOSED 15m bar
  const prior = c15.slice(-1 - cfg.breakoutLookback, -1);
  const volBase = c15.slice(-21, -1);
  const rangeHigh = Math.max(...prior.map((b) => b.h));
  const avgVol = volBase.reduce((s, b) => s + b.v, 0) / volBase.length;
  const rvol = avgVol > 0 ? last.v / avgVol : 0;
  const range = last.h - last.l;
  const closeLoc = range > 0 ? (last.c - last.l) / range : 0;
  const upperWick = range > 0 ? (last.h - Math.max(last.o, last.c)) / range : 0;
  const vw = vwap(c15.slice(-16));
  if (!(last.c >= vw)) return reject('below 4h VWAP');

  const ema20_15 = emaSeries(c15.map((b) => b.c), 20);
  const pat15 = analyzeCandlestickPattern(raw15);
  if (pat15.isBearishReversal) return reject(`bearish ${pat15.bearishPatternName} (15m)`);
  if (k5) {
    const pat5 = analyzeCandlestickPattern(closedOnly(k5, now));
    if (pat5.isBearishReversal) return reject(`bearish ${pat5.bearishPatternName} (5m)`);
  }

  const isBreakout = last.c > rangeHigh && last.c > last.o && rvol >= cfg.minRvol &&
    closeLoc >= 0.65 && upperWick <= 0.30;
  const isPattern = pat15.isBullishEntry && rvol >= 1.1 &&
    last.c > ema20_15[ema20_15.length - 1] && last.c >= rangeHigh * 0.985;
  if (!isBreakout && !isPattern) return reject('no closed-bar trigger');

  // 4) stop: below the 3-bar swing low, at least 1.2 x ATR, within [min,max]
  const swingLow = Math.min(...c15.slice(-3).map((b) => b.l));
  const structPct = ((last.c - swingLow) / last.c) * 100 + cfg.stopBufferPct;
  const stopPct = Math.max(structPct, atr * cfg.atrStopMult, cfg.minStopPct);
  if (stopPct > cfg.maxStopPct) return reject(`stop ${stopPct.toFixed(2)}% wider than ${cfg.maxStopPct}%`);

  return {
    ok: true,
    pattern: isBreakout ? '2H RANGE BREAKOUT' : pat15.bullishPatternName,
    refPrice: last.c,
    stopPrice: last.c * (1 - stopPct / 100),
    stopPct,
    atrPct: atr,
    rvol: Number(rvol.toFixed(2)),
    signalBarTime: last.t
  };
}

// --------------------------------------------------------- entry planning
/**
 * Turns a signal into an order using the LIVE price.
 * Rejects if price already ran away or if the stop would be out of bounds.
 */
function planEntry(signal, livePrice, equityUSDT, freeCashUSDT, cfg = CONFIG) {
  if (!signal || !signal.ok) return reject('no signal');
  if (!(livePrice > 0)) return reject('no live price');
  const R = signal.refPrice - signal.stopPrice;
  if (livePrice > signal.refPrice + cfg.maxChaseR * R) return reject('price ran away from signal (chasing)');
  if (livePrice <= signal.stopPrice) return reject('price already below stop');

  const stopPct = ((livePrice - signal.stopPrice) / livePrice) * 100;
  if (stopPct < cfg.minStopPct * 0.8 || stopPct > cfg.maxStopPct) return reject(`live stop ${stopPct.toFixed(2)}% out of bounds`);

  const riskUSDT = equityUSDT * (cfg.riskPerTradePct / 100);
  let notional = Math.min(riskUSDT / (stopPct / 100), equityUSDT * (cfg.maxAllocPct / 100), freeCashUSDT);
  if (notional < cfg.minNotional) {
    // Small account: allow the $10.50 minimum only if the risk stays <= 2x plan.
    const minRisk = cfg.minNotional * (stopPct / 100);
    if (freeCashUSDT >= cfg.minNotional && minRisk <= riskUSDT * 2) notional = cfg.minNotional;
    else return reject(`size $${notional.toFixed(2)} below Binance minimum`);
  }
  return {
    ok: true,
    entryPrice: livePrice,
    stopPrice: signal.stopPrice,
    stopPct,
    notional: Number(notional.toFixed(2)),
    qty: notional / livePrice,
    riskUSDT: Number((notional * stopPct / 100).toFixed(4))
  };
}

// ------------------------------------------------------ position manager
/**
 * Called on every price tick. MUTATES pos (sl, peak, stage) and returns what
 * to do: { action: 'HOLD' } | { action: 'PARTIAL', fraction, reason }
 *                            | { action: 'EXIT', reason, isLoss }
 *
 * Required pos fields: entryPriceUSDT, initialStopPrice, sl, openedAt, atrPct
 * Optional: peakHighPrice, tp1Secured, ratchetStage
 *
 * Note on the v4 bug this replaces: v4 only executed the stop when
 * "price <= sl AND price >= entry*1.0035". With sl == entry*1.0035 that window
 * has zero width, so the breakeven lock could never fire and a +1.5% trade
 * could fall to -2.8% (demoted) and then -3.8% (disaster). Here the stop is
 * unconditional.
 */
function managePosition(pos, price, now = Date.now(), cfg = CONFIG) {
  const entry = num(pos.entryPriceUSDT);
  const R = entry - num(pos.initialStopPrice);
  if (!(entry > 0) || !(R > 0) || !(price > 0)) return { action: 'HOLD' };

  pos.peakHighPrice = Math.max(num(pos.peakHighPrice) || entry, price);
  const gainR = (price - entry) / R;
  const peakR = (pos.peakHighPrice - entry) / R;
  pos.peakGainPct = ((pos.peakHighPrice - entry) / entry) * 100;

  // 1) STOP — checked first, always.
  if (price <= num(pos.sl)) {
    const stageName = pos.ratchetStage || 'INITIAL';
    const label = stageName === 'INITIAL' ? 'STOP LOSS' :
      stageName === 'BREAKEVEN' ? 'BREAKEVEN STOP' : 'TRAILING STOP';
    return { action: 'EXIT', reason: label, isLoss: price < entry };
  }

  // 2) Final target
  if (gainR >= cfg.finalTargetR) return { action: 'EXIT', reason: `TARGET ${cfg.finalTargetR}R`, isLoss: false };

  // 3) TP1: sell part, lock profit
  if (!pos.tp1Secured && gainR >= cfg.tp1AtR) {
    pos.tp1Secured = true;
    pos.ratchetStage = 'RUNNER_TRAIL';
    pos.sl = Math.max(num(pos.sl), entry + cfg.lockAfterTp1R * R);
    return { action: 'PARTIAL', fraction: cfg.tp1Fraction, reason: `TP1 ${cfg.tp1AtR}R` };
  }

  // 4) Breakeven once +1R has been seen (covers both fees)
  if (peakR >= cfg.breakevenAtR && (pos.ratchetStage || 'INITIAL') === 'INITIAL') {
    pos.ratchetStage = 'BREAKEVEN';
    pos.sl = Math.max(num(pos.sl), entry * (1 + (2 * cfg.feePct + 0.05) / 100));
  }

  // 5) Trail the runner
  if (pos.tp1Secured) {
    const atr = num(pos.atrPct) || 0.6;
    pos.sl = Math.max(num(pos.sl), pos.peakHighPrice * (1 - (cfg.trailAtrMult * atr) / 100));
  }

  // 6) Time stops — a trade that is not working is closed small, not parked.
  const holdMin = (now - num(pos.openedAt)) / 60000;
  if (!pos.tp1Secured && holdMin >= cfg.staleMinutes && peakR < cfg.staleMinR) {
    return { action: 'EXIT', reason: 'TIME STOP (no follow-through)', isLoss: price < entry };
  }
  if (!pos.tp1Secured && holdMin >= cfg.maxHoldMinutes) {
    return { action: 'EXIT', reason: 'MAX HOLD TIME', isLoss: price < entry };
  }
  return { action: 'HOLD' };
}

/** Net P&L in USDT for selling `qty` units, after fees on both sides. */
function netPnl(qty, entryPrice, exitPrice, cfg = CONFIG) {
  const gross = qty * (exitPrice - entryPrice);
  const fees = (qty * entryPrice + qty * exitPrice) * (cfg.feePct / 100);
  return gross - fees;
}

// --------------------------------------------------------- portfolio guard
/**
 * Circuit breaker. Pass the realized trade list (newest first, each with
 * pnlUSDT, id (ms), pair) and current equity.
 */
function portfolioGuard(trades, equityUSDT, now = Date.now(), cfg = CONFIG, tzOffsetMin = 330) {
  const dayStart = (() => {
    const local = new Date(now + tzOffsetMin * 60000);
    local.setUTCHours(0, 0, 0, 0);
    return local.getTime() - tzOffsetMin * 60000;
  })();
  const real = (trades || []).filter((t) => t && num(t.id) > 0 && !String(t.pair || '').startsWith('TEST'));
  const today = real.filter((t) => num(t.id) >= dayStart);
  const dayPnl = today.reduce((s, t) => s + (num(t.pnlUSDT) || 0), 0);
  if (equityUSDT > 0 && dayPnl <= -(equityUSDT * cfg.dailyLossLimitPct) / 100) {
    return { ok: false, reason: `Daily loss limit hit (${dayPnl.toFixed(2)} USDT). Trading resumes tomorrow (IST).` };
  }
  // consecutive losses among position-closing trades (skip partial TP1 rows)
  const closes = real.filter((t) => !String(t.type || '').includes('TP1')).sort((a, b) => num(b.id) - num(a.id));
  let streak = 0;
  for (const t of closes) { if (num(t.pnlUSDT) < 0) streak++; else break; }
  if (streak >= cfg.maxConsecutiveLosses && closes.length) {
    const resumeAt = num(closes[0].id) + cfg.lossStreakPauseMin * 60000;
    if (now < resumeAt) {
      return { ok: false, reason: `${streak} losses in a row — paused until ${new Date(resumeAt).toISOString().slice(11, 16)} UTC` };
    }
  }
  return { ok: true, dayPnl };
}

/** true if this symbol lost money recently and should be left alone. */
function inSymbolCooldown(trades, pair, now = Date.now(), cfg = CONFIG) {
  const p = String(pair).toUpperCase();
  return (trades || []).some((t) => String(t.pair).toUpperCase() === p && num(t.pnlUSDT) < 0 &&
    now - num(t.id) < cfg.symbolCooldownMin * 60000);
}

// ------------------------------------------------ Nison candlestick analyzer
// (unchanged from v4.3; now always fed CLOSED candles so it cannot repaint)
function analyzeCandlestickPattern(klines) {
  if (!Array.isArray(klines) || klines.length < 5) {
    return { 
      isBearishReversal: false, 
      isBullishReversal: false, 
      isBullishEntry: false, 
      bullishPatternName: 'NONE',
      bearishPatternName: 'NONE',
      patternName: 'NONE', 
      confidence: 0,
      patternRvol: 1.0,
      bodyPct: 0
    };
  }

  const parseBar = (bar) => {
    const open = parseFloat(bar[1]);
    const high = parseFloat(bar[2]);
    const low = parseFloat(bar[3]);
    const close = parseFloat(bar[4]);
    const vol = parseFloat(bar[5]);
    const takerBuy = parseFloat(bar[9]);
    const range = high - low;
    const body = Math.abs(close - open);
    const upperShadow = range > 0 ? (high - Math.max(open, close)) : 0;
    const lowerShadow = range > 0 ? (Math.min(open, close) - low) : 0;
    const isGreen = close >= open;
    const isRed = close < open;
    const takerBuyRatio = vol > 0 ? (takerBuy / vol) * 100 : 50;
    const rangePct = low > 0 ? ((range / low) * 100) : 0;
    const bodyPct = open > 0 ? (body / open) * 100 : 0;

    return { open, high, low, close, vol, takerBuy, range, body, upperShadow, lowerShadow, isGreen, isRed, takerBuyRatio, rangePct, bodyPct };
  };

  const cur = parseBar(klines[klines.length - 1]);
  const prev = parseBar(klines[klines.length - 2]);
  const prev2 = parseBar(klines[klines.length - 3]);
  const prev3 = klines.length >= 4 ? parseBar(klines[klines.length - 4]) : null;
  const prev4 = klines.length >= 5 ? parseBar(klines[klines.length - 5]) : null;

  // Relative Volume calculation relative to prior 8-10 completed bars
  let priorVolSum = 0;
  let priorVolCount = 0;
  for (let i = Math.max(0, klines.length - 12); i < klines.length - 2; i++) {
    priorVolSum += parseFloat(klines[i][5]);
    priorVolCount++;
  }
  const avgPriorVol = priorVolCount > 0 ? (priorVolSum / priorVolCount) : 1.0;
  const prevRvol = avgPriorVol > 0 ? Number((prev.vol / avgPriorVol).toFixed(2)) : 1.0;
  const curRvol = avgPriorVol > 0 ? Number((cur.vol / avgPriorVol).toFixed(2)) : 1.0;

  let isBearishReversal = false;
  let isBullishReversal = false;
  let isBullishEntry = false;
  let bullishPatternName = 'NONE';
  let bearishPatternName = 'NONE';
  let confidence = 0;
  let patternRvol = prevRvol;

  // Real-world continuation check: live candle holding green or shallow pullback
  const isCurConfirmingGreen = (cur.isGreen && cur.upperShadow <= (cur.range * 0.30)) || (cur.close >= (prev.close * 0.9985));

  // ==========================================
  // SECTION A: RECALIBRATED HIGH-EFFICIENCY BULLISH ENTRY SETUPS (INCL. TIER-1)
  // ==========================================

  // 1. BULLISH MARUBOZU (Daiken - Realistic Crypto Tolerances)
  if (prev.isGreen && prev.bodyPct >= 0.50 && prev.range > 0 && prev.body >= (prev.range * 0.72)) {
    if (prev.upperShadow <= (prev.range * 0.20) && prev.lowerShadow <= (prev.range * 0.18) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0) {
      if (isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH MARUBOZU (CONFIRMED)';
        confidence = 94;
        patternRvol = prevRvol;
      }
    }
  } else if (cur.rangePct >= 0.65 && cur.bodyPct >= 0.55 && cur.isGreen && cur.body >= (cur.range * 0.75)) {
    if (cur.upperShadow <= (cur.range * 0.15) && cur.lowerShadow <= (cur.range * 0.15) && curRvol >= 1.25 && cur.takerBuyRatio >= 52.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'BULLISH MARUBOZU (LIVE)';
      confidence = 92;
      patternRvol = curRvol;
    }
  }

  // 2. BULLISH BELT HOLD (Yorikiri)
  if (!isBullishEntry && prev.isGreen && prev.bodyPct >= 0.50 && prev.range > 0 && prev.body >= (prev.range * 0.68)) {
    if (prev.lowerShadow <= (prev.range * 0.10) && prev.upperShadow <= (prev.range * 0.22) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0) {
      if (isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH BELT HOLD (CONFIRMED)';
        confidence = 92;
        patternRvol = prevRvol;
      }
    }
  } else if (!isBullishEntry && cur.rangePct >= 0.60 && cur.bodyPct >= 0.50 && cur.isGreen && cur.body >= (cur.range * 0.70)) {
    if (cur.lowerShadow <= (cur.range * 0.08) && cur.upperShadow <= (cur.range * 0.20) && curRvol >= 1.25 && cur.takerBuyRatio >= 52.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'BULLISH BELT HOLD (LIVE)';
      confidence = 90;
      patternRvol = curRvol;
    }
  }

  // 3. BULLISH ENGULFING (Tsutsumi)
  if (!isBullishEntry && prev2.isRed && prev.isGreen && prev.bodyPct >= 0.45 && prev.range > 0 && prev2.range > 0) {
    if (prev.open <= (prev2.close * 1.001) && prev.close >= (prev2.open * 0.999) && prev.body >= (prev2.body * 1.05)) {
      if (prev.upperShadow <= (prev.range * 0.28) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0 && isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH ENGULFING';
        confidence = 95;
        patternRvol = prevRvol;
      }
    }
  }

  // 4. MORNING STAR (Sankei)
  if (!isBullishEntry && prev2.isRed && prev2.bodyPct >= 0.40) {
    const isStar = prev.body <= (prev.range * 0.40) && prev.low <= (prev2.low * 1.001);
    const isMorningGreen = cur.isGreen && cur.bodyPct >= 0.45 && cur.close >= (prev2.close + (prev2.body * 0.45));
    if (isStar && isMorningGreen && cur.upperShadow <= (cur.range * 0.28) && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'MORNING STAR';
      confidence = 96;
      patternRvol = curRvol;
    }
  }

  // 5. TAKURI HAMMER (Kanazuchi / Takuri)
  if (!isBullishEntry && prev.range > 0 && prev.lowerShadow >= (prev.body * 1.8) && prev.upperShadow <= (prev.range * 0.20) && prev.body <= (prev.range * 0.42)) {
    if (prevRvol >= 1.10 && cur.isGreen && cur.bodyPct >= 0.25 && cur.upperShadow <= (cur.range * 0.28)) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'HAMMER (CONFIRMED)';
      confidence = 92;
      patternRvol = prevRvol;
    }
  }

  // 6. INVERTED HAMMER (Gyaku Kanazuchi - Tier-1 Support Reversal)
  if (!isBullishEntry && prev.range > 0 && prev.upperShadow >= (prev.body * 1.8) && prev.lowerShadow <= (prev.range * 0.18) && prev.body <= (prev.range * 0.42)) {
    if (cur.isGreen && cur.bodyPct >= 0.30 && cur.close >= prev.close && cur.upperShadow <= (cur.range * 0.25)) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'INVERTED HAMMER (SUPPORT)';
      confidence = 93;
      patternRvol = curRvol;
    }
  }

  // 7. TWEEZERS BOTTOM (Kenuki Bottom - Tier-1 Support Double Test)
  if (!isBullishEntry && prev2 && prev && prev2.low > 0) {
    const isTweezerLows = Math.abs(prev.low - prev2.low) <= (prev2.low * 0.0010);
    const hasLowerRejection = (prev.lowerShadow >= prev.body * 0.7 || prev2.lowerShadow >= prev2.body * 0.7);
    if (isTweezerLows && hasLowerRejection && isCurConfirmingGreen) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'TWEEZERS BOTTOM';
      confidence = 94;
      patternRvol = Math.max(prevRvol, curRvol);
    }
  }

  // 8. RISING THREE METHODS (Sanpou - Tier-1 Institutional Continuation Base)
  if (!isBullishEntry && prev4 && prev3 && prev2 && prev && prev4.isGreen && prev4.bodyPct >= 0.45) {
    const isBaseDefended = (prev3.low >= prev4.low * 0.998 && prev3.high <= prev4.high * 1.008) &&
                           (prev2.low >= prev4.low * 0.998 && prev2.high <= prev4.high * 1.008) &&
                           (prev.low >= prev4.low * 0.998 && prev.high <= prev4.high * 1.008);
    const isBreakoutBar = cur.isGreen && cur.close >= prev4.high * 0.999 && cur.bodyPct >= 0.35;
    if (isBaseDefended && isBreakoutBar && curRvol >= 1.20) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'RISING THREE METHODS';
      confidence = 97;
      patternRvol = curRvol;
    }
  }

  // 9. PIERCING LINE (Kirikomi)
  if (!isBullishEntry && prev.isRed && prev.bodyPct >= 0.45 && cur.isGreen && cur.bodyPct >= 0.50 && cur.range > 0) {
    const midpoint = (prev.open + prev.close) / 2;
    if (cur.open <= (prev.low * 1.001) && cur.close >= (midpoint * 0.9995) && cur.close < prev.open && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'PIERCING LINE';
      confidence = 91;
      patternRvol = curRvol;
    }
  }

  // 10. THREE WHITE SOLDIERS (Aka Sansei)
  if (!isBullishEntry && prev2.isGreen && prev.isGreen && cur.isGreen) {
    if (prev2.bodyPct >= 0.35 && prev.bodyPct >= 0.35 && cur.bodyPct >= 0.35) {
      if (cur.close > prev.close && prev.close > prev2.close) {
        if (cur.upperShadow <= (cur.range * 0.25) && prev.upperShadow <= (prev.range * 0.25) && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
          isBullishEntry = true;
          isBullishReversal = true;
          bullishPatternName = 'THREE WHITE SOLDIERS';
          confidence = 96;
          patternRvol = curRvol;
        }
      }
    }
  }

  // 11. DRAGONFLY DOJI (Tonbo Doji)
  if (!isBullishEntry && cur.range > 0 && cur.body <= (cur.range * 0.10) && cur.lowerShadow >= (cur.range * 0.70)) {
    isBullishReversal = true;
    bullishPatternName = 'DRAGONFLY DOJI';
    confidence = 86;
  }

  // ==========================================
  // SECTION B: BEARISH APEX PATTERNS & TIER-1 ANTI-ENTRY VETOES
  // ==========================================

  // 1. TWEEZERS TOP (Kenuki Top - Twin High Rejection Veto)
  if (prev && cur && cur.high > 0) {
    const isTwinHigh = Math.abs(cur.high - prev.high) <= (cur.high * 0.0010);
    const hasUpperRejection = (cur.upperShadow >= cur.body * 0.8 || prev.upperShadow >= prev.body * 0.8);
    if (isTwinHigh && hasUpperRejection && (cur.isRed || cur.upperShadow >= cur.range * 0.28)) {
      isBearishReversal = true;
      bearishPatternName = 'TWEEZERS TOP';
      confidence = 96;
    }
  }

  // 2. HANGING MAN (Kubitsuri - Fragile Bid Warning at Highs)
  if (!isBearishReversal && cur.range > 0 && cur.lowerShadow >= (cur.body * 1.8) && cur.upperShadow <= (cur.range * 0.15) && cur.body <= (cur.range * 0.38)) {
    if (cur.high >= prev.high || cur.close >= prev.high * 0.998) {
      isBearishReversal = true;
      bearishPatternName = 'HANGING MAN';
      confidence = 92;
    }
  }

  // 3. DARK CLOUD COVER (Kabuze - Bearish Penetration Reversal)
  if (!isBearishReversal && prev.isGreen && cur.isRed && cur.open >= (prev.close * 0.999)) {
    const midpoint = (prev.open + prev.close) / 2;
    if (cur.close < midpoint && cur.close > prev.open) {
      isBearishReversal = true;
      bearishPatternName = 'DARK CLOUD COVER';
      confidence = 94;
    }
  }

  // 4. SHOOTING STAR (Nagareboshi)
  if (!isBearishReversal && cur.range > 0 && cur.upperShadow >= (cur.body * 2.0) && cur.lowerShadow <= (cur.range * 0.15) && cur.body <= (cur.range * 0.38)) {
    if (cur.high >= prev.high || cur.close > prev.open) {
      isBearishReversal = true;
      bearishPatternName = 'SHOOTING STAR';
      confidence = cur.takerBuyRatio < 44 ? 95 : 85;
    }
  }

  // 5. BEARISH ENGULFING (Tsutsumi)
  if (!isBearishReversal && prev.isGreen && cur.isRed && cur.range > 0 && prev.range > 0) {
    if (cur.open >= (prev.close * 0.9995) && cur.close <= (prev.open * 1.0005) && cur.body >= prev.body) {
      isBearishReversal = true;
      bearishPatternName = 'BEARISH ENGULFING';
      confidence = cur.takerBuyRatio < 44 ? 92 : 82;
    }
  }

  // 6. EVENING STAR (Sankei)
  if (!isBearishReversal && prev2 && prev2.isGreen && prev2.body >= (prev2.range * 0.40)) {
    const isStar = prev.body <= (prev.range * 0.40) && prev.high >= prev2.high;
    const isEveningRed = cur.isRed && cur.close <= (prev2.open + (prev2.body * 0.50));
    if (isStar && isEveningRed) {
      isBearishReversal = true;
      bearishPatternName = 'EVENING STAR';
      confidence = 94;
    }
  }

  // 7. GRAVESTONE DOJI (Toba Doji)
  if (!isBearishReversal && cur.range > 0 && cur.body <= (cur.range * 0.08) && cur.upperShadow >= (cur.range * 0.72)) {
    isBearishReversal = true;
    bearishPatternName = 'GRAVESTONE DOJI';
    confidence = 88;
  }

  const patternName = bearishPatternName !== 'NONE' 
    ? bearishPatternName 
    : (bullishPatternName !== 'NONE' ? bullishPatternName : 'NONE');

  return { 
    isBearishReversal, 
    isBullishReversal, 
    isBullishEntry, 
    bullishPatternName, 
    bearishPatternName, 
    patternName, 
    confidence, 
    patternRvol,
    bodyPct: cur.bodyPct,
    curBar: cur 
  };
}

module.exports = {
  CONFIG,
  closedOnly,
  toBars,
  emaSeries,
  atrPct,
  vwap,
  evaluateBtcRegime,
  evaluateEntry,
  planEntry,
  managePosition,
  netPnl,
  portfolioGuard,
  inSymbolCooldown,
  analyzeCandlestickPattern
};
