'use strict';
/**
 * v6.23: Fibonacci Retracement Screener & Autopilot Engine.
 * A line-by-line port of `fib_autopilot_engine.py` from the specification "Algorithmic Fibonacci Retracement
 * Screener & Autopilot Engine", Version 2.0 (Multi-Asset Crypto Production Specification).
 *
 * Python / pandas behaviour is copied wherever it decides a value:
 *  - Series.ewm(span | alpha, adjust=False).mean(): pandas' own recursion, its NaN handling (ignore_na=False,
 *    min_periods 1) and its alpha -> centre-of-mass -> alpha round trip;
 *  - Series.rolling(n).mean(): pandas' compensated running sum (min_periods = n); rolling(n).max() / min();
 *  - comparisons with NaN are false; Series.replace(0, nan); DataFrame.max(axis=1) skips NaN;
 *  - Python round(): ties to even, and round(x, n) on the exact binary value;
 *  - dict order (insertion) and list.sort (stable) for ranking.
 * The class keeps the spec's names (snake_case fields) so the two can be read side by side.
 * v6.24: FibConfluenceEngine (below) is the version the app runs: the document's v2 (1h) plus the confluence gate.
 * The spec's engine stays here as it was (tests and replays).
 * Only addition: autopilotStep(..., { allowEntries }) can skip STEP 3, used by the engine to catch up on bars it
 * missed (no entries at old prices) and to warm up the swing memory. With the default it is the spec's step.
 */

// ------------------------------------------------------------------------------------------------
// Python number semantics
// ------------------------------------------------------------------------------------------------
/** Python round(x) for a float: nearest integer, ties to even. */
function pyRoundInt(x) {
  if (!Number.isFinite(x)) return x;
  const f = Math.floor(x), d = x - f;   // exact for doubles
  if (d > 0.5) return f + 1;
  if (d < 0.5) return f;
  return f % 2 === 0 ? f : f + 1;
}

/** Python round(x, n) for n >= 0: correctly rounded on the exact binary value, ties to even. */
function pyRound(x, n) {
  if (!Number.isFinite(x) || x === 0) return x;
  const neg = x < 0, ax = Math.abs(x);
  if (ax >= 1e21) return x;
  const s = ax.toFixed(100);   // the exact decimal expansion of the double (all values used here are >= 1e-14)
  const dot = s.indexOf('.'), ip = s.slice(0, dot), fp = s.slice(dot + 1);
  const keep = fp.slice(0, n), rest = fp.slice(n);
  const digits = ip + keep;
  let up;
  if (rest[0] > '5') up = true;
  else if (rest[0] < '5') up = false;
  else up = /[1-9]/.test(rest.slice(1)) ? true : Number(digits[digits.length - 1]) % 2 === 1;
  let big = BigInt(digits);
  if (up) big += 1n;
  const str = big.toString().padStart(n + 1, '0');
  const out = parseFloat(n > 0 ? `${str.slice(0, str.length - n)}.${str.slice(str.length - n)}` : str);
  return neg ? -out : out;
}

/** Python repr() of a float (used in the spec's ids and reason strings). */
function pyRepr(x) {
  if (Number.isNaN(x)) return 'nan';
  if (x === Infinity) return 'inf';
  if (x === -Infinity) return '-inf';
  const ax = Math.abs(x);
  if (ax !== 0 && (ax < 1e-4 || ax >= 1e16)) {
    const [m, e] = x.toExponential().split('e');
    const ex = Number(e);
    return `${m}e${ex < 0 ? '-' : '+'}${String(Math.abs(ex)).padStart(2, '0')}`;
  }
  if (Number.isInteger(x)) return `${Object.is(x, -0) ? '-0' : String(x)}.0`;
  return String(x);
}

const isNaNum = (v) => v !== v;

// ------------------------------------------------------------------------------------------------
// pandas kernels
// ------------------------------------------------------------------------------------------------
/** Series.ewm(span=... | alpha=..., adjust=False).mean() */
function ewmMean(vals, { span, alpha }) {
  const com = span !== undefined ? (span - 1) / 2.0 : (1.0 - alpha) / alpha;
  const a = 1.0 / (1.0 + com), oldWtFactor = 1.0 - a, newWt = a;
  const n = vals.length, out = new Array(n);
  if (!n) return out;
  let weighted = vals[0], nobs = isNaNum(weighted) ? 0 : 1, oldWt = 1.0;
  out[0] = nobs >= 1 ? weighted : NaN;
  for (let i = 1; i < n; i++) {
    const cur = vals[i], isObs = !isNaNum(cur);
    if (isObs) nobs += 1;
    if (!isNaNum(weighted)) {
      oldWt *= oldWtFactor;   // ignore_na=False: a missing value still ages the old weight
      if (isObs) {
        if (weighted !== cur) {
          weighted = oldWt * weighted + newWt * cur;
          weighted /= (oldWt + newWt);
        }
        oldWt = 1.0;
      }
    } else if (isObs) {
      weighted = cur;
    }
    out[i] = nobs >= 1 ? weighted : NaN;
  }
  return out;
}

/** Series.rolling(w).mean(): pandas roll_mean (Kahan-compensated add/remove, min_periods = w). */
function rollingMean(vals, w) {
  const n = vals.length, out = new Array(n);
  let nobs = 0, sumX = 0, negCt = 0, compAdd = 0, compRem = 0, ncsv = 0, prev = NaN;
  const signbit = (v) => v < 0 || Object.is(v, -0);
  const add = (v) => {
    if (isNaNum(v)) return;
    nobs += 1;
    const y = v - compAdd, t = sumX + y;
    compAdd = t - sumX - y;
    sumX = t;
    if (signbit(v)) negCt += 1;
    if (v === prev) ncsv += 1; else ncsv = 1;
    prev = v;
  };
  const remove = (v) => {
    if (isNaNum(v)) return;
    nobs -= 1;
    const y = -v - compRem, t = sumX + y;
    compRem = t - sumX - y;
    sumX = t;
    if (signbit(v)) negCt -= 1;
  };
  for (let i = 0; i < n; i++) {
    const s = Math.max(0, i - w + 1);
    if (i === 0) {
      prev = vals[s]; ncsv = 0;
      add(vals[0]);
    } else {
      for (let j = Math.max(0, i - w); j < s; j++) remove(vals[j]);
      add(vals[i]);
    }
    let r = NaN;
    if (nobs >= w && nobs > 0) {
      r = sumX / nobs;
      if (ncsv >= nobs) r = prev;
      else if (negCt === 0 && r < 0) r = 0;
      else if (negCt === nobs && r > 0) r = 0;
    }
    out[i] = r;
  }
  return out;
}

/** Series.rolling(w).max() / .min() (min_periods = w) */
function rollingExtreme(vals, w, isMax) {
  const n = vals.length, out = new Array(n).fill(NaN);
  for (let i = w - 1; i < n; i++) {
    let m = vals[i - w + 1], ok = !isNaNum(m);
    for (let j = i - w + 2; j <= i && ok; j++) {
      const v = vals[j];
      if (isNaNum(v)) ok = false;
      else if (isMax ? v > m : v < m) m = v;
    }
    out[i] = ok ? m : NaN;
  }
  return out;
}

const diff = (a) => a.map((v, i) => (i === 0 ? NaN : v - a[i - 1]));
const shift = (a, k) => a.map((_, i) => (i - k >= 0 ? a[i - k] : NaN));
const replaceZero = (a) => a.map((v) => (v === 0 ? NaN : v));

// ------------------------------------------------------------------------------------------------
// precision helpers (round_tick / floor_step)
// ------------------------------------------------------------------------------------------------
function round_tick(price, tick_size) {
  const decimals = tick_size < 1 ? Math.max(0, -pyRoundInt(Math.log10(tick_size))) : 0;
  return pyRound(pyRoundInt(price / tick_size) * tick_size, decimals);
}

function floor_step(qty, step_size) {
  const decimals = step_size < 1 ? Math.max(0, -pyRoundInt(Math.log10(step_size))) : 0;
  return pyRound(Math.floor(qty / step_size) * step_size, decimals);
}

// ------------------------------------------------------------------------------------------------
// data models
// ------------------------------------------------------------------------------------------------
const SymbolSpec = (symbol, o = {}) => ({ symbol, tick_size: 0.0001, step_size: 0.001, min_notional: 10.0, taker_fee: 0.0005, ...o });
const PortfolioState = (o = {}) => ({ equity: 10000.0, free_balance: 10000.0, positions: {}, cooldown_until_bar: {}, bar_counters: {}, ...o });

/** Timeframe presets (spec section 6). The class defaults are the constructor's own defaults. */
const PRESETS = {
  intraday: { swing_lookback: 8, min_swing_atr_mult: 1.8, zone_buffer_atr_mult: 0.35, min_adx: 22.0, min_rr: 1.6, max_setup_bars: 60, cooldown_bars: 8 },
  swing: { swing_lookback: 6, min_swing_atr_mult: 1.6, zone_buffer_atr_mult: 0.30, min_adx: 20.0, min_rr: 1.8, max_setup_bars: 48, cooldown_bars: 6 }
};
const TIMEFRAMES = { '15m': 'intraday', '30m': 'intraday', '1h': 'swing', '4h': 'swing' };
const TF_MS = { '15m': 900000, '30m': 1800000, '1h': 3600000, '4h': 14400000 };

const LEVEL_ORDER = ['0.650', '0.618', '0.786', '0.500', '0.382'];
const LEVEL_WEIGHTS = { '0.650': 35.0, '0.618': 35.0, '0.786': 26.0, '0.500': 22.0, '0.382': 15.0 };

// pandas str(Timestamp) of a UTC bar time, e.g. "2026-11-19 10:15:00+00:00"
const tsString = (ms) => `${new Date(ms).toISOString().slice(0, 19).replace('T', ' ')}+00:00`;

// ------------------------------------------------------------------------------------------------
// FIBONACCI SCREENER + AUTOPILOT ENGINE
// ------------------------------------------------------------------------------------------------
class CryptoFibAutopilotEngine {
  constructor({
    // Indicator & Swing Parameters
    swing_lookback = 6, ema_period = 200, rsi_period = 14, atr_period = 14, adx_period = 14,
    // Screener Filter Thresholds
    min_adx = 20.0, min_atr_pct = 0.004, min_24h_quote_vol = 10_000_000, min_swing_atr_mult = 1.6,
    zone_buffer_atr_mult = 0.30, min_rr = 1.6, max_setup_bars = 48,
    // Autopilot Risk & Portfolio Limits
    risk_per_trade = 0.01, max_portfolio_risk = 0.04, max_open_positions = 4, max_same_direction = 2,
    leverage = 1.0, cooldown_bars = 6
  } = {}) {
    this.N = swing_lookback;
    this.ema_period = ema_period;
    this.rsi_period = rsi_period;
    this.atr_period = atr_period;
    this.adx_period = adx_period;

    this.min_adx = min_adx;
    this.min_atr_pct = min_atr_pct;
    this.min_24h_quote_vol = min_24h_quote_vol;
    this.min_swing_atr_mult = min_swing_atr_mult;
    this.zone_buffer_atr_mult = zone_buffer_atr_mult;
    this.min_rr = min_rr;
    this.max_setup_bars = max_setup_bars;

    this.risk_per_trade = risk_per_trade;
    this.max_portfolio_risk = max_portfolio_risk;
    this.max_open_positions = max_open_positions;
    this.max_same_direction = max_same_direction;
    this.leverage = leverage;
    this.cooldown_bars = cooldown_bars;

    // Persistent per-symbol setup memory
    this._pivots = {};
    this._active_setups = {};
  }

  /** The swing memory, to save and restore (the spec keeps it in the process). */
  memory() { return { pivots: this._pivots, setups: this._active_setups }; }
  restore(mem) {
    this._pivots = (mem && mem.pivots) || {};
    this._active_setups = (mem && mem.setups) || {};
  }

  // --------------------------------------------------------------------------
  // 1. INDICATOR COMPUTATION   (bars: [{ t, open, high, low, close, volume }], oldest first)
  // --------------------------------------------------------------------------
  compute_indicators(bars) {
    const open = bars.map((b) => b.open), high = bars.map((b) => b.high), low = bars.map((b) => b.low);
    const close = bars.map((b) => b.close), volume = bars.map((b) => b.volume);
    const n = bars.length;

    // EMA & Slope
    const ema = ewmMean(close, { span: this.ema_period });
    const ema_slope = diff(ema);

    // True Range & Wilder's ATR
    const prevClose = shift(close, 1);
    const tr = new Array(n);
    for (let i = 0; i < n; i++) {
      const hl = high[i] - low[i], hc = Math.abs(high[i] - prevClose[i]), lc = Math.abs(low[i] - prevClose[i]);
      const xs = [hl, hc, lc].filter((v) => !isNaNum(v));   // max(axis=1) skips NaN
      tr[i] = xs.length ? Math.max(...xs) : NaN;
    }
    const atr = ewmMean(tr, { alpha: 1 / this.atr_period });

    // Wilder's RSI
    const delta = diff(close);
    const gain = delta.map((d) => (isNaNum(d) ? NaN : d < 0 ? 0 : d));        // clip(lower=0)
    const lossNeg = delta.map((d) => (isNaNum(d) ? NaN : d > 0 ? 0 : d));     // clip(upper=0)
    const gainE = ewmMean(gain, { alpha: 1 / this.rsi_period });
    const lossE = ewmMean(lossNeg.map((v) => -v), { alpha: 1 / this.rsi_period });
    const lossR = replaceZero(lossE);
    const rsi = gainE.map((g, i) => 100 - (100 / (1 + (g / lossR[i]))));

    // Wilder's ADX (Trend Strength)
    const up_move = diff(high);
    const down_move = diff(low).map((v) => -v);
    const plus_dm = up_move.map((u, i) => ((u > down_move[i]) && (u > 0) ? u : 0.0));
    const minus_dm = down_move.map((d, i) => ((d > up_move[i]) && (d > 0) ? d : 0.0));
    const atr_safe = replaceZero(atr);
    const pdmE = ewmMean(plus_dm, { alpha: 1 / this.adx_period }), mdmE = ewmMean(minus_dm, { alpha: 1 / this.adx_period });
    const plus_di = pdmE.map((v, i) => 100 * (v / atr_safe[i]));
    const minus_di = mdmE.map((v, i) => 100 * (v / atr_safe[i]));
    const sumDi = replaceZero(plus_di.map((v, i) => v + minus_di[i]));
    const dx = plus_di.map((v, i) => 100 * (Math.abs(v - minus_di[i]) / sumDi[i]));
    const adx = ewmMean(dx, { alpha: 1 / this.adx_period });

    // Volume SMA
    const vol_sma = rollingMean(volume, 20);

    // Causal Fractal Pivots (confirmed N bars after occurrence)
    const win = 2 * this.N + 1;
    const hiN = shift(high, this.N), loN = shift(low, this.N);
    const rmax = rollingExtreme(high, win, true), rmin = rollingExtreme(low, win, false);
    const conf_high = hiN.map((v, i) => (v === rmax[i] ? v : NaN));
    const conf_low = loN.map((v, i) => (v === rmin[i] ? v : NaN));

    return { open, high, low, close, volume, ema, ema_slope, atr, rsi, adx, vol_sma, conf_high, conf_low, n };
  }

  // --------------------------------------------------------------------------
  // 2. PIVOT & FIBONACCI CONSTRUCTION
  // --------------------------------------------------------------------------
  _update_symbol_pivots(symbol, row, bar_idx) {
    if (!this._pivots[symbol]) this._pivots[symbol] = [];
    const pivots = this._pivots[symbol];
    let updated = false;

    const candidates = [];
    if (!isNaNum(row.conf_high)) candidates.push({ kind: 'HIGH', bar_idx: bar_idx - this.N, price: Number(row.conf_high) });
    if (!isNaNum(row.conf_low)) candidates.push({ kind: 'LOW', bar_idx: bar_idx - this.N, price: Number(row.conf_low) });

    if (candidates.length === 2 && row.close >= row.open) candidates.reverse();

    for (const cand of candidates) {
      if (!pivots.length) {
        pivots.push(cand);
        updated = true;
        continue;
      }
      const last = pivots[pivots.length - 1];
      if (last.kind === cand.kind) {
        if ((cand.kind === 'HIGH' && cand.price > last.price) || (cand.kind === 'LOW' && cand.price < last.price)) {
          pivots[pivots.length - 1] = cand;
          updated = true;
        }
      } else {
        pivots.push(cand);
        updated = true;
      }
    }

    // Keep memory bounded
    if (pivots.length > 20) this._pivots[symbol] = pivots.slice(-20);

    return updated;
  }

  static _build_fib_setup(symbol, swing_low, swing_high, direction, bar_idx) {
    const d = swing_high - swing_low;
    const ratios = [0.236, 0.382, 0.500, 0.618, 0.650, 0.786];
    const ext_ratios = [1.272, 1.618];
    const retracements = {}, extensions = {};
    if (direction === 'BULLISH') {
      for (const r of ratios) retracements[r.toFixed(3)] = swing_high - d * r;
      for (const e of ext_ratios) extensions[e.toFixed(3)] = swing_low + d * e;
    } else {
      for (const r of ratios) retracements[r.toFixed(3)] = swing_low + d * r;
      for (const e of ext_ratios) extensions[e.toFixed(3)] = swing_high - d * e;
    }
    const setup_id = `${symbol}_${direction}_${bar_idx}_${pyRepr(pyRound(swing_low, 4))}_${pyRepr(pyRound(swing_high, 4))}`;
    return { setup_id, symbol, direction, swing_low, swing_high, created_bar: bar_idx, retracements, extensions, active: true, traded: false };
  }

  // --------------------------------------------------------------------------
  // 3. SCREENER: EVALUATE & RANK COINS
  // --------------------------------------------------------------------------
  /** Evaluates the latest closed bar for one coin and returns a ranked ScreenerSignal if triggered. */
  screen_coin(spec, bars, state, quote_volume_24h, btc_regime = null) {
    if (bars.length <= Math.max(this.ema_period, 2 * this.N + 2)) return null;

    // Liquidity filter
    if (quote_volume_24h < this.min_24h_quote_vol) return null;

    const data = this.compute_indicators(bars);
    const i = data.n - 1;
    const row = { open: data.open[i], high: data.high[i], low: data.low[i], close: data.close[i], volume: data.volume[i], ema: data.ema[i], ema_slope: data.ema_slope[i],
      atr: data.atr[i], rsi: data.rsi[i], adx: data.adx[i], vol_sma: data.vol_sma[i], conf_high: data.conf_high[i], conf_low: data.conf_low[i] };
    const prev_rsi = data.rsi[i - 1];
    const ts = tsString(bars[bars.length - 1].t);   // data.index[-1]

    const bar_idx = (state.bar_counters[spec.symbol] || 0) + 1;
    state.bar_counters[spec.symbol] = bar_idx;

    const atr = Number(row.atr), adx = Number(row.adx);
    const close_p = Number(row.close), open_p = Number(row.open), high_p = Number(row.high), low_p = Number(row.low);
    const candle_range = high_p - low_p;

    if (isNaNum(atr) || isNaNum(adx) || close_p <= 0 || candle_range <= 0) return null;

    // Update pivots & Fibonacci structure
    const pivot_updated = this._update_symbol_pivots(spec.symbol, row, bar_idx);
    const pivots = this._pivots[spec.symbol] || [];

    if (pivot_updated && pivots.length >= 2) {
      const p_prev = pivots[pivots.length - 2], p_last = pivots[pivots.length - 1];
      const swing_range = Math.abs(p_last.price - p_prev.price);
      if (swing_range >= this.min_swing_atr_mult * atr) {
        if (p_prev.kind === 'LOW' && p_last.kind === 'HIGH' && close_p > row.ema && row.ema_slope > 0) {
          this._active_setups[spec.symbol] = CryptoFibAutopilotEngine._build_fib_setup(spec.symbol, p_prev.price, p_last.price, 'BULLISH', bar_idx);
        } else if (p_prev.kind === 'HIGH' && p_last.kind === 'LOW' && close_p < row.ema && row.ema_slope < 0) {
          this._active_setups[spec.symbol] = CryptoFibAutopilotEngine._build_fib_setup(spec.symbol, p_last.price, p_prev.price, 'BEARISH', bar_idx);
        }
      }
    }

    const setup = this._active_setups[spec.symbol];
    if (!setup || !setup.active || setup.traded) return null;

    // Expiration & cooldown check
    if ((bar_idx - setup.created_bar) > this.max_setup_bars) {
      setup.active = false;
      return null;
    }
    if (bar_idx < (state.cooldown_until_bar[spec.symbol] || 0)) return null;

    // Volatility & Trend strength filters
    if ((atr / close_p) < this.min_atr_pct || adx < this.min_adx) return null;

    const buffer = this.zone_buffer_atr_mult * atr;
    const vol_ratio = row.vol_sma > 0 ? Number(row.volume / row.vol_sma) : 1.0;

    // --- BULLISH SCREENER CHECK ---
    if (setup.direction === 'BULLISH') {
      if (btc_regime === 'BEARISH') return null;
      if (close_p < setup.swing_low - buffer || close_p > setup.swing_high + buffer) {
        setup.active = false;
        return null;
      }
      for (const lvl_key of LEVEL_ORDER) {
        const lvl = setup.retracements[lvl_key];
        const touched = low_p <= (lvl + buffer) && close_p >= (lvl - buffer);
        const rejection = close_p > open_p && ((close_p - low_p) / candle_range) >= 0.5;
        const rsi_turn = row.rsi < 55 && row.rsi > prev_rsi;
        if (touched && rejection && rsi_turn) {
          const sl_anchor = lvl_key === '0.786' ? setup.swing_low : setup.retracements['0.786'];
          const sl = round_tick(sl_anchor - 0.5 * atr, spec.tick_size);
          const tp1 = round_tick(setup.swing_high, spec.tick_size);
          const tp2 = round_tick(setup.extensions['1.272'], spec.tick_size);
          const entry = round_tick(close_p, spec.tick_size);

          const fee_cost = entry * spec.taker_fee * 2;
          const risk = (entry - sl) + fee_cost;
          const reward = (tp1 - entry) - fee_cost;
          if (risk <= 0 || (reward / risk) < this.min_rr) continue;

          const rr = reward / risk;
          const score = CryptoFibAutopilotEngine._compute_confluence_score(LEVEL_WEIGHTS[lvl_key], rr, vol_ratio, adx);
          return { symbol: spec.symbol, timestamp: ts, side: 'LONG', entry_price: entry, stop_loss: sl, take_profit_1: tp1, take_profit_2: tp2,
            fib_level: lvl_key, rr_ratio: pyRound(rr, 2), confluence_score: score, setup_id: setup.setup_id };
        }
      }
    // --- BEARISH SCREENER CHECK ---
    } else if (setup.direction === 'BEARISH') {
      if (btc_regime === 'BULLISH') return null;
      if (close_p > setup.swing_high + buffer || close_p < setup.swing_low - buffer) {
        setup.active = false;
        return null;
      }
      for (const lvl_key of LEVEL_ORDER) {
        const lvl = setup.retracements[lvl_key];
        const touched = high_p >= (lvl - buffer) && close_p <= (lvl + buffer);
        const rejection = close_p < open_p && ((high_p - close_p) / candle_range) >= 0.5;
        const rsi_turn = row.rsi > 45 && row.rsi < prev_rsi;
        if (touched && rejection && rsi_turn) {
          const sl_anchor = lvl_key === '0.786' ? setup.swing_high : setup.retracements['0.786'];
          const sl = round_tick(sl_anchor + 0.5 * atr, spec.tick_size);
          const tp1 = round_tick(setup.swing_low, spec.tick_size);
          const tp2 = round_tick(setup.extensions['1.272'], spec.tick_size);
          const entry = round_tick(close_p, spec.tick_size);

          const fee_cost = entry * spec.taker_fee * 2;
          const risk = (sl - entry) + fee_cost;
          const reward = (entry - tp1) - fee_cost;
          if (risk <= 0 || (reward / risk) < this.min_rr) continue;

          const rr = reward / risk;
          const score = CryptoFibAutopilotEngine._compute_confluence_score(LEVEL_WEIGHTS[lvl_key], rr, vol_ratio, adx);
          return { symbol: spec.symbol, timestamp: ts, side: 'SHORT', entry_price: entry, stop_loss: sl, take_profit_1: tp1, take_profit_2: tp2,
            fib_level: lvl_key, rr_ratio: pyRound(rr, 2), confluence_score: score, setup_id: setup.setup_id };
        }
      }
    }
    return null;
  }

  static _compute_confluence_score(level_pts, rr, vol_ratio, adx) {
    const rr_pts = Math.min(25.0, Math.max(0.0, (rr - 1.0) * 12.5));
    const vol_pts = Math.min(20.0, Math.max(0.0, (vol_ratio - 0.8) * 15.0));
    const adx_pts = Math.min(20.0, Math.max(0.0, (adx - 15.0) * 0.8));
    return pyRound(level_pts + rr_pts + vol_pts + adx_pts, 2);
  }

  // --------------------------------------------------------------------------
  // 4. AUTOPILOT EXECUTION CYCLE (MANAGE POSITIONS + OPEN TOP RANKED SIGNALS)
  // --------------------------------------------------------------------------
  /**
   * universe: Map(symbol -> { spec, bars, quote_volume_24h }) in the caller's order (the spec's dict order).
   * Runs on every closed bar across all screened coins:
   *   1. Manages Stop-Loss, TP1 (+ breakeven SL update), and TP2 on open positions.
   *   2. Scans all coins and sorts triggers by Confluence Score.
   *   3. Opens new positions within portfolio risk, direction, and balance caps (skipped with allowEntries: false).
   * Returns { orders, candidates } (candidates: the ranked screener signals of this bar, for the log).
   */
  autopilot_step(universe, state, btc_regime = null, { allowEntries = true } = {}) {
    const orders = [];

    // --- STEP 1: MANAGE EXISTING OPEN POSITIONS ---
    for (const symbol of Object.keys(state.positions)) {
      if (!universe.has(symbol)) continue;
      const pos = state.positions[symbol];
      const { spec, bars } = universe.get(symbol);
      const last_bar = bars[bars.length - 1];
      const high_p = Number(last_bar.high), low_p = Number(last_bar.low);
      const bar_idx = state.bar_counters[symbol] || 0;

      if (pos.side === 'LONG') {
        if (low_p <= pos.stop_loss) {
          const pnl = (pos.stop_loss - pos.entry_price) * pos.remaining_size;
          const margin_released = (pos.entry_price * pos.remaining_size) / this.leverage;
          state.equity += pnl;
          state.free_balance += margin_released + pnl;
          state.cooldown_until_bar[symbol] = bar_idx + this.cooldown_bars;
          delete state.positions[symbol];
          orders.push({ client_order_id: `${pos.position_id}_SL`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size,
            price: pos.stop_loss, stop_loss: null, take_profit_1: null, take_profit_2: null, reason: 'STOP_LOSS_HIT', realized_pnl: pyRound(pnl, 2) });
          continue;
        }
        if (!pos.tp1_hit && high_p >= pos.take_profit_1) {
          const close_qty = floor_step(pos.initial_size * 0.5, spec.step_size);
          if (close_qty > 0 && (pos.remaining_size - close_qty) >= spec.step_size) {
            const pnl = (pos.take_profit_1 - pos.entry_price) * close_qty;
            const margin_released = (pos.entry_price * close_qty) / this.leverage;
            state.equity += pnl;
            state.free_balance += margin_released + pnl;
            pos.remaining_size = floor_step(pos.remaining_size - close_qty, spec.step_size);
            pos.tp1_hit = true;
            pos.stop_loss = pos.entry_price;
            pos.risk_amount = 0.0;   // Risk-free after breakeven
            orders.push({ client_order_id: `${pos.position_id}_TP1`, symbol, action: 'CLOSE_PARTIAL', order_type: 'MARKET', quantity: close_qty,
              price: pos.take_profit_1, stop_loss: pos.stop_loss, take_profit_1: null, take_profit_2: null, reason: 'TP1_PARTIAL_AND_BREAKEVEN', realized_pnl: pyRound(pnl, 2) });
          }
        }
        if (pos.tp1_hit && high_p >= pos.take_profit_2) {
          const pnl = (pos.take_profit_2 - pos.entry_price) * pos.remaining_size;
          const margin_released = (pos.entry_price * pos.remaining_size) / this.leverage;
          state.equity += pnl;
          state.free_balance += margin_released + pnl;
          delete state.positions[symbol];
          orders.push({ client_order_id: `${pos.position_id}_TP2`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size,
            price: pos.take_profit_2, stop_loss: null, take_profit_1: null, take_profit_2: null, reason: 'TP2_FINAL_TARGET', realized_pnl: pyRound(pnl, 2) });
        }
      } else if (pos.side === 'SHORT') {
        if (high_p >= pos.stop_loss) {
          const pnl = (pos.entry_price - pos.stop_loss) * pos.remaining_size;
          const margin_released = (pos.entry_price * pos.remaining_size) / this.leverage;
          state.equity += pnl;
          state.free_balance += margin_released + pnl;
          state.cooldown_until_bar[symbol] = bar_idx + this.cooldown_bars;
          delete state.positions[symbol];
          orders.push({ client_order_id: `${pos.position_id}_SL`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size,
            price: pos.stop_loss, stop_loss: null, take_profit_1: null, take_profit_2: null, reason: 'STOP_LOSS_HIT', realized_pnl: pyRound(pnl, 2) });
          continue;
        }
        if (!pos.tp1_hit && low_p <= pos.take_profit_1) {
          const close_qty = floor_step(pos.initial_size * 0.5, spec.step_size);
          if (close_qty > 0 && (pos.remaining_size - close_qty) >= spec.step_size) {
            const pnl = (pos.entry_price - pos.take_profit_1) * close_qty;
            const margin_released = (pos.entry_price * close_qty) / this.leverage;
            state.equity += pnl;
            state.free_balance += margin_released + pnl;
            pos.remaining_size = floor_step(pos.remaining_size - close_qty, spec.step_size);
            pos.tp1_hit = true;
            pos.stop_loss = pos.entry_price;
            pos.risk_amount = 0.0;
            orders.push({ client_order_id: `${pos.position_id}_TP1`, symbol, action: 'CLOSE_PARTIAL', order_type: 'MARKET', quantity: close_qty,
              price: pos.take_profit_1, stop_loss: pos.stop_loss, take_profit_1: null, take_profit_2: null, reason: 'TP1_PARTIAL_AND_BREAKEVEN', realized_pnl: pyRound(pnl, 2) });
          }
        }
        if (pos.tp1_hit && low_p <= pos.take_profit_2) {
          const pnl = (pos.entry_price - pos.take_profit_2) * pos.remaining_size;
          const margin_released = (pos.entry_price * pos.remaining_size) / this.leverage;
          state.equity += pnl;
          state.free_balance += margin_released + pnl;
          delete state.positions[symbol];
          orders.push({ client_order_id: `${pos.position_id}_TP2`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size,
            price: pos.take_profit_2, stop_loss: null, take_profit_1: null, take_profit_2: null, reason: 'TP2_FINAL_TARGET', realized_pnl: pyRound(pnl, 2) });
        }
      }
    }

    // --- STEP 2: SCREEN ALL COINS & RANK CANDIDATES ---
    const candidates = [];
    for (const [symbol, u] of universe) {
      if (symbol in state.positions) continue;
      const sig = this.screen_coin(u.spec, u.bars, state, u.quote_volume_24h, btc_regime);
      if (sig !== null) candidates.push(sig);
    }
    candidates.sort((a, b) => b.confluence_score - a.confluence_score);   // stable, like list.sort(reverse=True)

    if (!allowEntries) return { orders, candidates };

    // --- STEP 3: AUTOPILOT ALLOCATION & ORDER GENERATION ---
    for (const sig of candidates) {
      if (Object.keys(state.positions).length >= this.max_open_positions) break;

      const same_dir_count = Object.values(state.positions).filter((p) => p.side === sig.side).length;
      if (same_dir_count >= this.max_same_direction) continue;

      const current_open_risk = Object.values(state.positions).reduce((a, p) => a + p.risk_amount, 0);
      const max_allowed_risk = state.equity * this.max_portfolio_risk;
      const trade_risk_budget = Math.min(state.equity * this.risk_per_trade, max_allowed_risk - current_open_risk);
      if (trade_risk_budget <= 0) break;

      const spec = universe.get(sig.symbol).spec;
      const price_risk = Math.abs(sig.entry_price - sig.stop_loss);
      if (price_risk <= 0) continue;

      const raw_qty = trade_risk_budget / price_risk;
      const max_affordable_qty = (state.free_balance * this.leverage * 0.98) / sig.entry_price;
      const qty = floor_step(Math.min(raw_qty, max_affordable_qty), spec.step_size);

      const notional = qty * sig.entry_price;
      const required_margin = notional / this.leverage;
      if (qty <= 0 || notional < spec.min_notional || required_margin > state.free_balance) continue;

      const pos_id = `POS_${sig.setup_id}`;
      const actual_risk = qty * price_risk;
      state.free_balance -= required_margin;
      state.positions[sig.symbol] = { position_id: pos_id, symbol: sig.symbol, side: sig.side, entry_time: String(sig.timestamp), entry_price: sig.entry_price,
        stop_loss: sig.stop_loss, take_profit_1: sig.take_profit_1, take_profit_2: sig.take_profit_2, initial_size: qty, remaining_size: qty,
        fib_level: sig.fib_level, risk_amount: actual_risk, tp1_hit: false };

      if (sig.symbol in this._active_setups) this._active_setups[sig.symbol].traded = true;

      orders.push({ client_order_id: `${pos_id}_ENTRY`, symbol: sig.symbol, action: `OPEN_${sig.side}`, order_type: 'MARKET', quantity: qty, price: sig.entry_price,
        stop_loss: sig.stop_loss, take_profit_1: sig.take_profit_1, take_profit_2: sig.take_profit_2,
        reason: `FIB_${sig.fib_level}_SCORE_${pyRepr(sig.confluence_score)}_RR_${pyRepr(sig.rr_ratio)}`, realized_pnl: 0.0 });
    }

    return { orders, candidates };
  }
}

// ------------------------------------------------------------------------------------------------
// v6.24: FIBONACCI v2 + CONFLUENCE (the best Fibonacci version in the replays, 1h)
// v2 = the "1h structural" engine from the user's second document (fib_v2_engine.py), ported line by line with the
// same kernels as above: swings of 10 bars each side and at least 3 ATR, in an EMA50 / EMA200 trend; the entry needs a
// touch of 0.650 / 0.618 / 0.786, a structural reversal candle (beyond the last bar's high / low, closing in its outer
// 45%) and RSI turning from below 45 / above 55; stop beyond the swing (at least 1.2%), 35% at TP1 (the swing high /
// low, then the stop to entry + fees), the rest at the 1.272 extension; 1% risk a trade, 3% in all, 3 trades, 2 a side, 2x.
// Confluence (from web research, 21 Nov): the 0.618-0.786 zone, widened by the buffer, must hold the swing point before
// the impulse (old resistance = new support). With { confluence: false } the class is the document's v2 exactly.
// ------------------------------------------------------------------------------------------------
const pyMin2 = (a, b) => (b < a ? b : a);   // Python min(a, b): the first argument unless b is smaller (NaN-safe)
const pyMax2 = (a, b) => (b > a ? b : a);
const diffN = (a, k) => a.map((v, i) => (i - k >= 0 ? v - a[i - k] : NaN));

class FibConfluenceEngine {
  constructor({
    swing_lookback = 10, ema_fast_period = 50, ema_slow_period = 200, rsi_period = 14, atr_period = 14, adx_period = 14,
    min_adx = 22.0, min_atr_pct = 0.006, min_24h_quote_vol = 15_000_000, min_swing_atr_mult = 3.0, zone_buffer_atr_mult = 0.25,
    min_stop_pct = 0.012, min_rr = 1.8, max_setup_bars = 36,
    risk_per_trade = 0.01, max_portfolio_risk = 0.03, max_notional_pct = 0.50, tp1_close_ratio = 0.35,
    max_open_positions = 3, max_same_direction = 2, leverage = 2.0, cooldown_bars = 6, confluence = true
  } = {}) {
    Object.assign(this, { N: swing_lookback, ema_fast_period, ema_slow_period, rsi_period, atr_period, adx_period,
      min_adx, min_atr_pct, min_24h_quote_vol, min_swing_atr_mult, zone_buffer_atr_mult, min_stop_pct, min_rr, max_setup_bars,
      risk_per_trade, max_portfolio_risk, max_notional_pct, tp1_close_ratio, max_open_positions, max_same_direction, leverage, cooldown_bars, confluence });
    this._pivots = {};
    this._active_setups = {};
  }

  /** The settings, for the app's status line. */
  params() {
    return { swing_lookback: this.N, min_adx: this.min_adx, min_atr_pct: this.min_atr_pct, min_24h_quote_vol: this.min_24h_quote_vol, min_swing_atr_mult: this.min_swing_atr_mult,
      zone_buffer_atr_mult: this.zone_buffer_atr_mult, min_stop_pct: this.min_stop_pct, min_rr: this.min_rr, max_setup_bars: this.max_setup_bars,
      risk_per_trade: this.risk_per_trade, max_portfolio_risk: this.max_portfolio_risk, max_notional_pct: this.max_notional_pct, tp1_close_ratio: this.tp1_close_ratio,
      max_open_positions: this.max_open_positions, max_same_direction: this.max_same_direction, leverage: this.leverage, cooldown_bars: this.cooldown_bars, confluence: this.confluence };
  }

  /** The swing memory, to save and restore. */
  memory() { return { pivots: this._pivots, setups: this._active_setups }; }
  restore(mem) {
    this._pivots = (mem && mem.pivots) || {};
    this._active_setups = (mem && mem.setups) || {};
  }

  compute_indicators(bars) {
    const open = bars.map((b) => b.open), high = bars.map((b) => b.high), low = bars.map((b) => b.low);
    const close = bars.map((b) => b.close), volume = bars.map((b) => b.volume), n = bars.length;
    const ema_fast = ewmMean(close, { span: this.ema_fast_period });
    const ema_slow = ewmMean(close, { span: this.ema_slow_period });
    const ema_slow_slope = diffN(ema_slow, 3);
    const prevClose = shift(close, 1), tr = new Array(n);
    for (let i = 0; i < n; i++) {
      const xs = [high[i] - low[i], Math.abs(high[i] - prevClose[i]), Math.abs(low[i] - prevClose[i])].filter((v) => !isNaNum(v));
      tr[i] = xs.length ? Math.max(...xs) : NaN;
    }
    const atr = ewmMean(tr, { alpha: 1 / this.atr_period });
    const delta = diffN(close, 1);
    const gainE = ewmMean(delta.map((d) => (isNaNum(d) ? NaN : d < 0 ? 0 : d)), { alpha: 1 / this.rsi_period });
    const lossE = ewmMean(delta.map((d) => (isNaNum(d) ? NaN : d > 0 ? 0 : d)).map((v) => -v), { alpha: 1 / this.rsi_period });
    const lossR = replaceZero(lossE);
    const rsi = gainE.map((g, i) => 100 - (100 / (1 + (g / lossR[i]))));
    const up = diffN(high, 1), down = diffN(low, 1).map((v) => -v);
    const pdm = up.map((u, i) => (u > down[i] && u > 0 ? u : 0.0)), mdm = down.map((d, i) => (d > up[i] && d > 0 ? d : 0.0));
    const atrSafe = replaceZero(atr);
    const pE = ewmMean(pdm, { alpha: 1 / this.adx_period }), mE = ewmMean(mdm, { alpha: 1 / this.adx_period });
    const pdi = pE.map((v, i) => 100 * (v / atrSafe[i])), mdi = mE.map((v, i) => 100 * (v / atrSafe[i]));
    const sum = replaceZero(pdi.map((v, i) => v + mdi[i]));
    const adx = ewmMean(pdi.map((v, i) => 100 * (Math.abs(v - mdi[i]) / sum[i])), { alpha: 1 / this.adx_period });
    const vol_sma = rollingMean(volume, 20);
    const win = 2 * this.N + 1, hiN = shift(high, this.N), loN = shift(low, this.N);
    const rmax = rollingExtreme(high, win, true), rmin = rollingExtreme(low, win, false);
    return { open, high, low, close, volume, ema_fast, ema_slow, ema_slow_slope, atr, rsi, adx, vol_sma,
      conf_high: hiN.map((v, i) => (v === rmax[i] ? v : NaN)), conf_low: loN.map((v, i) => (v === rmin[i] ? v : NaN)), n };
  }

  _update_symbol_pivots(symbol, row, bar_idx) {
    if (!this._pivots[symbol]) this._pivots[symbol] = [];
    const pivots = this._pivots[symbol];
    let updated = false;
    const c = [];
    if (!isNaNum(row.conf_high)) c.push({ kind: 'HIGH', bar_idx: bar_idx - this.N, price: Number(row.conf_high) });
    if (!isNaNum(row.conf_low)) c.push({ kind: 'LOW', bar_idx: bar_idx - this.N, price: Number(row.conf_low) });
    if (c.length === 2 && row.close >= row.open) c.reverse();
    for (const cand of c) {
      if (!pivots.length) { pivots.push(cand); updated = true; continue; }
      const last = pivots[pivots.length - 1];
      if (last.kind === cand.kind) {
        if ((cand.kind === 'HIGH' && cand.price > last.price) || (cand.kind === 'LOW' && cand.price < last.price)) { pivots[pivots.length - 1] = cand; updated = true; }
      } else { pivots.push(cand); updated = true; }
    }
    if (pivots.length > 20) this._pivots[symbol] = pivots.slice(-20);
    return updated;
  }

  _setup(symbol, swing_low, swing_high, direction, bar_idx, prior) {
    const s = CryptoFibAutopilotEngine._build_fib_setup(symbol, swing_low, swing_high, direction, bar_idx);   // the same levels as v1
    if (this.confluence) s.prior = prior;   // the swing point before the impulse
    return s;
  }

  screen_coin(spec, bars, state, quote_volume_24h, btc_regime = null) {
    if (bars.length <= Math.max(this.ema_slow_period, 2 * this.N + 2)) return null;
    if (quote_volume_24h < this.min_24h_quote_vol) return null;
    const d = this.compute_indicators(bars), i = d.n - 1;
    const row = {}, prev = {};
    for (const k of Object.keys(d)) if (Array.isArray(d[k])) { row[k] = d[k][i]; prev[k] = d[k][i - 1]; }
    const ts = tsString(bars[bars.length - 1].t);
    const bar_idx = (state.bar_counters[spec.symbol] || 0) + 1;
    state.bar_counters[spec.symbol] = bar_idx;
    const atr = Number(row.atr), adx = Number(row.adx);
    const close_p = Number(row.close), open_p = Number(row.open), high_p = Number(row.high), low_p = Number(row.low);
    const prev_high = Number(prev.high), prev_low = Number(prev.low), candle_range = high_p - low_p;
    if (isNaNum(atr) || isNaNum(adx) || close_p <= 0 || candle_range <= 0) return null;

    const pivot_updated = this._update_symbol_pivots(spec.symbol, row, bar_idx);
    const pivots = this._pivots[spec.symbol] || [];
    if (pivot_updated && pivots.length >= 2) {
      const p_prev = pivots[pivots.length - 2], p_last = pivots[pivots.length - 1];
      const prior = pivots.length >= 3 ? pivots[pivots.length - 3].price : null;
      if (Math.abs(p_last.price - p_prev.price) >= this.min_swing_atr_mult * atr) {
        const bullish_stack = close_p > row.ema_fast && row.ema_fast > row.ema_slow && row.ema_slow_slope > 0;
        const bearish_stack = close_p < row.ema_fast && row.ema_fast < row.ema_slow && row.ema_slow_slope < 0;
        if (p_prev.kind === 'LOW' && p_last.kind === 'HIGH' && bullish_stack) this._active_setups[spec.symbol] = this._setup(spec.symbol, p_prev.price, p_last.price, 'BULLISH', bar_idx, prior);
        else if (p_prev.kind === 'HIGH' && p_last.kind === 'LOW' && bearish_stack) this._active_setups[spec.symbol] = this._setup(spec.symbol, p_last.price, p_prev.price, 'BEARISH', bar_idx, prior);
      }
    }
    const setup = this._active_setups[spec.symbol];
    if (!setup || !setup.active || setup.traded) return null;
    if ((bar_idx - setup.created_bar) > this.max_setup_bars) { setup.active = false; return null; }
    if (bar_idx < (state.cooldown_until_bar[spec.symbol] || 0)) return null;
    if ((atr / close_p) < this.min_atr_pct || adx < this.min_adx) return null;
    const buffer = this.zone_buffer_atr_mult * atr;
    const vol_ratio = row.vol_sma > 0 ? Number(row.volume / row.vol_sma) : 1.0;
    const W = { '0.650': 35.0, '0.618': 35.0, '0.786': 28.0 }, r = this.tp1_close_ratio, R = setup.retracements;
    if (this.confluence) {
      const lo = Math.min(R['0.618'], R['0.786']) - buffer, hi = Math.max(R['0.618'], R['0.786']) + buffer;
      if (!(setup.prior !== null && setup.prior !== undefined && setup.prior >= lo && setup.prior <= hi)) return null;
    }

    if (setup.direction === 'BULLISH') {
      if (btc_regime === 'BEARISH') return null;
      if (close_p < setup.swing_low - buffer || close_p > setup.swing_high + buffer) { setup.active = false; return null; }
      for (const lvl_key of ['0.650', '0.618', '0.786']) {
        const lvl = R[lvl_key];
        const touched = (low_p <= (lvl + buffer) || prev_low <= (lvl + buffer)) && close_p >= (lvl - buffer);
        const structural_reversal = close_p > open_p && close_p > prev_high && ((close_p - low_p) / candle_range) >= 0.55;
        const rsi_recovery = pyMin2(Number(row.rsi), Number(prev.rsi)) < 45.0 && row.rsi > prev.rsi;
        if (touched && structural_reversal && rsi_recovery) {
          const entry = round_tick(close_p, spec.tick_size);
          const sl = round_tick(pyMin2(setup.swing_low - 0.5 * atr, entry * (1.0 - this.min_stop_pct)), spec.tick_size);
          const tp1 = round_tick(setup.swing_high, spec.tick_size), tp2 = round_tick(setup.extensions['1.272'], spec.tick_size);
          const fee_cost = entry * spec.taker_fee * 2;
          const risk = (entry - sl) + fee_cost;
          const reward = (((r * tp1) + ((1.0 - r) * tp2)) - entry) - fee_cost;
          if (risk <= 0 || (reward / risk) < this.min_rr) continue;
          const rr = reward / risk;
          return { symbol: spec.symbol, timestamp: ts, side: 'LONG', entry_price: entry, stop_loss: sl, take_profit_1: tp1, take_profit_2: tp2,
            fib_level: lvl_key, rr_ratio: pyRound(rr, 2), confluence_score: FibConfluenceEngine._compute_confluence_score(W[lvl_key], rr, vol_ratio, adx), setup_id: setup.setup_id };
        }
      }
    } else if (setup.direction === 'BEARISH') {
      if (btc_regime === 'BULLISH') return null;
      if (close_p > setup.swing_high + buffer || close_p < setup.swing_low - buffer) { setup.active = false; return null; }
      for (const lvl_key of ['0.650', '0.618', '0.786']) {
        const lvl = R[lvl_key];
        const touched = (high_p >= (lvl - buffer) || prev_high >= (lvl - buffer)) && close_p <= (lvl + buffer);
        const structural_reversal = close_p < open_p && close_p < prev_low && ((high_p - close_p) / candle_range) >= 0.55;
        const rsi_recovery = pyMax2(Number(row.rsi), Number(prev.rsi)) > 55.0 && row.rsi < prev.rsi;
        if (touched && structural_reversal && rsi_recovery) {
          const entry = round_tick(close_p, spec.tick_size);
          const sl = round_tick(pyMax2(setup.swing_high + 0.5 * atr, entry * (1.0 + this.min_stop_pct)), spec.tick_size);
          const tp1 = round_tick(setup.swing_low, spec.tick_size), tp2 = round_tick(setup.extensions['1.272'], spec.tick_size);
          const fee_cost = entry * spec.taker_fee * 2;
          const risk = (sl - entry) + fee_cost;
          const reward = (entry - ((r * tp1) + ((1.0 - r) * tp2))) - fee_cost;
          if (risk <= 0 || (reward / risk) < this.min_rr) continue;
          const rr = reward / risk;
          return { symbol: spec.symbol, timestamp: ts, side: 'SHORT', entry_price: entry, stop_loss: sl, take_profit_1: tp1, take_profit_2: tp2,
            fib_level: lvl_key, rr_ratio: pyRound(rr, 2), confluence_score: FibConfluenceEngine._compute_confluence_score(W[lvl_key], rr, vol_ratio, adx), setup_id: setup.setup_id };
        }
      }
    }
    return null;
  }

  static _compute_confluence_score(level_pts, rr, vol_ratio, adx) {
    const rr_pts = pyMin2(25.0, pyMax2(0.0, (rr - 1.2) * 12.5));
    const vol_pts = pyMin2(20.0, pyMax2(0.0, (vol_ratio - 0.8) * 15.0));
    const adx_pts = pyMin2(20.0, pyMax2(0.0, (adx - 18.0) * 0.8));
    return pyRound(level_pts + rr_pts + vol_pts + adx_pts, 2);
  }

  autopilot_step(universe, state, btc_regime = null, { allowEntries = true } = {}) {
    const orders = [];
    const mk = (o) => ({ stop_loss: null, take_profit_1: null, take_profit_2: null, reason: '', realized_pnl: 0.0, ...o });
    for (const symbol of Object.keys(state.positions)) {
      if (!universe.has(symbol)) continue;
      const pos = state.positions[symbol], { spec, bars } = universe.get(symbol), last = bars[bars.length - 1];
      const high_p = Number(last.high), low_p = Number(last.low), bar_idx = state.bar_counters[symbol] || 0, fee = spec.taker_fee, L = this.leverage;
      const long = pos.side === 'LONG';
      if (pos.side !== 'LONG' && pos.side !== 'SHORT') continue;
      if (long ? low_p <= pos.stop_loss : high_p >= pos.stop_loss) {
        const exit_fee = pos.stop_loss * pos.remaining_size * fee;
        const pnl = (long ? (pos.stop_loss - pos.entry_price) : (pos.entry_price - pos.stop_loss)) * pos.remaining_size - exit_fee;
        state.equity += pnl;
        state.free_balance += (pos.entry_price * pos.remaining_size) / L + pnl;
        state.cooldown_until_bar[symbol] = bar_idx + this.cooldown_bars;
        delete state.positions[symbol];
        orders.push(mk({ client_order_id: `${pos.position_id}_SL`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size, price: pos.stop_loss, reason: 'STOP_LOSS_HIT', realized_pnl: pyRound(pnl, 4) }));
        continue;
      }
      if (!pos.tp1_hit && (long ? high_p >= pos.take_profit_1 : low_p <= pos.take_profit_1)) {
        const close_qty = floor_step(pos.initial_size * this.tp1_close_ratio, spec.step_size);
        if (close_qty > 0 && (pos.remaining_size - close_qty) >= spec.step_size) {
          const exit_fee = pos.take_profit_1 * close_qty * fee;
          const pnl = (long ? (pos.take_profit_1 - pos.entry_price) : (pos.entry_price - pos.take_profit_1)) * close_qty - exit_fee;
          state.equity += pnl;
          state.free_balance += (pos.entry_price * close_qty) / L + pnl;
          pos.remaining_size = floor_step(pos.remaining_size - close_qty, spec.step_size);
          pos.tp1_hit = true;
          pos.stop_loss = round_tick(pos.entry_price * (long ? (1.0 + 2.2 * fee) : (1.0 - 2.2 * fee)), spec.tick_size);
          pos.risk_amount = 0.0;
          orders.push(mk({ client_order_id: `${pos.position_id}_TP1`, symbol, action: 'CLOSE_PARTIAL', order_type: 'MARKET', quantity: close_qty, price: pos.take_profit_1, stop_loss: pos.stop_loss, reason: 'TP1_PARTIAL_AND_BREAKEVEN', realized_pnl: pyRound(pnl, 4) }));
        }
      }
      if (pos.tp1_hit && (long ? high_p >= pos.take_profit_2 : low_p <= pos.take_profit_2)) {
        const exit_fee = pos.take_profit_2 * pos.remaining_size * fee;
        const pnl = (long ? (pos.take_profit_2 - pos.entry_price) : (pos.entry_price - pos.take_profit_2)) * pos.remaining_size - exit_fee;
        state.equity += pnl;
        state.free_balance += (pos.entry_price * pos.remaining_size) / L + pnl;
        delete state.positions[symbol];
        orders.push(mk({ client_order_id: `${pos.position_id}_TP2`, symbol, action: 'CLOSE_ALL', order_type: 'MARKET', quantity: pos.remaining_size, price: pos.take_profit_2, reason: 'TP2_FINAL_TARGET', realized_pnl: pyRound(pnl, 4) }));
      }
    }
    const candidates = [];
    for (const [symbol, u] of universe) {
      if (symbol in state.positions) continue;
      const sig = this.screen_coin(u.spec, u.bars, state, u.quote_volume_24h, btc_regime);
      if (sig !== null) candidates.push(sig);
    }
    candidates.sort((a, b) => b.confluence_score - a.confluence_score);
    if (!allowEntries) return { orders, candidates };
    for (const sig of candidates) {
      if (Object.keys(state.positions).length >= this.max_open_positions) break;
      if (Object.values(state.positions).filter((p) => p.side === sig.side).length >= this.max_same_direction) continue;
      const current_open_risk = Object.values(state.positions).reduce((a, p) => a + p.risk_amount, 0);
      const trade_risk_budget = Math.min(state.equity * this.risk_per_trade, state.equity * this.max_portfolio_risk - current_open_risk);
      if (trade_risk_budget <= 0) break;
      const spec = universe.get(sig.symbol).spec, price_risk = Math.abs(sig.entry_price - sig.stop_loss);
      if (price_risk <= 0) continue;
      const raw_qty = trade_risk_budget / price_risk;
      const max_notional = Math.min(state.free_balance * this.leverage * 0.98, state.equity * this.leverage * this.max_notional_pct);
      const qty = floor_step(Math.min(raw_qty, max_notional / sig.entry_price), spec.step_size);
      const notional = qty * sig.entry_price, entry_fee = notional * spec.taker_fee;
      const required_margin = (notional / this.leverage) + entry_fee;
      if (qty <= 0 || notional < spec.min_notional || required_margin > state.free_balance) continue;
      const pos_id = `POS_${sig.setup_id}`;
      state.free_balance -= required_margin;
      state.equity -= entry_fee;
      state.positions[sig.symbol] = { position_id: pos_id, symbol: sig.symbol, side: sig.side, entry_time: String(sig.timestamp), entry_price: sig.entry_price,
        stop_loss: sig.stop_loss, take_profit_1: sig.take_profit_1, take_profit_2: sig.take_profit_2, initial_size: qty, remaining_size: qty,
        fib_level: sig.fib_level, risk_amount: (qty * price_risk) + (notional * spec.taker_fee * 2), tp1_hit: false };
      if (sig.symbol in this._active_setups) this._active_setups[sig.symbol].traded = true;
      orders.push(mk({ client_order_id: `${pos_id}_ENTRY`, symbol: sig.symbol, action: `OPEN_${sig.side}`, order_type: 'MARKET', quantity: qty, price: sig.entry_price,
        stop_loss: sig.stop_loss, take_profit_1: sig.take_profit_1, take_profit_2: sig.take_profit_2,
        reason: `FIB_${sig.fib_level}_SCORE_${pyRepr(sig.confluence_score)}_RR_${pyRepr(sig.rr_ratio)}` }));
    }
    return { orders, candidates };
  }
}

/** Engine parameters for a timeframe: the class defaults, the spec's preset for that timeframe, and the leverage. */
function engineParams(timeframe = '15m', leverage = 3.0) {
  const preset = PRESETS[TIMEFRAMES[timeframe] || 'intraday'];
  return { ...preset, leverage };
}

/** Binance kline arrays -> spec bars. */
const toBars = (klines) => klines.map((k) => ({ t: Number(k[0]), open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]),
  volume: parseFloat(k[5]), qv: parseFloat(k[7]), closeTime: Number(k[6]) }));

/** 24h quote volume at the last bar: the sum of the bars' quote volume over the last 24 hours. */
function quoteVolume24h(bars, tfMs) {
  const k = Math.round(86400000 / tfMs);
  let s = 0;
  for (let i = Math.max(0, bars.length - k); i < bars.length; i++) s += Number(bars[i].qv) || 0;
  return s;
}

module.exports = {
  CryptoFibAutopilotEngine, FibConfluenceEngine, SymbolSpec, PortfolioState, PRESETS, TIMEFRAMES, TF_MS, LEVEL_ORDER, LEVEL_WEIGHTS,
  engineParams, toBars, quoteVolume24h,
  // kernels and helpers (exported for tests and the cross-check)
  pyRound, pyRoundInt, pyRepr, ewmMean, rollingMean, rollingExtreme, round_tick, floor_step, tsString
};
