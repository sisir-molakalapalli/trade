/**
 * =========================================================================
 * SISIR QUANT - CANDLESTICK ENGINE (after Steve Nison, "Japanese Candlestick
 * Charting Techniques", 2nd ed.) — pure functions, no I/O.
 * =========================================================================
 * Implements the book's patterns AND its rules for using them:
 *   1. TREND CONTEXT  — a reversal pattern only counts if there is a trend to
 *      reverse (hammer after a decline, hanging man after a rally, ...).
 *   2. CONFIRMATION   — weaker signals (hanging man, inverted hammer, shooting
 *      star, harami, doji, tweezers, counterattack) need the NEXT candle to
 *      confirm before they are acted on.
 *   3. CONVERGENCE    — a signal is stronger when it lines up with Western
 *      tools: volume, support/resistance (incl. change of polarity) and RSI.
 *
 * CRYPTO ADAPTATION: crypto trades 24/7, so a candle almost always opens at the
 * previous close and true price gaps ("windows") are rare on intraday bars.
 * Patterns that need a gap use the REAL-BODY gap (body vs body) where Nison
 * allows it (stars, two crows), and a true high/low gap only where the gap IS
 * the pattern (windows, tasuki, abandoned baby, gapping plays) — those simply
 * fire rarely on liquid 24/7 pairs.
 *
 * Input: Binance klines [openTime, o, h, l, c, v, closeTime, qv, n, takerBuyBase, ...],
 * CLOSED candles only, oldest first.
 * =========================================================================
 */
'use strict';

const num = (x) => Number(x);

function parse(klines) {
  return klines.map((k) => {
    const o = num(k[1]), h = num(k[2]), l = num(k[3]), c = num(k[4]), v = num(k[5]), tb = num(k[9]);
    const range = Math.max(h - l, 1e-12), body = Math.abs(c - o);
    return {
      o, h, l, c, v, tb, range, body,
      top: Math.max(o, c), bot: Math.min(o, c), mid: (o + c) / 2,
      upper: h - Math.max(o, c), lower: Math.min(o, c) - l,
      white: c > o, black: c < o, takerBuyPct: v > 0 ? (tb / v) * 100 : 50
    };
  });
}

function ema(vals, p) { const k = 2 / (p + 1), o = []; vals.forEach((x, i) => o.push(i ? x * k + o[i - 1] * (1 - k) : x)); return o; }
function rsi(closes, p = 14) {
  if (closes.length <= p) return closes.map(() => 50);
  const out = [50]; let g = 0, l = 0;
  for (let i = 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1], up = Math.max(d, 0), dn = Math.max(-d, 0);
    if (i <= p) { g += up / p; l += dn / p; } else { g = (g * (p - 1) + up) / p; l = (l * (p - 1) + dn) / p; }
    out.push(l === 0 ? 100 : 100 - 100 / (1 + g / l));
  }
  return out;
}

// ---------------------------------------------------------------- context
function makeCtx(B) {
  const closes = B.map((b) => b.c);
  const e10 = ema(closes, 10);
  const rsi14 = rsi(closes, 14);
  const avgBody = (i, n = 10) => { let s = 0, k = 0; for (let j = Math.max(0, i - n); j < i; j++) { s += B[j].body; k++; } return k ? s / k : B[i].body; };
  const avgVol = (i, n = 10) => { let s = 0, k = 0; for (let j = Math.max(0, i - n); j < i; j++) { s += B[j].v; k++; } return k ? s / k : B[i].v; };
  const avgRange = (i, n = 10) => { let s = 0, k = 0; for (let j = Math.max(0, i - n); j < i; j++) { s += B[j].range; k++; } return k ? s / k : B[i].range; };
  // short-term trend BEFORE index i (Nison: the trend the pattern would reverse)
  const trendBefore = (i) => {
    const j = i - 1; if (j < 8) return 'none';
    const fell = B[j].c < B[j - 6].c && e10[j] < e10[j - 3];
    const rose = B[j].c > B[j - 6].c && e10[j] > e10[j - 3];
    return fell ? 'down' : rose ? 'up' : 'none';
  };
  const isDoji = (b) => b.body <= b.range * 0.1;
  const isLong = (i) => B[i].body >= Math.max(avgBody(i) * 1.3, B[i].range * 0.55);
  const isSmall = (i) => B[i].body <= Math.max(avgBody(i) * 0.6, 0) || B[i].body <= B[i].range * 0.3;
  const isSpinning = (i) => !isDoji(B[i]) && B[i].body <= B[i].range * 0.3 && B[i].upper >= B[i].body && B[i].lower >= B[i].body;
  const lowestLow = (i, n) => Math.min(...B.slice(Math.max(0, i - n), i).map((b) => b.l));
  const highestHigh = (i, n) => Math.max(...B.slice(Math.max(0, i - n), i).map((b) => b.h));
  return { B, e10, rsi14, avgBody, avgVol, avgRange, trendBefore, isDoji, isLong, isSmall, isSpinning, lowestLow, highestHigh };
}

// ---------------------------------------------------------------- pattern catalog
// Each detector: (C, i) -> truthy if the pattern COMPLETES on bar i.
// dir: bull | bear | neutral ; kind: reversal | continuation | warning ;
// trend: required prior trend ('down' | 'up' | null) ; confirm: needs next-bar confirmation.
const P = [];
const def = (name, dir, kind, trend, confirm, bars, fn, weight = 1) => P.push({ name, dir, kind, trend, confirm, bars, fn, weight });

// --- Chapter 4: hammer & hanging man, engulfing, dark-cloud cover, piercing
const hammerShape = (b) => b.lower >= 2 * b.body && b.upper <= Math.max(b.body * 0.5, b.range * 0.1) && b.body > 0 && b.body <= b.range * 0.4;
def('HAMMER', 'bull', 'reversal', 'down', false, 1, (C, i) => hammerShape(C.B[i]) && C.B[i].l <= C.lowestLow(i, 8));
def('HANGING MAN', 'bear', 'reversal', 'up', true, 1, (C, i) => hammerShape(C.B[i]) && C.B[i].top >= C.highestHigh(i, 8) * 0.997);
def('BULLISH ENGULFING', 'bull', 'reversal', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.black && b.white && b.top >= a.top && b.bot <= a.bot && b.body > a.body; }, 1.3);
def('BEARISH ENGULFING', 'bear', 'reversal', 'up', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.white && b.black && b.top >= a.top && b.bot <= a.bot && b.body > a.body; }, 1.3);
def('DARK-CLOUD COVER', 'bear', 'reversal', 'up', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && a.white && b.black && b.o >= a.c && b.c < a.mid && b.c > a.o; }, 1.2);
def('PIERCING PATTERN', 'bull', 'reversal', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && a.black && b.white && b.o <= a.c && b.c > a.mid && b.c < a.o; }, 1.2);

// --- Chapter 5: stars (crypto: the star's REAL BODY gaps from the first body)
const bodyGapDown = (a, b) => b.top < a.bot || b.top <= a.c * 1.0005;   // star body at/below first close
const bodyGapUp = (a, b) => b.bot > a.top || b.bot >= a.c * 0.9995;
def('MORNING STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return C.isLong(i - 2) && a.black && C.isSmall(i - 1) && !C.isDoji(s) && bodyGapDown(a, s) && c.white && c.c > a.mid; }, 1.4);
def('EVENING STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return C.isLong(i - 2) && a.white && C.isSmall(i - 1) && !C.isDoji(s) && bodyGapUp(a, s) && c.black && c.c < a.mid; }, 1.4);
def('MORNING DOJI STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return C.isLong(i - 2) && a.black && C.isDoji(s) && bodyGapDown(a, s) && c.white && c.c > a.mid; }, 1.5);
def('EVENING DOJI STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return C.isLong(i - 2) && a.white && C.isDoji(s) && bodyGapUp(a, s) && c.black && c.c < a.mid; }, 1.5);
def('BULLISH ABANDONED BABY', 'bull', 'reversal', 'down', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.black && C.isDoji(s) && s.h < a.l && s.h < c.l && c.white; }, 1.6);
def('BEARISH ABANDONED BABY', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && C.isDoji(s) && s.l > a.h && s.l > c.h && c.black; }, 1.6);
const shootingShape = (b) => b.upper >= 2 * b.body && b.lower <= Math.max(b.body * 0.5, b.range * 0.1) && b.body > 0 && b.body <= b.range * 0.4;
def('SHOOTING STAR', 'bear', 'reversal', 'up', true, 2, (C, i) => shootingShape(C.B[i]) && C.B[i].bot >= C.B[i - 1].c * 0.999);
def('INVERTED HAMMER', 'bull', 'reversal', 'down', true, 2, (C, i) => shootingShape(C.B[i]) && C.B[i].top <= C.B[i - 1].c * 1.001 && C.B[i - 1].black);

// --- Chapter 6: harami, tweezers, belt-hold, two crows, three crows/soldiers, counterattack, towers, triples
const harami = (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && b.top <= a.top && b.bot >= a.bot && b.body <= a.body * 0.5; };
def('BULLISH HARAMI', 'bull', 'reversal', 'down', true, 2, (C, i) => C.B[i - 1].black && !C.isDoji(C.B[i]) && harami(C, i));
def('BEARISH HARAMI', 'bear', 'reversal', 'up', true, 2, (C, i) => C.B[i - 1].white && !C.isDoji(C.B[i]) && harami(C, i));
def('BULLISH HARAMI CROSS', 'bull', 'reversal', 'down', true, 2, (C, i) => C.B[i - 1].black && C.isDoji(C.B[i]) && harami(C, i), 1.2);
def('BEARISH HARAMI CROSS', 'bear', 'reversal', 'up', true, 2, (C, i) => C.B[i - 1].white && C.isDoji(C.B[i]) && harami(C, i), 1.2);
def('TWEEZERS BOTTOM', 'bull', 'reversal', 'down', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return Math.abs(a.l - b.l) <= a.l * 0.0008 && (C.isLong(i - 1) || a.lower >= a.body || b.lower >= b.body) && b.l <= C.lowestLow(i - 1, 8) * 1.001; });
def('TWEEZERS TOP', 'bear', 'reversal', 'up', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return Math.abs(a.h - b.h) <= a.h * 0.0008 && (C.isLong(i - 1) || a.upper >= a.body || b.upper >= b.body) && b.h >= C.highestHigh(i - 1, 8) * 0.999; });
def('BULLISH BELT-HOLD', 'bull', 'reversal', 'down', false, 1, (C, i) => { const b = C.B[i]; return b.white && C.isLong(i) && b.lower <= b.range * 0.05 && b.o <= C.lowestLow(i, 8) * 1.001; });
def('BEARISH BELT-HOLD', 'bear', 'reversal', 'up', false, 1, (C, i) => { const b = C.B[i]; return b.black && C.isLong(i) && b.upper <= b.range * 0.05 && b.o >= C.highestHigh(i, 8) * 0.999; });
def('UPSIDE-GAP TWO CROWS', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, x, y] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return C.isLong(i - 2) && a.white && x.black && x.bot > a.top && y.black && y.o > x.o && y.c < x.c && y.c > a.c; }, 1.2);
def('THREE BLACK CROWS', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return [a, b, c].every((k) => k.black && k.body >= k.range * 0.6 && k.lower <= k.range * 0.2) && b.c < a.c && c.c < b.c && b.o <= a.o && b.o >= a.c && c.o <= b.o && c.o >= b.c; }, 1.4);
def('THREE WHITE SOLDIERS', 'bull', 'continuation', null, false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return [a, b, c].every((k) => k.white && k.body >= k.range * 0.6 && k.upper <= k.range * 0.2) && b.c > a.c && c.c > b.c && b.o >= a.o && b.o <= a.c && c.o >= b.o && c.o <= b.c; }, 1.3);
def('ADVANCE BLOCK', 'bear', 'warning', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && b.white && c.white && b.c > a.c && c.c > b.c && b.body < a.body && c.body < b.body && (b.upper > a.upper && c.upper > b.upper); });
def('STALLED PATTERN', 'bear', 'warning', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && b.white && C.isLong(i - 2) && C.isLong(i - 1) && b.c > a.c && (C.isSmall(i) || C.isSpinning(i)) && c.bot >= b.c * 0.998; });
def('BULLISH COUNTERATTACK', 'bull', 'reversal', 'down', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && a.black && C.isLong(i) && b.white && b.o < a.c && Math.abs(b.c - a.c) <= a.body * 0.1; });
def('BEARISH COUNTERATTACK', 'bear', 'reversal', 'up', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && a.white && C.isLong(i) && b.black && b.o > a.c && Math.abs(b.c - a.c) <= a.body * 0.1; });
def('TOWER BOTTOM', 'bull', 'reversal', 'down', false, 5, (C, i) => { const a = C.B[i - 4], end = C.B[i]; const mids = C.B.slice(i - 3, i); return C.isLong(i - 4) && a.black && mids.every((m, k) => C.isSmall(i - 3 + k)) && C.isLong(i) && end.white && end.c > a.mid; });
def('TOWER TOP', 'bear', 'reversal', 'up', false, 5, (C, i) => { const a = C.B[i - 4], end = C.B[i]; const mids = C.B.slice(i - 3, i); return C.isLong(i - 4) && a.white && mids.every((m, k) => C.isSmall(i - 3 + k)) && C.isLong(i) && end.black && end.c < a.mid; });
// three rivers (inverted three Buddha) bottom / three mountains (three Buddha) top: 3 similar swing extremes in 40 bars
function swings(C, i, n, type) {
  const out = [];
  for (let j = Math.max(2, i - n); j <= i - 2; j++) {
    const b = C.B[j];
    if (type === 'low' && b.l < C.B[j - 1].l && b.l < C.B[j - 2].l && b.l <= C.B[j + 1].l && b.l <= C.B[j + 2].l) out.push(b.l);
    if (type === 'high' && b.h > C.B[j - 1].h && b.h > C.B[j - 2].h && b.h >= C.B[j + 1].h && b.h >= C.B[j + 2].h) out.push(b.h);
  }
  return out;
}
// three similar swing lows (highs) within 40 bars, then a candle that breaks out of the last 10 bars' range
def('THREE RIVERS BOTTOM', 'bull', 'reversal', null, false, 1, (C, i) => { if (i < 42) return false; const lows = swings(C, i, 40, 'low').slice(-3); if (lows.length < 3) return false; const lo = Math.min(...lows), hi = Math.max(...lows); return (hi - lo) / lo <= 0.004 && C.B[i].white && C.B[i].c > Math.max(...C.B.slice(i - 10, i).map((b) => b.h)); }, 1.3);
def('THREE MOUNTAINS TOP', 'bear', 'reversal', null, false, 1, (C, i) => { if (i < 42) return false; const highs = swings(C, i, 40, 'high').slice(-3); if (highs.length < 3) return false; const lo = Math.min(...highs), hi = Math.max(...highs); return (hi - lo) / hi <= 0.004 && C.B[i].black && C.B[i].c < Math.min(...C.B.slice(i - 10, i).map((b) => b.l)); }, 1.3);

// --- Chapter 7: continuation — windows, tasuki, gapping plays, three methods, separating lines, side-by-side whites
def('RISING WINDOW', 'bull', 'continuation', 'up', false, 2, (C, i) => C.B[i].l > C.B[i - 1].h);
def('FALLING WINDOW', 'bear', 'continuation', 'down', false, 2, (C, i) => C.B[i].h < C.B[i - 1].l);
def('UPWARD-GAP TASUKI', 'bull', 'continuation', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && b.white && b.l > a.h && c.black && c.o > b.bot && c.o < b.top && c.c < b.bot && c.c > a.h; });
def('DOWNWARD-GAP TASUKI', 'bear', 'continuation', 'down', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.black && b.black && b.h < a.l && c.white && c.o < b.top && c.o > b.bot && c.c > b.top && c.c < a.l; });
def('HIGH-PRICE GAPPING PLAY', 'bull', 'continuation', 'up', false, 1, (C, i) => { if (i < 8) return false; const base = C.B.slice(i - 6, i); const hi = Math.max(...base.map((b) => b.h)), lo = Math.min(...base.map((b) => b.l)); return C.isLong(i - 7) && C.B[i - 7].white && (hi - lo) <= C.B[i - 7].range && lo >= C.B[i - 7].mid && C.B[i].l > hi; });
def('LOW-PRICE GAPPING PLAY', 'bear', 'continuation', 'down', false, 1, (C, i) => { if (i < 8) return false; const base = C.B.slice(i - 6, i); const hi = Math.max(...base.map((b) => b.h)), lo = Math.min(...base.map((b) => b.l)); return C.isLong(i - 7) && C.B[i - 7].black && (hi - lo) <= C.B[i - 7].range && hi <= C.B[i - 7].mid && C.B[i].h < lo; });
def('GAPPING SIDE-BY-SIDE WHITE LINES', 'bull', 'continuation', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && b.white && c.white && b.l > a.h && Math.abs(c.o - b.o) <= b.body * 0.15 && Math.abs(c.body - b.body) <= b.body * 0.3; });
const threeMethods = (C, i, up) => { const first = C.B[i - 4], last = C.B[i], mid = C.B.slice(i - 3, i);
  if (!C.isLong(i - 4) || !C.isLong(i)) return false;
  if (up ? !(first.white && last.white && last.c > first.c) : !(first.black && last.black && last.c < first.c)) return false;
  return mid.every((m, k) => C.isSmall(i - 3 + k) && m.h <= first.h * 1.001 && m.l >= first.l * 0.999); };
def('RISING THREE METHODS', 'bull', 'continuation', 'up', false, 5, (C, i) => threeMethods(C, i, true), 1.3);
def('FALLING THREE METHODS', 'bear', 'continuation', 'down', false, 5, (C, i) => threeMethods(C, i, false), 1.3);
def('BULLISH SEPARATING LINE', 'bull', 'continuation', 'up', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.black && b.white && C.isLong(i) && Math.abs(b.o - a.o) <= a.range * 0.1 && b.lower <= b.range * 0.1; });
def('BEARISH SEPARATING LINE', 'bear', 'continuation', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.white && b.black && C.isLong(i) && Math.abs(b.o - a.o) <= a.range * 0.1 && b.upper <= b.range * 0.1; });

// --- Chapter 8: doji family
def('NORTHERN DOJI', 'bear', 'warning', 'up', true, 2, (C, i) => C.isDoji(C.B[i]) && C.isLong(i - 1) && C.B[i - 1].white, 1.1);
def('SOUTHERN DOJI', 'bull', 'warning', 'down', true, 2, (C, i) => C.isDoji(C.B[i]) && C.isLong(i - 1) && C.B[i - 1].black);
def('GRAVESTONE DOJI', 'bear', 'reversal', 'up', true, 1, (C, i) => { const b = C.B[i]; return C.isDoji(b) && b.upper >= b.range * 0.7 && b.lower <= b.range * 0.1; }, 1.1);
def('DRAGONFLY DOJI', 'bull', 'reversal', 'down', true, 1, (C, i) => { const b = C.B[i]; return C.isDoji(b) && b.lower >= b.range * 0.7 && b.upper <= b.range * 0.1; }, 1.1);
def('LONG-LEGGED DOJI', 'neutral', 'warning', null, false, 1, (C, i) => { const b = C.B[i]; return C.isDoji(b) && b.upper >= b.range * 0.35 && b.lower >= b.range * 0.35 && b.range >= C.avgRange(i) * 1.5; });
def('BULLISH TRI-STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => [0, 1, 2].every((k) => C.isDoji(C.B[i - k])) && C.B[i - 1].top < Math.min(C.B[i - 2].bot, C.B[i].bot));
def('BEARISH TRI-STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => [0, 1, 2].every((k) => C.isDoji(C.B[i - k])) && C.B[i - 1].bot > Math.max(C.B[i - 2].top, C.B[i].top));

// --- Chapter 3 basics: indecision candles (context only, never an entry)
def('SPINNING TOP', 'neutral', 'warning', null, false, 1, (C, i) => C.isSpinning(i));
def('HIGH-WAVE CANDLE', 'neutral', 'warning', null, false, 1, (C, i) => { const b = C.B[i]; return b.body <= b.range * 0.25 && b.upper >= b.range * 0.35 && b.lower >= b.range * 0.35 && b.range >= C.avgRange(i) * 1.5; });

const CATALOG = P.map(({ name, dir, kind, trend, confirm }) => ({ name, dir, kind, trend, confirm }));

// ---------------------------------------------------------------- convergence (Nison Part 2)
function convergence(C, i, dir) {
  const notes = [];
  const b = C.B[i], rv = C.avgVol(i) > 0 ? b.v / C.avgVol(i) : 1;
  if (rv >= 1.3) notes.push(`volume ${rv.toFixed(1)}x`);
  const r = C.rsi14[i];
  if (dir === 'bull' && r <= 35) notes.push(`RSI ${r.toFixed(0)} oversold`);
  if (dir === 'bear' && r >= 65) notes.push(`RSI ${r.toFixed(0)} overbought`);
  // support / resistance: prior swing levels in the last 60 bars (incl. change of polarity:
  // an old resistance high that price is now testing from above acts as support).
  const n = Math.min(60, i - 3);
  if (n > 10) {
    const lows = swings(C, i - 1, n, 'low'), highs = swings(C, i - 1, n, 'high');
    const tol = b.c * 0.003;
    if (dir === 'bull') {
      if (lows.some((x) => Math.abs(b.l - x) <= tol)) notes.push('at prior support');
      else if (highs.some((x) => Math.abs(b.l - x) <= tol && b.c > x)) notes.push('change of polarity (old resistance as support)');
    } else if (dir === 'bear') {
      if (highs.some((x) => Math.abs(b.h - x) <= tol)) notes.push('at prior resistance');
      else if (lows.some((x) => Math.abs(b.h - x) <= tol && b.c < x)) notes.push('change of polarity (old support as resistance)');
    }
  }
  return { notes, rvol: rv };
}

// ---------------------------------------------------------------- scan
/**
 * Scans the last closed candles. Returns every pattern that completed on the last bar
 * (`fresh`) and every confirmation-needing pattern that completed on the bar before and
 * was CONFIRMED by the last bar (`confirmed`).
 */
function scan(klines) {
  const B = parse(Array.isArray(klines) ? klines : []);
  const empty = { patterns: [], bull: [], bear: [], warnings: [], trend: 'none', ctx: null };
  if (B.length < 12) return empty;
  const C = makeCtx(B), n = B.length - 1;
  const found = [];
  const test = (p, i) => {
    if (i - p.bars + 1 < 1) return false;
    if (p.trend) { const t = C.trendBefore(i - p.bars + 1); if (t !== p.trend) return false; }
    try { return !!p.fn(C, i); } catch (e) { return false; }
  };
  for (const p of P) {
    if (!p.confirm && test(p, n)) found.push({ ...p, at: n, status: 'complete' });
    else if (p.confirm && test(p, n - 1)) {
      const last = B[n - 1], nxt = B[n];
      const ok = p.dir === 'bull' ? (nxt.white && nxt.c > last.top) : p.dir === 'bear' ? (nxt.black && nxt.c < last.bot) : false;
      if (ok) found.push({ ...p, at: n - 1, status: 'confirmed' });
    } else if (p.confirm && test(p, n)) found.push({ ...p, at: n, status: 'awaiting confirmation' });
  }
  const withConv = found.map((f) => {
    const conv = convergence(C, f.at, f.dir);
    const strength = Math.min(99, Math.round(60 + 10 * f.weight + 6 * conv.notes.length + (f.status === 'confirmed' ? 6 : 0)));
    return { name: f.name, dir: f.dir, kind: f.kind, status: f.status, strength, convergence: conv.notes, rvol: Number(conv.rvol.toFixed(2)), barsAgo: n - f.at };
  }).sort((a, b) => b.strength - a.strength);
  const actionable = (f) => f.status !== 'awaiting confirmation';
  return {
    patterns: withConv,
    bull: withConv.filter((f) => f.dir === 'bull' && actionable(f) && f.kind !== 'warning'),
    bear: withConv.filter((f) => f.dir === 'bear' && actionable(f) && f.kind !== 'warning'),
    warnings: withConv.filter((f) => f.kind === 'warning' || f.status === 'awaiting confirmation'),
    trend: C.trendBefore(n + 1),
    ctx: C
  };
}

module.exports = { scan, parse, CATALOG };
