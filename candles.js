/**
 * =========================================================================
 * SISIR QUANT - CANDLESTICK ENGINE
 * Rebuilt from a full reading of Steve Nison, "Japanese Candlestick Charting
 * Techniques". Pure functions, no I/O.
 * =========================================================================
 * Nison's rules, each enforced in code:
 *  - "Reversal" = the prior trend should change. It needs a trend to reverse, and a
 *    new position only goes WITH the major trend (Ch 4; Ex 7.9 and 20.3: a hammer or
 *    morning star in a bear market is not a buy). Spot-only, so bearish signals protect
 *    or block longs and never open shorts.
 *  - Act only on a completed or confirmed formation (Ex 7.34).
 *  - Must confirm: hanging man, inverted hammer, shooting star, doji, harami (close above
 *    the pattern's high), dark-cloud under 50%, a hammer right after a shaven black candle.
 *  - Piercing must close beyond the midpoint. Under that it is on-neck / in-neck /
 *    thrusting, which are BEARISH in a decline.
 *  - Tweezers matter only with another candlestick signal (Ch 6).
 *  - Doji are judged against recent bodies, ignored when the chart is full of doji, and
 *    need more confirmation at bottoms than tops (Ch 8).
 *  - White soldiers count from a low area, not after an extended run; advance block and
 *    stalled patterns protect longs (Ch 7).
 *  - Three windows, or 8-10 record sessions, mean a correction is due (Ch 7).
 *  - Rule of Multiple Techniques (Part 2): volume, support, change of polarity, moving
 *    average, 38/50/62% retracement, oversold, springs and upthrusts.
 *  - Trade location: pattern extreme is the risk, next resistance the objective (Ch 11, 20).
 *
 * 24/7 adaptation: Nison gives latitude where the open sits at the prior close (equities,
 * Ex 5.20-5.21). Crypto is the same, so star / dark-cloud / piercing use his latitude
 * versions. Windows keep the true shadow gap and so fire rarely.
 *
 * Input: Binance klines, CLOSED candles only, oldest first.
 * =========================================================================
 */
'use strict';

const num = (x) => Number(x);

function parse(klines) {
  return klines.map((k) => {
    const o = num(k[1]), h = num(k[2]), l = num(k[3]), c = num(k[4]), v = num(k[5]), tb = num(k[9]);
    const range = Math.max(h - l, 1e-12), body = Math.abs(c - o);
    return { o, h, l, c, v, tb, range, body, top: Math.max(o, c), bot: Math.min(o, c), mid: (o + c) / 2,
      upper: h - Math.max(o, c), lower: Math.min(o, c) - l, white: c > o, black: c < o, takerBuyPct: v > 0 ? (tb / v) * 100 : 50 };
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
  const closes = B.map((b) => b.c), e10 = ema(closes, 10), e20 = ema(closes, 20), e50 = ema(closes, 50), rsi14 = rsi(closes, 14);
  const avg = (f, i, n) => { let s = 0, k = 0; for (let j = Math.max(0, i - n); j < i; j++) { s += f(B[j]); k++; } return k ? s / k : f(B[i]); };
  const avgBody = (i, n = 20) => avg((b) => b.body, i, n);
  const avgVol = (i, n = 20) => avg((b) => b.v, i, n);
  const avgRange = (i, n = 14) => avg((b) => b.range, i, n);
  const lowestLow = (i, n) => Math.min(...B.slice(Math.max(0, i - n), i).map((b) => b.l));
  const highestHigh = (i, n) => Math.max(...B.slice(Math.max(0, i - n), i).map((b) => b.h));
  // the minor trend the pattern would reverse ("even a minor one", Ch 4)
  const trendBefore = (i) => {
    const j = i - 1; if (j < 8) return 'none';
    const fell = B[j].c < B[j - 5].c && e10[j] < e10[j - 3];
    const rose = B[j].c > B[j - 5].c && e10[j] > e10[j - 3];
    return fell ? 'down' : rose ? 'up' : 'none';
  };
  const majorTrend = (i) => {
    if (i < 55) return 'flat';
    const slope = e50[i] - e50[i - 10];
    if (B[i].c >= e50[i] * 0.995 && slope >= 0) return 'up';
    if (B[i].c < e50[i] && slope < 0) return 'down';
    return 'flat';
  };
  const rawDoji = (b) => b.body <= b.range * 0.1;
  const isDoji = (i) => rawDoji(B[i]) && B[i].body <= avgBody(i) * 0.35 && B[i].range >= avgRange(i) * 0.35;
  const dojiCrowded = (i) => { const s = B.slice(Math.max(0, i - 30), i); return s.length > 10 && s.filter(rawDoji).length / s.length > 0.2; };
  const isLong = (i) => B[i].body >= avgBody(i) * 1.2 && B[i].body >= B[i].range * 0.5;
  const isSmall = (i) => B[i].body <= avgBody(i) * 0.7;
  const isSpinning = (i) => !rawDoji(B[i]) && isSmall(i) && B[i].upper >= B[i].body && B[i].lower >= B[i].body;
  const atLow = (i, n = 20, tol = 0.25) => { const lo = lowestLow(i + 1, n), hi = highestHigh(i + 1, n); return B[i].c <= lo + (hi - lo) * tol; };
  const atHigh = (i, n = 20, tol = 0.25) => { const lo = lowestLow(i + 1, n), hi = highestHigh(i + 1, n); return B[i].c >= hi - (hi - lo) * tol; };
  return { B, e10, e20, e50, rsi14, avgBody, avgVol, avgRange, trendBefore, majorTrend, isDoji, dojiCrowded, isLong, isSmall, isSpinning, lowestLow, highestHigh, atLow, atHigh };
}

function swings(C, i, n, type) {
  const out = [];
  for (let j = Math.max(2, i - n); j <= i - 2; j++) {
    const b = C.B[j], B = C.B;
    if (type === 'low' && b.l < B[j - 1].l && b.l < B[j - 2].l && b.l <= B[j + 1].l && b.l <= B[j + 2].l) out.push(b.l);
    if (type === 'high' && b.h > B[j - 1].h && b.h > B[j - 2].h && b.h >= B[j + 1].h && b.h >= B[j + 2].h) out.push(b.h);
  }
  return out;
}

// ---------------------------------------------------------------- shapes
const hammerShape = (b, ratio = 2) => b.lower >= ratio * Math.max(b.body, b.range * 0.02) && b.upper <= Math.max(b.body * 0.6, b.range * 0.12) && b.body <= b.range * 0.4;
const shootingShape = (b, ratio = 2) => b.upper >= ratio * Math.max(b.body, b.range * 0.02) && b.lower <= Math.max(b.body * 0.6, b.range * 0.12) && b.body <= b.range * 0.4;
const shavenBlack = (b) => b.black && b.upper <= b.range * 0.1 && b.lower <= b.range * 0.1;
const closeTol = (a) => a.c * 0.0006;           // "no rigid rules": a few ticks of latitude
const tol = (a) => a.range * 0.05 + a.c * 0.0003;

// A detector returns false, true, or { confirm:true } when that instance needs the next candle.
const P = [];
const def = (name, dir, kind, trend, confirm, bars, fn, weight = 1, extra = {}) => P.push({ name, dir, kind, trend, confirm, bars, fn, weight, ...extra });

// --- Chapter 4
def('HAMMER', 'bull', 'reversal', 'down', false, 1, (C, i) => {
  const b = C.B[i]; if (!hammerShape(b) || b.l > C.lowestLow(i, 8) * 1.002) return false;
  const p = C.B[i - 1];
  return (shavenBlack(p) && C.isLong(i - 1)) ? { confirm: true } : true;   // Ex 4.10 hammer 4
}, 1.2);
def('HANGING MAN', 'bear', 'reversal', 'up', true, 1, (C, i) => hammerShape(C.B[i]) && C.B[i].top >= C.highestHigh(i, 8) * 0.997);
const engulf = (C, i, up) => {
  const a = C.B[i - 1], b = C.B[i], t = closeTol(a);
  if (!(up ? b.white : b.black) || b.top < a.top - t || b.bot > a.bot + t || b.body <= a.body) return false;
  return (up ? a.black : a.white) || a.body <= C.avgBody(i - 1) * 0.2;   // tiny first body: colour exception
};
def('BULLISH ENGULFING', 'bull', 'reversal', 'down', false, 2, (C, i) => engulf(C, i, true), 1.5);
def('BEARISH ENGULFING', 'bear', 'reversal', 'up', false, 2, (C, i) => engulf(C, i, false), 1.5);
def('DARK-CLOUD COVER', 'bear', 'reversal', 'up', false, 2, (C, i) => {
  const a = C.B[i - 1], b = C.B[i];
  if (!(a.white && b.black && C.isLong(i - 1) && b.o >= a.c - closeTol(a) && b.h >= a.h - tol(a) && b.c > a.o && b.c < a.c)) return false;
  return b.c < a.mid ? true : (b.c <= a.c - a.body * 0.3 ? { confirm: true } : false);  // more latitude than piercing
}, 1.3);
def('PIERCING PATTERN', 'bull', 'reversal', 'down', false, 2, (C, i) => {
  const a = C.B[i - 1], b = C.B[i];
  return a.black && C.isLong(i - 1) && b.white && b.o <= a.c + closeTol(a) && b.l <= a.l + tol(a) && b.c > a.mid && b.c < a.o;  // strict midpoint
}, 1.3);
const neck = (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.black && C.isLong(i - 1) && b.white && b.o <= a.c + closeTol(a); };
def('ON-NECK LINE', 'bear', 'continuation', 'down', false, 2, (C, i) => neck(C, i) && C.isSmall(i) && Math.abs(C.B[i].c - C.B[i - 1].l) <= tol(C.B[i - 1]));
def('IN-NECK LINE', 'bear', 'continuation', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return neck(C, i) && C.isSmall(i) && b.c >= a.c && b.c <= a.c + a.body * 0.15; });
def('THRUSTING LINE', 'bear', 'continuation', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return neck(C, i) && !C.isSmall(i) && b.c > a.c + a.body * 0.15 && b.c <= a.mid; });

// --- Chapter 5: stars
const starBelow = (a, s) => s.top <= a.c + closeTol(a);
const starAbove = (a, s) => s.bot >= a.c - closeTol(a);
const morning = (C, i, doji) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.black && C.isLong(i - 2) && (doji ? C.isDoji(i - 1) : (C.isSmall(i - 1) && !C.isDoji(i - 1))) && starBelow(a, s) && c.white && c.c > a.mid; };
const evening = (C, i, doji) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && C.isLong(i - 2) && (doji ? C.isDoji(i - 1) : (C.isSmall(i - 1) && !C.isDoji(i - 1))) && starAbove(a, s) && c.black && c.c < a.mid; };
def('MORNING STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => morning(C, i, false), 1.6);
def('EVENING STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => evening(C, i, false), 1.6);
def('MORNING DOJI STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => morning(C, i, true) && !C.dojiCrowded(i), 1.7);
def('EVENING DOJI STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => evening(C, i, true) && !C.dojiCrowded(i), 1.7);
def('BULLISH ABANDONED BABY', 'bull', 'reversal', 'down', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.black && C.isDoji(i - 1) && s.h < a.l && s.h < c.l && c.white; }, 1.8);
def('BEARISH ABANDONED BABY', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, s, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && C.isDoji(i - 1) && s.l > a.h && s.l > c.h && c.black; }, 1.8);
def('SHOOTING STAR', 'bear', 'reversal', 'up', true, 2, (C, i) => shootingShape(C.B[i]) && C.B[i].h >= C.highestHigh(i, 8) * 0.998);
def('INVERTED HAMMER', 'bull', 'reversal', 'down', true, 2, (C, i) => shootingShape(C.B[i]) && C.B[i].l <= C.lowestLow(i, 8) * 1.003);

// --- Chapter 6
const harami = (C, i) => { const a = C.B[i - 1], b = C.B[i]; return C.isLong(i - 1) && b.top <= a.top && b.bot >= a.bot && b.body <= a.body * 0.5; };
def('BULLISH HARAMI', 'bull', 'reversal', 'down', true, 2, (C, i) => C.B[i - 1].black && !C.isDoji(i) && harami(C, i), 0.9, { confirmAbove: 'mother' });
def('BEARISH HARAMI', 'bear', 'reversal', 'up', true, 2, (C, i) => C.B[i - 1].white && !C.isDoji(i) && harami(C, i), 0.9);
def('BULLISH HARAMI CROSS', 'bull', 'reversal', 'down', true, 2, (C, i) => C.B[i - 1].black && C.isDoji(i) && harami(C, i) && !C.dojiCrowded(i), 1.2, { confirmAbove: 'mother' });
def('BEARISH HARAMI CROSS', 'bear', 'reversal', 'up', false, 2, (C, i) => C.B[i - 1].white && C.isDoji(i) && harami(C, i) && !C.dojiCrowded(i), 1.3);  // "better at tops"
// tweezers count only with another candlestick signal on the second line
const twzBot = (C, i) => { const a = C.B[i - 1], b = C.B[i]; if (Math.abs(a.l - b.l) > a.l * 0.0008) return false; return hammerShape(b, 1.5) || (a.black && C.isLong(i - 1) && b.white && b.c > a.mid) || (a.black && C.isLong(i - 1) && C.isDoji(i)) || engulf(C, i, true); };
const twzTop = (C, i) => { const a = C.B[i - 1], b = C.B[i]; if (Math.abs(a.h - b.h) > a.h * 0.0008) return false; return hammerShape(b, 1.5) || shootingShape(b, 1.5) || (a.white && C.isLong(i - 1) && C.isDoji(i)) || (a.white && C.isLong(i - 1) && b.black && b.c < a.mid) || engulf(C, i, false); };
def('TWEEZERS BOTTOM', 'bull', 'reversal', 'down', false, 2, twzBot, 1.1);
def('TWEEZERS TOP', 'bear', 'reversal', 'up', false, 2, twzTop, 1.1);
def('BULLISH BELT-HOLD', 'bull', 'reversal', 'down', false, 1, (C, i) => { const b = C.B[i]; return b.white && C.isLong(i) && b.body >= C.avgBody(i) * 1.5 && b.lower <= b.range * 0.05 && C.atLow(i - 1, 20, 0.3); });
def('BEARISH BELT-HOLD', 'bear', 'reversal', 'up', false, 1, (C, i) => { const b = C.B[i]; return b.black && C.isLong(i) && b.body >= C.avgBody(i) * 1.5 && b.upper <= b.range * 0.05 && C.atHigh(i - 1, 20, 0.3); });
def('UPSIDE-GAP TWO CROWS', 'bear', 'reversal', 'up', false, 3, (C, i) => { const [a, x, y] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && C.isLong(i - 2) && x.black && x.bot >= a.c - closeTol(a) && y.black && y.o >= x.o && y.c <= x.c; }, 1.3);
const crows = (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return [a, b, c].every((k) => k.black && k.body >= k.range * 0.55 && k.lower <= k.range * 0.2) && b.c < a.c && c.c < b.c && b.o <= a.o + closeTol(a) && b.o >= a.c - closeTol(a) && c.o <= b.o + closeTol(b) && c.o >= b.c - closeTol(b); };
def('THREE BLACK CROWS', 'bear', 'reversal', 'up', false, 3, crows, 1.6);
const soldiers = (a, b, c) => [a, b, c].every((k) => k.white && k.body >= k.range * 0.55) && b.c > a.c && c.c > b.c && b.o >= a.o && b.o <= a.c * 1.0006 && c.o >= b.o && c.o <= b.c * 1.0006;
def('THREE WHITE SOLDIERS', 'bull', 'reversal', null, false, 3, (C, i) => {
  const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]];
  if (!soldiers(a, b, c) || ![a, b, c].every((k) => k.upper <= k.range * 0.25)) return false;
  return C.atLow(i - 3, 20, 0.4) && (c.c - a.o) <= C.avgRange(i - 2) * 6;     // from a low area, not overextended
}, 1.5);
def('ADVANCE BLOCK', 'bear', 'warning', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return [a, b, c].every((k) => k.white) && b.c > a.c && c.c > b.c && ((b.body < a.body && c.body < b.body) || (c.upper >= c.body && b.upper >= b.body * 0.5)); });
def('STALLED PATTERN', 'bear', 'warning', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.white && b.white && c.white && C.isLong(i - 2) && C.isLong(i - 1) && b.c > a.c && C.isSmall(i) && c.bot >= b.c - b.body * 0.25; });
def('BULLISH COUNTERATTACK', 'bull', 'reversal', 'down', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.black && C.isLong(i - 1) && b.white && C.isLong(i) && b.o < a.c - a.body * 0.3 && Math.abs(b.c - a.c) <= a.body * 0.1; });
def('BEARISH COUNTERATTACK', 'bear', 'reversal', 'up', true, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.white && C.isLong(i - 1) && b.black && C.isLong(i) && b.o > a.c + a.body * 0.3 && Math.abs(b.c - a.c) <= a.body * 0.1; });
def('TOWER BOTTOM', 'bull', 'reversal', 'down', false, 5, (C, i) => { const a = C.B[i - 4], e = C.B[i]; return a.black && C.isLong(i - 4) && [1, 2, 3].every((k) => C.isSmall(i - k)) && e.white && C.isLong(i) && e.c > a.mid; }, 1.3);
def('TOWER TOP', 'bear', 'reversal', 'up', false, 5, (C, i) => { const a = C.B[i - 4], e = C.B[i]; return a.white && C.isLong(i - 4) && [1, 2, 3].every((k) => C.isSmall(i - k)) && e.black && C.isLong(i) && e.c < a.mid; }, 1.3);
def('UNIQUE THREE RIVER BOTTOM', 'bull', 'reversal', 'down', true, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return a.black && C.isLong(i - 2) && b.black && b.c > a.c && b.l <= a.l && c.white && C.isSmall(i) && c.top <= b.bot + b.body; }, 1.1);
// three tests of a low/high; a close beyond the intervening peak/trough confirms
const threeBuddha = (C, i, low) => {
  if (i < 42) return false;
  const pts = swings(C, i, 40, low ? 'low' : 'high').slice(-3); if (pts.length < 3) return false;
  const lo = Math.min(...pts), hi = Math.max(...pts), avgr = C.avgRange(i);
  if (hi - lo > avgr * 2.2) return false;
  const last10 = C.B.slice(i - 10, i);
  return low ? (C.B[i].white && C.B[i].c > Math.max(...last10.map((b) => b.h))) : (C.B[i].black && C.B[i].c < Math.min(...last10.map((b) => b.l)));
};
def('THREE RIVERS BOTTOM', 'bull', 'reversal', null, false, 1, (C, i) => threeBuddha(C, i, true), 1.5);
def('THREE MOUNTAINS TOP', 'bear', 'reversal', null, false, 1, (C, i) => threeBuddha(C, i, false), 1.5);
// rounded base / top of small bodies completed by a window
const rounded = (C, i, base) => { if (i < 14) return false; const s = C.B.slice(i - 12, i); const smallShare = s.filter((_, k) => C.isSmall(i - 12 + k)).length / 12; if (smallShare < 0.6) return false;
  const closes = s.map((b) => b.c), m = closes.slice(4, 8).reduce((x, y) => x + y) / 4, ends = (closes[0] + closes[11]) / 2;
  return base ? (m < ends && C.B[i].l > s[11].h) : (m > ends && C.B[i].h < s[11].l); };
def('FRY PAN BOTTOM', 'bull', 'reversal', null, false, 1, (C, i) => rounded(C, i, true), 1.4);
def('DUMPLING TOP', 'bear', 'reversal', null, false, 1, (C, i) => rounded(C, i, false), 1.4);

// --- Chapter 7: continuation
def('RISING WINDOW', 'bull', 'continuation', 'up', false, 2, (C, i) => C.B[i].l > C.B[i - 1].h);
def('FALLING WINDOW', 'bear', 'continuation', 'down', false, 2, (C, i) => C.B[i].h < C.B[i - 1].l);
def('UPWARD-GAP TASUKI', 'bull', 'continuation', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return b.white && b.l > a.h && c.black && c.o > b.bot && c.o < b.top && c.c < b.bot && c.c > a.h && Math.abs(c.body - b.body) <= b.body * 0.5; });
def('DOWNWARD-GAP TASUKI', 'bear', 'continuation', 'down', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return b.black && b.h < a.l && c.white && c.o < b.top && c.o > b.bot && c.c > b.top && c.c < a.l && Math.abs(c.body - b.body) <= b.body * 0.5; });
const gapPlay = (C, i, up) => { for (let n = 3; n <= 11; n++) { if (i - n - 1 < 0) break; const lead = C.B[i - n - 1], s = C.B.slice(i - n, i); if (!C.isLong(i - n - 1) || !(up ? lead.white : lead.black)) continue; if (!s.every((_, k) => C.isSmall(i - n + k))) continue; const hi = Math.max(...s.map((b) => b.h)), lo = Math.min(...s.map((b) => b.l)); if (up && lo >= lead.mid && C.B[i].l > hi) return true; if (!up && hi <= lead.mid && C.B[i].h < lo) return true; } return false; };
def('HIGH-PRICE GAPPING PLAY', 'bull', 'continuation', 'up', false, 1, (C, i) => gapPlay(C, i, true), 1.2);
def('LOW-PRICE GAPPING PLAY', 'bear', 'continuation', 'down', false, 1, (C, i) => gapPlay(C, i, false), 1.2);
def('GAPPING SIDE-BY-SIDE WHITE LINES', 'bull', 'continuation', 'up', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return b.white && c.white && b.l > a.h && Math.abs(c.o - b.o) <= b.body * 0.2 && Math.abs(c.body - b.body) <= b.body * 0.3; });
def('DOWNGAP SIDE-BY-SIDE WHITE LINES', 'bear', 'continuation', 'down', false, 3, (C, i) => { const [a, b, c] = [C.B[i - 2], C.B[i - 1], C.B[i]]; return b.white && c.white && b.h < a.l && Math.abs(c.o - b.o) <= b.body * 0.2; });
const threeMethods = (C, i, n, up) => { const first = C.B[i - n - 1], last = C.B[i]; if (!C.isLong(i - n - 1) || !C.isLong(i)) return false;
  if (up ? !(first.white && last.white && last.c > first.c && last.o >= C.B[i - 1].c - closeTol(C.B[i - 1])) : !(first.black && last.black && last.c < first.c && last.o <= C.B[i - 1].c + closeTol(C.B[i - 1]))) return false;
  for (let k = 1; k <= n; k++) { const m = C.B[i - k]; if (!C.isSmall(i - k) || m.top > first.h || m.bot < first.l) return false; } return true; };
def('RISING THREE METHODS', 'bull', 'continuation', 'up', false, 5, (C, i) => [2, 3, 4].some((n) => i - n - 1 >= 0 && threeMethods(C, i, n, true)), 1.5);
def('FALLING THREE METHODS', 'bear', 'continuation', 'down', false, 5, (C, i) => [2, 3, 4].some((n) => i - n - 1 >= 0 && threeMethods(C, i, n, false)), 1.5);
def('MAT-HOLD', 'bull', 'continuation', 'up', false, 5, (C, i) => [2, 3, 4].some((n) => { if (i - n - 1 < 0) return false; const a = C.B[i - n - 1], s = C.B.slice(i - n, i), e = C.B[i]; return a.white && C.isLong(i - n - 1) && s[0].black && s[0].bot >= a.c - closeTol(a) && s.every((m) => m.black) && e.white && e.c > Math.max(...s.map((m) => m.h)); }), 1.5);
def('BULLISH SEPARATING LINE', 'bull', 'continuation', 'up', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.black && b.white && C.isLong(i) && Math.abs(b.o - a.o) <= a.range * 0.1 && b.lower <= b.range * 0.1; });
def('BEARISH SEPARATING LINE', 'bear', 'continuation', 'down', false, 2, (C, i) => { const a = C.B[i - 1], b = C.B[i]; return a.white && b.black && C.isLong(i) && Math.abs(b.o - a.o) <= a.range * 0.1 && b.upper <= b.range * 0.1; });

// --- Chapter 8: doji
def('DOJI AFTER LONG WHITE', 'bear', 'warning', 'up', true, 2, (C, i) => C.isDoji(i) && C.isLong(i - 1) && C.B[i - 1].white && !C.dojiCrowded(i), 1.2);
def('DOJI AFTER LONG BLACK', 'bull', 'warning', 'down', true, 2, (C, i) => C.isDoji(i) && C.isLong(i - 1) && C.B[i - 1].black && !C.dojiCrowded(i), 0.7);
def('GRAVESTONE DOJI', 'bear', 'reversal', 'up', false, 1, (C, i) => { const b = C.B[i]; return C.isDoji(i) && !C.dojiCrowded(i) && b.upper >= b.range * 0.7 && b.lower <= b.range * 0.1; }, 1.2);
def('DRAGONFLY DOJI', 'bull', 'reversal', 'down', true, 1, (C, i) => { const b = C.B[i]; return C.isDoji(i) && !C.dojiCrowded(i) && b.lower >= b.range * 0.7 && b.upper <= b.range * 0.1; }, 0.8);
def('LONG-LEGGED DOJI', 'bear', 'warning', 'up', false, 1, (C, i) => { const b = C.B[i]; return C.isDoji(i) && !C.dojiCrowded(i) && b.upper >= b.range * 0.3 && b.lower >= b.range * 0.3 && b.range >= C.avgRange(i) * 1.5; });
def('BULLISH TRI-STAR', 'bull', 'reversal', 'down', false, 3, (C, i) => [0, 1, 2].every((k) => C.isDoji(i - k)) && C.B[i - 1].top < Math.min(C.B[i - 2].bot, C.B[i].bot), 1.5);
def('BEARISH TRI-STAR', 'bear', 'reversal', 'up', false, 3, (C, i) => [0, 1, 2].every((k) => C.isDoji(i - k)) && C.B[i - 1].bot > Math.max(C.B[i - 2].top, C.B[i].top), 1.5);
def('SPINNING TOP', 'neutral', 'warning', null, false, 1, (C, i) => C.isSpinning(i));
def('HIGH-WAVE CANDLE', 'neutral', 'warning', null, false, 1, (C, i) => { const b = C.B[i]; return C.isSmall(i) && b.upper >= b.range * 0.35 && b.lower >= b.range * 0.35 && b.range >= C.avgRange(i) * 1.5; });

// --- Chapter 11: springs and upthrusts of a lateral band
const band = (C, i) => { if (i < 34) return null; const s = C.B.slice(i - 30, i - 1); const hi = Math.max(...s.map((b) => b.h)), lo = Math.min(...s.map((b) => b.l)); return (hi - lo) <= C.avgRange(i) * 12 ? { hi, lo } : null; };
def('SPRING', 'bull', 'reversal', null, false, 1, (C, i) => { const r = band(C, i), b = C.B[i]; return !!r && b.l < r.lo && b.c > r.lo && b.c >= b.l + b.range * 0.5; }, 1.6);
def('UPTHRUST', 'bear', 'reversal', null, false, 1, (C, i) => { const r = band(C, i), b = C.B[i]; return !!r && b.h > r.hi && b.c < r.hi && b.c <= b.l + b.range * 0.5; }, 1.6);

const CATALOG = P.map(({ name, dir, kind, trend, confirm }) => ({ name, dir, kind, trend, confirm }));

// ---------------------------------------------------------------- Rule of Multiple Techniques
function convergence(C, i, dir) {
  const notes = [], b = C.B[i], rv = C.avgVol(i) > 0 ? b.v / C.avgVol(i) : 1, r = C.rsi14[i];
  const near = Math.max(b.c * 0.002, C.avgRange(i) * 0.4), n = Math.min(80, i - 3);
  if (rv >= 1.3) notes.push(`volume ${rv.toFixed(1)}x`);
  if (dir === 'bull' && r <= 35) notes.push(`RSI ${r.toFixed(0)} oversold`);
  if (dir === 'bear' && r >= 65) notes.push(`RSI ${r.toFixed(0)} overbought`);
  if (n > 10) {
    const lows = swings(C, i - 1, n, 'low'), highs = swings(C, i - 1, n, 'high');
    if (dir === 'bull') {
      if (lows.some((x) => Math.abs(b.l - x) <= near)) { notes.push('at prior support'); if (rv <= 0.7) notes.push('light-volume test of support'); }
      else if (highs.some((x) => Math.abs(b.l - x) <= near && b.c > x)) notes.push('change of polarity (old resistance as support)');
      if ((b.l <= C.e20[i] + near && b.c > C.e20[i]) || (b.l <= C.e50[i] + near && b.c > C.e50[i])) notes.push('held moving average');
      const s = C.B.slice(i - n, i + 1); let hiIdx = 0; s.forEach((x, k) => { if (x.h > s[hiIdx].h) hiIdx = k; });
      if (hiIdx > 3) { const hi = s[hiIdx].h, lo = Math.min(...s.slice(0, hiIdx).map((x) => x.l)), leg = hi - lo;
        for (const f of [0.382, 0.5, 0.618]) if (leg > 0 && hiIdx < s.length - 1 && Math.abs(b.l - (hi - leg * f)) <= near) { notes.push(`${Math.round(f * 100)}% retracement`); break; } }
    } else if (dir === 'bear') {
      if (highs.some((x) => Math.abs(b.h - x) <= near)) notes.push('at prior resistance');
      else if (lows.some((x) => Math.abs(b.h - x) <= near && b.c < x)) notes.push('change of polarity (old support as resistance)');
      if (b.h >= C.e50[i] - near && b.c < C.e50[i]) notes.push('failed at moving average');
    }
  }
  return { notes, rvol: rv };
}

// "the stomach is 80% full": 8+ record highs without a meaningful correction
function recordSessions(C, i) {
  let n = 0, miss = 0;
  for (let j = i; j > Math.max(0, i - 16) && miss < 2; j--) { if (C.B[j].h > C.B[j - 1].h) { n++; miss = 0; } else miss++; }
  return n;
}
function risingWindows(C, i) { let n = 0; for (let j = i; j > Math.max(0, i - 25); j--) { if (C.B[j].l > C.B[j - 1].h) n++; if (C.B[j].c < C.e50[j]) break; } return n; }

// ---------------------------------------------------------------- scan
function scan(klines) {
  const B = parse(Array.isArray(klines) ? klines : []);
  const empty = { patterns: [], bull: [], bear: [], warnings: [], trend: 'none', major: 'flat', ctx: null };
  if (B.length < 12) return empty;
  const C = makeCtx(B), n = B.length - 1, found = [];
  const test = (p, i) => {
    if (i - p.bars < 1) return false;
    if (p.trend && C.trendBefore(i - p.bars + 1) !== p.trend) return false;
    try { return p.fn(C, i) || false; } catch (e) { return false; }
  };
  const low = (i, bars) => Math.min(...B.slice(i - bars + 1, i + 1).map((b) => b.l));
  const high = (i, bars) => Math.max(...B.slice(i - bars + 1, i + 1).map((b) => b.h));
  for (const p of P) {
    const now = test(p, n);
    if (now && !(p.confirm || now.confirm)) { found.push({ p, at: n, status: 'complete' }); continue; }
    if (now) { found.push({ p, at: n, status: 'awaiting confirmation' }); continue; }
    // confirmation window: the next one or two candles
    let settled = false;
    for (const back of [1, 2]) {
      if (settled || n - back < 1) continue;
      const hit = test(p, n - back);
      if (!hit || !(p.confirm || hit.confirm)) continue;
      settled = true;
      const sig = B[n - back], after = B.slice(n - back + 1, n + 1), last = B[n];
      const lvl = p.confirmAbove === 'mother' ? Math.max(B[n - back - 1].h, sig.h) : sig.top;
      if (p.dir === 'bull') {
        if (after.some((x) => x.c < low(n - back, p.bars))) continue;            // pattern voided
        if (last.white && last.c > lvl) found.push({ p, at: n - back, status: 'confirmed' });
        else if (back === 1) found.push({ p, at: n - back, status: 'awaiting confirmation' });
      } else if (p.dir === 'bear') {
        if (last.black && last.c < sig.bot) found.push({ p, at: n - back, status: 'confirmed' });
        else if (back === 1) found.push({ p, at: n - back, status: 'awaiting confirmation' });
      }
    }
  }
  const out = found.map(({ p, at, status }) => {
    const conv = convergence(C, at, p.dir);
    const strength = Math.min(99, Math.round(55 + 10 * p.weight + 6 * conv.notes.length + (status === 'confirmed' ? 6 : 0)));
    return { name: p.name, dir: p.dir, kind: p.kind, status, strength, convergence: conv.notes, rvol: Number(conv.rvol.toFixed(2)), barsAgo: n - at, patternLow: low(at, p.bars), patternHigh: high(at, p.bars) };
  }).sort((a, b) => b.strength - a.strength);
  const actionable = (f) => f.status !== 'awaiting confirmation';
  return {
    patterns: out,
    bull: out.filter((f) => f.dir === 'bull' && actionable(f) && f.kind !== 'warning'),
    bear: out.filter((f) => f.dir === 'bear' && actionable(f) && f.kind !== 'warning'),
    warnings: out.filter((f) => f.kind === 'warning' || f.status === 'awaiting confirmation'),
    trend: C.trendBefore(n + 1), major: C.majorTrend(n), ctx: C
  };
}

// The hard gates are: completed/confirmed pattern, no bearish signal, major trend not down.
// Convergence, room to resistance, record sessions and windows are reported and scored. Replay
// over 39 coins / 180 days showed that making them vetoes removed trades without improving results.
const ASSESS_DEFAULTS = { minConvergence: 0, minRewardRisk: 0, recordLimit: Infinity, windowLimit: Infinity };

/** Major trend from a higher timeframe's closed klines (needs 55+ bars). */
function majorTrendOf(klines) {
  const B = parse(Array.isArray(klines) ? klines : []);
  if (B.length < 55) return 'flat';
  return makeCtx(B).majorTrend(B.length - 1);
}

/**
 * Nison's checklist for a LONG, on closed candles.
 * opts.major overrides the trend read (pass the higher timeframe's 'up'|'flat'|'down').
 * Returns { buy, bearish, pattern, score, stop, target, rewardRisk, vetoes, ... }.
 */
function assess(klines, opts = {}) {
  const cfg = { ...ASSESS_DEFAULTS, ...opts };
  const r = scan(klines);
  const res = { buy: false, bearish: r.bear.length > 0, pattern: 'NONE', bearPattern: r.bear[0] ? r.bear[0].name : 'NONE', status: '', score: 0,
    convergence: [], stop: 0, target: 0, rewardRisk: 0, rvol: 1, vetoes: [], major: cfg.major || r.major, trend: r.trend, patterns: r.patterns };
  if (!r.ctx) return res;
  const C = r.ctx, n = C.B.length - 1, last = C.B[n];
  if (res.bearish) res.vetoes.push(`bearish ${r.bear[0].name}`);
  if (res.major === 'down') res.vetoes.push('major trend down');
  const rec = recordSessions(C, n); if (rec >= cfg.recordLimit) res.vetoes.push(`${rec} record highs (stomach 80% full)`);
  const win = risingWindows(C, n); if (win >= cfg.windowLimit) res.vetoes.push('three windows');
  const best = r.bull.filter((f) => f.barsAgo <= 1)[0] || r.bull[0];
  if (!best) { res.vetoes.push('no completed bullish pattern'); return res; }
  res.pattern = best.name; res.status = best.status; res.score = best.strength; res.convergence = best.convergence; res.rvol = best.rvol; res.stop = best.patternLow;
  const needConv = best.kind === 'continuation' ? 0 : cfg.minConvergence;
  if (best.convergence.length < needConv && best.name !== 'SPRING') res.vetoes.push('no second technique agrees');
  // trade location: risk = pattern low, objective = nearest resistance above
  const m = Math.min(100, n - 3), levels = m > 10 ? swings(C, n, m, 'high').filter((x) => x > last.c) : [];
  const windowHigh = Math.max(...C.B.slice(Math.max(0, n - m), n + 1).map((b) => b.h));
  const target = levels.length ? Math.min(...levels) : (windowHigh > last.c ? windowHigh : last.c + C.avgRange(n) * 4);
  const risk = Math.max(last.c - best.patternLow, last.c * 0.0005);
  res.target = target; res.rewardRisk = Number(((target - last.c) / risk).toFixed(2));
  if (res.rewardRisk < cfg.minRewardRisk) res.vetoes.push(`resistance too close (R/R ${res.rewardRisk})`);
  res.buy = res.vetoes.length === 0;
  return res;
}

/**
 * The same checklist for a SHORT: completed/confirmed bearish pattern, no bullish signal,
 * major trend not up. Second techniques are reported and scored, as they are for longs.
 */
function assessShort(klines, opts = {}) {
  const r = scan(klines);
  const res = { sell: false, bullish: r.bull.length > 0, pattern: 'NONE', bullPattern: r.bull[0] ? r.bull[0].name : 'NONE', status: '', score: 0,
    convergence: [], stop: 0, rvol: 1, vetoes: [], major: opts.major || r.major, trend: r.trend, patterns: r.patterns };
  if (!r.ctx) return res;
  if (res.bullish) res.vetoes.push(`bullish ${r.bull[0].name}`);
  if (res.major === 'up') res.vetoes.push('major trend up');
  const best = r.bear.filter((f) => f.barsAgo <= 1)[0] || r.bear[0];
  if (!best) { res.vetoes.push('no completed bearish pattern'); return res; }
  res.pattern = best.name; res.status = best.status; res.score = best.strength; res.convergence = best.convergence; res.rvol = best.rvol; res.stop = best.patternHigh;
  res.sell = res.vetoes.length === 0;
  return res;
}

module.exports = { scan, assess, assessShort, majorTrendOf, parse, CATALOG, ASSESS_DEFAULTS };
