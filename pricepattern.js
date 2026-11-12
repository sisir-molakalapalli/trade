'use strict';
// =====================================================================================================
// PRICE ACTION AND PATTERN ENTRIES
// From Young Ho Seo, "Scientific Guide to Price Action and Pattern Trading" (2017, v21.1).
// Each rule names the part and chapter of the book it comes from.
//
// Everything runs on CLOSED candles and is causal: a signal on bar i uses bars 0..i only, so the back
// test and the live scanner see exactly the same thing. Candles are Binance kline arrays
// [openTime, open, high, low, close, ...] (strings or numbers). Peaks and troughs use bar highs and
// lows, never open / close (Part 2 ch 4.2).
//
// Families
//   HARMONIC  Gartley and AB=CD (Part 2 ch 4, X3 tables Part 3)       reversal at point D
//   ELLIOTT   .12345 impulse with an extended wave 3 (Part 2 ch 5, X3)  reversal after wave 5
//   SR        horizontal / diagonal support and resistance (Part 2 ch 2, triangles and wedges ch 6)
//   EFW       shape-ratio (Fibonacci retracement) trading (Part 2 ch 1 and ch 3)
//   CHANNEL   Equilibrium Fractal Wave channel (Part 2 ch 3)
// =====================================================================================================

const RULES = Object.freeze({
  atrPeriod: 14,
  zigzagAtr: 3,          // Part 1 ch 12: ZigZag peaks / troughs; a point is confirmed once price reverses this many ATR(14)
  trigger: 0.15,         // Part 2 ch 2: enter when price moves this share of the last triangle's height off / through the line (5-20%)
  minAccuracy: 80,       // Part 2 ch 4.3 and ch 5: pattern matching accuracy = 100 - MAPE; trade only >= 80%
  pci: 0.10,             // Part 2 ch 4.3: Pattern Completion Interval = ideal ratio of the last point +-10% (book figure 4-16)
  harmonicEntry: 'PCI',  // 'PCI': enter while price is inside the interval (ch 4.3); 'TRIGGER': wait for a turn of `trigger` x last leg
  rewardRisk: 3,         // Part 2 ch 4.3 and the risk chapter: take profit = 3 x the stop distance
  touchTol: 0.10,        // Part 2 ch 2: a touch is within 10% of the average triangle height from the line
  minTouchGap: 3,        // Part 2 ch 2: touches too close together count as one (bars)
  efwTol: 0.10,          // Part 1 ch 13: a triangle has a shape ratio when it is within +-10% of it
  efwMinIndex: 0.10,     // Part 1 ch 13: EFW index under 10% for the Fibonacci ratios -> no Fibonacci / harmonic / Elliott / EFW trades
  efwWindow: 1000,       // the EFW index counts the peaks / troughs made in the last 1000 bars (the live scanner loads 1500, 500 to warm up)
  efwRatios: [0.382, 0.5, 0.618, 0.786, 0.85],   // candidate shape ratios; the one with the highest EFW index is traded (ch 3.2)
  efwGateRatios: [0.382, 0.5, 0.618],
  efwMinPivots: 12,
  channelTrigger: 0.20,  // Part 2 ch 3.4: EFW channel trigger = 20% of the channel height
  minBars: 300,
  priority: ['HARMONIC', 'ELLIOTT', 'SR', 'EFW', 'CHANNEL'],
});
// On by default: the families that beat candle patterns on risk in the 180-day test (CHANGES.md v6.4).
// EFW and CHANNEL are complete and tested but off: they added trades with a lower profit per trade and a worse worst case.
const FAMILIES = Object.freeze({ HARMONIC: true, ELLIOTT: true, SR: true, EFW: false, CHANNEL: false });

// ---------- fixed pattern structures (Part 3 X3 notation: "trade only patterns with a fixed structure") ----------
// Ratio spec: { v } one ideal value, { range: [lo, hi] } ideal range, { any: [a, b] } either value.
// A leg is [from point, to point]; a ratio is leg / base leg. `anchor` is the ratio that places the last point:
// the Pattern Completion Interval is built on it.
const PATTERNS = Object.freeze([
  { family: 'HARMONIC', name: 'Gartley', points: 5, ratios: [   // Part 2 ch 4.3 (figure 4-10, worked MAPE example)
    { id: 'AB/XA', leg: [1, 2], base: [0, 1], v: 0.618 },
    { id: 'BC/AB', leg: [2, 3], base: [1, 2], any: [0.618, 0.786] },
    { id: 'CD/BC', leg: [3, 4], base: [2, 3], any: [1.272, 1.618] },
    { id: 'AD/XA', leg: [1, 4], base: [0, 1], v: 0.786, anchor: true },
  ] },
  { family: 'HARMONIC', name: 'AB=CD', points: 4, ratios: [     // Part 3: "AB=CD with range", N2, R1 0.618, R0 1.272-1.618
    { id: 'BC/AB', leg: [1, 2], base: [0, 1], v: 0.618 },
    { id: 'CD/BC', leg: [2, 3], base: [1, 2], range: [1.272, 1.618], anchor: true },
  ] },
  { family: 'ELLIOTT', name: 'Elliott .12345', points: 6, ratios: [   // Part 3: N4, R3 0.618, R2 1.618-3.0, R1 0.382, R0 1.618-2.618, F0 1.0
    { id: 'W2/W1', leg: [1, 2], base: [0, 1], v: 0.618 },
    { id: 'W3/W2', leg: [2, 3], base: [1, 2], range: [1.618, 3.0] },
    { id: 'W4/W3', leg: [3, 4], base: [2, 3], v: 0.382 },
    { id: 'W5/W4', leg: [4, 5], base: [3, 4], range: [1.618, 2.618] },
    { id: 'W5/W1', leg: [4, 5], base: [0, 1], v: 1.0, anchor: true },
  ] },
]);

const num = (x) => +x;

// Wilder ATR
function atrSeries(H, L, C, n) {
  const a = new Float64Array(H.length).fill(NaN);
  let s = 0;
  for (let i = 1; i < H.length; i++) {
    const tr = Math.max(H[i] - L[i], Math.abs(H[i] - C[i - 1]), Math.abs(L[i] - C[i - 1]));
    if (i <= n) { s += tr; if (i === n) a[i] = s / n; } else a[i] = (a[i - 1] * (n - 1) + tr) / n;
  }
  return a;
}

// Part 1 ch 12 / Part 2 ch 4.2: ZigZag. Peaks and troughs alternate. To stay causal the last leg is never
// repainted: a peak is confirmed when price falls mult x ATR below the highest high of the up leg (and the
// mirror for troughs). Until then the leg's running extreme is the TENTATIVE point.
function makeZigzag(H, L, atr, mult) {
  const pts = [];
  let dir = 0, ext = -1, hi = -1, lo = -1;
  const confirm = (j, type, i) => {
    const pv = { i: j, p: type > 0 ? H[j] : L[j], type, at: i };
    pts.push(pv);
    dir = -type; ext = j + 1;
    for (let q = j + 2; q <= i; q++) if (dir < 0 ? L[q] < L[ext] : H[q] > H[ext]) ext = q;
    return pv;
  };
  const step = (i) => {
    if (!(atr[i] > 0)) return null;
    const thr = mult * atr[i];
    if (dir === 0) {
      if (hi < 0 || H[i] > H[hi]) hi = i;
      if (lo < 0 || L[i] < L[lo]) lo = i;
      if (hi < i && H[hi] - L[i] >= thr) return confirm(hi, 1, i);
      if (lo < i && H[i] - L[lo] >= thr) return confirm(lo, -1, i);
      return null;
    }
    if (dir > 0) { if (H[i] > H[ext]) { ext = i; return null; } if (H[ext] - L[i] >= thr) return confirm(ext, 1, i); }
    else { if (L[i] < L[ext]) { ext = i; return null; } if (H[i] - L[ext] >= thr) return confirm(ext, -1, i); }
    return null;
  };
  const tentative = () => (dir === 0 || ext < 0 ? null : { i: ext, p: dir > 0 ? H[ext] : L[ext], type: dir });
  return { pts, step, tentative };
}

function zigzag(k, mult = RULES.zigzagAtr, n = RULES.atrPeriod) {
  const H = k.map((b) => num(b[2])), L = k.map((b) => num(b[3])), C = k.map((b) => num(b[4]));
  const zz = makeZigzag(H, L, atrSeries(H, L, C, n), mult);
  for (let i = 1; i < k.length; i++) zz.step(i);
  return { pivots: zz.pts, tentative: zz.tentative() };
}

// Part 2 ch 4.3: error of one measured ratio against its ideal (percent error; 0 inside an ideal range)
function ratioError(r, spec) {
  if (spec.any) return Math.min(...spec.any.map((v) => Math.abs(r - v) / v));
  if (spec.range) { const [lo, hi] = spec.range; return r < lo ? (lo - r) / lo : r > hi ? (r - hi) / hi : 0; }
  return Math.abs(r - spec.v) / spec.v;
}
// Pattern matching accuracy = 100 - MAPE
function accuracy(ratios, specs) {
  let e = 0; for (let j = 0; j < specs.length; j++) e += ratioError(ratios[j], specs[j]);
  return 100 - (100 * e) / specs.length;
}
// Pattern Completion Interval in ratio units: the ideal ratio(s) of the last point +-tol
function completionBand(spec, r, tol) {
  if (spec.any) { const v = spec.any.reduce((b, x) => (Math.abs(r - x) < Math.abs(r - b) ? x : b)); return [v * (1 - tol), v * (1 + tol)]; }
  if (spec.range) return [spec.range[0] * (1 - tol), spec.range[1] * (1 + tol)];
  return [spec.v * (1 - tol), spec.v * (1 + tol)];
}

// Part 1 ch 13: EFW index = triangles with the shape ratio / number of peaks and troughs.
// Shape ratio of a triangle = current move / previous move (Y2 / Y1).
function efwIndex(pivots, ratios = RULES.efwRatios, tol = RULES.efwTol) {
  const out = {}; ratios.forEach((r) => { out[r] = 0; });
  if (pivots.length < 3) return out;
  for (let j = 2; j < pivots.length; j++) {
    const y1 = Math.abs(pivots[j - 1].p - pivots[j - 2].p), y2 = Math.abs(pivots[j].p - pivots[j - 1].p);
    if (!(y1 > 0)) continue;
    const sr = y2 / y1;
    for (const r of ratios) if (Math.abs(sr - r) <= tol * r) out[r]++;
  }
  for (const r of ratios) out[r] /= pivots.length;
  return out;
}

// -----------------------------------------------------------------------------------------------------
// scan(k, opts): every signal, bar by bar. opts.from = first bar to report (earlier bars are still
// evaluated so a pattern that already fired is not fired again).
// -----------------------------------------------------------------------------------------------------
function scan(k, opts = {}) {
  const R = { ...RULES, ...opts }, F = { ...FAMILIES, ...(opts.families || {}) };
  const n = k.length, out = [];
  if (n < R.minBars) return out;
  const T = k.map((b) => num(b[0])), H = k.map((b) => num(b[2])), L = k.map((b) => num(b[3])), C = k.map((b) => num(b[4]));
  const atr = atrSeries(H, L, C, R.atrPeriod);
  const zz = makeZigzag(H, L, atr, R.zigzagAtr), P = zz.pts;
  const fired = new Set(), lineOk = new Map();
  const from = Number.isFinite(opts.from) ? opts.from : 0;
  let w0 = 0, efwKey = '', efw = null;

  const efwAt = (i) => {                       // EFW index over the peaks / troughs made in the last efwWindow bars
    while (w0 < P.length && P[w0].i <= i - R.efwWindow) w0++;
    const key = `${w0}|${P.length}`;
    if (key !== efwKey) {
      efwKey = key;
      const win = P.slice(w0);
      const idx = efwIndex(win, [...new Set([...R.efwRatios, ...R.efwGateRatios])], R.efwTol);
      let best = R.efwRatios[0]; for (const r of R.efwRatios) if (idx[r] > idx[best]) best = r;
      efw = { pivots: win.length, idx, best, gate: win.length >= R.efwMinPivots && Math.max(...R.efwGateRatios.map((r) => idx[r])) >= R.efwMinIndex };
    }
    return efw;
  };

  for (let i = 1; i < n; i++) {
    zz.step(i);
    if (i < R.minBars) continue;
    const tent = zz.tentative();
    if (!tent || P.length < 4) continue;
    const c = C[i], cands = [];
    const add = (key, s) => {
      if (fired.has(key)) return;
      if (s.side === 'LONG' ? !(s.stop < c) : !(s.stop > c)) return;     // the stop must be on the losing side
      fired.add(key);
      if (i < from) return;
      const sg = s.side === 'LONG' ? 1 : -1, risk = Math.abs(c - s.stop);
      cands.push({ i, time: T[i], ...s, ref: c, target: c + sg * R.rewardRisk * risk, stopPct: (risk / c) * 100 });
    };
    const last = P[P.length - 1];
    let gate = null;
    const gateOk = () => { if (gate === null) gate = efwAt(i).gate; return gate; };

    // ---- HARMONIC and ELLIOTT: reversal at the last point (tentative = running extreme of the current leg)
    if ((F.HARMONIC || F.ELLIOTT) && gateOk()) {
      for (const pat of PATTERNS) {
        if (!F[pat.family]) continue;
        const cases = [{ Q: [...P.slice(-(pat.points - 1)), tent], live: true }];
        if (R.harmonicEntry === 'TRIGGER') cases.push({ Q: P.slice(-pat.points), live: false });
        for (const { Q, live } of cases) {
          if (Q.length < pat.points || Q.some((q) => !q)) continue;
          const D = Q[Q.length - 1], prevPt = Q[Q.length - 2];
          if (D.type === prevPt.type) continue;
          const leg = (a, b) => Math.abs(Q[b].p - Q[a].p);
          const ratios = pat.ratios.map((x) => leg(...x.leg) / leg(...x.base));
          if (ratios.some((r) => !Number.isFinite(r))) continue;
          const acc = accuracy(ratios, pat.ratios);
          if (acc < R.minAccuracy) continue;
          // structure: inner points sit between their neighbours as the pattern drawing requires
          const u = Q[1].type;                 // +1: point 1 is a peak
          let okS = true;
          if (pat.name === 'Gartley') okS = (Q[2].p - Q[0].p) * u > 0 && (Q[1].p - Q[2].p) * u > 0 && (Q[3].p - Q[2].p) * u > 0 && (Q[1].p - Q[3].p) * u > 0;
          else if (pat.name === 'AB=CD') okS = (Q[0].p - Q[2].p) * Q[0].type > 0 && (Q[2].p - Q[1].p) * Q[0].type > 0;
          else if (pat.family === 'ELLIOTT') {
            const w1 = leg(0, 1), w3 = leg(2, 3), w5 = leg(4, 5);
            okS = (Q[2].p - Q[0].p) * u > 0           // rule 1: wave 2 never retraces more than 100% of wave 1
              && (Q[4].p - Q[1].p) * u > 0             // rule 2: wave 4 never enters wave 1 territory
              && !(w3 < w1 && w3 < w5)                 // rule 3: wave 3 is never the shortest
              && (Q[3].p - Q[1].p) * u > 0 && (Q[5].p - Q[3].p) * u > 0;
          }
          if (!okS) continue;
          const an = pat.ratios.find((x) => x.anchor), ai = pat.ratios.indexOf(an);
          const ref = Q[an.leg[0]].p, base = leg(...an.base), s = D.type;     // last point = ref + s * r * base
          const [rLo, rHi] = completionBand(an, ratios[ai], R.pci);
          if (ratios[ai] < rLo || ratios[ai] > rHi) continue;                  // last point outside the interval: no pattern
          const far = ref + s * rHi * base, near = ref + s * rLo * base;
          const side = s < 0 ? 'LONG' : 'SHORT', sg = s < 0 ? 1 : -1;
          const key = `${pat.name}|${Q.slice(0, -1).map((q) => q.i).join(',')}`;
          let go = false;
          if (R.harmonicEntry === 'TRIGGER') go = sg * (c - D.p) >= R.trigger * leg(Q.length - 2, Q.length - 1);
          else go = live && (c - far) * sg >= 0 && (near - c) * sg >= 0;         // close inside the interval, last entry = near limit
          if (!go) continue;
          const bull = side === 'LONG';
          const label = pat.family === 'ELLIOTT' ? `Elliott 5-wave ${bull ? 'down move complete (buy)' : 'up move complete (sell)'}` : `${bull ? 'Bullish' : 'Bearish'} ${pat.name}`;
          add(key, { side, family: pat.family, name: label, stop: far, accuracy: Math.round(acc * 10) / 10,
            note: `${pat.ratios.map((x, j) => `${x.id} ${ratios[j].toFixed(3)}`).join(', ')}; completion interval ${Math.min(far, near).toPrecision(6)}-${Math.max(far, near).toPrecision(6)}` });
        }
      }
    }

    // ---- SR: support / resistance through 3 confirmed same-type points; the current leg is the 4th touch
    // ---- EFW: shape-ratio level of the previous leg
    for (const sup of [true, false]) {
      // support: the leg toward it starts at the latest confirmed peak; resistance: at the latest trough
      const j0 = last.type === (sup ? 1 : -1) ? P.length - 1 : P.length - 2;
      if (j0 < 1) continue;
      const start = P[j0];
      const probe = j0 === P.length - 1 ? tent : P[P.length - 1];         // extreme of the leg since `start`
      if (!probe || probe.type === start.type) continue;
      const hs = sup ? 1 : -1;                                              // +1: price sits above the line

      if (F.SR && j0 >= 5) {
        const a = P[j0 - 5], b = P[j0 - 3], cc = P[j0 - 1];
        if (b.i - a.i >= R.minTouchGap && cc.i - b.i >= R.minTouchGap) {
          let avgLeg = 0, m = 0; for (let j = Math.max(1, j0 - 9); j <= j0; j++) { avgLeg += Math.abs(P[j].p - P[j - 1].p); m++; }
          const tol = R.touchTol * (avgLeg / m);
          const mean = (a.p + b.p + cc.p) / 3;
          let slope = null, kind = '';
          if ([a, b, cc].every((q) => Math.abs(q.p - mean) <= tol)) { slope = 0; kind = 'Horizontal'; }
          else {
            const sl = (cc.p - b.p) / (cc.i - b.i);
            if (Math.abs(a.p - (cc.p + sl * (a.i - cc.i))) <= tol) { slope = sl; kind = sl > 0 ? 'Ascending' : 'Descending'; }
          }
          if (slope !== null) {
            const lv = (x) => (slope === 0 ? mean : cc.p + slope * (x - cc.i));
            const lkey = `${sup}|${a.i}|${b.i}|${cc.i}|${start.i}`;
            if (!lineOk.has(lkey)) {                                        // the line held until the current leg started
              let ok = true;
              for (let q = a.i; q <= start.i && ok; q++) if (hs * ((sup ? L[q] : H[q]) - lv(q)) < -tol) ok = false;
              lineOk.set(lkey, ok);
            }
            const h = hs * (start.p - lv(start.i));
            if (lineOk.get(lkey) && h > 0) {
              const Y = R.trigger * h, line = lv(i), d = hs * (c - line);
              const touched = Math.abs(probe.p - lv(probe.i)) <= tol;
              const what = `${kind} ${sup ? 'support' : 'resistance'}`;
              const note = `line ${line.toPrecision(6)} through 3 ${sup ? 'troughs' : 'peaks'}, trigger ${Y.toPrecision(4)}`;
              if (touched && d >= Y) add(`SR|${lkey}|REV`, { side: sup ? 'LONG' : 'SHORT', family: 'SR', name: `${what} reversal`, stop: sup ? Math.min(probe.p, line - Y) : Math.max(probe.p, line + Y), accuracy: null, note });
              if (d <= -Y) add(`SR|${lkey}|BRK`, { side: sup ? 'SHORT' : 'LONG', family: 'SR', name: `${what} breakout`, stop: line + hs * Y, accuracy: null, note });
            }
          }
        }
      }

      if (F.EFW && gateOk()) {
        const p0 = P[j0 - 1], Hh = Math.abs(start.p - p0.p);
        if (Hh > 0) {
          const e = efwAt(i), rs = e.best, lv = start.p - hs * rs * Hh;    // level = previous leg retraced by the chosen ratio
          const r = Math.abs(start.p - probe.p) / Hh, Y = R.trigger * Hh, d = hs * (c - lv);
          const touched = Math.abs(r - rs) <= R.efwTol * rs;
          const note = `ratio ${rs} (EFW index ${(100 * e.idx[rs]).toFixed(0)}%), level ${lv.toPrecision(6)}, retraced ${r.toFixed(3)}`;
          if (touched && d >= Y) add(`EFW|${start.i}|REV`, { side: sup ? 'LONG' : 'SHORT', family: 'EFW', name: `${rs} retracement reversal`, stop: sup ? Math.min(probe.p, lv - Y) : Math.max(probe.p, lv + Y), accuracy: null, note });
          if (d <= -Y) add(`EFW|${start.i}|BRK`, { side: sup ? 'SHORT' : 'LONG', family: 'EFW', name: `${rs} retracement breakout`, stop: lv + hs * Y, accuracy: null, note });
        }
      }
    }

    // ---- CHANNEL: base line through points 1 and 3 of the last confirmed triangle, parallel line through point 2
    if (F.CHANNEL && P.length >= 3 && gateOk()) {
      const q1 = P[P.length - 3], q2 = P[P.length - 2], q3 = P[P.length - 1];
      const sr = Math.abs(q3.p - q2.p) / Math.abs(q2.p - q1.p);
      if (sr > 0 && sr < 1) {
        const u = q1.type < 0 ? 1 : -1;                                     // +1: rising channel (troughs higher), -1: falling
        const slope = (q3.p - q1.p) / (q3.i - q1.i), bl = (x) => q1.p + slope * (x - q1.i);
        const hgt = Math.abs(q2.p - bl(q2.i)), ct = R.channelTrigger * hgt, base = bl(i), par = base + u * hgt;
        if (hgt > 0) {
          const key = `CH|${q1.i}|${q2.i}|${q3.i}`, what = u > 0 ? 'Rising' : 'Falling';
          const note = `shape ratio ${sr.toFixed(3)}, base ${base.toPrecision(6)}, parallel ${par.toPrecision(6)}`;
          if (u * (c - par) >= ct) add(`${key}|MOM`, { side: u > 0 ? 'LONG' : 'SHORT', family: 'CHANNEL', name: `${what} EFW channel momentum`, stop: par - u * ct, accuracy: null, note });
          if (u * (base - c) >= ct) add(`${key}|TURN`, { side: u > 0 ? 'SHORT' : 'LONG', family: 'CHANNEL', name: `${what} EFW channel turn`, stop: base + u * ct, accuracy: null, note });
        }
      }
    }
    for (const s of cands) out.push(s);
  }
  return out;
}

// Live use: the best signal on the newest closed candle for the side the app allows ('LONG' / 'SHORT').
function findEntry(k, side, opts = {}) {
  if (!Array.isArray(k) || k.length < (opts.minBars || RULES.minBars)) return { ok: false, reason: 'not enough candles' };
  const pr = opts.priority || RULES.priority;
  const sigs = scan(k, { ...opts, from: k.length - 1 }).filter((s) => s.side === side);
  if (!sigs.length) return { ok: false, reason: 'no pattern on the last closed candle' };
  sigs.sort((a, b) => pr.indexOf(a.family) - pr.indexOf(b.family) || (b.accuracy || 0) - (a.accuracy || 0));
  return { ok: true, ...sigs[0], others: sigs.length - 1 };
}

module.exports = { RULES, FAMILIES, PATTERNS, atrSeries, zigzag, ratioError, accuracy, completionBand, efwIndex, scan, findEntry };
