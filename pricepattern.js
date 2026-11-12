'use strict';
// =====================================================================================================
// PRICE ACTION AND PATTERN ENTRIES
// From Young Ho Seo, "Scientific Guide to Price Action and Pattern Trading" (2017, v21.1): the text and
// the figures. Each rule names the part, chapter or figure it comes from.
//
// Everything runs on CLOSED candles and is causal: a signal on bar i uses bars 0..i only, so the back
// test and the live scanner see exactly the same thing. Candles are Binance kline arrays
// [openTime, open, high, low, close, ...] (strings or numbers). Peaks and troughs use bar highs and
// lows, never open / close (Part 2 ch 4.2).
//
// Families
//   HARMONIC  Gartley, Cypher, AB=CD (Part 2 ch 4, figures 4-10, 4-12, 4-15, X3 figure 2-7)   reversal at D
//   ELLIOTT   .12345 impulse, wave 3 extended (Part 2 ch 5, figures 5-3, 5-5, 5-13, X3 2-11) reversal after 5
//   SR        horizontal / diagonal support and resistance (Part 2 ch 2, figures 2-1 to 2-15)
//   EFW       shape-ratio (Fibonacci retracement) trading (Part 2 ch 1 and 3.2, figures 3-6, 3-8)
//   CHANNEL   Equilibrium Fractal Wave channel (Part 2 ch 3.3-3.4, figures 3-9, 3-17 to 3-22)
// =====================================================================================================

const RULES = Object.freeze({
  atrPeriod: 14,
  zigzagAtr: 3,          // Part 1 ch 12: ZigZag peaks / troughs; a point is confirmed once price reverses this many ATR(14)
  trigger: 0.15,         // Part 2 ch 2 (figure 2-5): enter when price moves this share of the last triangle's height off / through the line (5-20%)
  minAccuracy: 80,       // Part 2 ch 4.3 and ch 5.3: pattern matching accuracy = 100 - MAPE; trade only >= 80%
  pci: 0.10,             // Part 2 ch 4.3 (figure 4-16): Pattern Completion Interval = ideal ratio of the last point +-10%
  harmonicEntry: 'PCI',  // 'PCI': enter while price is inside the interval (ch 4.3); 'TRIGGER': wait for a turn of `trigger` x last leg
  rewardRisk: 3,         // figure 4-16 and the risk chapter: stop 1 x, target 3 x (completion-interval height, or the stop distance)
  touchTol: 0.10,        // Part 2 ch 2: a touch is within 10% of the average triangle height from the line (channel: of its height)
  minTouchGap: 3,        // Part 2 ch 2: touches too close together count as one (bars)
  srLookback: 3,         // same-type peaks / troughs searched for the touches. Figures 2-1 / 2-14 skip one point (lookback 4); tested 4, 5, 8: more lines, worse trades
  elliottLookback: 14,   // peaks / troughs searched for waves 0-4 (ch 5.2: try candidate peaks, keep the highest score)
  efwTol: 0.10,          // a retracement reached a shape-ratio level when within +-10% of the ratio
  efwCountTol: 0.10,     // a triangle counts toward a ratio's EFW index within +-10% of it (the book does not state this tolerance)
  efwMinIndex: 0.10,     // Part 1 ch 13: EFW index below 10% for the Fibonacci ratios -> no Fibonacci / harmonic / Elliott / EFW trades
  efwWindow: 1000,       // the EFW index counts the peaks / troughs made in the last 1000 bars (the live scanner loads 1500, 500 to warm up)
  efwRatios: [0.382, 0.5, 0.618, 0.786, 0.85],   // candidate shape ratios; the one with the highest EFW index is traded (ch 3.2)
  efwGateRatios: [0.382, 0.5, 0.618],
  efwMinPivots: 12,
  channelTrigger: 0.20,  // Part 2 ch 3.4: channel trigger = 20% of the channel height (20-30%, up to 50%)
  minBars: 300,
  priority: ['HARMONIC', 'ELLIOTT', 'SR', 'EFW', 'CHANNEL'],
});
// On by default: the families that beat candle patterns on risk in the 180-day test (CHANGES.md v6.4).
// EFW and CHANNEL are complete and tested but off: they added trades with a lower profit per trade and a worse worst case.
const FAMILIES = Object.freeze({ HARMONIC: true, ELLIOTT: true, SR: true, EFW: false, CHANNEL: false });

// ---------- fixed pattern structures (Part 3 X3 notation: "trade only patterns with a fixed structure") ----------
// Ratio spec: { v } one ideal value, { range: [lo, hi] } ideal range, { any: [a, b] } either value.
// A leg is [from point, to point]; a ratio is leg / base leg. `anchor` is the ratio that places the last point:
// the Pattern Completion Interval is built on it. `shape` lists [a, b, c]: point b lies strictly between a and c.
const PATTERNS = Object.freeze([
  { family: 'HARMONIC', name: 'Gartley', points: 5, ratios: [   // figure 4-10 (and the worked MAPE example)
    { id: 'AB/XA', leg: [1, 2], base: [0, 1], v: 0.618 },
    { id: 'BC/AB', leg: [2, 3], base: [1, 2], any: [0.618, 0.786] },
    { id: 'CD/BC', leg: [3, 4], base: [2, 3], any: [1.272, 1.618] },
    { id: 'AD/XA', leg: [1, 4], base: [0, 1], v: 0.786, anchor: true },   // the X-D line of figure 4-10 (D retraces 0.786 of XA)
  ], shape: [[0, 2, 1], [2, 3, 1], [0, 4, 1]] },
  { family: 'HARMONIC', name: 'Cypher', points: 5, ratios: [    // ideals read from figures 4-12 / 4-15 (0.384, 1.096, 1.967, 0.792 = 1.44% error); CD/BC 2.0 +-5% in the text
    { id: 'AB/XA', leg: [1, 2], base: [0, 1], v: 0.382 },
    { id: 'BC/AB', leg: [2, 3], base: [1, 2], v: 1.13 },
    { id: 'CD/BC', leg: [3, 4], base: [2, 3], v: 2.0, anchor: true },
    { id: 'AD/XA', leg: [1, 4], base: [0, 1], v: 0.786 },
  ], shape: [[0, 2, 1], [2, 1, 3], [0, 4, 1]] },                  // C goes beyond A
  { family: 'HARMONIC', name: 'AB=CD', points: 4, ratios: [     // X3 figure 2-7: "AB=CD with range", N2, R1 0.618, R0 1.272-1.618
    { id: 'BC/AB', leg: [1, 2], base: [0, 1], v: 0.618 },
    { id: 'CD/BC', leg: [2, 3], base: [1, 2], range: [1.272, 1.618], anchor: true },
  ], shape: [[1, 2, 0]] },
  { family: 'ELLIOTT', name: 'Elliott .12345', points: 6, ratios: [   // X3 figure 2-11 / figure 5-3 left: R3 0.618, R2 1.618-3.0, R1 0.382, R0 1.618-2.618, F0 1.0
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

// Part 2 ch 4.3 / figure 5-12: error of one measured ratio against its ideal (percent error; 0 inside an ideal range)
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
function efwIndex(pivots, ratios = RULES.efwRatios, tol = RULES.efwCountTol) {
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
  const fired = new Set(), lines = new Map();
  const from = Number.isFinite(opts.from) ? opts.from : 0;
  const EW = PATTERNS.find((p) => p.family === 'ELLIOTT');
  let w0 = 0, efwKey = '', efw = null;

  const efwAt = (i) => {                       // EFW index over the peaks / troughs made in the last efwWindow bars
    while (w0 < P.length && P[w0].i <= i - R.efwWindow) w0++;
    const key = `${w0}|${P.length}`;
    if (key !== efwKey) {
      efwKey = key;
      const win = P.slice(w0);
      const idx = efwIndex(win, [...new Set([...R.efwRatios, ...R.efwGateRatios])], R.efwCountTol);
      let best = R.efwRatios[0]; for (const r of R.efwRatios) if (idx[r] > idx[best]) best = r;
      efw = { pivots: win.length, idx, best, gate: win.length >= R.efwMinPivots && Math.max(...R.efwGateRatios.map((r) => idx[r])) >= R.efwMinIndex };
    }
    return efw;
  };

  // ---- Elliott (ch 5.2, figure 5-13): candidate waves 0-4 among the recent confirmed peaks / troughs. Smaller swings
  // inside a wave are allowed, but every wave point must be the extreme between its neighbours. Rebuilt per new pivot.
  let ewLen = -1, ewList = [];
  const elliottStructures = () => {
    if (ewLen === P.length) return ewList;
    ewLen = P.length; ewList = [];
    const N = P.length, lo = Math.max(0, N - R.elliottLookback);
    const budget = (EW.ratios.length * (100 - R.minAccuracy)) / 100;
    const extremeBetween = (j, a, b) => { for (let x = a + 1; x < b; x++) if ((P[x].p - P[j].p) * P[j].type > 0) return false; return true; };
    for (let j4 = N - 1; j4 - 4 >= lo; j4 -= 2)
      for (let j3 = j4 - 1; j3 - 3 >= lo; j3 -= 2)
        for (let j2 = j3 - 1; j2 - 2 >= lo; j2 -= 2)
          for (let j1 = j2 - 1; j1 - 1 >= lo; j1 -= 2)
            for (let j0 = j1 - 1; j0 >= lo; j0 -= 2) {
              const q = [P[j0], P[j1], P[j2], P[j3], P[j4]], u = q[1].type;
              if ((q[2].p - q[0].p) * u <= 0) continue;                 // rule 1: wave 2 never retraces more than 100% of wave 1
              if ((q[4].p - q[1].p) * u <= 0) continue;                 // rule 2: wave 4 never enters wave 1 territory
              if ((q[3].p - q[1].p) * u <= 0) continue;                 // wave 3 ends beyond wave 1
              if (!extremeBetween(j0, j0, j1) || !extremeBetween(j1, j0, j2) || !extremeBetween(j2, j1, j3) || !extremeBetween(j3, j2, j4) || !extremeBetween(j4, j3, N)) continue;
              const w = (a, b) => Math.abs(q[b].p - q[a].p);
              const e = ratioError(w(1, 2) / w(0, 1), EW.ratios[0]) + ratioError(w(2, 3) / w(1, 2), EW.ratios[1]) + ratioError(w(3, 4) / w(2, 3), EW.ratios[2]);
              if (e > budget) continue;                                  // even a perfect wave 5 could not reach the minimum score
              let far5 = -Infinity * u;                                  // furthest wave-5 sub-swing already confirmed after wave 4
              for (let x = j4 + 1; x < N; x++) if (P[x].type === u && (P[x].p - far5) * u > 0) far5 = P[x].p;
              ewList.push({ q, far5 });
            }
    return ewList;
  };

  // ---- EFW channel (ch 3.3-3.4): the latest confirmed triangle with shape ratio < 1 draws the channel (base line through
  // points 1 and 3, parallel line through point 2). It stays in force until a newer one forms or price breaks out of it,
  // and is traded like support / resistance on both lines (figures 3-17 to 3-22).
  let ch = null, chLen = -1;

  for (let i = 1; i < n; i++) {
    zz.step(i);
    if (P.length !== chLen) {
      chLen = P.length;
      if (P.length >= 3) {
        const q1 = P[P.length - 3], q2 = P[P.length - 2], q3 = P[P.length - 1];
        const sr = Math.abs(q3.p - q2.p) / Math.abs(q2.p - q1.p);
        if (sr > 0 && sr < 1) {
          const slope = (q3.p - q1.p) / (q3.i - q1.i), bl = (x) => q1.p + slope * (x - q1.i), hgt = Math.abs(q2.p - bl(q2.i));
          if (hgt > 0) ch = { q1, q2, q3, sr, u: q1.type < 0 ? 1 : -1, bl, hgt, from: i + 1, armLo: -1, armUp: -1, probeLo: Infinity, probeUp: -Infinity, spent: false, key: `CH|${q1.i}|${q2.i}|${q3.i}` };
        }
      }
    }
    // ---- CHANNEL: reversal or breakout at either channel line. State changes every bar; signals are emitted below.
    const chEv = [];
    if (F.CHANNEL && ch && !ch.spent && i >= ch.from) {
      const base = ch.bl(i), par = base + ch.u * ch.hgt, lower = Math.min(base, par), upper = Math.max(base, par);
      const Y = R.channelTrigger * ch.hgt, tolC = R.touchTol * ch.hgt, rising = ch.u > 0, nm = rising ? 'Rising' : 'Falling', c = C[i];
      const note = `shape ratio ${ch.sr.toFixed(3)}, lower ${lower.toPrecision(6)}, upper ${upper.toPrecision(6)}`;
      if (L[i] <= lower + tolC) { if (ch.armLo < 0) ch.armLo = i; ch.probeLo = Math.min(ch.probeLo, L[i]); }
      if (H[i] >= upper - tolC) { if (ch.armUp < 0) ch.armUp = i; ch.probeUp = Math.max(ch.probeUp, H[i]); }
      if (c <= lower - Y) {
        chEv.push({ key: `${ch.key}|DN`, s: { side: 'SHORT', family: 'CHANNEL', name: `${nm} EFW channel break down (${rising ? 'bearish turn' : 'bearish momentum'})`, stop: lower + Y, accuracy: null, note } });
        ch.spent = true;
      } else if (c >= upper + Y) {
        chEv.push({ key: `${ch.key}|UP`, s: { side: 'LONG', family: 'CHANNEL', name: `${nm} EFW channel break up (${rising ? 'bullish momentum' : 'bullish turn'})`, stop: upper - Y, accuracy: null, note } });
        ch.spent = true;
      } else {
        if (ch.armLo >= 0 && c >= lower + Y) {
          chEv.push({ key: `${ch.key}|LO|${ch.armLo}`, s: { side: 'LONG', family: 'CHANNEL', name: `${nm} EFW channel bounce off the lower line`, stop: Math.min(ch.probeLo, lower - Y), accuracy: null, note } });
          ch.armLo = -1; ch.probeLo = Infinity;
        }
        if (ch.armUp >= 0 && c <= upper - Y) {
          chEv.push({ key: `${ch.key}|HI|${ch.armUp}`, s: { side: 'SHORT', family: 'CHANNEL', name: `${nm} EFW channel rejected at the upper line`, stop: Math.max(ch.probeUp, upper + Y), accuracy: null, note } });
          ch.armUp = -1; ch.probeUp = -Infinity;
        }
      }
    }
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
      // figure 4-16: harmonic / Elliott target = 3 x the completion-interval height; otherwise 3 x the stop distance
      cands.push({ i, time: T[i], ...s, ref: c, target: c + sg * R.rewardRisk * (s.pciRange || risk), stopPct: (risk / c) * 100 });
    };
    const last = P[P.length - 1];
    let gate = null;
    const gateOk = () => { if (gate === null) gate = efwAt(i).gate; return gate; };

    // ---- HARMONIC: reversal at D (tentative = running extreme of the current leg); points are consecutive swings (figure 10-17)
    if (F.HARMONIC && gateOk()) {
      for (const pat of PATTERNS) {
        if (pat.family !== 'HARMONIC') continue;
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
          if (!pat.shape.every(([a, b, cc]) => (Q[b].p - Q[a].p) * (Q[cc].p - Q[b].p) > 0)) continue;   // the drawing's shape
          const an = pat.ratios.find((x) => x.anchor), ai = pat.ratios.indexOf(an);
          const ref = Q[an.leg[0]].p, base = leg(...an.base), s = D.type;     // last point = ref + s * r * base
          const [rLo, rHi] = completionBand(an, ratios[ai], R.pci);
          if (ratios[ai] < rLo || ratios[ai] > rHi) continue;                  // last point outside the interval: no pattern
          const far = ref + s * rHi * base, near = ref + s * rLo * base;
          const side = s < 0 ? 'LONG' : 'SHORT', sg = s < 0 ? 1 : -1;          // figure 4-5: D below C buy, D above C sell
          const key = `${pat.name}|${Q.slice(0, -1).map((q) => q.i).join(',')}`;
          const go = R.harmonicEntry === 'TRIGGER'
            ? sg * (c - D.p) >= R.trigger * leg(Q.length - 2, Q.length - 1)
            : live && (c - far) * sg >= 0 && (near - c) * sg >= 0;           // close inside the interval, last entry = near limit
          if (!go) continue;
          add(key, { side, family: 'HARMONIC', name: `${side === 'LONG' ? 'Bullish' : 'Bearish'} ${pat.name}`, stop: far, pciRange: Math.abs(far - near), accuracy: Math.round(acc * 10) / 10,
            note: `${pat.ratios.map((x, j) => `${x.id} ${ratios[j].toFixed(3)}`).join(', ')}; completion interval ${Math.min(far, near).toPrecision(6)}-${Math.max(far, near).toPrecision(6)}` });
        }
      }
    }

    // ---- ELLIOTT: wave 5 = the current leg's extreme; best-scoring count over the candidate waves 0-4 (ch 5.3)
    if (F.ELLIOTT && gateOk()) {
      let best = null;
      for (const st of elliottStructures()) {
        const q = [...st.q, tent], u = q[1].type;
        if (tent.type !== u || (tent.p - st.far5) * u < 0 || (tent.p - q[3].p) * u <= 0) continue;   // wave 5 ends beyond wave 3 and its own sub-swings
        const w = (a, b) => Math.abs(q[b].p - q[a].p);
        if (w(2, 3) < w(0, 1) && w(2, 3) < w(4, 5)) continue;               // rule 3: wave 3 is never the shortest
        const ratios = EW.ratios.map((x) => w(...x.leg) / w(...x.base));
        const acc = accuracy(ratios, EW.ratios);
        if (acc < R.minAccuracy || (best && acc <= best.acc)) continue;
        const an = EW.ratios[4], [rLo, rHi] = completionBand(an, ratios[4], R.pci);
        if (ratios[4] < rLo || ratios[4] > rHi) continue;
        best = { q, u, acc, ratios, far: q[4].p + u * rHi * w(0, 1), near: q[4].p + u * rLo * w(0, 1), w5: w(4, 5) };
      }
      if (best) {
        const { q, u, acc, ratios, far, near, w5 } = best, side = u > 0 ? 'SHORT' : 'LONG', sg = -u;
        const go = R.harmonicEntry === 'TRIGGER' ? sg * (c - q[5].p) >= R.trigger * w5 : (c - far) * sg >= 0 && (near - c) * sg >= 0;
        if (go) add(`ELLIOTT|${q[4].i}`, { side, family: 'ELLIOTT', name: `Elliott 5-wave ${u > 0 ? 'up move complete (sell)' : 'down move complete (buy)'}`, stop: far, pciRange: Math.abs(far - near), accuracy: Math.round(acc * 10) / 10,
          note: `wave structural score ${acc.toFixed(1)}%; ${EW.ratios.map((x, j) => `${x.id} ${ratios[j].toFixed(3)}`).join(', ')}` });
      }
    }

    // ---- SR: support / resistance through 3+ confirmed same-type points; the current leg is the next touch
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
        const lkey = `${sup}|${start.i}`;
        if (!lines.has(lkey)) lines.set(lkey, findLine(P, j0, sup, hs, H, L, R));
        const ln = lines.get(lkey);
        if (ln) {
          const h = hs * (start.p - ln.lv(start.i));                      // figure 2-5: height of the last triangle at the line
          if (h > 0) {
            const Y = R.trigger * h, line = ln.lv(i), d = hs * (c - line);
            const touched = Math.abs(probe.p - ln.lv(probe.i)) <= ln.tol;
            const what = `${ln.kind} ${sup ? 'support' : 'resistance'}`;
            const note = `line ${line.toPrecision(6)} through ${ln.touches} ${sup ? 'troughs' : 'peaks'}, trigger ${Y.toPrecision(4)}`;
            if (touched && d >= Y) add(`SR|${lkey}|REV`, { side: sup ? 'LONG' : 'SHORT', family: 'SR', name: `${what} reversal`, stop: sup ? Math.min(probe.p, line - Y) : Math.max(probe.p, line + Y), accuracy: null, note });
            if (d <= -Y) add(`SR|${lkey}|BRK`, { side: sup ? 'SHORT' : 'LONG', family: 'SR', name: `${what} breakout`, stop: line + hs * Y, accuracy: null, note });
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

    if (chEv.length && gateOk()) for (const ev of chEv) add(ev.key, ev.s);
    for (const s of cands) out.push(s);
  }
  return out;
}

// Part 2 ch 2 (figures 2-1, 2-5, 2-14): the line through the latest same-type point before the current leg and at least
// two earlier ones. Points in between may sit away from the line, but no bar may break through it (beyond the touch
// tolerance) until the current leg starts. Horizontal first, then diagonal (newest second point first).
function findLine(P, j0, sup, hs, H, L, R) {
  const cc = P[j0 - 1], olders = [];
  for (let j = j0 - 3; j >= 0 && olders.length < R.srLookback - 1; j -= 2) olders.push(P[j]);
  let avgLeg = 0, m = 0; for (let j = Math.max(1, j0 - 9); j <= j0; j++) { avgLeg += Math.abs(P[j].p - P[j - 1].p); m++; }
  const tol = R.touchTol * (avgLeg / m), startI = P[j0].i;
  const gapsOk = (pts) => { const s = pts.map((q) => q.i).sort((a, b) => a - b); for (let x = 1; x < s.length; x++) if (s[x] - s[x - 1] < R.minTouchGap) return false; return true; };
  const holds = (lv, firstI) => { for (let q = firstI; q <= startI; q++) if (hs * ((sup ? L[q] : H[q]) - lv(q)) < -tol) return false; return true; };
  const tries = [];
  {
    let pts = [cc, ...olders.filter((q) => Math.abs(q.p - cc.p) <= tol)];
    if (pts.length >= 3) {
      const mean = pts.reduce((s, q) => s + q.p, 0) / pts.length;
      pts = pts.filter((q) => Math.abs(q.p - mean) <= tol);
      if (pts.length >= 3) tries.push({ kind: 'Horizontal', lv: () => mean, pts });
    }
  }
  for (const b of olders) {
    const sl = (cc.p - b.p) / (cc.i - b.i), lv = (x) => cc.p + sl * (x - cc.i);
    const pts = [cc, b, ...olders.filter((q) => q !== b && Math.abs(q.p - lv(q.i)) <= tol)];
    if (pts.length >= 3) tries.push({ kind: sl > 0 ? 'Ascending' : 'Descending', lv, pts });
  }
  for (const t of tries) {
    if (!gapsOk(t.pts)) continue;
    const firstI = Math.min(...t.pts.map((q) => q.i));
    if (holds(t.lv, firstI)) return { kind: t.kind, lv: t.lv, tol, touches: t.pts.length };
  }
  return null;
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
