/**
 * =========================================================================
 * SISIR QUANT - ORDER BLOCK ENTRIES (SMC / ICT)
 * Pure functions, no I/O. Input: Binance klines, CLOSED 15m candles, oldest first.
 * =========================================================================
 * The model that beat the candle patterns in the 180-day replay (39 coins, $110, 10% compounding):
 *  1. A displacement candle: a body at least 1.8x the average of the 20 before it, in the trade's
 *     direction, that closes beyond the last confirmed swing (break of structure).
 *  2. The order block is the last opposite-colour candle in the 5 before the displacement.
 *  3. Entry when the newest closed candle is the FIRST to come back into that block and closes
 *     inside or beyond it (not through it).
 *  4. Only in the London (07-10 UTC) and New York (12-16 UTC) sessions.
 * Stop reference: the far edge of the block. Exits stay with the engine's no-loss rules.
 * =========================================================================
 */
'use strict';

const OB_RULES = { minBars: 121, swingLookback: 60, dispBodyMult: 1.8, dispLookback: 20, obLookback: 5, cooldownBars: 4 };

const H = (k, i) => +k[i][2], L = (k, i) => +k[i][3], O = (k, i) => +k[i][1], C = (k, i) => +k[i][4];

/** London open 07-10 UTC and New York open 12-16 UTC. */
function inKillzone(t) { const h = new Date(t).getUTCHours(); return (h >= 7 && h < 10) || (h >= 12 && h < 16); }
function nextKillzone(t) {
  const d = new Date(t), h = d.getUTCHours();
  const start = h < 7 ? 7 : h < 12 ? 12 : 31;   // 31 = 07:00 tomorrow
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) + start * 3600000;
}

function swingsUpTo(k, i, n) {
  const hi = [], lo = [];
  for (let j = Math.max(2, i - n); j <= i - 2; j++) {
    if (H(k, j) > H(k, j - 1) && H(k, j) > H(k, j - 2) && H(k, j) >= H(k, j + 1) && H(k, j) >= H(k, j + 2)) hi.push(j);
    if (L(k, j) < L(k, j - 1) && L(k, j) < L(k, j - 2) && L(k, j) <= L(k, j + 1) && L(k, j) <= L(k, j + 2)) lo.push(j);
  }
  return { hi, lo };
}
const avgBody = (k, i, n = 20) => { let s = 0; for (let j = i - n; j < i; j++) s += Math.abs(C(k, j) - O(k, j)); return s / n; };

/**
 * Is the newest closed candle a first retest of an order block?
 * @param klines closed 15m klines, oldest first (at least 121)
 * @param sg +1 long, -1 short
 * @returns { ok, reason } or { ok:true, obTime, dispTime, zoneLow, zoneHigh, stop, barTime }
 */
function findOrderBlockEntry(klines, sg, R = OB_RULES) {
  const k = Array.isArray(klines) ? klines : [];
  const i = k.length - 1;
  if (i < R.minBars - 1) return { ok: false, reason: 'not enough candles' };
  if (sg !== 1 && sg !== -1) return { ok: false, reason: 'no side' };
  const sw = swingsUpTo(k, i, R.swingLookback);
  for (let d = i - 2; d >= i - R.dispLookback && d > 3; d--) {
    const disp = Math.abs(C(k, d) - O(k, d)) >= avgBody(k, d) * R.dispBodyMult && (sg > 0 ? C(k, d) > O(k, d) : C(k, d) < O(k, d));
    if (!disp) continue;
    const ref = (sg > 0 ? sw.hi : sw.lo).filter((j) => j < d - 1).pop();
    if (ref === undefined) continue;
    const bos = sg > 0 ? C(k, d) > H(k, ref) : C(k, d) < L(k, ref);
    if (!bos) continue;
    // the most recent displacement that broke structure decides; older ones are stale
    let ob = -1;
    for (let j = d - 1; j >= d - R.obLookback; j--) if (sg > 0 ? C(k, j) < O(k, j) : C(k, j) > O(k, j)) { ob = j; break; }
    if (ob < 0) return { ok: false, reason: 'no order block before the displacement' };
    const zLo = L(k, ob), zHi = H(k, ob);
    for (let j = d + 1; j < i; j++) if (sg > 0 ? L(k, j) <= zHi : H(k, j) >= zLo) return { ok: false, reason: 'order block already retested' };
    const touch = sg > 0 ? L(k, i) <= zHi && C(k, i) >= zLo : H(k, i) >= zLo && C(k, i) <= zHi;
    if (!touch) return { ok: false, reason: 'price has not returned to the order block' };
    return { ok: true, obTime: +k[ob][0], dispTime: +k[d][0], zoneLow: zLo, zoneHigh: zHi, stop: sg > 0 ? zLo : zHi, barTime: +k[i][0] };
  }
  return { ok: false, reason: 'no displacement that broke structure in the last 5 hours' };
}

module.exports = { OB_RULES, findOrderBlockEntry, inKillzone, nextKillzone };
