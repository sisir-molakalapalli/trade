'use strict';
/**
 * v6.33: the hourly MOMENTUM REPORT, written by Google Gemini (paid key) on the server.
 *
 * The user's theory (25 Nov 2026, from paper trading): the bags that drag on for days are the ones opened when the
 * coin had no momentum in the trade's direction. So every hour, just after the 1h candle closes, the engine sends
 * Gemini compact Binance USD-M data for the radar's top coins (1h trend, RSI, volume, taker flow, funding, open
 * interest), Gemini also searches the web for the latest crypto news, and it answers per coin: the direction of its
 * momentum (bear / bull / flat) and a 0-100 score (high = strong momentum likely to carry on, low = weak, choppy or
 * likely to reverse). The autopilot then opens a short only on a coin with bearish momentum at or above the minimum
 * score (a long only on bullish momentum), and with auto leverage on, the score sets the leverage instead of volatility.
 *
 * Not replay-tested: a model that searches today's news can't be run honestly on past data. Every trade records the
 * score it was opened with, so paper trading can show whether high-score entries bag less.
 *
 * The key is GEMINI_MOMENTUM_KEY (or GEMINI_API_KEY) in the server's environment; it is sent only to Google, in a header.
 */

const ENDPOINT = 'https://generativelanguage.googleapis.com/v1beta/models/';
const DEFAULT_MODELS = ['gemini-3.8-flash', 'gemini-flash-latest', 'gemini-3.6-flash'];
const RULES = {
  coins: 40,                  // the radar's top coins by screener score that get a reading
  maxAgeMs: 2 * 3600000,      // a report older than 2 hours no longer opens trades
  retryMs: 5 * 60000,         // a failed report is tried again 5 minutes later
  runAtMinute: 2,             // each hour at hh:02, after the 1h candle has closed
  timeoutMs: 180000,          // web search + thinking can take a while
  minScore: 60,               // default minimum score to open a trade
  // leverage from the score (auto leverage on): under 70 1x, 70-79 2x, 80+ 3x (capped at the engine's maximum)
  leverageSteps: [[80, 3], [70, 2], [0, 1]]
};

const r = (x, d = 2) => (Number.isFinite(Number(x)) ? Math.round(Number(x) * 10 ** d) / 10 ** d : null);
const num = (x) => { const n = Number(x); return Number.isFinite(n) ? n : NaN; };
const pctCh = (a, b) => (b > 0 && Number.isFinite(a) ? ((a - b) / b) * 100 : NaN);

function ema(values, period) {
  const k = 2 / (period + 1); let e = NaN;
  for (const v of values) e = Number.isFinite(e) ? v * k + e * (1 - k) : v;
  return e;
}
function rsi(closes, period = 14) {
  if (closes.length <= period) return NaN;
  let g = 0, l = 0;
  for (let i = 1; i <= period; i++) { const d = closes[i] - closes[i - 1]; if (d >= 0) g += d; else l -= d; }
  g /= period; l /= period;
  for (let i = period + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    g = (g * (period - 1) + Math.max(d, 0)) / period; l = (l * (period - 1) + Math.max(-d, 0)) / period;
  }
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

/**
 * One compact row per coin from Binance data, so Gemini reads numbers instead of raw candles (far fewer tokens).
 * k1h: futures 1h klines, oldest first, the last one may still be open; oi: openInterestHist rows (1h), oldest first;
 * funding: the last funding rate (0.0001 = 0.01%). Returns null when there are too few candles.
 */
const COLS = ['coin', 'price', 'ch1h%', 'ch4h%', 'ch24h%', 'vsEma20%', 'ema20vs50%', 'rsi14', 'atr14%', 'vol3h/avg24', 'takerBuy3h%', 'funding%', 'oi1h%', 'oi24h%', 'closes12h%'];
function features(coin, k1h, oi = [], funding = NaN) {
  const bars = (Array.isArray(k1h) ? k1h : []).map((b) => ({ o: num(b[1]), h: num(b[2]), l: num(b[3]), c: num(b[4]), v: num(b[5]), tb: num(b[9]) }))
    .filter((b) => b.c > 0);
  if (bars.length < 30) return null;
  const closes = bars.map((b) => b.c), last = closes[closes.length - 1], at = (n) => closes[closes.length - 1 - n];
  const e20 = ema(closes, 20), e50 = ema(closes, 50);
  let trSum = 0; const tail = bars.slice(-15);
  for (let i = 1; i < tail.length; i++) trSum += Math.max(tail[i].h - tail[i].l, Math.abs(tail[i].h - tail[i - 1].c), Math.abs(tail[i].l - tail[i - 1].c));
  const atr = (trSum / (tail.length - 1) / last) * 100;
  const v3 = bars.slice(-3), v24 = bars.slice(-27, -3);
  const avg = (a) => a.reduce((s, x) => s + x, 0) / (a.length || 1);
  const vr = avg(v3.map((b) => b.v)) / (avg(v24.map((b) => b.v)) || NaN);
  const tbv = v3.reduce((s, b) => s + b.v, 0), tb = tbv > 0 ? (v3.reduce((s, b) => s + (Number.isFinite(b.tb) ? b.tb : b.v / 2), 0) / tbv) * 100 : NaN;
  const oiv = (Array.isArray(oi) ? oi : []).map((x) => num(x && (x.sumOpenInterestValue ?? x.sumOpenInterest))).filter((x) => x > 0);
  const oiNow = oiv[oiv.length - 1];
  return [
    coin, r(last, last >= 100 ? 2 : last >= 1 ? 4 : 8), r(pctCh(last, at(1))), r(pctCh(last, at(4))), r(pctCh(last, at(24))),
    r(pctCh(last, e20)), r(pctCh(e20, e50)), r(rsi(closes), 1), r(atr), r(vr), r(tb, 1),
    Number.isFinite(num(funding)) ? r(num(funding) * 100, 4) : null,
    oiv.length >= 2 ? r(pctCh(oiNow, oiv[oiv.length - 2])) : null,
    oiv.length >= 25 ? r(pctCh(oiNow, oiv[oiv.length - 25])) : null,
    closes.slice(-13, -1).map((c) => r(pctCh(c, last), 1))
  ];
}

const SYSTEM = [
  'You are a crypto futures momentum analyst for a PAPER-TRADING autopilot (no real money). It trades Binance USD-M perpetuals intraday.',
  'You get one compact row per coin (columns in "cols"; closes12h% = each of the last 12 hourly closes as % from the current price, oldest first;',
  'funding% is the last funding rate, positive = longs pay shorts; oi = open interest value change). You also get "news": the latest crypto',
  'headlines from a web search made a minute ago (market-wide and per coin, with their tone). Weigh them in.',
  'For EVERY coin judge its momentum over the next few hours:',
  '- d: "bear" (falling with force), "bull" (rising with force) or "flat" (no clear push, choppy or stalling).',
  '- s: 0-100, how strong AND how likely to carry on that momentum is. 80+ strong and clean, 60-79 decent, 40-59 weak or mixed, under 40 none or about to reverse.',
  'Lower the score for: moves already overextended (far from EMA20, extreme RSI) that tend to snap back; squeeze risk (price falling while open interest',
  'rises and funding is very negative is a short squeeze risk, the mirror for longs); falling volume; news that goes against the move. Raise it for',
  'aligned trend (price and EMA20 below EMA50 for bear), rising volume with taker flow on the same side, open interest building with the move, supportive news.',
  'Reply with JSON only, no other text:',
  '{"market":{"bias":"bear|bull|flat"},"coins":[{"c":"SOL","d":"bear","s":74,"w":"reason, at most 90 characters"}]}',
  'Include every coin from the rows exactly once, with the coin symbol as given.'
].join('\n');

// Step 1: a short web search for the news. (With the whole data table in one request Gemini tended to skip the search and
// answer from memory; a short question that asks for a search does search: 6 queries and 13 sources in the test on 25 Nov.)
function buildNewsRequest(coins, now = Date.now()) {
  const text = `Use web search. It is ${new Date(now).toISOString().slice(0, 16)}Z. Find the most important crypto news of the last 24 hours: the overall market `
    + `(macro, ETF flows, liquidations, regulation, hacks) and anything specific to these coins: ${coins.join(', ')}. Reply with JSON only: `
    + '{"news":[{"h":"headline","src":"source site","coins":["SOL"],"tone":"bear|bull|mixed"}]} with at most 8 items, the most market-moving first.';
  return { contents: [{ role: 'user', parts: [{ text }] }], tools: [{ google_search: {} }], generationConfig: { temperature: 0.2, maxOutputTokens: 4096 } };
}
function parseNews(text) {
  const raw = String(text || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
  let d = null;
  try { d = JSON.parse(raw); } catch (e) { const a = raw.indexOf('{'), b = raw.lastIndexOf('}'); if (a >= 0 && b > a) { try { d = JSON.parse(raw.slice(a, b + 1)); } catch (e2) { d = null; } } }
  const list = d && Array.isArray(d.news) ? d.news : [];
  return list.map((x) => ({
    h: clean(x && x.h, 150), src: clean(x && x.src, 40),
    coins: (Array.isArray(x && x.coins) ? x.coins : []).map((c) => String(c).toUpperCase().replace(/[^A-Z0-9]/g, '')).filter(Boolean).slice(0, 6),
    tone: ['bear', 'bull', 'mixed'].includes(String(x && x.tone).toLowerCase()) ? String(x.tone).toLowerCase() : 'mixed'
  })).filter((x) => x.h).slice(0, 8);
}
// Step 2: the scoring, with the data rows and the news (JSON mode, no tools).
function buildRequest(rows, market, news = []) {
  const input = { cols: COLS, rows, marketMode: market || null, news, now: new Date().toISOString().slice(0, 16) + 'Z' };
  return {
    systemInstruction: { parts: [{ text: SYSTEM }] },
    contents: [{ role: 'user', parts: [{ text: JSON.stringify(input) }] }],
    generationConfig: { responseMimeType: 'application/json', temperature: 0.3, maxOutputTokens: 8192 }
  };
}

const clean = (s, max) => {
  let t = String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f-\u009f]+/g, ' ').replace(/[<>`]/g, '').replace(/\s+/g, ' ').trim();
  if (/https?:|www\./i.test(t)) t = t.replace(/\(?https?:\S+\)?|www\.\S+/gi, '').trim();
  return t.length > max ? `${t.slice(0, max - 1).trim()}…` : t;
};

/** Gemini's answer as { market, coins: { SOL: { d, s, w } } }, only for coins that were asked about; null if unusable. */
function parseReport(text, asked) {
  const raw = String(text || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
  let d = null;
  try { d = JSON.parse(raw); } catch (e) {
    const a = raw.indexOf('{'), b = raw.lastIndexOf('}');
    if (a >= 0 && b > a) { try { d = JSON.parse(raw.slice(a, b + 1)); } catch (e2) { d = null; } }
  }
  if (!d || typeof d !== 'object' || !Array.isArray(d.coins)) return null;
  const want = new Set((asked || []).map((x) => String(x).toUpperCase()));
  const coins = {};
  for (const x of d.coins) {
    if (!x || typeof x !== 'object') continue;
    const c = String(x.c || x.coin || '').toUpperCase().replace(/USDT$/, '');
    const dir = String(x.d || x.dir || '').toLowerCase(), s = Math.round(Number(x.s ?? x.score));
    if (!want.has(c) || !['bear', 'bull', 'flat'].includes(dir) || !Number.isFinite(s)) continue;
    coins[c] = { d: dir, s: Math.max(0, Math.min(100, s)), w: clean(x.w || x.why || '', 110) };
  }
  if (Object.keys(coins).length < Math.min(3, want.size)) return null;   // too little to trade on
  const m = d.market && typeof d.market === 'object' ? d.market : {};
  const bias = ['bear', 'bull', 'flat'].includes(String(m.bias).toLowerCase()) ? String(m.bias).toLowerCase() : null;
  return { market: { bias, news: [] }, coins };
}

/**
 * May the autopilot open this side on this coin? report: { at, coins }. side: 'SHORT' | 'LONG'.
 * Returns { ok, reason, entry } (entry = the coin's reading when there is one).
 */
function gate(report, coin, side, { minScore = RULES.minScore, maxAgeMs = RULES.maxAgeMs, now = Date.now() } = {}) {
  if (!report || !report.at) return { ok: false, reason: 'no momentum report yet' };
  if (now - report.at > maxAgeMs) return { ok: false, reason: `the momentum report is ${Math.round((now - report.at) / 60000)} min old` };
  const c = String(coin || '').toUpperCase().replace(/USDT$/, ''), e = report.coins && report.coins[c];
  if (!e) return { ok: false, reason: 'not in this hour\'s momentum report' };
  const want = side === 'LONG' ? 'bull' : 'bear';
  if (e.d !== want) return { ok: false, reason: `momentum is ${e.d} ${e.s}, not ${want}`, entry: e };
  if (e.s < minScore) return { ok: false, reason: `${want} momentum ${e.s} is under ${minScore}`, entry: e };
  return { ok: true, reason: `${want} momentum ${e.s}`, entry: e };
}

/** Leverage from the momentum score (auto leverage on): under 70 1x, 70-79 2x, 80+ 3x, never above max. */
function leverageFor(score, max = 3) {
  const s = Number(score);
  if (!Number.isFinite(s)) return 1;
  const step = RULES.leverageSteps.find(([min]) => s >= min);
  return Math.max(1, Math.min(max, step ? step[1] : 1));
}

/** When the next report is due: hh:02 after the last one, or a retry after a failure. */
function nextRunAt(lastOkAt, lastTryAt, now = Date.now()) {
  const hourMark = (t) => { const d = new Date(t); d.setUTCMinutes(RULES.runAtMinute, 0, 0); if (d.getTime() <= t) d.setUTCHours(d.getUTCHours() + 1); return d.getTime(); };
  if (!lastOkAt) return lastTryAt ? lastTryAt + RULES.retryMs : now;
  const due = hourMark(lastOkAt);
  return lastTryAt > lastOkAt ? Math.max(lastTryAt + RULES.retryMs, Math.min(due, lastTryAt + RULES.retryMs)) : due;
}

function create({ apiKey = '', models = DEFAULT_MODELS, fetchFn = (typeof fetch === 'function' ? fetch : null), log = () => {} } = {}) {
  const key = String(apiKey || '').trim();
  const st = { report: null, lastOkAt: 0, lastTryAt: 0, lastError: '', model: '', busy: false, runs: 0, usage: null };

  async function callModel(model, body) {
    const ac = new AbortController(), timer = setTimeout(() => ac.abort(), RULES.timeoutMs);
    try {
      const res = await fetchFn(`${ENDPOINT}${encodeURIComponent(model)}:generateContent`, {
        method: 'POST', signal: ac.signal, headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key }, body: JSON.stringify(body)
      });
      const txt = await res.text();
      let d = null; try { d = JSON.parse(txt); } catch (e) { d = null; }
      if (!res.ok) return { ok: false, status: res.status, msg: (d && d.error && d.error.message) || `HTTP ${res.status}` };
      const c = ((d && d.candidates) || [])[0] || {};
      const text = ((c.content && c.content.parts) || []).filter((p) => !p.thought && typeof p.text === 'string').map((p) => p.text).join('');
      if (!text) return { ok: false, status: 200, msg: `empty reply (${c.finishReason || 'no text'})` };
      const gm = c.groundingMetadata || {};
      return { ok: true, text, usage: d.usageMetadata || null, searches: Array.isArray(gm.webSearchQueries) ? gm.webSearchQueries.length : 0 };
    } catch (e) {
      return { ok: false, status: 0, msg: e && e.name === 'AbortError' ? 'timed out' : String((e && e.message) || e) };
    } finally { clearTimeout(timer); }
  }

  /** Builds a report from rows (see features) when one is due. getRows: async () => ({ rows, market }). */
  async function tick(getRows, now = Date.now(), force = false) {
    if (!key || !fetchFn || st.busy) return false;
    if (!force && now < nextRunAt(st.lastOkAt, st.lastTryAt, now)) return false;
    st.busy = true; st.lastTryAt = now; st.runs++;
    try {
      const { rows, market } = await getRows();
      if (!Array.isArray(rows) || rows.length < 3) { st.lastError = 'not enough Binance data for a report'; return false; }
      const asked = rows.map((x) => x[0]);
      // step 1: the news (if no model finds any, the report goes ahead on the numbers alone)
      // (Google returns the search details only some of the time; the prompt tokens it counts show the search results
      // were read in either way: about 110 tokens of question came back as 270-690 prompt tokens in the test.)
      let news = [], searches = 0;
      for (const m of models) {
        const res = await callModel(m, buildNewsRequest(asked, now));
        if (!res.ok) continue;
        news = parseNews(res.text); searches = res.searches;
        if (news.length) break;
      }
      // step 2: the scores
      const body = buildRequest(rows, market, news);
      for (const m of models) {
        const res = await callModel(m, body);
        if (!res.ok) { st.lastError = `${m}: ${res.msg}`.slice(0, 200); continue; }
        const rep = parseReport(res.text, asked);
        if (!rep) { st.lastError = `${m}: the reply was not a usable report`; continue; }
        rep.market.news = news.map((x) => (x.src ? `${x.h} (${x.src})` : x.h)).slice(0, 5);
        st.report = { at: Date.now(), model: m, ...rep, asked: asked.length, searches };
        st.lastOkAt = st.report.at; st.lastError = ''; st.model = m; st.usage = res.usage;
        const bears = Object.values(rep.coins).filter((x) => x.d === 'bear').length, bulls = Object.values(rep.coins).filter((x) => x.d === 'bull').length;
        log(`Momentum report (${m}; ${news.length ? `${news.length} news items${searches ? ` from ${searches} web searches` : ''}` : 'no news found this hour'}): ${Object.keys(rep.coins).length} of ${asked.length} coins read, ${bears} bearish, ${bulls} bullish; market ${rep.market.bias || 'unclear'}.`, 'info');
        return true;
      }
      log(`Momentum report failed (${st.lastError}). Trying again in 5 minutes.`, 'warning');
      return false;
    } catch (e) {
      st.lastError = String((e && e.message) || e).slice(0, 200);
      return false;
    } finally { st.busy = false; }
  }

  function load(saved) {
    if (!saved || typeof saved !== 'object' || !saved.report || !saved.report.coins) return;
    st.report = saved.report; st.lastOkAt = Number(saved.report.at) || 0; st.model = saved.report.model || '';
  }
  const toJSON = () => ({ report: st.report });
  const status = (now = Date.now()) => ({
    on: Boolean(key && fetchFn), at: st.lastOkAt || null, model: st.model || null, coins: st.report ? Object.keys(st.report.coins).length : 0,
    bias: st.report ? st.report.market.bias : null, fresh: Boolean(st.report && now - st.report.at <= RULES.maxAgeMs),
    nextAt: key ? nextRunAt(st.lastOkAt, st.lastTryAt, now) : null, lastError: st.lastError || null, busy: st.busy
  });
  return { tick, load, toJSON, status, report: () => st.report, _st: st };
}

module.exports = { create, features, buildRequest, buildNewsRequest, parseNews, parseReport, gate, leverageFor, nextRunAt, COLS, SYSTEM, RULES, DEFAULT_MODELS };
