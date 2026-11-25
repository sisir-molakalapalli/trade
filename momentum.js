'use strict';
/**
 * v6.33: the hourly MOMENTUM REPORT, written by Google Gemini (paid key) on the server.
 *
 * The user's theory (25 Nov 2026, from paper trading): the bags that drag on for days are the ones opened when the
 * coin had no momentum in the trade's direction. So every hour, just after the 1h candle closes, the engine sends
 * Gemini compact Binance USD-M data for the radar's top coins (1h trend, RSI, volume, taker flow, funding, open
 * interest), Gemini also searches the web for the latest crypto news, and it answers per coin: the direction of its
 * momentum (bear / bull / flat) and a 0-100 score (high = strong momentum likely to carry on, low = weak, choppy or
 * likely to reverse). v6.34 (the user's rule): the DIRECTION decides the trade (a short only where the momentum is
 * bearish, a long only where it is bullish); the SCORE is used only for leverage, with auto leverage on.
 * Every coin on the radar gets a reading (up to 150 USD-M futures, all the engine can trade), scored in batches of 50.
 *
 * Not replay-tested: a model that searches today's news can't be run honestly on past data. Every trade records the
 * score it was opened with, so paper trading can show whether high-score entries bag less.
 *
 * The key is GEMINI_MOMENTUM_KEY (or GEMINI_API_KEY) in the server's environment; it is sent only to Google, in a header.
 *
 * v6.40: far fewer tokens (the user found the report costly). The scoring call no longer "thinks" (thinking tokens are
 * billed as output and were most of the cost); every coin goes in ONE call (it was 50 + 50 + 1, each repeating the
 * instructions and the news); rows are short CSV lines (no price, two shape points instead of twelve closes); the reply
 * is a compact list with a short reason only for bearish and bullish coins; and the news search runs every 4 hours
 * for the top 12 coins instead of every hour for all of them, its result reused in between. Every report records the
 * tokens it used, shown in the app.
 */

const ENDPOINT = 'https://generativelanguage.googleapis.com/v1beta/models/';
const DEFAULT_MODELS = ['gemini-3.8-flash', 'gemini-flash-latest', 'gemini-3.6-flash'];
const RULES = {
  coins: 400,                 // v6.34: every coin on the radar (the radar holds up to 150 by default)
  batch: 150,                 // v6.40: coins per scoring request (one call for the whole radar; split evenly above that)
  newsEveryMs: 4 * 3600000,   // v6.40: the web search for news runs every 4 hours; the hours between reuse it
  newsCoins: 12,              // v6.40: the news search asks about the top 12 coins (and the market), not every coin
  maxAgeMs: 2 * 3600000,      // a report older than 2 hours no longer opens trades
  retryMs: 5 * 60000,         // a failed report is tried again 5 minutes later
  runAtMinute: 2,             // each hour at hh:02, after the 1h candle has closed
  timeoutMs: 180000,          // the web search can take a while
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
 * v6.40: no price (the reading is all relative), 1 decimal on most numbers, and the shape of the last day as five
 * changes (1h, 4h, 8h, 12h, 24h) instead of twelve hourly closes.
 */
const COLS = ['coin', 'ch1h%', 'ch4h%', 'ch8h%', 'ch12h%', 'ch24h%', 'vsEma20%', 'ema20vs50%', 'rsi', 'atr%', 'vol3h/24h', 'takerBuy%', 'fund%', 'oi1h%', 'oi24h%'];
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
    coin, r(pctCh(last, at(1))), r(pctCh(last, at(4)), 1), r(pctCh(last, at(8)), 1), r(pctCh(last, at(12)), 1), r(pctCh(last, at(24)), 1),
    r(pctCh(last, e20), 1), r(pctCh(e20, e50), 1), r(rsi(closes), 0), r(atr), r(vr, 1), r(tb, 0),
    Number.isFinite(num(funding)) ? r(num(funding) * 100, 3) : null,
    oiv.length >= 2 ? r(pctCh(oiNow, oiv[oiv.length - 2]), 1) : null,
    oiv.length >= 25 ? r(pctCh(oiNow, oiv[oiv.length - 25]), 1) : null
  ];
}

const SYSTEM = [
  'You are a crypto futures momentum analyst for a paper-trading autopilot (no real money) on Binance USD-M perpetuals, intraday.',
  'Input: one CSV row per coin. ch*% = price change over that many hours; vsEma20% = price vs its 1h EMA20; ema20vs50% = EMA20 vs EMA50;',
  'rsi = 1h RSI14; atr% = 1h ATR14; vol3h/24h = last 3h volume vs the 24h average; takerBuy% = taker-buy share of the last 3h;',
  'fund% = last funding rate (+ = longs pay); oi1h%/oi24h% = open interest change. Also recent news headlines with their tone.',
  'For EVERY coin judge its momentum over the next few hours: d = bear (falling with force), bull (rising with force) or flat (no clear push);',
  's = 0-100, how strong and how likely to carry on: 80+ strong and clean, 60-79 decent, 40-59 weak or mixed, under 40 none or about to reverse.',
  'Lower s for overextended moves (far from EMA20, extreme RSI), squeeze risk (price falling while open interest rises and funding is very',
  'negative; the mirror for longs), fading volume, news against the move. Raise it for an aligned trend, rising volume with taker flow on the',
  'same side, open interest building with the move, supportive news.',
  'Reply with JSON only: {"bias":"bear|bull|flat","c":[["SOL","bear",74,"short reason"],["ADA","flat",35]]}',
  'One entry per coin, symbol as given. A reason (at most 40 characters) only for bear and bull coins.'
].join('\n');

// Step 1: a short web search for the news. (With the whole data table in one request Gemini tended to skip the search and
// answer from memory; a short question that asks for a search does search: 6 queries and 13 sources in the test on 25 Nov.)
function buildNewsRequest(coins, now = Date.now()) {
  const text = `Use web search. It is ${new Date(now).toISOString().slice(0, 16)}Z. Find the most important crypto news of the last 24 hours: the overall market `
    + `(macro, ETF flows, liquidations, regulation, hacks) and anything specific to these coins: ${coins.join(', ')}. Reply with JSON only: `
    + '{"news":[{"h":"headline","src":"source site","coins":["SOL"],"tone":"bear|bull|mixed"}]} with at most 6 items, the most market-moving first, headlines under 100 characters.';
  return { contents: [{ role: 'user', parts: [{ text }] }], tools: [{ google_search: {} }], generationConfig: { temperature: 0.2, maxOutputTokens: 2048 } };
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
  })).filter((x) => x.h).slice(0, 6);
}
// Step 2: the scoring, with the data rows and the news (JSON mode, no tools). v6.40: plain text with CSV rows (fewer
// tokens than JSON), and no thinking: a reading of numbers doesn't need it, and its tokens were most of the bill.
const NO_THINKING = { thinkingBudget: 0 };
function buildRequest(rows, market, news = [], now = Date.now()) {
  const cell = (v) => (v == null || (typeof v === 'number' && !Number.isFinite(v)) ? '' : String(v));
  const lines = [`time ${new Date(now).toISOString().slice(0, 16)}Z`];
  if (market) lines.push(`market mode ${JSON.stringify(market)}`);
  if (news.length) { lines.push('news (tone|coins|headline):'); news.forEach((x) => lines.push(`${x.tone}|${(x.coins || []).join(' ')}|${x.h}`)); }
  lines.push(COLS.join(','));
  rows.forEach((row) => lines.push(row.map(cell).join(',')));
  return {
    systemInstruction: { parts: [{ text: SYSTEM }] },
    contents: [{ role: 'user', parts: [{ text: lines.join('\n') }] }],
    generationConfig: { responseMimeType: 'application/json', temperature: 0.3, maxOutputTokens: 8192, thinkingConfig: NO_THINKING }
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
  const list = d && typeof d === 'object' ? (Array.isArray(d.c) ? d.c : Array.isArray(d.coins) ? d.coins : null) : null;
  if (!list) return null;
  const want = new Set((asked || []).map((x) => String(x).toUpperCase()));
  const coins = {};
  for (let x of list) {
    if (Array.isArray(x)) x = { c: x[0], d: x[1], s: x[2], w: x[3] };   // v6.40: the compact ["SOL","bear",74,"why"]
    if (!x || typeof x !== 'object') continue;
    const c = String(x.c || x.coin || '').toUpperCase().replace(/USDT$/, '');
    const dir = String(x.d || x.dir || '').toLowerCase(), s = Math.round(Number(x.s ?? x.score));
    if (!want.has(c) || !['bear', 'bull', 'flat'].includes(dir) || !Number.isFinite(s)) continue;
    coins[c] = { d: dir, s: Math.max(0, Math.min(100, s)), w: clean(x.w || x.why || '', 110) };
  }
  if (Object.keys(coins).length < Math.min(3, want.size) || Object.keys(coins).length < want.size / 2) return null;   // too little to trade on
  const m = d.market && typeof d.market === 'object' ? d.market : { bias: d.bias };
  const bias = ['bear', 'bull', 'flat'].includes(String(m.bias).toLowerCase()) ? String(m.bias).toLowerCase() : null;
  return { market: { bias, news: [] }, coins };
}

/**
 * May the autopilot open this side on this coin? report: { at, coins }. side: 'SHORT' | 'LONG'.
 * v6.34: only the direction counts (bearish for a short, bullish for a long); the score never blocks a trade.
 * Returns { ok, reason, entry } (entry = the coin's reading when there is one).
 */
function gate(report, coin, side, { maxAgeMs = RULES.maxAgeMs, now = Date.now() } = {}) {
  if (!report || !report.at) return { ok: false, reason: 'no momentum report yet' };
  if (now - report.at > maxAgeMs) return { ok: false, reason: `the momentum report is ${Math.round((now - report.at) / 60000)} min old` };
  const c = String(coin || '').toUpperCase().replace(/USDT$/, ''), e = report.coins && report.coins[c];
  if (!e) return { ok: false, reason: 'not in this hour\'s momentum report' };
  const want = side === 'LONG' ? 'bull' : 'bear';
  if (e.d !== want) return { ok: false, reason: `momentum is ${e.d}, not ${want}`, entry: e };
  return { ok: true, reason: `${want} momentum (score ${e.s}, sets the leverage)`, entry: e };
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

function create({ apiKey = '', models = DEFAULT_MODELS, fetchFn = (typeof fetch === 'function' ? fetch : null), log = () => {}, batch = RULES.batch } = {}) {
  const key = String(apiKey || '').trim();
  const st = { report: null, lastOkAt: 0, lastTryAt: 0, lastError: '', model: '', busy: false, runs: 0, usage: null,
    news: null,                              // v6.40: { at, items, searches }: the last news search, reused for 4 hours
    day: { d: '', tokens: 0, calls: 0, reports: 0 } };   // v6.40: tokens used today (UTC)
  const dayOf = (t) => new Date(t).toISOString().slice(0, 10);
  // v6.40: every call's tokens are added up, failed replies included (they are billed too)
  function count(sum, u, now) {
    sum.calls++;
    if (u) {
      sum.prompt += Number(u.promptTokenCount) || 0; sum.output += Number(u.candidatesTokenCount) || 0;
      sum.thoughts += Number(u.thoughtsTokenCount) || 0; sum.search += Number(u.toolUsePromptTokenCount) || 0;
      sum.total += Number(u.totalTokenCount) || ((Number(u.promptTokenCount) || 0) + (Number(u.candidatesTokenCount) || 0) + (Number(u.thoughtsTokenCount) || 0));
    }
    if (st.day.d !== dayOf(now)) st.day = { d: dayOf(now), tokens: 0, calls: 0, reports: 0 };
    st.day.calls++; st.day.tokens += u ? (Number(u.totalTokenCount) || 0) : 0;
  }

  async function callModel(model, body) {
    const res = await callOnce(model, body);
    // a model that won't take "no thinking": once with a small budget, then without the setting
    const tc = body.generationConfig && body.generationConfig.thinkingConfig;
    if (!res.ok && res.status === 400 && tc && /thinking/i.test(res.msg)) {
      const small = await callOnce(model, { ...body, generationConfig: { ...body.generationConfig, thinkingConfig: { thinkingBudget: 512 } } });
      if (small.ok || !(small.status === 400 && /thinking/i.test(small.msg))) return small;
      const { thinkingConfig, ...gc } = body.generationConfig;
      return callOnce(model, { ...body, generationConfig: gc });
    }
    return res;
  }
  async function callOnce(model, body) {
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
      if (!text) return { ok: false, status: 200, msg: `empty reply (${c.finishReason || 'no text'})`, usage: (d && d.usageMetadata) || null };
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
      const sum = { calls: 0, prompt: 0, output: 0, thoughts: 0, search: 0, total: 0 };
      // step 1: the news (if no model finds any, the report goes ahead on the numbers alone)
      // (Google returns the search details only some of the time; the prompt tokens it counts show the search results
      // were read in either way: about 110 tokens of question came back as 270-690 prompt tokens in the test.)
      // v6.40: searched every 4 hours for the top coins; the hours between reuse the last result
      let fresh = false;
      if (!st.news || now - st.news.at >= RULES.newsEveryMs) {
        let items = [], searches = 0;
        for (const m of models) {
          const res = await callModel(m, buildNewsRequest(asked.slice(0, RULES.newsCoins), now));
          count(sum, res.usage, now);
          if (!res.ok) continue;
          items = parseNews(res.text); searches = res.searches;
          if (items.length) break;
        }
        // nothing found: try again next hour (not every coin's worth of searching again in 5 minutes)
        st.news = { at: items.length ? now : now - RULES.newsEveryMs + 3600000, items, searches };
        fresh = true;
      }
      const news = st.news.items || [], searches = fresh ? st.news.searches : 0;
      // step 2: the scores: v6.40 every coin in one call (split evenly above 150). A batch that no model answers leaves its
      // coins out of this report (they can't be traded this hour); at least one batch must succeed.
      const coins = {}, biases = []; let used = '';
      const parts = Math.max(1, Math.ceil(rows.length / batch));
      for (let p = 0; p < parts; p++) {
        const part = rows.slice(Math.round(p * rows.length / parts), Math.round((p + 1) * rows.length / parts));
        const names = part.map((x) => x[0]), body = buildRequest(part, market, news, now);
        for (const m of models) {
          const res = await callModel(m, body);
          count(sum, res.usage, now);
          if (!res.ok) { st.lastError = `${m}: ${res.msg}`.slice(0, 200); continue; }
          const rep = parseReport(res.text, names);
          if (!rep) { st.lastError = `${m}: the reply was not a usable report`; continue; }
          Object.assign(coins, rep.coins); if (rep.market.bias) biases.push(rep.market.bias);
          used = used || m; st.usage = res.usage;
          break;
        }
      }
      if (!Object.keys(coins).length) { log(`Momentum report failed (${st.lastError}). Trying again in 5 minutes.`, 'warning'); return false; }
      const tally = (d) => Object.values(coins).filter((x) => x.d === d).length;
      const bias = biases.length ? ['bear', 'bull', 'flat'].sort((a, b) => biases.filter((x) => x === b).length - biases.filter((x) => x === a).length)[0] : null;
      st.report = { at: Date.now(), model: used, market: { bias, news: news.map((x) => (x.src ? `${x.h} (${x.src})` : x.h)).slice(0, 5) }, coins, asked: asked.length, searches,
        newsAt: st.news.at, usage: sum };
      st.lastOkAt = st.report.at; st.model = used; st.day.reports++;
      const missing = asked.length - Object.keys(coins).length;
      st.lastError = missing ? `${missing} coin${missing === 1 ? '' : 's'} got no reading this hour` : '';
      log(`Momentum report (${used}; ${news.length ? `${news.length} news items${fresh ? (searches ? ` from ${searches} web searches` : ' (new search)') : ' (from the last search)'}` : 'no news found'}): ${Object.keys(coins).length} of ${asked.length} coins read, ${tally('bear')} bearish, ${tally('bull')} bullish, ${tally('flat')} flat; market ${bias || 'unclear'}; ${sum.total} tokens in ${sum.calls} call${sum.calls === 1 ? '' : 's'}.`, 'info');
      return true;
    } catch (e) {
      st.lastError = String((e && e.message) || e).slice(0, 200);
      return false;
    } finally { st.busy = false; }
  }

  function load(saved) {
    if (!saved || typeof saved !== 'object' || !saved.report || !saved.report.coins) return;
    st.report = saved.report; st.lastOkAt = Number(saved.report.at) || 0; st.model = saved.report.model || '';
    if (saved.news && Array.isArray(saved.news.items)) st.news = { at: Number(saved.news.at) || 0, items: saved.news.items, searches: Number(saved.news.searches) || 0 };
    if (saved.day && typeof saved.day === 'object') st.day = { d: String(saved.day.d || ''), tokens: Number(saved.day.tokens) || 0, calls: Number(saved.day.calls) || 0, reports: Number(saved.day.reports) || 0 };
  }
  const toJSON = () => ({ report: st.report, news: st.news, day: st.day });
  const status = (now = Date.now()) => ({
    on: Boolean(key && fetchFn), at: st.lastOkAt || null, model: st.model || null, coins: st.report ? Object.keys(st.report.coins).length : 0,
    bias: st.report ? st.report.market.bias : null, fresh: Boolean(st.report && now - st.report.at <= RULES.maxAgeMs),
    nextAt: key ? nextRunAt(st.lastOkAt, st.lastTryAt, now) : null, lastError: st.lastError || null, busy: st.busy,
    usage: st.report && st.report.usage ? st.report.usage : null,                         // v6.40: tokens of the last report
    today: st.day.d === dayOf(now) ? { tokens: st.day.tokens, calls: st.day.calls, reports: st.day.reports } : { tokens: 0, calls: 0, reports: 0 },
    newsAt: st.news ? st.news.at : null
  });
  return { tick, load, toJSON, status, report: () => st.report, _st: st };
}

module.exports = { create, features, buildRequest, buildNewsRequest, parseNews, parseReport, gate, leverageFor, nextRunAt, COLS, SYSTEM, RULES, DEFAULT_MODELS };
