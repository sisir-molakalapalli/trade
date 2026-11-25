'use strict';
/**
 * v6.31: character comments written by Google Gemini, on the server.
 *
 * Four characters speak in the app:
 *   Pip               the scooter courier, on open positions, recovery bags and manual positions
 *   Master Aldric     the medieval reckoner, on closed trades (autopilot and manual)
 *   Sergeant Rook     the radar operator at his desk, on the radar's top coins
 *   Professor Quill   the market analyst who walks the Market mode card
 *
 * One Gemini call writes whatever is due for all of them (new trades, a changed mood, a stale report), at most once a
 * minute and at most DAILY_CAP times a day (the free tier). The key comes from GEMINI_API_KEY and never leaves the
 * server: the app only ever reads the finished comments from GET /api/commentary. Only trade and market numbers are
 * sent (coins, sides, percentages, durations, paper P&L), never URLs, sheet ids or personal details.
 * When Gemini is busy, out of quota or switched off, nothing breaks: the app falls back to the built-in lines.
 */

const ENDPOINT = 'https://generativelanguage.googleapis.com/v1beta/models/';
// v6.32: the free tier is often overloaded, so several models (each has its own capacity); 2.5 models are closed to new users
const DEFAULT_MODELS = ['gemini-flash-lite-latest', 'gemini-3.5-flash-lite', 'gemini-3.1-flash-lite', 'gemini-3.6-flash', 'gemini-flash-latest', 'gemini-3.8-flash', 'gemini-3.7-flash'];
const LIMITS = {
  minGapMs: 60000,          // at most one call a minute
  dailyCap: 600,            // answered calls a day (UTC); busy replies don't count
  maxAttempts: 2000,        // every request a day, answered or not
  timeoutMs: 30000,
  pipRefreshMs: 10 * 60000, // Pip gets fresh notes on a trade every 10 minutes even if its mood is unchanged
  pipMoodGapMs: 75000,      // ...or when its mood changes, at most once every 75 s per trade
  soldierMs: 4 * 60000,     // new radar reports every 4 minutes
  analystMs: 5 * 60000,     // new market opinions every 5 minutes, or when the market mode changes
  nearDue: 0.6,             // a call that goes anyway also refreshes whoever is 60% of the way to due (fewer calls)
  maxPip: 6, maxAldric: 5, maxRadar: 8, keepAldric: 300, keepPip: 80,
  // v6.32: several different lines per call, so a character never has to repeat itself
  counts: { pip: 3, aldricMore: 2, soldier: 10, analyst: 6 },
  maxChars: { pip: 150, aldric: 170, soldier: 130, analyst: 190 }
};

const SYSTEM = [
  'You write short in-character comments for four characters in a PAPER-TRADING crypto app (no real money). The user is their sovereign.',
  'Characters and voices:',
  '- Sir Edmund, the Loyal Minister (JSON key "minister"): a devoted royal minister who inspects every open position and recovery bag for his sovereign',
  '  and gives frank, personal counsel. Courtly but plain modern English (not Old English), addresses the user as "Your Majesty" or "Sire" now and then.',
  '  Each note takes a clear stance (pleased, confident, uneasy, worried, impatient) and gives the reason from the trade\'s own numbers (move %, how long',
  '  it has been open or in the bag, leverage, exit stage, momentum at entry), sometimes with advice in character (patience, vigilance, courage).',
  '  Loyal but candid: never flatters when the numbers are bad. Max 130 characters.',
  '- Master Aldric: a medieval accountant who audits closed trades in his ledger. Old English (thee, thou, hath, verily). Max 140 characters.',
  '- Sergeant Rook: a veteran radar operator at his desk, dry humour, strong opinions. Each line is HIS PERSONAL OPINION about one coin (or a comparison',
  '  of two): what he likes, distrusts or doubts and WHY, from that coin\'s numbers (score, 24h move, where it sits in its day range, its momentum',
  '  reading, whether we already hold it), plus his gut feeling or experience ("I like", "I don\'t trust", "my gut says", "I\'ve seen this before").',
  '  First person. Never the "Contact: X, score Y" report format, never two lines built the same way; radio words like "over" at most twice in all.',
  '  Max 120 characters per line.',
  '- Professor Quill: a dry-witted market analyst who reads the market data and gives his opinion. Max 170 characters per line.',
  'Rules: mention the coin and use only the numbers you are given; never invent prices or facts.',
  'Write proper sentences with punctuation. Percentages with a % sign and a sign (+0.84%, -14.9%), amounts in dollars ($0.14), durations in words as given.',
  'Every line is an opinion or a feeling with a reason, not a list of numbers. Vary the wording; never start two comments the same way.',
  'A trap is a pump-and-dump the engine refuses to touch. A recovery bag is a trade that fell 3.5% and waits to get back to its entry. Score 65 or more means a coin qualifies.',
  'Professor Quill interprets: what the market mode, BTC against its 1h EMA50 and the share of coins above their EMA50 mean for the autopilot right now; one line may judge the recent record.',
  'Be playful, varied and kind; no profanity, no emojis, no hashtags, no links. Never tell anyone to buy, sell or risk real money.',
  'They are living characters with memory and moods: never repeat or closely paraphrase anything in lastSaid, previously_said or previous_opinions.',
  'Let them react to change (better, worse, still stuck), recall what they said before, change their minds when the numbers change, tease each other now and then, and show a personal touch',
  '(Sir Edmund: the court, the treasury, his duty; Aldric: candles, the guild, the king; Rook: coffee, the night shift, his old sergeant; Quill: his tea, his old papers, his students). Keep every line about the data.',
  'Reply with JSON only, exactly this shape (leave out any part you were not asked for):',
  '{"minister":[{"id":"p1","texts":["...","...","..."]}],"aldric":[{"id":"a1","text":"...","more":["...","..."]}],"soldier":["...",...],"analyst":["...",...]}',
  'For each minister id write 3 different notes (each a new angle on that trade). For each aldric id write a verdict in "text" and 2 further remarks in "more".',
  'Write 8 to 10 soldier lines (opinions on different coins, each coin at most twice, at most one about his coffee or shift) and 5 or 6 analyst lines, each from a different angle.'
].join('\n');

const r2 = (x) => (Number.isFinite(Number(x)) ? Math.round(Number(x) * 100) / 100 : null);
const dayOf = (t) => new Date(t).toISOString().slice(0, 10);

/** Same moods as the app's Pip, so a changed mood means a new note. */
function moodOf(item, now = Date.now()) {
  const g = Number(item.gainPct) || 0;
  if (item.kind === 'bag') return g >= 0 ? 'above' : g > -0.3 ? 'close' : g > -1.5 ? 'mid' : g > -4 ? 'deep' : 'abyss';
  if (Number(item.openedAt) > 0 && now - Number(item.openedAt) < 5 * 60000) return 'new';
  return g >= 1.2 ? 'rocket' : g >= 0.3 ? 'up' : g >= 0.05 ? 'tiny' : g > -0.05 ? 'flat' : g > -0.5 ? 'dip' : g > -1.5 ? 'down' : 'ouch';
}

/** One line of plain text: no control characters, no links, no markup, at most max characters. */
function clean(s, max = 160) {
  let t = String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]+/g, ' ').replace(/[<>`]/g, '').replace(/\s+/g, ' ').trim();
  t = t.replace(/^["'“”‘’]+|["'“”‘’]+$/g, '').trim();
  if (!t || /https?:|www\.|\.com\b/i.test(t)) return '';
  if (t.length > max) { const cut = t.slice(0, max - 1); t = `${cut.slice(0, Math.max(cut.lastIndexOf(' '), max - 25)).trim()}…`; }
  return t;
}

/**
 * Decides what is due and builds the request. snap: { now, positions, bags, manual, closed, radar, market, account }
 * (see server.js aiSnapshot). store: the comments so far. Returns null when nothing is due.
 */
function plan(snap, store, now = Date.now(), L = LIMITS) {
  const jobs = { pip: [], aldric: [], soldier: false, analyst: false };
  const open = [...(snap.positions || []), ...(snap.bags || []), ...(snap.manual || [])];
  const pipDue = open.map((it) => ({ it, mood: moodOf(it, now), old: store.pip[it.key] }))
    .filter(({ mood, old }) => !old || (old.mood !== mood && now - old.at > L.pipMoodGapMs) || now - old.at > L.pipRefreshMs)
    .sort((a, b) => (a.old ? 1 : 0) - (b.old ? 1 : 0))
    .slice(0, L.maxPip);
  pipDue.forEach(({ it, mood, old }, i) => jobs.pip.push({ id: `p${i + 1}`, key: it.key, mood, it, last: old && Array.isArray(old.texts) ? old.texts.slice(0, 3) : [] }));
  (snap.closed || []).filter((t) => !store.aldric[t.key]).slice(0, L.maxAldric)
    .forEach((t, i) => jobs.aldric.push({ id: `a${i + 1}`, key: t.key, it: t }));
  const radar = (snap.radar || []).slice(0, L.maxRadar), mk = snap.market || null;
  const age = (x) => now - (Number(x.at) || 0);
  const soldierDue = radar.length > 0 && (!store.soldier.lines.length || age(store.soldier) > L.soldierMs);
  const analystDue = Boolean(mk && mk.mode) && (!store.analyst.lines.length || age(store.analyst) > L.analystMs || store.analyst.mode !== mk.mode);
  if (!jobs.pip.length && !jobs.aldric.length && !soldierDue && !analystDue) return null;
  // a call is going anyway: whoever is most of the way to due comes along (one call instead of two)
  jobs.soldier = soldierDue || (radar.length > 0 && age(store.soldier) > L.soldierMs * L.nearDue);
  jobs.analyst = analystDue || (Boolean(mk && mk.mode) && age(store.analyst) > L.analystMs * L.nearDue);

  const strip = ({ key, openedAt, ...rest }) => rest;   // the app's keys and timestamps are not needed by Gemini
  const input = { ask: [] };
  if (jobs.pip.length) { input.ask.push('minister'); input.minister_positions_and_bags = jobs.pip.map((j) => ({ id: j.id, mood: j.mood, ...strip(j.it), ...(j.last.length ? { lastSaid: j.last } : {}) })); }
  if (jobs.aldric.length) { input.ask.push('aldric'); input.aldric_closed_trades = jobs.aldric.map((j) => ({ id: j.id, ...strip(j.it) })); }
  if (jobs.soldier) { input.ask.push('soldier'); input.soldier_radar_coins = radar; if (store.soldier.lines.length) input.soldier_previously_said = store.soldier.lines.slice(0, 10); }
  if (jobs.analyst) {
    input.ask.push('analyst'); input.analyst_market = mk; input.analyst_account = snap.account || null;
    if (store.analyst.lines.length) input.analyst_previous_opinions = store.analyst.lines.slice(0, 6);
  }
  const body = {
    systemInstruction: { parts: [{ text: SYSTEM }] },
    contents: [{ role: 'user', parts: [{ text: JSON.stringify(input) }] }],
    // no thinking: with it the flash models took 35-45 s and ran out of room before the JSON ended
    generationConfig: { responseMimeType: 'application/json', temperature: 1.0, maxOutputTokens: 3072, thinkingConfig: { thinkingBudget: 0 } }
  };
  return { jobs, body, input };
}

/** Reads Gemini's reply into the comments that were asked for; anything else is ignored. */
function parseReply(text, jobs) {
  let d = null;
  const raw = String(text || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
  try { d = JSON.parse(raw); } catch (e) {
    const a = raw.indexOf('{'), b = raw.lastIndexOf('}');
    if (a >= 0 && b > a) { try { d = JSON.parse(raw.slice(a, b + 1)); } catch (e2) { d = null; } }
  }
  if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
  const out = { pip: {}, aldric: {}, soldier: [], analyst: [] };
  const byId = (arr) => { const m = {}; (Array.isArray(arr) ? arr : []).forEach((x) => { if (x && typeof x === 'object' && typeof x.id === 'string') m[x.id] = x; }); return m; };
  const asList = (v) => (Array.isArray(v) ? v : v == null ? [] : [v]);
  // cleaned, no duplicates, nothing that was said last time
  const lines = (arr, max, n, old = []) => {
    const seen = new Set(old.map((x) => String(x).toLowerCase())), keep = [];
    for (const x of asList(arr)) {
      const t = clean(typeof x === 'string' ? x : x && x.text, max), k = t.toLowerCase();
      if (t && !seen.has(k)) { seen.add(k); keep.push(t); }
      if (keep.length >= n) break;
    }
    return keep;
  };
  const pip = byId(d.minister || d.pip), ald = byId(d.aldric);   // v6.36: the minister (was Pip)
  jobs.pip.forEach((j) => {
    const x = pip[j.id] || {}, texts = lines([...asList(x.texts), ...asList(x.text)], LIMITS.maxChars.pip, 4, j.last || []);
    if (texts.length) out.pip[j.key] = { texts, mood: j.mood };
  });
  jobs.aldric.forEach((j) => {
    const x = ald[j.id] || {}, t = clean(x.text, LIMITS.maxChars.aldric);
    if (t) out.aldric[j.key] = { text: t, more: lines(x.more, LIMITS.maxChars.aldric, 3, [t]) };
  });
  if (jobs.soldier) out.soldier = lines(d.soldier, LIMITS.maxChars.soldier, 12, jobs.soldierOld || []);
  if (jobs.analyst) out.analyst = lines(d.analyst, LIMITS.maxChars.analyst, 8, jobs.analystOld || []);
  return out;
}

function create({ apiKey = '', models = DEFAULT_MODELS, fetchFn = (typeof fetch === 'function' ? fetch : null), log = () => {}, limits = {} } = {}) {
  const L = { ...LIMITS, ...limits };
  const key = String(apiKey || '').trim();
  const store = { pip: {}, aldric: {}, soldier: { lines: [], at: 0 }, analyst: { lines: [], at: 0, mode: null } };
  const st = { rev: 0, day: '', calls: 0, okToday: 0, okCalls: 0, lastCallAt: 0, lastOkAt: 0, lastError: '', backoffUntil: 0, backoffMs: 0, model: '', busy: false, coolUntil: {} };

  function load(saved) {
    if (!saved || typeof saved !== 'object') return;
    const many = (v, max, n) => (Array.isArray(v) ? v : v == null ? [] : [v]).map((x) => clean(x, max)).filter(Boolean).slice(0, n);
    for (const [k, v] of Object.entries(saved.pip || {})) {   // v6.31 saved one text, v6.32 several
      const texts = v ? many(v.texts || v.text, L.maxChars.pip, 4) : [];
      if (texts.length) store.pip[k] = { texts, mood: String(v.mood || ''), at: Number(v.at) || 0 };
    }
    for (const [k, v] of Object.entries(saved.aldric || {})) if (v && typeof v.text === 'string') store.aldric[k] = { text: clean(v.text, L.maxChars.aldric), more: many(v.more, L.maxChars.aldric, 3), at: Number(v.at) || 0 };
    if (saved.soldier && Array.isArray(saved.soldier.lines)) store.soldier = { lines: many(saved.soldier.lines, L.maxChars.soldier, 12), at: Number(saved.soldier.at) || 0 };
    if (saved.analyst && Array.isArray(saved.analyst.lines)) store.analyst = { lines: many(saved.analyst.lines, L.maxChars.analyst, 8), at: Number(saved.analyst.at) || 0, mode: saved.analyst.mode || null };
    if (saved.day) { st.day = String(saved.day); st.calls = Number(saved.calls) || 0; st.okToday = Number(saved.okToday) || 0; }
    st.rev = 1;
  }
  const toJSON = () => ({ ...store, day: st.day, calls: st.calls, okToday: st.okToday });
  const view = () => ({ rev: st.rev, ...store });
  const status = () => ({
    on: Boolean(key && fetchFn), rev: st.rev, model: st.model || null, callsToday: st.okToday, attemptsToday: st.calls, dailyCap: L.dailyCap,
    lastOkAt: st.lastOkAt || null, lastError: st.lastError || null, backoffUntil: st.backoffUntil > Date.now() ? st.backoffUntil : null
  });

  function prune(snap, now) {
    const live = new Set([...(snap.positions || []), ...(snap.bags || []), ...(snap.manual || [])].map((x) => x.key));
    let changed = false;
    for (const k of Object.keys(store.pip)) if (!live.has(k)) { delete store.pip[k]; changed = true; }
    const ks = Object.keys(store.aldric);
    if (ks.length > L.keepAldric) { ks.sort((a, b) => store.aldric[a].at - store.aldric[b].at).slice(0, ks.length - L.keepAldric).forEach((k) => delete store.aldric[k]); changed = true; }
    if (changed) st.rev++;
    return now;
  }

  async function callModel(model, body) {
    const r = await callOnce(model, body);
    // a model that doesn't take the thinking setting: once more without it
    if (!r.ok && r.status === 400 && /thinking/i.test(r.msg) && body.generationConfig && body.generationConfig.thinkingConfig) {
      const { thinkingConfig, ...gc } = body.generationConfig;
      return callOnce(model, { ...body, generationConfig: gc });
    }
    return r;
  }
  async function callOnce(model, body) {
    const ac = new AbortController(), timer = setTimeout(() => ac.abort(), L.timeoutMs);
    try {
      const r = await fetchFn(`${ENDPOINT}${encodeURIComponent(model)}:generateContent`, {
        method: 'POST', signal: ac.signal,
        headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key },
        body: JSON.stringify(body)
      });
      const txt = await r.text();
      let d = null; try { d = JSON.parse(txt); } catch (e) { d = null; }
      if (!r.ok) return { ok: false, status: r.status, msg: (d && d.error && d.error.message) || `HTTP ${r.status}` };
      const parts = (((d && d.candidates) || [])[0] || {}).content;
      const text = ((parts && parts.parts) || []).filter((p) => !p.thought && typeof p.text === 'string').map((p) => p.text).join('');
      if (!text) return { ok: false, status: 200, msg: `empty reply (${(((d && d.candidates) || [])[0] || {}).finishReason || 'no text'})` };
      return { ok: true, text, usage: d.usageMetadata || null };
    } catch (e) {
      return { ok: false, status: 0, msg: e && e.name === 'AbortError' ? 'timed out' : String((e && e.message) || e) };
    } finally { clearTimeout(timer); }
  }

  /** Runs at most one call when something is due. Resolves true when stored comments changed. */
  async function tick(snap, now = Date.now()) {
    if (!key || !fetchFn || st.busy || !snap) return false;
    const before = st.rev;
    prune(snap, now);
    if (dayOf(now) !== st.day) { st.day = dayOf(now); st.calls = 0; st.okToday = 0; }
    if (now < st.backoffUntil || now - st.lastCallAt < L.minGapMs || st.okToday >= L.dailyCap || st.calls >= L.maxAttempts) return st.rev !== before;
    const p = plan(snap, store, now, L);
    if (!p) return st.rev !== before;
    p.jobs.soldierOld = store.soldier.lines; p.jobs.analystOld = store.analyst.lines;   // a new batch must not repeat the last one
    st.busy = true; st.lastCallAt = now;
    try {
      let res = null, used = '';
      for (const m of models) {
        if ((st.coolUntil[m] || 0) > now) continue;
        st.calls++;
        const r = await callModel(m, p.body);
        if (r.ok) { res = r; used = m; break; }
        st.lastError = `${m}: ${r.msg}`.slice(0, 200);
        // overloaded or out of quota: try the next model; a model that is gone or refused rests for 6 hours
        st.coolUntil[m] = now + (r.status === 429 ? 3 * 60000 : [400, 403, 404].includes(r.status) ? 6 * 3600000 : 60000);
        if (st.calls >= L.maxAttempts) break;
      }
      if (!res) {
        st.backoffMs = Math.min(30 * 60000, st.backoffMs ? st.backoffMs * 2 : 2 * 60000);
        st.backoffUntil = now + st.backoffMs;
        log(`Gemini didn't answer (${st.lastError}). The characters use their own lines; next try in ${Math.round(st.backoffMs / 60000)} min.`, 'warning');
        return st.rev !== before;
      }
      const out = parseReply(res.text, p.jobs);
      if (!out) { st.lastError = `${used}: reply was not the expected JSON`; st.backoffUntil = now + 2 * 60000; return st.rev !== before; }
      st.backoffMs = 0; st.lastOkAt = now; st.okCalls++; st.okToday++; st.lastError = ''; st.model = used;
      const t = Date.now();
      for (const [k, v] of Object.entries(out.pip)) store.pip[k] = { ...v, at: t };
      for (const [k, v] of Object.entries(out.aldric)) store.aldric[k] = { ...v, at: t };
      if (out.soldier.length) store.soldier = { lines: out.soldier, at: t };
      if (out.analyst.length) store.analyst = { lines: out.analyst, at: t, mode: (snap.market && snap.market.mode) || null };
      const n = Object.keys(out.pip).length + Object.keys(out.aldric).length + out.soldier.length + out.analyst.length;
      if (n) st.rev++;
      if (st.okCalls === 1) log(`Gemini (${used}) is writing the characters' comments.`, 'success');
      return st.rev !== before;
    } finally { st.busy = false; }
  }

  return { tick, load, toJSON, view, status, store, _st: st };
}

module.exports = { create, plan, parseReply, clean, moodOf, SYSTEM, LIMITS, DEFAULT_MODELS, r2 };
