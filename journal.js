'use strict';
/**
 * =========================================================================
 * v6.47 TRADE JOURNAL: every trade with every number the engine had, kept for analysis later.
 * =========================================================================
 * - open():   when a trade opens, what the engine saw: its checks (order flow, momentum, trend, candles, order book,
 *             the momentum report, the signal and the sizing), the coin's radar row and funding, the market (mode,
 *             breadth, BTC prediction, bull fading), the account and the settings.
 * - sample(): while it is open (every 5 s): the best and the worst move and when, the move every 5 minutes (the path),
 *             and each change of its exit stage, stop, take profit, leverage, and its move to a recovery bag.
 * - close():  one record per close, a partial close too (as in the ledger), with the market and the coin at the close.
 * The engine keeps the last 1,000 records (carried to a new build at a handover) and sends each one to the sheet's
 * TradeJournal tab (ledger script v3, tools/ledger_apps_script.gs). An older script is told apart by its reply, and the
 * records wait on the engine until the script is updated. Nothing here changes a trade.
 */
const fs = require('fs');

const VERSION = 'v6.47';
const RULES = {
  sampleMs: 5000,             // how often open trades are read (best / worst move)
  pathEveryMs: 5 * 60000,     // one point of the path every 5 minutes
  pathMax: 1200,              // above this the path keeps every other point (and its spacing doubles)
  eventsMax: 300,
  stopEventPct: 0.10,         // a stop move this big (in % of the entry) is written as an event
  keep: 1000,                 // records kept on the engine
  batch: 25,                  // records per write to the sheet
  retryMs: 60000, retryMaxMs: 15 * 60000, oldScriptRetryMs: 15 * 60000,
  goneAfterMs: 30 * 60000,    // an open trade the engine no longer holds, with no close recorded, is written after this
  strMax: 400, depth: 5, arrMax: 40,
  saveDelayMs: 10000
};

const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
const round = (n, d = 4) => (Number.isFinite(Number(n)) ? Number(Number(n).toFixed(d)) : null);
const iso = (t) => (Number(t) > 0 ? new Date(Number(t)).toISOString() : null);
// never an address, a key or a password in a record
const PRIVATE = /url$|^url|apikey|token|secret|password|ledger/i;

/** Numbers, text, true/false and small objects, as they are; kline arrays, maps and functions are left out. */
function clean(v, depth = RULES.depth) {
  if (v === null || v === undefined) return undefined;
  if (typeof v === 'number') return Number.isFinite(v) ? Number(v.toPrecision(10)) : undefined;
  if (typeof v === 'boolean') return v;
  if (typeof v === 'string') return v.length > RULES.strMax ? v.slice(0, RULES.strMax) : v;
  if (typeof v !== 'object' || depth <= 0) return undefined;
  if (v instanceof Map || v instanceof Set || Buffer.isBuffer(v)) return undefined;
  if (Array.isArray(v)) {
    if (v.length > RULES.arrMax || (v.length > 10 && v.some(Array.isArray))) return undefined;   // klines, book levels
    return v.map((x) => clean(x, depth - 1)).filter((x) => x !== undefined);
  }
  const out = {};
  for (const [k, x] of Object.entries(v)) {
    if (k.startsWith('_') || PRIVATE.test(k)) continue;
    const c = clean(x, depth - 1);
    if (c !== undefined) out[k] = c;
  }
  return out;
}

const isShortPos = (p) => String((p && p.side) || '').toUpperCase() === 'SHORT';
/** The price move in the trade's favour, in % of the entry. */
function movePct(pos, price) {
  const e = num(pos && pos.entryPriceUSDT), p = num(price);
  if (!(e > 0) || !(p > 0)) return null;
  return ((isShortPos(pos) ? e - p : p - e) / e) * 100;
}
// what is watched for changes while a trade is open
function watchOf(pos) {
  const f = pos.fib || {};
  return {
    stage: pos.ratchetStage || null, bagTarget: pos.targetStep || null,
    takeProfitPct: num(pos.userTpPct) || num(pos.takeProfitPct) || null, leverage: pos.leverage ? round(pos.leverage, 2) : null,
    tp1Secured: Boolean(pos.tp1Secured), inBag: Boolean(pos.movedAt), bullFadeClose: pos.fadeClosePrice ? round(pos.fadeClosePrice, 10) : null,
    fibTp1: Boolean(f.tp1Hit), fibStop: f.sl ? round(f.sl, 10) : null
  };
}

/** One record as one sheet row: summary columns first, then every field flattened ("entry.checks.flow.takerBuyRatio"). */
function flatten(obj, prefix = '', out = {}) {
  for (const [k, v] of Object.entries(obj || {})) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === 'object' && !Array.isArray(v)) flatten(v, key, out);
    else if (Array.isArray(v)) out[key] = JSON.stringify(v);
    else if (v !== undefined) out[key] = v;
  }
  return out;
}
function istDate(t) { return new Date(t).toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }); }
function istTime(t) { return new Date(t).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false }); }
function sheetRow(rec) {
  const r = { ...rec }; delete r._sent;
  const head = {
    'Journal ID': rec.journalId, 'Trade ID': rec.tradeId || '', 'Date (IST)': istDate(rec.closedAt), 'Time (IST)': istTime(rec.closedAt),
    Pair: rec.pair, Side: rec.side, Strategy: rec.strategy, 'Entry type': rec.entryType, 'Exit type': rec.exitType,
    'Entry (USDT)': rec.entryPrice, 'Exit (USDT)': rec.exitPrice, 'Move %': rec.movePct, 'PnL (USDT)': rec.pnlUSDT,
    'Hold (min)': rec.holdMin, 'Best %': rec.best ? rec.best.pct : '', 'Worst %': rec.worst ? rec.worst.pct : '', 'Went to bag': rec.bagAt ? 'yes' : 'no'
  };
  const path = (rec.path || []).map((p) => `${p[0]}:${p[1]}`).join(' ');
  delete r.path;
  const flat = flatten(r);
  let json = JSON.stringify(rec, (k, v) => (k === '_sent' ? undefined : v));
  if (json.length > 49000) json = JSON.stringify({ ...rec, path: undefined, _sent: undefined, events: (rec.events || []).slice(-60) }).slice(0, 49000);
  return { ...head, ...flat, path: path.slice(0, 49000), json };
}

function csvOf(records) {
  const rows = records.map(sheetRow).map((r) => { delete r.json; return r; });
  const cols = [];
  rows.forEach((r) => Object.keys(r).forEach((k) => { if (!cols.includes(k)) cols.push(k); }));
  const cell = (v) => {
    if (v === undefined || v === null) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.map(cell).join(','), ...rows.map((r) => cols.map((c) => cell(r[c])).join(','))].join('\n');
}

function create({ file = null, post = null, ledgerUrl = () => '', canSend = () => true, context = () => ({}), coin = () => ({}),
  feeRate = 0.0005, log = () => {}, now = () => Date.now(), rules = {} } = {}) {
  const R = { ...RULES, ...rules };
  const st = { open: {}, records: [], sentTotal: 0, lost: 0, since: 0, sheet: 'unknown', lastSentAt: 0, lastError: '', nextTryAt: 0,
    failMs: 0, busy: false, warnedOld: false, saveTimer: null, lastSampleAt: 0 };

  const keyOf = (book, k, pos) => (book === 'manual' ? `manual:${pos.id || k}` : `${k}@${num(pos.openedAt)}`);
  const safe = (fn, fallback) => { try { return fn(); } catch (e) { return fallback; } };

  function trackEntry(key, book, k, pos, extra, t) {
    const opened = num(pos.openedAt) || t;
    const e = {
      key, k, pair: pos.pair || `${String(k).replace(/USDT$/, '')}/USDT`, side: isShortPos(pos) ? 'SHORT' : 'LONG', openedAt: opened,
      book, strategy: extra.strategy || (pos.fib ? 'FIB' : pos.manual ? 'MANUAL' : pos.entrySource || 'CANDLE'),
      entryType: pos.candlestickPattern || extra.entryType || null,
      entry: {
        recordedAt: iso(t), exact: !extra.late,
        ...(extra.late ? { note: `the trade was open before the journal (${VERSION}) started: no entry checks; the market is from when it was first seen` } : {}),
        checks: clean(extra.checks) || {}, signal: clean(extra.signal), plan: clean(extra.plan),
        position: clean(pos), coin: clean(safe(() => coin(k, pos), {})), ...clean(safe(() => context(), {}))
      },
      best: { pct: 0, at: opened, price: num(pos.entryPriceUSDT) }, worst: { pct: 0, at: opened, price: num(pos.entryPriceUSDT) },
      path: [[0, 0]], pathEveryMs: R.pathEveryMs, lastPathAt: opened, events: [], watch: watchOf(pos), stop: num(pos.sl) > 0 ? round(pos.sl, 10) : null, closes: 0, seenAt: t
    };
    st.open[key] = e;
    if (!st.since) st.since = t;
    return e;
  }

  /** A trade just opened: what the engine saw. extra: { strategy, checks, signal, plan } */
  function open(book, k, pos, extra = {}) {
    if (!pos) return null;
    const t = now();
    const e = trackEntry(keyOf(book, k, pos), book, k, pos, extra, t);
    saveSoon();
    return e.key;
  }

  function note(e, pos, price, t) {
    const pct = movePct(pos, price);
    if (pct === null) return;
    if (pct > e.best.pct) e.best = { pct: round(pct, 3), at: t, price };
    if (pct < e.worst.pct) e.worst = { pct: round(pct, 3), at: t, price };
    if (t - e.lastPathAt >= e.pathEveryMs) {
      e.path.push([round((t - e.openedAt) / 60000, 1), round(pct, 3)]);
      e.lastPathAt = t;
      if (e.path.length > R.pathMax) { e.path = e.path.filter((_, i) => i % 2 === 0); e.pathEveryMs *= 2; }
    }
    const w = watchOf(pos);
    for (const [f, v] of Object.entries(w)) {
      if (JSON.stringify(v) === JSON.stringify(e.watch[f])) continue;
      e.events.push({ at: iso(t), min: round((t - e.openedAt) / 60000, 1), what: f, from: e.watch[f], to: v, movePct: round(pct, 3), price });
      if (e.events.length > R.eventsMax) e.events.splice(0, e.events.length - R.eventsMax);
    }
    e.watch = w;
    // a trailing stop moves on most reads: it is written when it has moved 0.10% of the entry since the last one written
    const sl = num(pos.sl), ent = num(pos.entryPriceUSDT);
    if (sl > 0 && ent > 0 && (!e.stop || Math.abs(sl - e.stop) / ent * 100 >= R.stopEventPct)) {
      if (e.stop) e.events.push({ at: iso(t), min: round((t - e.openedAt) / 60000, 1), what: 'stop', from: e.stop, to: round(sl, 10), stopPct: round(movePct(pos, sl), 3), movePct: round(pct, 3), price });
      if (e.events.length > R.eventsMax) e.events.splice(0, e.events.length - R.eventsMax);
      e.stop = round(sl, 10);
    }
    e.seenAt = t;
  }

  /** Every few seconds: books = [{ book, entries: [[k, pos], ...] }]. Trades open before v6.47 start being tracked here. */
  function sample(books) {
    const t = now();
    const live = new Set();
    for (const { book, entries } of books) {
      for (const [k, pos] of entries) {
        if (!pos) continue;
        const key = keyOf(book, k, pos);
        live.add(key);
        const e = st.open[key] || trackEntry(key, book, k, pos, { late: true }, t);
        e.book = book;
        const price = num(pos.livePriceUSDT);
        if (price > 0) note(e, pos, price, t);
      }
    }
    // no close was recorded for a trade the engine no longer holds: write what there is
    for (const e of Object.values(st.open)) {
      if (!live.has(e.key) && t - e.seenAt >= R.goneAfterMs) finish(e, null, { exitType: 'GONE (no close recorded)', notATrade: true, fullyClosed: true }, t);
    }
    st.lastSampleAt = t;
  }

  function finish(e, pos, x, t) {
    const p = pos || {};
    const entryPrice = num(p.entryPriceUSDT) || (e.entry.position && num(e.entry.position.entryPriceUSDT)) || null;
    const exitPrice = num(x.exitPrice) || null;
    if (pos && exitPrice) note(e, pos, exitPrice, t);
    e.closes += 1;
    const qty = num(x.qtyClosed);
    const fees = qty > 0 && entryPrice && exitPrice ? qty * (entryPrice + exitPrice) * feeRate : null;
    const bagAt = num(p.movedAt) || null;
    const lev = num((x.trade || {}).leverage) || num(p.leverageAtEntry || p.leverage) || null;
    const margin = num(x.marginClosed) || null;
    const pnl = x.trade && Number.isFinite(Number(x.trade.pnlUSDT)) ? Number(x.trade.pnlUSDT) : null;
    const rec = {
      journalId: `${e.key}#${e.closes}`, tradeId: (x.trade && x.trade.id) || null, version: VERSION,
      pair: e.pair, side: e.side, book: e.book, strategy: e.strategy, entryType: e.entryType,
      exitType: x.exitType || (x.trade && x.trade.type) || null, exitReason: (x.trade && x.trade.reason) || x.reason || null,
      notATrade: Boolean(x.notATrade), closeNo: e.closes, fraction: x.fraction === undefined ? 1 : round(x.fraction, 4), fullyClosed: Boolean(x.fullyClosed),
      openedAt: iso(e.openedAt), closedAt: iso(t), holdMin: round((t - e.openedAt) / 60000, 1),
      bagAt: iso(bagAt), minToBag: bagAt ? round((bagAt - e.openedAt) / 60000, 1) : null, minInBag: bagAt ? round((t - bagAt) / 60000, 1) : null,
      entryPrice, exitPrice, movePct: exitPrice && entryPrice ? round(movePct({ ...p, side: e.side, entryPriceUSDT: entryPrice }, exitPrice), 4) : null,
      qtyClosed: qty || null, notionalUSDT: qty && entryPrice ? round(qty * entryPrice, 4) : (x.trade ? round(x.trade.notionalUSDT, 4) : null),
      leverage: lev, marginUSDT: margin ? round(margin, 4) : null, pnlUSDT: pnl === null ? null : round(pnl, 6),
      marginReturnPct: pnl !== null && margin ? round((pnl / margin) * 100, 3) : null,
      feesUSDT: fees === null ? null : round(fees, 6), fundingUSDT: x.trade && x.trade.fundingUSDT !== undefined ? round(x.trade.fundingUSDT, 6) : null,
      best: { pct: e.best.pct, min: round((e.best.at - e.openedAt) / 60000, 1), price: e.best.price },
      worst: { pct: e.worst.pct, min: round((e.worst.at - e.openedAt) / 60000, 1), price: e.worst.price },
      events: e.events.slice(), path: e.path.concat(exitPrice && entryPrice ? [[round((t - e.openedAt) / 60000, 1), round(movePct({ side: e.side, entryPriceUSDT: entryPrice }, exitPrice), 3)]] : []),
      entry: e.entry,
      exit: { recordedAt: iso(t), coin: clean(safe(() => coin(e.k, p), {})), ...clean(safe(() => context(), {})) },
      final: clean(p), trade: clean(x.trade), _sent: false
    };
    st.records.push(JSON.parse(JSON.stringify(rec)));   // plain JSON: the same after a save, a handover or the sheet
    if (st.records.length > R.keep) {
      const drop = st.records.splice(0, st.records.length - R.keep);
      st.lost += drop.filter((d) => !d._sent).length;
    }
    if (x.fullyClosed) delete st.open[e.key];
    saveSoon();
    return st.records[st.records.length - 1];
  }

  /** A close (or a part of one). x: { trade, exitType, exitPrice, qtyClosed, marginClosed, fraction, fullyClosed, notATrade } */
  function close(book, k, pos, x = {}) {
    if (!pos) return null;
    const t = now();
    const key = keyOf(book, k, pos);
    const e = st.open[key] || trackEntry(key, book, k, pos, { late: true }, t);
    e.book = book;
    return finish(e, pos, { ...x, exitPrice: x.exitPrice || (x.trade && x.trade.exitPriceUSDT) }, t);
  }

  /** Sends the records the sheet does not have yet, a batch at a time. */
  async function flush() {
    const t = now();
    if (st.busy || t < st.nextTryAt || typeof post !== 'function' || !canSend()) return false;
    const url = String(ledgerUrl() || '').trim();
    if (!url) { st.sheet = 'no-sheet'; return false; }
    const batch = st.records.filter((r) => !r._sent).slice(0, R.batch);
    if (!batch.length) return false;
    st.busy = true;
    try {
      const res = await post(url, { action: 'RECORD_JOURNAL', rows: batch.map(sheetRow) });
      if (res && res.success !== false && res.journal === true) {
        batch.forEach((r) => { r._sent = true; });
        st.sentTotal += batch.length;
        Object.assign(st, { sheet: 'ready', lastSentAt: t, lastError: '', failMs: 0, nextTryAt: 0, warnedOld: false });
        saveSoon();
        return true;
      }
      if (res && res.success !== false) {   // an older ledger script answers, but has no journal tab
        Object.assign(st, { sheet: 'old-script', lastError: 'the sheet script has no journal (it is older than v3)', nextTryAt: t + R.oldScriptRetryMs });
        if (!st.warnedOld) {
          st.warnedOld = true;
          log('Trade journal: the ledger sheet script is older than v3, so the journal stays on the engine for now (the last 1,000 trades). Paste tools/ledger_apps_script.gs into the sheet\'s Apps Script and update the deployment to send it to the TradeJournal tab. Trades are not affected.', 'warning');
        }
        return false;
      }
      throw new Error((res && (res.error || res.message)) || 'no reply');
    } catch (err) {
      st.failMs = Math.min(R.retryMaxMs, st.failMs ? st.failMs * 2 : R.retryMs);
      Object.assign(st, { sheet: 'error', lastError: String((err && err.message) || err).slice(0, 200), nextTryAt: t + st.failMs });
      return false;
    } finally { st.busy = false; }
  }

  const toJSON = () => ({ v: VERSION, open: st.open, records: st.records, sentTotal: st.sentTotal, lost: st.lost, since: st.since });
  /** Saved records onto these: the same record (journal id) is kept once, sent if either copy was sent. */
  function load(saved) {
    if (!saved || typeof saved !== 'object') return;
    if (saved.open && typeof saved.open === 'object') st.open = { ...st.open, ...saved.open };
    if (Array.isArray(saved.records)) {
      const byId = new Map(st.records.map((r) => [r.journalId, r]));
      for (const r of saved.records) {
        if (!r || !r.journalId) continue;
        const had = byId.get(r.journalId);
        byId.set(r.journalId, had ? { ...r, _sent: Boolean(had._sent || r._sent) } : r);
      }
      st.records = [...byId.values()].sort((a, b) => Date.parse(a.closedAt) - Date.parse(b.closedAt)).slice(-R.keep);
      // a close counted on the other copy keeps its number here, so a later close gets the next one
      for (const r of st.records) { const k = String(r.journalId).replace(/#\d+$/, ''); if (st.open[k]) st.open[k].closes = Math.max(st.open[k].closes || 0, num(r.closeNo)); }
    }
    st.sentTotal = Math.max(st.sentTotal, num(saved.sentTotal));
    st.lost = Math.max(st.lost, num(saved.lost));
    if (num(saved.since) > 0) st.since = st.since ? Math.min(st.since, num(saved.since)) : num(saved.since);
  }
  function saveNow() {
    if (st.saveTimer) { clearTimeout(st.saveTimer); st.saveTimer = null; }
    if (!file) return;
    try { fs.writeFileSync(file, JSON.stringify(toJSON()), 'utf-8'); } catch (e) {}
  }
  function saveSoon() {
    if (!file || st.saveTimer) return;
    st.saveTimer = setTimeout(saveNow, R.saveDelayMs);
    if (st.saveTimer.unref) st.saveTimer.unref();
  }
  function loadFile() {
    if (!file) return;
    try { if (fs.existsSync(file)) load(JSON.parse(fs.readFileSync(file, 'utf-8'))); } catch (e) {}
  }

  const status = () => ({
    on: true, version: VERSION, since: st.since || null, records: st.records.length, keep: R.keep,
    unsent: st.records.filter((r) => !r._sent).length, sentTotal: st.sentTotal, lost: st.lost, openTracked: Object.keys(st.open).length,
    sheet: st.sheet, lastSentAt: st.lastSentAt || null, lastError: st.lastError || null, nextTryAt: st.nextTryAt > now() ? st.nextTryAt : null
  });
  const records = (limit = R.keep) => st.records.slice(-Math.max(1, Math.min(R.keep, Number(limit) || R.keep))).map((r) => { const c = { ...r }; delete c._sent; return c; });
  const openTrades = () => Object.values(st.open).map((e) => ({ key: e.key, pair: e.pair, side: e.side, book: e.book, strategy: e.strategy, entryType: e.entryType,
    openedAt: iso(e.openedAt), best: e.best, worst: e.worst, events: e.events, path: e.path, entry: e.entry }));

  // the journal never stops a trade: an error here is noted and the trade goes on
  const guard = (name, fn) => (...a) => {
    try { return fn(...a); } catch (e) { st.lastError = `${name}: ${String((e && e.message) || e).slice(0, 160)}`; try { console.warn('[Journal]', st.lastError); } catch (_) {} return null; }
  };
  return { open: guard('open', open), sample: guard('sample', sample), close: guard('close', close), flush, load: guard('load', load), loadFile, saveNow, toJSON,
    status, records, openTrades, csv: () => csvOf(st.records), _st: st };
}

module.exports = { create, clean, movePct, flatten, sheetRow, csvOf, RULES, VERSION };
