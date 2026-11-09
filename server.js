/**
 * =========================================================================
 * SISIR QUANT TERMINAL - BACKEND ENGINE v5.8 (v4 ENTRIES, NO-LOSS EXITS, RECOVERY BAGS)
 * =========================================================================
 * PAPER TRADING ENGINE: positions are simulated against live Binance prices.
 * No orders are sent to Binance from this file.
 *
 * Exits: the engine never closes below +0.30% (long) or +0.20% (short). A failing position moves to a Spot
 * Recovery Bag, which closes only at +0.30%. See CHANGES.md v5.8.
 * Strategy logic lives in strategy.js (shared with backtest.js).
 * =========================================================================
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');

// Reuse TCP/TLS connections to Binance and Google instead of a fresh handshake per request.
const keepAliveHttps = new https.Agent({ keepAlive: true, maxSockets: 8 });
const keepAliveHttp = new http.Agent({ keepAlive: true, maxSockets: 8 });

// Read a response body, inflating it when the server compressed it.
function readBody(res, done) {
  const enc = String(res.headers['content-encoding'] || '').toLowerCase();
  let stream = res;
  if (enc === 'gzip') stream = res.pipe(zlib.createGunzip());
  else if (enc === 'deflate') stream = res.pipe(zlib.createInflate());
  else if (enc === 'br') stream = res.pipe(zlib.createBrotliDecompress());
  const chunks = [];
  stream.on('data', (c) => chunks.push(c));
  stream.on('end', () => done(null, Buffer.concat(chunks).toString('utf8')));
  stream.on('error', (e) => done(e));
}

// Process-level crash resilience
process.on('uncaughtException', (err) => {
  console.error('[Server Sentry] Uncaught Exception intercepted:', err.message);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Server Sentry] Unhandled Rejection intercepted:', reason);
});

// Import Autonomous Learning Engine
let learningEngine;
try {
  learningEngine = require('./learning_engine');
} catch (e) {
  console.warn('[Server] Notice: learning_engine fallback loaded:', e.message);
  learningEngine = {
    recalculateDynamicHurdles: () => {},
    createEntrySnapshot: () => ({}),
    recordTradeOutcome: () => {},
    trainOnHistoricalTrades: () => {},
    state: { dynamicHurdles: { scoreOffset: 0, wickCeiling: 0.40, minOddsRequired: 45 } }
  };
}


// Strategy core (pure functions, shared with backtest.js)
const Strategy = require('./strategy');

const PORT = process.env.PORT || 3000;
const DISK_STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');

// Paper account size. On the first boot with a new CAPITAL_RESET_ID the saved account is
// re-based so total value (cash + open positions) equals STARTING_EQUITY_USDT.
const STARTING_EQUITY_USDT = Number(process.env.STARTING_EQUITY_USDT) || 110;
const CAPITAL_RESET_ID = process.env.CAPITAL_RESET_ID || 'equity-110usd-2026-11-09';

// In-memory trade dispatch deduplication
const recentTradeDispatches = new Set();

// -------------------------------------------------------------
// CORE SYSTEM STATE
// -------------------------------------------------------------
const State = {
  autopilotEnabled: true,
  portfolio: {
    cashUSDT: STARTING_EQUITY_USDT,
    holdings: {},
    realizedPnL: 0.00,
    bagSubsidyPoolUSDT: 0.00,
    sizingMode: 'PERCENTAGE',
    allocationPct: 15.0,
    fixedNotionalUSDT: 5.00
  },
  activePositions: {},
  recoveryBags: {},
  tradeLogs: [],
  universe: {},
  cooldowns: {},
  subsystems: {
    liveFeed: [],
    candlestickSentry: { 
      status: 'ONLINE', 
      engine: 'Tier-1 Nison Triad + Anti-Veto (v4.3)',
      bullishPatternsTracked: 'Marubozu, Belt Hold, Engulfing, Morning Star, Hammer, Inverted Hammer, Tweezers Bottom, Rising Three Methods, Piercing Line',
      bearishPatternsTracked: 'Tweezers Top, Hanging Man, Dark Cloud Cover, Shooting Star, Bearish Engulfing, Evening Star, Gravestone Doji',
      lastPatternDetected: '--' 
    },
    pumpDumpTrap: { status: 'ARMED', trappedTokensDetected: 0, lastBlockedSymbol: '--', minLiquidityHurdleUSD: '$70M' },
    bStockShield: { status: 'ACTIVE', blockedAssets: ['SOXLB', 'SNDKB', 'TSLAB', 'NVDB'], maxUnitPrice: '$350.00' },
    screener: { status: 'ACTIVE', scannedPairsCount: 350, qualifiedCandidatesCount: 22, cadenceSec: 60, liquidityFloorUSD: 'High-Sustain ($3.5M+)' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 240, activeBookDepth: 20 },
    marketMode: { mode: 'NEUTRAL', side: 'NONE', reason: 'Reading the market', breadth: null, seen: 0, updatedAt: 0 },
    futures: { status: 'CONNECTING', perpetuals: 0, reason: 'Loading USD-M contracts', leverage: 1 },
    riskGuard: { status: 'ARMED', reason: 'Awaiting first evaluation', equityUSDT: 0, riskPerTradePct: 0.5, dailyLossLimitPct: 2.0 },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 65, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Min exit +0.30% long, +0.20% short • Candlestick harvest • Trail • Failing trades move to recovery bags' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Bags close at +0.30% only', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
    quotaManager: { status: 'NORMAL', executed24hCount: 0, maxAllowed: 'UNLIMITED', quotaRemaining: '∞', vipSlotsRemaining: '∞', formattedReset: 'Dynamic Reserve Active' },
    persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Sheet Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
  },
  marketRegime: 'BALANCED',
  activeThresholds: { regime: 'BALANCED', scoreThreshold: 65, obiThreshold: 0.45, maxSpread: 0.20, marketAvgRange: 4.5 },
  tradeFrequencyController: {
    dynamicGovernorEnabled: true,
    quotaBypassEnabled: false,
    cashReserveFloorUSDT: 300.00,
    maxRecoveryBagsLimit: 5,
    governorTier: 'UNLIMITED_SPRINT',
    maxTradesPer24h: 9999,
    rollingTrades24h: [],
    consecutiveWinStreak: 0,
    nextSlotReleaseTimestamp: 0,
    pacingMode: 'HIGH_VELOCITY'
  },
  usdtToInrRate: 94.94,
  masterLedgerUrl: 'https://script.google.com/macros/s/AKfycbwwFkRk4O2oJYmgrRcQBpx3z5KBKEOCJoX6YzL1yJXdot93V1T08ewIGaF78O1n6Z1ktg/exec',
  learningSheetUrl: 'https://script.google.com/macros/s/AKfycbwut72lTkCVcqIV2k7YlzlsIOwO3GXCKqlF-4UBxvTvJHckJApKKxzbfFp5JN0VLND8/exec'
};

// -------------------------------------------------------------
// UNROUNDED HIGH-PRECISION PRICE FORMATTER
// -------------------------------------------------------------
function formatPrecisePrice(val) {
  if (val === undefined || val === null || val === '') return '--';
  const n = Number(val);
  if (isNaN(n)) return String(val);
  if (n === 0) return '0.00';
  if (n >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  if (n >= 1) {
    const s = n.toFixed(5);
    return s.replace(/\.?0+$/, '') || n.toFixed(2);
  }
  if (n >= 0.0001) {
    const s = n.toFixed(6);
    return s.replace(/\.?0+$/, '');
  }
  const s = n.toFixed(8);
  return s.replace(/\.?0+$/, '');
}

// -------------------------------------------------------------
// CORE NETWORK & LOGGING UTILITIES
// -------------------------------------------------------------
function safeFetch(url, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    try {
      const u = new URL(url);
      const isHttps = u.protocol === 'https:';
      const client = isHttps ? https : http;
      const req = client.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/4.2',
          // Binance's full ticker list is 1.9 MB plain vs ~0.28 MB gzipped
          'Accept-Encoding': 'gzip, deflate'
        },
        agent: isHttps ? keepAliveHttps : keepAliveHttp,
        timeout: timeoutMs
      }, (res) => {
        readBody(res, (err, data) => {
          if (err) return reject(err);
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            resolve(data);
          }
        });
      });
      req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout fetching ${url}`)); });
      req.on('error', err => reject(err));
    } catch (e) {
      reject(e);
    }
  });
}

function safePostJson(url, postData, timeoutMs = 12000) {
  return new Promise((resolve, reject) => {
    try {
      const u = new URL(url);
      const isHttps = u.protocol === 'https:';
      const client = isHttps ? https : http;
      const bodyStr = JSON.stringify(postData);
      const req = client.request(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(bodyStr),
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/4.2',
          'Accept-Encoding': 'gzip, deflate'
        },
        agent: isHttps ? keepAliveHttps : keepAliveHttp,
        timeout: timeoutMs
      }, (res) => {
        readBody(res, (err, data) => {
          if (err) return reject(err);
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            resolve({ raw: data });
          }
        });
      });
      req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout posting to ${url}`)); });
      req.on('error', err => reject(err));
      req.write(bodyStr);
      req.end();
    } catch (e) {
      reject(e);
    }
  });
}

function logOperation(category, message, level = 'info') {
  const now = new Date();
  const time = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });
  const logItem = { time, category, message, level };
  if (!Array.isArray(State.subsystems.liveFeed)) {
    State.subsystems.liveFeed = [];
  }
  State.subsystems.liveFeed.unshift(logItem);
  if (State.subsystems.liveFeed.length > 50) {
    State.subsystems.liveFeed.pop();
  }
}

// -------------------------------------------------------------
// IN-MEMORY ZERO-LATENCY KLINE CACHE (ELIMINATES BINANCE RATE LIMITS)
// -------------------------------------------------------------
const klineCache = new Map();
// maxAgeMs: callers that read the LIVE (still open) candle — e.g. the BTC gate — pass 20000 so the
// open bar is never more than 20s old.
async function getCachedKlines(symbol, interval, limit, timeoutMs = 3500, maxAgeMs = 0, futures = false) {
  const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
  const cacheKey = `${futures ? 'FUT_' : ''}${pairSym}_${interval}_${limit}`;
  const now = Date.now();
  const cached = klineCache.get(cacheKey);

  // Valid until the open candle closes (so a newly CLOSED bar is picked up within ~1s,
  // faster than the old fixed 20s), but never older than 5 minutes.
  if (cached && now < cached.expiresAt && (!maxAgeMs || now - cached.timestamp < maxAgeMs)) {
    return cached.data;
  }

  const query = `symbol=${pairSym}&interval=${interval}&limit=${limit}`;
  const url = futures ? `${FAPI_BASE}/fapi/v1/klines?${query}` : `https://api.binance.com/api/v3/klines?${query}`;
  const data = await safeFetch(url, timeoutMs);

  if (Array.isArray(data) && data.length > 0) {
    const openBarCloseMs = Number(data[data.length - 1][6]) || 0;
    const expiresAt = Math.min(now + 300000, Math.max(now + 1000, openBarCloseMs + 1000));
    klineCache.set(cacheKey, { timestamp: now, expiresAt, data });
    if (klineCache.size > 600) klineCache.delete(klineCache.keys().next().value);
  } else if (data && data.code && data.code < 0) {
    console.warn(`[Binance API Notice] ${pairSym} returned: ${data.msg || data.code}`);
  }
  return data;
}

// -------------------------------------------------------------
// FAIL-CLOSED MARKET DATA HELPERS (v5)
// Any missing or rate-limited data => no trade. v4 returned "passed: true"
// whenever a fetch failed, so outages and Binance rate limits opened trades.
// -------------------------------------------------------------
async function getKlinesStrict(symbol, interval, limit) {
  try {
    const d = await getCachedKlines(symbol, interval, limit);
    return Array.isArray(d) && d.length > 0 ? d : null;
  } catch (e) {
    return null;
  }
}

// -------------------------------------------------------------
// MARKET MODE (replaces the BTC gate): see strategy.evaluateMarketMode
// -------------------------------------------------------------
async function refreshMarketMode(tickers) {
  const top = tickers
    .filter((t) => t.symbol.endsWith('USDT') && t.symbol !== 'BTCUSDT' && !/^(USDC|FDUSD|TUSD|EUR|USD1|RLUSD|PAXG)/.test(t.symbol))
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume)).slice(0, 30).map((t) => t.symbol);
  const [btc, ...coins] = await Promise.all(['BTCUSDT', ...top].map((s) => getKlinesStrict(s, '1h', 120)));
  const r = Strategy.evaluateMarketMode(btc ? Strategy.closedOnly(btc) : null, coins.filter(Boolean).map((k) => Strategy.closedOnly(k)));
  const before = State.subsystems.marketMode.mode;
  State.subsystems.marketMode = { ...r, breadth: r.breadth === null || r.breadth === undefined ? null : Number(r.breadth.toFixed(2)), updatedAt: Date.now() };
  if (before !== r.mode) {
    logOperation('MARKET_MODE', `Market is now ${r.mode}: ${r.reason}. ${r.side === 'LONG' ? 'Autopilot opens futures longs.' : r.side === 'SHORT' ? 'Autopilot opens futures shorts.' : r.side === 'BOTH' ? 'Autopilot follows each coin\'s own 1h trend.' : 'No new trades.'} Open positions keep their exits.`, 'warning');
  }
  return r;
}

// -------------------------------------------------------------
// USD-M FUTURES DATA (paper only: nothing here places an order)
// -------------------------------------------------------------
const FAPI_BASE = process.env.FAPI_BASE || 'https://fapi.binance.com';
// Binance moved aggTrade to the /market path in 2026; the old /ws path no longer carries it.
const FSTREAM_URL = process.env.FSTREAM_URL || 'wss://fstream.binance.com/market/ws';
const FUT_FEE_RATE = Strategy.FUTURES.feePct / 100;
const isShort = (p) => Boolean(p) && p.side === 'SHORT';
// Spot positions opened before the move to futures have no market field and finish on spot rules.
const isFutures = (p) => isShort(p) || (Boolean(p) && p.market === 'FUTURES');
const minExitFor = (p) => (isFutures(p) ? Strategy.EXIT_RULES.shortFloorPct : Strategy.EXIT_RULES.floorPct);
let perpetuals = new Map();
let perpetualsAt = 0;

async function refreshPerpetuals() {
  if (perpetuals.size && Date.now() - perpetualsAt < 6 * 3600000) return;
  const f = State.subsystems.futures;
  let info = null;
  try { info = await safeFetch(`${FAPI_BASE}/fapi/v1/exchangeInfo`, 8000); } catch (e) { info = null; }
  if (!info || !Array.isArray(info.symbols)) {
    f.status = perpetuals.size ? 'STALE' : 'UNAVAILABLE';
    f.reason = 'Binance Futures API is not responding from this server. Shorts stay off.';
    throttledNotice('fapi', 'FUTURES', f.reason, 'danger', 300000);
    return;
  }
  const next = new Map();
  info.symbols.forEach((s) => {
    if (s.contractType !== 'PERPETUAL' || s.status !== 'TRADING' || s.quoteAsset !== 'USDT') return;
    const mn = (s.filters || []).find((x) => x.filterType === 'MIN_NOTIONAL');
    next.set(s.symbol, { minNotional: parseFloat(mn && mn.notional) || 5, listedAt: Number(s.onboardDate) || 0 });
  });
  perpetuals = next;
  perpetualsAt = Date.now();
  Object.assign(f, { status: 'ONLINE', perpetuals: next.size, reason: `${next.size} USDT perpetuals available`, leverage: Strategy.FUTURES.leverage });
}

// What a market order would actually get: a long sells at the bid, a short buys back at the ask.
const quoteMemo = new Map();
async function executableExitPrice(k, pos) {
  const short = isShort(pos), fut = isFutures(pos);
  const hit = quoteMemo.get(k);
  let q = hit && Date.now() - hit.at < 250 ? hit.q : null;
  if (!q) {
    const url = fut ? `${FAPI_BASE}/fapi/v1/ticker/bookTicker?symbol=${k}` : `https://api.binance.com/api/v3/ticker/bookTicker?symbol=${k}`;
    q = await safeFetch(url, 1500).catch(() => null);
    if (q) quoteMemo.set(k, { at: Date.now(), q });
  }
  const px = parseFloat(q && (short ? q.askPrice : q.bidPrice));
  return px > 0 ? px : null;
}

// One request covers every contract. Refreshed every 5 minutes; older than 15 minutes counts as unknown.
let fundingRates = new Map();
let fundingRatesAt = 0;
async function refreshFundingRates() {
  if (Date.now() - fundingRatesAt < 300000) return;
  const rows = await safeFetch(`${FAPI_BASE}/fapi/v1/premiumIndex`, 6000).catch(() => null);
  if (!Array.isArray(rows)) return;
  fundingRates = new Map(rows.map((r) => [r.symbol, parseFloat(r.lastFundingRate)]));
  fundingRatesAt = Date.now();
}
const currentFundingRate = (sym) => (Date.now() - fundingRatesAt < 900000 ? fundingRates.get(sym) : undefined);

async function fetchFuturesPrice(pairKey) {
  try {
    const d = await safeFetch(`${FAPI_BASE}/fapi/v1/ticker/price?symbol=${pairKey}`, 2500);
    const p = parseFloat(d && d.price);
    return p > 0 ? p : null;
  } catch (e) { return null; }
}

// Funding is booked per position when each settlement passes. Positive rate: shorts receive it, longs pay it.
let fundingBusy = false;
async function settleFunding() {
  if (fundingBusy) return;
  fundingBusy = true;
  try {
    const held = [...Object.entries(State.activePositions || {}), ...Object.entries(State.recoveryBags || {})].filter(([, p]) => isFutures(p));
    for (const [k, p] of held) {
      if (Date.now() < Number(p.nextFundingAt || 0)) continue;
      let d = null;
      try { d = await safeFetch(`${FAPI_BASE}/fapi/v1/premiumIndex?symbol=${k}`, 2500); } catch (e) { d = null; }
      if (!d || !(parseFloat(d.markPrice) > 0)) continue;
      if (p.nextFundingAt) {
        const amount = (isShort(p) ? 1 : -1) * Number(p.qty) * parseFloat(d.markPrice) * (parseFloat(d.lastFundingRate) || 0);
        p.fundingUSDT = Number(((Number(p.fundingUSDT) || 0) + amount).toFixed(6));
        logOperation('FUNDING', `${p.pair}: funding ${amount >= 0 ? 'received' : 'paid'} $${Math.abs(amount).toFixed(4)}.`, 'info');
      }
      p.nextFundingAt = Number(d.nextFundingTime) || Date.now() + 3600000;
    }
  } finally { fundingBusy = false; }
}

// Taker buy ratio on the last 3 CLOSED 5m bars. Fails closed.
async function checkTakerFlow(pairKey) {
  const k = await getKlinesStrict(pairKey, '5m', 30);
  if (!k) return { passed: false, reason: '5m data unavailable' };
  const closed = Strategy.closedOnly(k).slice(-3);
  if (closed.length < 3) return { passed: false, reason: 'not enough closed 5m bars' };
  let vol = 0, buy = 0;
  closed.forEach((b) => { vol += parseFloat(b[5]); buy += parseFloat(b[9]); });
  const ratio = vol > 0 ? (buy / vol) * 100 : 0;
  if (ratio < 48) return { passed: false, takerBuyRatio: ratio, reason: `taker buy ${ratio.toFixed(1)}% < 48% (sellers in control)` };
  return { passed: true, takerBuyRatio: Number(ratio.toFixed(1)), k5: k };
}

async function fetchTickerPrice(pairKey) {
  try {
    const d = await safeFetch(`https://api.binance.com/api/v3/ticker/price?symbol=${pairKey}`, 2500);
    const p = parseFloat(d && d.price);
    return p > 0 ? p : null;
  } catch (e) {
    return null;
  }
}

function computeEquity() {
  let eq = Number(State.portfolio.cashUSDT) || 0;
  const add = (p) => {
    const px = Number(p.livePriceUSDT || p.entryPriceUSDT) || 0;
    const qty = Number(p.qty) || (Number(p.notionalUSDT) / Number(p.entryPriceUSDT || 1));
    eq += isShort(p) ? qty * (2 * Number(p.entryPriceUSDT) - px) + (Number(p.fundingUSDT) || 0) : qty * px + (isFutures(p) ? Number(p.fundingUSDT) || 0 : 0);
  };
  Object.values(State.activePositions || {}).forEach(add);
  Object.values(State.recoveryBags || {}).forEach(add);
  return eq;
}

// -------------------------------------------------------------
// DORMANT PUMP & DUMP TRAP SHIELD
// -------------------------------------------------------------
async function checkDormantPumpAndDumpTrap(symbol, volQuote, change24, rangePct) {
  try {
    if (volQuote >= 70000000) return { isTrap: false };

    const klines = await getCachedKlines(symbol, '1h', 24, 3000);
    if (!Array.isArray(klines) || klines.length < 12) return { isTrap: false };

    let priorVolSum = 0;
    for (let i = 0; i < klines.length - 2; i++) {
      priorVolSum += parseFloat(klines[i][5]);
    }
    const avgPriorVol = priorVolSum / (klines.length - 2);
    const recentVol = parseFloat(klines[klines.length - 2][5]) + parseFloat(klines[klines.length - 1][5]);

    if (avgPriorVol > 0 && (recentVol / 2) > (avgPriorVol * 8) && volQuote < 15000000) {
      State.subsystems.pumpDumpTrap.trappedTokensDetected++;
      State.subsystems.pumpDumpTrap.lastBlockedSymbol = symbol;
      return { isTrap: true, reason: `Dormant Pump Spike (${((recentVol/2)/avgPriorVol).toFixed(1)}x vol spike on <$15M quote vol)` };
    }
    return { isTrap: false };
  } catch (e) {
    return { isTrap: false };
  }
}

async function fetchRealOrderBookDepth(symbol, futures = false) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = futures ? `${FAPI_BASE}/fapi/v1/depth?symbol=${pairSym}&limit=20` : `https://api.binance.com/api/v3/depth?symbol=${pairSym}&limit=20`;
    const data = await safeFetch(url, 2500);
    if (!data || !Array.isArray(data.bids) || !Array.isArray(data.asks) || data.bids.length === 0 || data.asks.length === 0) {
      return null;
    }
    const bestBid = parseFloat(data.bids[0][0]);
    const bestAsk = parseFloat(data.asks[0][0]);
    const spreadPct = bestBid > 0 ? ((bestAsk - bestBid) / bestBid) * 100 : 0.05;

    let bidVol = 0;
    let askVol = 0;
    data.bids.forEach(b => bidVol += parseFloat(b[1]));
    data.asks.forEach(a => askVol += parseFloat(a[1]));
    const total = bidVol + askVol;
    const obi = total > 0 ? (bidVol / total) : 0.55;

    return {
      bestBid,
      bestAsk,
      spreadPct: Number(spreadPct.toFixed(3)),
      obi: Number(obi.toFixed(3)),
      bidDepth: bidVol,
      askDepth: askVol
    };
  } catch (err) {
    return null;
  }
}

// Bandwidth: the full list is 0.28 MB gzipped. Pull a reduced full list every 15 minutes to learn
// which coins are liquid, and only those coins (about 4 KB) on the other minutes.
let liquidSymbols = [];
let liquidAt = 0;
const TICKER_URL = 'https://api.binance.com/api/v3/ticker/24hr?type=MINI';
async function fetchScreenerTickers() {
  let rows = null;
  if (liquidSymbols.length && Date.now() - liquidAt < 900000) {
    rows = await safeFetch(`${TICKER_URL}&symbols=${encodeURIComponent(JSON.stringify(liquidSymbols))}`, 4500).catch(() => null);
  }
  if (!Array.isArray(rows)) {
    rows = await safeFetch(TICKER_URL, 6000);
    if (!Array.isArray(rows)) return null;
    liquidSymbols = rows.filter((t) => t.symbol.endsWith('USDT') && parseFloat(t.quoteVolume) >= 2500000).map((t) => t.symbol);
    liquidAt = Date.now();
  }
  rows.forEach((t) => { const o = parseFloat(t.openPrice); t.priceChangePercent = o > 0 ? ((parseFloat(t.lastPrice) - o) / o) * 100 : 0; });
  return rows;
}

async function runDynamicScreener() {
  try {
    const tickers = await fetchScreenerTickers();
    if (!tickers) return;

    await refreshPerpetuals();
    await refreshFundingRates();
    const mode = await refreshMarketMode(tickers);

    const universe = {};
    const candidateKlinesQueue = [];

    for (const t of tickers) {
      const sym = t.symbol;
      if (!sym.endsWith('USDT')) continue;

      if (sym.includes('UP') || sym.includes('DOWN') || sym.includes('BULL') || sym.includes('BEAR')) continue;
      if (['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'EURUSDT'].includes(sym)) continue;

      const blockedDerivatives = [
        'SOXLB', 'SNDKB', 'TSLAB', 'AAPLB', 'NVDB', 'MSFTB', 'AMZNB', 
        'SPYB', 'QQQB', 'GOOGB', 'METAB', 'COINB', 'MSTRB', 'BABAB'
      ];
      if (blockedDerivatives.some(d => sym.startsWith(d))) continue;

      const last = parseFloat(t.lastPrice);
      if (last > 350.0 && !['BTCUSDT', 'ETHUSDT', 'PAXGUSDT'].includes(sym)) continue;

      const volQuote = parseFloat(t.quoteVolume);
      if (volQuote < 3500000) continue;

      const high = parseFloat(t.highPrice);
      const low = parseFloat(t.lowPrice);
      const change24 = parseFloat(t.priceChangePercent);

      const rangePct = low > 0 ? ((high - low) / low) * 100 : 0;
      const posInRange = (high - low) > 0 ? (last - low) / (high - low) : 0.5;

      let isPumpDump = false;
      let pumpDumpReason = '';

      if (volQuote < 70000000 && (change24 >= 7.0 || rangePct >= 8.5)) {
        const pdResult = await checkDormantPumpAndDumpTrap(sym, volQuote, change24, rangePct);
        if (pdResult.isTrap) {
          isPumpDump = true;
          pumpDumpReason = pdResult.reason;
        }
      }

      // Bull: strongest coins. Bear: weakest coins. Neutral: upper half of the day's range ranks as a long, lower half as a short.
      const side = mode.side === 'SHORT' || (mode.side === 'BOTH' && posInRange < 0.5) ? 'SHORT' : 'LONG';
      const bearMode = side === 'SHORT';
      const tradeBlock = !perpetuals.has(sym) ? 'no USD-M contract'
        : Strategy.futuresExclusion({ side, volumeUSD: volQuote, fundingRate: currentFundingRate(sym), listedAt: perpetuals.get(sym).listedAt });
      const canTrade = !tradeBlock;
      let score = bearMode
        ? 60 + ((1 - posInRange) * 22) + (Math.min(-change24, 15) * 0.9)
        : 60 + (posInRange * 22) + (Math.min(change24, 15) * 0.9);
      if (volQuote >= 10000000) score += 5;
      if (rangePct >= 5.0) score += 3;

      if (isPumpDump || !canTrade) {
        score = 0;
      }

      universe[sym] = {
        symbol: sym.replace('USDT', ''),
        priceUSDT: last,
        priceINR: Number((last * State.usdtToInrRate).toFixed(2)),
        change24h: change24,
        intradayRangePct: Number(rangePct.toFixed(2)),
        pricePositionInRange: Number(posInRange.toFixed(2)),
        score: Math.min(99, Math.round(score)),
        volumeM: Math.round(volQuote / 1000000),
        side,
        canTrade,
        tradeBlock,
        obi: 0.55,
        isPumpAndDump: isPumpDump,
        pumpDumpReason: pumpDumpReason,
        candlestickPattern: 'WATCH'
      };

      if (!isPumpDump && score >= 60 && canTrade && candidateKlinesQueue.length < 35) {
        candidateKlinesQueue.push(sym);
      }
    }

    // Inspect candlestick patterns on top candidates to award Steve Nison momentum bonuses
    for (const topSym of candidateKlinesQueue) {
      try {
        // 100 bars = same cache key the entry evaluator uses; patterns on CLOSED bars only
        const klines15m = await getCachedKlines(topSym, '15m', 100, 2000, 0, true);
        const bearMode = universe[topSym] && universe[topSym].side === 'SHORT';
        const closed15m = Strategy.closedOnly(klines15m);
        const pat = bearMode ? null : Strategy.assessCandles(closed15m);
        if (universe[topSym] && bearMode) {
          // Mirror of the long scoring: penalize weak-24h coins that are bouncing hard in the last hour.
          if (Array.isArray(klines15m) && klines15m.length >= 5) {
            const open1h = parseFloat(klines15m[klines15m.length - 5][1]);
            const closeNow = parseFloat(klines15m[klines15m.length - 1][4]);
            if (open1h > 0 && ((closeNow - open1h) / open1h) * 100 > 0.30) universe[topSym].score = Math.max(0, universe[topSym].score - 12);
          }
          const sp = Strategy.assessShortCandles(closed15m);
          if (sp.bullish) {
            universe[topSym].candlestickPattern = `⚠️ ${sp.bullPattern}`;
            universe[topSym].score = Math.max(0, universe[topSym].score - 18);
          } else if (sp.sell) {
            universe[topSym].candlestickPattern = sp.pattern;
            universe[topSym].score = Math.min(99, universe[topSym].score + 10 + Math.min(6, sp.convergence.length * 2));
          } else if (sp.pattern !== 'NONE') {
            universe[topSym].candlestickPattern = `${sp.pattern} (against trend)`;
          }
        } else if (universe[topSym]) {
          // Penalize stale 24h gainers that are actively bleeding in the 1h timeframe
          if (Array.isArray(klines15m) && klines15m.length >= 5) {
            const open1h = parseFloat(klines15m[klines15m.length - 5][1]);
            const closeNow = parseFloat(klines15m[klines15m.length - 1][4]);
            const change1h = open1h > 0 ? ((closeNow - open1h) / open1h) * 100 : 0;
            if (change1h < -0.30) {
              universe[topSym].score = Math.max(0, universe[topSym].score - 12);
            }
          }

          if (pat.bearish) {
            universe[topSym].candlestickPattern = `⚠️ ${pat.bearPattern}`;
            universe[topSym].score = Math.max(0, universe[topSym].score - 18);
          } else if (pat.buy) {
            universe[topSym].candlestickPattern = pat.pattern;
            universe[topSym].score = Math.min(99, universe[topSym].score + 10 + Math.min(6, pat.convergence.length * 2));
          } else if (pat.pattern !== 'NONE') {
            universe[topSym].candlestickPattern = `${pat.pattern} (against trend)`;
          }
        }
      } catch (err) {}
    }

    State.universe = universe;
    State.subsystems.screener.scannedPairsCount = Object.keys(universe).length;
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= (State.activeThresholds?.scoreThreshold || 65) && !c.isPumpAndDump).length;

    logOperation('RADAR_SCREENER', `[${mode.mode}] Scanned ${Object.keys(universe).length} pairs for ${mode.side === 'SHORT' ? 'futures shorts' : mode.side === 'LONG' ? 'futures longs' : 'futures longs and shorts'}. ${State.subsystems.screener.qualifiedCandidatesCount} candidates qualified (Score >= ${State.activeThresholds?.scoreThreshold || 65}). Candlestick Sentinel active.`, 'info');
    broadcastStateUpdate({ withUniverse: true });
  } catch (err) {
    console.error('[Screener] Error running scan:', err.message);
  }
}

// -------------------------------------------------------------
// ORDER SIMULATION: OPEN / CLOSE WITH CORRECT CASH + FEES (v5)
// NOTE: this engine is a PAPER TRADER. No order is sent to Binance.
// -------------------------------------------------------------
const FEE_RATE = Strategy.CONFIG.feePct / 100;

function openLong(pairKey, signal, plan, takerBuyRatio) {
  const sym = pairKey.replace(/USDT$/, '');
  const entry = plan.entryPrice;
  const R = Strategy.EXIT_RULES;
  const cost = plan.notional * (1 + FUT_FEE_RATE); // 1x margin plus the opening fee
  if (State.portfolio.cashUSDT < cost) {
    logOperation('CASH_GUARD', `Cannot long ${sym}/USDT: need $${cost.toFixed(2)}, free cash $${State.portfolio.cashUSDT.toFixed(2)}.`, 'danger');
    return false;
  }
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - cost).toFixed(6));
  const bagAt = entry * (1 + R.demotePct / 100);
  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`, pairSymbol: sym, side: 'LONG', market: 'FUTURES', leverage: Strategy.FUTURES.leverage, strategyVersion: 6,
    openedAt: Date.now(), entryPriceUSDT: entry, livePriceUSDT: entry, lastTickAt: Date.now(),
    qty: plan.qty, notionalUSDT: plan.notional, initialNotionalUSDT: plan.notional,
    initialStopPrice: bagAt, sl: bagAt, stopPct: Number(plan.stopPct.toFixed(3)), riskUSDT: plan.riskUSDT, atrPct: Number(signal.atrPct.toFixed(3)),
    breakevenPrice: entry * (1 + R.shortFloorPct / 100), tp1Price: entry * (1 + R.tp1Pct / 100), targetPrice: entry * 1.03,
    fundingUSDT: 0, nextFundingAt: 0,
    peakGainPct: 0, peakHighPrice: entry, ratchetStage: 'INITIAL', tp1Secured: false, candlestickPattern: signal.pattern
  };
  logOperation('EXECUTION', `LONGED ${sym}/USDT @ $${formatPrecisePrice(entry)} ($${plan.notional.toFixed(2)}, 1x USD-M, paper) via [${signal.pattern}] • Min exit +0.20% • Moves to recovery bag at -3.50% • Taker buy ${takerBuyRatio}%`, 'success');
  saveDiskState();
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
  return true;
}

function openShort(pairKey, signal, plan, takerBuyRatio) {
  const sym = pairKey.replace(/USDT$/, '');
  const entry = plan.entryPrice;
  const R = Strategy.EXIT_RULES;
  const cost = plan.notional * (1 + FUT_FEE_RATE); // 1x margin plus the opening fee
  if (State.portfolio.cashUSDT < cost) {
    logOperation('CASH_GUARD', `Cannot short ${sym}/USDT: need $${cost.toFixed(2)}, free cash $${State.portfolio.cashUSDT.toFixed(2)}.`, 'danger');
    return false;
  }
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - cost).toFixed(6));
  const bagAt = entry * (1 - R.demotePct / 100);
  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`, pairSymbol: sym, side: 'SHORT', market: 'FUTURES', leverage: Strategy.FUTURES.leverage, strategyVersion: 6,
    openedAt: Date.now(), entryPriceUSDT: entry, livePriceUSDT: entry, lastTickAt: Date.now(),
    qty: plan.qty, notionalUSDT: plan.notional, initialNotionalUSDT: plan.notional,
    initialStopPrice: bagAt, sl: bagAt, stopPct: Number(plan.stopPct.toFixed(3)), riskUSDT: plan.riskUSDT, atrPct: Number(signal.atrPct.toFixed(3)),
    breakevenPrice: entry * (1 - R.shortFloorPct / 100), tp1Price: entry * (1 - R.tp1Pct / 100), targetPrice: entry * 0.97,
    liquidationPrice: Strategy.shortLiquidationPrice(entry), fundingUSDT: 0, nextFundingAt: 0,
    peakGainPct: 0, peakLowPrice: entry, ratchetStage: 'INITIAL', tp1Secured: false, candlestickPattern: signal.pattern
  };
  logOperation('EXECUTION', `SHORTED ${sym}/USDT @ $${formatPrecisePrice(entry)} ($${plan.notional.toFixed(2)}, 1x USD-M, paper) via [${signal.pattern}] • Min exit +0.20% • Moves to recovery bag at -3.50% • Taker buy ${takerBuyRatio}%`, 'success');
  saveDiskState();
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
  return true;
}

/**
 * Sells `fraction` of a position/bag at `price`. Credits cash with proceeds
 * minus fee, records net P&L (both fees), deletes the position when fully closed.
 */
function realizeExit(bookName, key, fraction, price, typeLabel, reasonText) {
  const bookObj = State[bookName];
  const pos = bookObj && bookObj[key];
  if (!pos || pos.exitInProgress) return null;

  const entry = Number(pos.entryPriceUSDT);
  // Hard rule: only the user's own manual close can realize a loss.
  const short = isShort(pos);
  if (!/MANUAL|LIQUIDATED/.test(typeLabel) && (short ? price > entry : price < entry)) {
    logOperation('SENTRY', `${pos.pair}: blocked ${typeLabel} below entry. Position stays open.`, 'warning');
    return null;
  }
  const totalQty = Number(pos.qty) || (Number(pos.notionalUSDT) / entry);
  const qtySold = fraction >= 1 ? totalQty : totalQty * fraction;
  const fut = isFutures(pos);
  const fundingShare = fut ? (Number(pos.fundingUSDT) || 0) * (fraction >= 1 ? 1 : fraction) : 0;
  const proceeds = short ? qtySold * (2 * entry - price) - qtySold * price * FUT_FEE_RATE + fundingShare
    : fut ? qtySold * price - qtySold * price * FUT_FEE_RATE + fundingShare : qtySold * price * (1 - FEE_RATE);
  const pnl = short ? Strategy.netPnlShort(qtySold, entry, price) + fundingShare
    : fut ? Strategy.netPnlLongFutures(qtySold, entry, price) + fundingShare : Strategy.netPnl(qtySold, entry, price);
  if (fut && fraction < 1) pos.fundingUSDT = Number(((Number(pos.fundingUSDT) || 0) - fundingShare).toFixed(6));

  State.portfolio.cashUSDT = Number((Number(State.portfolio.cashUSDT) + proceeds).toFixed(6));
  State.portfolio.realizedPnL = Number((Number(State.portfolio.realizedPnL || 0) + pnl).toFixed(6));

  const remainingQty = totalQty - qtySold;
  const fullyClosed = fraction >= 1 || remainingQty * price < 1;
  if (fullyClosed) {
    pos.exitInProgress = true;
    delete bookObj[key];
  } else {
    pos.qty = remainingQty;
    pos.notionalUSDT = Number((remainingQty * entry).toFixed(4));
  }

  const gainPct = (short ? entry - price : price - entry) / entry * 100;
  const tradePayload = {
    id: Date.now(),
    pair: pos.pair || `${key.replace('USDT', '')}/USDT`,
    type: short ? `SHORT ${typeLabel}` : fut ? `LONG ${typeLabel}` : typeLabel,
    side: short ? 'SHORT' : 'LONG',
    market: fut ? 'FUTURES' : 'SPOT',
    fundingUSDT: fut ? Number(fundingShare.toFixed(4)) : 0,
    entryPriceUSDT: entry,
    exitPriceUSDT: price,
    notionalUSDT: Number((qtySold * entry).toFixed(4)),
    pnlUSDT: Number(pnl.toFixed(4)),
    holdMinutes: Math.round((Date.now() - Number(pos.openedAt || Date.now())) / 60000),
    feesIncluded: true, // v5 P&L is already net of both 0.10% fees
    strategyVersion: 6,
    reason: reasonText
  };

  logOperation(pnl >= 0 ? 'SENTRY_EXIT' : 'RISK_STOP',
    `${tradePayload.type} on ${tradePayload.pair} @ $${formatPrecisePrice(price)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%) • ` +
    `Net ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)} after fees${fullyClosed ? '' : ' • runner continues'}`,
    pnl >= 0 ? 'success' : 'warning');

  saveDiskState();
  broadcastStateUpdate();
  recordTradeToCloudLedger(tradePayload).catch((e) => console.error('[Ledger Dispatch Notice]', e.message));
  return tradePayload;
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (shared state)
// -------------------------------------------------------------
let isEvaluatingEntries = false;
const noticeTimes = {};
const candidateCooldownMemo = new Map();

function throttledNotice(key, category, message, level = 'info', everyMs = 60000) {
  if (Date.now() - (noticeTimes[key] || 0) < everyMs) return;
  noticeTimes[key] = Date.now();
  logOperation(category, message, level);
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY GATES (restored from v4 — see autopilot_engine_complete_logic_specification.md)
// -------------------------------------------------------------
async function checkCandlestickSentry(symbol, futures = false) {
  const none = { isShortEntry: false, shortPatternName: 'NONE', isBullishSignal: false, isBearishReversal: false, isBullishEntry: false, bullishPatternName: 'NONE', bearishPatternName: 'NONE', patternName: 'NONE', confidence: 0, patternRvol: 1.0, convergence: [], major: 'flat' };
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const [k15, k1h] = await Promise.all([
      getCachedKlines(pairSym, '15m', 100, 2500, 0, futures),
      getCachedKlines(pairSym, '1h', 60, 3500, 0, futures)
    ]);
    const closed15 = Strategy.closedOnly(k15);
    if (!Array.isArray(closed15) || closed15.length < 30) return none;
    const a = Strategy.assessCandles(closed15, { major: Strategy.majorTrendOf(Strategy.closedOnly(k1h)) });
    const bull = a.pattern !== 'NONE' ? `${a.pattern} (15m)` : 'NONE';
    const bear = a.bearPattern !== 'NONE' ? `${a.bearPattern} (15m)` : 'NONE';
    const patternName = bear !== 'NONE' ? bear : bull;
    if (patternName !== 'NONE') State.subsystems.candlestickSentry.lastPatternDetected = `${pairSym}: ${patternName}`;
    const sh = Strategy.assessShortCandles(closed15, { major: a.major });
    return { isShortEntry: sh.sell, shortPatternName: sh.pattern !== 'NONE' ? `${sh.pattern} (15m)` : 'NONE', isBullishSignal: sh.bullish, bullSignalName: sh.bullPattern !== 'NONE' ? `${sh.bullPattern} (15m)` : 'NONE',
      isBearishReversal: a.bearish, isBullishEntry: a.buy, bullishPatternName: bull, bearishPatternName: bear, patternName,
      confidence: a.score, patternRvol: a.rvol, convergence: a.convergence, major: a.major, vetoes: a.vetoes };
  } catch (e) {
    return none;
  }
}

async function checkVeteranOrderFlowAndVSA(symbol) {
  try {
    const klines = await getCachedKlines(symbol, '5m', 8, 2500, 20000, true);
    if (!Array.isArray(klines) || klines.length < 3) {
      return { passed: false, reason: 'Futures data unavailable' };
    }

    let totalVol = 0;
    let takerBuyVol = 0;
    for (let i = klines.length - 3; i < klines.length; i++) {
      totalVol += parseFloat(klines[i][5]);
      takerBuyVol += parseFloat(klines[i][9]);
    }

    const takerBuyRatio = totalVol > 0 ? Number(((takerBuyVol / totalVol) * 100).toFixed(1)) : 50;
    const curBar = klines[klines.length - 1];
    const open = parseFloat(curBar[1]);
    const high = parseFloat(curBar[2]);
    const low = parseFloat(curBar[3]);
    const close = parseFloat(curBar[4]);
    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;

    if (takerBuyRatio < 46.0 && clv < 0.35) {
      return { passed: false, takerBuyRatio, reason: `Seller Absorption: Taker Buy ${takerBuyRatio}% (<46%) & CLV ${(clv*100).toFixed(0)}%` };
    }

    return { passed: true, takerBuyRatio, clv: Number(clv.toFixed(2)) };
  } catch (e) {
    return { passed: false, reason: 'Futures data unavailable' };
  }
}

async function checkMicrostructureMomentumAndVwap(symbol) {
  try {
    const klines = await getCachedKlines(symbol, '15m', 16, 2500, 45000, true);
    if (!Array.isArray(klines) || klines.length < 8) {
      return { passed: false, reason: 'Futures data unavailable' };
    }

    const curBar = klines[klines.length - 1];
    const curOpen = parseFloat(curBar[1]);
    const curHigh = parseFloat(curBar[2]);
    const curLow = parseFloat(curBar[3]);
    const curClose = parseFloat(curBar[4]);
    const curVol = parseFloat(curBar[5]);

    const change15mPct = curOpen > 0 ? Number((((curClose - curOpen) / curOpen) * 100).toFixed(2)) : 0;
    const range = curHigh - curLow;
    const upperWickRatio = range > 0 ? (curHigh - Math.max(curOpen, curClose)) / range : 0;

    // Upper Rejection Shadow Ceiling (<= 30%)
    if (upperWickRatio > 0.30 && range > 0) {
      return { passed: false, reason: `Upper rejection shadow too high (${(upperWickRatio * 100).toFixed(0)}% > 30%)` };
    }

    // Rolling 4h VWAP calculation
    let volSum = 0;
    let vwapSum = 0;
    for (let i = Math.max(0, klines.length - 16); i < klines.length; i++) {
      const h = parseFloat(klines[i][2]);
      const l = parseFloat(klines[i][3]);
      const c = parseFloat(klines[i][4]);
      const v = parseFloat(klines[i][5]);
      vwapSum += (((h + l + c) / 3) * v);
      volSum += v;
    }
    const vwap4h = volSum > 0 ? (vwapSum / volSum) : curClose;

    // Strict Zero-Tolerance VWAP Support: Price MUST be >= VWAP
    if (curClose < vwap4h) {
      return { passed: false, reason: `Below 15m/4h VWAP ($${formatPrecisePrice(curClose)} < $${formatPrecisePrice(vwap4h)})` };
    }

    // Relative Volume (RVOL)
    let priorVolSum = 0;
    for (let i = Math.max(0, klines.length - 10); i < klines.length - 1; i++) {
      priorVolSum += parseFloat(klines[i][5]);
    }
    const avgVol = priorVolSum / 9;
    const rvol = avgVol > 0 ? Number((curVol / avgVol).toFixed(2)) : 1.0;

    // 2-Hour Base Ceiling Distance Gate (<= 2.50%)
    let highPriorBase = 0;
    for (let i = Math.max(0, klines.length - 9); i < klines.length - 1; i++) {
      const h = parseFloat(klines[i][2]);
      if (h > highPriorBase) highPriorBase = h;
    }
    const distFromBaseHighPct = highPriorBase > 0 ? ((highPriorBase - curClose) / highPriorBase) * 100 : 0;
    if (distFromBaseHighPct > 2.50) {
      return {
        passed: false,
        reason: `Mid-range chop: Price is -${distFromBaseHighPct.toFixed(2)}% below 2h base ceiling ($${formatPrecisePrice(highPriorBase)}). Max allowed 2.50%.`
      };
    }

    return {
      passed: true,
      change15mPct,
      rvol,
      vwap4h: Number(vwap4h.toFixed(6)),
      upperWickRatio: Number(upperWickRatio.toFixed(2))
    };
  } catch (e) {
    return { passed: false, reason: 'Futures data unavailable' };
  }
}

async function check1hTrendAlignment(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '1h', 60, 3500, 120000, true);

    if (!Array.isArray(klines) || klines.length < 20) {
      return { passed: false, reason: 'Futures data unavailable' };
    }
    const curClose = parseFloat(klines[klines.length - 1][4]);
    
    // EMA 20
    const k20 = 2 / (20 + 1);
    let ema20 = parseFloat(klines[0][4]);
    let emaPrev2 = ema20;
    for (let i = 1; i < klines.length; i++) {
      const c = parseFloat(klines[i][4]);
      ema20 = (c * k20) + (ema20 * (1 - k20));
      if (i === klines.length - 3) {
        emaPrev2 = ema20;
      }
    }

    // EMA 50 (evaluated across up to 60 bars)
    let ema50 = null;
    if (klines.length >= 45) {
      const k50 = 2 / (50 + 1);
      ema50 = parseFloat(klines[0][4]);
      for (let i = 1; i < klines.length; i++) {
        const c = parseFloat(klines[i][4]);
        ema50 = (c * k50) + (ema50 * (1 - k50));
      }
    }
    
    // Strict Zero-Tolerance 1h 20-EMA Floor (Price MUST be on or above EMA20)
    if (curClose < (ema20 * 0.9985)) {
      return { passed: false, reason: `Price below 1h 20-EMA ($${formatPrecisePrice(curClose)} < $${formatPrecisePrice(ema20)})` };
    }

    // 1h Dual-EMA Bullish Stack Gate:
    // Rejects candidates trading in an entrenched macro bear channel where EMA20 is deep below EMA50 (> 1.20%)
    if (ema50 && ema20 < (ema50 * 0.988)) {
      return { 
        passed: false, 
        reason: `Macro Bear Stack: 1h 20-EMA ($${formatPrecisePrice(ema20)}) is -${(((ema50 - ema20) / ema50) * 100).toFixed(2)}% below 50-EMA ($${formatPrecisePrice(ema50)}). Downward bear channel.` 
      };
    }

    // Decoupled 1h EMA Slope Gate:
    // Only veto if 1h 20-EMA is in severe macro downtrend (< 0.9970) to prevent lagging 20h vetoes on fresh turnarounds
    if (ema20 < (emaPrev2 * 0.9970)) {
      return { 
        passed: false, 
        reason: `1h 20-EMA slope is declining sharply ($${formatPrecisePrice(ema20)} < $${formatPrecisePrice(emaPrev2)}). Macro bear flag.` 
      };
    }

    // 1h 20-EMA Extension Shield (Prevents FOMO into overextended 2nd/3rd wave impulses)
    const distAboveEmaPct = ema20 > 0 ? ((curClose - ema20) / ema20) * 100 : 0;
    if (distAboveEmaPct > 5.50) {
      return { 
        passed: false, 
        reason: `Overextended above 1h 20-EMA (+${distAboveEmaPct.toFixed(2)}% > 5.50%). Awaiting base consolidation.` 
      };
    }

    return { 
      passed: true, 
      ema1h: Number(ema20.toFixed(4)), 
      ema50: ema50 ? Number(ema50.toFixed(4)) : null,
      distAboveEmaPct: Number(distAboveEmaPct.toFixed(2)) 
    };
  } catch (err) {
    return { passed: false, reason: 'Futures data unavailable' };
  }
}

// -------------------------------------------------------------
// SHORT ENTRY GATES: the long gates mirrored. These fail closed: no data, no short.
// -------------------------------------------------------------
async function checkShortOrderFlow(symbol) {
  const k = await getCachedKlines(symbol, '5m', 8, 2500, 20000, true).catch(() => null);
  if (!Array.isArray(k) || k.length < 3) return { passed: false, reason: '5m data unavailable' };
  let vol = 0, buy = 0;
  k.slice(-3).forEach((b) => { vol += parseFloat(b[5]); buy += parseFloat(b[9]); });
  const takerBuyRatio = vol > 0 ? Number(((buy / vol) * 100).toFixed(1)) : 50;
  const [h, l, c] = [k[k.length - 1][2], k[k.length - 1][3], k[k.length - 1][4]].map(parseFloat);
  const clv = h - l > 0 ? (c - l) / (h - l) : 0.5;
  if (takerBuyRatio > 54 && clv > 0.65) return { passed: false, takerBuyRatio, reason: `Buyer absorption: taker buy ${takerBuyRatio}% (>54%) and CLV ${(clv * 100).toFixed(0)}%` };
  return { passed: true, takerBuyRatio };
}

async function checkShortMomentum(symbol) {
  const k = await getCachedKlines(symbol, '15m', 16, 2500, 45000, true).catch(() => null);
  if (!Array.isArray(k) || k.length < 8) return { passed: false, reason: '15m data unavailable' };
  const [o, h, l, c, v] = k[k.length - 1].slice(1, 6).map(parseFloat);
  const change15mPct = o > 0 ? Number((((c - o) / o) * 100).toFixed(2)) : 0;
  const lowerWick = h - l > 0 ? (Math.min(o, c) - l) / (h - l) : 0;
  if (lowerWick > 0.30) return { passed: false, reason: `Lower rejection shadow too long (${(lowerWick * 100).toFixed(0)}% > 30%)` };
  let vs = 0, ps = 0;
  k.slice(-16).forEach((b) => { const vol = parseFloat(b[5]); ps += ((parseFloat(b[2]) + parseFloat(b[3]) + parseFloat(b[4])) / 3) * vol; vs += vol; });
  const vw = vs > 0 ? ps / vs : c;
  if (c > vw) return { passed: false, reason: `Above 4h VWAP ($${formatPrecisePrice(c)} > $${formatPrecisePrice(vw)})` };
  const prior = k.slice(-10, -1), avg = prior.reduce((s, b) => s + parseFloat(b[5]), 0) / prior.length;
  const floor = Math.min(...k.slice(-9, -1).map((b) => parseFloat(b[3])));
  const dist = floor > 0 ? ((c - floor) / floor) * 100 : 0;
  if (dist > 2.5) return { passed: false, reason: `Mid-range chop: +${dist.toFixed(2)}% above the 2h floor. Max 2.50%.` };
  return { passed: true, change15mPct, rvol: avg > 0 ? Number((v / avg).toFixed(2)) : 1 };
}

async function checkShort1hTrend(symbol) {
  const k = await getCachedKlines(symbol, '1h', 60, 3500, 120000, true).catch(() => null);
  if (!Array.isArray(k) || k.length < 45) return { passed: false, reason: '1h data unavailable' };
  const closes = k.map((b) => parseFloat(b[4])), n = closes.length - 1, c = closes[n];
  const e20 = Strategy.emaSeries(closes, 20), e50 = Strategy.emaSeries(closes, 50);
  if (c > e20[n] * 1.0015) return { passed: false, reason: 'Price above the 1h 20-EMA' };
  if (e20[n] > e50[n] * 1.012) return { passed: false, reason: '1h 20-EMA well above the 50-EMA, bull channel' };
  if (e20[n] > e20[n - 2] * 1.003) return { passed: false, reason: '1h 20-EMA rising sharply' };
  const below = e20[n] > 0 ? ((e20[n] - c) / e20[n]) * 100 : 0;
  if (below > 5.5) return { passed: false, reason: `Overextended below the 1h 20-EMA (-${below.toFixed(2)}% > 5.50%)` };
  return { passed: true };
}

// Neutral market: a coin may only trade in the direction of its own closed 1h trend.
async function coinTrendAgrees(pairKey, side) {
  const k = await getCachedKlines(pairKey, '1h', 120, 3500, 120000, true).catch(() => null);
  if (!Array.isArray(k)) return false;
  return Strategy.coinTrend(Strategy.closedOnly(k).map((b) => parseFloat(b[4]))) === side;
}

async function evaluateShortEntries(maxBagsLimit) {
  if (!perpetuals.size) {
    throttledNotice('noperps', 'FUTURES', 'Market is bearish, but Binance Futures data is unavailable. No shorts opened.', 'danger', 60000);
    return;
  }
  const minScore = State.activeThresholds?.scoreThreshold || 65;
  const sorted = Object.keys(State.universe || {}).sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));
  isEvaluatingEntries = true;
  try {
    let evaluated = 0;
    const now = Date.now();
    const veto = (pairKey, ms, cat, msg, lvl = 'warning') => { candidateCooldownMemo.set(pairKey, now + ms); logOperation(cat, `${pairKey} short: ${msg}`, lvl); };
    for (const k of sorted) {
      if (Object.keys(State.recoveryBags || {}).length >= maxBagsLimit) break;
      const coin = State.universe[k];
      if (!coin || !coin.symbol || coin.isPumpAndDump || coin.side !== 'SHORT' || !coin.canTrade) continue;
      if ((coin.score || 0) < minScore) break;
      const baseSym = String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
      const pairKey = `${baseSym}USDT`;
      if (Number(coin.priceUSDT || 0) > 350 && !['BTCUSDT', 'ETHUSDT'].includes(pairKey)) continue;
      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;
      if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > now) continue;
      if ((candidateCooldownMemo.get(pairKey) || 0) > now) continue;
      if (++evaluated > 16) break;

      const change24h = Number(coin.change24h || 0), rangePct = Number(coin.intradayRangePct || 0);
      if (change24h > -0.20 && rangePct < 3.0) continue;
      if (Number(coin.pricePositionInRange ?? 0.3) > 0.50) continue;
      if (State.subsystems.marketMode.mode === 'NEUTRAL' && !(await coinTrendAgrees(pairKey, 'SHORT'))) { candidateCooldownMemo.set(pairKey, now + 60000); continue; }

      const flow = await checkShortOrderFlow(pairKey);
      if (!flow.passed) { veto(pairKey, 30000, 'FLOW_VETO', flow.reason); continue; }
      if (Number(coin.priceUSDT || 0) >= 0.50 && rangePct < 4.0 && change24h > -0.60) { veto(pairKey, 30000, 'VOL_VETO', `low volatility (range ${rangePct.toFixed(2)}%).`, 'info'); continue; }
      const mom = await checkShortMomentum(pairKey);
      if (!mom.passed) { veto(pairKey, 30000, 'MOM_VETO', mom.reason); continue; }
      const trend = await checkShort1hTrend(pairKey);
      if (!trend.passed) { veto(pairKey, 30000, 'TREND_VETO', trend.reason); continue; }
      const candle = await checkCandlestickSentry(pairKey, true);
      if (candle.isBullishSignal) { veto(pairKey, 20000, 'CANDLE_VETO', `bullish pattern [${candle.bullSignalName}].`); continue; }
      const breakdown = mom.change15mPct <= -0.80 && mom.rvol >= 1.35 && flow.takerBuyRatio <= 48;
      if (!candle.isShortEntry && !breakdown) { veto(pairKey, 30000, 'CANDLE_WAIT', `waiting for a completed bearish pattern on 15m (1h trend ${candle.major}) or a breakdown (15m <= -0.80%).`, 'info'); continue; }
      const label = candle.isShortEntry ? candle.shortPatternName : 'HIGH_VOL_BREAKDOWN';

      const book = await fetchRealOrderBookDepth(pairKey, true);
      if (!book) { veto(pairKey, 30000, 'BOOK_VETO', 'futures order book unavailable.'); continue; }
      if (book.obi > 0.55 || book.spreadPct > 0.20) { veto(pairKey, 30000, 'BOOK_VETO', `bid share ${Math.round(book.obi * 100)}% (req <= 55%) or spread ${book.spreadPct.toFixed(2)}%.`); continue; }

      // planEntry only uses the stop distance for sizing, so the distance is passed on the long side of the price.
      const px = book.bestBid, cfg = Strategy.CONFIG;
      let atr = NaN, structPct = cfg.minStopPct;
      const k15 = await getCachedKlines(pairKey, '15m', 16, 2500, 45000, true).catch(() => null);
      if (Array.isArray(k15) && k15.length >= 15) {
        const bars = Strategy.toBars(k15);
        atr = Strategy.atrPct(bars, 14);
        structPct = ((Math.max(...bars.slice(-3).map((b) => b.h)) - px) / px) * 100 + cfg.stopBufferPct;
      }
      const stopPct = Math.min(cfg.maxStopPct, Math.max(structPct, (atr || 0) * cfg.atrStopMult, cfg.minStopPct));
      const signal = { ok: true, pattern: label, refPrice: px, stopPrice: px * (1 - stopPct / 100), stopPct, atrPct: Number.isFinite(atr) ? atr : stopPct / cfg.atrStopMult, rvol: mom.rvol };
      const plan = Strategy.planCompoundEntry(signal, px, computeEquity(), Number(State.portfolio.cashUSDT));
      if (!plan.ok) { veto(pairKey, 30000, 'CASH_GUARD', plan.reason); continue; }
      const minN = perpetuals.get(pairKey)?.minNotional || 5;
      if (plan.notional < minN) { veto(pairKey, 300000, 'CASH_GUARD', `Binance minimum for this contract is $${minN}, position would be $${plan.notional.toFixed(2)}.`, 'info'); continue; }
      logOperation('BOOK_CLEAR', `${pairKey} short confirmed [${label}]: bid share ${Math.round(book.obi * 100)}%, spread ${book.spreadPct.toFixed(2)}%, taker buy ${flow.takerBuyRatio}%. Entering...`, 'success');
      openShort(pairKey, signal, plan, flow.takerBuyRatio);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// A v4-approved entry still needs a real stop for the position sentry: below the 3-bar swing low,
// at least 1.2 x ATR, clamped to 0.6%..1.5%. Clamped, never rejected: sizing must not add a gate.
async function buildEntrySignal(pairKey, price, pattern, microMom) {
  const cfg = Strategy.CONFIG;
  let atr = NaN, structPct = cfg.minStopPct;
  try {
    const k = await getCachedKlines(pairKey, '15m', 16, 2500, 45000, true);
    if (Array.isArray(k) && k.length >= 15) {
      const bars = Strategy.toBars(k);
      atr = Strategy.atrPct(bars, 14);
      structPct = ((price - Math.min(...bars.slice(-3).map((b) => b.l))) / price) * 100 + cfg.stopBufferPct;
    }
  } catch (e) {}
  const stopPct = Math.min(cfg.maxStopPct, Math.max(structPct, (atr || 0) * cfg.atrStopMult, cfg.minStopPct));
  return { ok: true, pattern, refPrice: price, stopPrice: price * (1 - stopPct / 100), stopPct,
    atrPct: Number.isFinite(atr) ? atr : stopPct / cfg.atrStopMult, rvol: microMom.rvol };
}

async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled || isEvaluatingEntries) return;

  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  const maxBagsLimit = State.tradeFrequencyController?.maxRecoveryBagsLimit || 5;
  if (activeBagsCount >= maxBagsLimit) {
    throttledNotice('bags', 'AUTOPILOT', `Max recovery bags active (${activeBagsCount}/${maxBagsLimit}). Awaiting relief recoveries before opening fresh trades.`, 'warning', 30000);
    return;
  }

  // The spec has no daily-loss / losing-streak stop, so this is status display only.
  const equity = computeEquity();
  const guard = Strategy.portfolioGuard(State.tradeLogs, equity);
  State.subsystems.riskGuard = {
    status: 'ARMED',
    reason: guard.ok ? `Day P&L ${guard.dayPnl >= 0 ? '+' : ''}$${(guard.dayPnl || 0).toFixed(2)}` : `Not enforced: ${guard.reason}`,
    equityUSDT: Number(equity.toFixed(2)),
    riskPerTradePct: Strategy.CONFIG.riskPerTradePct,
    dailyLossLimitPct: Strategy.CONFIG.dailyLossLimitPct
  };

  // Market mode decides which side may trade. Open positions are managed either way.
  const mode = State.subsystems.marketMode;
  if (mode.side === 'NONE') {
    throttledNotice('mode', 'MARKET_MODE', `No new trades: ${mode.reason}.`, 'info', 120000);
    return;
  }
  if (Number(State.portfolio.cashUSDT) < Strategy.CONFIG.minNotional * (1 + FUT_FEE_RATE)) {
    throttledNotice('cash', 'CASH_GUARD', 'Free cash is below the $5 minimum position. Waiting for open trades to close.', 'info', 60000);
    return;
  }
  if (mode.side !== 'LONG') await evaluateShortEntries(maxBagsLimit);
  if (mode.side === 'SHORT') return;

  if (learningEngine.recalculateDynamicHurdles) learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const minScore = State.activeThresholds?.scoreThreshold || 65;
  const keys = Object.keys(State.universe || {});
  if (keys.length === 0) return;
  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  isEvaluatingEntries = true;
  try {
    let evaluatedCount = 0;
    const now = Date.now();

    for (const k of sorted) {
      if (Object.keys(State.recoveryBags || {}).length >= maxBagsLimit) break;
      const coin = State.universe[k];
      if (!coin || !coin.symbol || coin.side !== 'LONG' || !coin.canTrade) continue;

      if (coin.isPumpAndDump) continue;
      if ((coin.score || 0) < minScore) break;

      const baseSym = String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
      const pairKey = `${baseSym}USDT`;

      const blockedDerivatives = ['SOXLB', 'SNDKB', 'TSLAB', 'AAPLB', 'NVDB', 'MSFTB', 'AMZNB', 'SPYB', 'QQQB'];
      if (blockedDerivatives.some(d => pairKey.startsWith(d))) continue;
      if (Number(coin.priceUSDT || 0) > 350.0 && !['BTCUSDT', 'ETHUSDT'].includes(pairKey)) continue;
      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;
      if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > now) continue;

      const nextAllowed = candidateCooldownMemo.get(pairKey) || 0;
      if (nextAllowed > now) continue;

      if (++evaluatedCount > 16) break;

      const change24h = Number(coin.priceChangePct || coin.change24h || 0);
      const rangePct = Number(coin.intradayRangePct || 0);
      if (change24h < 0.20 && rangePct < 3.0) continue;

      const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
      if (posInRange < 0.50) continue;
      if (State.subsystems.marketMode.mode === 'NEUTRAL' && !(await coinTrendAgrees(pairKey, 'LONG'))) { candidateCooldownMemo.set(pairKey, now + 60000); continue; }

      const volQuote = (coin.volumeM || 0) * 1000000;
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pdCheck.isTrap) {
          candidateCooldownMemo.set(pairKey, now + 300000);
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pdCheck.reason}`, 'danger');
          continue;
        }
      }

      const orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
      if (!orderFlow.passed) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('FLOW_VETO', `${pairKey} halted: ${orderFlow.reason || 'Negative Taker CVD'}`, 'warning');
        continue;
      }

      const priceUnit = Number(coin.priceUSDT || 0);
      if (priceUnit >= 0.50 && rangePct < 4.0 && change24h < 0.60) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('VOL_VETO', `${pairKey} skipped: Large-cap low volatility (Range: ${rangePct.toFixed(2)}% < 4.00%). Scalping slots reserved for high-beta momentum.`, 'info');
        continue;
      }

      const microMom = await checkMicrostructureMomentumAndVwap(pairKey);
      if (!microMom.passed) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('MOM_VETO', `${pairKey} halted: ${microMom.reason}`, 'warning');
        continue;
      }

      const trend1h = await check1hTrendAlignment(pairKey);
      if (!trend1h.passed) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('TREND_VETO', `${pairKey} halted: ${trend1h.reason}`, 'warning');
        continue;
      }

      const candleCheck = await checkCandlestickSentry(pairKey, true);
      if (candleCheck.isBearishReversal) {
        candidateCooldownMemo.set(pairKey, now + 20000);
        logOperation('CANDLE_VETO', `${pairKey} halted: Bearish pattern detected [${candleCheck.patternName}]. Rejecting distribution wick.`, 'warning');
        continue;
      }

      const isConfirmedPatternEntry = candleCheck.isBullishEntry;
      const isAuthenticIgnition = (microMom.change15mPct >= 0.80 && microMom.rvol >= 1.35 && orderFlow.takerBuyRatio >= 52.0);
      if (!isConfirmedPatternEntry && !isAuthenticIgnition) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('CANDLE_WAIT', `${pairKey} on standby: Awaiting a completed Nison pattern on 15m (1h trend ${candleCheck.major}) or Ignition (15m >= +0.80%).`, 'info');
        continue;
      }
      const patternLabel = isConfirmedPatternEntry ? candleCheck.bullishPatternName : 'HIGH_VOL_IGNITION';

      const book = await fetchRealOrderBookDepth(pairKey, true);
      if (!book) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('BOOK_VETO', `${pairKey} halted: futures order book unavailable.`, 'warning');
        continue;
      }
      if (book.obi < 0.45 || book.spreadPct > 0.20) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('BOOK_VETO', `${pairKey} halted: OBI ${Math.round(book.obi * 100)}% (req >= 45%) or Spread ${book.spreadPct.toFixed(2)}% (req <= 0.20%).`, 'warning');
        continue;
      }

      logOperation('BOOK_CLEAR', `${pairKey} confirmed [${patternLabel}]: OBI ${Math.round(book.obi * 100)}%, Spread ${book.spreadPct.toFixed(2)}%, Taker Buy ${orderFlow.takerBuyRatio}%. Entering...`, 'success');

      const signal = await buildEntrySignal(pairKey, book.bestAsk, patternLabel, microMom);
      const plan = Strategy.planCompoundEntry(signal, book.bestAsk, computeEquity(), Number(State.portfolio.cashUSDT));
      if (!plan.ok) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('CASH_GUARD', `${pairKey}: ${plan.reason}`, 'warning');
        continue;
      }
      const minN = perpetuals.get(pairKey)?.minNotional || 5;
      if (plan.notional < minN) {
        candidateCooldownMemo.set(pairKey, now + 300000);
        logOperation('CASH_GUARD', `${pairKey} long: Binance minimum for this contract is $${minN}, position would be $${plan.notional.toFixed(2)}.`, 'info');
        continue;
      }
      openLong(pairKey, signal, plan, orderFlow.takerBuyRatio);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// -------------------------------------------------------------
// POSITION SENTRY: never closes below the minimum exit, failing trades move to recovery bags
// -------------------------------------------------------------
let sentryBusy = false;

function migrateLegacyPosition(k, pos) {
  if (pos.strategyVersion === 6) return;
  // Older positions carried real stops. Those stops no longer apply.
  const entry = Number(pos.entryPriceUSDT);
  pos.initialStopPrice = entry * (1 + Strategy.EXIT_RULES.demotePct / 100);
  pos.sl = pos.initialStopPrice;
  pos.ratchetStage = 'INITIAL';
  pos.qty = Number(pos.qty) || Number(pos.notionalUSDT) / entry;
  pos.tp1Price = entry * (1 + Strategy.EXIT_RULES.tp1Pct / 100);
  pos.targetPrice = entry * 1.03;
  pos.strategyVersion = 6;
  logOperation('SENTRY', `${pos.pair}: moved to no-loss rules (never closed below the minimum exit, failing trades go to a recovery bag).`, 'warning');
}

// Live 5m candle read, same thresholds as the original Self-Aware detector.
async function checkSelfAwareOrderFlow(symbol, futures = false) {
  const quiet = { isApexExhausted: false, isUpsideDead: false, isBottomExhausted: false, isDownsideDead: false };
  try {
    const k = await getCachedKlines(symbol, '5m', 5, 2500, 20000, futures);
    if (!Array.isArray(k) || !k.length) return quiet;
    const cur = k[k.length - 1];
    const [o, h, l, c, vol, tb] = [cur[1], cur[2], cur[3], cur[4], cur[5], cur[9]].map(parseFloat);
    const range = h - l;
    const wick = range > 0 ? (h - Math.max(o, c)) / range : 0;
    const taker = vol > 0 ? (tb / vol) * 100 : 50;
    const clv = range > 0 ? (c - l) / range : 0.5;
    return { isApexExhausted: (taker < 42 && wick >= 0.35) || (clv < 0.30 && range > 0), isUpsideDead: taker < 40 && c < o && clv < 0.35,
      isBottomExhausted: (taker > 58 && (range > 0 ? (Math.min(o, c) - l) / range : 0) >= 0.35) || (clv > 0.70 && range > 0), isDownsideDead: taker > 60 && c > o && clv > 0.65 };
  } catch (e) { return quiet; }
}

function demoteToBag(k, pos, price, reason) {
  State.recoveryBags[k] = {
    pair: pos.pair, pairSymbol: pos.pairSymbol, openedAt: pos.openedAt, movedAt: Date.now(),
    entryPriceUSDT: Number(pos.entryPriceUSDT), livePriceUSDT: price, lastTickAt: Date.now(),
    notionalUSDT: pos.notionalUSDT, qty: pos.qty,
    side: isShort(pos) ? 'SHORT' : 'LONG', market: isFutures(pos) ? 'FUTURES' : 'SPOT', fundingUSDT: pos.fundingUSDT || 0, nextFundingAt: pos.nextFundingAt || 0, liquidationPrice: pos.liquidationPrice,
    targetStep: 'CLOSES AT +0.30%', targetExitPriceUSDT: Number(pos.entryPriceUSDT) * (1 + (isShort(pos) ? -1 : 1) * Strategy.EXIT_RULES.bagTargetPct / 100),
    troughPrice: price, exitInProgress: false, candlestickPattern: pos.candlestickPattern || 'MONITORING', strategyVersion: 6
  };
  delete State.activePositions[k];
  logOperation('SENTRY_DEMOTE', `${pos.pair}: ${reason}. Moved to ${isShort(pos) ? 'Short' : isFutures(pos) ? 'Long' : 'Spot'} Recovery Bag, closes at +0.30% ($${formatPrecisePrice(State.recoveryBags[k].targetExitPriceUSDT)}).`, 'warning');
  saveDiskState();
  broadcastStateUpdate();
}

async function livePriceFor(k, pos) {
  const tickPrice = Number(pos.livePriceUSDT || State.universe[k]?.priceUSDT);
  if (tickPrice > 0 && Date.now() - Number(pos.lastTickAt || 0) < 20000) return tickPrice;
  // Stream went quiet for this symbol: never manage risk on a stale price.
  const rest = isFutures(pos) ? await fetchFuturesPrice(k) : await fetchTickerPrice(k);
  if (rest) {
    pos.livePriceUSDT = rest;
    pos.lastTickAt = Date.now();
    return rest;
  }
  return tickPrice > 0 ? tickPrice : null;
}

// Exit signals need candle data, so they are refreshed here and the sentry only reads them.
// A slow Binance reply can then never hold up a price tick.
const exitSignals = new Map();
let signalsBusy = false;
async function refreshExitSignals() {
  if (signalsBusy) return;
  signalsBusy = true;
  try {
    for (const k of Object.keys(State.activePositions || {})) {
      const pos = State.activePositions[k];
      const entry = Number(pos.entryPriceUSDT), live = Number(pos.livePriceUSDT) || entry, short = isShort(pos);
      const gainPct = (short ? entry - live : live - entry) / entry * 100;
      const R = Strategy.EXIT_RULES;
      const s = {};
      if (gainPct >= minExitFor(pos) - 0.10) {
        const candle = await checkCandlestickSentry(k, isFutures(pos));
        const flow = await checkSelfAwareOrderFlow(k, isFutures(pos));
        if (short ? candle.isBullishSignal : candle.isBearishReversal) s.bearishPattern = short ? candle.bullSignalName : candle.bearishPatternName;
        s.peakExhausted = short ? flow.isBottomExhausted : flow.isApexExhausted;
      }
      exitSignals.set(k, s);
    }
    [...exitSignals.keys()].forEach((k) => { if (!State.activePositions[k]) exitSignals.delete(k); });
  } finally { signalsBusy = false; }
}

// Ticks arriving during a pass are not dropped: one more pass runs with the newest price.
let sentryPending = false;
function kickSentry() {
  if (sentryBusy) { sentryPending = true; return; }
  runPredictiveSentry().catch((e) => console.error('[Sentry Error]', e.message)).finally(() => {
    if (sentryPending) { sentryPending = false; setImmediate(kickSentry); }
  });
}

async function runPredictiveSentry() {
  if (sentryBusy) return;
  sentryBusy = true;
  try {
    // ---- active positions ----
    for (const k of Object.keys(State.activePositions || {})) {
      const pos = State.activePositions[k];
      if (!pos) continue;
      migrateLegacyPosition(k, pos);
      const price = await livePriceFor(k, pos);
      if (!price) continue;
      pos.livePriceUSDT = price;

      const entry = Number(pos.entryPriceUSDT);
      const short = isShort(pos);
      if (short && price >= Number(pos.liquidationPrice)) {
        realizeExit('activePositions', k, 1, price, 'LIQUIDATED', `1x margin used up at $${formatPrecisePrice(price)}`);
        continue;
      }
      const gainPct = (short ? entry - price : price - entry) / entry * 100;
      const held = (Date.now() - Number(pos.openedAt || Date.now())) / 60000;
      const cached = exitSignals.get(k) || {};
      // Harvest signals count only in profit, the dead-flow signal only in the soft-demote window.
      const minExitPct = minExitFor(pos);
      const signals = gainPct >= minExitPct ? { bearishPattern: cached.bearishPattern, peakExhausted: cached.peakExhausted } : {};

      const prevStage = pos.ratchetStage;
      const r = short ? Strategy.manageShort(pos, price, signals)
        : Strategy.manageNoLoss(pos, price, signals, Date.now(), { ...Strategy.EXIT_RULES, floorPct: minExitPct });
      if (pos.ratchetStage !== prevStage && pos.ratchetStage === 'BREAKEVEN_LOCKED') {
        logOperation('SENTRY', `${pos.pair}: +${minExitPct.toFixed(2)}% minimum exit locked ($${formatPrecisePrice(pos.sl)}).`, 'success');
      }

      // The trigger comes from the last trade; the fill must be what the book really pays.
      let fill = price;
      if (r.action === 'EXIT' || r.action === 'PARTIAL') {
        fill = (await executableExitPrice(k, pos)) || price;
        const fillGain = (short ? entry - fill : fill - entry) / entry * 100;
        if (fillGain < minExitPct) continue;
      }

      if (r.action === 'PARTIAL') {
        const sellValue = Number(pos.qty) * price * r.fraction;
        if (sellValue < Strategy.EXIT_RULES.minPartialUSDT) {
          logOperation('SENTRY', `${pos.pair}: +1.80% reached. Position too small to split, trailing all of it.`, 'success');
        } else {
          realizeExit('activePositions', k, r.fraction, fill, r.reason, '50% banked at +1.80%, runner trails');
        }
      } else if (r.action === 'EXIT') {
        realizeExit('activePositions', k, 1, fill, r.reason, `${r.reason} @ $${formatPrecisePrice(fill)} (peak +${(pos.peakGainPct || 0).toFixed(2)}%)`);
      } else if (r.action === 'DEMOTE') {
        demoteToBag(k, pos, price, r.reason);
      }
    }

    // ---- recovery bags: close at +0.30%, never in loss ----
    for (const k of Object.keys(State.recoveryBags || {})) {
      const bag = State.recoveryBags[k];
      if (!bag || bag.exitInProgress) continue;
      const price = await livePriceFor(k, bag);
      if (!price) continue;
      bag.livePriceUSDT = price;
      if (isShort(bag) && price >= Number(bag.liquidationPrice)) {
        realizeExit('recoveryBags', k, 1, price, 'LIQUIDATED', `1x margin used up at $${formatPrecisePrice(price)}`);
        continue;
      }
      const r = isShort(bag) ? Strategy.manageShortBag(bag, price) : Strategy.manageBag(bag, price);
      if (r.action === 'EXIT') {
        const fill = (await executableExitPrice(k, bag)) || price;
        const check = isShort(bag) ? Strategy.manageShortBag(bag, fill) : Strategy.manageBag(bag, fill);
        if (check.action === 'EXIT') realizeExit('recoveryBags', k, 1, fill, r.reason, `Recovery bag closed at +0.30% or better @ $${formatPrecisePrice(fill)}`);
      }
    }
  } finally {
    sentryBusy = false;
  }
}

// -------------------------------------------------------------
// CLOUD LEDGER & PERSISTENCE
// -------------------------------------------------------------
function normalizeDateToIndianFormat(dateVal, id, timeVal) {
  let idDate = null;
  const idNum = Number(id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    idDate = new Date(idNum);
  }

  let s = String(dateVal || '').trim();
  const sTime = String(timeVal || '').trim();

  if (!s || s === 'Recent' || s === '0' || s.includes('1899') || (s.includes(':') && !s.includes('/'))) {
    if (sTime && (sTime.includes('/') || sTime.includes('-')) && !sTime.includes('1899')) {
      s = sTime;
    } else if (idDate) {
      const d = String(idDate.getDate()).padStart(2, '0');
      const m = String(idDate.getMonth() + 1).padStart(2, '0');
      const y = idDate.getFullYear();
      return `${d}/${m}/${y}`;
    } else {
      return '01/11/2026';
    }
  }

  const slashMatch = s.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
  if (slashMatch) {
    const d = parseInt(slashMatch[1], 10);
    const m = parseInt(slashMatch[2], 10);
    let y = parseInt(slashMatch[3], 10);
    if (y < 2000 && idDate) y = idDate.getFullYear();
    return `${String(d).padStart(2, '0')}/${String(m).padStart(2, '0')}/${y}`;
  }

  if (idDate) {
    const d = String(idDate.getDate()).padStart(2, '0');
    const m = String(idDate.getMonth() + 1).padStart(2, '0');
    const y = idDate.getFullYear();
    return `${d}/${m}/${y}`;
  }

  return '01/11/2026';
}

function getTradeTimeMs(t) {
  if (!t) return 0;
  const dStr = String(t.date || '').trim();
  const tStr = String(t.time || '').trim();
  const slashMatch = dStr.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
  if (slashMatch) {
    const day = parseInt(slashMatch[1], 10);
    const month = parseInt(slashMatch[2], 10) - 1;
    const year = parseInt(slashMatch[3], 10);
    let hours = 0, minutes = 0, seconds = 0;
    const timeMatch = tStr.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (timeMatch) {
      hours = parseInt(timeMatch[1], 10);
      minutes = parseInt(timeMatch[2], 10);
      seconds = timeMatch[3] ? parseInt(timeMatch[3], 10) : 0;
    }
    const dt = new Date(year, month, day, hours, minutes, seconds);
    if (!isNaN(dt.getTime())) return dt.getTime();
  }

  const idNum = Number(t.id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    return idNum;
  }
  return idNum || 0;
}

function sanitizeAndAuditTradeHistory(logs) {
  if (!Array.isArray(logs)) return [];
  const seenSignatures = new Set();

  return logs.filter(t => {
    if (!t || typeof t !== 'object') return false;
    const p = String(t.pair || t.Pair || t.symbol || t.Symbol || '').trim().toUpperCase();
    if (!p || p === 'CRYPTO/USDT' || p.includes('CRYPTO') || p === 'UNKNOWN/USDT' || !p.includes('USDT')) return false;

    const entry = Number(t.entryPriceUSDT || t['Entry Price (USDT)'] || t['Entry (USDT)'] || t.entryPrice || t.entry || t.Entry || 0);
    const exit = Number(t.exitPriceUSDT || t['Exit Price (USDT)'] || t['Exit (USDT)'] || t.exitPrice || t.exit || t.Exit || 0);
    if (isNaN(entry) || entry <= 0 || isNaN(exit) || exit <= 0) return false;

    return true;
  }).map(t => {
    const rawPair = String(t.pair || t.Pair || t.symbol || '').trim().toUpperCase();
    const pair = rawPair.includes('/') ? rawPair : `${rawPair.replace('USDT', '')}/USDT`;
    const entry = Number(t.entryPriceUSDT || t['Entry Price (USDT)'] || t['Entry (USDT)'] || t.entryPrice || t.entry || t.Entry || 1);
    const exit = Number(t.exitPriceUSDT || t['Exit Price (USDT)'] || t['Exit (USDT)'] || t.exitPrice || t.exit || t.Exit || entry);
    const tradeId = Number(t.id || t.ID || t.Id) || Date.now();
    const cleanDate = normalizeDateToIndianFormat(t.date, tradeId, t.time);
    const cleanTime = String(t.time || '').trim() || '00:00:00';

    const existingLocal = (State.tradeLogs || []).find(x => x.id === tradeId || (x.pair === pair && x.date === cleanDate && Math.abs(Number(x.entryPriceUSDT || 0) - entry) < 0.0001));
    let notional = existingLocal && existingLocal.notionalUSDT && existingLocal.notionalUSDT !== 50
      ? Number(existingLocal.notionalUSDT)
      : Number(t.notionalUSDT || t['Notional (USDT)'] || t.notional || 0);

    let pnlUSDT = Number(t.pnlUSDT !== undefined ? t.pnlUSDT : (t['PnL (USDT)'] !== undefined ? t['PnL (USDT)'] : t.pnl));

    const priceGainRatio = (entry > 0 && exit > 0) ? Math.abs((exit - entry) / entry) : 0;
    if ((!notional || notional === 50) && !isNaN(pnlUSDT) && Math.abs(pnlUSDT) > 0.0001 && priceGainRatio > 0.0005) {
      const derivedNotional = Math.abs(pnlUSDT) / priceGainRatio;
      if (derivedNotional >= 5.0 && derivedNotional <= 5000.0) {
        notional = Number(derivedNotional.toFixed(2));
      }
    }

    if (!notional || notional === 50) {
      notional = existingLocal?.notionalUSDT ? Number(existingLocal.notionalUSDT) : (Number(State.portfolio.fixedNotionalUSDT) || 10.50);
    }

    if (isNaN(pnlUSDT)) {
      pnlUSDT = (exit - entry) * (notional / entry);
    }

    return {
      ...t,
      id: tradeId,
      date: cleanDate,
      time: cleanTime,
      pair,
      type: String(t.type || t.Type || 'EXIT').trim().toUpperCase(),
      entryPriceUSDT: entry,
      exitPriceUSDT: exit,
      notionalUSDT: Number(notional.toFixed(2)),
      pnlUSDT: Number(pnlUSDT.toFixed(4)),
      pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2))
    };
  }).filter(t => {
    const timeMinute = String(t.time || '').slice(0, 5);
    const sig = `${t.pair}_${t.date}_${timeMinute}_${t.entryPriceUSDT.toFixed(8)}_${t.exitPriceUSDT.toFixed(8)}_${t.type}`;
    if (seenSignatures.has(sig)) return false;
    seenSignatures.add(sig);
    return true;
  }).sort((a, b) => getTradeTimeMs(b) - getTradeTimeMs(a));
}

async function syncTradesFromGoogleSheets() {
  const masterUrl = State.masterLedgerUrl;

  try {
    const fullUrl = masterUrl.includes('?') ? `${masterUrl}&action=GET_ALL` : `${masterUrl}?action=GET_ALL`;
    const data = await safeFetch(fullUrl, 10000);
    let fetchedTrades = [];
    if (data && Array.isArray(data.trades) && data.trades.length > 0) {
      fetchedTrades = data.trades;
    } else if (Array.isArray(data) && data.length > 0) {
      fetchedTrades = data;
    }

    if (fetchedTrades.length > 0) {
      const merged = [...fetchedTrades];
      (State.tradeLogs || []).forEach(localTrade => {
        if (!merged.some(f => f.id === localTrade.id || (f.pair === localTrade.pair && f.date === localTrade.date && f.time === localTrade.time))) {
          merged.push(localTrade);
        }
      });
      State.tradeLogs = sanitizeAndAuditTradeHistory(merged);
      saveDiskState();
      broadcastStateUpdate();
      console.log(`[Sync Ledger] Successfully synchronized ${State.tradeLogs.length} verified trades.`);
      return State.tradeLogs;
    }
  } catch (err) {
    console.warn(`[Sync Ledger] Notice:`, err.message);
  }
  return State.tradeLogs;
}

async function recordTradeToCloudLedger(trade) {
  const tradeId = Number(trade.id || Date.now());
  const now = new Date();
  const cleanDate = normalizeDateToIndianFormat(trade.date || now.toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }), tradeId);
  const cleanTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });

  const record = {
    ...trade,
    action: 'RECORD_TRADE',
    id: tradeId,
    date: cleanDate,
    time: cleanTime,
    pair: trade.pair,
    type: trade.type,
    entryPriceUSDT: Number(trade.entryPriceUSDT),
    exitPriceUSDT: Number(trade.exitPriceUSDT),
    notionalUSDT: Number(trade.notionalUSDT || 10.05),
    pnlUSDT: Number(trade.pnlUSDT),
    pnlINR: Number((trade.pnlUSDT * State.usdtToInrRate).toFixed(2)),
    regime: State.marketRegime || 'BALANCED',
    volatilityPct: 4.5,
    reason: trade.reason || 'Closed trade'
  };

  const timeMinute = cleanTime.slice(0, 5);
  const signature = `${record.pair}_${cleanDate}_${timeMinute}_${record.entryPriceUSDT}_${record.exitPriceUSDT}_${record.type}`;
  if (recentTradeDispatches.has(signature)) return;
  recentTradeDispatches.add(signature);
  if (recentTradeDispatches.size > 200) {
    const first = recentTradeDispatches.values().next().value;
    recentTradeDispatches.delete(first);
  }

  State.tradeLogs.unshift(record);
  saveDiskState();

  const targetUrl = (trade.targetUrl || State.masterLedgerUrl || '').trim();
  let success = false;

  try {
    console.log(`[Cloud Ledger] Dispatching trade ${record.pair} (${record.type}) strictly to Sisir_Quant_Trade_Ledger...`);
    const res = await safePostJson(targetUrl, record, 15000);
    console.log(`[Cloud Ledger Response]`, JSON.stringify(res));
    if (res && res.success !== false) {
      success = true;
    }
  } catch (err) {
    console.error(`[Cloud Ledger POST Error] ${err.message}. Retrying via GET fallback...`);
  }

  if (!success) {
    try {
      const getUrl = targetUrl.includes('?') 
        ? `${targetUrl}&action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`
        : `${targetUrl}?action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`;
      const getRes = await safeFetch(getUrl, 15000);
      console.log(`[Cloud Ledger GET Fallback Response]`, JSON.stringify(getRes));
      if (getRes && getRes.success !== false) {
        success = true;
      }
    } catch (err2) {
      console.warn(`[Cloud Ledger GET Fallback Notice]`, err2.message);
    }
  }

  learningEngine.recordTradeOutcome(
    record.pair.replace('/USDT', 'USDT'),
    record.type,
    record.pnlUSDT,
    1.0
  );

  broadcastStateUpdate();
  return { success: true };
}

function saveDiskState() {
  try {
    const data = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      recoveryBags: State.recoveryBags,
      tradeLogs: State.tradeLogs.slice(0, 150),
      masterLedgerUrl: State.masterLedgerUrl,
      learningSheetUrl: State.learningSheetUrl,
      capitalResetId: State.capitalResetId || CAPITAL_RESET_ID
    };
    fs.writeFileSync(DISK_STATE_FILE, JSON.stringify(data, null, 2), 'utf-8');
  } catch (e) {}
}

function loadDiskState() {
  try {
    if (fs.existsSync(DISK_STATE_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DISK_STATE_FILE, 'utf-8'));
      if (parsed.portfolio) State.portfolio = parsed.portfolio;
      if (parsed.activePositions) State.activePositions = parsed.activePositions;
      if (parsed.recoveryBags) State.recoveryBags = parsed.recoveryBags;
      if (Array.isArray(parsed.tradeLogs)) State.tradeLogs = parsed.tradeLogs;
      if (parsed.masterLedgerUrl && !parsed.masterLedgerUrl.includes('AKfycbw3cTF2Q3X') && !parsed.masterLedgerUrl.includes('AKfycbxzrx')) {
        State.masterLedgerUrl = parsed.masterLedgerUrl;
      } else {
        State.masterLedgerUrl = 'https://script.google.com/macros/s/AKfycbwwFkRk4O2oJYmgrRcQBpx3z5KBKEOCJoX6YzL1yJXdot93V1T08ewIGaF78O1n6Z1ktg/exec';
      }
      if (parsed.learningSheetUrl && !parsed.learningSheetUrl.includes('AKfycbxzrxMMq')) {
        State.learningSheetUrl = parsed.learningSheetUrl;
      } else {
        State.learningSheetUrl = 'https://script.google.com/macros/s/AKfycbwut72lTkCVcqIV2k7YlzlsIOwO3GXCKqlF-4UBxvTvJHckJApKKxzbfFp5JN0VLND8/exec';
      }
      State.capitalResetId = parsed.capitalResetId;
    }
  } catch (e) {}
  applyCapitalReset();
}

// One-time re-base of the paper account to STARTING_EQUITY_USDT.
// Open positions are kept; cash becomes whatever is left after their market value.
function applyCapitalReset() {
  if (State.capitalResetId === CAPITAL_RESET_ID) return;
  const held = computeEquity() - (Number(State.portfolio.cashUSDT) || 0);
  State.portfolio.cashUSDT = Number(Math.max(0, STARTING_EQUITY_USDT - held).toFixed(6));
  State.portfolio.fixedNotionalUSDT = Strategy.CONFIG.minNotional;
  State.capitalResetId = CAPITAL_RESET_ID;
  console.log(`[Portfolio] Account re-based to $${STARTING_EQUITY_USDT}: cash $${State.portfolio.cashUSDT.toFixed(2)}, open positions $${held.toFixed(2)}.`);
  saveDiskState();
}

// -------------------------------------------------------------
// EXPRESS APP & HTTP SERVER SETUP
// -------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const indexHtml = path.join(__dirname, 'index.html');
  if (fs.existsSync(indexHtml)) {
    return res.sendFile(indexHtml);
  }
  res.json({ status: 'ONLINE', engine: 'Sisir Quant Trading Engine' });
});

app.get(['/pnl_dashboard', '/pnl_dashboard.html'], (req, res) => {
  const pnlHtml = path.join(__dirname, 'pnl_dashboard.html');
  if (fs.existsSync(pnlHtml)) {
    return res.sendFile(pnlHtml);
  }
  res.status(404).send('P&L Dashboard file not found');
});

// REST API Endpoints
app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs,
    subsystems: State.subsystems,
    tradeFrequencyController: State.tradeFrequencyController,
    activeThresholds: State.activeThresholds,
    marketRegime: State.marketRegime,
    usdtToInrRate: State.usdtToInrRate,
    masterLedgerUrl: State.masterLedgerUrl,
    learningSheetUrl: State.learningSheetUrl,
    learningEngineState: learningEngine.state || {},
    riskConfig: Strategy.CONFIG,
    equityUSDT: computeEquity()
  });
});

app.post('/api/portfolio/cash', (req, res) => {
  const cash = parseFloat(req.body.cashUSDT);
  if (!isNaN(cash) && cash >= 0) {
    State.portfolio.cashUSDT = cash;
    saveDiskState();
    broadcastStateUpdate();
    return res.json({ success: true, cashUSDT: cash });
  }
  res.status(400).json({ success: false, error: 'Invalid cash' });
});

app.post('/api/portfolio/sizing', (req, res) => {
  const pct = parseFloat(req.body.allocationPct);
  if (!isNaN(pct) && pct >= 1 && pct <= 100) {
    State.portfolio.allocationPct = pct;
    saveDiskState();
    broadcastStateUpdate();
    return res.json({ success: true, allocationPct: pct });
  }
  res.status(400).json({ success: false, error: 'Invalid sizing' });
});

app.get('/api/screener/run', async (req, res) => {
  await runDynamicScreener();
  res.json({ success: true, universe: State.universe });
});

app.get('/api/p2p/sync', (req, res) => {
  res.json({ success: true, usdtToInrRate: State.usdtToInrRate });
});

app.post('/api/trades/purge-empty', (req, res) => {
  State.tradeLogs = State.tradeLogs.filter(t => t && t.pair && t.pair.includes('USDT') && t.entryPriceUSDT > 0);
  saveDiskState();
  res.json({ success: true, trades: State.tradeLogs });
});

app.post('/api/trades/sync-strict-sheet', async (req, res) => {
  try {
    const trades = await syncTradesFromGoogleSheets();
    res.json({ success: true, count: trades.length, trades });
  } catch (err) {
    res.json({ success: true, count: State.tradeLogs.length, trades: State.tradeLogs });
  }
});

app.post('/api/ledger/set-url', (req, res) => {
  const { url } = req.body;
  if (url && typeof url === 'string') {
    State.masterLedgerUrl = url.trim();
    saveDiskState();
    return res.json({ success: true, masterLedgerUrl: State.masterLedgerUrl });
  }
  res.status(400).json({ success: false, error: 'Invalid URL' });
});

app.post('/api/ledger/test', async (req, res) => {
  const testUrl = (req.body.url || State.masterLedgerUrl || '').trim();
  if (!testUrl) return res.status(400).json({ success: false, error: 'Missing ledger URL' });
  try {
    const fetchUrl = testUrl.includes('?') ? `${testUrl}&action=TEST` : `${testUrl}?action=TEST`;
    const data = await safeFetch(fetchUrl, 15000);
    return res.json({ success: true, source: 'SERVER_PROXY', data });
  } catch (err) {
    return res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/ledger/send-test-trade', async (req, res) => {
  const targetUrl = (req.body.url || State.masterLedgerUrl || '').trim();
  if (!targetUrl) return res.status(400).json({ success: false, error: 'Missing ledger URL' });

  const testTrade = {
    id: Date.now(),
    date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
    pair: 'TEST/USDT',
    type: 'DIAGNOSTIC TEST WRITE',
    entryPriceUSDT: 1.0000,
    exitPriceUSDT: 1.0050,
    notionalUSDT: 10.00,
    pnlUSDT: 0.0500,
    pnlINR: Number((0.05 * State.usdtToInrRate).toFixed(2)),
    reason: 'Connection test from terminal UI',
    targetUrl: targetUrl
  };

  try {
    const writeResult = await recordTradeToCloudLedger(testTrade);
    res.json({ success: true, trade: testTrade, cloudResult: writeResult });
  } catch (err) {
    res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/learning/test', async (req, res) => {
  const testUrl = (req.body.url || State.learningSheetUrl || '').trim();
  if (!testUrl) return res.status(400).json({ success: false, error: 'Missing learning URL' });
  try {
    const fetchUrl = testUrl.includes('?') ? `${testUrl}&action=TEST` : `${testUrl}?action=TEST`;
    const data = await safeFetch(fetchUrl, 15000);
    return res.json({ success: true, source: 'SERVER_PROXY', data });
  } catch (err) {
    return res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/positions/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const pos = State.activePositions[pairKey];
  if (!pos) return res.status(404).json({ success: false, error: 'Position not found' });
  const price = await livePriceFor(pairKey, pos);
  if (!price) return res.status(503).json({ success: false, error: 'No live price available' });
  const fill = (await executableExitPrice(pairKey, pos)) || price;
  const trade = realizeExit('activePositions', pairKey, 1, fill, 'MANUAL EXIT', `Manual close at $${formatPrecisePrice(fill)}`);
  res.json({ success: Boolean(trade), trade });
});

app.post('/api/bags/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const bag = State.recoveryBags[pairKey];
  if (!bag) return res.status(404).json({ success: false, error: 'Bag not found' });
  const price = await livePriceFor(pairKey, bag);
  if (!price) return res.status(503).json({ success: false, error: 'No live price available' });
  const fill = (await executableExitPrice(pairKey, bag)) || price;
  const trade = realizeExit('recoveryBags', pairKey, 1, fill, 'BAG MANUAL EXIT', `Manual bag close at $${formatPrecisePrice(fill)}`);
  res.json({ success: Boolean(trade), trade });
});

app.get('/api/risk', (req, res) => {
  const equity = computeEquity();
  res.json({
    mode: 'PAPER',
    equityUSDT: Number(equity.toFixed(2)),
    guard: Strategy.portfolioGuard(State.tradeLogs, equity),
    config: Strategy.CONFIG
  });
});

// -------------------------------------------------------------
// WEBSOCKET SERVER & REAL-TIME BROADCASTS
// -------------------------------------------------------------
const server = http.createServer(app);
// permessage-deflate: JSON state compresses 60-90%, so every message to the app costs less.
// Browsers and the Android WebView negotiate it automatically.
const wss = new WebSocket.Server({
  server,
  perMessageDeflate: { threshold: 256, zlibDeflateOptions: { level: 6 } }
});

const BROADCAST_TRADE_LOGS = 50; // the app keeps older trades and syncs the full ledger from the sheet
const hasClients = () => [...wss.clients].some((c) => c.readyState === WebSocket.OPEN);
const sendAll = (msg) => wss.clients.forEach((c) => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
// lastTickAt changes every tick but the app never reads it; leaving it out lets
// unchanged price frames be skipped instead of re-sent.
const omitTickStamps = (key, value) => (key === 'lastTickAt' ? undefined : value);

// withUniverse: only the screener changes the radar list, so only it re-sends it.
function broadcastStateUpdate({ withUniverse = false } = {}) {
  if (!hasClients()) return;
  const payload = {
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    tradeLogs: State.tradeLogs.slice(0, BROADCAST_TRADE_LOGS),
    subsystems: State.subsystems,
    riskConfig: Strategy.CONFIG,
    equityUSDT: computeEquity()
  };
  if (withUniverse) payload.universe = State.universe;
  sendAll(JSON.stringify(payload, omitTickStamps));
  lastDetailSig = JSON.stringify({ ...State.activePositions, ...State.recoveryBags }, omitLive); // this frame already carried them
  lastSentPrices = Object.fromEntries(Object.entries({ ...State.activePositions, ...State.recoveryBags }).map(([k, p]) => [k, p.livePriceUSDT]));
}

// Every 250 ms the app gets only the prices that moved (about 60 bytes). Position details such
// as trail levels change far less often, so they go at most every 2 seconds, and only if changed.
let lastSentPrices = {};
let lastDetailSig = '';
let lastDetailAt = 0;
const omitLive = (key, value) => (key === 'lastTickAt' || key === 'livePriceUSDT' ? undefined : value);
function broadcastPriceStream() {
  if (!hasClients()) return;
  const held = { ...State.activePositions, ...State.recoveryBags };
  const sig = JSON.stringify(held, omitLive);
  if (sig !== lastDetailSig && Date.now() - lastDetailAt >= 2000) {
    lastDetailSig = sig;
    lastDetailAt = Date.now();
    lastSentPrices = Object.fromEntries(Object.entries(held).map(([k, p]) => [k, p.livePriceUSDT]));
    sendAll(JSON.stringify({ type: 'PRICE_STREAM', activePositions: State.activePositions, recoveryBags: State.recoveryBags }, omitTickStamps));
    return;
  }
  const moved = {};
  Object.entries(held).forEach(([k, p]) => { if (p.livePriceUSDT !== lastSentPrices[k]) moved[k] = lastSentPrices[k] = p.livePriceUSDT; });
  if (Object.keys(moved).length) sendAll(JSON.stringify({ type: 'P', p: moved }));
}

// -------------------------------------------------------------
// ZERO-LATENCY DIRECT BINANCE WEBSOCKET INGESTION (SINGAPORE)
// -------------------------------------------------------------
let binanceGlobalWs = null;
let subscribedActiveStreams = new Set();

// Bandwidth: this used to stream !miniTicker@arr (every Binance pair, every second,
// ~1.2 GB/day) although only held coins need live ticks — radar prices refresh with the
// screener every minute. Now it subscribes <sym>@aggTrade (every real trade, unlike miniTicker's 1s batches) for held coins only: same
// 1-second cadence for the coins the sentry manages, ~1% of the data.
function connectBinanceGlobalStream() {
  const wsEndpoint = 'wss://stream.binance.com:9443/ws';

  try {
    if (binanceGlobalWs) {
      try { binanceGlobalWs.terminate(); } catch (e) {}
      binanceGlobalWs = null;
    }

    binanceGlobalWs = new WebSocket(wsEndpoint);

    binanceGlobalWs.on('open', () => {
      console.log('[Binance Stream] Connected. Live ticks for held coins only.');
      subscribedActiveStreams = new Set(); // a fresh connection has no subscriptions
      syncActiveSymbolSubscriptions();
    });

    binanceGlobalWs.on('message', (data) => {
      try {
        const parsed = JSON.parse(data);
        // trades arrive one object at a time; ignore SUBSCRIBE acks
        const tickers = parsed && parsed.e === 'aggTrade' ? [{ s: parsed.s, c: parsed.p }] : null;
        if (!tickers) return;

        const rate = Number(State.usdtToInrRate) || 94.94;
        let activePriceChanged = false;

        tickers.forEach(t => {
          const sym = t.s;
          if (!sym || !sym.endsWith('USDT')) return;

          const closePrice = parseFloat(t.c);
          if (isNaN(closePrice) || closePrice <= 0) return;

          if (State.universe[sym]) {
            State.universe[sym].priceUSDT = closePrice;
            State.universe[sym].priceINR = Number((closePrice * rate).toFixed(2));
          }

          if (State.activePositions[sym] && !isFutures(State.activePositions[sym])) {
            State.activePositions[sym].livePriceUSDT = closePrice;
            State.activePositions[sym].lastTickAt = Date.now();
            activePriceChanged = true;
          }

          if (State.recoveryBags[sym] && !isFutures(State.recoveryBags[sym])) {
            State.recoveryBags[sym].livePriceUSDT = closePrice;
            State.recoveryBags[sym].lastTickAt = Date.now();
            activePriceChanged = true;
          }
        });

        if (activePriceChanged) kickSentry();
      } catch (err) {}
    });

    binanceGlobalWs.on('error', (err) => {
      console.warn('[Binance Stream Error]', err.message);
    });

    binanceGlobalWs.on('close', () => {
      setTimeout(connectBinanceGlobalStream, 2500);
    });
  } catch (err) {
    setTimeout(connectBinanceGlobalStream, 4000);
  }
}

// Futures positions are marked on the futures price. This socket exists only while one is held.
let futuresWs = null;
let futuresStreams = new Set();
const heldFutures = () => [...Object.entries(State.activePositions || {}), ...Object.entries(State.recoveryBags || {})].filter(([, p]) => isFutures(p)).map(([k]) => k);

function syncFuturesStream() {
  const wanted = new Set(heldFutures().map((s) => `${s.toLowerCase()}@aggTrade`));
  if (!wanted.size) {
    if (futuresWs) { const w = futuresWs; futuresWs = null; futuresStreams = new Set(); try { w.terminate(); } catch (e) {} }
    return;
  }
  if (!futuresWs) {
    const w = new WebSocket(FSTREAM_URL);
    futuresWs = w;
    futuresStreams = new Set();
    w.on('open', syncFuturesStream);
    w.on('message', (data) => {
      try {
        const t = JSON.parse(data);
        if (!t || t.e !== 'aggTrade') return;
        const price = parseFloat(t.p);
        if (!(price > 0)) return;
        let changed = false;
        [State.activePositions[t.s], State.recoveryBags[t.s]].forEach((p) => {
          if (isFutures(p)) { p.livePriceUSDT = price; p.lastTickAt = Date.now(); changed = true; }
        });
        if (changed) kickSentry();
      } catch (e) {}
    });
    w.on('error', (e) => console.warn('[Futures Stream Error]', e.message));
    w.on('close', () => { if (futuresWs === w) { futuresWs = null; setTimeout(syncFuturesStream, 2500); } });
    return;
  }
  if (futuresWs.readyState !== WebSocket.OPEN) return;
  const add = [...wanted].filter((s) => !futuresStreams.has(s));
  const drop = [...futuresStreams].filter((s) => !wanted.has(s));
  if (add.length) { add.forEach((s) => futuresStreams.add(s)); futuresWs.send(JSON.stringify({ method: 'SUBSCRIBE', params: add, id: Date.now() })); }
  if (drop.length) { drop.forEach((s) => futuresStreams.delete(s)); futuresWs.send(JSON.stringify({ method: 'UNSUBSCRIBE', params: drop, id: Date.now() + 1 })); }
}

function syncActiveSymbolSubscriptions() {
  syncFuturesStream();
  if (!binanceGlobalWs || binanceGlobalWs.readyState !== WebSocket.OPEN) return;
  const currentSymbols = new Set([
    ...Object.keys(State.activePositions || {}).filter((k) => !isFutures(State.activePositions[k])),
    ...Object.keys(State.recoveryBags || {}).filter((k) => !isFutures(State.recoveryBags[k]))
  ]);

  const wanted = new Set([...currentSymbols].map((sym) => `${sym.toLowerCase()}@aggTrade`));
  const newStreams = [...wanted].filter((s) => !subscribedActiveStreams.has(s));
  const oldStreams = [...subscribedActiveStreams].filter((s) => !wanted.has(s));

  if (newStreams.length > 0) {
    newStreams.forEach((s) => subscribedActiveStreams.add(s));
    binanceGlobalWs.send(JSON.stringify({ method: 'SUBSCRIBE', params: newStreams, id: Date.now() }));
  }
  if (oldStreams.length > 0) {
    // stop paying for ticks on coins that were sold
    oldStreams.forEach((s) => subscribedActiveStreams.delete(s));
    binanceGlobalWs.send(JSON.stringify({ method: 'UNSUBSCRIBE', params: oldStreams, id: Date.now() + 1 }));
  }
}

wss.on('connection', (ws) => {
  // Only what the dashboard reads (learning-engine state, regime etc. were never used by it).
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs.slice(0, 150),
    subsystems: State.subsystems,
    riskConfig: Strategy.CONFIG
  }, omitTickStamps));

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'SYNC_STATE') {
        broadcastStateUpdate();
      } else if (msg.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(msg.enabled);
        broadcastStateUpdate();
      } else if (msg.type === 'SET_CASH' && !isNaN(parseFloat(msg.cashUSDT))) {
        State.portfolio.cashUSDT = parseFloat(msg.cashUSDT);
        saveDiskState();
        broadcastStateUpdate();
      } else if (msg.type === 'SET_SIZING_CONFIG') {
        if (!isNaN(parseFloat(msg.allocationPct))) {
          State.portfolio.allocationPct = parseFloat(msg.allocationPct);
        }
        if (msg.sizingMode) State.portfolio.sizingMode = msg.sizingMode;
        saveDiskState();
        broadcastStateUpdate();
      } else if (msg.type === 'SET_GOOGLE_SHEET_URL') {
        if (msg.masterLedgerUrl) State.masterLedgerUrl = msg.masterLedgerUrl;
        if (msg.learningSheetUrl) State.learningSheetUrl = msg.learningSheetUrl;
        saveDiskState();
        broadcastStateUpdate();
      }
    } catch (e) {}
  });
});

// -------------------------------------------------------------
// BOOTSTRAP TIMERS & SERVICE INITIALIZATION
// -------------------------------------------------------------
loadDiskState();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(`SISIR QUANT ENGINE ONLINE ON PORT ${PORT}`);
  console.log(`Low-Latency Singapore Stream + Sentinel Active (<5ms)`);
  console.log(`Strategy v5.0 RISK-FIRST • PAPER TRADING (no real orders)`);
  console.log(`Stop 0.6-1.5% • Risk ${Strategy.CONFIG.riskPerTradePct}%/trade • Daily limit -${Strategy.CONFIG.dailyLossLimitPct}%`);
  console.log(`====================================================`);

  runDynamicScreener().then(() => {
    console.log('[Screener] Initialized', Object.keys(State.universe).length, 'Binance Spot USDT pairs.');
  }).catch((e) => {
    console.error('[Screener Init Error]', e.message);
  });

  syncTradesFromGoogleSheets().catch(() => {});
  connectBinanceGlobalStream();
});

// The engine writes its own trades, so this only picks up edits made in the sheet.
setInterval(() => {
  syncTradesFromGoogleSheets().catch(() => {});
}, 600000);

setInterval(async () => {
  try {
    await runPredictiveSentry();
    await evaluateAutopilotEntries();
    syncActiveSymbolSubscriptions(); // add ticks for new buys, drop them for sold coins
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 1000);

setInterval(() => { settleFunding().catch((e) => console.error('[Funding]', e.message)); }, 60000);
setInterval(broadcastPriceStream, 250);
setInterval(() => { refreshExitSignals().catch(() => {}); }, 3000);
setInterval(runDynamicScreener, 60000);
