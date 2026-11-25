/**
 * =========================================================================
 * SISIR QUANT TERMINAL - BACKEND ENGINE v5.8 (v4 ENTRIES, NO-LOSS EXITS, RECOVERY BAGS)
 * =========================================================================
 * PAPER TRADING ENGINE: positions are simulated against live Binance prices.
 * No orders are sent to Binance from this file.
 *
 * Exits: the engine never closes below +0.30% (long) or +0.20% (short). A failing position moves to a Recovery Bag,
 * which closes at entry (0.00%), however long it takes (v6.22; no time-based cut). A bag still above 1x
 * gets free cash every second until it is 1x (v6.19), but only cash above one new trade's margin (v6.21).
 * v6.23: Settings > Entry strategy picks the candlestick model above or the Fibonacci retracement autopilot (fib.js),
 * which has its own stop-loss and targets. v6.24: the Fibonacci engine is v2 + confluence on 1h.
 * See CHANGES.md v5.8, v6.18 to v6.24.
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
const Fib = require('./fib');   // v6.23: Fibonacci retracement autopilot (Settings > Entry strategy)
// v6.31: the characters' comments, written by Google Gemini on the server. The key is GEMINI_API_KEY in Render's
// environment and never leaves this server; without it the app uses the characters' built-in lines.
const Commentary = require('./commentary').create({
  apiKey: process.env.GEMINI_API_KEY,
  models: process.env.GEMINI_MODELS ? process.env.GEMINI_MODELS.split(',').map((s) => s.trim()).filter(Boolean) : undefined,
  log: (msg, level) => logOperation('AI', msg, level)
});
// v6.33: the hourly momentum report (momentum.js), written by Gemini with the paid key GEMINI_MOMENTUM_KEY
// (GEMINI_API_KEY if that is not set). v6.34: its direction decides which coins may be traded (bearish for a short);
// its score is used only for the leverage, with auto leverage on.
const Momentum = require('./momentum');
const MomentumReport = Momentum.create({
  apiKey: process.env.GEMINI_MOMENTUM_KEY || process.env.GEMINI_API_KEY,
  models: process.env.GEMINI_MOMENTUM_MODELS ? process.env.GEMINI_MOMENTUM_MODELS.split(',').map((s) => s.trim()).filter(Boolean) : undefined,
  log: (msg, level) => logOperation('MOMENTUM', msg, level)
});

const PORT = process.env.PORT || 3000;
const DISK_STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');

// v6.15: a new build takes over the open trades of the build it replaces. On Render a deploy starts the new instance
// while the old one still answers the public URL, and the new instance's disk starts empty. Before it opens its port,
// the new instance asks the old one (through the public URL, which still reaches it) to pause trading and hand over its
// whole account (POST /api/handoff). The old one stays paused; it resumes only if the new build never goes live.
const BOOT_ID = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const HANDOFF_URL = String(process.env.HANDOFF_URL || process.env.RENDER_EXTERNAL_URL || '').trim().replace(/\/+$/, '');
const Handover = { ready: false, frozen: false, to: '', at: 0, how: 'none', from: '', restoredAt: 0, note: '' };
// Nothing may open, close or change a trade while this build is starting or after it handed its account over.
const tradingPaused = () => !Handover.ready || Handover.frozen;

// Paper account size. On the first boot with a new CAPITAL_RESET_ID the saved account is
// re-based so total value (cash + open positions) equals STARTING_EQUITY_USDT.
const STARTING_EQUITY_USDT = Number(process.env.STARTING_EQUITY_USDT) || 110;
const CAPITAL_RESET_ID = process.env.CAPITAL_RESET_ID || 'equity-110usd-2026-11-09';

// In-memory trade dispatch deduplication
const recentTradeDispatches = new Set();

// -------------------------------------------------------------
// CORE SYSTEM STATE
// -------------------------------------------------------------
const MANUAL_START_USDT = Number(process.env.MANUAL_START_USDT) || Strategy.MANUAL.startUSDT;
// Autopilot entries are candle patterns (the Entry signals switch was removed in v6.5). A high vol breakdown signal is
// shorted (SHORT, the normal process) or, as a paper test, bought and closed only at +0.30% (LONG): Settings > High vol breakdown.
const BREAKDOWN_SIDES = { SHORT: 'Short (normal exits)', LONG: 'Long, closes only at +0.30% (test)' };
// Each new autopilot trade is positionPct of account value (1% to 100%, default 20%): Settings > Position size.
const sizeConfig = (leverage = 1) => ({ ...Strategy.FUTURES, positionPct: State.positionPct, leverage });
const State = {
  autopilotEnabled: true,
  positionPct: Strategy.validPositionPct(process.env.POSITION_PCT) || Strategy.FUTURES.positionPct,
  // Settings > Leverage (v6.8, paper test): off = every trade 1x; on = the autopilot picks 1x to 3x per coin.
  leverageAuto: ['on', '1', 'true'].includes(String(process.env.LEVERAGE_AUTO || '').toLowerCase()),
  // Settings > Both sides (v6.9): on = longs and shorts in every market mode; off = BULL longs only, BEAR shorts only.
  bothSides: !['off', '0', 'false'].includes(String(process.env.BOTH_SIDES || '').toLowerCase()),
  // Settings > Trade direction (v6.30): SHORT = shorts only (default), BOTH = shorts and longs as above. SIDE_MODE=both starts it on BOTH.
  sideMode: Strategy.validSideMode(process.env.SIDE_MODE) || 'SHORT',
  // Settings > Momentum report (v6.33): on = a new short needs the hourly Gemini report to call the coin bearish (v6.34).
  // Starts on only when the server has a Gemini key, so an upload without the key doesn't stop new trades. MOMENTUM=off starts it off.
  momentumOn: !['off', '0', 'false'].includes(String(process.env.MOMENTUM || '').toLowerCase()) && Boolean(process.env.GEMINI_MOMENTUM_KEY || process.env.GEMINI_API_KEY),
  breakdownSide: BREAKDOWN_SIDES[process.env.BREAKDOWN_SIDE] ? process.env.BREAKDOWN_SIDE : 'SHORT',
  breakdownLongs: { opened: 0, target: 0, bagged: 0, bagClosed: 0, last: '' },
  // Manual paper futures: its own cash, positions and history. Never touched by the autopilot or the sentry.
  manual: { startUSDT: MANUAL_START_USDT, cashUSDT: MANUAL_START_USDT, realizedPnL: 0, positions: {}, history: [] },
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
  ledgerSync: { okAt: 0, triedAt: 0, rows: 0, note: 'not read yet' },   // v6.11: last read of the Google Sheet ledger
  lastTrendMode: null,   // v6.17: the last BULL / BEAR market mode (for "neutral after bullish: shorts only")
  // v6.23: Settings > Entry strategy: 'CANDLE' (the candlestick model) or 'FIB' (the Fibonacci retracement autopilot)
  entryStrategy: String(process.env.ENTRY_STRATEGY || '').toUpperCase() === 'FIB' ? 'FIB' : 'CANDLE',
  fibTimeframe: '1h',   // v6.24: the Fibonacci strategy (v2 + confluence) runs on 1h candles only
  // the Fibonacci engine's own state: swing memory, its positions / cooldowns / bar counters, the last bar it ran on
  fib: { memory: { pivots: {}, setups: {} }, portfolio: { positions: {}, cooldown_until_bar: {}, bar_counters: {} }, lastBarT: 0, warm: false, timeframe: null, engine: null },
  fibUniverse: [],   // the radar's coins, in its order (biggest futures volume first)
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
    screener: { status: 'ACTIVE', scannedPairsCount: 0, qualifiedCandidatesCount: 0, cadenceSec: 60, liquidityFloorUSD: '$10M+ futures volume', source: 'USD-M futures' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 240, activeBookDepth: 20 },
    marketMode: { mode: 'NEUTRAL', side: 'NONE', reason: 'Reading the market', breadth: null, seen: 0, updatedAt: 0 },
    futures: { status: 'CONNECTING', perpetuals: 0, reason: 'Loading USD-M contracts', leverage: 1 },
    leverage: { on: false, maxLeverage: Strategy.LEVERAGE.maxLeverage, atrTargetPct: Strategy.LEVERAGE.atrTargetPct, mmrPct: Strategy.LEVERAGE.mmrPct },
    sizing: { positionPct: Strategy.FUTURES.positionPct, minPct: Strategy.POSITION_SIZE.minPct, maxPct: Strategy.POSITION_SIZE.maxPct },
    breakdown: { side: 'SHORT', label: BREAKDOWN_SIDES.SHORT, longs: { opened: 0, target: 0, bagged: 0, bagClosed: 0, last: '' } },
    riskGuard: { status: 'ARMED', reason: 'Awaiting first evaluation', equityUSDT: 0, riskPerTradePct: 0.5, dailyLossLimitPct: 2.0 },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 65, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Min exit +0.30% long, +0.20% short • Candlestick harvest • From +0.50%, trail 0.20% behind the peak • Failing trades move to recovery bags' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Bags close at entry (0.00%), with no time limit', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
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
        noteBinanceWeight(url, res);
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
    if (klineCache.size > 2500) klineCache.delete(klineCache.keys().next().value);   // ~4 a coin, 400 coins max
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
// Which side(s) new trades may take: shorts only when Settings > Trade direction is Shorts only (v6.30, the default);
// otherwise the market mode's side, or both when Settings > Both sides is on (v6.9).
// A trade against the market mode's side needs the coin's own 1h trend to agree, as every trade in NEUTRAL does.
function tradeSide(mode = State.subsystems.marketMode) {
  return Strategy.tradeSideFor(mode, { sideMode: State.sideMode, bothSides: State.bothSides });
}
function sideText(mode) {
  const ts = tradeSide(mode);
  if (ts === 'NONE') return 'No new trades.';
  if (State.sideMode === 'SHORT') return 'Shorts only (Settings): autopilot opens only futures shorts; outside a Bearish market each needs the coin\'s own 1h downtrend. No new longs.';
  if (mode.shortsOnly) return 'Autopilot opens only futures shorts, where a coin\'s own 1h trend is down. No new longs.';
  if (ts === 'LONG') return 'Autopilot opens futures longs.';
  if (ts === 'SHORT') return 'Autopilot opens futures shorts.';
  if (mode.side === 'BOTH') return 'Autopilot follows each coin\'s own 1h trend.';
  return mode.side === 'LONG' ? 'Autopilot opens futures longs, and shorts where a coin\'s own 1h trend is down.'
    : 'Autopilot opens futures shorts, and longs where a coin\'s own 1h trend is up.';
}

async function refreshMarketMode(tickers) {
  const top = tickers
    .filter((t) => t.symbol.endsWith('USDT') && t.symbol !== 'BTCUSDT' && !/^(USDC|FDUSD|TUSD|EUR|USD1|RLUSD|PAXG)/.test(t.symbol))
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume)).slice(0, 30).map((t) => t.symbol);
  const [btc, ...coins] = await Promise.all(['BTCUSDT', ...top].map((s) => getKlinesStrict(s, '1h', 200)));
  const btcC = btc ? Strategy.closedOnly(btc) : null, coinsC = coins.filter(Boolean).map((k) => Strategy.closedOnly(k));
  let r = Strategy.evaluateMarketMode(btcC, coinsC);
  // v6.17: remember the last BULL / BEAR. After a start it is read once from the 1h history (up to about 5 days back).
  if (r.mode === 'BULL' || r.mode === 'BEAR') State.lastTrendMode = r.mode;
  else if (!State.lastTrendMode && btcC) State.lastTrendMode = Strategy.lastTrendMode(btcC, coinsC);
  const shortsOnly = Strategy.neutralShortsOnly(r, State.lastTrendMode);
  if (shortsOnly) r = { ...r, reason: `${r.reason}; came after a bullish market and BTC is below its 1h EMA50, so shorts only` };
  r = { ...r, shortsOnly, lastTrend: State.lastTrendMode };
  const before = State.subsystems.marketMode.mode, beforeShorts = Boolean(State.subsystems.marketMode.shortsOnly);
  State.subsystems.marketMode = { ...r, tradeSide: tradeSide(r), breadth: r.breadth === null || r.breadth === undefined ? null : Number(r.breadth.toFixed(2)), updatedAt: Date.now() };
  if (before !== r.mode || beforeShorts !== shortsOnly) {
    logOperation('MARKET_MODE', `Market is now ${r.mode}: ${r.reason}. ${sideText(r)} Open positions keep their exits.`, 'warning');
  }
  return r;
}

// -------------------------------------------------------------
// USD-M FUTURES DATA (paper only: nothing here places an order)
// -------------------------------------------------------------
const FAPI_BASE = process.env.FAPI_BASE || 'https://fapi.binance.com';
// Binance moved aggTrade to the /market path in 2026; the old /ws path no longer carries it.
const FSTREAM_URL = process.env.FSTREAM_URL || 'wss://fstream.binance.com/market/ws';
// The radar holds the biggest futures contracts by 24h volume with $10M+ (v6.7). 150 by default, 20 to 400.
const SCREENER_MAX_COINS = Math.min(400, Math.max(20, Math.round(Number(process.env.SCREENER_MAX_COINS) || Strategy.SCREENER.maxCoins)));
// Binance USD-M allows 2400 request weight a minute per IP, and Render's IP may be shared. At 1800 the search for
// new trades pauses until the minute ends, so prices, exits and bags always have room. A 429 / 418 reply
// pauses it for as long as Binance asks (Retry-After), 60s if it does not say.
const FAPI_WEIGHT = { used: 0, minute: 0, pauseUntil: 0, pauseAt: 1800, limit: 2400 };
function noteBinanceWeight(url, res) {
  if (!String(url).startsWith(FAPI_BASE) || !res) return;
  const w = Number(res.headers && res.headers['x-mbx-used-weight-1m']);
  if (Number.isFinite(w) && w >= 0) { FAPI_WEIGHT.used = w; FAPI_WEIGHT.minute = Math.floor(Date.now() / 60000); }
  if (res.statusCode === 429 || res.statusCode === 418) {
    const sec = Number(res.headers && res.headers['retry-after']);
    FAPI_WEIGHT.pauseUntil = Math.max(FAPI_WEIGHT.pauseUntil, Date.now() + (Number.isFinite(sec) && sec > 0 ? sec * 1000 : 60000));
  }
}
function fapiBusy() {
  const now = Date.now();
  if (now < FAPI_WEIGHT.pauseUntil) return `Binance asked the engine to slow down, ${Math.ceil((FAPI_WEIGHT.pauseUntil - now) / 1000)}s left`;
  if (FAPI_WEIGHT.minute === Math.floor(now / 60000) && FAPI_WEIGHT.used >= FAPI_WEIGHT.pauseAt) return `Binance request weight ${FAPI_WEIGHT.used} of ${FAPI_WEIGHT.limit} this minute`;
  return null;
}
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
    const pf = (s.filters || []).find((x) => x.filterType === 'PRICE_FILTER');
    const ls = (s.filters || []).find((x) => x.filterType === 'MARKET_LOT_SIZE') || (s.filters || []).find((x) => x.filterType === 'LOT_SIZE');
    next.set(s.symbol, { minNotional: parseFloat(mn && mn.notional) || 5, listedAt: Number(s.onboardDate) || 0,
      tickSize: parseFloat(pf && pf.tickSize) || 0, stepSize: parseFloat(ls && ls.stepSize) || 0 });   // v6.23: for the Fibonacci orders
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
  if (fundingBusy || tradingPaused()) return;
  fundingBusy = true;
  try {
    const held = [...Object.entries(State.activePositions || {}), ...Object.entries(State.recoveryBags || {})].filter(([, p]) => isFutures(p))
      .concat(Object.values(State.manual.positions || {}).map((p) => [p.symbol, p]));
    for (const [k, p] of held) {
      if (Date.now() < Number(p.nextFundingAt || 0)) continue;
      let d = null;
      try { d = await safeFetch(`${FAPI_BASE}/fapi/v1/premiumIndex?symbol=${k}`, 2500); } catch (e) { d = null; }
      if (!d || !(parseFloat(d.markPrice) > 0)) continue;
      if (p.nextFundingAt) {
        const amount = (isShort(p) ? 1 : -1) * Number(p.qty) * parseFloat(d.markPrice) * (parseFloat(d.lastFundingRate) || 0);
        p.fundingUSDT = Number(((Number(p.fundingUSDT) || 0) + amount).toFixed(6));
        logOperation('FUNDING', `${p.pair}${p.manual ? ' (manual)' : ''}: funding ${amount >= 0 ? 'received' : 'paid'} $${Math.abs(amount).toFixed(4)}.`, 'info');
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
    if (!isFutures(p)) { eq += qty * px; return; }
    const entry = Number(p.entryPriceUSDT), margin = Number(p.marginUSDT) > 0 ? Number(p.marginUSDT) : qty * entry;
    eq += Math.max(0, margin + (isShort(p) ? qty * (entry - px) : qty * (px - entry)) + (Number(p.fundingUSDT) || 0));   // v6.8: margin-based
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

    const klines = await getCachedKlines(symbol, '1h', 24, 3000, 0, true);
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
    const ts = tradeSide(mode);

    // v6.7: the radar is built from USD-M futures (every trading USDT perpetual with $10M+ of 24h futures volume,
    // the biggest SCREENER_MAX_COINS). Market mode above still reads the spot list, exactly as every back test did.
    const futTickers = await safeFetch(`${FAPI_BASE}/fapi/v1/ticker/24hr`, 8000).catch(() => null);
    if (!Array.isArray(futTickers) || !perpetuals.size) {
      throttledNotice('futtickers', 'FUTURES', 'Binance Futures tickers are unavailable. The radar keeps its last list.', 'warning', 300000);
      return;
    }
    const eligible = Strategy.futuresUniverse(futTickers, perpetuals, { ...Strategy.SCREENER, maxCoins: Infinity });
    const picked = eligible.slice(0, SCREENER_MAX_COINS);
    State.fibUniverse = picked.map((t) => t.symbol);   // v6.23: the Fibonacci screener's coins (it applies its own $10M gate too)
    const universe = {};
    const candidates = [];

    for (const t of picked) {
      const sym = t.symbol;
      const last = parseFloat(t.lastPrice);
      const volQuote = parseFloat(t.quoteVolume);

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
      // BOTH: lower half of the day's range ranks as a short, upper half as a long (the entry rules need exactly that anyway).
      const side = ts === 'SHORT' || (ts === 'BOTH' && posInRange < 0.5) ? 'SHORT' : 'LONG';
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

      if (!isPumpDump && score >= 60 && canTrade) candidates.push(sym);
    }
    // Candle checks for the 35 best-scoring candidates; skipped while Binance request weight is high.
    const candidateKlinesQueue = fapiBusy() ? [] : candidates.sort((a, b) => universe[b].score - universe[a].score).slice(0, 35);

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
          const closeMs = closed15m.length ? Number(closed15m[closed15m.length - 1][6]) + 1 : 0;
          if (sp.sell && !Strategy.patternEntryOpen(closeMs, sp.signalAge)) {
            universe[topSym].candlestickPattern = `${sp.pattern} (entry window passed)`;
          } else if (sp.bullish) {
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

          const closeMsL = closed15m.length ? Number(closed15m[closed15m.length - 1][6]) + 1 : 0;
          if (pat.buy && !pat.bearish && !Strategy.patternEntryOpen(closeMsL, pat.signalAge)) {
            universe[topSym].candlestickPattern = `${pat.pattern} (entry window passed)`;
          } else if (pat.bearish) {
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
    Object.assign(State.subsystems.screener, { source: 'USD-M futures', contractsListed: perpetuals.size, eligibleCount: eligible.length, maxCoins: SCREENER_MAX_COINS,
      liquidityFloorUSD: '$10M+ futures volume', binanceWeight1m: FAPI_WEIGHT.minute === Math.floor(Date.now() / 60000) ? FAPI_WEIGHT.used : 0 });
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= (State.activeThresholds?.scoreThreshold || 65) && !c.isPumpAndDump).length;

    logOperation('RADAR_SCREENER', `[${mode.mode}] Scanned ${Object.keys(universe).length} futures contracts (the biggest of ${eligible.length} with $10M+ volume, ${perpetuals.size} listed) for ${ts === 'SHORT' ? 'futures shorts' : ts === 'LONG' ? 'futures longs' : 'futures longs and shorts'}. ${State.subsystems.screener.qualifiedCandidatesCount} candidates qualified (Score >= ${State.activeThresholds?.scoreThreshold || 65}). Candlestick Sentinel active.`, 'info');
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

function openLong(pairKey, signal, plan, takerBuyRatio, opts = {}) {
  if (tradingPaused()) return false;
  const sym = pairKey.replace(/USDT$/, '');
  const entry = plan.entryPrice;
  const R = Strategy.EXIT_RULES;
  const lev = Math.max(1, Number(plan.leverage) || 1), margin = Number(plan.margin) > 0 ? Number(plan.margin) : plan.notional;
  const cost = margin + plan.notional * FUT_FEE_RATE; // margin plus the opening fee on the whole position (1x: margin = position)
  if (State.portfolio.cashUSDT < cost) {
    logOperation('CASH_GUARD', `Cannot long ${sym}/USDT: need $${cost.toFixed(2)}, free cash $${State.portfolio.cashUSDT.toFixed(2)}.`, 'danger');
    return false;
  }
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - cost).toFixed(6));
  const bagAt = entry * (1 + R.demotePct / 100);
  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`, pairSymbol: sym, side: 'LONG', market: 'FUTURES', leverage: lev, leverageAtEntry: lev, marginUSDT: margin, strategyVersion: 6,
    openedAt: Date.now(), entryPriceUSDT: entry, livePriceUSDT: entry, lastTickAt: Date.now(),
    qty: plan.qty, notionalUSDT: plan.notional, initialNotionalUSDT: plan.notional,
    initialStopPrice: bagAt, sl: bagAt, stopPct: Number(plan.stopPct.toFixed(3)), riskUSDT: plan.riskUSDT, atrPct: Number(signal.atrPct.toFixed(3)),
    breakevenPrice: entry * (1 + R.shortFloorPct / 100), tp1Price: entry * (1 + R.tp1Pct / 100), targetPrice: entry * 1.03,
    liquidationPrice: Strategy.liquidationPrice(entry, 'LONG', lev), fundingUSDT: 0, nextFundingAt: 0,
    peakGainPct: 0, peakHighPrice: entry, ratchetStage: 'INITIAL', tp1Secured: false, candlestickPattern: signal.pattern
  };
  // Breakdown long (test): closes only at its fixed target, never by harvest, trail, scratch or partial.
  const target = opts.targetOnlyPct ? entry * (1 + opts.targetOnlyPct / 100) : 0;
  if (target) Object.assign(State.activePositions[pairKey], { targetOnlyPct: opts.targetOnlyPct, entrySource: opts.entrySource, ratchetStage: 'TARGET_ONLY', breakevenPrice: target, tp1Price: target, targetPrice: target });
  logOperation('EXECUTION', `LONGED ${sym}/USDT @ $${formatPrecisePrice(entry)} ($${plan.notional.toFixed(2)}${lev > 1 ? ` on $${margin.toFixed(2)} margin` : ''}, ${lev}x USD-M, paper${lev > 1 ? `, liquidation $${formatPrecisePrice(State.activePositions[pairKey].liquidationPrice)}` : ''}) via [${signal.pattern}] • ${target ? `Closes only at +${opts.targetOnlyPct.toFixed(2)}% ($${formatPrecisePrice(target)})` : 'Min exit +0.20%'} • Moves to recovery bag at -3.50% • Taker buy ${takerBuyRatio}%`, 'success');
  saveDiskState();
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
  return true;
}

function openShort(pairKey, signal, plan, takerBuyRatio) {
  if (tradingPaused()) return false;
  const sym = pairKey.replace(/USDT$/, '');
  const entry = plan.entryPrice;
  const R = Strategy.EXIT_RULES;
  const lev = Math.max(1, Number(plan.leverage) || 1), margin = Number(plan.margin) > 0 ? Number(plan.margin) : plan.notional;
  const cost = margin + plan.notional * FUT_FEE_RATE; // margin plus the opening fee on the whole position (1x: margin = position)
  if (State.portfolio.cashUSDT < cost) {
    logOperation('CASH_GUARD', `Cannot short ${sym}/USDT: need $${cost.toFixed(2)}, free cash $${State.portfolio.cashUSDT.toFixed(2)}.`, 'danger');
    return false;
  }
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - cost).toFixed(6));
  const bagAt = entry * (1 - R.demotePct / 100);
  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`, pairSymbol: sym, side: 'SHORT', market: 'FUTURES', leverage: lev, leverageAtEntry: lev, marginUSDT: margin, strategyVersion: 6,
    openedAt: Date.now(), entryPriceUSDT: entry, livePriceUSDT: entry, lastTickAt: Date.now(),
    qty: plan.qty, notionalUSDT: plan.notional, initialNotionalUSDT: plan.notional,
    initialStopPrice: bagAt, sl: bagAt, stopPct: Number(plan.stopPct.toFixed(3)), riskUSDT: plan.riskUSDT, atrPct: Number(signal.atrPct.toFixed(3)),
    breakevenPrice: entry * (1 - R.shortFloorPct / 100), tp1Price: entry * (1 - R.tp1Pct / 100), targetPrice: entry * 0.97,
    liquidationPrice: Strategy.liquidationPrice(entry, 'SHORT', lev), fundingUSDT: 0, nextFundingAt: 0,
    peakGainPct: 0, peakLowPrice: entry, ratchetStage: 'INITIAL', tp1Secured: false, candlestickPattern: signal.pattern
  };
  logOperation('EXECUTION', `SHORTED ${sym}/USDT @ $${formatPrecisePrice(entry)} ($${plan.notional.toFixed(2)}${lev > 1 ? ` on $${margin.toFixed(2)} margin` : ''}, ${lev}x USD-M, paper${lev > 1 ? `, liquidation $${formatPrecisePrice(State.activePositions[pairKey].liquidationPrice)}` : ''}) via [${signal.pattern}] • Min exit +0.20% • Moves to recovery bag at -3.50% • Taker buy ${takerBuyRatio}%`, 'success');
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
  if (!pos || pos.exitInProgress || tradingPaused()) return null;

  const entry = Number(pos.entryPriceUSDT);
  // Hard rule: only the user's own manual close can realize a loss. And a recovery bag's own close at entry (BAG BREAKEVEN,
  // fees only; v6.22 has no BAG CUT stage, the label stays allowed for older builds' bags) (Strategy.bagStage); nothing else.
  const short = isShort(pos);
  const bagCut = bookName === 'recoveryBags' && /^BAG (CUT|BREAKEVEN) /.test(typeLabel);
  const fibExit = bookName === 'activePositions' && Boolean(pos.fib) && /^FIB /.test(typeLabel);   // v6.23: its own stop-loss
  if (!/MANUAL|LIQUIDATED/.test(typeLabel) && !bagCut && !fibExit && (short ? price > entry : price < entry)) {
    logOperation('SENTRY', `${pos.pair}: blocked ${typeLabel} below entry. Position stays open.`, 'warning');
    return null;
  }
  const totalQty = Number(pos.qty) || (Number(pos.notionalUSDT) / entry);
  const qtySold = fraction >= 1 ? totalQty : totalQty * fraction;
  const fut = isFutures(pos);
  const fundingShare = fut ? (Number(pos.fundingUSDT) || 0) * (fraction >= 1 ? 1 : fraction) : 0;
  // v6.8: a futures position holds its own margin (1x: the whole position, so these are the old formulas). Closing
  // returns the margin plus the move, less the exit fee; a liquidation loses all of the margin (isolated), as on Binance.
  const part = fraction >= 1 ? 1 : fraction;
  const marginTotal = fut ? (Number(pos.marginUSDT) > 0 ? Number(pos.marginUSDT) : totalQty * entry) : 0;
  const marginPart = marginTotal * part;
  const liquidated = fut && typeLabel === 'LIQUIDATED';
  const move = short ? qtySold * (entry - price) : qtySold * (price - entry);
  const proceeds = liquidated ? 0
    : fut ? marginPart + move - qtySold * price * FUT_FEE_RATE + fundingShare : qtySold * price * (1 - FEE_RATE);
  const pnl = liquidated ? -(marginPart + qtySold * entry * FUT_FEE_RATE)
    : short ? Strategy.netPnlShort(qtySold, entry, price) + fundingShare
    : fut ? Strategy.netPnlLongFutures(qtySold, entry, price) + fundingShare : Strategy.netPnl(qtySold, entry, price);
  if (fut && fraction < 1) pos.fundingUSDT = Number(((Number(pos.fundingUSDT) || 0) - fundingShare).toFixed(6));
  if (fut && fraction < 1 && Number(pos.marginUSDT) > 0) pos.marginUSDT = Number((marginTotal - marginPart).toFixed(6));
  const levAtEntry = fut ? Number(pos.leverageAtEntry || pos.leverage) || 1 : 1;

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
    leverage: levAtEntry,
    ...(pos.momentum ? { momentumScore: pos.momentum.s, momentumDir: pos.momentum.d } : {}),   // v6.33: the score it was opened with
    reason: `${reasonText}${levAtEntry > 1 ? ` • ${levAtEntry}x leverage` : ''}${pos.momentum ? ` • momentum ${pos.momentum.d} ${pos.momentum.s}` : ''}`
  };

  logOperation(pnl >= 0 ? 'SENTRY_EXIT' : 'RISK_STOP',
    `${tradePayload.type} on ${tradePayload.pair} @ $${formatPrecisePrice(price)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%) • ` +
    `Net ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)} after fees${fullyClosed ? '' : ' • runner continues'}`,
    pnl >= 0 ? 'success' : 'warning');

  // v6.11: record first (the list update is synchronous), so this one frame carries the cash, the closed position and the
  // trade, and Realized P&L / Take-home move together with the account value. The sheet write continues in the background.
  recordTradeToCloudLedger(tradePayload, { announce: false }).catch((e) => console.error('[Ledger Dispatch Notice]', e.message));
  saveDiskState();
  broadcastStateUpdate();
  return tradePayload;
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (shared state)
// -------------------------------------------------------------
let isEvaluatingEntries = false;
const noticeTimes = {};
const candidateCooldownMemo = new Map();
// v6.10: a pattern trade opens only within Strategy.PATTERN_ENTRY.windowMin minutes of its signal candle's close.
// A late pattern counts as no pattern (a breakdown / ignition can still trade); the feed says so once per coin per candle.
const lateNoted = new Set();
function lateCheck(pairKey, side, closeMs, barsAgo, name) {
  if (Strategy.patternEntryOpen(closeMs, barsAgo)) return '';
  const msg = `${name} signal is ${Math.round(Strategy.patternSignalAgeMin(closeMs, barsAgo))} min old. Pattern trades open only in the first ${Strategy.PATTERN_ENTRY.windowMin} min after the signal candle closes; later the price has usually moved.`;
  const key = `${pairKey}|${side}|${closeMs}`;
  if (!lateNoted.has(key)) { if (lateNoted.size > 2000) lateNoted.clear(); lateNoted.add(key); logOperation('CANDLE_LATE', `${pairKey} ${side === 'SHORT' ? 'short' : 'long'}: ${msg}`, 'info'); }
  return msg;
}

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
      getCachedKlines(pairSym, '1h', 200, 3500, 0, futures)
    ]);
    const closed15 = Strategy.closedOnly(k15);
    if (!Array.isArray(closed15) || closed15.length < 30) return none;
    const a = Strategy.assessCandles(closed15, { major: Strategy.majorTrendOf(Strategy.closedOnly(k1h)) });
    const bull = a.pattern !== 'NONE' ? `${a.pattern} (15m)` : 'NONE';
    const bear = a.bearPattern !== 'NONE' ? `${a.bearPattern} (15m)` : 'NONE';
    const patternName = bear !== 'NONE' ? bear : bull;
    if (patternName !== 'NONE') State.subsystems.candlestickSentry.lastPatternDetected = `${pairSym}: ${patternName}`;
    const sh = Strategy.assessShortCandles(closed15, { major: a.major });
    return { lastCloseMs: Number(closed15[closed15.length - 1][6]) + 1, shortSignalBarsAgo: sh.signalAge || 0, bullSignalBarsAgo: a.signalAge || 0,
      isShortEntry: sh.sell, shortPatternName: sh.pattern !== 'NONE' ? `${sh.pattern} (15m)` : 'NONE', isBullishSignal: sh.bullish, bullSignalName: sh.bullPattern !== 'NONE' ? `${sh.bullPattern} (15m)` : 'NONE',
      isBearishReversal: a.bearish, isBullishEntry: a.buy, bullishPatternName: bull, bearishPatternName: bear, patternName,
      confidence: a.score, patternRvol: a.rvol, convergence: a.convergence, major: a.major, vetoes: a.vetoes };
  } catch (e) {
    return none;
  }
}

async function checkVeteranOrderFlowAndVSA(symbol, maxAgeMs = 20000) {
  try {
    const klines = await getCachedKlines(symbol, '5m', 8, 2500, maxAgeMs, true);
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

async function checkMicrostructureMomentumAndVwap(symbol, maxAgeMs = 45000) {
  try {
    const klines = await getCachedKlines(symbol, '15m', 16, 2500, maxAgeMs, true);
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
    const klines = await getCachedKlines(pairSym, '1h', 200, 3500, 120000, true);

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
async function checkShortOrderFlow(symbol, maxAgeMs = 20000) {
  const k = await getCachedKlines(symbol, '5m', 8, 2500, maxAgeMs, true).catch(() => null);
  if (!Array.isArray(k) || k.length < 3) return { passed: false, reason: '5m data unavailable' };
  let vol = 0, buy = 0;
  k.slice(-3).forEach((b) => { vol += parseFloat(b[5]); buy += parseFloat(b[9]); });
  const takerBuyRatio = vol > 0 ? Number(((buy / vol) * 100).toFixed(1)) : 50;
  const [h, l, c] = [k[k.length - 1][2], k[k.length - 1][3], k[k.length - 1][4]].map(parseFloat);
  const clv = h - l > 0 ? (c - l) / (h - l) : 0.5;
  if (takerBuyRatio > 54 && clv > 0.65) return { passed: false, takerBuyRatio, reason: `Buyer absorption: taker buy ${takerBuyRatio}% (>54%) and CLV ${(clv * 100).toFixed(0)}%` };
  return { passed: true, takerBuyRatio };
}

async function checkShortMomentum(symbol, maxAgeMs = 45000) {
  const k = await getCachedKlines(symbol, '15m', 16, 2500, maxAgeMs, true).catch(() => null);
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
  const k = await getCachedKlines(symbol, '1h', 200, 3500, 120000, true).catch(() => null);
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
  const k = await getCachedKlines(pairKey, '1h', 200, 3500, 120000, true).catch(() => null);
  if (!Array.isArray(k)) return false;
  return Strategy.coinTrend(Strategy.closedOnly(k).map((b) => parseFloat(b[4]))) === side;
}

// Settings > Leverage (v6.8): 1x when off; when on, from the coin's 15m ATR on closed candles, as back-tested.
async function leverageFor(pairKey) {
  if (!State.leverageAuto) return 1;
  const mo = momentumEntry(pairKey);   // v6.33: with the momentum report on, its score sets the leverage, not volatility
  if (mo) return Momentum.leverageFor(mo.s, Strategy.LEVERAGE.maxLeverage);
  const k = await getCachedKlines(pairKey, '15m', 16, 2500, 45000, true).catch(() => null);
  const closed = Array.isArray(k) ? Strategy.closedOnly(k) : [];
  const atr = closed.length >= 15 ? Strategy.atrPct(Strategy.toBars(closed), 14) : NaN;
  return Strategy.autoLeverage({ atrPct: atr });
}

// ---------- v6.33: the hourly momentum report
const momentumLogged = new Set();
function momentumFresh() { const r = MomentumReport.report(); return Boolean(r && Date.now() - r.at <= Momentum.RULES.maxAgeMs); }
// this coin's reading, when the report is on and fresh
function momentumEntry(pairKey) {
  if (!State.momentumOn || !momentumFresh()) return null;
  const r = MomentumReport.report();
  return (r.coins || {})[String(pairKey).replace(/USDT$/, '')] || null;
}
// may the autopilot open this side here? Only the direction counts (v6.34). Logs a skipped coin once per report.
function momentumCheck(pairKey, side) {
  if (!State.momentumOn) return { ok: true };
  const r = MomentumReport.report(), g = Momentum.gate(r, pairKey, side);
  if (!g.ok && r && r.at) {
    const k = `${r.at}|${pairKey}|${side}`;
    if (!momentumLogged.has(k)) {
      momentumLogged.add(k); if (momentumLogged.size > 600) momentumLogged.delete(momentumLogged.values().next().value);
      logOperation('MOMENTUM', `${pairKey} ${side.toLowerCase()} skipped: ${g.reason} (report ${new Date(r.at).toISOString().slice(11, 16)} UTC).`, 'info');
    }
  }
  return g;
}
// the reading goes with the trade: onto the position, into its bag and into the closed trade
function tagMomentum(pairKey, g) {
  const pos = State.activePositions[pairKey], r = MomentumReport.report();
  if (!pos || !g || !g.entry) return;
  pos.momentum = { d: g.entry.d, s: g.entry.s, w: g.entry.w, at: r ? r.at : null };
}
// the Binance numbers Gemini reads: every coin on the radar the engine could trade (v6.34), and BTC for context
async function momentumRows() {
  const keys = Object.keys(State.universe || {}).filter((k) => { const c = State.universe[k]; return c && c.canTrade && !c.isPumpAndDump && c.symbol !== 'BTC'; })
    .sort((a, b) => (State.universe[b].score || 0) - (State.universe[a].score || 0)).slice(0, Momentum.RULES.coins);
  if (perpetuals.has('BTCUSDT')) keys.unshift('BTCUSDT');
  const rows = [];
  for (let i = 0; i < keys.length; i += 4) {
    for (let w = 0; w < 12 && fapiBusy(); w++) await new Promise((res) => setTimeout(res, 5000));
    const part = await Promise.all(keys.slice(i, i + 4).map(async (k) => {
      const [kl, oi] = await Promise.all([
        getCachedKlines(k, '1h', 60, 6000, 0, true).catch(() => null),
        safeFetch(`${FAPI_BASE}/futures/data/openInterestHist?symbol=${k}&period=1h&limit=25`, 6000).catch(() => null)
      ]);
      return Momentum.features(k.replace(/USDT$/, ''), kl, Array.isArray(oi) ? oi : [], currentFundingRate(k));
    }));
    rows.push(...part.filter(Boolean));
  }
  const mm = State.subsystems.marketMode || {};
  const market = mm.mode ? { mode: mm.mode, reason: String(mm.reason || '').slice(0, 160), coinsAboveEma50Pct: mm.breadth == null ? null : Math.round(mm.breadth * 100),
    btcVsEma50Pct: Number(mm.ema50) > 0 ? Number(((mm.btcClose / mm.ema50 - 1) * 100).toFixed(2)) : null } : null;
  return { rows, market };
}
function syncMomentumStatus() { State.subsystems.momentum = { ...MomentumReport.status(), enabled: State.momentumOn }; }

async function evaluateShortEntries(maxBagsLimit) {
  if (!perpetuals.size) {
    throttledNotice('noperps', 'FUTURES', 'Binance Futures data is unavailable. No shorts opened.', 'danger', 60000);
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
      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;
      if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > now) continue;
      if ((candidateCooldownMemo.get(pairKey) || 0) > now) continue;
      if (++evaluated > 16) break;

      const change24h = Number(coin.change24h || 0), rangePct = Number(coin.intradayRangePct || 0);
      if (change24h > -0.20 && rangePct < 3.0) continue;
      if (Number(coin.pricePositionInRange ?? 0.3) > 0.50) continue;
      const mg = momentumCheck(pairKey, 'SHORT');   // v6.33/v6.34: the hourly momentum report must call it bearish (any score)
      if (!mg.ok) { candidateCooldownMemo.set(pairKey, now + 300000); continue; }
      if (State.subsystems.marketMode.side !== 'SHORT' && !(await coinTrendAgrees(pairKey, 'SHORT'))) { candidateCooldownMemo.set(pairKey, now + 60000); continue; }

      let flow = await checkShortOrderFlow(pairKey);
      if (!flow.passed) { veto(pairKey, 30000, 'FLOW_VETO', flow.reason); continue; }
      if (Number(coin.priceUSDT || 0) >= 0.50 && rangePct < 4.0 && change24h > -0.60) { veto(pairKey, 30000, 'VOL_VETO', `low volatility (range ${rangePct.toFixed(2)}%).`, 'info'); continue; }
      let mom = await checkShortMomentum(pairKey);
      if (!mom.passed) { veto(pairKey, 30000, 'MOM_VETO', mom.reason); continue; }
      const trend = await checkShort1hTrend(pairKey);
      if (!trend.passed) { veto(pairKey, 30000, 'TREND_VETO', trend.reason); continue; }
      let candle = await checkCandlestickSentry(pairKey, true);
      if (candle.isBullishSignal) { veto(pairKey, 20000, 'CANDLE_VETO', `bullish pattern [${candle.bullSignalName}].`); continue; }
      const late = candle.isShortEntry ? lateCheck(pairKey, 'SHORT', candle.lastCloseMs, candle.shortSignalBarsAgo, candle.shortPatternName) : '';
      if (late) candle = { ...candle, isShortEntry: false, shortPatternName: 'NONE' };
      const breakdown = mom.change15mPct <= -0.80 && mom.rvol >= 1.35 && flow.takerBuyRatio <= 48;
      if (late && !breakdown) { candidateCooldownMemo.set(pairKey, now + 30000); continue; }
      if (!candle.isShortEntry && !breakdown) { veto(pairKey, 30000, 'CANDLE_WAIT', `waiting for a completed bearish pattern on 15m (1h trend ${candle.major}) or a breakdown (15m <= -0.80%).`, 'info'); continue; }
      const label = candle.isShortEntry ? candle.shortPatternName : 'HIGH_VOL_BREAKDOWN';
      // v6.8: a breakdown is read from the live 15m candle, and the checks above may have used candles up to 45 s
      // (order flow 20 s) old. Before entering, read both fresh: the breakdown, and every momentum check, must still hold.
      if (!candle.isShortEntry) {
        const [fm, ff] = await Promise.all([checkShortMomentum(pairKey, 1000), checkShortOrderFlow(pairKey, 1000)]);
        if (!fm.passed || !ff.passed || !(fm.change15mPct <= -0.80 && fm.rvol >= 1.35 && ff.takerBuyRatio <= 48)) {
          const now15 = !fm.passed ? fm.reason : !ff.passed ? ff.reason : `15m ${fm.change15mPct}%, volume ${fm.rvol}x, taker buy ${ff.takerBuyRatio}%`;
          veto(pairKey, 30000, 'MOM_VETO', `breakdown gone on fresh data: ${now15} (cached: ${mom.change15mPct}%, ${mom.rvol}x, ${flow.takerBuyRatio}%).`, 'info');
          continue;
        }
        mom = fm; flow = ff;
      }

      const book = await fetchRealOrderBookDepth(pairKey, true);
      if (!book) { veto(pairKey, 30000, 'BOOK_VETO', 'futures order book unavailable.'); continue; }
      if (book.obi > 0.55 || book.spreadPct > 0.20) { veto(pairKey, 30000, 'BOOK_VETO', `bid share ${Math.round(book.obi * 100)}% (req <= 55%) or spread ${book.spreadPct.toFixed(2)}%.`); continue; }

      // Settings > High vol breakdown > Long (paper test): the same breakdown signal, after the same checks, is bought
      // at the ask and closes only at +0.30%. Candle-pattern shorts are never flipped.
      if (!candle.isShortEntry && State.breakdownSide === 'LONG' && State.sideMode !== 'SHORT') {   // v6.30: shorts only shorts every breakdown
        const ask = book.bestAsk, tPct = Strategy.BREAKDOWN_LONG.targetPct;
        const lsig = await buildEntrySignal(pairKey, ask, 'HIGH_VOL_BREAKDOWN LONG', mom);
        const lplan = Strategy.planCompoundEntry(lsig, ask, computeEquity(), Number(State.portfolio.cashUSDT), sizeConfig(await leverageFor(pairKey)));
        if (!lplan.ok) { candidateCooldownMemo.set(pairKey, now + 30000); logOperation('CASH_GUARD', `${pairKey} breakdown long: ${lplan.reason}`, 'warning'); continue; }
        const minNL = perpetuals.get(pairKey)?.minNotional || 5;
        if (lplan.notional < minNL) { candidateCooldownMemo.set(pairKey, now + 300000); logOperation('CASH_GUARD', `${pairKey} breakdown long: Binance minimum for this contract is $${minNL}, position would be $${lplan.notional.toFixed(2)}.`, 'info'); continue; }
        logOperation('BOOK_CLEAR', `${pairKey} high vol breakdown (15m ${mom.change15mPct}%, volume ${mom.rvol}x, taker buy ${flow.takerBuyRatio}%) bought instead of shorted (test): long at the ask $${formatPrecisePrice(ask)}, closes only at +${tPct.toFixed(2)}% ($${formatPrecisePrice(ask * (1 + tPct / 100))}). Entering...`, 'success');
        if (openLong(pairKey, lsig, lplan, flow.takerBuyRatio, { targetOnlyPct: tPct, entrySource: 'BREAKDOWN_LONG' })) {
          State.breakdownLongs.opened++;
          State.breakdownLongs.last = `${pairKey} long @ $${formatPrecisePrice(ask)}, ${new Date().toISOString().slice(11, 16)} UTC`;
          saveDiskState();
        }
        continue;
      }

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
      const plan = Strategy.planCompoundEntry(signal, px, computeEquity(), Number(State.portfolio.cashUSDT), sizeConfig(await leverageFor(pairKey)));
      if (!plan.ok) { veto(pairKey, 30000, 'CASH_GUARD', plan.reason); continue; }
      const minN = perpetuals.get(pairKey)?.minNotional || 5;
      if (plan.notional < minN) { veto(pairKey, 300000, 'CASH_GUARD', `Binance minimum for this contract is $${minN}, position would be $${plan.notional.toFixed(2)}.`, 'info'); continue; }
      logOperation('BOOK_CLEAR', `${pairKey} short confirmed [${label}]: bid share ${Math.round(book.obi * 100)}%, spread ${book.spreadPct.toFixed(2)}%, taker buy ${flow.takerBuyRatio}%. Entering...`, 'success');
      if (openShort(pairKey, signal, plan, flow.takerBuyRatio)) tagMomentum(pairKey, mg);
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
  if (!State.autopilotEnabled || isEvaluatingEntries || tradingPaused()) return;
  if (State.entryStrategy === 'FIB') return;   // v6.23: the Fibonacci autopilot opens the trades (fibTick)
  if (State.momentumOn && !momentumFresh()) {   // v6.33
    const st = MomentumReport.status();
    throttledNotice('momentum', 'MOMENTUM', st.on ? `No new trades: waiting for the hourly momentum report${st.lastError ? ` (last try: ${st.lastError})` : ''}. Open positions and bags are still managed.`
      : 'No new trades: the momentum report is on but the server has no Gemini key (GEMINI_MOMENTUM_KEY). Turn it off in Settings, or add the key on Render.', 'warning', 300000);
    return;
  }

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
  const side = tradeSide(mode);
  if (side === 'NONE') {
    throttledNotice('mode', 'MARKET_MODE', `No new trades: ${mode.reason}.`, 'info', 120000);
    return;
  }
  if (Number(State.portfolio.cashUSDT) < Strategy.CONFIG.minNotional * (1 + FUT_FEE_RATE)) {
    throttledNotice('cash', 'CASH_GUARD', 'Free cash is below the $5 minimum position. Waiting for open trades to close.', 'info', 60000);
    return;
  }
  const busy = fapiBusy();
  if (busy) {
    throttledNotice('weight', 'AUTOPILOT', `New-trade checks paused: ${busy}. Open positions and bags are still managed.`, 'warning', 60000);
    return;
  }
  if (side !== 'LONG') await evaluateShortEntries(maxBagsLimit);
  if (side === 'SHORT') return;

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
      const mgL = momentumCheck(pairKey, 'LONG');   // v6.33/v6.34: the hourly momentum report must call it bullish (any score)
      if (!mgL.ok) { candidateCooldownMemo.set(pairKey, now + 300000); continue; }
      if (State.subsystems.marketMode.side !== 'LONG' && !(await coinTrendAgrees(pairKey, 'LONG'))) { candidateCooldownMemo.set(pairKey, now + 60000); continue; }

      const volQuote = (coin.volumeM || 0) * 1000000;
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pdCheck.isTrap) {
          candidateCooldownMemo.set(pairKey, now + 300000);
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pdCheck.reason}`, 'danger');
          continue;
        }
      }

      let orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
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

      let microMom = await checkMicrostructureMomentumAndVwap(pairKey);
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

      let candleCheck = await checkCandlestickSentry(pairKey, true);
      if (candleCheck.isBearishReversal) {
        candidateCooldownMemo.set(pairKey, now + 20000);
        logOperation('CANDLE_VETO', `${pairKey} halted: Bearish pattern detected [${candleCheck.patternName}]. Rejecting distribution wick.`, 'warning');
        continue;
      }

      const lateLong = candleCheck.isBullishEntry ? lateCheck(pairKey, 'LONG', candleCheck.lastCloseMs, candleCheck.bullSignalBarsAgo, candleCheck.bullishPatternName) : '';
      if (lateLong) candleCheck = { ...candleCheck, isBullishEntry: false, bullishPatternName: 'NONE' };
      const isConfirmedPatternEntry = candleCheck.isBullishEntry;
      const isAuthenticIgnition = (microMom.change15mPct >= 0.80 && microMom.rvol >= 1.35 && orderFlow.takerBuyRatio >= 52.0);
      if (lateLong && !isAuthenticIgnition) { candidateCooldownMemo.set(pairKey, now + 30000); continue; }
      if (!isConfirmedPatternEntry && !isAuthenticIgnition) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('CANDLE_WAIT', `${pairKey} on standby: Awaiting a completed Nison pattern on 15m (1h trend ${candleCheck.major}) or Ignition (15m >= +0.80%).`, 'info');
        continue;
      }
      const patternLabel = isConfirmedPatternEntry ? candleCheck.bullishPatternName : 'HIGH_VOL_IGNITION';
      // v6.8: an ignition is read from the live 15m candle (cached up to 45 s, flow 20 s). Confirm it on fresh data first.
      if (!isConfirmedPatternEntry) {
        const [fm, ff] = await Promise.all([checkMicrostructureMomentumAndVwap(pairKey, 1000), checkVeteranOrderFlowAndVSA(pairKey, 1000)]);
        if (!fm.passed || !ff.passed || !(fm.change15mPct >= 0.80 && fm.rvol >= 1.35 && ff.takerBuyRatio >= 52.0)) {
          const now15 = !fm.passed ? fm.reason : !ff.passed ? ff.reason : `15m +${fm.change15mPct}%, volume ${fm.rvol}x, taker buy ${ff.takerBuyRatio}%`;
          candidateCooldownMemo.set(pairKey, now + 30000);
          logOperation('MOM_VETO', `${pairKey} ignition gone on fresh data: ${now15} (cached: +${microMom.change15mPct}%, ${microMom.rvol}x, ${orderFlow.takerBuyRatio}%).`, 'info');
          continue;
        }
        microMom = fm; orderFlow = ff;
      }

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
      const plan = Strategy.planCompoundEntry(signal, book.bestAsk, computeEquity(), Number(State.portfolio.cashUSDT), sizeConfig(await leverageFor(pairKey)));
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
      if (openLong(pairKey, signal, plan, orderFlow.takerBuyRatio)) tagMomentum(pairKey, mgL);
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

// v6.8: a leveraged trade that moves to a recovery bag gets free cash added to its margin, back to 1x (or as close as
// free cash allows), so it cannot be liquidated while it waits for +0.30%. Done once, when it becomes a bag (as back-tested).
// v6.21: cash a top-up leaves free for new trades (one new trade's margin plus its opening fee).
const bagReserve = () => Strategy.bagTopUpReserve(computeEquity(), sizeConfig(1));

function topUpBag(k, bag) {
  const notional = Number(bag.qty) * Number(bag.entryPriceUSDT), margin = Number(bag.marginUSDT);
  if (!(margin > 0) || margin >= notional - 0.005) return;
  const reserve = bagReserve();
  const add = Strategy.bagTopUp(margin, notional, Number(State.portfolio.cashUSDT) - reserve);
  if (!(add > 0)) {
    logOperation('RECOVERY', `${bag.pair}: no free cash above the $${reserve.toFixed(2)} kept for new trades, so nothing added to the bag's margin yet. It stays ${Number(bag.leverage).toFixed(2)}x, liquidation $${formatPrecisePrice(bag.liquidationPrice)}, until more cash frees up.`, 'warning');
    return;
  }
  applyBagTopUp(bag, add);
}

// Moves `add` of free cash into a bag's margin and works out its new leverage and liquidation price.
function applyBagTopUp(bag, add, later = false) {
  const notional = Number(bag.qty) * Number(bag.entryPriceUSDT), margin = Number(bag.marginUSDT);
  State.portfolio.cashUSDT = Number((Number(State.portfolio.cashUSDT) - add).toFixed(6));
  bag.marginUSDT = Number((margin + add).toFixed(6));
  const L = notional / bag.marginUSDT;
  bag.leverage = Number(L.toFixed(2));
  bag.liquidationPrice = Strategy.liquidationPrice(Number(bag.entryPriceUSDT), isShort(bag) ? 'SHORT' : 'LONG', L);
  logOperation('RECOVERY', `${bag.pair}: $${add.toFixed(2)} of free cash added to the bag's margin${later ? ' (cash freed up)' : ''}, now ${L <= 1.0001
    ? `1x (${isShort(bag) ? `liquidation only at $${formatPrecisePrice(bag.liquidationPrice)}, about +99%` : 'no liquidation'})`
    : `${L.toFixed(2)}x, liquidation $${formatPrecisePrice(bag.liquidationPrice)} (more is added as cash frees up)`}.`, L <= 1.0001 ? 'success' : 'warning');
}

// v6.19: a bag that is still leveraged (no free cash when it moved to the bag) gets cash as it frees up, until it is back
// to 1x. Runs every second before the entry checks. v6.21: only cash above one new trade's margin goes to bags, so the
// autopilot can always open its next trade.
function topUpBags() {
  if (tradingPaused()) return;
  const plan = Strategy.planBagTopUps(State.recoveryBags, Number(State.portfolio.cashUSDT), undefined, bagReserve());
  for (const { key, add } of plan) { const bag = State.recoveryBags[key]; if (bag) applyBagTopUp(bag, add, true); }
  if (plan.length) { saveDiskState(); broadcastStateUpdate(); }
}

function demoteToBag(k, pos, price, reason) {
  const now = Date.now(), st = Strategy.bagStage({ movedAt: now }, now), sgn = isShort(pos) ? -1 : 1;   // v6.20: entry at once
  State.recoveryBags[k] = {
    pair: pos.pair, pairSymbol: pos.pairSymbol, openedAt: pos.openedAt, movedAt: now,
    entryPriceUSDT: Number(pos.entryPriceUSDT), livePriceUSDT: price, lastTickAt: now,
    notionalUSDT: pos.notionalUSDT, qty: pos.qty,
    side: isShort(pos) ? 'SHORT' : 'LONG', market: isFutures(pos) ? 'FUTURES' : 'SPOT', fundingUSDT: pos.fundingUSDT || 0, nextFundingAt: pos.nextFundingAt || 0, liquidationPrice: pos.liquidationPrice,
    targetStep: `CLOSES AT ${st.pct >= 0 ? '+' : ''}${st.pct.toFixed(2)}%`, targetExitPriceUSDT: Number(pos.entryPriceUSDT) * (1 + sgn * st.pct / 100),
    nextStepAt: st.nextAt, nextStepPct: st.nextPct, lossAt: st.lossAt,   // v6.18 stages, from Strategy.bagStage
    troughPrice: price, exitInProgress: false, candlestickPattern: pos.candlestickPattern || 'MONITORING', strategyVersion: 6,
    entrySource: pos.entrySource, leverage: pos.leverage, leverageAtEntry: pos.leverageAtEntry, marginUSDT: pos.marginUSDT,
    ...(pos.momentum ? { momentum: pos.momentum } : {})   // v6.33
  };
  // v6.12: your take profit stays with the trade when it becomes a bag (in place of the bag's +0.30%)
  const mine = Number(pos.userTpPct) > 0;
  if (mine) {
    Object.assign(State.recoveryBags[k], { userTpPct: pos.userTpPct, userTpSetAt: pos.userTpSetAt });
    Strategy.manageUserTp(State.recoveryBags[k], price, { inBag: true });
  }
  delete State.activePositions[k];
  if (isFutures(State.recoveryBags[k])) topUpBag(k, State.recoveryBags[k]);
  logOperation('SENTRY_DEMOTE', `${pos.pair}: ${reason}. Moved to ${isShort(pos) ? 'Short' : isFutures(pos) ? 'Long' : 'Spot'} Recovery Bag, closes at ${mine ? `your take profit +${pos.userTpPct}%` : `entry ($${formatPrecisePrice(State.recoveryBags[k].targetExitPriceUSDT)}), however long it takes`}${mine ? ` ($${formatPrecisePrice(State.recoveryBags[k].targetExitPriceUSDT)})` : ''}.`, 'warning');
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
      if (pos.targetOnlyPct || Number(pos.userTpPct) > 0) { exitSignals.delete(k); continue; }
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
  if (sentryBusy || tradingPaused()) return;
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
      const liqPx = Number(pos.liquidationPrice);
      if (liqPx > 0 && (short ? price >= liqPx : price <= liqPx)) {
        realizeExit('activePositions', k, 1, price, 'LIQUIDATED', `1x margin used up at $${formatPrecisePrice(price)}`);
        continue;
      }
      // v6.23: a Fibonacci trade is managed only at each closed bar (fibTick): stop-loss, TP1 35% + stop to entry (v6.24), TP2.
      // No minimum exit, trail, harvest, scratch or move to a recovery bag.
      if (pos.fib) continue;
      // v6.12: your take profit replaces the autopilot's exits for this trade; the -3.50% move to a bag stays.
      if (Number(pos.userTpPct) > 0) {
        const r = Strategy.manageUserTp(pos, price);
        if (r.action === 'EXIT') {
          const fill = (await executableExitPrice(k, pos)) || price;
          if (State.activePositions[k] === pos && Strategy.manageUserTp(pos, fill).action === 'EXIT') {
            realizeExit('activePositions', k, 1, fill, r.reason, `Your take profit +${Number(pos.userTpPct).toFixed(2)}% reached @ $${formatPrecisePrice(fill)}`);
          }
        } else if (r.action === 'DEMOTE') {
          demoteToBag(k, pos, price, r.reason);
        }
        continue;
      }
      // Breakdown long (test): its +0.30% target on the real bid, or a recovery bag at -3.50%. Nothing else.
      if (pos.targetOnlyPct) {
        const r = Strategy.manageTargetLong(pos, price);
        if (r.action === 'EXIT') {
          const fill = (await executableExitPrice(k, pos)) || price;
          if (Strategy.manageTargetLong(pos, fill).action === 'EXIT' &&
            realizeExit('activePositions', k, 1, fill, r.reason, `Breakdown long (test) closed at its +${Number(pos.targetOnlyPct).toFixed(2)}% target @ $${formatPrecisePrice(fill)}`)) {
            State.breakdownLongs.target++; saveDiskState();
          }
        } else if (r.action === 'DEMOTE') {
          demoteToBag(k, pos, price, r.reason);
          State.breakdownLongs.bagged++; saveDiskState();
        }
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

    // ---- recovery bags: close at their stage's target (v6.22: entry, however long it takes) ----
    for (const k of Object.keys(State.recoveryBags || {})) {
      const bag = State.recoveryBags[k];
      if (!bag || bag.exitInProgress) continue;
      const price = await livePriceFor(k, bag);
      if (!price) continue;
      bag.livePriceUSDT = price;
      const bagLiq = Number(bag.liquidationPrice);
      if (isFutures(bag) && bagLiq > 0 && (isShort(bag) ? price >= bagLiq : price <= bagLiq)) {
        realizeExit('recoveryBags', k, 1, price, 'LIQUIDATED', `1x margin used up at $${formatPrecisePrice(price)}`);
        continue;
      }
      // v6.12: a bag with your take profit closes there instead of at +0.30%
      const mine = Number(bag.userTpPct) > 0;
      const manage = (px) => (mine ? Strategy.manageUserTp(bag, px, { inBag: true }) : isShort(bag) ? Strategy.manageShortBag(bag, px) : Strategy.manageBag(bag, px));
      const r = manage(price);
      if (r.action === 'EXIT') {
        const fill = (await executableExitPrice(k, bag)) || price;
        const check = State.recoveryBags[k] === bag ? manage(fill) : { action: 'HOLD' };
        if (check.action === 'EXIT') {
          const bdl = bag.entrySource === 'BREAKDOWN_LONG', lvl = (String(r.reason).match(/[-+]?\d+\.\d+%$/) || [''])[0];
          const inBagMin = Math.round((Date.now() - Number(bag.movedAt || bag.openedAt)) / 60000);
          const inBag = inBagMin < 120 ? `${inBagMin} min` : `${(inBagMin / 60).toFixed(1)} h`;
          const why = mine ? `Recovery bag closed at your take profit +${Number(bag.userTpPct).toFixed(2)}% @ $${formatPrecisePrice(fill)}`
            : `Recovery bag${bdl ? ' (breakdown long test)' : ''} closed at ${lvl === '0.00%' ? 'entry (0.00%)' : lvl} or better, ${inBag} after it moved to the bag @ $${formatPrecisePrice(fill)}`;
          if (realizeExit('recoveryBags', k, 1, fill, r.reason, why) && bdl && !mine) {
            State.breakdownLongs.bagClosed++; saveDiskState();
          }
        }
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

    // v6.14: the engine's own record of this trade, by id first (the old fixed $0.0001 price window matched other trades
    // of a cheap coin on the same day and gave this row their size)
    const existingLocal = Strategy.ledgerMatch(State.tradeLogs, { id: tradeId, pair, date: cleanDate, time: cleanTime, entryPriceUSDT: entry });
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

// v6.11: Google Apps Script answers a GET with a 302 redirect to Google's content server. safeFetch does not follow redirects,
// so the engine never actually read the sheet. This does (Node 18+ fetch); safeFetch stays as the fallback.
async function fetchLedgerJson(url, timeoutMs = 20000) {
  if (typeof fetch !== 'function') return safeFetch(url, timeoutMs);
  const r = await fetch(url, { redirect: 'follow', signal: AbortSignal.timeout(timeoutMs) });
  const text = await r.text();
  try { return JSON.parse(text); } catch (e) { return text; }
}

async function syncTradesFromGoogleSheets() {
  const masterUrl = State.masterLedgerUrl;

  try {
    const fullUrl = masterUrl.includes('?') ? `${masterUrl}&action=GET_ALL` : `${masterUrl}?action=GET_ALL`;
    const data = await fetchLedgerJson(fullUrl, 20000);
    let fetchedTrades = [];
    if (data && Array.isArray(data.trades) && data.trades.length > 0) {
      fetchedTrades = data.trades;
    } else if (Array.isArray(data) && data.length > 0) {
      fetchedTrades = data;
    }
    const answered = Boolean(data && (Array.isArray(data) || Array.isArray(data.trades)));
    State.ledgerSync = answered ? { okAt: Date.now(), triedAt: Date.now(), rows: fetchedTrades.length, note: '' }
      : { ...State.ledgerSync, triedAt: Date.now(), note: 'the sheet did not answer with trades' };

    // v6.16: our ledger script answers { success: true, trades: [...] }. That answer is the trade list, so a row deleted in
    // the sheet leaves the app here (a trade closed in the last hour that the sheet has not shown yet is kept).
    if (data && data.success === true && Array.isArray(data.trades)) {
      const { trades, removed } = Strategy.ledgerReconcile(fetchedTrades, State.tradeLogs || []);
      State.tradeLogs = sanitizeAndAuditTradeHistory(trades);
      State.ledgerSync.removed = removed.length;
      if (removed.length) {
        const names = removed.slice(0, 5).map((t) => `${t.pair} ${t.date || ''}`.trim()).join(', ');
        logOperation('LEDGER', `${removed.length} trade${removed.length === 1 ? '' : 's'} deleted in your sheet ${removed.length === 1 ? 'was' : 'were'} removed from the trade list (${names}${removed.length > 5 ? ', ...' : ''}). Balance and open positions are unchanged.`, 'info');
        console.log(`[Sync Ledger] Removed ${removed.length} trades that are no longer in the sheet:`, removed.map((t) => `${t.id} ${t.pair} ${t.date} ${t.time} ${t.pnlUSDT}`).join(' | '));
      }
      saveDiskState();
      broadcastStateUpdate();
      console.log(`[Sync Ledger] ${State.tradeLogs.length} trades, as in the sheet (${fetchedTrades.length} rows).`);
      return State.tradeLogs;
    }

    if (fetchedTrades.length > 0) {
      // v6.11: for a trade the engine holds, its own record wins: the sheet has no side / market / funding / leverage
      // columns and gives a size worked back from the net P&L (15-45% too small). The sheet adds the trades the engine
      // does not hold (from before a restart), and trades only the engine holds are kept.
      const same = (a, b) => (Number(a.id) > 0 && Number(a.id) === Number(b.id) && a.pair === b.pair) || (a.pair === b.pair && a.date === b.date && a.time === b.time);
      const local = State.tradeLogs || [];
      const merged = fetchedTrades.map((f) => { const l = local.find((x) => same(x, f)); return l ? { ...f, ...l } : f; });
      local.forEach((l) => { if (!merged.some((f) => same(f, l))) merged.push(l); });
      State.tradeLogs = sanitizeAndAuditTradeHistory(merged);
      saveDiskState();
      broadcastStateUpdate();
      console.log(`[Sync Ledger] Successfully synchronized ${State.tradeLogs.length} verified trades.`);
      return State.tradeLogs;
    }
  } catch (err) {
    State.ledgerSync = { ...State.ledgerSync, triedAt: Date.now(), note: err.message };
    console.warn(`[Sync Ledger] Notice:`, err.message);
  }
  return State.tradeLogs;
}

async function recordTradeToCloudLedger(trade, { announce = true } = {}) {
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

  // v6.11: the app gets the trade now, not after the sheet write (that took up to 30 s). A diagnostic test write goes to the
  // sheet only, never into the trade list or P&L.
  if (!Strategy.isTestTrade(record)) State.tradeLogs.unshift(record);
  saveDiskState();
  if (announce) broadcastStateUpdate();

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

  return { success: true };
}

// =============================================================
// v6.23: FIBONACCI RETRACEMENT AUTOPILOT (Settings > Entry strategy).
// v6.24: the engine is fib.js FibConfluenceEngine, the best version in the replays: the document's v2 (1h structural),
// checked against its Python on real candles, plus the confluence gate (the 0.618-0.786 zone must hold the swing point
// before the impulse). It runs once per closed 1h bar over the radar's coins, as autopilot_step:
//   STEP 1 manages its open trades on the closed bar: stop-loss, TP1 (35% at the swing extreme, stop to entry + fees),
//          TP2 (the rest at the 1.272 extension),
//   STEP 2 screens every coin (10-bar swings of 3+ ATR in an EMA50 / EMA200 trend, a touch of 0.650 / 0.618 / 0.786,
//          a structural reversal candle, RSI turning, R:R 1.8+, confluence), ranks by confluence score,
//   STEP 3 opens the best within its caps: 1% of account value at risk a trade, 3% in all, 3 trades, 2 a side, 2x.
// Fills are the engine's prices (entry at the signal bar's close, exits at the stop / target), with the usual 0.05% fees.
// =============================================================
const FIB = { engine: 'v2c', timeframe: '1h', leverage: 2.0, window: 1000, warmBars: 500, maxCatchUp: 200 };
const fibSeries = new Map();   // symbol -> { tf, bars } closed bars, oldest first (at most window + warmBars)
let fibEngine = null, fibEngineTf = null, fibBusy = false;
const fibSleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fibBarTime = (t, tfMs) => new Date(t + tfMs).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false });

function resetFibMemory(tf) {
  State.fib.memory = { pivots: {}, setups: {} };
  Object.assign(State.fib.portfolio, { cooldown_until_bar: {}, bar_counters: {} });
  Object.assign(State.fib, { lastBarT: 0, warm: false, timeframe: tf, engine: FIB.engine });
  fibEngine = null;
}

function fibEngineFor(tf) {
  if (!fibEngine || fibEngineTf !== tf) {
    fibEngine = new Fib.FibConfluenceEngine({ leverage: FIB.leverage });   // v6.24
    fibEngineTf = tf;
  }
  // the engine works on State.fib.memory itself, so saving the state (and a handover) carries its swing memory
  if (fibEngine._pivots !== State.fib.memory.pivots || fibEngine._active_setups !== State.fib.memory.setups) fibEngine.restore(State.fib.memory);
  return fibEngine;
}

async function updateFibSeries(sym, tf, tfMs, needT, full) {
  const have = fibSeries.get(sym);
  const whole = full || !have || have.tf !== tf || !have.bars.length || needT - have.bars[have.bars.length - 1].t > 3 * tfMs;
  const k = await safeFetch(`${FAPI_BASE}/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=${whole ? FIB.window + FIB.warmBars : 5}`, 8000).catch(() => null);
  if (!Array.isArray(k)) return false;
  const now = Date.now();
  const fresh = Fib.toBars(k.filter((x) => Number(x[6]) < now));   // closed bars only
  let bars = whole ? fresh : have.bars.concat(fresh.filter((b) => b.t > have.bars[have.bars.length - 1].t));
  if (bars.length > FIB.window + FIB.warmBars) bars = bars.slice(-(FIB.window + FIB.warmBars));
  fibSeries.set(sym, { tf, bars });
  return true;
}

function fibRegime() {
  const m = State.subsystems.marketMode && State.subsystems.marketMode.mode;
  return m === 'BULL' ? 'BULLISH' : m === 'BEAR' ? 'BEARISH' : null;   // the spec's btc_regime
}

function syncFibStatus(extra = {}) {
  const tf = State.fibTimeframe, prm = new Fib.FibConfluenceEngine({ leverage: FIB.leverage }).params(), port = State.fib.portfolio;
  const waiting = Object.values(State.fib.memory.setups || {}).filter((st) => st.active && !st.traded
    && ((port.bar_counters[st.symbol] || 0) - st.created_bar) <= prm.max_setup_bars).length;
  State.subsystems.fib = { ...(State.subsystems.fib || {}), strategy: State.entryStrategy, timeframe: tf, timeframes: [FIB.timeframe], engine: FIB.engine,
    warm: State.fib.warm, lastBarT: State.fib.lastBarT, setupsWaiting: waiting, positions: Object.keys(port.positions || {}).length,
    params: prm, ...extra };
}

function openFib(o, sig, tf, T) {
  const k = o.symbol, side = o.action === 'OPEN_LONG' ? 'LONG' : 'SHORT', lev = FIB.leverage, port = State.fib.portfolio;
  const sp = port.positions[k] || {};
  const undo = (why) => { delete port.positions[k]; logOperation('FIB', `${k}: Fibonacci ${side.toLowerCase()} not opened: ${why}.`, 'warning'); return false; };
  if (tradingPaused()) return undo('trading is paused');
  if (side === 'LONG' && State.sideMode === 'SHORT') return undo('Settings > Trade direction is Shorts only');   // v6.30
  if ((State.activePositions[k] && !State.activePositions[k].fib) || State.recoveryBags[k]) return undo('this coin already has an open trade or bag');
  const entry = Number(o.price), qty = Number(o.quantity), notional = qty * entry, margin = notional / lev, cost = margin + notional * FUT_FEE_RATE;
  if (!(qty > 0) || Number(State.portfolio.cashUSDT) < cost) return undo(`needs $${cost.toFixed(2)}, free cash $${Number(State.portfolio.cashUSDT).toFixed(2)}`);
  State.portfolio.cashUSDT = Number((Number(State.portfolio.cashUSDT) - cost).toFixed(6));
  const sym = k.replace(/USDT$/, '');
  State.activePositions[k] = {
    pair: `${sym}/USDT`, pairSymbol: sym, side, market: 'FUTURES', leverage: lev, leverageAtEntry: lev, marginUSDT: margin, strategyVersion: 6, strategy: 'FIB',
    openedAt: Date.now(), entryPriceUSDT: entry, livePriceUSDT: entry, lastTickAt: Date.now(),
    qty, notionalUSDT: Number(notional.toFixed(4)), initialNotionalUSDT: Number(notional.toFixed(4)),
    initialStopPrice: o.stop_loss, sl: o.stop_loss, breakevenPrice: entry, tp1Price: o.take_profit_1, targetPrice: o.take_profit_2,
    liquidationPrice: Strategy.liquidationPrice(entry, side, lev), fundingUSDT: 0, nextFundingAt: 0,
    peakGainPct: 0, ratchetStage: 'FIB', candlestickPattern: `FIB ${sp.fib_level || ''} ${side === 'LONG' ? 'long' : 'short'}`.replace(/\s+/g, ' '),
    fib: { level: sp.fib_level, sl: o.stop_loss, tp1: o.take_profit_1, tp2: o.take_profit_2, tp1Hit: false, initialQty: qty,
      score: sig ? sig.confluence_score : null, rr: sig ? sig.rr_ratio : null, riskUSDT: Number(sp.risk_amount) || 0,
      timeframe: tf, barT: T, positionId: sp.position_id || null, setupId: sig ? sig.setup_id : null }
  };
  logOperation('EXECUTION', `${side === 'LONG' ? 'LONGED' : 'SHORTED'} ${sym}/USDT @ $${formatPrecisePrice(entry)} (Fibonacci ${sp.fib_level} ${side.toLowerCase()}, ${tf}; $${notional.toFixed(2)} on $${margin.toFixed(2)} margin, ${lev}x, paper) • stop $${formatPrecisePrice(o.stop_loss)} (risk $${(Number(sp.risk_amount) || 0).toFixed(2)}) • TP1 $${formatPrecisePrice(o.take_profit_1)} (35%, then the stop to entry + fees) • TP2 $${formatPrecisePrice(o.take_profit_2)}${sig ? ` • score ${sig.confluence_score}, R:R ${sig.rr_ratio}` : ''}`, 'success');
  return true;
}

function executeFibOrders(orders, candidates, tf, T) {
  for (const o of orders) {
    const k = o.symbol;
    if (o.action === 'OPEN_LONG' || o.action === 'OPEN_SHORT') { openFib(o, candidates.find((c) => c.symbol === k), tf, T); continue; }
    const pos = State.activePositions[k];
    if (!pos || !pos.fib) { logOperation('FIB', `${k}: ${o.reason} at $${formatPrecisePrice(o.price)}, but the trade is no longer open here.`, 'info'); continue; }
    const short = isShort(pos);
    if (o.action === 'CLOSE_PARTIAL') {
      const fraction = Math.min(1, Number(o.quantity) / Number(pos.qty));
      realizeExit('activePositions', k, fraction, o.price, 'FIB TP1 35%', `Fibonacci TP1: ${Math.round(fraction * 100)}% closed at the swing ${short ? 'low' : 'high'} $${formatPrecisePrice(o.price)}; stop moved to entry + fees $${formatPrecisePrice(o.stop_loss)}`);
      const still = State.activePositions[k];
      if (still && still.fib) { Object.assign(still.fib, { sl: o.stop_loss, tp1Hit: true }); still.sl = o.stop_loss; }
    } else if (o.reason === 'STOP_LOSS_HIT') {
      const be = Boolean(pos.fib.tp1Hit);
      realizeExit('activePositions', k, 1, o.price, be ? 'FIB BREAKEVEN STOP' : 'FIB STOP LOSS',
        be ? `Fibonacci: the rest closed at its entry stop $${formatPrecisePrice(o.price)}` : `Fibonacci stop-loss hit at $${formatPrecisePrice(o.price)} (${pos.fib.level} ${short ? 'short' : 'long'})`);
    } else {
      realizeExit('activePositions', k, 1, o.price, 'FIB TP2 1.272', `Fibonacci TP2: the rest closed at the 1.272 extension $${formatPrecisePrice(o.price)}`);
    }
  }
}

async function fibCycle(tf, tfMs, targetT, active) {
  const eng = fibEngineFor(tf), port = State.fib.portfolio;
  // a Fibonacci trade closed here by hand (or liquidated) leaves the engine's book too
  for (const s of Object.keys(port.positions)) { const p = State.activePositions[s]; if (!p || !p.fib) delete port.positions[s]; }
  const held = new Set(Object.keys(port.positions));
  const blocked = new Set([...Object.keys(State.activePositions).filter((k) => !State.activePositions[k].fib), ...Object.keys(State.recoveryBags || {})]);
  const list = [...new Set([...(State.fibUniverse || []), ...held])].filter((s) => perpetuals.has(s) && (held.has(s) || !blocked.has(s)));
  const warmUp = !State.fib.warm;

  // candles: a full read (weight 10) a few coins a second, then 5 bars (weight 1) per coin per bar
  const needsFull = (s) => warmUp || !fibSeries.get(s) || fibSeries.get(s).tf !== tf;
  const full = list.filter(needsFull), quick = list.filter((s) => !needsFull(s));
  for (let i = 0; i < full.length; i += 2) {
    let waits = 0;
    while (fapiBusy() && waits++ < 20) await fibSleep(3000);
    await Promise.all(full.slice(i, i + 2).map((s) => updateFibSeries(s, tf, tfMs, targetT, true)));
    await fibSleep(1000);
  }
  for (let i = 0; i < quick.length; i += 10) await Promise.all(quick.slice(i, i + 10).map((s) => updateFibSeries(s, tf, tfMs, targetT, false)));

  const times = [];
  const from = warmUp ? targetT - (FIB.warmBars - 1) * tfMs : Math.max(State.fib.lastBarT + tfMs, targetT - (FIB.maxCatchUp - 1) * tfMs);
  for (let T = from; T <= targetT; T += tfMs) times.push(T);
  const index = new Map();
  for (const s of list) { const ser = fibSeries.get(s); if (ser) { const m = new Map(); ser.bars.forEach((b, i) => m.set(b.t, i)); index.set(s, m); } }
  const regime = fibRegime();
  let last = null;
  for (const T of times) {
    const live = T === targetT;
    const universe = new Map();
    for (const s of list) {
      const i = index.get(s) && index.get(s).get(T), p = perpetuals.get(s);
      if (i === undefined || !p || !(p.tickSize > 0) || !(p.stepSize > 0)) continue;
      const bars = fibSeries.get(s).bars.slice(Math.max(0, i - FIB.window + 1), i + 1);
      universe.set(s, { spec: Fib.SymbolSpec(s, { tick_size: p.tickSize, step_size: p.stepSize, min_notional: p.minNotional || 5, taker_fee: FUT_FEE_RATE }),
        bars, quote_volume_24h: Fib.quoteVolume24h(bars, tfMs) });
    }
    // while warming up, open trades wait for the live bar (the replayed bars are from before they opened)
    const parked = {};
    if (warmUp && !live) for (const s of Object.keys(port.positions)) { parked[s] = port.positions[s]; delete port.positions[s]; }
    port.equity = computeEquity();
    port.free_balance = Number(State.portfolio.cashUSDT);
    const r = eng.autopilot_step(universe, port, regime, { allowEntries: live && active && State.autopilotEnabled });
    Object.assign(port.positions, parked);
    executeFibOrders(r.orders, r.candidates, tf, T);
    if (live) last = { coins: universe.size, candidates: r.candidates.slice(0, 5), orders: r.orders.length };
    await new Promise((res) => setImmediate(res));   // the sentry and the app keep running between bars
  }
  delete port.equity; delete port.free_balance;
  State.fib.lastBarT = targetT;
  State.fib.timeframe = tf;
  if (warmUp) {
    State.fib.warm = true;
    logOperation('FIB', `Fibonacci ${tf}: read the last ${FIB.warmBars} closed bars of ${list.length} coins to find their current swings. It trades from the bar that closed at ${fibBarTime(targetT, tfMs)}.`, 'success');
  }
  syncFibStatus({ coins: last ? last.coins : 0, lastSignals: last ? last.candidates.map((c) => ({ symbol: c.symbol, side: c.side, level: c.fib_level, score: c.confluence_score, rr: c.rr_ratio })) : [] });
  const f = State.subsystems.fib, sig = last && last.candidates.length
    ? `${last.candidates.length} triggered (${last.candidates.map((c) => `${c.symbol.replace(/USDT$/, '')} ${c.fib_level} ${c.side.toLowerCase()}, score ${c.confluence_score}, R:R ${c.rr_ratio}`).join('; ')})`
    : 'no setup triggered';
  logOperation('FIB', `Fibonacci ${tf} bar ${fibBarTime(targetT, tfMs)}: ${last ? last.coins : 0} coins screened, ${sig}; ${f.setupsWaiting} setups waiting, ${f.positions} Fibonacci trade${f.positions === 1 ? '' : 's'} open${active ? '' : ' (entries off: candlestick mode)'}.`, 'info');
  saveDiskState();
  broadcastStateUpdate();
}

async function fibTick() {
  if (fibBusy || tradingPaused()) return;
  const tf = State.fibTimeframe, tfMs = Fib.TF_MS[tf];
  const active = State.entryStrategy === 'FIB';
  if (!active && !Object.keys(State.fib.portfolio.positions || {}).length) return;
  if ((State.fib.timeframe && State.fib.timeframe !== tf) || State.fib.engine !== FIB.engine) resetFibMemory(tf);   // v6.24: a new engine re-reads the swings
  const now = Date.now(), targetT = Math.floor(now / tfMs) * tfMs - tfMs;   // open time of the last closed bar
  if (now - (targetT + tfMs) < 4000) return;                               // a few seconds for Binance to close the bar
  if (State.fib.lastBarT >= targetT || fapiBusy() || !perpetuals.size || !(State.fibUniverse || []).length) return;
  fibBusy = true;
  try {
    await fibCycle(tf, tfMs, targetT, active);
  } catch (e) {
    console.error('[Fib]', e && e.message);
    logOperation('FIB', `Fibonacci cycle error: ${e && e.message}. It retries on the next bar.`, 'warning');
  } finally {
    fibBusy = false;
  }
}

// Everything the account needs to carry on: saved to disk, and (v6.15, with the live extras) handed to the next build.
function persistableState(keepTrades = 150, live = false) {
  const data = {
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    tradeLogs: State.tradeLogs.slice(0, keepTrades),
    manual: State.manual,
    positionPct: State.positionPct,
    leverageAuto: State.leverageAuto,
    bothSides: State.bothSides,
    sideMode: State.sideMode,
    commentary: Commentary.toJSON(),   // v6.31
    momentumOn: State.momentumOn, momentum: MomentumReport.toJSON(),   // v6.33
    breakdownSide: State.breakdownSide,
    breakdownLongs: State.breakdownLongs,
    masterLedgerUrl: State.masterLedgerUrl,
    learningSheetUrl: State.learningSheetUrl,
    capitalResetId: State.capitalResetId || CAPITAL_RESET_ID,
    lastTrendMode: State.lastTrendMode,
    entryStrategy: State.entryStrategy,   // v6.23
    fibTimeframe: State.fibTimeframe,
    fib: State.fib
  };
  if (live) Object.assign(data, { autopilotEnabled: State.autopilotEnabled, cooldowns: State.cooldowns, usdtToInrRate: State.usdtToInrRate, marketMode: State.subsystems.marketMode });
  return data;
}

function saveDiskState() {
  try {
    fs.writeFileSync(DISK_STATE_FILE, JSON.stringify(persistableState(), null, 2), 'utf-8');
  } catch (e) {}
}

// The saved fields onto State: from the disk file, or (live = true) from the account a previous build handed over.
function applyPersistedState(parsed, live = false) {
  if (!parsed || typeof parsed !== 'object') return;
  if (parsed.portfolio) State.portfolio = parsed.portfolio;
  if (parsed.activePositions) State.activePositions = parsed.activePositions;
  if (parsed.recoveryBags) State.recoveryBags = parsed.recoveryBags;
  if (Array.isArray(parsed.tradeLogs)) State.tradeLogs = parsed.tradeLogs;
  if (parsed.manual && parsed.manual.positions) State.manual = { ...State.manual, ...parsed.manual };
  if (Strategy.validPositionPct(parsed.positionPct)) State.positionPct = Strategy.validPositionPct(parsed.positionPct);
  if (typeof parsed.leverageAuto === 'boolean') State.leverageAuto = parsed.leverageAuto;
  if (typeof parsed.bothSides === 'boolean') State.bothSides = parsed.bothSides;
  if (Strategy.validSideMode(parsed.sideMode)) State.sideMode = Strategy.validSideMode(parsed.sideMode);   // v6.30
  if (parsed.commentary) Commentary.load(parsed.commentary);   // v6.31
  if (typeof parsed.momentumOn === 'boolean') State.momentumOn = parsed.momentumOn;   // v6.33
  if (parsed.momentum) MomentumReport.load(parsed.momentum);
  if (BREAKDOWN_SIDES[parsed.breakdownSide]) State.breakdownSide = parsed.breakdownSide;
  if (parsed.breakdownLongs && typeof parsed.breakdownLongs === 'object') State.breakdownLongs = { ...State.breakdownLongs, ...parsed.breakdownLongs };
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
  if (parsed.lastTrendMode === 'BULL' || parsed.lastTrendMode === 'BEAR') State.lastTrendMode = parsed.lastTrendMode;
  if (parsed.entryStrategy === 'CANDLE' || parsed.entryStrategy === 'FIB') State.entryStrategy = parsed.entryStrategy;   // v6.23
  // v6.24: the Fibonacci timeframe is fixed at 1h (a saved 15m / 30m / 4h is ignored; its swing memory is re-read)
  if (parsed.fib && typeof parsed.fib === 'object' && parsed.fib.portfolio && parsed.fib.memory) {
    State.fib = { memory: { pivots: parsed.fib.memory.pivots || {}, setups: parsed.fib.memory.setups || {} },
      portfolio: { positions: parsed.fib.portfolio.positions || {}, cooldown_until_bar: parsed.fib.portfolio.cooldown_until_bar || {}, bar_counters: parsed.fib.portfolio.bar_counters || {} },
      lastBarT: Number(parsed.fib.lastBarT) || 0, warm: Boolean(parsed.fib.warm), timeframe: parsed.fib.timeframe || null, engine: parsed.fib.engine || null };
  }
  if (live) {
    if (typeof parsed.autopilotEnabled === 'boolean') State.autopilotEnabled = parsed.autopilotEnabled;
    if (parsed.cooldowns && typeof parsed.cooldowns === 'object') State.cooldowns = parsed.cooldowns;
    if (Number(parsed.usdtToInrRate) > 0) State.usdtToInrRate = Number(parsed.usdtToInrRate);
    if (parsed.marketMode && parsed.marketMode.mode) State.subsystems.marketMode = parsed.marketMode;
  }
}

function loadDiskState() {
  try {
    if (fs.existsSync(DISK_STATE_FILE)) applyPersistedState(JSON.parse(fs.readFileSync(DISK_STATE_FILE, 'utf-8')));
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

// v6.15: while this build is starting or has handed its account to a new build, nothing may change the account here.
app.use((req, res, next) => {
  if (req.method === 'GET' || req.path === '/api/handoff' || !tradingPaused()) return next();
  res.status(503).json({ success: false, error: Handover.frozen
    ? 'This build has handed its trades to the new build. Reload the app in a few seconds.'
    : 'The engine is starting. Try again in a few seconds.' });
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
// v6.15, build handover. whoami: which build answers the public URL now.
app.get('/api/whoami', (req, res) => res.json({ bootId: BOOT_ID, ready: Handover.ready, frozen: Handover.frozen }));

// A new build asks for the account: trading stops here (for good, unless the new build never goes live) and the whole
// account is returned. A repeated ask gets the same paused account. Nothing here takes an account from outside.
app.post('/api/handoff', (req, res) => {
  const to = String((req.body && req.body.boot) || '').slice(0, 40);
  if (!to || to === BOOT_ID) return res.status(400).json({ ok: false, error: 'the new build must send its boot id' });
  if (!Handover.ready) return res.status(409).json({ ok: false, error: 'this build is still starting' });
  if (!Handover.frozen) {
    Object.assign(Handover, { frozen: true, to, at: Date.now() });
    const n = Object.keys(State.activePositions || {}).length, b = Object.keys(State.recoveryBags || {}).length;
    logOperation('HANDOVER', `A new build is starting: trading paused here, ${n} open trade${n === 1 ? '' : 's'} and ${b} bag${b === 1 ? '' : 's'} handed over to it.`, 'warning');
    saveDiskState();
    broadcastStateUpdate();
    watchHandover();
  }
  res.json({ ok: true, bootId: BOOT_ID, frozenAt: Handover.at, state: persistableState(Infinity, true) });
});

// The old build, paused: once the public URL answers from another build, it stays paused (Render stops it about a
// minute later). If after 5 minutes the public URL still answers from here, the new build never went live: resume.
function watchHandover() {
  const since = Date.now();
  const check = async () => {
    if (!Handover.frozen) return;
    let who = '';
    if (HANDOFF_URL && typeof fetch === 'function') {
      try { const r = await fetch(`${HANDOFF_URL}/api/whoami`, { signal: AbortSignal.timeout(8000) }); if (r.ok) who = String((await r.json()).bootId || ''); } catch (e) {}
    }
    if (who && who !== BOOT_ID) {
      logOperation('HANDOVER', 'The new build is live and has the account. This build stays paused until Render stops it.', 'success');
      broadcastStateUpdate();
      return;
    }
    if (Date.now() - since >= 5 * 60000 && (who === BOOT_ID || !HANDOFF_URL)) {
      Object.assign(Handover, { frozen: false, to: '', at: 0 });
      logOperation('HANDOVER', 'The new build did not go live within 5 minutes: trading resumed here.', 'warning');
      broadcastStateUpdate();
      return;
    }
    setTimeout(check, 10000);
  };
  setTimeout(check, 10000);
}

// An older build's public /api/state as saved fields (its settings are shown under subsystems). Its capital reset is
// the current one, so the account is not re-based.
function legacyHandover(st) {
  const sub = st.subsystems || {}, m = st.manual;
  return {
    portfolio: st.portfolio, activePositions: st.activePositions, recoveryBags: st.recoveryBags, tradeLogs: st.tradeLogs,
    manual: m && m.positions ? { startUSDT: m.startUSDT, cashUSDT: m.cashUSDT, realizedPnL: m.realizedPnL, positions: m.positions, history: m.history || [] } : undefined,
    positionPct: sub.sizing && sub.sizing.positionPct, leverageAuto: sub.leverage && sub.leverage.on, bothSides: sub.sides && sub.sides.on, sideMode: sub.sides && sub.sides.mode,
    breakdownSide: sub.breakdown && sub.breakdown.side, breakdownLongs: sub.breakdown && sub.breakdown.longs,
    masterLedgerUrl: st.masterLedgerUrl, learningSheetUrl: st.learningSheetUrl, capitalResetId: CAPITAL_RESET_ID,
    usdtToInrRate: st.usdtToInrRate, marketMode: sub.marketMode
  };
}

// The new build, before it opens its port: take the account from the build that still answers the public URL.
async function takeOverFromPreviousBuild() {
  if (!HANDOFF_URL || typeof fetch !== 'function') { Handover.note = 'no public URL to ask'; return; }
  const call = async (p, opts = {}) => {
    const r = await fetch(`${HANDOFF_URL}${p}`, { ...opts, signal: AbortSignal.timeout(15000) });
    if (!r.ok) return null;
    try { return await r.json(); } catch (e) { return null; }
  };
  try {
    const j = await call('/api/handoff', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ boot: BOOT_ID }) });
    if (j && j.ok && j.state && j.bootId && j.bootId !== BOOT_ID) {
      applyPersistedState(j.state, true);
      Object.assign(Handover, { how: 'handoff', from: j.bootId, restoredAt: Date.now() });
      return;
    }
  } catch (e) {   // nothing answered (no previous build running, e.g. a wake-up after Render put the service to sleep)
    Handover.note = e && e.name === 'TimeoutError' ? 'no previous build answered' : `previous build not reached (${e && e.message})`;
    return;
  }
  // A build from before v6.15 has no handover: read its public state. It keeps trading until Render stops it.
  try {
    const st = await call('/api/state');
    if (st && !st.handover && st.portfolio && st.activePositions && st.recoveryBags) {
      applyPersistedState(legacyHandover(st), true);
      Object.assign(Handover, { how: 'state', from: 'a build from before v6.15', restoredAt: Date.now() });
    } else Handover.note = st && st.handover ? 'the previous build did not hand over its account' : 'no previous build answered';
  } catch (e) { Handover.note = `previous build not read (${e && e.message})`; }
}

// v6.31: the characters' Gemini comments (the app fetches them when subsystems.ai.rev changes). No key, only text.
app.get('/api/commentary', (req, res) => { res.json({ ...Commentary.view(), status: Commentary.status() }); });

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
    equityUSDT: computeEquity(),
    manual: manualView(),
    pnl: pnlNow(),
    handover: { bootId: BOOT_ID, ready: Handover.ready, frozen: Handover.frozen, how: Handover.how, restoredAt: Handover.restoredAt }
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

// v6.16: the app's sync button and its 2-minute sync come here, so the engine (which holds each trade's exact record)
// does the matching and the app shows the result. sheet.okAt tells the app whether the sheet answered this time.
app.post('/api/trades/sync-strict-sheet', async (req, res) => {
  const t0 = Date.now();
  try {
    const trades = await syncTradesFromGoogleSheets();
    res.json({ success: true, count: trades.length, trades, sheet: { ...State.ledgerSync, answered: State.ledgerSync.okAt >= t0 } });
  } catch (err) {
    res.json({ success: true, count: State.tradeLogs.length, trades: State.tradeLogs, sheet: { ...State.ledgerSync, answered: false } });
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

// v6.12: your take profit on autopilot trades, at any time. { pairKey, book? } sets one trade, { all: true } every open
// position and recovery bag. takeProfitPct 0.1 to 100; null, '' or 0 clears it (the trade goes back to the autopilot's exits).
app.post('/api/tp', (req, res) => {
  const body = req.body || {};
  const tp = Strategy.userTakeProfit(body.takeProfitPct);
  if (!tp.ok) return res.status(400).json({ success: false, error: tp.reason });
  const all = body.all === true;
  if (!all && !body.pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const books = ['activePositions', 'recoveryBags'].filter((b) => all || !body.book || body.book === b);
  if (!all && State.activePositions[body.pairKey] && State.activePositions[body.pairKey].fib && body.book !== 'recoveryBags') {
    return res.status(400).json({ success: false, error: 'A Fibonacci trade closes only at its own stop-loss and targets' });
  }
  const hits = [];
  books.forEach((book) => Object.entries(State[book] || {}).forEach(([k, p]) => {
    if (p && !p.exitInProgress && !p.fib && (all || k === body.pairKey)) hits.push({ book, k, p });   // v6.23: not on Fibonacci trades
  }));
  if (!hits.length) return res.status(404).json({ success: false, error: all ? 'No open positions or bags' : 'Position not found' });
  hits.forEach(({ book, k, p }) => {
    const bag = book === 'recoveryBags', px = Number(p.livePriceUSDT) || Number(p.entryPriceUSDT);
    if (tp.pct) {
      p.userTpPct = tp.pct; p.userTpSetAt = Date.now(); p.userTpPrice = Strategy.userTpPrice(p);
      if (bag) Strategy.manageUserTp(p, px, { inBag: true });
    } else {
      delete p.userTpPct; delete p.userTpPrice; delete p.userTpSetAt;
      if (bag) { if (isShort(p)) Strategy.manageShortBag(p, px); else Strategy.manageBag(p, px); }   // the bag's own target again (v6.18 stages)
    }
    if (!all) {
      logOperation('SENTRY', tp.pct
        ? `${p.pair}: your take profit +${tp.pct}% ($${formatPrecisePrice(p.userTpPrice)}). It closes only there${bag ? ', in place of the bag\'s own target' : '; the autopilot\'s exits are off for it, the -3.50% move to a bag stays'}.`
        : `${p.pair}: your take profit removed. ${bag ? `The bag is back on its own target: ${String(p.targetStep || '').toLowerCase()}` : 'Back to the autopilot\'s exits'}.`, 'info');
    }
  });
  if (all) {
    logOperation('SENTRY', tp.pct
      ? `Your take profit +${tp.pct}% set on ${hits.length} open trade${hits.length === 1 ? '' : 's'} (${hits.map((h) => h.p.pair).join(', ')}). Each closes only there; the autopilot's exits are off for them, the -3.50% move to a bag stays.`
      : `Your take profit removed from ${hits.length} open trade${hits.length === 1 ? '' : 's'}. Back to the autopilot's exits (bags: at entry).`, 'info');
  }
  saveDiskState();
  broadcastStateUpdate();
  kickSentry();   // a take profit the price has already passed closes on this pass, at what the book pays
  res.json({ success: true, takeProfitPct: tp.pct, updated: hits.map(({ book, k, p }) => ({ book, pairKey: k, pair: p.pair, userTpPct: p.userTpPct || null, userTpPrice: p.userTpPrice || null })) });
});

// -------------------------------------------------------------
// MANUAL PAPER FUTURES: the user's own long/short positions on any USD-M perpetual.
// Fills at the real book (long buys the ask, short sells the bid). Positions stay open until the user closes them;
// the only exception is a 1x short whose margin is used up (price about +99%), which the exchange would liquidate.
// -------------------------------------------------------------
async function futuresQuote(sym) {
  const q = await safeFetch(`${FAPI_BASE}/fapi/v1/ticker/bookTicker?symbol=${sym}`, 2500).catch(() => null);
  const bid = parseFloat(q && q.bidPrice), ask = parseFloat(q && q.askPrice);
  if (bid > 0 && ask > 0) return { bid, ask };
  const last = await fetchFuturesPrice(sym);
  return last ? { bid: last, ask: last } : null;
}

function manualView() {
  const m = State.manual, pos = Object.values(m.positions || {});
  const marked = pos.reduce((a, p) => a + Strategy.manualMark(p, Number(p.livePriceUSDT) || Number(p.entryPriceUSDT)), 0);
  return { startUSDT: m.startUSDT, cashUSDT: m.cashUSDT, realizedPnL: m.realizedPnL, positions: m.positions, history: (m.history || []).slice(0, 50),
    equityUSDT: Number((Number(m.cashUSDT) + marked).toFixed(6)), futures: State.subsystems.futures.status };
}

function closeManual(id, price, label) {
  const m = State.manual, p = m.positions[id];
  if (!p || p.closing || tradingPaused()) return null;
  p.closing = true;
  const r = Strategy.manualClose(p, price);
  m.cashUSDT = Number((Number(m.cashUSDT) + r.proceeds).toFixed(6));
  m.realizedPnL = Number((Number(m.realizedPnL || 0) + r.pnl).toFixed(6));
  delete m.positions[id];
  const trade = { id: Date.now(), positionId: id, symbol: p.symbol, pair: p.pair, side: p.side, type: label, entryPriceUSDT: p.entryPriceUSDT, exitPriceUSDT: price,
    notionalUSDT: p.notionalUSDT, fundingUSDT: Number((Number(p.fundingUSDT) || 0).toFixed(4)), pnlUSDT: Number(r.pnl.toFixed(4)), gainPct: Number(r.gainPct.toFixed(3)),
    openedAt: p.openedAt, closedAt: Date.now(), holdMinutes: Math.round((Date.now() - p.openedAt) / 60000) };
  m.history = [trade, ...(m.history || [])].slice(0, Strategy.MANUAL.historyKeep);
  logOperation('MANUAL', `${label}: ${p.side} ${p.pair} @ $${formatPrecisePrice(price)} (${r.gainPct >= 0 ? '+' : ''}${r.gainPct.toFixed(2)}%) • Net ${r.pnl >= 0 ? '+' : ''}$${r.pnl.toFixed(4)}`, r.pnl >= 0 ? 'success' : 'warning');
  saveDiskState();
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
  return trade;
}

// Take-profit: once the trade price reaches the target, close at what the book really pays
// (long sells the bid, short buys the ask), and only if that fill still meets the target.
const tpBusy = new Set();
async function checkManualTakeProfit(sym, price) {
  for (const [id, p] of Object.entries(State.manual.positions || {})) {
    if (p.symbol !== sym || !p.takeProfitPct || p.closing || tpBusy.has(id)) continue;
    if (Strategy.manualGainPct(p, price) < p.takeProfitPct) continue;
    tpBusy.add(id);
    try {
      const q = await futuresQuote(sym);
      if (!q || !State.manual.positions[id]) continue;
      const fill = p.side === 'SHORT' ? q.ask : q.bid;
      if (Strategy.manualGainPct(p, fill) >= p.takeProfitPct) closeManual(id, fill, 'TAKE PROFIT');
    } finally { tpBusy.delete(id); }
  }
}

function checkManualLiquidation(sym, price) {
  Object.entries(State.manual.positions || {}).forEach(([id, p]) => {
    if (p.symbol === sym && p.side === 'SHORT' && p.liquidationPrice && price >= p.liquidationPrice) closeManual(id, p.liquidationPrice, 'LIQUIDATED');
  });
}

function syncLeverageStatus() {
  State.subsystems.leverage = { on: State.leverageAuto, maxLeverage: Strategy.LEVERAGE.maxLeverage, atrTargetPct: Strategy.LEVERAGE.atrTargetPct, mmrPct: Strategy.LEVERAGE.mmrPct };
}

function syncSidesStatus() {
  State.subsystems.sides = { on: State.bothSides, mode: State.sideMode, label: Strategy.SIDE_MODES[State.sideMode] };
  const mm = State.subsystems.marketMode;
  if (mm) mm.tradeSide = tradeSide(mm);
}

// v6.33: Settings > Momentum report. v6.34: on = a new short needs the hourly Gemini report to call the coin bearish (a long:
// bullish); the score is used only for leverage.
app.get('/api/momentum', (req, res) => { res.json({ report: MomentumReport.report(), status: State.subsystems.momentum || null }); });
app.post('/api/momentum', (req, res) => {
  const v = req.body.on, on = v === true || v === 'true' || v === 'on', off = v === false || v === 'false' || v === 'off';
  if (!on && !off) return res.status(400).json({ success: false, error: 'on must be true or false' });
  State.momentumOn = on;
  syncMomentumStatus();
  logOperation('ENTRIES', State.momentumOn
    ? `Momentum report ON: a new short needs the hourly Gemini report to call the coin bearish (a long: bullish), whatever its score${State.leverageAuto ? '; the score sets the leverage (under 70 1x, 70-79 2x, 80+ 3x)' : ''}. Open positions and bags keep their exits.`
    : 'Momentum report OFF: new trades no longer need it; auto leverage follows volatility again. Open positions and bags keep their exits.', 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, on: State.momentumOn });
});
app.post('/api/momentum/refresh', (req, res) => {
  const st = MomentumReport.status();
  if (!st.on) return res.status(400).json({ success: false, error: 'The server has no Gemini key for the momentum report (GEMINI_MOMENTUM_KEY).' });
  if (st.busy) return res.json({ success: true, started: false, note: 'A report is already being written.' });
  MomentumReport.tick(momentumRows, Date.now(), true).then((ok) => { syncMomentumStatus(); if (ok) { saveDiskState(); broadcastStateUpdate(); } }).catch(() => {});
  setTimeout(syncMomentumStatus, 50);
  res.json({ success: true, started: true });
});

// v6.30: Settings > Trade direction. SHORT = shorts only; BOTH = shorts and longs (the market mode, Both sides and v6.17 decide).
app.post('/api/trade-direction', (req, res) => {
  const m = Strategy.validSideMode(req.body.mode);
  if (!m) return res.status(400).json({ success: false, error: 'mode must be SHORT or BOTH' });
  State.sideMode = m;
  syncSidesStatus();
  logOperation('ENTRIES', m === 'SHORT'
    ? 'Trade direction: SHORTS ONLY. The autopilot opens only futures shorts, in every market mode; outside a Bearish market each needs the coin\'s own 1h downtrend. High vol breakdowns are shorted. No new longs. Open positions and bags keep their exits.'
    : `Trade direction: SHORTS AND LONGS. New trades follow the market mode${State.bothSides ? ' with Both sides on' : ''}, as before. Open positions and bags keep their exits.`, 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, mode: m, label: Strategy.SIDE_MODES[m], tradeSide: tradeSide() });
});

app.post('/api/both-sides', (req, res) => {
  const v = req.body.on, on = v === true || v === 'true' || v === 'on', off = v === false || v === 'false' || v === 'off';
  if (!on && !off) return res.status(400).json({ success: false, error: 'on must be true or false' });
  State.bothSides = on;
  syncSidesStatus();
  logOperation('ENTRIES', on
    ? 'Both sides ON: longs and shorts in every market mode. A trade against the market mode needs the coin\'s own 1h trend to agree. Open positions keep their exits.'
    : 'Both sides OFF: Bullish market longs only, Bearish market shorts only, Neutral follows each coin. Open positions keep their exits.', 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, on });
});

app.post('/api/leverage', (req, res) => {
  const v = req.body.on, on = v === true || v === 'true' || v === 'on', off = v === false || v === 'false' || v === 'off';
  if (!on && !off) return res.status(400).json({ success: false, error: 'on must be true or false' });
  State.leverageAuto = on;
  syncLeverageStatus();
  logOperation('ENTRIES', on
    ? `Auto leverage ON (test): new trades use 1x to ${Strategy.LEVERAGE.maxLeverage}x from each coin's volatility (${Strategy.LEVERAGE.atrTargetPct} / 15m ATR%). A trade that moves to a recovery bag gets free cash added to its margin, back to 1x. Open positions keep their leverage.`
    : 'Auto leverage OFF: new trades are 1x. Open positions and bags keep their leverage.', 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, on });
});

function syncSizingStatus() {
  State.subsystems.sizing = { positionPct: State.positionPct, minPct: Strategy.POSITION_SIZE.minPct, maxPct: Strategy.POSITION_SIZE.maxPct };
}

app.post('/api/position-size', (req, res) => {
  const pct = Strategy.validPositionPct(req.body.pct);
  if (!pct) return res.status(400).json({ success: false, error: `position size must be ${Strategy.POSITION_SIZE.minPct}% to ${Strategy.POSITION_SIZE.maxPct}% of account value` });
  State.positionPct = pct;
  syncSizingStatus();
  logOperation('SIZING', `Position size set to ${pct}% of account value ($${(computeEquity() * pct / 100).toFixed(2)} now). New trades only: open positions and bags keep their size.`, 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, pct });
});

function syncBreakdownStatus() {
  State.subsystems.breakdown = { side: State.breakdownSide, label: BREAKDOWN_SIDES[State.breakdownSide], longs: State.breakdownLongs };
}

app.post('/api/breakdown-side', (req, res) => {
  const side = String(req.body.side || '').toUpperCase();
  if (!BREAKDOWN_SIDES[side]) return res.status(400).json({ success: false, error: `side must be one of: ${Object.keys(BREAKDOWN_SIDES).join(', ')}` });
  State.breakdownSide = side;
  syncBreakdownStatus();
  logOperation('ENTRIES', side === 'LONG'
    ? 'High vol breakdown set to LONG (test): breakdown signals are bought and close only at +0.30%. Candle-pattern shorts and all longs are unchanged.'
    : 'High vol breakdown set to SHORT: breakdown signals are shorted with the normal exits. Open breakdown longs keep their +0.30% target.', 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, side, label: BREAKDOWN_SIDES[side] });
});

// v6.23: Settings > Entry strategy. { strategy: 'CANDLE' | 'FIB', timeframe?: '15m' | '30m' | '1h' | '4h' }
app.post('/api/strategy', (req, res) => {
  const b = req.body || {};
  const strategy = b.strategy === undefined ? State.entryStrategy : String(b.strategy).toUpperCase();
  const tf = b.timeframe === undefined ? State.fibTimeframe : String(b.timeframe);
  if (!['CANDLE', 'FIB'].includes(strategy)) return res.status(400).json({ success: false, error: 'strategy must be CANDLE or FIB' });
  if (tf !== FIB.timeframe) return res.status(400).json({ success: false, error: 'the Fibonacci strategy runs on 1h only (v6.24)' });
  const was = State.entryStrategy, tfChanged = tf !== State.fibTimeframe;
  State.entryStrategy = strategy;
  State.fibTimeframe = tf;
  if (tfChanged) resetFibMemory(tf);
  const nFib = Object.keys(State.fib.portfolio.positions || {}).length;
  if (was !== strategy || tfChanged) {
    logOperation('ENTRIES', strategy === 'FIB'
      ? `Entry strategy: Fibonacci retracement (${tf}). New trades come from the Fibonacci screener at each ${tf} bar close, with a stop-loss and two targets; candlestick entries are off. Open candlestick trades and bags keep their own exits.${tfChanged || !State.fib.warm ? ' It first reads the last 500 bars of every coin (a few minutes).' : ''}`
      : `Entry strategy: Candlestick pattern (the usual model).${nFib ? ` ${nFib} open Fibonacci trade${nFib === 1 ? '' : 's'} keep${nFib === 1 ? 's' : ''} ${nFib === 1 ? 'its' : 'their'} stop-loss and targets until closed.` : ''}`, 'warning');
  }
  syncFibStatus();
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, strategy, timeframe: tf });
});

app.get('/api/manual', (req, res) => res.json(manualView()));

// Price preview for the ticket when the phone cannot reach Binance Futures directly.
app.get('/api/manual/quote', async (req, res) => {
  const s = String(req.query.symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!perpetuals.has(s)) return res.status(400).json({ error: 'not a USD-M perpetual' });
  const q = await futuresQuote(s);
  res.json(q ? { symbol: s, bid: q.bid, ask: q.ask } : { error: 'no price' });
});

app.get('/api/manual/symbols', async (req, res) => {
  await refreshPerpetuals().catch(() => {});
  res.json({ status: State.subsystems.futures.status, symbols: [...perpetuals.keys()].sort() });
});

app.post('/api/manual/open', async (req, res) => {
  const symbol = String(req.body.symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const sym = symbol.endsWith('USDT') ? symbol : `${symbol}USDT`;
  const side = String(req.body.side || '').toUpperCase();
  const notional = Math.floor(parseFloat(req.body.notionalUSDT) * 100) / 100;
  const tp = Strategy.manualTakeProfit(req.body.takeProfitPct);
  if (!tp.ok) return res.status(400).json({ success: false, error: tp.reason });
  await refreshPerpetuals().catch(() => {});
  if (!perpetuals.has(sym)) return res.status(400).json({ success: false, error: `${sym} is not a trading USD-M perpetual on Binance` });
  const minN = Math.max(Strategy.MANUAL.minNotional, perpetuals.get(sym).minNotional || 5);
  const q = await futuresQuote(sym);
  if (!q) return res.status(503).json({ success: false, error: 'No live futures price right now. Nothing was opened.' });
  const price = side === 'SHORT' ? q.bid : q.ask;
  const plan = Strategy.manualOpenPlan(Number(State.manual.cashUSDT), notional, price, side, Strategy.FUTURES, minN);
  if (!plan.ok) return res.status(400).json({ success: false, error: plan.reason });
  const id = `m${Date.now()}${Math.floor(Math.random() * 1000)}`;
  State.manual.cashUSDT = Number((Number(State.manual.cashUSDT) - plan.cost).toFixed(6));
  State.manual.positions[id] = { id, manual: true, symbol: sym, pair: `${sym.replace(/USDT$/, '')}/USDT`, side, market: 'FUTURES', leverage: Strategy.FUTURES.leverage,
    openedAt: Date.now(), entryPriceUSDT: price, livePriceUSDT: price, lastTickAt: Date.now(), qty: plan.qty, notionalUSDT: plan.notional,
    openFeeUSDT: Number((plan.cost - plan.notional).toFixed(6)), liquidationPrice: plan.liquidationPrice, fundingUSDT: 0, nextFundingAt: 0, takeProfitPct: tp.pct };
  State.manual.positions[id].takeProfitPrice = Strategy.manualTakeProfitPrice(State.manual.positions[id]);
  logOperation('MANUAL', `Opened ${side} ${sym} @ $${formatPrecisePrice(price)} ($${plan.notional.toFixed(2)}, 1x USD-M, paper, manual account)${tp.pct ? `, closes at +${tp.pct}%` : ''}`, 'success');
  saveDiskState();
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
  res.json({ success: true, position: State.manual.positions[id], manual: manualView() });
});

app.post('/api/manual/close', async (req, res) => {
  const p = State.manual.positions[req.body.id];
  if (!p) return res.status(404).json({ success: false, error: 'Position not found' });
  const q = await futuresQuote(p.symbol);
  if (!q) return res.status(503).json({ success: false, error: 'No live futures price right now. Your position is still open.' });
  const trade = closeManual(p.id, p.side === 'SHORT' ? q.ask : q.bid, 'MANUAL CLOSE');
  res.json({ success: Boolean(trade), trade, manual: manualView() });
});

app.post('/api/manual/tp', (req, res) => {
  const p = State.manual.positions[req.body.id];
  if (!p) return res.status(404).json({ success: false, error: 'Position not found' });
  const tp = Strategy.manualTakeProfit(req.body.takeProfitPct);
  if (!tp.ok) return res.status(400).json({ success: false, error: tp.reason });
  p.takeProfitPct = tp.pct; p.takeProfitPrice = Strategy.manualTakeProfitPrice(p);
  logOperation('MANUAL', `${p.pair} ${p.side}: ${tp.pct ? `closes automatically at +${tp.pct}% ($${formatPrecisePrice(p.takeProfitPrice)})` : 'take profit off'}.`, 'info');
  saveDiskState();
  broadcastStateUpdate();
  if (tp.pct && p.livePriceUSDT) checkManualTakeProfit(p.symbol, Number(p.livePriceUSDT)).catch(() => {});
  res.json({ success: true, position: p, manual: manualView() });
});

// v6.30: Settings > Reset paper account. Every open autopilot position and bag is removed without being recorded as a trade
// (none was closed, so nothing goes to the sheet), and cash goes back to the starting amount with no realized P&L.
// Closed trades, the sheet, the manual account and every setting stay as they are.
app.post('/api/account/reset', (req, res) => {
  if (req.body.confirm !== 'RESET') return res.status(400).json({ success: false, error: 'confirm must be RESET' });
  const pos = Object.keys(State.activePositions || {}), bags = Object.keys(State.recoveryBags || {}), before = computeEquity();
  State.activePositions = {};
  State.recoveryBags = {};
  if (State.fib && State.fib.portfolio) State.fib.portfolio.positions = {};
  State.portfolio = { ...State.portfolio, cashUSDT: STARTING_EQUITY_USDT, holdings: {}, realizedPnL: 0, bagSubsidyPoolUSDT: 0 };
  const names = [...pos, ...bags.map((k) => `${k} (bag)`)].map((k) => k.replace('USDT', '')).join(', ');
  logOperation('ACCOUNT', `Paper account reset: ${pos.length} open position${pos.length === 1 ? '' : 's'} and ${bags.length} bag${bags.length === 1 ? '' : 's'} removed without being recorded${names ? ` (${names})` : ''}. Value was $${before.toFixed(2)}; cash is now $${STARTING_EQUITY_USDT.toFixed(2)}. Closed trades and settings are unchanged.`, 'warning');
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, removedPositions: pos, removedBags: bags, valueBefore: Number(before.toFixed(2)), cashUSDT: STARTING_EQUITY_USDT });
});

app.post('/api/manual/reset', (req, res) => {
  if (Object.keys(State.manual.positions || {}).length) return res.status(400).json({ success: false, error: 'Close your open manual positions first' });
  State.manual = { startUSDT: MANUAL_START_USDT, cashUSDT: MANUAL_START_USDT, realizedPnL: 0, positions: {}, history: [] };
  saveDiskState();
  broadcastStateUpdate();
  res.json({ success: true, manual: manualView() });
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

// v6.11: the P&L statement the app shows (Realized P&L, Take-home after tax, the statement sheet), over every trade the
// engine holds, with when the Google Sheet ledger was last read.
// v6.31: what the characters may comment on, as plain numbers (no URLs, sheet ids or personal details).
function aiSnapshot(now = Date.now()) {
  const r2 = (x) => (Number.isFinite(Number(x)) ? Math.round(Number(x) * 100) / 100 : null);
  const coin = (pair, k) => String(pair || k || '').replace('/USDT', '').replace(/USDT$/, '').slice(0, 20);
  const move = (side, e, l) => { e = Number(e) || 0; l = Number(l) || e; return e ? ((side === 'SHORT' ? e - l : l - e) / e) * 100 : 0; };
  const mins = (t) => (Number(t) > 0 ? Math.max(0, Math.round((now - Number(t)) / 60000)) : null);
  const words = (m) => (m == null ? null : m < 90 ? `${m} min` : m < 2880 ? `${Math.round(m / 60)} hours` : `${Math.round(m / 1440)} days`);
  const open = (k, p, kind) => ({
    key: k, kind, coin: coin(p.pair, k), side: isShort(p) ? 'SHORT' : 'LONG', gainPct: r2(move(p.side, p.entryPriceUSDT, p.livePriceUSDT)),
    openedAt: Number(p.openedAt) || 0, openFor: words(mins(p.openedAt)), leverage: r2(Number(p.leverage) || 1),
    ...(kind === 'bag' ? { inBagFor: words(mins(p.movedAt)) } : { exitStage: p.ratchetStage || null }),
    ...(Number(p.userTpPct) > 0 || Number(p.takeProfitPct) > 0 ? { ownTakeProfitPct: Number(p.userTpPct || p.takeProfitPct) } : {}),
    ...(p.momentum && Number.isFinite(Number(p.momentum.s)) ? { momentumAtEntry: `${p.momentum.d} ${p.momentum.s}` } : {})   // v6.36
  });
  const positions = Object.entries(State.activePositions || {}).map(([k, p]) => open(k, p, 'position'));
  const bags = Object.entries(State.recoveryBags || {}).map(([k, p]) => open(k, p, 'bag'));
  const manual = Object.values((State.manual && State.manual.positions) || {}).map((p) => open(String(p.id), p, 'manual position'));
  const closed = [
    // the key is the app's own: its numeric id and its type in capitals (the app renumbers any other id)
    ...(State.tradeLogs || []).filter((t) => t && Number(t.id) > 0).slice(0, 20).map((t) => {
      const type = String(t.type || 'EXIT').trim().toUpperCase(), side = t.side === 'SHORT' || /^SHORT/.test(type) ? 'SHORT' : 'LONG';
      return { key: `${Number(t.id)}_${type}`, coin: coin(t.pair), side, how: type.replace(/^(LONG|SHORT)\s+/, '').slice(0, 60), pnlUSDT: r2(t.pnlUSDT),
        gainPct: r2(move(side, t.entryPriceUSDT, t.exitPriceUSDT)), heldFor: words(Number(t.holdMinutes) || null) };
    }),
    ...(((State.manual && State.manual.history) || []).slice(0, 5)).map((t) => ({ key: `m${t.id}`, coin: coin(t.pair), side: t.side === 'SHORT' ? 'SHORT' : 'LONG',
      how: `manual: ${String(t.type || 'closed').slice(0, 40)}`, pnlUSDT: r2(t.pnlUSDT), gainPct: r2(t.gainPct), heldFor: words(Number(t.holdMinutes) || null) }))
  ];
  const minScore = (State.activeThresholds && State.activeThresholds.scoreThreshold) || 65;
  // v6.36: more for Sergeant Rook's opinions: where each coin sits in its day range, its momentum reading, whether we hold it
  const moCoins = (MomentumReport.report() || {}).coins || {};
  const radar = Object.entries(State.universe || {}).filter(([, c]) => c && c.symbol && c.symbol !== 'BTC')
    .sort((a, b) => (Number(b[1].score) || 0) - (Number(a[1].score) || 0)).slice(0, 8)
    .map(([k, c]) => {
      const mo = moCoins[String(c.symbol).toUpperCase()];
      return { coin: c.symbol, price: Number(c.priceUSDT) || null, change24hPct: r2(c.change24h), score: Math.round(Number(c.score) || 0), side: c.side,
        qualifies: (Number(c.score) || 0) >= minScore && !c.isPumpAndDump, trap: Boolean(c.isPumpAndDump), dayRangePct: r2(c.intradayRangePct),
        whereInDayRangePct: Number.isFinite(Number(c.pricePositionInRange)) ? Math.round(Number(c.pricePositionInRange) * 100) : null,
        ...(mo ? { momentum: `${mo.d} ${mo.s}`, momentumWhy: mo.w || undefined } : {}),
        weHold: (State.activePositions || {})[k] ? 'open position' : (State.recoveryBags || {})[k] ? 'recovery bag' : null };
    });
  const mm = State.subsystems.marketMode || {}, btc = (State.universe || {}).BTCUSDT;
  const market = mm.mode ? {
    mode: mm.mode, newTrades: mm.tradeSide, direction: State.sideMode === 'SHORT' ? 'shorts only (the user\'s setting)' : 'shorts and longs',
    reason: String(mm.reason || '').slice(0, 160), coinsAboveEma50Pct: mm.breadth == null ? null : Math.round(mm.breadth * 100),
    btc: r2(mm.btcClose), btcVsEma50Pct: Number(mm.ema50) > 0 ? r2((mm.btcClose / mm.ema50 - 1) * 100) : null,
    btcEma20AboveEma50: Number(mm.ema20) > 0 && Number(mm.ema50) > 0 ? mm.ema20 > mm.ema50 : null,
    btc24hPct: btc ? r2(btc.change24h) : null, lastTrend: mm.lastTrend || null,
    // v6.33: Professor Quill also reads the hourly momentum report's market view and news
    ...(MomentumReport.report() ? { momentumBias: MomentumReport.report().market.bias, news: (MomentumReport.report().market.news || []).slice(0, 3) } : {})
  } : null;
  const last = (State.tradeLogs || []).slice(0, 20);
  const account = {
    valueUSDT: r2(computeEquity()), cashUSDT: r2(State.portfolio.cashUSDT), openPositions: positions.length, bags: bags.length,
    last20Trades: { wins: last.filter((t) => Number(t.pnlUSDT) > 0).length, losses: last.filter((t) => Number(t.pnlUSDT) < 0).length, netUSDT: r2(last.reduce((a, t) => a + (Number(t.pnlUSDT) || 0), 0)) }
  };
  // v6.37: what Lady Seraphina, the Royal Oracle, presents: the hourly momentum report in short
  const rep = MomentumReport.report();
  let oracle = null;
  if (rep && rep.coins) {
    const all = Object.entries(rep.coins).map(([coin, e]) => ({ coin, s: e.s, d: e.d, why: e.w || undefined }));
    const by = (d) => all.filter((x) => x.d === d).sort((a, b) => b.s - a.s);
    oracle = { reportAt: rep.at, minutesAgo: Math.max(0, Math.round((now - rep.at) / 60000)), bias: rep.market ? rep.market.bias : null,
      counts: { bearish: by('bear').length, bullish: by('bull').length, flat: by('flat').length },
      strongestBearish: by('bear').slice(0, 6).map(({ d, ...x }) => x), strongestBullish: by('bull').slice(0, 3).map(({ d, ...x }) => x),
      news: ((rep.market && rep.market.news) || []).slice(0, 5) };
  }
  return { now, positions, bags, manual, closed, radar, market, account, oracle };
}
function syncAiStatus() { State.subsystems.ai = Commentary.status(); }

function pnlNow() {
  return { ...Strategy.pnlStatement(State.tradeLogs), ledger: { ...State.ledgerSync } };
}

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
    equityUSDT: computeEquity(),
    manual: manualView(),
    pnl: pnlNow()
  };
  if (withUniverse) payload.universe = State.universe;
  sendAll(JSON.stringify(payload, omitTickStamps));
  lastDetailSig = JSON.stringify({ ...State.activePositions, ...State.recoveryBags }, omitLive); // this frame already carried them
  lastSentPrices = Object.fromEntries(Object.entries({ ...State.activePositions, ...State.recoveryBags }).map(([k, p]) => [k, p.livePriceUSDT]));
}

// Every 250 ms the app gets only the prices that moved (about 60 bytes). Position details such
// as trail levels change far less often, so they go at most every 2 seconds, and only if changed.
let lastSentPrices = {};
const lastManualPx = {};
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
  const moved = {}, m = {};
  Object.entries(held).forEach(([k, p]) => { if (p.livePriceUSDT !== lastSentPrices[k]) moved[k] = lastSentPrices[k] = p.livePriceUSDT; });
  Object.values(State.manual.positions || {}).forEach((p) => { if (p.livePriceUSDT !== lastManualPx[p.id]) m[p.id] = lastManualPx[p.id] = p.livePriceUSDT; });
  if (Object.keys(moved).length || Object.keys(m).length) sendAll(JSON.stringify(Object.keys(m).length ? { type: 'P', p: moved, m } : { type: 'P', p: moved }));
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
const heldFutures = () => [...new Set([...Object.entries(State.activePositions || {}), ...Object.entries(State.recoveryBags || {})].filter(([, p]) => isFutures(p)).map(([k]) => k)
  .concat(Object.values(State.manual.positions || {}).map((p) => p.symbol)))];

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
        Object.values(State.manual.positions || {}).forEach((p) => { if (p.symbol === t.s) { p.livePriceUSDT = price; p.lastTickAt = Date.now(); } });
        checkManualLiquidation(t.s, price);
        checkManualTakeProfit(t.s, price).catch(() => {});
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
    riskConfig: Strategy.CONFIG,
    manual: manualView(),
    pnl: pnlNow()
  }, omitTickStamps));

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (tradingPaused() && msg.type !== 'SYNC_STATE') return;   // v6.15: no changes while paused
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
// v6.15: the saved file, then the account of the previous build if one still answers the public URL (it wins: it is
// live). Only then does this build open its port (Render moves traffic here once it does) and start trading.
async function boot() {
  const hadFile = fs.existsSync(DISK_STATE_FILE);
  loadDiskState();
  const t0 = Date.now();
  await takeOverFromPreviousBuild();
  applyCapitalReset();
  if (Handover.restoredAt) saveDiskState();
  syncBreakdownStatus();
  syncSizingStatus();
  syncLeverageStatus();
  syncSidesStatus();
  syncFibStatus();   // v6.23
  Handover.ready = true;
  const n = Object.keys(State.activePositions || {}).length, b = Object.keys(State.recoveryBags || {}).length;
  const what = `${n} open trade${n === 1 ? '' : 's'}, ${b} bag${b === 1 ? '' : 's'}, free cash $${Number(State.portfolio.cashUSDT).toFixed(2)}`;
  if (Handover.how === 'handoff') logOperation('HANDOVER', `New build: took over from the previous build (${what}). Trading was paused for ${Math.max(1, Math.round((Date.now() - t0) / 1000))} s during the switch.`, 'success');
  else if (Handover.how === 'state') logOperation('HANDOVER', `New build: took over from the previous build (${what}). That build is from before v6.15 and keeps trading until Render stops it, about a minute: a trade it closes in that minute can show twice in the sheet.`, 'warning');
  else if (HANDOFF_URL) logOperation('HANDOVER', `New build: ${Handover.note || 'no previous build answered'}. Started from ${hadFile ? 'the saved file' : 'a fresh account'} (${what}).`, 'warning');
  console.log(`[Handover] boot ${BOOT_ID}: ${Handover.how}${Handover.note ? ` (${Handover.note})` : ''}`);
  startServerAndTimers();
}

let serverStarted = false;
function startServerAndTimers() {
if (serverStarted) return;
serverStarted = true;
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

// The engine writes its own trades, so this picks up edits and (v6.16) deletions made in the sheet.
setInterval(() => {
  syncTradesFromGoogleSheets().catch(() => {});
}, 600000);

setInterval(async () => {
  if (tradingPaused()) return;
  try {
    await runPredictiveSentry();
    topUpBags();   // v6.19: leveraged bags get freed cash before new trades
    await evaluateAutopilotEntries();
    syncActiveSymbolSubscriptions(); // add ticks for new buys, drop them for sold coins
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 1000);

setInterval(() => { settleFunding().catch((e) => console.error('[Funding]', e.message)); }, 60000);
// v6.31: Gemini writes what is due for the characters (at most one call a minute); the app reads it when subsystems.ai.rev changes
syncAiStatus();
setInterval(() => {
  if (tradingPaused()) return;
  Commentary.tick(aiSnapshot()).then((changed) => {
    const before = State.subsystems.ai && State.subsystems.ai.rev;
    syncAiStatus();
    if (changed || before !== State.subsystems.ai.rev) { saveDiskState(); broadcastStateUpdate(); }
  }).catch((e) => console.error('[AI]', e.message));
}, 20000);
// v6.33: the hourly momentum report (hh:02 UTC; a failed one is tried again 5 minutes later)
syncMomentumStatus();
setInterval(() => {
  if (tradingPaused() || !State.momentumOn) return;
  MomentumReport.tick(momentumRows).then((ok) => { syncMomentumStatus(); if (ok) { saveDiskState(); broadcastStateUpdate(); } }).catch((e) => console.error('[Momentum]', e.message));
}, 30000);
setInterval(() => { fibTick().catch(() => {}); }, 5000);   // v6.23: the Fibonacci autopilot, once per closed bar
setInterval(async () => {
  if (tradingPaused()) return;
  for (const p of Object.values(State.manual.positions || {})) {
    if (Date.now() - Number(p.lastTickAt || 0) < 15000) continue;
    const px = await fetchFuturesPrice(p.symbol);
    if (px) { p.livePriceUSDT = px; p.lastTickAt = Date.now(); checkManualLiquidation(p.symbol, px); await checkManualTakeProfit(p.symbol, px).catch(() => {}); }
  }
}, 10000);
setInterval(broadcastPriceStream, 250);
setInterval(() => { refreshExitSignals().catch(() => {}); }, 3000);
setInterval(runDynamicScreener, 60000);
}

// Render stops an old instance with SIGTERM (about a minute after the new build goes live): stop trading and save.
process.on('SIGTERM', () => {
  Handover.frozen = true;
  saveDiskState();
  console.log('[Handover] SIGTERM: trading stopped, state saved.');
  setTimeout(() => process.exit(0), 1000);
});

boot().catch((e) => {
  console.error('[Boot]', e && e.message);
  Handover.ready = true;
  startServerAndTimers();
});
