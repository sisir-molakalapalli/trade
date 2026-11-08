/**
 * =========================================================================
 * SISIR QUANT TERMINAL - BACKEND ENGINE v5.0 (RISK-FIRST REBUILD)
 * =========================================================================
 * PAPER TRADING ENGINE: positions are simulated against live Binance prices.
 * No orders are sent to Binance from this file.
 *
 * What changed from v4.3 and why (full notes in CHANGES.md):
 *  - Every trade has a real stop (0.6%..1.5%) from the moment it opens.
 *    v4 never sold below +0.35%, parked losers as "recovery bags" and only cut
 *    them at -3.80% -> that rule is what produced the hard-disaster cuts.
 *  - The v4 breakeven/profit locks could not fire (zero-width trigger window).
 *  - Size is set from the stop: one stop-out = 0.5% of equity.
 *  - Exits in R: breakeven at +1R, 50% off at +1.5R, trail, full exit at +3R,
 *    time-stop if a trade does nothing for 90 minutes.
 *  - Entries use CLOSED candles only, require a 1h uptrend + healthy BTC,
 *    skip coins too volatile for the stop, never chase. Missing data = no trade.
 *  - Daily loss limit (-2%), pause after 3 losses in a row, 4h cooldown on a
 *    coin after it loses, learning-engine blacklist actually enforced.
 *  - Fixed: double-run sentry, TP1 cash leak, fake $500 cash refills,
 *    fees missing from P&L.
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
const STARTING_EQUITY_USDT = Number(process.env.STARTING_EQUITY_USDT) || 21;
const CAPITAL_RESET_ID = process.env.CAPITAL_RESET_ID || 'equity-21usd-2026-11-07';

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
    btcMacroGate: { status: 'NORMAL', allowAltcoinEntries: true, btcChange15m: 0, reason: 'BTC macro healthy' },
    riskGuard: { status: 'ARMED', reason: 'Awaiting first evaluation', equityUSDT: 0, riskPerTradePct: 0.5, dailyLossLimitPct: 2.0 },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 65, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'v5: Hard stop 0.6-1.5% • BE at +1R • TP1 50% at +1.5R • Trail • Exit +3R • 90m time-stop' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'v5: no new bags. Legacy bags exit at breakeven+fees or -3.80%', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
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
    maxConcurrentPositions: 3,
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
async function getCachedKlines(symbol, interval, limit, timeoutMs = 3500, maxAgeMs = 0) {
  const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
  const cacheKey = `${pairSym}_${interval}_${limit}`;
  const now = Date.now();
  const cached = klineCache.get(cacheKey);

  // Valid until the open candle closes (so a newly CLOSED bar is picked up within ~1s,
  // faster than the old fixed 20s), but never older than 5 minutes.
  if (cached && now < cached.expiresAt && (!maxAgeMs || now - cached.timestamp < maxAgeMs)) {
    return cached.data;
  }

  const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=${interval}&limit=${limit}`;
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

let btcRegimeCache = { at: 0, result: null };
// BTC Macro Weather Gate (see strategy.evaluateBtcRegime): live BTCUSDT 15m candle,
// klines limit 3 through a 20-second memo cache, sampled by the 1s autopilot loop. Fail-open.
async function refreshBtcRegime() {
  let m = null;
  try { m = await getCachedKlines('BTCUSDT', '15m', 3, 3000, 20000); } catch (e) { m = null; }
  const r = Strategy.evaluateBtcRegime(null, Array.isArray(m) ? m : null);
  const g = State.subsystems.btcMacroGate;
  g.status = r.ok ? 'NORMAL' : 'PAUSED';
  g.allowAltcoinEntries = r.ok;
  g.btcChange15m = Number((r.chg15 || 0).toFixed(2));
  g.btcOpen15m = r.btcOpen15 || null;
  g.reason = r.ok ? (r.failOpen ? 'BTC data unavailable (gate fails open)' : 'BTC macro healthy') : r.reason;
  btcRegimeCache = { at: Date.now(), result: r };
  return r;
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
    eq += qty * px;
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

async function fetchRealOrderBookDepth(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/depth?symbol=${pairSym}&limit=20`;
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

async function runDynamicScreener() {
  try {
    const url = 'https://api.binance.com/api/v3/ticker/24hr';
    const tickers = await safeFetch(url, 4500);
    if (!Array.isArray(tickers)) return;

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

      let score = 60 + (posInRange * 22) + (Math.min(change24, 15) * 0.9);
      if (volQuote >= 10000000) score += 5;
      if (rangePct >= 5.0) score += 3;

      if (isPumpDump) {
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
        obi: 0.55,
        isPumpAndDump: isPumpDump,
        pumpDumpReason: pumpDumpReason,
        candlestickPattern: 'WATCH'
      };

      if (!isPumpDump && score >= 60 && candidateKlinesQueue.length < 35) {
        candidateKlinesQueue.push(sym);
      }
    }

    // Inspect candlestick patterns on top candidates to award Steve Nison momentum bonuses
    for (const topSym of candidateKlinesQueue) {
      try {
        // 100 bars = same cache key the entry evaluator uses; patterns on CLOSED bars only
        const klines15m = await getCachedKlines(topSym, '15m', 100, 2000);
        const pat = Strategy.analyzeCandlestickPattern(Strategy.closedOnly(klines15m));
        if (universe[topSym]) {
          // Penalize stale 24h gainers that are actively bleeding in the 1h timeframe
          if (Array.isArray(klines15m) && klines15m.length >= 5) {
            const open1h = parseFloat(klines15m[klines15m.length - 5][1]);
            const closeNow = parseFloat(klines15m[klines15m.length - 1][4]);
            const change1h = open1h > 0 ? ((closeNow - open1h) / open1h) * 100 : 0;
            if (change1h < -0.30) {
              universe[topSym].score = Math.max(0, universe[topSym].score - 12);
            }
          }

          if (pat.bullishPatternName !== 'NONE') {
            universe[topSym].candlestickPattern = pat.bullishPatternName;
            const rvolBonus = (pat.patternRvol >= 1.25) ? 5 : 0;
            universe[topSym].score = Math.min(99, universe[topSym].score + 10 + rvolBonus);
          } else if (pat.bearishPatternName !== 'NONE') {
            universe[topSym].candlestickPattern = `⚠️ ${pat.bearishPatternName}`;
            universe[topSym].score = Math.max(0, universe[topSym].score - 18);
          }
        }
      } catch (err) {}
    }

    State.universe = universe;
    State.subsystems.screener.scannedPairsCount = Object.keys(universe).length;
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= (State.activeThresholds?.scoreThreshold || 65) && !c.isPumpAndDump).length;

    logOperation('RADAR_SCREENER', `Scanned ${Object.keys(universe).length} pairs. ${State.subsystems.screener.qualifiedCandidatesCount} candidates qualified (Score >= ${State.activeThresholds?.scoreThreshold || 65}). Candlestick Sentinel active.`, 'info');
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

function openPosition(pairKey, signal, plan, takerBuyRatio) {
  const sym = pairKey.replace(/USDT$/, '');
  const entry = plan.entryPrice;
  const R = entry - plan.stopPrice;
  const cost = plan.notional * (1 + FEE_RATE);
  if (State.portfolio.cashUSDT < cost) {
    logOperation('CASH_GUARD', `Cannot open ${sym}/USDT: need $${cost.toFixed(2)}, free cash $${State.portfolio.cashUSDT.toFixed(2)}.`, 'danger');
    return false;
  }
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - cost).toFixed(6));

  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`,
    pairSymbol: sym,
    strategyVersion: 5,
    openedAt: Date.now(),
    entryPriceUSDT: entry,
    livePriceUSDT: entry,
    lastTickAt: Date.now(),
    qty: plan.qty,
    notionalUSDT: plan.notional,
    initialNotionalUSDT: plan.notional,
    initialStopPrice: plan.stopPrice,
    sl: plan.stopPrice,
    stopPct: Number(plan.stopPct.toFixed(3)),
    riskUSDT: plan.riskUSDT,
    atrPct: Number(signal.atrPct.toFixed(3)),
    breakevenPrice: entry + Strategy.CONFIG.breakevenAtR * R,
    tp1Price: entry + Strategy.CONFIG.tp1AtR * R,
    targetPrice: entry + Strategy.CONFIG.finalTargetR * R,
    peakGainPct: 0,
    peakHighPrice: entry,
    ratchetStage: 'INITIAL',
    tp1Secured: false,
    candlestickPattern: signal.pattern
  };

  logOperation('EXECUTION',
    `BOUGHT ${sym}/USDT @ $${formatPrecisePrice(entry)} ($${plan.notional.toFixed(2)}) via [${signal.pattern}] • ` +
    `Stop $${formatPrecisePrice(plan.stopPrice)} (-${plan.stopPct.toFixed(2)}%, risk $${plan.riskUSDT.toFixed(2)}) • ` +
    `TP1 $${formatPrecisePrice(entry + 1.5 * R)} • Target $${formatPrecisePrice(entry + 3 * R)} • RVOL ${signal.rvol}x • Taker ${takerBuyRatio}%`,
    'success');
  console.log(`[Order] BUY ${pairKey} @ ${entry} notional ${plan.notional} stop ${plan.stopPrice}`);
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
  const totalQty = Number(pos.qty) || (Number(pos.notionalUSDT) / entry);
  const qtySold = fraction >= 1 ? totalQty : totalQty * fraction;
  const proceeds = qtySold * price * (1 - FEE_RATE);
  const pnl = Strategy.netPnl(qtySold, entry, price);

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

  const gainPct = ((price - entry) / entry) * 100;
  const tradePayload = {
    id: Date.now(),
    pair: pos.pair || `${key.replace('USDT', '')}/USDT`,
    type: typeLabel,
    entryPriceUSDT: entry,
    exitPriceUSDT: price,
    notionalUSDT: Number((qtySold * entry).toFixed(4)),
    pnlUSDT: Number(pnl.toFixed(4)),
    holdMinutes: Math.round((Date.now() - Number(pos.openedAt || Date.now())) / 60000),
    feesIncluded: true, // v5 P&L is already net of both 0.10% fees
    strategyVersion: 5,
    reason: reasonText
  };

  logOperation(pnl >= 0 ? 'SENTRY_EXIT' : 'RISK_STOP',
    `${typeLabel} on ${tradePayload.pair} @ $${formatPrecisePrice(price)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%) • ` +
    `Net ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)} after fees${fullyClosed ? '' : ' • runner continues'}`,
    pnl >= 0 ? 'success' : 'warning');

  saveDiskState();
  broadcastStateUpdate();
  recordTradeToCloudLedger(tradePayload).catch((e) => console.error('[Ledger Dispatch Notice]', e.message));
  return tradePayload;
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (v5: closed candles, fail-closed, risk-sized)
// -------------------------------------------------------------
let isEvaluatingEntries = false;
const noticeTimes = {};
const candidateCooldownMemo = new Map();

function throttledNotice(key, category, message, level = 'info', everyMs = 60000) {
  if (Date.now() - (noticeTimes[key] || 0) < everyMs) return;
  noticeTimes[key] = Date.now();
  logOperation(category, message, level);
}

function vetoCategory(reason) {
  const r = String(reason);
  if (r.includes('bearish')) return 'CANDLE_VETO';
  if (r.includes('no closed-bar trigger')) return 'CANDLE_WAIT';
  if (r.includes('ATR') || r.includes('stop')) return 'VOL_VETO';
  if (r.includes('EMA') || r.includes('uptrend') || r.includes('overextended')) return 'TREND_VETO';
  return 'MOM_VETO';
}

async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled || isEvaluatingEntries) return;
  const cfg = Strategy.CONFIG;

  const openCount = Object.keys(State.activePositions || {}).length;
  if (openCount >= cfg.maxConcurrent) {
    throttledNotice('slots', 'AUTOPILOT', `All ${cfg.maxConcurrent} slots in use. Managing open positions.`);
    return;
  }

  const equity = computeEquity();
  const guard = Strategy.portfolioGuard(State.tradeLogs, equity);
  State.subsystems.riskGuard = {
    status: guard.ok ? 'ARMED' : 'TRADING PAUSED',
    reason: guard.ok ? `Day P&L ${guard.dayPnl >= 0 ? '+' : ''}$${(guard.dayPnl || 0).toFixed(2)}` : guard.reason,
    equityUSDT: Number(equity.toFixed(2)),
    riskPerTradePct: cfg.riskPerTradePct,
    dailyLossLimitPct: cfg.dailyLossLimitPct
  };
  if (!guard.ok) {
    throttledNotice('guard', 'CASH_GUARD', guard.reason, 'danger', 300000);
    return;
  }

  isEvaluatingEntries = true;
  try {
    const btc = await refreshBtcRegime();
    if (!btc.ok) {
      throttledNotice('btc', 'BTC_WEATHER', `Entries paused: ${btc.reason}`, 'warning');
      return;
    }

    const minScore = State.activeThresholds?.scoreThreshold || 65;
    const now = Date.now();
    const sorted = Object.keys(State.universe || {})
      .sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

    let evaluated = 0;
    for (const k of sorted) {
      if (Object.keys(State.activePositions).length >= cfg.maxConcurrent) break;
      const coin = State.universe[k];
      if (!coin || !coin.symbol || coin.isPumpAndDump) continue;
      if ((coin.score || 0) < minScore) break;

      const pairKey = `${String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '')}USDT`;
      if (pairKey === 'BTCUSDT') continue;
      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;
      if ((candidateCooldownMemo.get(pairKey) || 0) > now) continue;
      if (Strategy.inSymbolCooldown(State.tradeLogs, `${coin.symbol}/USDT`, now)) continue;

      const elig = learningEngine.evaluateCandidateEligibility
        ? learningEngine.evaluateCandidateEligibility(pairKey)
        : { allowed: true };
      if (!elig.allowed) {
        candidateCooldownMemo.set(pairKey, now + 30 * 60000);
        throttledNotice(`rep_${pairKey}`, 'TRAP_GUARD', `${pairKey} skipped: ${elig.reason}`, 'warning', 600000);
        continue;
      }

      if (++evaluated > 6) break; // keep Binance request weight low
      // A signal is tied to a closed 15m bar, so re-checking a rejected coin
      // more often than once a minute only burns rate limit.
      candidateCooldownMemo.set(pairKey, now + 60000);

      const volQuote = (coin.volumeM || 0) * 1e6;
      const change24h = Number(coin.change24h || 0);
      const rangePct = Number(coin.intradayRangePct || 0);
      if (change24h > 20) {
        throttledNotice(`chase_${pairKey}`, 'TRAP_GUARD', `${pairKey} skipped: already +${change24h.toFixed(1)}% in 24h (late-stage pump).`, 'warning', 600000);
        continue;
      }
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pd = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pd.isTrap) {
          candidateCooldownMemo.set(pairKey, now + 30 * 60000);
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pd.reason}`, 'danger');
          continue;
        }
      }

      const [k15, k1h] = await Promise.all([
        getKlinesStrict(pairKey, '15m', 100),
        getKlinesStrict(pairKey, '1h', 100)
      ]);
      if (!k15 || !k1h) continue; // fail closed

      const flow = await checkTakerFlow(pairKey);
      if (!flow.passed) {
        throttledNotice(`flow_${pairKey}`, 'FLOW_VETO', `${pairKey}: ${flow.reason}`, 'warning', 300000);
        continue;
      }

      const signal = Strategy.evaluateEntry({ k15, k1h, k5: flow.k5 });
      if (!signal.ok) {
        throttledNotice(`sig_${pairKey}`, vetoCategory(signal.reason), `${pairKey}: ${signal.reason}`, 'info', 300000);
        continue;
      }

      const book = await fetchRealOrderBookDepth(pairKey);
      if (!book) continue; // fail closed
      if (book.obi < 0.45 || book.spreadPct > 0.15) {
        logOperation('BOOK_VETO', `${pairKey}: OBI ${Math.round(book.obi * 100)}% (need >=45%) / spread ${book.spreadPct.toFixed(3)}% (need <=0.15%).`, 'warning');
        continue;
      }

      // Buy at the ASK (what a market order actually pays)
      const plan = Strategy.planEntry(signal, book.bestAsk, computeEquity(), Number(State.portfolio.cashUSDT));
      if (!plan.ok) {
        logOperation('MOM_VETO', `${pairKey}: ${plan.reason}`, 'info');
        continue;
      }

      logOperation('BOOK_CLEAR', `${pairKey} [${signal.pattern}] passed all gates: OBI ${Math.round(book.obi * 100)}%, spread ${book.spreadPct.toFixed(3)}%, ATR ${signal.atrPct.toFixed(2)}%.`, 'success');
      openPosition(pairKey, signal, plan, flow.takerBuyRatio);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// -------------------------------------------------------------
// POSITION SENTRY (v5): one runner at a time, unconditional stops
// -------------------------------------------------------------
let sentryBusy = false;

function migrateLegacyPosition(k, pos) {
  if (pos.strategyVersion === 5 && pos.initialStopPrice) return;
  // v4 positions had sl ABOVE entry and no real stop. Give them the widest v5 stop.
  const entry = Number(pos.entryPriceUSDT);
  pos.initialStopPrice = entry * (1 - Strategy.CONFIG.maxStopPct / 100);
  pos.sl = pos.tp1Secured ? Math.max(pos.initialStopPrice, entry * (1 + 2 * FEE_RATE)) : pos.initialStopPrice;
  pos.ratchetStage = pos.tp1Secured ? 'RUNNER_TRAIL' : 'INITIAL';
  pos.atrPct = pos.atrPct || 0.8;
  pos.stopPct = Strategy.CONFIG.maxStopPct;
  pos.qty = Number(pos.qty) || Number(pos.notionalUSDT) / entry;
  const R = entry - pos.initialStopPrice;
  pos.tp1Price = entry + Strategy.CONFIG.tp1AtR * R;
  pos.targetPrice = entry + Strategy.CONFIG.finalTargetR * R;
  pos.strategyVersion = 5;
  logOperation('SENTRY', `${pos.pair}: migrated v4 position to v5 risk rules (stop $${formatPrecisePrice(pos.sl)}).`, 'warning');
}

async function livePriceFor(k, pos) {
  const tickPrice = Number(pos.livePriceUSDT || State.universe[k]?.priceUSDT);
  if (tickPrice > 0 && Date.now() - Number(pos.lastTickAt || 0) < 90000) return tickPrice;
  // Stream went quiet for this symbol: never manage risk on a stale price.
  const rest = await fetchTickerPrice(k);
  if (rest) {
    pos.livePriceUSDT = rest;
    pos.lastTickAt = Date.now();
    return rest;
  }
  return tickPrice > 0 ? tickPrice : null;
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

      const prevStage = pos.ratchetStage;
      const r = Strategy.managePosition(pos, price);
      if (pos.ratchetStage !== prevStage && pos.ratchetStage === 'BREAKEVEN') {
        logOperation('SENTRY', `${pos.pair}: +1R reached — stop moved to breakeven + fees ($${formatPrecisePrice(pos.sl)}). This trade can no longer lose.`, 'success');
      }

      if (r.action === 'PARTIAL') {
        const sellValue = Number(pos.qty) * price * r.fraction;
        if (sellValue < 10) {
          logOperation('SENTRY', `${pos.pair}: ${r.reason} reached, position too small to split (<$10) — stop locked at $${formatPrecisePrice(pos.sl)}.`, 'success');
        } else {
          realizeExit('activePositions', k, r.fraction, price, r.reason, `${r.reason}: ${Math.round(r.fraction * 100)}% sold, runner trails`);
        }
      } else if (r.action === 'EXIT') {
        realizeExit('activePositions', k, 1, price, r.reason,
          `${r.reason} @ $${formatPrecisePrice(price)} (stop $${formatPrecisePrice(pos.sl)}, peak +${(pos.peakGainPct || 0).toFixed(2)}%)`);
      }
    }

    // ---- legacy v4 recovery bags (no new bags are ever created in v5) ----
    for (const k of Object.keys(State.recoveryBags || {})) {
      const bag = State.recoveryBags[k];
      if (!bag || bag.exitInProgress) continue;
      const price = await livePriceFor(k, bag);
      if (!price) continue;
      bag.livePriceUSDT = price;
      const entry = Number(bag.entryPriceUSDT);
      const gainPct = ((price - entry) / entry) * 100;
      bag.targetExitPriceUSDT = entry * (1 + 2 * FEE_RATE + 0.0005);
      bag.targetStep = 'LEGACY: EXIT AT BREAKEVEN + FEES';
      if (price >= bag.targetExitPriceUSDT) {
        realizeExit('recoveryBags', k, 1, price, 'LEGACY BAG RECOVERED', 'v4 bag closed at breakeven + fees');
      } else if (gainPct <= -3.8) {
        realizeExit('recoveryBags', k, 1, price, 'LEGACY BAG CUT (-3.80%)', 'v4 bag hit its original -3.80% cap');
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

// One-time re-base of the paper account to STARTING_EQUITY_USDT ($21).
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
  const trade = realizeExit('activePositions', pairKey, 1, price, 'MANUAL EXIT', `Manual close at $${formatPrecisePrice(price)}`);
  res.json({ success: Boolean(trade), trade });
});

app.post('/api/bags/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const bag = State.recoveryBags[pairKey];
  if (!bag) return res.status(404).json({ success: false, error: 'Bag not found' });
  const price = await livePriceFor(pairKey, bag);
  if (!price) return res.status(503).json({ success: false, error: 'No live price available' });
  const trade = realizeExit('recoveryBags', pairKey, 1, price, 'BAG MANUAL EXIT', `Manual bag close at $${formatPrecisePrice(price)}`);
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
let lastPriceFrame = '';

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
  lastPriceFrame = ''; // positions may have changed shape; let the next price frame through
}

// Still every 500 ms (low-latency display), but only the held coins' prices, and only
// when something actually changed since the last frame.
function broadcastPriceStream() {
  if (!hasClients()) return;
  const prices = {};
  [...Object.keys(State.activePositions || {}), ...Object.keys(State.recoveryBags || {})].forEach((k) => {
    if (State.universe[k]) prices[k] = State.universe[k].priceUSDT;
  });
  const msg = JSON.stringify({
    type: 'PRICE_STREAM',
    prices,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags
  }, omitTickStamps);
  if (msg === lastPriceFrame) return;
  lastPriceFrame = msg;
  sendAll(msg);
}

// -------------------------------------------------------------
// ZERO-LATENCY DIRECT BINANCE WEBSOCKET INGESTION (SINGAPORE)
// -------------------------------------------------------------
let binanceGlobalWs = null;
let subscribedActiveStreams = new Set();
let isSentryRunning = false;

// Bandwidth: this used to stream !miniTicker@arr (every Binance pair, every second,
// ~1.2 GB/day) although only held coins need live ticks — radar prices refresh with the
// screener every minute. Now it subscribes <sym>@miniTicker for held coins only: same
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
        // per-symbol miniTicker events arrive one object at a time; ignore SUBSCRIBE acks
        const tickers = Array.isArray(parsed) ? parsed : (parsed && parsed.e === '24hrMiniTicker' ? [parsed] : null);
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

          if (State.activePositions[sym]) {
            State.activePositions[sym].livePriceUSDT = closePrice;
            State.activePositions[sym].lastTickAt = Date.now();
            activePriceChanged = true;
          }

          if (State.recoveryBags[sym]) {
            State.recoveryBags[sym].livePriceUSDT = closePrice;
            State.recoveryBags[sym].lastTickAt = Date.now();
            activePriceChanged = true;
          }
        });

        if (activePriceChanged && !isSentryRunning) {
          isSentryRunning = true;
          runPredictiveSentry().finally(() => {
            isSentryRunning = false;
          });
        }
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

function syncActiveSymbolSubscriptions() {
  if (!binanceGlobalWs || binanceGlobalWs.readyState !== WebSocket.OPEN) return;
  const currentSymbols = new Set([
    ...Object.keys(State.activePositions || {}),
    ...Object.keys(State.recoveryBags || {})
  ]);

  const wanted = new Set([...currentSymbols].map((sym) => `${sym.toLowerCase()}@miniTicker`));
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

setInterval(() => {
  syncTradesFromGoogleSheets().catch(() => {});
}, 120000);

setInterval(async () => {
  try {
    await runPredictiveSentry();
    await evaluateAutopilotEntries();
    syncActiveSymbolSubscriptions(); // add ticks for new buys, drop them for sold coins
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 1000);

setInterval(broadcastPriceStream, 500);
setInterval(runDynamicScreener, 60000);
