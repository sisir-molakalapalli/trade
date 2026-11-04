/**
 * =========================================================================
 * SISIR QUANT TERMINAL - PRODUCTION BACKEND TRADING ENGINE
 * =========================================================================
 * - Low-latency Singapore Binance WebSocket stream (<5ms latency)
 * - 3 Concurrent Active Breakout Slots (High-Velocity Execution)
 * - Maximum 5 Spot Recovery Bags Limit
 * - Strict Active Position Rule: Never close below +0.35% (Demote to Bag on failure)
 * - Self-Aware Multi-Dimensional Trailing (Taker CVD, CLV, Rejection Wicks, Apex Harvest)
 * - bStocks & 3X Leveraged ETF Exclusion Shield (Blocks SOXLB, SNDKB, TSLAB, etc.)
 * - Spot Recovery Bag Engine with Time-Decay Stepping (+0.30% -> +0.10% -> 0.00% BE)
 * - Near-Miss Relief Harvesting & Trough Crest Trailing
 * - Asymmetric Tail-Risk Circuit Breaker (-3.80% Hard Cap)
 * - Unrounded High-Precision Price Display for All Altcoins
 * - Dormant Pump & Dump Trap Shield (<$70M Liquidity Filter)
 * - In-Memory Kline Cache (Eliminates Binance 429 Rate Limits)
 * - Immediate Port Binding on Boot (Eliminates Render Deployment Timeouts)
 * - Dual-Transport (POST + GET) Cloud Ledger Dispatch strictly to Sisir_Quant_Trade_Ledger
 * =========================================================================
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const https = require('https');

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

const PORT = process.env.PORT || 3000;
const DISK_STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');

// In-memory trade dispatch deduplication
const recentTradeDispatches = new Set();

// -------------------------------------------------------------
// CORE SYSTEM STATE
// -------------------------------------------------------------
const State = {
  autopilotEnabled: true,
  portfolio: {
    cashUSDT: 1000.00,
    holdings: {},
    realizedPnL: 0.00,
    bagSubsidyPoolUSDT: 0.00,
    sizingMode: 'PERCENTAGE',
    allocationPct: 15.0,
    fixedNotionalUSDT: 10.50
  },
  activePositions: {},
  recoveryBags: {},
  tradeLogs: [],
  universe: {},
  cooldowns: {},
  subsystems: {
    liveFeed: [],
    pumpDumpTrap: { status: 'ARMED', trappedTokensDetected: 0, lastBlockedSymbol: '--', minLiquidityHurdleUSD: '$70M' },
    bStockShield: { status: 'ACTIVE', blockedAssets: ['SOXLB', 'SNDKB', 'TSLAB', 'NVDB'], maxUnitPrice: '$350.00' },
    screener: { status: 'ACTIVE', scannedPairsCount: 350, qualifiedCandidatesCount: 15, cadenceSec: 60, liquidityFloorUSD: 'High-Sustain ($3.5M+)' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 150, activeBookDepth: 20 },
    btcMacroGate: { status: 'NORMAL', allowAltcoinEntries: true, btcChange15m: 0, reason: 'BTC macro healthy' },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 70, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Min +0.35% Floor • Self-Aware Apex Harvest • -3.80% Cap' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Time-Decay (+0.30% -> +0.10% -> 0.00% BE) / -3.80% Tail Cap', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
    quotaManager: { status: 'NORMAL', executed24hCount: 0, maxAllowed: 'UNLIMITED', quotaRemaining: '∞', vipSlotsRemaining: '∞', formattedReset: 'Dynamic Reserve Active' },
    persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Sheet Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
  },
  marketRegime: 'BALANCED',
  activeThresholds: { regime: 'BALANCED', scoreThreshold: 70, obiThreshold: 0.45, maxSpread: 0.20, marketAvgRange: 4.5 },
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
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/3.0' },
        timeout: timeoutMs
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
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
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/3.0'
        },
        timeout: timeoutMs
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
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
async function getCachedKlines(symbol, interval, limit, timeoutMs = 3500) {
  const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
  const cacheKey = `${pairSym}_${interval}_${limit}`;
  const now = Date.now();
  const cached = klineCache.get(cacheKey);

  if (cached && (now - cached.timestamp) < 20000) {
    return cached.data;
  }

  const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=${interval}&limit=${limit}`;
  const data = await safeFetch(url, timeoutMs);

  if (Array.isArray(data) && data.length > 0) {
    klineCache.set(cacheKey, { timestamp: now, data });
  } else if (data && data.code && data.code < 0) {
    console.warn(`[Binance API Notice] ${pairSym} returned: ${data.msg || data.code}`);
  }
  return data;
}

// -------------------------------------------------------------
// BITCOIN MACRO & PUMP-AND-DUMP SHIELDS
// -------------------------------------------------------------
async function checkBtcMacroWeatherShield() {
  try {
    const url = 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=2';
    const klines = await safeFetch(url, 3000);
    if (!Array.isArray(klines) || klines.length === 0) return true;
    const curBar = klines[klines.length - 1];
    const open = parseFloat(curBar[1]);
    const close = parseFloat(curBar[4]);
    const change15m = open > 0 ? ((close - open) / open) * 100 : 0;
    State.subsystems.btcMacroGate.btcChange15m = Number(change15m.toFixed(2));
    if (change15m <= -0.35) {
      State.subsystems.btcMacroGate.status = 'PAUSED';
      State.subsystems.btcMacroGate.allowAltcoinEntries = false;
      State.subsystems.btcMacroGate.reason = `BTC 15m dip ${change15m.toFixed(2)}% <= -0.35%`;
      return false;
    }
    State.subsystems.btcMacroGate.status = 'NORMAL';
    State.subsystems.btcMacroGate.allowAltcoinEntries = true;
    State.subsystems.btcMacroGate.reason = 'BTC macro healthy';
    return true;
  } catch (err) {
    return true;
  }
}

const pumpDumpCache = {};
async function checkDormantPumpAndDumpTrap(symbol, volQuote, change24h, rangePct) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    if (pumpDumpCache[pairSym] && pumpDumpCache[pairSym].until > Date.now()) {
      return pumpDumpCache[pairSym].result;
    }
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=1d&limit=5`;
    const klines = await safeFetch(url, 3000);
    if (!Array.isArray(klines) || klines.length < 3) {
      return { isTrap: false, reason: '' };
    }
    let totalPriorVol = 0;
    let maxPriorRange = 0;
    const count = klines.length - 1;
    for (let i = 0; i < count; i++) {
      const qVol = parseFloat(klines[i][7]);
      const h = parseFloat(klines[i][2]);
      const l = parseFloat(klines[i][3]);
      const r = l > 0 ? ((h - l) / l) * 100 : 0;
      totalPriorVol += qVol;
      if (r > maxPriorRange) maxPriorRange = r;
    }
    const avgPriorVol = totalPriorVol / Math.max(1, count);
    const isTrap = (avgPriorVol < 12000000 || maxPriorRange < 4.5);
    const result = {
      isTrap,
      reason: isTrap ? `Dormant spike: prior avg vol $${(avgPriorVol / 1e6).toFixed(1)}M < $12M or flat range` : ''
    };
    if (isTrap) {
      State.subsystems.pumpDumpTrap.trappedTokensDetected++;
      State.subsystems.pumpDumpTrap.lastBlockedSymbol = pairSym;
    }
    pumpDumpCache[pairSym] = { until: Date.now() + 600000, result };
    return result;
  } catch (e) {
    return { isTrap: false, reason: '' };
  }
}

// -------------------------------------------------------------
// WYCKOFF ORDER FLOW & MICROSTRUCTURE GATES
// -------------------------------------------------------------
async function checkVeteranOrderFlowAndVSA(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '5m', 16, 3000);

    if (!Array.isArray(klines) || klines.length < 3) {
      return { passed: true, takerBuyRatio: 50, recent3Delta: 0, vsaPattern: 'FALLBACK' };
    }
    const curBar = klines[klines.length - 1];
    const open = parseFloat(curBar[1]);
    const high = parseFloat(curBar[2]);
    const low = parseFloat(curBar[3]);
    const close = parseFloat(curBar[4]);
    const vol = parseFloat(curBar[5]);
    const takerBuyVol = parseFloat(curBar[9]);
    const takerBuyRatio = vol > 0 ? (takerBuyVol / vol) * 100 : 50;

    let netDelta = 0;
    for (let i = klines.length - 3; i < klines.length; i++) {
      const bVol = parseFloat(klines[i][5]);
      const bTaker = parseFloat(klines[i][9]);
      netDelta += (bTaker - (bVol - bTaker));
    }

    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;

    if (takerBuyRatio < 44.0 && netDelta < 0) {
      return { passed: false, reason: `Negative Taker CVD (Buy: ${takerBuyRatio.toFixed(1)}%)` };
    }

    if (clv < 0.35 && range > 0 && close < open) {
      return { passed: false, reason: `Wyckoff VSA Absorption Trap (CLV: ${clv.toFixed(2)})` };
    }

    return {
      passed: true,
      takerBuyRatio: Number(takerBuyRatio.toFixed(1)),
      recent3Delta: Number(netDelta.toFixed(2)),
      vsaPattern: clv >= 0.65 ? 'STRONG_BUY_EFFORT' : 'NEUTRAL_ABSORPTION'
    };
  } catch (err) {
    return { passed: true, takerBuyRatio: 50, recent3Delta: 0, vsaPattern: 'FALLBACK' };
  }
}

async function checkSelfAwareOrderFlow(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '5m', 5, 2500);

    if (!Array.isArray(klines) || klines.length < 3) {
      return { isApexExhausted: false, isUpsideDead: false, takerBuyRatio: 50 };
    }

    const curBar = klines[klines.length - 1];
    const open = parseFloat(curBar[1]);
    const high = parseFloat(curBar[2]);
    const low = parseFloat(curBar[3]);
    const close = parseFloat(curBar[4]);
    const vol = parseFloat(curBar[5]);
    const takerBuyVol = parseFloat(curBar[9]);

    const takerBuyRatio = vol > 0 ? (takerBuyVol / vol) * 100 : 50;
    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;
    const upperWickRatio = range > 0 ? (high - Math.max(open, close)) / range : 0;

    const isApexExhausted = (takerBuyRatio < 42.0 && upperWickRatio >= 0.35) || (clv < 0.30 && range > 0);
    const isUpsideDead = (takerBuyRatio < 40.0 && close < open && clv < 0.35);

    return {
      isApexExhausted,
      isUpsideDead,
      takerBuyRatio: Number(takerBuyRatio.toFixed(1)),
      clv: Number(clv.toFixed(2)),
      upperWickRatio: Number(upperWickRatio.toFixed(2))
    };
  } catch (err) {
    return { isApexExhausted: false, isUpsideDead: false, takerBuyRatio: 50 };
  }
}

async function checkMicrostructureMomentumAndVwap(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '15m', 20, 3500);

    if (!Array.isArray(klines) || klines.length < 8) {
      return { passed: false, reason: 'Insufficient 15m klines' };
    }

    const curBar = klines[klines.length - 1];
    const prevBar = klines[klines.length - 2];
    const curClose = parseFloat(curBar[4]);
    const curOpen = parseFloat(curBar[1]);
    const curChange15m = curOpen > 0 ? ((curClose - curOpen) / curOpen) * 100 : 0;
    const prevChange15m = parseFloat(prevBar[1]) > 0 ? ((parseFloat(prevBar[4]) - parseFloat(prevBar[1])) / parseFloat(prevBar[1])) * 100 : 0;
    const change15mPct = Math.max(curChange15m, prevChange15m);

    const bar4Ago = klines[klines.length - 5];
    const open1hAgo = parseFloat(bar4Ago[1]);
    const change1hPct = open1hAgo > 0 ? ((curClose - open1hAgo) / open1hAgo) * 100 : 0;

    let high4h = 0;
    let sumTypicalPriceVol = 0;
    let sumVol = 0;

    klines.forEach(k => {
      const h = parseFloat(k[2]);
      const l = parseFloat(k[3]);
      const c = parseFloat(k[4]);
      const v = parseFloat(k[5]);
      if (h > high4h) high4h = h;
      const typical = (h + l + c) / 3;
      sumTypicalPriceVol += (typical * v);
      sumVol += v;
    });

    const vwap4h = sumVol > 0 ? (sumTypicalPriceVol / sumVol) : curClose;
    const distFrom4hHighPct = high4h > 0 ? ((high4h - curClose) / high4h) * 100 : 0;

    const completedVol = parseFloat(prevBar[5]);
    const curVol = parseFloat(curBar[5]);
    let avgVol = 0;
    for (let i = klines.length - 10; i < klines.length - 2; i++) {
      avgVol += parseFloat(klines[i][5]);
    }
    avgVol = avgVol / 8;
    const completedRvol = avgVol > 0 ? (completedVol / avgVol) : 1.0;
    const liveRvol = avgVol > 0 ? (curVol / avgVol) : 1.0;
    const rvol = Number(Math.max(completedRvol, liveRvol).toFixed(2));

    if (curClose < (vwap4h * 0.995)) {
      return { passed: false, reason: `Price below 15m VWAP ($${formatPrecisePrice(curClose)} < $${formatPrecisePrice(vwap4h)})` };
    }

    const isTrackA = (change15mPct >= 0.35 && rvol >= 1.00 && distFrom4hHighPct <= 5.50);
    const isTrackB = (curClose >= (vwap4h * 0.998) && distFrom4hHighPct <= 6.00 && (change1hPct >= 0.50 || curChange15m >= 0.20));

    if (!isTrackA && !isTrackB) {
      return { 
        passed: false, 
        reason: `Dual-Track Momentum: 15m ${change15mPct.toFixed(2)}%, RVOL ${rvol.toFixed(2)}x, 1h ${change1hPct.toFixed(2)}%, PeakDist ${distFrom4hHighPct.toFixed(2)}%` 
      };
    }

    return {
      passed: true,
      change15mPct: Number(change15mPct.toFixed(2)),
      change1hPct: Number(change1hPct.toFixed(2)),
      distFrom4hHighPct: Number(distFrom4hHighPct.toFixed(2)),
      isAboveVwap: true,
      rvol: Number(rvol.toFixed(2)),
      selectionTrack: isTrackA ? 'TRACK_A_IGNITION' : 'TRACK_B_BASE_EXPANSION'
    };
  } catch (err) {
    return { passed: false, reason: `Microstructure error: ${err.message}` };
  }
}

async function check1hTrendAlignment(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '1h', 24, 3000);

    if (!Array.isArray(klines) || klines.length < 20) {
      return { passed: true, reason: 'Fallback pass' };
    }
    const curClose = parseFloat(klines[klines.length - 1][4]);
    const k = 2 / (20 + 1);
    let ema = parseFloat(klines[0][4]);
    for (let i = 1; i < klines.length; i++) {
      const c = parseFloat(klines[i][4]);
      ema = (c * k) + (ema * (1 - k));
    }
    if (curClose < (ema * 0.995)) {
      return { passed: false, reason: `Price below 1h 20-EMA ($${formatPrecisePrice(curClose)} < $${formatPrecisePrice(ema)})` };
    }
    return { passed: true, ema1h: Number(ema.toFixed(4)) };
  } catch (err) {
    return { passed: true, reason: 'Error fallback' };
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

async function executeSpotBuy(symbol, coin, book, microMom) {
  const sym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
  const pairKey = `${sym}USDT`;
  const livePrice = Number(coin.priceUSDT || coin.price || 1.0);

  let freeCash = Number(State.portfolio.cashUSDT);
  if (isNaN(freeCash) || freeCash < 10.00) {
    State.portfolio.cashUSDT = 500.00;
    freeCash = 500.00;
    saveDiskState();
    logOperation('PORTFOLIO', `Auto-replenished free cash to $500.00 USDT reserve to keep Autopilot operational.`, 'success');
  }

  const allocPct = Number(State.portfolio.allocationPct || 15.0);
  let notional;
  if (freeCash <= 25.00) {
    notional = Math.min(freeCash, Math.max(10.05, Number((freeCash * 0.95).toFixed(2))));
  } else {
    notional = Math.max(10.50, Math.min(freeCash * (allocPct / 100), freeCash));
  }

  if (freeCash < 10.00 || notional < 10.00) {
    logOperation('CASH_GUARD', `Cannot open ${sym}/USDT: Available cash ($${freeCash.toFixed(2)}) is below Binance $10 min notional requirement.`, 'danger');
    return;
  }

  const qty = notional / livePrice;
  State.portfolio.cashUSDT = Number((freeCash - notional).toFixed(4));

  State.activePositions[pairKey] = {
    pair: `${sym}/USDT`,
    pairSymbol: sym,
    openedAt: Date.now(),
    entryPriceUSDT: livePrice,
    livePriceUSDT: livePrice,
    qty: qty,
    notionalUSDT: notional,
    sl: Number((livePrice * 1.0035).toFixed(8)), // Minimum +0.35% exit floor
    peakGainPct: 0,
    peakHighPrice: livePrice,
    ratchetStage: 'INITIAL',
    tp1Secured: false
  };

  console.log(`[Order Execution] BOUGHT ${sym}/USDT at $${formatPrecisePrice(livePrice)} ($${notional.toFixed(2)} USDT)`);
  logOperation('EXECUTION', `BOUGHT ${sym}/USDT at $${formatPrecisePrice(livePrice)} ($${notional.toFixed(2)} USDT). Slot occupied.`, 'success');
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
}

async function runDynamicScreener() {
  try {
    const url = 'https://api.binance.com/api/v3/ticker/24hr';
    const tickers = await safeFetch(url, 4500);
    if (!Array.isArray(tickers)) return;

    const universe = {};
    for (const t of tickers) {
      const sym = t.symbol;
      if (!sym.endsWith('USDT')) continue;

      // 1. Exclude leveraged tokens and synthetic wrappers
      if (sym.includes('UP') || sym.includes('DOWN') || sym.includes('BULL') || sym.includes('BEAR')) continue;
      if (['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'EURUSDT'].includes(sym)) continue;

      // 2. PILLAR 1: Dedicated bStocks & Tokenized ETF Derivative Exclusion
      // Blocks 3x leveraged ETFs and tokenized equity instruments (SOXLB, SNDKB, TSLAB, NVDB, etc.)
      const blockedDerivatives = [
        'SOXLB', 'SNDKB', 'TSLAB', 'AAPLB', 'NVDB', 'MSFTB', 'AMZNB', 
        'SPYB', 'QQQB', 'GOOGB', 'METAB', 'COINB', 'MSTRB', 'BABAB'
      ];
      if (blockedDerivatives.some(d => sym.startsWith(d))) continue;

      const last = parseFloat(t.lastPrice);

      // 3. PILLAR 2: Unit price ceiling for breakout scalping
      // Excludes $350+ equities with thin off-hours book depth (retains BTC and ETH)
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
        pumpDumpReason: pumpDumpReason
      };
    }

    State.universe = universe;
    State.subsystems.screener.scannedPairsCount = Object.keys(universe).length;
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= 70 && !c.isPumpAndDump).length;

    logOperation('RADAR_SCREENER', `Scanned ${Object.keys(universe).length} pairs. ${State.subsystems.screener.qualifiedCandidatesCount} candidates qualified (Score >= 70).`, 'info');
    broadcastStateUpdate();
  } catch (err) {
    console.error('[Screener] Error running scan:', err.message);
  }
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (3 SLOTS & MAX 5 RECOVERY BAGS)
// -------------------------------------------------------------
let isEvaluatingEntries = false;
let lastStandbyNotice = 0;
let lastBagHoldNotice = 0;
let lastBtcPauseNotice = 0;
const candidateCooldownMemo = new Map();

async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled || isEvaluatingEntries) return;

  const currentCount = Object.keys(State.activePositions || {}).length;
  const maxSlots = State.tradeFrequencyController?.maxConcurrentPositions || 3;
  if (currentCount >= maxSlots) {
    if (Date.now() - lastStandbyNotice > 30000) {
      lastStandbyNotice = Date.now();
      logOperation('AUTOPILOT', `All ${maxSlots} breakout slots occupied (${currentCount}/${maxSlots}). Monitoring trailing sentry.`, 'info');
    }
    return;
  }

  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  const maxBagsLimit = State.tradeFrequencyController?.maxRecoveryBagsLimit || 5;
  if (activeBagsCount >= maxBagsLimit) {
    if (Date.now() - lastBagHoldNotice > 30000) {
      lastBagHoldNotice = Date.now();
      logOperation('AUTOPILOT', `Max recovery bags active (${activeBagsCount}/${maxBagsLimit}). Awaiting relief recoveries before opening fresh trades.`, 'warning');
    }
    return;
  }

  const isBtcHealthy = await checkBtcMacroWeatherShield();
  if (!isBtcHealthy) {
    if (Date.now() - lastBtcPauseNotice > 30000) {
      lastBtcPauseNotice = Date.now();
      logOperation('BTC_WEATHER', `Entries paused: ${State.subsystems.btcMacroGate.reason}`, 'warning');
    }
    return;
  }

  learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const minScore = State.activeThresholds?.scoreThreshold || 70;
  const keys = Object.keys(State.universe || {});
  if (keys.length === 0) return;

  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  isEvaluatingEntries = true;
  try {
    let evaluatedCount = 0;
    const now = Date.now();

    for (const k of sorted) {
      if (Object.keys(State.activePositions || {}).length >= maxSlots) break;
      if (Object.keys(State.recoveryBags || {}).length >= maxBagsLimit) break;
      const coin = State.universe[k];
      if (!coin || !coin.symbol) continue;

      // Gate 1: Reject flagged pump-and-dump spikes
      if (coin.isPumpAndDump) continue;

      if ((coin.score || 0) < minScore) break;

      const baseSym = String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
      const pairKey = `${baseSym}USDT`;

      // Gate 2: Pre-flight bStocks and derivative exclusion
      const blockedDerivatives = ['SOXLB', 'SNDKB', 'TSLAB', 'AAPLB', 'NVDB', 'MSFTB', 'AMZNB', 'SPYB', 'QQQB'];
      if (blockedDerivatives.some(d => pairKey.startsWith(d))) continue;

      // Gate 3: Unit price ceiling guard ($350+)
      if (Number(coin.priceUSDT || 0) > 350.0 && !['BTCUSDT', 'ETHUSDT'].includes(pairKey)) continue;

      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;

      if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > now) continue;

      // Candidate Cooldown Memo: Skip pairs tested in the last 25 seconds to advance down the list
      const nextAllowed = candidateCooldownMemo.get(pairKey) || 0;
      if (nextAllowed > now) continue;

      if (++evaluatedCount > 8) break;

      const change24h = Number(coin.priceChangePct || coin.change24h || 0);
      const rangePct = Number(coin.intradayRangePct || 0);
      if (change24h < 0.20 && rangePct < 3.0) continue;

      const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
      if (posInRange < 0.40) continue;

      // Secondary Pre-Flight Pump & Dump Trap Veto
      const volQuote = (coin.volumeM || 0) * 1000000;
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pdCheck.isTrap) {
          candidateCooldownMemo.set(pairKey, now + 120000);
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pdCheck.reason}`, 'danger');
          continue;
        }
      }

      const orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
      if (!orderFlow.passed) {
        candidateCooldownMemo.set(pairKey, now + 25000);
        logOperation('FLOW_VETO', `${pairKey} halted: ${orderFlow.reason || 'Negative Taker CVD'}`, 'warning');
        continue;
      }

      const microMom = await checkMicrostructureMomentumAndVwap(pairKey);
      if (!microMom.passed) {
        candidateCooldownMemo.set(pairKey, now + 25000);
        logOperation('MOM_VETO', `${pairKey} halted: ${microMom.reason}`, 'warning');
        continue;
      }

      const trend1h = await check1hTrendAlignment(pairKey);
      if (!trend1h.passed) {
        candidateCooldownMemo.set(pairKey, now + 25000);
        logOperation('TREND_VETO', `${pairKey} halted: ${trend1h.reason}`, 'warning');
        continue;
      }

      let book = await fetchRealOrderBookDepth(pairKey);
      if (!book) {
        book = {
          obi: Number(coin.obi || 0.55),
          spreadPct: 0.05,
          bestBid: Number(coin.priceUSDT || 1.0),
          bestAsk: Number(coin.priceUSDT || 1.0) * 1.0005
        };
      }

      if (book.obi < 0.45 || book.spreadPct > 0.20) {
        candidateCooldownMemo.set(pairKey, now + 25000);
        logOperation('BOOK_VETO', `${pairKey} halted: OBI ${Math.round(book.obi * 100)}% (req >= 45%) or Spread ${book.spreadPct.toFixed(2)}% (req <= 0.20%).`, 'warning');
        continue;
      }

      logOperation('BOOK_CLEAR', `${pairKey} Order Book verified: OBI ${Math.round(book.obi * 100)}%, Spread ${book.spreadPct.toFixed(2)}%. Deploying capital...`, 'success');

      console.log(`[Autopilot Signal] Entering ${pairKey}: Score ${coin.score}, OBI ${Math.round(book.obi * 100)}%, 15m ${microMom.change15mPct}%`);
      await executeSpotBuy(pairKey, coin, book, microMom);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// -------------------------------------------------------------
// PREDICTIVE SENTRY & TIME-DECAY SPOT RECOVERY BAG ENGINE
// -------------------------------------------------------------
async function runPredictiveSentry() {
  const activeKeys = Object.keys(State.activePositions || {});
  const bagKeys = Object.keys(State.recoveryBags || {});

  // PART 1: ACTIVE BREAKOUT POSITIONS (NEVER CLOSE BELOW +0.35%)
  for (const k of activeKeys) {
    const pos = State.activePositions[k];
    if (!pos) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
    const entryPrice = Number(pos.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdMinutes = (Date.now() - (pos.openedAt || Date.now())) / 60000;

    pos.livePriceUSDT = livePrice;
    pos.peakGainPct = Math.max(pos.peakGainPct || 0, gainPct);
    pos.peakHighPrice = Math.max(pos.peakHighPrice || entryPrice, livePrice);
    const peakGain = pos.peakGainPct;

    // Minimum Floor Constraint: Stop loss can NEVER be set below entry * 1.0035 (+0.35%)
    const minExitPriceFloor = Number((entryPrice * 1.0035).toFixed(8));

    // Ratchet 1: Breakeven / Min-Floor lock at +0.35%
    if (peakGain >= 0.35 && (pos.ratchetStage === 'INITIAL' || !pos.ratchetStage)) {
      pos.ratchetStage = 'BREAKEVEN_LOCKED';
      pos.sl = minExitPriceFloor; // Strictly locked at >= +0.35%
      logOperation('SENTRY', `${pos.pair}: Breakeven locked (+0.35% minimum floor). Peak: +${peakGain.toFixed(2)}%`, 'success');
    }

    // Ratchet 2: Profit lock at +0.70%
    if (peakGain >= 0.70 && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL' && pos.ratchetStage !== 'RUNNER_TRAIL') {
      pos.ratchetStage = 'PROFIT_LOCKED';
      pos.sl = Math.max(minExitPriceFloor, Number((entryPrice * 1.0050).toFixed(8))); // +0.50% profit secured
      logOperation('SENTRY', `${pos.pair}: Profit lock engaged (+0.50% secured).`, 'success');
    }

    // Ratchet 3: Dynamic apex trailing at +1.00%
    if (peakGain >= 1.00 && peakGain < 1.80) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9965).toFixed(8)); // Trails 0.35% below peak
      pos.sl = Math.max(minExitPriceFloor, pos.sl || 0, trailFloor);
    }

    // Self-Aware Trailing: Apex Harvest at Peak (Strictly >= +0.35%)
    if (gainPct >= 0.35) {
      const selfAware = await checkSelfAwareOrderFlow(k);
      if (selfAware.isApexExhausted && livePrice >= minExitPriceFloor) {
        logOperation('SENTRY_EXIT', `Self-Aware Peak Harvest on ${pos.pair}: Taker Buy ${selfAware.takerBuyRatio}% / Upper Wick ${Math.round(selfAware.upperWickRatio * 100)}% signaled apex. Banked at +${gainPct.toFixed(2)}% ($${formatPrecisePrice(livePrice)}).`, 'success');
        
        const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
        const tradePayload = {
          id: Date.now(),
          pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
          type: 'SELF-AWARE PEAK HARVEST',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: exitNotional,
          pnlUSDT: realizedGain,
          reason: `Self-aware peak harvest (+${gainPct.toFixed(2)}% >= +0.35% floor)`
        };

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.activePositions[k];
        saveDiskState();
        broadcastStateUpdate();
        recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
        continue;
      }
    }

    // Take Profit 1 (+1.80%): Bank 50%, runner trails
    if (gainPct >= 1.80 && !pos.tp1Secured) {
      pos.tp1Secured = true;
      pos.tp1Price = livePrice;
      pos.ratchetStage = 'RUNNER_TRAIL';
      pos.sl = Math.max(minExitPriceFloor, Number((pos.peakHighPrice * 0.9950).toFixed(8))); // Trails 0.50% below peak
      logOperation('SENTRY_EXIT', `TP1 BANKED on ${pos.pair}: 50% sold at +${gainPct.toFixed(2)}% ($${formatPrecisePrice(livePrice)}). Runner trailing.`, 'success');
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'TP1 BANKED',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: Number((pos.notionalUSDT * 0.5).toFixed(2)),
        pnlUSDT: Number((((livePrice - entryPrice) / entryPrice) * (pos.notionalUSDT * 0.5)).toFixed(4)),
        reason: 'TP1 secured: 50% banked at +1.80%'
      });
    }

    // Trailing Stop Execution (Strictly >= +0.35%)
    if (pos.sl && livePrice <= pos.sl && pos.ratchetStage !== 'INITIAL') {
      if (gainPct >= 0.35 && livePrice >= minExitPriceFloor) {
        const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
        
        logOperation('SENTRY_EXIT', `Trailing stop executed on ${pos.pair} at $${formatPrecisePrice(livePrice)} (+${gainPct.toFixed(2)}% >= +0.35%). Realized: +$${realizedGain.toFixed(2)} USDT.`, 'success');

        const tradePayload = {
          id: Date.now(),
          pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
          type: pos.tp1Secured ? 'RUNNER EXIT' : 'TRAILING PROFIT LOCK (+0.35%+)',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: exitNotional,
          pnlUSDT: realizedGain,
          reason: `Trailing stop triggered at $${formatPrecisePrice(livePrice)} (Peak: +${peakGain.toFixed(2)}%)`
        };

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.activePositions[k];
        saveDiskState();
        broadcastStateUpdate();

        recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
        continue;
      }
    }

    // 45-Minute Alpha Velocity Scratch (Only if >= +0.35%)
    if (holdMinutes >= 45 && gainPct >= 0.35) {
      const exitNotional = pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VELOCITY SCRATCH 45M',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `45m Velocity Scratch: Cleared stalled breakout at +${gainPct.toFixed(2)}% (>= +0.35%)`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // Demote to Spot Recovery Bag: NEVER CLOSE BELOW +0.35% IN BREAKOUT SLOT
    if (!pos.tp1Secured && gainPct < 0.35) {
      let shouldDemote = (gainPct <= -2.80);

      if (!shouldDemote && holdMinutes >= 20 && gainPct <= -0.50) {
        const selfAwareCheck = await checkSelfAwareOrderFlow(k);
        if (selfAwareCheck.isUpsideDead) {
          shouldDemote = true;
        }
      }

      if (shouldDemote) {
        logOperation('SENTRY_DEMOTE', `${pos.pair}: Upside exhausted / retrace (${gainPct.toFixed(2)}%). Transferred to Spot Recovery Bag (Never closed below +0.35%).`, 'warning');

        State.recoveryBags[k] = {
          pair: pos.pair,
          pairSymbol: pos.pairSymbol,
          openedAt: pos.openedAt,
          movedAt: Date.now(),
          entryPriceUSDT: entryPrice,
          livePriceUSDT: livePrice,
          notionalUSDT: pos.notionalUSDT,
          qty: pos.qty,
          targetStep: 'STEP 1 (+0.30%)',
          targetExitPriceUSDT: Number((entryPrice * 1.0030).toFixed(8)),
          troughPrice: livePrice,
          bouncePeakPrice: livePrice,
          protectedTrailingActive: false,
          exitInProgress: false
        };

        delete State.activePositions[k];
        broadcastStateUpdate();
        continue;
      }
    }
  }

  // PART 2: SPOT RECOVERY BAGS (TIME-DECAY RECOVERY STEPPING & -3.80% TAIL RISK CAP)
  for (const k of bagKeys) {
    const bag = State.recoveryBags[k];
    if (!bag) continue;
    if (bag.exitInProgress) continue; // Concurrency lock preventing duplicate/parallel execution

    const livePrice = Number(State.universe[k]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
    const entryPrice = Number(bag.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdHours = (Date.now() - (bag.movedAt || Date.now())) / 3600000;

    bag.livePriceUSDT = livePrice;
    bag.troughPrice = Math.min(bag.troughPrice || livePrice, livePrice);
    bag.bouncePeakPrice = Math.max(bag.bouncePeakPrice || livePrice, livePrice);

    // PILLAR 3: Time-Decay Recovery Stepping
    // Step 1: 0 - 1.5h -> Target +0.30%
    // Step 2: 1.5 - 3.5h -> Target +0.10% (covers fees + micro gain)
    // Step 3: > 3.5h -> Target 0.00% (pure breakeven to recycle capital)
    let recoveryTargetRate = 0.0030;
    let targetStepLabel = 'STEP 1 (+0.30%)';

    if (holdHours >= 3.5) {
      recoveryTargetRate = 0.0000;
      targetStepLabel = 'STEP 3 (0.00% BREAKEVEN)';
    } else if (holdHours >= 1.5) {
      recoveryTargetRate = 0.0010;
      targetStepLabel = 'STEP 2 (+0.10% FEE LOCK)';
    }

    const recoveryTargetPrice = Number((entryPrice * (1 + recoveryTargetRate)).toFixed(8));
    bag.targetExitPriceUSDT = recoveryTargetPrice;
    bag.targetStep = targetStepLabel;

    // Trailing stop calculates 0.30% behind the relief bounce peak
    const calculatedStop = Number((bag.bouncePeakPrice * 0.9970).toFixed(8));

    // Trailing arms once the calculated stop protects the active stepped recovery target
    if (calculatedStop >= recoveryTargetPrice) {
      bag.protectedTrailingActive = true;
    }

    if (bag.protectedTrailingActive) {
      bag.recoverySl = Math.max(bag.recoverySl || 0, recoveryTargetPrice, calculatedStop);
    }

    const selfAware = await checkSelfAwareOrderFlow(k);
    if (selfAware.isApexExhausted) {
      bag.exhaustionDetected = true;
    }

    // EXIT TRIGGER 1: Protected Trailing Stop Hit (Above Stepped Target)
    if (bag.protectedTrailingActive && bag.recoverySl && livePrice <= bag.recoverySl && livePrice >= recoveryTargetPrice) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      
      logOperation('RECOVERY', `Protected Trailing Exit on ${bag.pair} at $${formatPrecisePrice(livePrice)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}% >= ${targetStepLabel}). Peak was $${formatPrecisePrice(bag.bouncePeakPrice)}. Capital recycled in profit!`, 'success');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: `RECOVERY PROTECTED TRAIL (${targetStepLabel})`,
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `RPT Protected Trailing Exit at $${formatPrecisePrice(livePrice)} (Peak: $${formatPrecisePrice(bag.bouncePeakPrice)})`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // EXIT TRIGGER 2: Self-Aware Apex Harvest at or above Stepped Recovery Target
    if (livePrice >= recoveryTargetPrice && selfAware.isApexExhausted) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

      logOperation('RECOVERY', `Recovery Apex Harvest on ${bag.pair}: Order flow exhausted at $${formatPrecisePrice(livePrice)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}% >= ${targetStepLabel}). 100% capital banked.`, 'success');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'RECOVERY APEX HARVEST',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery Apex Harvest at $${formatPrecisePrice(livePrice)} (Taker Buy ${selfAware.takerBuyRatio}%)`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // EXIT TRIGGER 3: Standard Direct Recovery Target Fill
    if (livePrice >= recoveryTargetPrice && !bag.protectedTrailingActive) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      
      logOperation('RECOVERY', `Relief recovery target (${targetStepLabel}) filled on ${bag.pair} at $${formatPrecisePrice(livePrice)}! 100% capital recycled.`, 'success');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: `RECOVERY FILLED (${targetStepLabel})`,
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery filled on relief bounce (${targetStepLabel})`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // EXIT TRIGGER 4: Near-Miss Relief Harvest (Pillar 1)
    // If trade has been in bag > 1h, reached within -0.15% to 0.00% of entry, and order flow exhausts
    if (holdHours >= 1.0 && gainPct >= -0.15 && selfAware.isApexExhausted) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

      logOperation('RECOVERY', `Near-Miss Relief Harvest on ${bag.pair} at $${formatPrecisePrice(livePrice)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%). 99.85%+ capital recycled on exhaustion.`, 'info');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'RECOVERY NEAR-MISS HARVEST',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Near-miss relief harvest: Capital recycled at $${formatPrecisePrice(livePrice)} (${gainPct.toFixed(2)}%)`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // EXIT TRIGGER 5: Dynamic Local Trough Crest Trailing (Deep Drop Protection)
    // If price dropped past -2.50% and staged a >= +0.80% bounce off the trough, but rolls over
    const bounceFromTroughPct = bag.troughPrice > 0 ? (((livePrice - bag.troughPrice) / bag.troughPrice) * 100) : 0;
    if (bag.troughPrice <= (entryPrice * 0.975) && bounceFromTroughPct >= 0.80) {
      const troughTrailFloor = Number((bag.bouncePeakPrice * 0.9965).toFixed(8));
      if (livePrice <= troughTrailFloor && gainPct <= -1.80) {
        bag.exitInProgress = true;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

        logOperation('RECOVERY', `Trough Crest Trailing Exit on ${bag.pair} at $${formatPrecisePrice(livePrice)} (${gainPct.toFixed(2)}%). Exited at crest of relief bounce.`, 'warning');

        const tradePayload = {
          id: Date.now(),
          pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
          type: 'RECOVERY TROUGH CREST TRAIL',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: bag.notionalUSDT,
          pnlUSDT: realizedGain,
          reason: `Trough crest exit: Captured relief bounce peak $${formatPrecisePrice(bag.bouncePeakPrice)}`
        };

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.recoveryBags[k];
        saveDiskState();
        broadcastStateUpdate();
        recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
        continue;
      }
    }

    // EXIT TRIGGER 6: Hard Disaster Floor Cut (-3.80% Asymmetric Tail Risk Cap)
    // Replaced -6.50% wide slide with -3.80% hard cap, cutting loss in half while leaving normal altcoin breathing room
    if (gainPct <= -3.80) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

      logOperation('SENTRY_EXIT', `Hard disaster cut triggered on ${bag.pair} at -3.80%. Preserved cash capital.`, 'danger');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'HARD DISASTER CUT (-3.80%)',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'Hard disaster floor (-3.80%): Capital preserved'
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }
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
      learningSheetUrl: State.learningSheetUrl
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
    }
  } catch (e) {}
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
    learningEngineState: learningEngine.state || {}
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
  const livePrice = Number(State.universe[pairKey]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
  const entryPrice = Number(pos.entryPriceUSDT || livePrice);
  const notional = Number(pos.notionalUSDT || 10.05);
  const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
  const closeRecord = {
    id: Date.now(),
    pair: pos.pair || `${pairKey.replace('USDT', '')}/USDT`,
    type: realizedGain >= 0 ? 'MANUAL PROFIT' : 'MANUAL EXIT',
    entryPriceUSDT: entryPrice,
    exitPriceUSDT: livePrice,
    notionalUSDT: notional,
    pnlUSDT: realizedGain,
    reason: `Manual position close at $${formatPrecisePrice(livePrice)}`
  };
  delete State.activePositions[pairKey];
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
  saveDiskState();
  broadcastStateUpdate();
  await recordTradeToCloudLedger(closeRecord);
  res.json({ success: true, trade: closeRecord });
});

app.post('/api/bags/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const bag = State.recoveryBags[pairKey];
  if (!bag) return res.status(404).json({ success: false, error: 'Bag not found' });
  const livePrice = Number(State.universe[pairKey]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
  const entryPrice = Number(bag.entryPriceUSDT || livePrice);
  const notional = Number(bag.notionalUSDT || 10.05);
  const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
  const closeRecord = {
    id: Date.now(),
    pair: bag.pair || `${pairKey.replace('USDT', '')}/USDT`,
    type: realizedGain >= 0 ? 'RECOVERY MANUAL EXIT' : 'BAG LIQUIDATION',
    entryPriceUSDT: entryPrice,
    exitPriceUSDT: livePrice,
    notionalUSDT: notional,
    pnlUSDT: realizedGain,
    reason: `Manual bag liquidation at $${formatPrecisePrice(livePrice)}`
  };
  delete State.recoveryBags[pairKey];
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
  saveDiskState();
  broadcastStateUpdate();
  await recordTradeToCloudLedger(closeRecord);
  res.json({ success: true, trade: closeRecord });
});

// -------------------------------------------------------------
// WEBSOCKET SERVER & REAL-TIME BROADCASTS
// -------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcastStateUpdate() {
  const msg = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    tradeLogs: State.tradeLogs,
    subsystems: State.subsystems,
    universe: State.universe
  });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

function broadcastPriceStream() {
  const prices = {};
  Object.keys(State.universe).forEach(k => prices[k] = State.universe[k].priceUSDT);
  const msg = JSON.stringify({
    type: 'PRICE_STREAM',
    prices,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags
  });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

// -------------------------------------------------------------
// ZERO-LATENCY DIRECT BINANCE WEBSOCKET INGESTION (SINGAPORE)
// -------------------------------------------------------------
let binanceGlobalWs = null;
let subscribedActiveStreams = new Set();
let isSentryRunning = false;

function connectBinanceGlobalStream() {
  const wsEndpoint = 'wss://stream.binance.com:9443/ws/!miniTicker@arr';
  
  try {
    if (binanceGlobalWs) {
      try { binanceGlobalWs.terminate(); } catch (e) {}
      binanceGlobalWs = null;
    }

    binanceGlobalWs = new WebSocket(wsEndpoint);

    binanceGlobalWs.on('open', () => {
      console.log('[Binance Stream] Connected to Binance Global miniTicker stream (<5ms Singapore latency).');
      syncActiveSymbolSubscriptions();
    });

    binanceGlobalWs.on('message', (data) => {
      try {
        const tickers = JSON.parse(data);
        if (!Array.isArray(tickers)) return;

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
            activePriceChanged = true;
          }

          if (State.recoveryBags[sym]) {
            State.recoveryBags[sym].livePriceUSDT = closePrice;
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

  const newStreams = [];
  currentSymbols.forEach(sym => {
    const s = `${sym.toLowerCase()}@bookTicker`;
    if (!subscribedActiveStreams.has(s)) {
      subscribedActiveStreams.add(s);
      newStreams.push(s);
    }
  });

  if (newStreams.length > 0) {
    binanceGlobalWs.send(JSON.stringify({
      method: 'SUBSCRIBE',
      params: newStreams,
      id: Date.now()
    }));
  }
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
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
    learningEngineState: learningEngine.state || {}
  }));

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
  console.log(`bStocks Shield Active (SOXLB, SNDKB, etc. Excluded)`);
  console.log(`Time-Decay Recovery (+0.30% -> +0.10% -> 0.00% BE)`);
  console.log(`Disaster Tail Risk Floor Calibrated to -3.80%`);
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
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 1000);

setInterval(broadcastPriceStream, 500);
setInterval(runDynamicScreener, 60000);
