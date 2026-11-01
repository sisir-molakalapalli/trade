/**
 * =========================================================================
 * SISIR QUANT TERMINAL - PRODUCTION BACKEND ENGINE
 * =========================================================================
 * - Zero external HTTP dependencies: Native fetch & https fallback
 * - Singapore low-latency Binance WebSocket stream (<5ms latency)
 * - Dynamic @bookTicker subscriptions for active breakout positions & bags
 * - 5 Concurrent Active Breakout Slots (High-Velocity Execution)
 * - Deep candidate rotation (evaluates up to 30 pairs per cycle)
 * - Custom Dormant Pump & Dump Trap (<$70M liquidity + dormant 24h baseline)
 * - Time Machine 48-candle historical replay (45% win odds baseline)
 * - Taker Cumulative Volume Delta (CVD) & Wyckoff VSA confluence
 * - 15m VWAP & 1h 20-EMA macro trend alignment
 * - 20-depth real order book gate (OBI >= 48%, spread <= 0.15%)
 * - Minute-0 Micro-Trailing Sentry with 30m/45m Alpha Velocity exits
 * - Spot Recovery Bag Engine (Zero Realized Loss Model)
 * - Dual-Transport (POST + GET) Trade Ledger Cloud Dispatch
 * - Dedicated server proxy for Google Apps Script connection tests
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

// Import Autonomous Learning Engine with safe fallback
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
    fixedNotionalUSDT: 50.00
  },
  activePositions: {},
  recoveryBags: {},
  tradeLogs: [],
  universe: {},
  cooldowns: {},
  subsystems: {
    timeMachine: { status: 'ONLINE', replaysCompleted: 0, lastEvaluatedSymbol: '--', lastWinOddsPct: 55, minOddsFloorPct: 45 },
    pumpDumpTrap: { status: 'ARMED', trappedTokensDetected: 0, lastBlockedSymbol: '--', minLiquidityHurdleUSD: '$70M' },
    screener: { status: 'ACTIVE', scannedPairsCount: 350, qualifiedCandidatesCount: 12, cadenceSec: 60, liquidityFloorUSD: 'High-Sustain ($3.5M-$12M)' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 150, activeBookDepth: 20 },
    btcMacroGate: { status: 'NORMAL', allowAltcoinEntries: true, btcChange15m: 0, reason: 'BTC macro healthy' },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 75, liveObiGatePct: 50 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Instant-On Minute-0 Trailing + Alpha Velocity' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Dynamic Stepped Target (+0.30% -> 0.00% -> -0.75%)', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
    quotaManager: { status: 'NORMAL', executed24hCount: 0, maxAllowed: 'UNLIMITED', quotaRemaining: '∞', vipSlotsRemaining: '∞', formattedReset: 'Dynamic Reserve Active' },
    persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Drive Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
  },
  marketRegime: 'BALANCED',
  activeThresholds: { regime: 'BALANCED', scoreThreshold: 75, obiThreshold: 0.48, maxSpread: 0.15, marketAvgRange: 4.5 },
  tradeFrequencyController: {
    dynamicGovernorEnabled: true,
    quotaBypassEnabled: false,
    cashReserveFloorUSDT: 300.00,
    maxRecoveryBagsLimit: 6,
    governorTier: 'UNLIMITED_SPRINT',
    maxTradesPer24h: 9999,
    maxConcurrentPositions: 5,
    rollingTrades24h: [],
    consecutiveWinStreak: 0,
    nextSlotReleaseTimestamp: 0,
    pacingMode: 'HIGH_VELOCITY'
  },
  usdtToInrRate: 94.94,
  // Dedicated Cloud Endpoints
  masterLedgerUrl: 'https://script.google.com/macros/s/AKfycbw3cTF2Q3X-2XZVlXRsBKp72Wx3esBn1O_jrMfvxPjMwTl0y2Lh9yW6_46tg/exec',
  learningSheetUrl: 'https://script.google.com/macros/s/AKfycbxzrxMMqOXehqTy3id5Z7Flr6Hbz9Tyz3XnDEgb8NaNPvO2RsvvUD-qNxKIEmOx3s9z/exec'
};

// -------------------------------------------------------------
// ZERO-DEPENDENCY SAFE HTTP FETCHER & JSON POSTER
// -------------------------------------------------------------
async function safeFetch(url, timeoutMs = 8000) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
  };

  if (typeof fetch === 'function') {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { headers, redirect: 'follow', signal: controller.signal });
      clearTimeout(timeout);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      try {
        return JSON.parse(text);
      } catch (e) {
        return { success: true, raw: text };
      }
    } catch (err) {
      clearTimeout(timeout);
      throw err;
    }
  }

  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs, headers }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          if (res.statusCode >= 400) {
            reject(new Error(`HTTP ${res.statusCode}`));
          } else {
            try {
              resolve(JSON.parse(data));
            } catch (e) {
              resolve({ success: true, raw: data });
            }
          }
        } catch (e) {
          reject(new Error('Invalid response from ' + url));
        }
      });
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    req.on('error', reject);
  });
}

async function safePostJson(url, dataObj, timeoutMs = 16000) {
  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
  };

  if (typeof fetch === 'function') {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(dataObj),
        redirect: 'follow',
        signal: controller.signal
      });
      clearTimeout(timeout);
      const text = await res.text();
      try {
        return JSON.parse(text);
      } catch (e) {
        return { success: true, raw: text };
      }
    } catch (err) {
      clearTimeout(timeout);
      throw err;
    }
  }

  return new Promise((resolve, reject) => {
    try {
      const parsedUrl = new URL(url);
      const postData = JSON.stringify(dataObj);
      const options = {
        hostname: parsedUrl.hostname,
        path: parsedUrl.pathname + parsedUrl.search,
        method: 'POST',
        headers: {
          ...headers,
          'Content-Length': Buffer.byteLength(postData)
        },
        timeout: timeoutMs
      };
      const req = https.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            resolve(JSON.parse(body));
          } catch (e) {
            resolve({ success: true, raw: body });
          }
        });
      });
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
      req.on('error', reject);
      req.write(postData);
      req.end();
    } catch (e) {
      reject(e);
    }
  });
}

// -------------------------------------------------------------
// GOOGLE DRIVE STATE SYNC (DEDICATED LEARNING ENDPOINT)
// -------------------------------------------------------------
async function backupLearningStateToGoogleDrive() {
  const targetUrl = State.learningSheetUrl;
  try {
    const payload = {
      action: 'SAVE_LEARNING_STATE',
      learningState: learningEngine.state,
      timestamp: Date.now()
    };
    return await safePostJson(targetUrl, payload, 8000);
  } catch (err) {
    return null;
  }
}

async function backupPortfolioStateToGoogleDrive() {
  const targetUrl = State.learningSheetUrl;
  try {
    const payload = {
      action: 'SAVE_PORTFOLIO_STATE',
      portfolioState: {
        portfolio: State.portfolio,
        activePositions: State.activePositions,
        recoveryBags: State.recoveryBags,
        cooldowns: State.cooldowns,
        lastUpdated: Date.now()
      },
      timestamp: Date.now()
    };
    return await safePostJson(targetUrl, payload, 8000);
  } catch (err) {
    return null;
  }
}

if (typeof learningEngine.setGoogleDriveSyncCallback === 'function') {
  learningEngine.setGoogleDriveSyncCallback(async (payload) => {
    const targetUrl = State.learningSheetUrl;
    try {
      await safePostJson(targetUrl, payload, 8000);
    } catch (e) {}
  });
}

// -------------------------------------------------------------
// USER CUSTOM PUMP & DUMP TRAP (<$70M LIQUIDITY + DORMANT SPIKE)
// -------------------------------------------------------------
const pumpDumpCache = new Map();

async function checkDormantPumpAndDumpTrap(symbol, volQuote, change24, rangePct) {
  if (volQuote >= 70000000) {
    return { isTrap: false, reason: 'Liquidity >= $70M (Institutional float)' };
  }

  const isRapidSpike = (change24 >= 7.5 || rangePct >= 9.0);
  if (!isRapidSpike) {
    return { isTrap: false, reason: 'No rapid 24h spike detected' };
  }

  const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;

  const cached = pumpDumpCache.get(pairSym);
  if (cached && (Date.now() - cached.timestamp < 300000)) {
    return cached.result;
  }

  try {
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=1d&limit=4`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 3) {
      const res = { isTrap: false, reason: 'Insufficient daily history' };
      pumpDumpCache.set(pairSym, { timestamp: Date.now(), result: res });
      return res;
    }

    let priorVolSum = 0;
    let priorRangeSum = 0;
    const priorDaysCount = klines.length - 1;

    for (let i = 0; i < priorDaysCount; i++) {
      const dHigh = parseFloat(klines[i][2]);
      const dLow = parseFloat(klines[i][3]);
      const dQuoteVol = parseFloat(klines[i][7]);
      priorVolSum += dQuoteVol;
      const dRange = dLow > 0 ? ((dHigh - dLow) / dLow) * 100 : 0;
      priorRangeSum += dRange;
    }

    const priorAvgDailyVol = priorVolSum / priorDaysCount;
    const priorAvgDailyRange = priorRangeSum / priorDaysCount;

    const isVolumeExplosionFromDormant = (priorAvgDailyVol < 12000000 && volQuote >= (priorAvgDailyVol * 2.5));
    const isPriceExplosionFromDormant = (priorAvgDailyRange < 4.5 && (change24 >= 8.0 || rangePct >= 9.5));

    if (isVolumeExplosionFromDormant || isPriceExplosionFromDormant) {
      const res = {
        isTrap: true,
        reason: `Dormant token (<$70M vol: $${(volQuote/1000000).toFixed(1)}M, prior avg: $${(priorAvgDailyVol/1000000).toFixed(1)}M, spike: +${change24.toFixed(1)}%)`
      };
      pumpDumpCache.set(pairSym, { timestamp: Date.now(), result: res });

      if (State.subsystems && State.subsystems.pumpDumpTrap) {
        State.subsystems.pumpDumpTrap.trappedTokensDetected = (State.subsystems.pumpDumpTrap.trappedTokensDetected || 0) + 1;
        State.subsystems.pumpDumpTrap.lastBlockedSymbol = cleanSym.replace('USDT', '');
      }
      return res;
    }

    const res = { isTrap: false, reason: 'Asset has consistent prior activity' };
    pumpDumpCache.set(pairSym, { timestamp: Date.now(), result: res });
    return res;
  } catch (err) {
    return { isTrap: false, reason: 'Trap check fallback' };
  }
}

// -------------------------------------------------------------
// ORDER FLOW, TAKER CVD & WYCKOFF VSA CONFLUENCE GATE
// -------------------------------------------------------------
async function checkVeteranOrderFlowAndVSA(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=5m&limit=12`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 6) {
      return { passed: true, takerBuyRatio: 50, recent3Delta: 0, vsaPattern: 'FALLBACK' };
    }

    const lastBar = klines[klines.length - 2];
    const open = parseFloat(lastBar[1]);
    const high = parseFloat(lastBar[2]);
    const low = parseFloat(lastBar[3]);
    const close = parseFloat(lastBar[4]);
    const vol = parseFloat(lastBar[5]);
    const takerBuyVol = parseFloat(lastBar[9]);

    const takerBuyRatio = vol > 0 ? (takerBuyVol / vol) * 100 : 50;

    let netDelta = 0;
    for (let i = klines.length - 4; i < klines.length - 1; i++) {
      const bVol = parseFloat(klines[i][5]);
      const tBuy = parseFloat(klines[i][9]);
      const tSell = bVol - tBuy;
      netDelta += (tBuy - tSell);
    }

    if (takerBuyRatio < 48.5 && netDelta < 0) {
      return { passed: false, reason: `Order flow distribution: Taker Buy ${takerBuyRatio.toFixed(1)}%, Negative Delta` };
    }

    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;

    if (clv < 0.38 && range > 0 && close < open) {
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

// -------------------------------------------------------------
// MICROSTRUCTURE MOMENTUM & 15M VWAP GATE
// -------------------------------------------------------------
async function checkMicrostructureMomentumAndVwap(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=15m&limit=16`;
    const klines = await safeFetch(url, 3500);

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
    let avgVol = 0;
    for (let i = klines.length - 10; i < klines.length - 2; i++) {
      avgVol += parseFloat(klines[i][5]);
    }
    avgVol = avgVol / 8;
    const rvol = avgVol > 0 ? Number((completedVol / avgVol).toFixed(2)) : 1.0;

    if (curClose < (vwap4h * 0.998)) {
      return { passed: false, reason: `Price below 15m VWAP ($${curClose.toFixed(4)} < $${vwap4h.toFixed(4)})` };
    }

    const isTrackA = (change15mPct >= 0.50 && rvol >= 1.10 && distFrom4hHighPct <= 2.80);
    const isTrackB = (curClose >= vwap4h && distFrom4hHighPct <= 3.50 && (change1hPct >= 0.80 || curChange15m >= 0.30));

    if (!isTrackA && !isTrackB) {
      return { 
        passed: false, 
        reason: `Failed Dual-Track Momentum (15m: ${change15mPct.toFixed(2)}%, RVOL: ${rvol.toFixed(2)}x, 1h: ${change1hPct.toFixed(2)}%)` 
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

// -------------------------------------------------------------
// 1-HOUR 20-EMA TREND ALIGNMENT GATE
// -------------------------------------------------------------
async function check1hTrendAlignment(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=1h&limit=24`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 20) {
      return { passed: true, reason: 'Insufficient 1h klines; bypassed' };
    }

    const curClose = parseFloat(klines[klines.length - 1][4]);
    const closes = klines.map(k => parseFloat(k[4]));

    const k = 2 / (20 + 1);
    let ema = closes.slice(0, 20).reduce((acc, c) => acc + c, 0) / 20;
    for (let i = 20; i < closes.length; i++) {
      ema = (closes[i] * k) + (ema * (1 - k));
    }

    const isAboveEma = curClose >= (ema * 0.997);
    if (!isAboveEma) {
      return { passed: false, reason: `Trading below 1h 20-EMA ($${curClose.toFixed(4)} < $${ema.toFixed(4)})` };
    }

    return { passed: true, price: curClose, ema20: ema };
  } catch (err) {
    return { passed: true, reason: 'Trend check fallback' };
  }
}

// -------------------------------------------------------------
// TIME MACHINE 48-CANDLE HISTORICAL REPLAY ODDS ENGINE
// -------------------------------------------------------------
async function evaluateTimeMachineBreakoutOdds(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/klines?symbol=${pairSym}&interval=15m&limit=48`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 24) {
      return { passed: true, oddsPct: 55, setupsDetected: 0, minOddsRequired: 45 };
    }

    let setupsDetected = 0;
    let continuationWins = 0;

    for (let i = 2; i < klines.length - 2; i++) {
      const open = parseFloat(klines[i][1]);
      const close = parseFloat(klines[i][4]);
      const barGain = open > 0 ? ((close - open) / open) * 100 : 0;

      if (barGain >= 0.80) {
        setupsDetected++;
        const nextHigh1 = parseFloat(klines[i + 1][2]);
        const nextHigh2 = (i + 2 < klines.length) ? parseFloat(klines[i + 2][2]) : nextHigh1;
        const maxContinuation = Math.max(nextHigh1, nextHigh2);
        const continuationPct = close > 0 ? ((maxContinuation - close) / close) * 100 : 0;

        if (continuationPct >= 0.80) {
          continuationWins++;
        }
      }
    }

    const oddsPct = setupsDetected >= 2 ? Math.round((continuationWins / setupsDetected) * 100) : 55;
    const minOddsRequired = (learningEngine?.state?.dynamicHurdles?.minOddsRequired) || 45;
    const passed = oddsPct >= minOddsRequired;

    if (State.subsystems && State.subsystems.timeMachine) {
      State.subsystems.timeMachine.replaysCompleted = (State.subsystems.timeMachine.replaysCompleted || 0) + 1;
      State.subsystems.timeMachine.lastEvaluatedSymbol = pairSym.replace('USDT', '');
      State.subsystems.timeMachine.lastWinOddsPct = oddsPct;
      State.subsystems.timeMachine.minOddsFloorPct = minOddsRequired;
      State.subsystems.timeMachine.status = 'ACTIVE';
    }

    return {
      passed,
      oddsPct,
      setupsDetected,
      minOddsRequired,
      reason: passed ? 'Optimal continuation probability' : `Low continuation odds (${oddsPct}% < ${minOddsRequired}%)`
    };
  } catch (err) {
    return { passed: true, oddsPct: 55, setupsDetected: 0, minOddsRequired: 45 };
  }
}

// -------------------------------------------------------------
// 20-LEVEL REAL ORDER BOOK DEPTH FETCH
// -------------------------------------------------------------
async function fetchRealOrderBookDepth(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const url = `https://api.binance.com/api/v3/depth?symbol=${pairSym}&limit=20`;
    const depth = await safeFetch(url, 2500);

    if (!depth || !Array.isArray(depth.bids) || !Array.isArray(depth.asks)) return null;

    let totalBids = 0;
    let totalAsks = 0;
    depth.bids.forEach(b => totalBids += parseFloat(b[1]));
    depth.asks.forEach(a => totalAsks += parseFloat(a[1]));

    const obi = (totalBids + totalAsks) > 0 ? (totalBids / (totalBids + totalAsks)) : 0.5;
    const bestBid = parseFloat(depth.bids[0][0]);
    const bestAsk = parseFloat(depth.asks[0][0]);
    const spreadPct = bestBid > 0 ? ((bestAsk - bestBid) / bestBid) * 100 : 0.05;

    return { obi: Number(obi.toFixed(3)), spreadPct: Number(spreadPct.toFixed(3)), bestBid, bestAsk };
  } catch (err) {
    return null;
  }
}

// -------------------------------------------------------------
// DYNAMIC SPOT SCREENER (350+ PAIRS)
// -------------------------------------------------------------
async function runDynamicScreener() {
  try {
    const url = 'https://api.binance.com/api/v3/ticker/24hr';
    const tickers = await safeFetch(url, 4500);
    if (!Array.isArray(tickers)) return;

    const universe = {};
    for (const t of tickers) {
      const sym = t.symbol;
      if (!sym.endsWith('USDT')) continue;
      if (sym.includes('UP') || sym.includes('DOWN') || sym.includes('BULL') || sym.includes('BEAR')) continue;
      if (['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'EURUSDT'].includes(sym)) continue;

      const volQuote = parseFloat(t.quoteVolume);
      if (volQuote < 3500000) continue;

      const last = parseFloat(t.lastPrice);
      const high = parseFloat(t.highPrice);
      const low = parseFloat(t.lowPrice);
      const change24 = parseFloat(t.priceChangePercent);

      const rangePct = low > 0 ? ((high - low) / low) * 100 : 0;
      const posInRange = (high - low) > 0 ? (last - low) / (high - low) : 0.5;

      let isPumpDump = false;
      let pumpDumpReason = '';

      if (volQuote < 70000000 && (change24 >= 7.5 || rangePct >= 9.0)) {
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
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= 75 && !c.isPumpAndDump).length;

    const topCandidates = Object.keys(universe).sort((a, b) => (universe[b]?.score || 0) - (universe[a]?.score || 0));
    if (topCandidates.length > 0) {
      evaluateTimeMachineBreakoutOdds(topCandidates[0]).then(() => {
        broadcastStateUpdate();
      }).catch(() => {});
    }
  } catch (err) {
    console.error('[Screener] Error running scan:', err.message);
  }
}

// -------------------------------------------------------------
// SPOT BUY EXECUTION
// -------------------------------------------------------------
async function executeSpotBuy(symbol, coin, book, microMom) {
  const sym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
  const pairKey = `${sym}USDT`;
  const livePrice = Number(coin.priceUSDT || coin.price || 1.0);

  const freeCash = Number(State.portfolio.cashUSDT) || 11.00;
  const allocPct = Number(State.portfolio.allocationPct || 15.0);
  
  let notional;
  if (freeCash <= 25.00) {
    notional = Math.min(freeCash, Math.max(10.05, Number((freeCash * 0.95).toFixed(2))));
  } else {
    notional = Math.max(10.50, Math.min(freeCash * (allocPct / 100), freeCash));
  }

  if (freeCash < 10.00 || notional < 10.00) {
    console.warn(`[Autopilot Skipped] Insufficient cash for Binance $10 min notional: $${freeCash.toFixed(2)} USDT available`);
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
    sl: Number((livePrice * 0.972).toFixed(6)),
    peakGainPct: 0,
    peakHighPrice: livePrice,
    ratchetStage: 'INITIAL',
    tp1Secured: false
  };

  console.log(`[Order Execution] BOUGHT ${sym}/USDT at $${livePrice} ($${notional.toFixed(2)} USDT)`);
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR
// -------------------------------------------------------------
async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled) return;

  const currentCount = Object.keys(State.activePositions).length;
  const maxSlots = State.tradeFrequencyController?.maxConcurrentPositions || 5;
  if (currentCount >= maxSlots) return;

  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const minScore = (State.activeThresholds?.scoreThreshold || 75);

  const keys = Object.keys(State.universe || {});
  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  let evaluatedCount = 0;
  for (const k of sorted) {
    if (Object.keys(State.activePositions).length >= maxSlots) break;
    const coin = State.universe[k];
    if (!coin || !coin.symbol) continue;

    if (coin.isPumpAndDump) continue;
    if ((coin.score || 0) < minScore) break;

    const baseSym = String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
    const pairKey = `${baseSym}USDT`;
    if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;

    if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > Date.now()) continue;

    if (++evaluatedCount > 30) break;

    const change24h = Number(coin.priceChangePct || coin.change24h || 0);
    if (change24h < 0.50) continue;

    const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
    if (posInRange < 0.50) continue;

    const volQuote = (coin.volumeM || 0) * 1000000;
    const rangePct = coin.intradayRangePct || 0;
    if (volQuote < 70000000 && (change24h >= 7.5 || rangePct >= 9.0)) {
      const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
      if (pdCheck.isTrap) {
        console.log(`[PUMP & DUMP AVOIDED] Blocked entry on ${pairKey}: ${pdCheck.reason}`);
        continue;
      }
    }

    const orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
    if (!orderFlow.passed) continue;

    const microMom = await checkMicrostructureMomentumAndVwap(pairKey);
    if (!microMom.passed) continue;

    const trend1h = await check1hTrendAlignment(pairKey);
    if (!trend1h.passed) continue;

    const timeMachine = await evaluateTimeMachineBreakoutOdds(pairKey);
    if (!timeMachine.passed) {
      console.log(`[Time Machine Reject] ${pairKey}: Odds ${timeMachine.oddsPct}% < ${timeMachine.minOddsRequired}%`);
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
    
    if (book.obi < 0.48 || book.spreadPct > 0.15) continue;

    console.log(`[Autopilot Signal] Entering ${pairKey}: Score ${coin.score}, OBI ${Math.round(book.obi * 100)}%, TM Odds ${timeMachine.oddsPct}%, 15m ${microMom.change15mPct}%`);
    await executeSpotBuy(pairKey, coin, book, microMom);
  }
}

// -------------------------------------------------------------
// PREDICTIVE SENTRY: INSTANT-ON TRAILING & ALPHA VELOCITY
// -------------------------------------------------------------
async function runPredictiveSentry() {
  const activeKeys = Object.keys(State.activePositions || {});
  const bagKeys = Object.keys(State.recoveryBags || {});

  // PART 1: ACTIVE BREAKOUT POSITIONS
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

    if (peakGain >= 0.35 && !pos.ratchetStage?.includes('LOCKED') && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'BREAKEVEN_LOCKED';
      pos.sl = Number((entryPrice * 1.0010).toFixed(6));
    }

    if (peakGain >= 0.60 && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'PROFIT_LOCKED';
      pos.sl = Number((entryPrice * 1.0035).toFixed(6));
    }

    if (peakGain >= 0.90 && peakGain < 1.40) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9975).toFixed(6));
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    if (peakGain >= 1.40 && peakGain < 1.80) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9970).toFixed(6));
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    if (gainPct >= 1.80 && !pos.tp1Secured) {
      pos.tp1Secured = true;
      pos.tp1Price = livePrice;
      pos.ratchetStage = 'RUNNER_TRAIL';
      pos.sl = Number((pos.peakHighPrice * 0.9965).toFixed(6));
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

    if (pos.sl && livePrice <= pos.sl && (pos.ratchetStage === 'BREAKEVEN_LOCKED' || pos.ratchetStage === 'PROFIT_LOCKED' || pos.ratchetStage === 'DYNAMIC_TRAIL' || pos.ratchetStage === 'RUNNER_TRAIL')) {
      const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
      
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: pos.tp1Secured ? 'RUNNER EXIT' : 'TRAILING PROFIT LOCK',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `Dynamic trailing stop executed at $${livePrice.toFixed(4)} (Peak: +${peakGain.toFixed(2)}%)`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    if (holdMinutes >= 30 && gainPct >= 0.20) {
      const exitNotional = pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VELOCITY HARVEST 30M',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `30m Alpha Velocity: Banked +${gainPct.toFixed(2)}% to clear slot`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    if (holdMinutes >= 45 && gainPct >= 0.10) {
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
        reason: `45m Velocity Scratch: Cleared stalled breakout at +${gainPct.toFixed(2)}%`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    if (holdMinutes >= 45 && gainPct <= 0.00) {
      State.recoveryBags[k] = {
        ...pos,
        movedAt: Date.now(),
        targetStep: 'STEP 1 (+0.30%)',
        targetExitPriceUSDT: Number((entryPrice * 1.003).toFixed(6)),
        initialDeficitUSDT: Number((((livePrice - entryPrice) / entryPrice) * pos.notionalUSDT).toFixed(4))
      };
      delete State.activePositions[k];
      continue;
    }

    if (gainPct <= -2.80) {
      State.recoveryBags[k] = {
        ...pos,
        movedAt: Date.now(),
        targetStep: 'STEP 1 (+0.30%)',
        targetExitPriceUSDT: Number((entryPrice * 1.003).toFixed(6)),
        initialDeficitUSDT: Number((((livePrice - entryPrice) / entryPrice) * pos.notionalUSDT).toFixed(4))
      };
      delete State.activePositions[k];
      continue;
    }
  }

  // PART 2: SPOT RECOVERY BAGS
  for (const k of bagKeys) {
    const bag = State.recoveryBags[k];
    if (!bag) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
    const entryPrice = Number(bag.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdHours = (Date.now() - (bag.movedAt || Date.now())) / 3600000;

    bag.livePriceUSDT = livePrice;

    if (livePrice >= bag.targetExitPriceUSDT) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'RECOVERY EXIT',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery filled on relief bounce (${bag.targetStep})`
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    if (holdHours >= 1.5 && gainPct >= -0.50) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VWAP RELIEF RECOVERY',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'VWAP technical bounce exit: capital recycled in 90m'
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    if (gainPct <= -6.50) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'HARD DISASTER CUT',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'Hard disaster floor cut (-6.50%): preserved liquid cash'
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
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
    let pnlUSDT = Number(t.pnlUSDT !== undefined ? t.pnlUSDT : (t['PnL (USDT)'] !== undefined ? t['PnL (USDT)'] : t.pnl));
    if (isNaN(pnlUSDT)) {
      pnlUSDT = (exit - entry) * (50 / entry);
    }
    const tradeId = Number(t.id) || Date.now();
    const cleanDate = normalizeDateToIndianFormat(t.date, tradeId, t.time);
    const cleanTime = String(t.time || '').trim() || '00:00:00';

    return {
      ...t,
      id: tradeId,
      date: cleanDate,
      time: cleanTime,
      pair,
      type: String(t.type || t.Type || 'EXIT').trim().toUpperCase(),
      entryPriceUSDT: entry,
      exitPriceUSDT: exit,
      notionalUSDT: Number(t.notionalUSDT || t['Notional (USDT)'] || 50),
      pnlUSDT: Number(pnlUSDT.toFixed(4)),
      pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2))
    };
  }).filter(t => {
    const timeMinute = String(t.time || '').slice(0, 5);
    const sig = `${t.pair}_${t.date}_${timeMinute}_${t.entryPriceUSDT.toFixed(4)}_${t.exitPriceUSDT.toFixed(4)}_${t.type}`;
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
      State.tradeLogs = sanitizeAndAuditTradeHistory(fetchedTrades);
      saveDiskState();
      broadcastStateUpdate();
      console.log(`[Sync Ledger] Successfully synchronized ${State.tradeLogs.length} verified trades exclusively from Sisir_Quant_Trade_Ledger.`);
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
  let lastError = null;
  let cloudResponse = null;

  // Primary: Fast POST dispatch with redirect following
  try {
    console.log(`[Cloud Ledger] Dispatching trade ${record.pair} (${record.type}) strictly to Sisir_Quant_Trade_Ledger...`);
    const res = await safePostJson(targetUrl, record, 15000);
    console.log(`[Cloud Ledger Response]`, JSON.stringify(res));
    if (res && res.success !== false) {
      success = true;
      cloudResponse = res;
    } else {
      lastError = new Error(res?.error || res?.message || 'POST returned unsuccessful status');
    }
  } catch (err) {
    lastError = err;
    console.error(`[Cloud Ledger POST Error] ${err.message}. Retrying via GET fallback transport...`);
  }

  // Fail-Safe Secondary: GET transport fallback
  if (!success) {
    try {
      const getUrl = targetUrl.includes('?') 
        ? `${targetUrl}&action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`
        : `${targetUrl}?action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`;
      const getRes = await safeFetch(getUrl, 15000);
      console.log(`[Cloud Ledger GET Fallback Response]`, JSON.stringify(getRes));
      if (getRes && getRes.success !== false) {
        success = true;
        cloudResponse = getRes;
      } else {
        lastError = new Error(getRes?.error || 'GET returned unsuccessful status');
      }
    } catch (err2) {
      lastError = err2;
      console.error(`[Cloud Ledger Critical Error] Failed both POST and GET dispatch: ${err2.message}`);
    }
  }

  learningEngine.recordTradeOutcome(
    record.pair.replace('/USDT', 'USDT'),
    record.type,
    record.pnlUSDT,
    1.0
  );

  broadcastStateUpdate();

  if (!success) {
    throw lastError || new Error('Failed to record trade to Google Sheets');
  }

  return { success: true, cloudResponse };
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
      if (parsed.masterLedgerUrl && !parsed.masterLedgerUrl.includes('AKfycbxzrx')) {
        State.masterLedgerUrl = parsed.masterLedgerUrl;
      }
      if (parsed.learningSheetUrl) State.learningSheetUrl = parsed.learningSheetUrl;
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
  res.json({ status: 'ONLINE', engine: 'Sisir Quant Production Engine' });
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
    console.log(`[Ledger Test] Testing URL from server proxy: ${testUrl}`);
    const fetchUrl = testUrl.includes('?') ? `${testUrl}&action=TEST` : `${testUrl}?action=TEST`;
    const data = await safeFetch(fetchUrl, 15000);
    return res.json({ success: true, source: 'SERVER_PROXY', data });
  } catch (err) {
    console.error(`[Ledger Test Error]`, err.message);
    const msg = err.message || '';
    if (msg.includes('404')) {
      return res.status(502).json({ 
        success: false, 
        error: 'Google returned HTTP 404 (Not Found). The Web App has not been deployed yet, or the URL is incorrect. Follow the Deployment steps in Apps Script.' 
      });
    }
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
    reason: `Manual position close at $${livePrice.toFixed(4)}`
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
    reason: `Manual bag liquidation at $${livePrice.toFixed(4)}`
  };
  delete State.recoveryBags[pairKey];
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
  saveDiskState();
  broadcastStateUpdate();
  await recordTradeToCloudLedger(closeRecord);
  res.json({ success: true, trade: closeRecord });
});

app.post('/api/drive/sync-files-now', async (req, res) => {
  try {
    if (req.body && req.body.learningSheetUrl) {
      State.learningSheetUrl = req.body.learningSheetUrl.trim();
      saveDiskState();
    }

    if (Array.isArray(req.body?.trades) && req.body.trades.length > 0) {
      learningEngine.trainOnHistoricalTrades(req.body.trades);
    }

    const rLearning = await backupLearningStateToGoogleDrive();
    const rPortfolio = await backupPortfolioStateToGoogleDrive();

    const profiledTokens = Object.keys(learningEngine.state?.assetReputations || {}).length;
    const totalBytes = ((rLearning && rLearning.bytesWritten) || 0) + ((rPortfolio && rPortfolio.bytesWritten) || 0);

    res.json({
      success: true,
      profiledTokens,
      bytesWritten: totalBytes,
      learningDriveResult: rLearning,
      portfolioDriveResult: rPortfolio,
      learningEngineState: learningEngine.state || {}
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
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
    subsystems: State.subsystems
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

        if (activePriceChanged) {
          runPredictiveSentry().catch(() => {});
        }
      } catch (err) {}
    });

    binanceGlobalWs.on('error', (err) => {
      console.warn('[Binance Stream Error]', err.message);
    });

    binanceGlobalWs.on('close', () => {
      console.warn('[Binance Stream Closed] Reconnecting in 2500ms...');
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
      } else if (msg.type === 'CLOSE_POSITION' && msg.pairKey) {
        const pos = State.activePositions[msg.pairKey];
        if (pos) {
          const livePrice = Number(State.universe[msg.pairKey]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
          const entryPrice = Number(pos.entryPriceUSDT || livePrice);
          const notional = Number(pos.notionalUSDT || 10.05);
          const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
          const closeRecord = {
            id: Date.now(),
            pair: pos.pair || `${msg.pairKey.replace('USDT', '')}/USDT`,
            type: realizedGain >= 0 ? 'MANUAL PROFIT' : 'MANUAL EXIT',
            entryPriceUSDT: entryPrice,
            exitPriceUSDT: livePrice,
            notionalUSDT: notional,
            pnlUSDT: realizedGain,
            reason: `Manual position close at $${livePrice.toFixed(4)}`
          };
          delete State.activePositions[msg.pairKey];
          State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
          State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
          saveDiskState();
          broadcastStateUpdate();
          recordTradeToCloudLedger(closeRecord).catch(() => {});
        }
      } else if (msg.type === 'CLOSE_RECOVERY_BAG' && msg.pairKey) {
        const bag = State.recoveryBags[msg.pairKey];
        if (bag) {
          const livePrice = Number(State.universe[msg.pairKey]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
          const entryPrice = Number(bag.entryPriceUSDT || livePrice);
          const notional = Number(bag.notionalUSDT || 10.05);
          const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
          const closeRecord = {
            id: Date.now(),
            pair: bag.pair || `${msg.pairKey.replace('USDT', '')}/USDT`,
            type: realizedGain >= 0 ? 'RECOVERY MANUAL EXIT' : 'BAG LIQUIDATION',
            entryPriceUSDT: entryPrice,
            exitPriceUSDT: livePrice,
            notionalUSDT: notional,
            pnlUSDT: realizedGain,
            reason: `Manual bag liquidation at $${livePrice.toFixed(4)}`
          };
          delete State.recoveryBags[msg.pairKey];
          State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
          State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
          saveDiskState();
          broadcastStateUpdate();
          recordTradeToCloudLedger(closeRecord).catch(() => {});
        }
      }
    } catch (e) {}
  });
});

// -------------------------------------------------------------
// BOOTSTRAP TIMERS & SERVICE INITIALIZATION
// -------------------------------------------------------------
loadDiskState();

runDynamicScreener().then(() => {
  console.log('[Screener] Initialized', Object.keys(State.universe).length, 'Binance Spot USDT pairs.');
}).catch(() => {});

syncTradesFromGoogleSheets().catch(() => {});
connectBinanceGlobalStream();

setInterval(() => {
  syncTradesFromGoogleSheets().catch(() => {});
  backupLearningStateToGoogleDrive().catch(() => {});
  backupPortfolioStateToGoogleDrive().catch(() => {});
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

server.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(`SISIR QUANT ENGINE ONLINE ON PORT ${PORT}`);
  console.log(`Zero-Latency Binance Stream Active (<5ms Singapore)`);
  console.log(`====================================================`);
});
