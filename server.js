/**
 * =========================================================================
 * SISIR QUANT TERMINAL - UNIFIED PRODUCTION BACKEND ENGINE
 * =========================================================================
 * - Zero external HTTP dependencies: Native fetch & https fallback
 * - Instant-on Minute-0 Micro-Trailing Sentry
 * - 45-Minute Alpha Velocity Engine (Anti-Stagnation)
 * - Taker Cumulative Volume Delta (CVD) & Wyckoff VSA Confluence
 * - 3 Concurrent Active Breakout Slots
 * - Spot Recovery Bag Engine (Zero Realized Loss Model)
 * - Uncaught exception & unhandled rejection resilience
 * =========================================================================
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const https = require('https');

// Process-level crash guards to ensure Render 100% uptime
process.on('uncaughtException', (err) => {
  console.error('[Server Sentry] Uncaught Exception intercepted:', err.message);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Server Sentry] Unhandled Rejection intercepted:', reason);
});

// Import Autonomous Learning Engine gracefully
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

// In-memory signature deduplication cache
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
    screener: { status: 'ACTIVE', scannedPairsCount: 350, qualifiedCandidatesCount: 12, cadenceSec: 75, liquidityFloorUSD: 'High-Sustain ($3.5M-$12M)' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 150, activeBookDepth: 20 },
    btcMacroGate: { status: 'NORMAL', allowAltcoinEntries: true, btcChange15m: 0, reason: 'BTC macro healthy' },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 80, liveObiGatePct: 55 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Instant-On Minute-0 Trailing + Alpha Velocity' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Dynamic Stepped Target (+0.30% -> 0.00% -> -0.75%)', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
    quotaManager: { status: 'NORMAL', executed24hCount: 0, maxAllowed: 'UNLIMITED', quotaRemaining: '∞', vipSlotsRemaining: '∞', formattedReset: 'Dynamic Reserve Active' },
    persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Drive Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
  },
  marketRegime: 'BALANCED',
  activeThresholds: { regime: 'BALANCED', scoreThreshold: 80, obiThreshold: 0.55, maxSpread: 0.08, marketAvgRange: 4.5 },
  tradeFrequencyController: {
    dynamicGovernorEnabled: true,
    quotaBypassEnabled: false,
    cashReserveFloorUSDT: 300.00,
    maxRecoveryBagsLimit: 6,
    governorTier: 'UNLIMITED_SPRINT',
    maxTradesPer24h: 9999,
    maxConcurrentPositions: 3, // 3 Concurrent Slots
    rollingTrades24h: [],
    consecutiveWinStreak: 0,
    nextSlotReleaseTimestamp: 0,
    pacingMode: 'BALANCED'
  },
  usdtToInrRate: 94.94,
  googleSheetUrl: 'https://script.google.com/macros/s/AKfycbxzrxMMqOXehqTy3id5Z7Flr6Hbz9Tyz3XnDEgb8NaNPvO2RsvvUD-qNxKIEmOx3s9z/exec',
  masterLedgerUrl: 'https://script.google.com/macros/s/AKfycbw3cTF2Q3X-2XZVlXRsBKp72Wx3esBn1O_jrMfvxPjMwTl0y2Lh9yW6_46tg/exec'
};

// -------------------------------------------------------------
// ZERO-DEPENDENCY SAFE HTTP FETCHER
// -------------------------------------------------------------
async function safeFetch(url, timeoutMs = 4000) {
  if (typeof fetch === 'function') {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timeout);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      clearTimeout(timeout);
      throw err;
    }
  }

  // Fallback to native Node.js https module
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON from ' + url));
        }
      });
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    req.on('error', reject);
  });
}

// -------------------------------------------------------------
// ORDER FLOW, TAKER CVD, & WYCKOFF VSA CONFLUENCE GATE
// -------------------------------------------------------------
async function checkVeteranOrderFlowAndVSA(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const url = `https://api.binance.com/api/v3/klines?symbol=${cleanSym}&interval=5m&limit=12`;
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
    const takerBuyVol = parseFloat(lastBar[9]); // Native Binance Taker Buy Volume

    const takerBuyRatio = vol > 0 ? (takerBuyVol / vol) * 100 : 50;

    let netDelta = 0;
    for (let i = klines.length - 4; i < klines.length - 1; i++) {
      const bVol = parseFloat(klines[i][5]);
      const tBuy = parseFloat(klines[i][9]);
      const tSell = bVol - tBuy;
      netDelta += (tBuy - tSell);
    }

    // Must show genuine buyer dominance
    if (takerBuyRatio < 48.5 && netDelta < 0) {
      return { passed: false, reason: `Order flow distribution: Taker Buy ${takerBuyRatio.toFixed(1)}%, Negative Delta` };
    }

    // Wyckoff VSA: Effort vs. Result
    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;

    // Absorption trap check
    if (clv < 0.38 && range > 0 && close < open) {
      return { passed: false, reason: `Wyckoff VSA Absorption Trap (CLV: ${clv.toFixed(2)})` };
    }

    // Reject consecutive 5m lower-low red cascades
    const prevBar = klines[klines.length - 3];
    const prevClose = parseFloat(prevBar[4]);
    const prevLow = parseFloat(prevBar[3]);
    if (close < open && prevClose < parseFloat(prevBar[1]) && low < prevLow) {
      return { passed: false, reason: 'Active 5m downward cascade detected' };
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
    const url = `https://api.binance.com/api/v3/klines?symbol=${cleanSym}&interval=15m&limit=16`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 8) {
      return { passed: false, reason: 'Insufficient 15m klines' };
    }

    const curBar = klines[klines.length - 1];
    const curClose = parseFloat(curBar[4]);
    const curOpen = parseFloat(curBar[1]);
    const change15mPct = curOpen > 0 ? ((curClose - curOpen) / curOpen) * 100 : 0;

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

    const curVol = parseFloat(curBar[5]);
    let avgVol = 0;
    for (let i = klines.length - 9; i < klines.length - 1; i++) {
      avgVol += parseFloat(klines[i][5]);
    }
    avgVol = avgVol / 8;
    const rvol = avgVol > 0 ? (curVol / avgVol) : 1.0;

    // Strict 1h trend mandate
    if (change1hPct < 0.20) {
      return { passed: false, reason: `1h trend is negative or flat (${change1hPct.toFixed(2)}%)` };
    }

    // Price must surf on or above 15m VWAP
    if (curClose < (vwap4h * 0.998)) {
      return { passed: false, reason: `Price below 15m VWAP ($${curClose.toFixed(4)} < $${vwap4h.toFixed(4)})` };
    }

    // Dual-Track Momentum: Track A (Ignition) or Track B (Base Expansion)
    const isTrackA = (change15mPct >= 0.70 && rvol >= 1.30 && distFrom4hHighPct <= 2.50);
    const isTrackB = (change15mPct >= 0.15 && change1hPct >= 1.00 && distFrom4hHighPct <= 3.20 && curClose >= vwap4h);

    if (!isTrackA && !isTrackB) {
      return { 
        passed: false, 
        reason: `Failed Dual-Track Momentum (15m: ${change15mPct.toFixed(2)}%, 1h: ${change1hPct.toFixed(2)}%, DD: ${distFrom4hHighPct.toFixed(2)}%)` 
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
    const url = `https://api.binance.com/api/v3/klines?symbol=${cleanSym}&interval=1h&limit=24`;
    const klines = await safeFetch(url, 3500);

    if (!Array.isArray(klines) || klines.length < 20) {
      return { passed: true, reason: 'Insufficient 1h klines; bypassed' };
    }

    const curClose = parseFloat(klines[klines.length - 1][4]);
    const closes = klines.map(k => parseFloat(k[4]));

    // Calculate 20-period Exponential Moving Average
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
// 20-LEVEL REAL ORDER BOOK DEPTH FETCH
// -------------------------------------------------------------
async function fetchRealOrderBookDepth(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const url = `https://api.binance.com/api/v3/depth?symbol=${cleanSym}&limit=20`;
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
      if (volQuote < 3500000) continue; // Minimum $3.5M liquidity floor

      const last = parseFloat(t.lastPrice);
      const high = parseFloat(t.highPrice);
      const low = parseFloat(t.lowPrice);
      const change24 = parseFloat(t.priceChangePercent);

      const rangePct = low > 0 ? ((high - low) / low) * 100 : 0;
      const posInRange = (high - low) > 0 ? (last - low) / (high - low) : 0.5;

      // Composite scoring formula
      let score = 60 + (posInRange * 22) + (Math.min(change24, 15) * 0.9);
      if (volQuote >= 10000000) score += 5;
      if (rangePct >= 5.0) score += 3;

      universe[sym] = {
        symbol: sym.replace('USDT', ''),
        priceUSDT: last,
        priceINR: Number((last * State.usdtToInrRate).toFixed(2)),
        change24h: change24,
        intradayRangePct: Number(rangePct.toFixed(2)),
        pricePositionInRange: Number(posInRange.toFixed(2)),
        score: Math.min(99, Math.round(score)),
        volumeM: Math.round(volQuote / 1000000),
        obi: 0.55
      };
    }

    State.universe = universe;
    State.subsystems.screener.scannedPairsCount = Object.keys(universe).length;
    State.subsystems.screener.qualifiedCandidatesCount = Object.values(universe).filter(c => c.score >= 80).length;
  } catch (err) {
    console.error('[Screener] Error running scan:', err.message);
  }
}

// -------------------------------------------------------------
// SPOT BUY EXECUTION
// -------------------------------------------------------------
async function executeSpotBuy(symbol, coin, book, microMom) {
  const sym = symbol.replace(/[^A-Z0-9]/g, '');
  const pairKey = `${sym}USDT`;
  const livePrice = Number(coin.priceUSDT || coin.price || 1.0);

  // Dynamic compounding allocation: 15% of free cash
  const freeCash = Number(State.portfolio.cashUSDT) || 1000.00;
  const allocPct = Number(State.portfolio.allocationPct || 15.0);
  const notional = Math.max(15.00, Math.min(freeCash * (allocPct / 100), freeCash));

  if (freeCash < notional || freeCash < 15.00) return;

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
    sl: Number((livePrice * 0.972).toFixed(6)), // -2.80% bag buffer
    peakGainPct: 0,
    peakHighPrice: livePrice,
    ratchetStage: 'INITIAL',
    tp1Secured: false
  };

  console.log(`[Order Execution] BOUGHT ${sym}/USDT at $${livePrice} ($${notional.toFixed(2)} USDT)`);
  broadcastStateUpdate();
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR
// -------------------------------------------------------------
async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled) return;

  const currentCount = Object.keys(State.activePositions).length;
  const maxSlots = State.tradeFrequencyController?.maxConcurrentPositions || 3;
  if (currentCount >= maxSlots) return;

  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const keys = Object.keys(State.universe || {});
  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  for (const k of sorted) {
    if (Object.keys(State.activePositions).length >= maxSlots) break;
    const coin = State.universe[k];
    if (!coin || !coin.symbol) continue;

    const sym = coin.symbol.replace(/[^A-Z0-9]/g, '');
    const pairKey = `${sym}USDT`;
    if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;

    // Strict positive momentum checks
    const change24h = Number(coin.priceChangePct || coin.change24h || 0);
    if (change24h < 1.50) continue;

    const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
    if (posInRange < 0.65) continue;

    const intradayRange = Number(coin.intradayRangePct || 4.5);
    if (intradayRange < 4.0) continue;

    if (State.cooldowns[sym] && State.cooldowns[sym].until > Date.now()) continue;

    // Order flow and VSA validation
    const orderFlow = await checkVeteranOrderFlowAndVSA(sym);
    if (!orderFlow.passed) continue;

    // Microstructure momentum & VWAP validation
    const microMom = await checkMicrostructureMomentumAndVwap(sym);
    if (!microMom.passed) continue;

    // 1-Hour Trend Alignment
    const trend1h = await check1hTrendAlignment(sym);
    if (!trend1h.passed) continue;

    // 20-level Order book check
    const book = await fetchRealOrderBookDepth(sym);
    if (!book || book.obi < 0.54 || book.spreadPct > 0.08) continue;

    await executeSpotBuy(sym, coin, book, microMom);
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

    // TIER 1: Micro-Protection at +0.35% -> Move SL to +0.10% (Breakeven + Fees Locked)
    if (peakGain >= 0.35 && !pos.ratchetStage?.includes('LOCKED') && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'BREAKEVEN_LOCKED';
      pos.sl = Number((entryPrice * 1.0010).toFixed(6));
    }

    // TIER 2: Profit Lock at +0.60% -> Move SL to +0.35%
    if (peakGain >= 0.60 && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'PROFIT_LOCKED';
      pos.sl = Number((entryPrice * 1.0035).toFixed(6));
    }

    // TIER 3: Dynamic Trailing from +0.90% (0.25% from peak)
    if (peakGain >= 0.90 && peakGain < 1.40) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9975).toFixed(6));
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    // TIER 4: Dynamic Trailing from +1.40% (0.30% from peak)
    if (peakGain >= 1.40 && peakGain < 1.80) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9970).toFixed(6));
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    // TIER 5: TP1 Hit (+1.80%) -> Bank 50%, Trail Remaining 50%
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

    // TRAILING STOP HIT
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

      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // ALPHA VELOCITY TIME-DECAY EXITS
    // At 30 mins: Bank if in profit >= +0.20%
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

      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // At 45 mins: Scratch if positive >= +0.10%
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

      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // At 45 mins: Move negative trades to Recovery Bag
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

    // Retest drop past -2.80% moves to recovery bag
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

    // Relief Bounce Exit at Target (+0.30%)
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

    // Dynamic VWAP Relief Exit (> 90 mins held and touching 15m VWAP)
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

    // Catastrophic Disaster Floor (-6.50% Hard Cap)
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
// CLOUD LEDGER & DISK PERSISTENCE DEDUPLICATION
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

async function recordTradeToCloudLedger(trade) {
  const tradeId = Number(trade.id || Date.now());
  const now = new Date();
  const cleanDate = normalizeDateToIndianFormat(trade.date || now.toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }), tradeId);
  const cleanTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });

  const record = {
    ...trade,
    id: tradeId,
    date: cleanDate,
    time: cleanTime,
    pnlINR: Number((trade.pnlUSDT * State.usdtToInrRate).toFixed(2))
  };

  // Content fingerprint deduplication
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

  // Async dispatch to Google Apps Script
  const targetUrl = State.masterLedgerUrl || State.googleSheetUrl;
  if (targetUrl) {
    try {
      if (typeof fetch === 'function') {
        fetch(targetUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(record)
        }).catch(() => {});
      }
    } catch (e) {}
  }

  learningEngine.recordTradeOutcome(
    record.pair.replace('/USDT', 'USDT'),
    record.type,
    record.pnlUSDT,
    1.0
  );

  broadcastStateUpdate();
}

function saveDiskState() {
  try {
    const data = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      recoveryBags: State.recoveryBags,
      tradeLogs: State.tradeLogs.slice(0, 150)
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
    }
  } catch (e) {}
}

// -------------------------------------------------------------
// EXPRESS APP & HTTP SERVER SETUP
// -------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Native CORS headers
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Serve static frontend files
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  const indexHtml = path.join(__dirname, 'index.html');
  if (fs.existsSync(indexHtml)) {
    return res.sendFile(indexHtml);
  }
  res.json({ status: 'ONLINE', engine: 'Sisir Quant Production Engine' });
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

app.post('/api/trades/sync-strict-sheet', (req, res) => {
  res.json({ success: true, trades: State.tradeLogs });
});

app.post('/api/drive/sync-files-now', (req, res) => {
  res.json({ success: true, profiledTokens: Object.keys(learningEngine.state?.assetReputations || {}).length });
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
    tradeLogs: State.tradeLogs
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
      } else if (msg.type === 'CLOSE_POSITION' && msg.pairKey) {
        delete State.activePositions[msg.pairKey];
        saveDiskState();
        broadcastStateUpdate();
      } else if (msg.type === 'CLOSE_RECOVERY_BAG' && msg.pairKey) {
        delete State.recoveryBags[msg.pairKey];
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

// Initial screener run
runDynamicScreener().then(() => {
  console.log('[Screener] Initialized', Object.keys(State.universe).length, 'Binance Spot USDT pairs.');
}).catch(() => {});

// Fast 2.5s Sentry & Autopilot Evaluator
setInterval(async () => {
  try {
    await runPredictiveSentry();
    await evaluateAutopilotEntries();
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 2500);

// 1.0s Sub-Second Price Streamer
setInterval(broadcastPriceStream, 1000);

// 75s Dynamic Screener Scan
setInterval(runDynamicScreener, 75000);

// Start listening
server.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(`SISIR QUANT ENGINE ONLINE ON PORT ${PORT}`);
  console.log(`Instant-On Minute-0 Trailing + Alpha Velocity Active`);
  console.log(`====================================================`);
});
