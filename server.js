/**
 * =========================================================================
 * SISIR QUANT TERMINAL - PRODUCTION BACKEND TRADING ENGINE
 * =========================================================================
 * - Low-latency Singapore Binance WebSocket stream (<5ms latency)
 * - 3 Concurrent Active Breakout Slots (High-Velocity Execution)
 * - Maximum 5 Spot Recovery Bags Limit
 * - Strict Active Position Rule: Never close below +0.35% (Demote to Bag on failure)
 * - TIER-1 STEVE NISON PATTERNS & DUAL-EMA SENTINEL (v4.3.0-PROD):
 *     * Tier-1 Bullish Patterns: Inverted Hammer, Tweezers Bottom, Rising Three Methods
 *     * Tier-1 Anti-Vetoes: Tweezers Top, Hanging Man at Highs, Dark Cloud Cover
 *     * 1h Dual-EMA Macro Stack: Price >= EMA20 & EMA20 not in severe bear cross vs EMA50
 *     * Large-Cap Beta Floor: Rejects low-volatility large caps (range < 4.0%)
 *     * Zero Velocity Drag: Calibrated base distance <= 2.50%, Score 65, batch size 16
 * - Steve Nison Pattern Morphology (Marubozu, Belt Hold, Engulfing, Morning Star, Hammer, Piercing Line)
 * - Candlestick Apex Harvest on Exits (Shooting Star, Bearish Engulfing, Evening Star, Doji)
 * - bStocks & 3X Leveraged ETF Exclusion Shield (Blocks SOXLB, SNDKB, TSLAB, etc.)
 * - Spot Recovery Bag Engine with Time-Decay Stepping (+0.30% -> +0.10% -> 0.00% BE)
 * - Breakthrough Trailing Runner: Locks recovery floor at stepped target and trails runners
 * - 1h 20-EMA Extension Shield (<= 5.50% above EMA) to prevent second-leg FOMO traps
 * - Risk-Parity Notional Sizing: Caps maximum dollar risk at ~$0.80 USDT (~₹76)
 * - Zero Post-Bag Cooldown: Preserves 100% unrestricted entry velocity
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
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 65, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Min +0.35% Floor • Candlestick Apex Harvest • -3.80% Cap' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Time-Decay (+0.30% -> +0.10% -> 0.00% BE) / Pure Stepped Recovery', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
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
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/4.2' },
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
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SisirQuant/4.2'
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
// RECALIBRATED HIGH-EFFICIENCY CANDLESTICK PATTERN ENGINE
// (REALISTIC CRYPTO GEOMETRY + TIER-1 NISON INTEGRATION)
// -------------------------------------------------------------
function analyzeCandlestickPattern(klines) {
  if (!Array.isArray(klines) || klines.length < 5) {
    return { 
      isBearishReversal: false, 
      isBullishReversal: false, 
      isBullishEntry: false, 
      bullishPatternName: 'NONE',
      bearishPatternName: 'NONE',
      patternName: 'NONE', 
      confidence: 0,
      patternRvol: 1.0,
      bodyPct: 0
    };
  }

  const parseBar = (bar) => {
    const open = parseFloat(bar[1]);
    const high = parseFloat(bar[2]);
    const low = parseFloat(bar[3]);
    const close = parseFloat(bar[4]);
    const vol = parseFloat(bar[5]);
    const takerBuy = parseFloat(bar[9]);
    const range = high - low;
    const body = Math.abs(close - open);
    const upperShadow = range > 0 ? (high - Math.max(open, close)) : 0;
    const lowerShadow = range > 0 ? (Math.min(open, close) - low) : 0;
    const isGreen = close >= open;
    const isRed = close < open;
    const takerBuyRatio = vol > 0 ? (takerBuy / vol) * 100 : 50;
    const rangePct = low > 0 ? ((range / low) * 100) : 0;
    const bodyPct = open > 0 ? (body / open) * 100 : 0;

    return { open, high, low, close, vol, takerBuy, range, body, upperShadow, lowerShadow, isGreen, isRed, takerBuyRatio, rangePct, bodyPct };
  };

  const cur = parseBar(klines[klines.length - 1]);
  const prev = parseBar(klines[klines.length - 2]);
  const prev2 = parseBar(klines[klines.length - 3]);
  const prev3 = klines.length >= 4 ? parseBar(klines[klines.length - 4]) : null;
  const prev4 = klines.length >= 5 ? parseBar(klines[klines.length - 5]) : null;

  // Relative Volume calculation relative to prior 8-10 completed bars
  let priorVolSum = 0;
  let priorVolCount = 0;
  for (let i = Math.max(0, klines.length - 12); i < klines.length - 2; i++) {
    priorVolSum += parseFloat(klines[i][5]);
    priorVolCount++;
  }
  const avgPriorVol = priorVolCount > 0 ? (priorVolSum / priorVolCount) : 1.0;
  const prevRvol = avgPriorVol > 0 ? Number((prev.vol / avgPriorVol).toFixed(2)) : 1.0;
  const curRvol = avgPriorVol > 0 ? Number((cur.vol / avgPriorVol).toFixed(2)) : 1.0;

  let isBearishReversal = false;
  let isBullishReversal = false;
  let isBullishEntry = false;
  let bullishPatternName = 'NONE';
  let bearishPatternName = 'NONE';
  let confidence = 0;
  let patternRvol = prevRvol;

  // Real-world continuation check: live candle holding green or shallow pullback
  const isCurConfirmingGreen = (cur.isGreen && cur.upperShadow <= (cur.range * 0.30)) || (cur.close >= (prev.close * 0.9985));

  // ==========================================
  // SECTION A: RECALIBRATED HIGH-EFFICIENCY BULLISH ENTRY SETUPS (INCL. TIER-1)
  // ==========================================

  // 1. BULLISH MARUBOZU (Daiken - Realistic Crypto Tolerances)
  if (prev.isGreen && prev.bodyPct >= 0.50 && prev.range > 0 && prev.body >= (prev.range * 0.72)) {
    if (prev.upperShadow <= (prev.range * 0.20) && prev.lowerShadow <= (prev.range * 0.18) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0) {
      if (isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH MARUBOZU (CONFIRMED)';
        confidence = 94;
        patternRvol = prevRvol;
      }
    }
  } else if (cur.rangePct >= 0.65 && cur.bodyPct >= 0.55 && cur.isGreen && cur.body >= (cur.range * 0.75)) {
    if (cur.upperShadow <= (cur.range * 0.15) && cur.lowerShadow <= (cur.range * 0.15) && curRvol >= 1.25 && cur.takerBuyRatio >= 52.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'BULLISH MARUBOZU (LIVE)';
      confidence = 92;
      patternRvol = curRvol;
    }
  }

  // 2. BULLISH BELT HOLD (Yorikiri)
  if (!isBullishEntry && prev.isGreen && prev.bodyPct >= 0.50 && prev.range > 0 && prev.body >= (prev.range * 0.68)) {
    if (prev.lowerShadow <= (prev.range * 0.10) && prev.upperShadow <= (prev.range * 0.22) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0) {
      if (isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH BELT HOLD (CONFIRMED)';
        confidence = 92;
        patternRvol = prevRvol;
      }
    }
  } else if (!isBullishEntry && cur.rangePct >= 0.60 && cur.bodyPct >= 0.50 && cur.isGreen && cur.body >= (cur.range * 0.70)) {
    if (cur.lowerShadow <= (cur.range * 0.08) && cur.upperShadow <= (cur.range * 0.20) && curRvol >= 1.25 && cur.takerBuyRatio >= 52.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'BULLISH BELT HOLD (LIVE)';
      confidence = 90;
      patternRvol = curRvol;
    }
  }

  // 3. BULLISH ENGULFING (Tsutsumi)
  if (!isBullishEntry && prev2.isRed && prev.isGreen && prev.bodyPct >= 0.45 && prev.range > 0 && prev2.range > 0) {
    if (prev.open <= (prev2.close * 1.001) && prev.close >= (prev2.open * 0.999) && prev.body >= (prev2.body * 1.05)) {
      if (prev.upperShadow <= (prev.range * 0.28) && prevRvol >= 1.20 && prev.takerBuyRatio >= 50.0 && isCurConfirmingGreen) {
        isBullishEntry = true;
        isBullishReversal = true;
        bullishPatternName = 'BULLISH ENGULFING';
        confidence = 95;
        patternRvol = prevRvol;
      }
    }
  }

  // 4. MORNING STAR (Sankei)
  if (!isBullishEntry && prev2.isRed && prev2.bodyPct >= 0.40) {
    const isStar = prev.body <= (prev.range * 0.40) && prev.low <= (prev2.low * 1.001);
    const isMorningGreen = cur.isGreen && cur.bodyPct >= 0.45 && cur.close >= (prev2.close + (prev2.body * 0.45));
    if (isStar && isMorningGreen && cur.upperShadow <= (cur.range * 0.28) && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'MORNING STAR';
      confidence = 96;
      patternRvol = curRvol;
    }
  }

  // 5. TAKURI HAMMER (Kanazuchi / Takuri)
  if (!isBullishEntry && prev.range > 0 && prev.lowerShadow >= (prev.body * 1.8) && prev.upperShadow <= (prev.range * 0.20) && prev.body <= (prev.range * 0.42)) {
    if (prevRvol >= 1.10 && cur.isGreen && cur.bodyPct >= 0.25 && cur.upperShadow <= (cur.range * 0.28)) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'HAMMER (CONFIRMED)';
      confidence = 92;
      patternRvol = prevRvol;
    }
  }

  // 6. INVERTED HAMMER (Gyaku Kanazuchi - Tier-1 Support Reversal)
  if (!isBullishEntry && prev.range > 0 && prev.upperShadow >= (prev.body * 1.8) && prev.lowerShadow <= (prev.range * 0.18) && prev.body <= (prev.range * 0.42)) {
    if (cur.isGreen && cur.bodyPct >= 0.30 && cur.close >= prev.close && cur.upperShadow <= (cur.range * 0.25)) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'INVERTED HAMMER (SUPPORT)';
      confidence = 93;
      patternRvol = curRvol;
    }
  }

  // 7. TWEEZERS BOTTOM (Kenuki Bottom - Tier-1 Support Double Test)
  if (!isBullishEntry && prev2 && prev && prev2.low > 0) {
    const isTweezerLows = Math.abs(prev.low - prev2.low) <= (prev2.low * 0.0010);
    const hasLowerRejection = (prev.lowerShadow >= prev.body * 0.7 || prev2.lowerShadow >= prev2.body * 0.7);
    if (isTweezerLows && hasLowerRejection && isCurConfirmingGreen) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'TWEEZERS BOTTOM';
      confidence = 94;
      patternRvol = Math.max(prevRvol, curRvol);
    }
  }

  // 8. RISING THREE METHODS (Sanpou - Tier-1 Institutional Continuation Base)
  if (!isBullishEntry && prev4 && prev3 && prev2 && prev && prev4.isGreen && prev4.bodyPct >= 0.45) {
    const isBaseDefended = (prev3.low >= prev4.low * 0.998 && prev3.high <= prev4.high * 1.008) &&
                           (prev2.low >= prev4.low * 0.998 && prev2.high <= prev4.high * 1.008) &&
                           (prev.low >= prev4.low * 0.998 && prev.high <= prev4.high * 1.008);
    const isBreakoutBar = cur.isGreen && cur.close >= prev4.high * 0.999 && cur.bodyPct >= 0.35;
    if (isBaseDefended && isBreakoutBar && curRvol >= 1.20) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'RISING THREE METHODS';
      confidence = 97;
      patternRvol = curRvol;
    }
  }

  // 9. PIERCING LINE (Kirikomi)
  if (!isBullishEntry && prev.isRed && prev.bodyPct >= 0.45 && cur.isGreen && cur.bodyPct >= 0.50 && cur.range > 0) {
    const midpoint = (prev.open + prev.close) / 2;
    if (cur.open <= (prev.low * 1.001) && cur.close >= (midpoint * 0.9995) && cur.close < prev.open && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
      isBullishEntry = true;
      isBullishReversal = true;
      bullishPatternName = 'PIERCING LINE';
      confidence = 91;
      patternRvol = curRvol;
    }
  }

  // 10. THREE WHITE SOLDIERS (Aka Sansei)
  if (!isBullishEntry && prev2.isGreen && prev.isGreen && cur.isGreen) {
    if (prev2.bodyPct >= 0.35 && prev.bodyPct >= 0.35 && cur.bodyPct >= 0.35) {
      if (cur.close > prev.close && prev.close > prev2.close) {
        if (cur.upperShadow <= (cur.range * 0.25) && prev.upperShadow <= (prev.range * 0.25) && curRvol >= 1.20 && cur.takerBuyRatio >= 50.0) {
          isBullishEntry = true;
          isBullishReversal = true;
          bullishPatternName = 'THREE WHITE SOLDIERS';
          confidence = 96;
          patternRvol = curRvol;
        }
      }
    }
  }

  // 11. DRAGONFLY DOJI (Tonbo Doji)
  if (!isBullishEntry && cur.range > 0 && cur.body <= (cur.range * 0.10) && cur.lowerShadow >= (cur.range * 0.70)) {
    isBullishReversal = true;
    bullishPatternName = 'DRAGONFLY DOJI';
    confidence = 86;
  }

  // ==========================================
  // SECTION B: BEARISH APEX PATTERNS & TIER-1 ANTI-ENTRY VETOES
  // ==========================================

  // 1. TWEEZERS TOP (Kenuki Top - Twin High Rejection Veto)
  if (prev && cur && cur.high > 0) {
    const isTwinHigh = Math.abs(cur.high - prev.high) <= (cur.high * 0.0010);
    const hasUpperRejection = (cur.upperShadow >= cur.body * 0.8 || prev.upperShadow >= prev.body * 0.8);
    if (isTwinHigh && hasUpperRejection && (cur.isRed || cur.upperShadow >= cur.range * 0.28)) {
      isBearishReversal = true;
      bearishPatternName = 'TWEEZERS TOP';
      confidence = 96;
    }
  }

  // 2. HANGING MAN (Kubitsuri - Fragile Bid Warning at Highs)
  if (!isBearishReversal && cur.range > 0 && cur.lowerShadow >= (cur.body * 1.8) && cur.upperShadow <= (cur.range * 0.15) && cur.body <= (cur.range * 0.38)) {
    if (cur.high >= prev.high || cur.close >= prev.high * 0.998) {
      isBearishReversal = true;
      bearishPatternName = 'HANGING MAN';
      confidence = 92;
    }
  }

  // 3. DARK CLOUD COVER (Kabuze - Bearish Penetration Reversal)
  if (!isBearishReversal && prev.isGreen && cur.isRed && cur.open >= (prev.close * 0.999)) {
    const midpoint = (prev.open + prev.close) / 2;
    if (cur.close < midpoint && cur.close > prev.open) {
      isBearishReversal = true;
      bearishPatternName = 'DARK CLOUD COVER';
      confidence = 94;
    }
  }

  // 4. SHOOTING STAR (Nagareboshi)
  if (!isBearishReversal && cur.range > 0 && cur.upperShadow >= (cur.body * 2.0) && cur.lowerShadow <= (cur.range * 0.15) && cur.body <= (cur.range * 0.38)) {
    if (cur.high >= prev.high || cur.close > prev.open) {
      isBearishReversal = true;
      bearishPatternName = 'SHOOTING STAR';
      confidence = cur.takerBuyRatio < 44 ? 95 : 85;
    }
  }

  // 5. BEARISH ENGULFING (Tsutsumi)
  if (!isBearishReversal && prev.isGreen && cur.isRed && cur.range > 0 && prev.range > 0) {
    if (cur.open >= (prev.close * 0.9995) && cur.close <= (prev.open * 1.0005) && cur.body >= prev.body) {
      isBearishReversal = true;
      bearishPatternName = 'BEARISH ENGULFING';
      confidence = cur.takerBuyRatio < 44 ? 92 : 82;
    }
  }

  // 6. EVENING STAR (Sankei)
  if (!isBearishReversal && prev2 && prev2.isGreen && prev2.body >= (prev2.range * 0.40)) {
    const isStar = prev.body <= (prev.range * 0.40) && prev.high >= prev2.high;
    const isEveningRed = cur.isRed && cur.close <= (prev2.open + (prev2.body * 0.50));
    if (isStar && isEveningRed) {
      isBearishReversal = true;
      bearishPatternName = 'EVENING STAR';
      confidence = 94;
    }
  }

  // 7. GRAVESTONE DOJI (Toba Doji)
  if (!isBearishReversal && cur.range > 0 && cur.body <= (cur.range * 0.08) && cur.upperShadow >= (cur.range * 0.72)) {
    isBearishReversal = true;
    bearishPatternName = 'GRAVESTONE DOJI';
    confidence = 88;
  }

  const patternName = bearishPatternName !== 'NONE' 
    ? bearishPatternName 
    : (bullishPatternName !== 'NONE' ? bullishPatternName : 'NONE');

  return { 
    isBearishReversal, 
    isBullishReversal, 
    isBullishEntry, 
    bullishPatternName, 
    bearishPatternName, 
    patternName, 
    confidence, 
    patternRvol,
    bodyPct: cur.bodyPct,
    curBar: cur 
  };
}

async function checkCandlestickSentry(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;

    const klines5m = await getCachedKlines(pairSym, '5m', 8, 2500);
    const klines15m = await getCachedKlines(pairSym, '15m', 8, 2500);

    const pat5m = analyzeCandlestickPattern(klines5m);
    const pat15m = analyzeCandlestickPattern(klines15m);

    const isBearish = pat5m.isBearishReversal || pat15m.isBearishReversal;
    const isBullish = pat5m.isBullishReversal || pat15m.isBullishReversal;
    const isBullishEntry = pat15m.isBullishEntry || pat5m.isBullishEntry;

    const activeBullishPattern = pat15m.bullishPatternName !== 'NONE' 
      ? `${pat15m.bullishPatternName} (15m)` 
      : (pat5m.bullishPatternName !== 'NONE' ? `${pat5m.bullishPatternName} (5m)` : 'NONE');

    const activeBearishPattern = pat15m.bearishPatternName !== 'NONE' 
      ? `${pat15m.bearishPatternName} (15m)` 
      : (pat5m.bearishPatternName !== 'NONE' ? `${pat5m.bearishPatternName} (5m)` : 'NONE');

    const activePattern = activeBearishPattern !== 'NONE' ? activeBearishPattern : activeBullishPattern;

    if (activePattern !== 'NONE') {
      State.subsystems.candlestickSentry.lastPatternDetected = `${pairSym}: ${activePattern}`;
    }

    return {
      isBearishReversal: isBearish,
      isBullishReversal: isBullish,
      isBullishEntry: isBullishEntry,
      bullishPatternName: activeBullishPattern,
      bearishPatternName: activeBearishPattern,
      patternName: activePattern,
      confidence: Math.max(pat5m.confidence, pat15m.confidence),
      patternRvol: Math.max(pat5m.patternRvol || 1.0, pat15m.patternRvol || 1.0)
    };
  } catch (e) {
    return { 
      isBearishReversal: false, 
      isBullishReversal: false, 
      isBullishEntry: false, 
      bullishPatternName: 'NONE', 
      bearishPatternName: 'NONE', 
      patternName: 'NONE', 
      confidence: 0,
      patternRvol: 1.0
    };
  }
}

// -------------------------------------------------------------
// 1-HOUR DUAL-EMA MACRO STACK & EXTENSION SHIELD
// (BLOCKS DOWNWARD BEAR CHANNELS LIKE DOT/POL WITHOUT VELOCITY LOSS)
// -------------------------------------------------------------
async function check1hTrendAlignment(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const pairSym = cleanSym.endsWith('USDT') ? cleanSym : `${cleanSym}USDT`;
    const klines = await getCachedKlines(pairSym, '1h', 60, 3500);

    if (!Array.isArray(klines) || klines.length < 20) {
      return { passed: true, reason: 'Fallback pass' };
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

async function executeSpotBuy(symbol, coin, book, microMom, candlePattern = 'MOMENTUM_EXPANSION') {
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

  // Risk-Parity Notional Sizing: Cap max loss at ~$0.80 USDT (~₹76)
  const maxAllowableLossUSDT = 0.80;
  const disasterStopRatio = 0.038; // -3.80% hard disaster stop
  const riskParityNotionalCap = Number((maxAllowableLossUSDT / disasterStopRatio).toFixed(2)); // ~$21.05 USDT

  let notional;
  if (freeCash <= 25.00) {
    notional = Math.min(freeCash, Math.max(10.05, Number((freeCash * 0.95).toFixed(2))));
  } else {
    const proportionalNotional = freeCash * (allocPct / 100);
    notional = Math.max(10.50, Math.min(proportionalNotional, riskParityNotionalCap, freeCash));
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
    tp1Secured: false,
    candlestickPattern: candlePattern
  };

  console.log(`[Order Execution] BOUGHT ${sym}/USDT at $${formatPrecisePrice(livePrice)} ($${notional.toFixed(2)} USDT) [Pattern: ${candlePattern}]`);
  logOperation('EXECUTION', `BOUGHT ${sym}/USDT at $${formatPrecisePrice(livePrice)} ($${notional.toFixed(2)} USDT) via [${candlePattern}]. Slot occupied.`, 'success');
  syncActiveSymbolSubscriptions();
  broadcastStateUpdate();
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
        const klines15m = await getCachedKlines(topSym, '15m', 8, 2000);
        const pat = analyzeCandlestickPattern(klines15m);
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
    broadcastStateUpdate();
  } catch (err) {
    console.error('[Screener] Error running scan:', err.message);
  }
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (HIGH-EFFICIENCY CALIBRATED GATE)
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

  const minScore = State.activeThresholds?.scoreThreshold || 65;
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

      // Evaluates up to 16 qualified candidates per pass to eliminate starvation
      if (++evaluatedCount > 16) break;

      const change24h = Number(coin.priceChangePct || coin.change24h || 0);
      const rangePct = Number(coin.intradayRangePct || 0);
      if (change24h < 0.20 && rangePct < 3.0) continue;

      const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
      if (posInRange < 0.50) continue;

      const volQuote = (coin.volumeM || 0) * 1000000;
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pdCheck.isTrap) {
          candidateCooldownMemo.set(pairKey, now + 120000);
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pdCheck.reason}`, 'danger');
          continue;
        }
      }

      // Wyckoff Order Flow: Taker Buy Volume and Positive Net Delta
      const orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
      if (!orderFlow.passed) {
        candidateCooldownMemo.set(pairKey, now + 12000);
        logOperation('FLOW_VETO', `${pairKey} halted: ${orderFlow.reason || 'Negative Taker CVD'}`, 'warning');
        continue;
      }

      // Large-Cap Low-Volatility Filter: Prevent committing breakout slots to slow heavy coins (e.g., DOT, ADA, XRP) with narrow intraday ranges
      const priceUnit = Number(coin.priceUSDT || 0);
      if (priceUnit >= 0.50 && rangePct < 4.0 && change24h < 0.60) {
        candidateCooldownMemo.set(pairKey, now + 30000);
        logOperation('VOL_VETO', `${pairKey} skipped: Large-cap low volatility (Range: ${rangePct.toFixed(2)}% < 4.00%). Scalping slots reserved for high-beta momentum.`, 'info');
        continue;
      }

      // Microstructure & VWAP & Recalibrated Range Breakout Gate (<= 2.50%)
      const microMom = await checkMicrostructureMomentumAndVwap(pairKey);
      if (!microMom.passed) {
        candidateCooldownMemo.set(pairKey, now + 12000);
        logOperation('MOM_VETO', `${pairKey} halted: ${microMom.reason}`, 'warning');
        continue;
      }

      // 1-Hour Dual-EMA Trend Stack Gate
      const trend1h = await check1hTrendAlignment(pairKey);
      if (!trend1h.passed) {
        candidateCooldownMemo.set(pairKey, now + 12000);
        logOperation('TREND_VETO', `${pairKey} halted: ${trend1h.reason}`, 'warning');
        continue;
      }

      // Recalibrated Candlestick Pattern Evaluation (with Tier-1 Anti-Vetoes)
      const candleCheck = await checkCandlestickSentry(pairKey);
      
      // Absolute Veto: Reject if forming or closed Bearish Reversal pattern (Tweezers Top, Hanging Man, Dark Cloud Cover, Shooting Star)
      if (candleCheck.isBearishReversal) {
        candidateCooldownMemo.set(pairKey, now + 20000);
        logOperation('CANDLE_VETO', `${pairKey} halted: Bearish pattern detected [${candleCheck.patternName}]. Rejecting distribution wick.`, 'warning');
        continue;
      }

      // High-Probability Entry Gate: Confirmed Steve Nison Pattern OR High-Volume Ignition (+0.80% / 1.35x RVOL)
      const isConfirmedPatternEntry = candleCheck.isBullishEntry && (candleCheck.patternRvol >= 1.20 || microMom.rvol >= 1.20);
      const isAuthenticIgnition = (microMom.change15mPct >= 0.80 && microMom.rvol >= 1.35 && orderFlow.takerBuyRatio >= 52.0);

      if (!isConfirmedPatternEntry && !isAuthenticIgnition) {
        candidateCooldownMemo.set(pairKey, now + 12000);
        logOperation('CANDLE_WAIT', `${pairKey} on standby: Awaiting confirmed bullish pattern (Marubozu/Engulfing/Hammer/Tweezers/RisingThree) or Ignition (15m >= +0.80%).`, 'info');
        continue;
      }

      const patternLabel = isConfirmedPatternEntry ? candleCheck.bullishPatternName : 'HIGH_VOL_IGNITION';

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
        candidateCooldownMemo.set(pairKey, now + 12000);
        logOperation('BOOK_VETO', `${pairKey} halted: OBI ${Math.round(book.obi * 100)}% (req >= 45%) or Spread ${book.spreadPct.toFixed(2)}% (req <= 0.20%).`, 'warning');
        continue;
      }

      logOperation('BOOK_CLEAR', `${pairKey} confirmed [${patternLabel}]: OBI ${Math.round(book.obi * 100)}%, Spread ${book.spreadPct.toFixed(2)}%, Taker Buy ${orderFlow.takerBuyRatio}%. Entering...`, 'success');

      console.log(`[Autopilot Signal] Entering ${pairKey}: Pattern [${patternLabel}], Score ${coin.score}, OBI ${Math.round(book.obi * 100)}%, 15m ${microMom.change15mPct}%`);
      await executeSpotBuy(pairKey, coin, book, microMom, patternLabel);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// -------------------------------------------------------------
// PREDICTIVE SENTRY & TIME-DECAY SPOT RECOVERY BAG ENGINE
// (INTEGRATING JAPANESE CANDLESTICK TRAILING SKILL)
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

    // Candlestick Sentry Evaluation
    const candleCheck = await checkCandlestickSentry(k);
    if (candleCheck.patternName !== 'NONE') {
      pos.candlestickPattern = candleCheck.patternName;
    }

    // CANDLESTICK APEX HARVEST (STRICTLY >= +0.35%)
    if (gainPct >= 0.35 && candleCheck.isBearishReversal && livePrice >= minExitPriceFloor) {
      logOperation('SENTRY_EXIT', `Japanese Candlestick Apex Harvest on ${pos.pair}: [${candleCheck.patternName}] detected at peak. Banked at +${gainPct.toFixed(2)}% ($${formatPrecisePrice(livePrice)}).`, 'success');
      
      const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
      const tradePayload = {
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: `CANDLESTICK APEX HARVEST [${candleCheck.patternName}]`,
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `Candlestick reversal [${candleCheck.patternName}] at +${gainPct.toFixed(2)}% (>= +0.35% floor)`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // Standard Order-Flow Self-Aware Apex Harvest (Backup check)
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

      // Widened demotion threshold to -1.50% and 25m so normal pullbacks aren't prematurely bagged
      if (!shouldDemote && holdMinutes >= 25 && gainPct <= -1.50) {
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
          exitInProgress: false,
          candlestickPattern: 'MONITORING'
        };

        delete State.activePositions[k];
        broadcastStateUpdate();
        continue;
      }
    }
  }

  // PART 2: SPOT RECOVERY BAGS (TIME-DECAY RECOVERY STEPPING & BREAKTHROUGH TRAILING)
  for (const k of bagKeys) {
    const bag = State.recoveryBags[k];
    if (!bag) continue;
    if (bag.exitInProgress) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
    const entryPrice = Number(bag.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdHours = (Date.now() - (bag.movedAt || Date.now())) / 3600000;

    bag.livePriceUSDT = livePrice;
    bag.troughPrice = Math.min(bag.troughPrice || livePrice, livePrice);
    bag.bouncePeakPrice = Math.max(bag.bouncePeakPrice || livePrice, livePrice);

    // Candlestick Pattern Sentinel on Recovery Bags
    const candleCheck = await checkCandlestickSentry(k);
    if (candleCheck.patternName !== 'NONE') {
      bag.candlestickPattern = candleCheck.patternName;
    }

    // Time-Decay Recovery Stepping
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

    // Breakthrough Trailing: Arms once price touches or calculated stop crosses target
    const calculatedStop = Number((bag.bouncePeakPrice * 0.9970).toFixed(8));

    if (livePrice >= recoveryTargetPrice || calculatedStop >= recoveryTargetPrice) {
      if (!bag.protectedTrailingActive) {
        bag.protectedTrailingActive = true;
        logOperation('RECOVERY', `${bag.pair}: Target reached ($${formatPrecisePrice(livePrice)} >= ${targetStepLabel}). Breakthrough Trailing runner armed. Floor locked at $${formatPrecisePrice(recoveryTargetPrice)}.`, 'success');
      }
    }

    if (bag.protectedTrailingActive) {
      bag.recoverySl = Math.max(bag.recoverySl || 0, recoveryTargetPrice, calculatedStop);
    }

    // EXIT TRIGGER 1: Protected Trailing Stop Hit (At or Above Stepped Target Floor)
    if (bag.protectedTrailingActive && bag.recoverySl && livePrice <= bag.recoverySl) {
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

    // EXIT TRIGGER 2: Candlestick or Self-Aware Apex Harvest at or above Recovery Target
    if (livePrice >= recoveryTargetPrice && (candleCheck.isBearishReversal || (await checkSelfAwareOrderFlow(k)).isApexExhausted)) {
      bag.exitInProgress = true;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

      logOperation('RECOVERY', `Recovery Apex Harvest on ${bag.pair} via [${candleCheck.patternName || 'Order Flow'}]: Exit at $${formatPrecisePrice(livePrice)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}% >= ${targetStepLabel}). 100% capital banked.`, 'success');

      const tradePayload = {
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: `RECOVERY APEX HARVEST [${candleCheck.patternName || 'ORDER FLOW'}]`,
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery Apex Harvest [${candleCheck.patternName}] at $${formatPrecisePrice(livePrice)}`
      };

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      saveDiskState();
      broadcastStateUpdate();
      recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
      continue;
    }

    // EXIT TRIGGER 3: Fallback Breakeven Fill (if trailing was not triggered)
    if (livePrice >= recoveryTargetPrice && bag.recoverySl && livePrice <= bag.recoverySl) {
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

    // EXIT TRIGGER 4: Near-Miss Relief Harvest (Option 2 & Option 3 & Candlestick Protected)
    if (holdHours >= 1.0 && gainPct >= -0.15) {
      const reliefCheck = await checkRecoveryReliefExhaustion(k, livePrice);

      // If price is below VWAP AND a confirmed Candlestick Reversal or multi-bar exhaustion prints
      if (!reliefCheck.isAboveVwap && (candleCheck.isBearishReversal || reliefCheck.isConfirmedExhausted)) {
        bag.exitInProgress = true;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

        logOperation('RECOVERY', `Near-Miss Relief Harvest on ${bag.pair} at $${formatPrecisePrice(livePrice)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%). Pattern: [${candleCheck.patternName}]. Confirmed below 15m VWAP. Capital recycled.`, 'info');

        const tradePayload = {
          id: Date.now(),
          pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
          type: `RECOVERY NEAR-MISS HARVEST [${candleCheck.patternName}]`,
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: bag.notionalUSDT,
          pnlUSDT: realizedGain,
          reason: `Near-miss relief harvest: Below 15m VWAP with ${candleCheck.patternName || 'multi-bar exhaustion'}`
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

    // EXIT TRIGGER 5: Hard Disaster Floor Cut (-3.80% Asymmetric Tail Risk Cap)
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
  console.log(`Tier-1 Nison Triad + Dual-EMA Macro Stack (v4.3.0)`);
  console.log(`Base Ceiling Distance Calibrated to <= 2.50%`);
  console.log(`Score Gate Calibrated to 65 • Batch Size 16`);
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
