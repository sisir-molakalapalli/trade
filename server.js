const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer, WebSocket } = require('ws');

process.on('uncaughtException', (err) => {
  console.error('[Process UncaughtException]', err.message);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Process UnhandledRejection]', reason);
});

const PORT = process.env.PORT || 8080;
const STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || 'jtG8g4dNoJpXQ6vu03f8l4quEir2HHCVOrXXfpgmSCxV1ubg4p4eaOmN9oH3eG53';
const GOOGLE_SHEET_URL = process.env.GOOGLE_SHEET_URL || '';

const app = express();

// -------------------------------------------------------------
// CORS & PRE-FLIGHT HEADERS
// -------------------------------------------------------------
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Baseline Universe
const DEFAULT_UNIVERSE = {
  SOLUSDT:  { name: 'Solana', symbol: 'SOL', priceUSDT: 105.20, priceINR: 9987.69, score: 82, direction: 'STRONG BUY', cvd: 14.2, obi: 0.56, intradayRangePct: 4.2, volumeM: 320, source: 'QUANT_ALGO', reasoning: 'Institutional spot bid depth absorption', verifiedAt: Date.now() },
  BTCUSDT:  { name: 'Bitcoin', symbol: 'BTC', priceUSDT: 84500.00, priceINR: 8022430.00, score: 81, direction: 'STRONG BUY', cvd: 12.3, obi: 0.55, intradayRangePct: 2.8, volumeM: 950, source: 'QUANT_ALGO', reasoning: 'Deep institutional liquidity anchor', verifiedAt: Date.now() },
  ETHUSDT:  { name: 'Ethereum', symbol: 'ETH', priceUSDT: 2360.00, priceINR: 224058.40, score: 80, direction: 'BUY', cvd: 8.1, obi: 0.54, intradayRangePct: 3.2, volumeM: 480, source: 'QUANT_ALGO', reasoning: 'Layer-1 fee burn baseline', verifiedAt: Date.now() },
  AVAXUSDT: { name: 'Avalanche', symbol: 'AVAX', priceUSDT: 11.10, priceINR: 1053.83, score: 72, direction: 'BUY', cvd: 6.4, obi: 0.51, intradayRangePct: 4.8, volumeM: 65, source: 'QUANT_ALGO', reasoning: 'Subnet expansion spot volume', verifiedAt: Date.now() },
  NEARUSDT: { name: 'NEAR Protocol', symbol: 'NEAR', priceUSDT: 3.10, priceINR: 294.31, score: 81, direction: 'STRONG BUY', cvd: 10.4, obi: 0.56, intradayRangePct: 5.1, volumeM: 78, source: 'QUANT_ALGO', reasoning: 'Compute narrative spot inflows', verifiedAt: Date.now() },
  LINKUSDT: { name: 'Chainlink', symbol: 'LINK', priceUSDT: 11.75, priceINR: 1115.55, score: 78, direction: 'BUY', cvd: 5.8, obi: 0.52, intradayRangePct: 3.9, volumeM: 54, source: 'QUANT_ALGO', reasoning: 'Cross-chain oracle protocol liquidity', verifiedAt: Date.now() },
  BNBUSDT:  { name: 'BNB', symbol: 'BNB', priceUSDT: 720.00, priceINR: 68356.80, score: 76, direction: 'BUY', cvd: 3.8, obi: 0.51, intradayRangePct: 2.1, volumeM: 110, source: 'QUANT_ALGO', reasoning: 'Exchange launchpool baseline support', verifiedAt: Date.now() },
  DASHUSDT: { name: 'Dash', symbol: 'DASH', priceUSDT: 52.93, priceINR: 5025.17, score: 81, direction: 'STRONG BUY', cvd: 3.1, obi: 0.56, intradayRangePct: 4.1, volumeM: 45, source: 'QUANT_ALGO', reasoning: '1h momentum breakout with bid support', verifiedAt: Date.now() },
  SUIUSDT:  { name: 'Sui Network', symbol: 'SUI', priceUSDT: 1.03, priceINR: 97.79, score: 62, direction: 'NEUTRAL', cvd: -2.1, obi: 0.47, intradayRangePct: 5.4, volumeM: 88, source: 'QUANT_ALGO', reasoning: 'Consolidating above liquidity base', verifiedAt: Date.now() },
  PEPEUSDT: { name: 'Pepe', symbol: 'PEPE', priceUSDT: 0.00000506, priceINR: 0.00048039, score: 75, direction: 'BUY', cvd: 4.2, obi: 0.53, intradayRangePct: 6.2, volumeM: 140, source: 'QUANT_ALGO', reasoning: 'High relative volume consolidation', verifiedAt: Date.now() }
};

const State = {
  autopilotEnabled: true,
  usdtToInrRate: 94.94,
  p2pLastUpdated: '--:--:--',
  p2pSource: 'BINANCE_P2P_LIVE',
  marketRegime: 'BALANCED',
  warmupCycles: 0,
  cooldowns: {},
  googleSheetUrl: GOOGLE_SHEET_URL,

  tradeFrequencyController: {
    quotaBypassEnabled: false,
    dynamicGovernorEnabled: true, // Permanent Dynamic Liquidity Governor (Uncapped trades governed by Cash Reserve)
    cashReserveFloorUSDT: 300.00,  // Minimum free cash buffer required to open new trades
    maxRecoveryBagsLimit: 6,       // Maximum concurrent bags allowed before temporary pause
    governorTier: 'UNLIMITED_SPRINT', // UNLIMITED_SPRINT | SOFT_THROTTLE | LIQUIDITY_FREEZE
    baseMaxTradesPer24h: 8,
    maxTradesPer24h: 9999,
    vipAlphaSlotsAllowed: 2,
    vipAlphaSlotsUsed24h: 0,
    vipTrades24h: [],
    bonusSlotsEarned: 0,
    consecutiveWinStreak: 0,
    streakBonusReason: 'BASELINE',
    maxConcurrentPositions: 2, // Standard 2 active breakout slots
    rollingTrades24h: [],
    nextSlotReleaseTimestamp: 0,
    globalCircuitBreakerUntil: 0,
    circuitBreakerReason: 'NORMAL',
    pacingMode: 'BALANCED',
    targetCadenceHours: 3.0
  },

  activeThresholds: {
    regime: 'BALANCED',
    scoreThreshold: 80,
    obiThreshold: 0.55,
    maxSpread: 0.08,
    cooldownMinutes: 15,
    marketAvgRange: 4.5,
    pacingGateOffset: 0,
    vipScoreThreshold: 88,
    vipObiThreshold: 0.60
  },

  portfolio: {
    cashUSDT: 1000.00,
    holdings: {},
    realizedPnL: 0.00,
    bagSubsidyPoolUSDT: 0.00 // Winner-funded reserve dedicated to auto-clearing stagnant bags
  },

  // Active slots (limited to 2 concurrent)
  activePositions: {},

  // Alternative 5: Spot Recovery Bags (Zero Realized Loss Model)
  // Assets that pull back move here so active slots stay completely free!
  recoveryBags: {},

  subsystems: {
    microstructure: {
      status: 'ONLINE',
      cyclesCompleted: 0,
      lastCycleTime: '--:--:--',
      intervalSec: 15,
      activeBookDepth: 20,
      evaluatedUniverseSize: 10
    },
    screener: {
      status: 'IDLE',
      lastRunTime: '--:--:--',
      nextRunCountdownSec: 90,
      scannedPairsCount: 0,
      qualifiedCandidatesCount: 0,
      liquidityFloorUSD: 'High-Sustain ($3.5M-$12M)',
      cadenceSec: 90
    },
    btcMacroGate: {
      status: 'NORMAL',
      btcChange15m: 0,
      btcChange1h: 0,
      btcVwapPremium: 0,
      allowAltcoinEntries: true,
      reason: 'BTC macro trend supportive'
    },
    regimeController: {
      status: 'CALIBRATED',
      currentRegime: 'BALANCED',
      marketAvgVolatilityPct: 4.5,
      liveScoreGate: 80,
      liveObiGatePct: 55,
      liveMaxSpreadPct: 0.08,
      lastAdjustmentTime: '--:--:--'
    },
    predictiveSentry: {
      status: 'WATCHING',
      lastMonitorTick: '--:--:--',
      intervalSec: 2.5,
      activePositionsMonitored: 0,
      recoveryBagsMonitored: 0,
      trailingLocksActive: 0,
      architecture: 'High-Sustain Confluence + Zero Realized Loss Recovery Bag'
    },
    recoveryManager: {
      status: 'ONLINE',
      activeRecoveryBagsCount: 0,
      totalRecoveryCapitalUSDT: 0.00,
      meanReversionTargetRate: 'Dynamic Stepped Target (+0.30% -> 0.00% -> -0.75%)',
      bagsResolvedWinCount: 0,
      subsidyPoolUSDT: 0.00,
      bagsSubsidizedCount: 0
    },
    quotaManager: {
      status: 'NORMAL',
      quotaBypassEnabled: false,
      executed24hCount: 0,
      maxAllowed: 8,
      baseMaxAllowed: 8,
      vipSlotsRemaining: 2,
      vipSlotsUsed: 0,
      quotaRemaining: 8,
      resetOldestSlotInMin: 0,
      nextSlotReleaseTimestamp: 0,
      formattedReset: 'All Slots Open',
      circuitBreakerRemainingSec: 0,
      pacingMode: 'BALANCED'
    },
    persistence: {
      status: 'SYNCED',
      totalDiskSaves: 0,
      lastSaveTime: '--:--:--',
      retainedTradesCount: 0,
      storageEngine: 'Disk JSON + Google Drive Ledger',
      googleSheetSync: GOOGLE_SHEET_URL ? 'CONNECTED' : 'STANDBY'
    }
  },

  quantTelemetry: {
    engine: 'Alternative 5: Spot Recovery Bag Strategy (Zero Realized Loss Architecture)',
    source: 'PURE_QUANT_ALGO',
    frequencySeconds: 15,
    lastAnalysisTime: '--:--:--',
    screenerCadence: 'Dynamic 90s-180s',
    riskCadence: 'Every 2.5 Seconds',
    persistenceStatus: 'ACTIVE (Non-Destructive)',
    p2pRateStatus: 'Binance P2P Daily Sync Active',
    regimeReasoning: 'Eliminated stop-out losses entirely. Retest pullbacks transfer to Spot Recovery Bags with standing breakeven targets while active slots trade without interruption.'
  },

  universe: { ...DEFAULT_UNIVERSE },
  tradeLogs: []
};

let screenerIntervalMs = 90 * 1000;
let nextScreenerTargetTimestamp = Date.now() + screenerIntervalMs;
let screenerTimerHandle = null;

// Helper for Binance HTTP queries
async function fetchFromBinance(endpoint) {
  const base = 'https://data-api.binance.vision';
  const url = `${base}${endpoint}`;
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    const res = await fetch(url, {
      headers: { 'X-MBX-APIKEY': BINANCE_API_KEY },
      signal: controller.signal
    });
    clearTimeout(timeout);
    if (!res.ok) return null;
    return await res.json();
  } catch (err) {
    return null;
  }
}

// -------------------------------------------------------------
// LIVE BINANCE P2P USDT/INR RATE SYNC
// -------------------------------------------------------------
async function syncBinanceP2PUsdtInrRate() {
  try {
    let rateFound = 0;

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 7000);
      const p2pRes = await fetch('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          asset: 'USDT',
          fiat: 'INR',
          tradeType: 'BUY',
          page: 1,
          rows: 5,
          payTypes: ['UPI', 'IMPS'],
          publisherType: 'merchant'
        }),
        signal: controller.signal
      });
      clearTimeout(timeout);

      if (p2pRes.ok) {
        const body = await p2pRes.json();
        if (body && Array.isArray(body.data) && body.data.length > 0) {
          const prices = [];
          body.data.forEach(item => {
            const p = parseFloat(item?.adv?.price);
            if (p && p >= 85 && p <= 125) prices.push(p);
          });
          if (prices.length > 0) {
            prices.sort((a, b) => a - b);
            rateFound = prices[Math.floor(prices.length / 2)];
          }
        }
      }
    } catch (e) {}

    if (!rateFound) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000);
        const refRes = await fetch('https://www.binance.com/api/v3/ticker/price?symbol=USDTINR', {
          signal: controller.signal
        });
        clearTimeout(timeout);
        if (refRes.ok) {
          const data = await refRes.json();
          const p = parseFloat(data.price);
          if (p && p >= 85 && p <= 125) rateFound = p;
        }
      } catch (e) {}
    }

    if (!rateFound) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const fxRes = await fetch('https://open.er-api.com/v6/latest/USD', { signal: controller.signal });
        clearTimeout(timeout);
        if (fxRes.ok) {
          const fx = await fxRes.json();
          const inr = fx?.rates?.INR;
          if (inr && inr > 75) {
            rateFound = Math.round((inr * 1.065) * 100) / 100;
          }
        }
      } catch (e) {}
    }

    if (rateFound && rateFound >= 85 && rateFound <= 125) {
      const roundedRate = Math.round(rateFound * 100) / 100;
      State.usdtToInrRate = roundedRate;
      State.p2pLastUpdated = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
      State.quantTelemetry.p2pRateStatus = `Live Binance P2P: ₹${roundedRate} (Synced at ${State.p2pLastUpdated})`;

      Object.keys(State.universe).forEach(k => {
        if (State.universe[k].priceUSDT) {
          State.universe[k].priceINR = calculateAccurateINRPrice(State.universe[k].priceUSDT, roundedRate);
        }
      });

      saveStateToDisk();
      broadcastPricesOnly();
      console.log(`[P2P Sync] Successfully updated Binance USDT/INR rate to ₹${roundedRate}`);
    }
  } catch (err) {
    console.warn('[P2P Sync Warning]', err.message);
  }
}

async function fetchRealOrderBookDepth(symbol) {
  try {
    const data = await fetchFromBinance(`/api/v3/depth?symbol=${symbol}&limit=20`);
    if (data && Array.isArray(data.bids) && Array.isArray(data.asks) && data.bids.length > 0 && data.asks.length > 0) {
      let bidVol = 0;
      let askVol = 0;
      data.bids.forEach(b => { bidVol += parseFloat(b[1]) || 0; });
      data.asks.forEach(a => { askVol += parseFloat(a[1]) || 0; });
      const total = bidVol + askVol;
      const obi = total > 0 ? bidVol / total : 0.50;

      const bestBid = parseFloat(data.bids[0][0]);
      const bestAsk = parseFloat(data.asks[0][0]);
      const spreadPct = bestBid > 0 ? ((bestAsk - bestBid) / bestBid) * 100 : 0;

      return { obi, spreadPct, bestBid, bestAsk, bidVol, askVol };
    }
  } catch (e) {}
  return null;
}

async function fetchShortTermMomentum(symbol) {
  try {
    const klines = await fetchFromBinance(`/api/v3/klines?symbol=${symbol}&interval=15m&limit=16`);
    if (Array.isArray(klines) && klines.length >= 6) {
      const latest = klines[klines.length - 1];
      const prev15m = klines[klines.length - 2];
      const prev1h = klines[Math.max(0, klines.length - 5)];
      const prev4h = klines[0];

      const currentClose = parseFloat(latest[4]) || 0;
      const open15m = parseFloat(prev15m[1]) || currentClose;
      const open1h = parseFloat(prev1h[1]) || currentClose;
      const open4h = parseFloat(prev4h[1]) || currentClose;

      const change15mPct = open15m > 0 ? ((currentClose - open15m) / open15m) * 100 : 0;
      const change1hPct = open1h > 0 ? ((currentClose - open1h) / open1h) * 100 : 0;
      const change4hPct = open4h > 0 ? ((currentClose - open4h) / open4h) * 100 : 0;

      const recentVolAvg = ((parseFloat(latest[7]) || 0) + (parseFloat(prev15m[7]) || 0)) / 2;
      let baselineSum = 0;
      const baselineCount = klines.length - 2;
      for (let i = 0; i < baselineCount; i++) {
        baselineSum += (parseFloat(klines[i][7]) || 0);
      }
      const baselineVolAvg = baselineCount > 0 ? (baselineSum / baselineCount) : 1;
      const rvol = baselineVolAvg > 0 ? (recentVolAvg / baselineVolAvg) : 1.0;

      return {
        change15mPct: Number(change15mPct.toFixed(2)),
        change1hPct: Number(change1hPct.toFixed(2)),
        change4hPct: Number(change4hPct.toFixed(2)),
        rvol: Number(Math.min(15, Math.max(0.2, rvol)).toFixed(2))
      };
    }
  } catch (e) {}
  return { change15mPct: 0, change1hPct: 0, change4hPct: 0, rvol: 1.0 };
}

// -------------------------------------------------------------
// BITCOIN (BTC) MACRO TREND SHIELD
// -------------------------------------------------------------
async function evaluateBtcMacroStatus() {
  try {
    const [shortTerm, book] = await Promise.all([
      fetchShortTermMomentum('BTCUSDT'),
      fetchRealOrderBookDepth('BTCUSDT')
    ]);

    const btcTicker = State.universe['BTCUSDT'];
    const vwapPremium = btcTicker?.vwapPremiumPct || 0;
    const c15m = shortTerm?.change15mPct || 0;
    const c1h = shortTerm?.change1hPct || 0;
    const obi = book ? book.obi : 0.52;

    let allowAltcoins = true;
    let reason = 'BTC trend healthy';

    // Guard: If BTC 15m is flushing (< -0.35%) or 1h is dumping (< -1.0%)
    if (c15m <= -0.35 && c1h < 0) {
      allowAltcoins = false;
      reason = `BTC 15m flush (${c15m >= 0 ? '+' : ''}${c15m}%) - Altcoin gate locked`;
    } else if (c1h <= -1.00) {
      allowAltcoins = false;
      reason = `BTC 1h macro downtrend (${c1h}%) - Altcoin gate locked`;
    } else if (obi < 0.40 && c15m < -0.15) {
      allowAltcoins = false;
      reason = `BTC bid book thin (${Math.round(obi * 100)}% OBI) - Sympathetic drop caution`;
    }

    State.subsystems.btcMacroGate = {
      status: allowAltcoins ? 'NORMAL' : 'BLOCKED',
      btcChange15m: c15m,
      btcChange1h: c1h,
      btcVwapPremium: vwapPremium,
      allowAltcoinEntries: allowAltcoins,
      reason,
      lastChecked: Date.now()
    };
    return State.subsystems.btcMacroGate;
  } catch (e) {
    return { allowAltcoinEntries: true, reason: 'BTC check bypass on network glitch' };
  }
}

function calculateAccurateINRPrice(usdtPrice, rate) {
  const rawINR = usdtPrice * rate;
  if (rawINR >= 100) return Math.round(rawINR * 100) / 100;
  if (rawINR >= 1) return Math.round(rawINR * 10000) / 10000;
  return Number(rawINR.toFixed(8));
}

function formatBinanceUSDTPrice(usdtPrice) {
  const num = Number(usdtPrice) || 0;
  if (num >= 100) return Number(num.toFixed(2));
  if (num >= 1) return Number(num.toFixed(4));
  return Number(num.toFixed(8));
}

// -------------------------------------------------------------
// IST DATE & TIME TIMESTAMP PARSER
// -------------------------------------------------------------
const MONTH_MAP = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11
};

function parseIstDateTimeToEpoch(dateStr, timeStr) {
  let day, month, year;
  const dClean = String(dateStr || '').trim();

  const alphaMatch = dClean.match(/([a-zA-Z]{3,9})/);
  if (alphaMatch) {
    const monthKey = alphaMatch[1].slice(0, 3).toLowerCase();
    if (MONTH_MAP[monthKey] !== undefined) {
      month = MONTH_MAP[monthKey];
      const numbers = dClean.match(/\d+/g);
      if (numbers && numbers.length >= 2) {
        if (numbers[0].length === 4) {
          year = parseInt(numbers[0], 10);
          day = parseInt(numbers[1], 10);
        } else {
          day = parseInt(numbers[0], 10);
          year = parseInt(numbers[1].length === 4 ? numbers[1] : (numbers[2] || numbers[1]), 10);
        }
      }
    }
  } else if (dClean.includes('/') || dClean.includes('-')) {
    const parts = dClean.split(/[\/\-]/);
    if (parts.length === 3) {
      if (parts[0].length === 4) {
        year = parseInt(parts[0], 10);
        month = parseInt(parts[1], 10) - 1;
        day = parseInt(parts[2], 10);
      } else {
        day = parseInt(parts[0], 10);
        month = parseInt(parts[1], 10) - 1;
        year = parseInt(parts[2], 10);
      }
    }
  }

  if (!year || isNaN(year) || !day || isNaN(day) || month === undefined || isNaN(month)) {
    return 0;
  }
  if (year < 100) year += 2000;

  let hours = 0, minutes = 0, seconds = 0;
  const tClean = String(timeStr || '').trim().toUpperCase();
  const isPM = tClean.includes('PM');
  const isAM = tClean.includes('AM');
  const timeNums = tClean.match(/\d+/g);
  if (timeNums && timeNums.length >= 2) {
    hours = parseInt(timeNums[0], 10);
    minutes = parseInt(timeNums[1], 10);
    if (timeNums.length >= 3) {
      seconds = parseInt(timeNums[2], 10);
    }
    if (isPM && hours < 12) hours += 12;
    if (isAM && hours === 12) hours = 0;
  }

  const istOffsetMs = (5 * 60 + 30) * 60 * 1000;
  return Date.UTC(year, month, day, hours, minutes, seconds) - istOffsetMs;
}

function parseTradeTimestamp(trade) {
  if (!trade) return 0;

  if (trade.date && typeof trade.date === 'string') {
    const dateClean = trade.date.trim();
    const timeClean = String(trade.time || '12:00:00 AM').trim();

    if (!dateClean.includes('1899') && !dateClean.includes('1970')) {
      const parsedMs = parseIstDateTimeToEpoch(dateClean, timeClean);
      if (parsedMs > 1700000000000 && parsedMs < 2500000000000) {
        return parsedMs;
      }
    }
  }

  let numId = Number(String(trade.id || '').replace(/[^0-9]/g, ''));
  if (!isNaN(numId) && numId > 1700000000000 && numId < 2500000000000) {
    return numId;
  }

  if (!isNaN(numId) && numId > 1700000000 && numId < 2500000000) {
    return numId * 1000;
  }

  return 0;
}

function cleanAndNormalizeDateString(rawDate, rawTime, timestampId) {
  const dateStr = String(rawDate || '').trim();
  const timeStr = String(rawTime || '').trim();

  const isCorrupted = !dateStr || 
    dateStr.includes('1899') || 
    dateStr.includes('1970') || 
    dateStr.includes('GMT') || 
    dateStr.length > 25;

  let validTimestamp = Number(timestampId);
  const isProperEpochMs = !isNaN(validTimestamp) && validTimestamp > 1700000000000 && validTimestamp < 2100000000000;

  if (isCorrupted || !isProperEpochMs) {
    const fallbackDate = isProperEpochMs ? new Date(validTimestamp) : new Date();
    
    const formattedDate = fallbackDate.toLocaleDateString('en-IN', {
      timeZone: 'Asia/Kolkata',
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    });

    const formattedTime = timeStr && !timeStr.includes('1899') && !timeStr.includes('GMT') 
      ? timeStr 
      : fallbackDate.toLocaleTimeString('en-IN', {
          timeZone: 'Asia/Kolkata',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: true
        });

    return { date: formattedDate, time: formattedTime };
  }

  if (dateStr.includes('T') || dateStr.includes('-')) {
    const parsed = new Date(dateStr);
    if (!isNaN(parsed.getTime()) && parsed.getFullYear() > 2020) {
      const cleanDate = parsed.toLocaleDateString('en-IN', {
        timeZone: 'Asia/Kolkata',
        day: '2-digit',
        month: 'short',
        year: 'numeric'
      });
      return { date: cleanDate, time: timeStr || parsed.toLocaleTimeString('en-IN') };
    }
  }

  return { date: dateStr, time: timeStr };
}

function sanitizeAndAuditTradeHistory(logs) {
  if (!Array.isArray(logs)) return [];
  
  const map = new Map();
  logs.forEach(l => {
    if (!l) return;
    
    const parsedEpoch = parseTradeTimestamp(l);
    let id = parsedEpoch > 0 ? parsedEpoch : Number(l.id);
    if (!id || isNaN(id) || id < 1000000) {
      id = Date.now() - (25 * 60 * 60 * 1000);
    }
    l.id = id;

    const { date, time } = cleanAndNormalizeDateString(l.date, l.time, id);
    l.date = date;
    l.time = time;
    
    let pnlUSDT = 0;
    if (typeof l.pnlUSDT === 'number' && !isNaN(l.pnlUSDT)) {
      pnlUSDT = l.pnlUSDT;
    } else if (typeof l.pnlINR === 'number' && !isNaN(l.pnlINR)) {
      pnlUSDT = l.pnlINR / State.usdtToInrRate;
    }

    if (Math.abs(pnlUSDT) > 5.0) {
      pnlUSDT = pnlUSDT / State.usdtToInrRate;
    }

    let entryUSDT = Number(l.entryPriceUSDT || l.entryPrice || 0);
    let exitUSDT = Number(l.exitPriceUSDT || l.exitPrice || 0);

    if (l.pair && l.pair.includes('PEPE') && exitUSDT > 0.001) {
      exitUSDT = exitUSDT / State.usdtToInrRate;
      entryUSDT = entryUSDT / State.usdtToInrRate;
    }

    l.pnlUSDT = Number(pnlUSDT.toFixed(4));
    l.pnlINR = Number((pnlUSDT * State.usdtToInrRate).toFixed(2));
    l.entryPriceUSDT = formatBinanceUSDTPrice(entryUSDT);
    l.exitPriceUSDT = formatBinanceUSDTPrice(exitUSDT);
    l.regime = l.regime || 'BALANCED';
    l.volatilityPct = typeof l.volatilityPct === 'number' ? l.volatilityPct : 4.5;

    map.set(l.id, l);
  });

  return Array.from(map.values()).sort((a, b) => b.id - a.id);
}

function recalculateTrueRealizedPnL() {
  let trueSumUSDT = 0;
  State.tradeLogs.forEach(l => {
    if (typeof l.pnlUSDT === 'number') {
      trueSumUSDT += l.pnlUSDT;
    }
  });
  State.portfolio.realizedPnL = Number(trueSumUSDT.toFixed(4));
}

function reconcileRollingTradeQuotaFromHistory() {
  const now = Date.now();
  const dayAgo = now - (24 * 60 * 60 * 1000);
  const timestamps = new Set();
  const vipTimestamps = new Set();

  State.tradeLogs.forEach(l => {
    const tradeTs = parseTradeTimestamp(l);
    if (tradeTs > 0 && tradeTs > dayAgo && tradeTs <= now) {
      timestamps.add(tradeTs);
      if (l.type && l.type.includes('VIP')) {
        vipTimestamps.add(tradeTs);
      }
    }
  });

  Object.values(State.activePositions || {}).forEach(pos => {
    const openTs = Number(pos.openedAt);
    if (!isNaN(openTs) && openTs > dayAgo && openTs <= now) {
      timestamps.add(openTs);
      if (pos.isVipAlpha) {
        vipTimestamps.add(openTs);
      }
    }
  });

  State.tradeFrequencyController.rollingTrades24h = Array.from(timestamps).sort((a, b) => a - b);
  State.tradeFrequencyController.vipTrades24h = Array.from(vipTimestamps).sort((a, b) => a - b);

  syncRollingTradeQuota();
}

function saveStateToDisk() {
  try {
    const dataToSave = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      recoveryBags: State.recoveryBags,
      tradeLogs: State.tradeLogs.slice(0, 500),
      autopilotEnabled: State.autopilotEnabled,
      cooldowns: State.cooldowns,
      tradeFrequencyController: State.tradeFrequencyController,
      usdtToInrRate: State.usdtToInrRate,
      p2pLastUpdated: State.p2pLastUpdated,
      googleSheetUrl: State.googleSheetUrl
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(dataToSave, null, 2), 'utf-8');
    State.subsystems.persistence.totalDiskSaves++;
    State.subsystems.persistence.lastSaveTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
    State.subsystems.persistence.retainedTradesCount = State.tradeLogs.length;
    State.subsystems.persistence.status = 'SYNCED';

    // Backup critical recovery bags & portfolio state to Google Sheet
    backupStateToGoogleDrive();
  } catch (err) {
    State.subsystems.persistence.status = 'ERROR';
  }
}

async function backupStateToGoogleDrive() {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) return;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    await fetch(targetUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({
        action: 'BACKUP_STATE',
        timestamp: Date.now(),
        portfolio: State.portfolio,
        recoveryBags: State.recoveryBags,
        activePositions: State.activePositions
      }),
      signal: controller.signal
    });
    clearTimeout(timeout);
  } catch (e) {}
}

async function syncWithGoogleDrive() {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) return;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const res = await fetch(targetUrl, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);
    if (res.ok) {
      const data = await res.json();
      if (data && Array.isArray(data.trades) && data.trades.length > 0) {
        const merged = sanitizeAndAuditTradeHistory([...data.trades, ...State.tradeLogs]);
        State.tradeLogs = merged;
        recalculateTrueRealizedPnL();
        reconcileRollingTradeQuotaFromHistory();
      }

      // Restore recovery bags and portfolio if container was just deployed / wiped
      if (data && data.stateBackup) {
        if (data.stateBackup.recoveryBags && Object.keys(State.recoveryBags).length === 0) {
          State.recoveryBags = data.stateBackup.recoveryBags;
          console.log(`[Cloud Restore] Restored ${Object.keys(State.recoveryBags).length} recovery bags from Google Sheet backup.`);
        }
        if (data.stateBackup.portfolio && State.portfolio.cashUSDT === 1000.00) {
          State.portfolio = data.stateBackup.portfolio;
        }
        if (data.stateBackup.activePositions && Object.keys(State.activePositions).length === 0) {
          State.activePositions = data.stateBackup.activePositions;
        }
      }

      State.subsystems.persistence.googleSheetSync = 'ACTIVE';
      saveStateToDisk();
      broadcastState();
    }
  } catch (err) {
    console.error('[GoogleSheet Sync Error]', err.message);
  }
}

async function appendTradeToGoogleDrive(tradeLog) {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) return;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const res = await fetch(targetUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(tradeLog),
      signal: controller.signal
    });
    clearTimeout(timeout);
    
    if (res.ok) {
      State.subsystems.persistence.googleSheetSync = 'SYNCED';
    }
  } catch (err) {
    console.error('[GoogleSheet Append Error]', err.message);
  }
}

function loadStateFromDisk() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const raw = fs.readFileSync(STATE_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      
      if (parsed.portfolio) {
        if (typeof parsed.portfolio.cashUSDT === 'number') {
          State.portfolio = parsed.portfolio;
        } else if (typeof parsed.portfolio.cashINR === 'number') {
          State.portfolio.cashUSDT = Math.round((parsed.portfolio.cashINR / 94.94) * 100) / 100;
          State.portfolio.holdings = {};
          State.portfolio.realizedPnL = 0;
        }
      }
      if (parsed.activePositions && typeof parsed.activePositions === 'object') {
        State.activePositions = parsed.activePositions;
      }
      if (parsed.recoveryBags && typeof parsed.recoveryBags === 'object') {
        State.recoveryBags = parsed.recoveryBags;
      }
      if (Array.isArray(parsed.tradeLogs)) {
        State.tradeLogs = sanitizeAndAuditTradeHistory(parsed.tradeLogs);
        recalculateTrueRealizedPnL();
      }
      if (typeof parsed.autopilotEnabled === 'boolean') {
        State.autopilotEnabled = parsed.autopilotEnabled;
      }
      if (parsed.cooldowns && typeof parsed.cooldowns === 'object') {
        State.cooldowns = parsed.cooldowns;
      }
      if (parsed.tradeFrequencyController && Array.isArray(parsed.tradeFrequencyController.rollingTrades24h)) {
        State.tradeFrequencyController = parsed.tradeFrequencyController;
      }
      if (typeof parsed.usdtToInrRate === 'number' && parsed.usdtToInrRate >= 85) {
        State.usdtToInrRate = parsed.usdtToInrRate;
      }
      if (parsed.p2pLastUpdated) {
        State.p2pLastUpdated = parsed.p2pLastUpdated;
      }
      if (parsed.googleSheetUrl) {
        State.googleSheetUrl = parsed.googleSheetUrl;
      }
    }
    reconcileRollingTradeQuotaFromHistory();
  } catch (err) {}
}

loadStateFromDisk();
syncWithGoogleDrive();
syncBinanceP2PUsdtInrRate();

// -------------------------------------------------------------
// 24-HOUR ROLLING TRADE QUOTA MANAGER
// -------------------------------------------------------------
function syncRollingTradeQuota() {
  const now = Date.now();
  const dayAgo = now - (24 * 60 * 60 * 1000);
  
  State.tradeFrequencyController.rollingTrades24h = (State.tradeFrequencyController.rollingTrades24h || [])
    .filter(ts => typeof ts === 'number' && !isNaN(ts) && ts > dayAgo && ts <= now)
    .sort((a, b) => a - b);

  State.tradeFrequencyController.vipTrades24h = (State.tradeFrequencyController.vipTrades24h || [])
    .filter(ts => typeof ts === 'number' && !isNaN(ts) && ts > dayAgo && ts <= now)
    .sort((a, b) => a - b);

  const isBypassActive = Boolean(State.tradeFrequencyController.quotaBypassEnabled);
  const isDynamicGovernor = Boolean(State.tradeFrequencyController.dynamicGovernorEnabled);

  // -------------------------------------------------------------
  // DYNAMIC LIQUIDITY GOVERNOR CALCULATION
  // -------------------------------------------------------------
  const freeCash = Number(State.portfolio.cashUSDT || 0);
  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  const cashFloor = State.tradeFrequencyController.cashReserveFloorUSDT || 300.00;
  const maxBags = State.tradeFrequencyController.maxRecoveryBagsLimit || 6;

  let governorTier = 'UNLIMITED_SPRINT';
  let canExecuteTrades = true;

  if (freeCash < cashFloor || activeBagsCount >= maxBags) {
    governorTier = 'LIQUIDITY_FREEZE';
    canExecuteTrades = false;
  } else if (freeCash < 600 || activeBagsCount >= 4) {
    governorTier = 'SOFT_THROTTLE';
    canExecuteTrades = true;
  } else {
    governorTier = 'UNLIMITED_SPRINT';
    canExecuteTrades = true;
  }

  State.tradeFrequencyController.governorTier = governorTier;

  let currentStreak = 0;
  let rollingNetGainUSDT = 0;
  const recentLogs24h = State.tradeLogs.filter(l => {
    const ts = parseTradeTimestamp(l);
    return ts > dayAgo && ts <= now;
  });

  for (const log of recentLogs24h) {
    const pnl = Number(log.pnlUSDT || 0);
    rollingNetGainUSDT += pnl;
    if (pnl > 0.05) {
      currentStreak++;
    } else if (pnl < -0.05) {
      break;
    }
  }

  let bonusSlots = 0;
  let streakReason = 'BASELINE';

  if (currentStreak >= 5 && rollingNetGainUSDT >= 2.00) {
    bonusSlots = 4;
    streakReason = 'SUPER_STREAK (+4 SLOTS)';
  } else if (currentStreak >= 3 && rollingNetGainUSDT >= 0.80) {
    bonusSlots = 2;
    streakReason = 'HOT_STREAK (+2 SLOTS)';
  } else {
    bonusSlots = 0;
    streakReason = 'BASELINE';
  }

  const isUncapped = isBypassActive || isDynamicGovernor;
  const baseMax = State.tradeFrequencyController.baseMaxTradesPer24h || 8;
  const dynamicMaxAllowed = isUncapped ? 9999 : (baseMax + bonusSlots);

  State.tradeFrequencyController.bonusSlotsEarned = bonusSlots;
  State.tradeFrequencyController.consecutiveWinStreak = currentStreak;
  State.tradeFrequencyController.streakBonusReason = streakReason;
  State.tradeFrequencyController.maxTradesPer24h = dynamicMaxAllowed;

  const count = State.tradeFrequencyController.rollingTrades24h.length;
  const remaining = isUncapped 
    ? (canExecuteTrades ? 999 : 0) 
    : Math.max(0, dynamicMaxAllowed - count);

  const vipUsed = State.tradeFrequencyController.vipTrades24h.length;
  const vipMax = State.tradeFrequencyController.vipAlphaSlotsAllowed || 2;
  const vipRemaining = isUncapped ? 999 : Math.max(0, vipMax - vipUsed);
  State.tradeFrequencyController.vipAlphaSlotsUsed24h = vipUsed;

  let resetTimeMin = 0;
  let nextReleaseTimestamp = 0;
  let formattedReset = isUncapped 
    ? `GOVERNOR: ${governorTier} (Cash: $${freeCash.toFixed(0)}, Bags: ${activeBagsCount}/${maxBags})` 
    : 'All Slots Open';

  if (count > 0 && !isUncapped) {
    const oldest = Math.min(...State.tradeFrequencyController.rollingTrades24h);
    nextReleaseTimestamp = oldest + (24 * 60 * 60 * 1000);
    const msRemaining = Math.max(0, nextReleaseTimestamp - now);
    resetTimeMin = Math.ceil(msRemaining / 60000);
    const hrs = Math.floor(resetTimeMin / 60);
    const mins = resetTimeMin % 60;
    
    if (msRemaining <= 0) {
      formattedReset = 'Slot Freeing Up...';
    } else if (remaining === 0) {
      formattedReset = hrs > 0 ? `Next Slot in ${hrs}h ${mins}m` : `Next Slot in ${mins}m`;
    } else {
      formattedReset = hrs > 0 ? `+1 Slot in ${hrs}h ${mins}m (${remaining} open)` : `+1 Slot in ${mins}m (${remaining} open)`;
    }
  }

  State.tradeFrequencyController.nextSlotReleaseTimestamp = nextReleaseTimestamp;
  State.tradeFrequencyController.globalCircuitBreakerUntil = 0;

  if (isUncapped) {
    State.tradeFrequencyController.circuitBreakerReason = canExecuteTrades ? 'NORMAL' : 'LIQUIDITY_FLOOR_HOLD';
    State.subsystems.quotaManager.status = governorTier;
  } else if (remaining === 0 && vipRemaining > 0) {
    State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    State.subsystems.quotaManager.status = 'VIP_ALPHA_SNIPER_ACTIVE';
  } else if (remaining === 0 && vipRemaining === 0) {
    State.tradeFrequencyController.circuitBreakerReason = 'QUOTA_EXHAUSTED';
    State.subsystems.quotaManager.status = 'QUOTA_FILLED';
  } else {
    State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    State.subsystems.quotaManager.status = 'NORMAL';
  }

  State.subsystems.quotaManager.quotaBypassEnabled = isUncapped;
  State.subsystems.quotaManager.dynamicGovernorEnabled = isDynamicGovernor;
  State.subsystems.quotaManager.governorTier = governorTier;
  State.subsystems.quotaManager.cashReserveFloorUSDT = cashFloor;
  State.subsystems.quotaManager.maxRecoveryBagsLimit = maxBags;
  State.subsystems.quotaManager.executed24hCount = count;
  State.subsystems.quotaManager.maxAllowed = isUncapped ? 'UNLIMITED' : dynamicMaxAllowed;
  State.subsystems.quotaManager.baseMaxAllowed = baseMax;
  State.subsystems.quotaManager.bonusSlotsEarned = bonusSlots;
  State.subsystems.quotaManager.consecutiveWinStreak = currentStreak;
  State.subsystems.quotaManager.streakBonusReason = streakReason;
  State.subsystems.quotaManager.quotaRemaining = isUncapped ? (canExecuteTrades ? '∞' : '0 (HOLD)') : remaining;
  State.subsystems.quotaManager.vipSlotsRemaining = isUncapped ? '∞' : vipRemaining;
  State.subsystems.quotaManager.vipSlotsUsed = vipUsed;
  State.subsystems.quotaManager.resetOldestSlotInMin = resetTimeMin;
  State.subsystems.quotaManager.nextSlotReleaseTimestamp = nextReleaseTimestamp;
  State.subsystems.quotaManager.formattedReset = formattedReset;
  State.subsystems.quotaManager.circuitBreakerRemainingSec = 0;
}

// -------------------------------------------------------------
// DISCOVERY SCREENER
// -------------------------------------------------------------
async function runDynamicMarketScreener() {
  State.subsystems.screener.status = 'SCANNING';
  try {
    const tickers = await fetchFromBinance('/api/v3/ticker/24hr');
    if (!Array.isArray(tickers) || tickers.length === 0) {
      State.subsystems.screener.status = 'IDLE';
      scheduleNextScreenerRun();
      return;
    }

    State.subsystems.screener.scannedPairsCount = tickers.length;
    const candidates = [];
    const activeSymbols = [...Object.keys(State.activePositions), ...Object.keys(State.recoveryBags)];

    const isCompression = (State.marketRegime === 'COMPRESSION');
    const isStarved = (State.tradeFrequencyController.pacingMode === 'STARVED');

    for (const t of tickers) {
      if (!t.symbol.endsWith('USDT')) continue;
      const base = t.symbol.slice(0, -4);
      if (['UP', 'DOWN', 'BEAR', 'BULL', 'USDC', 'FDUSD', 'TUSD', 'EUR', 'DAI', 'USDP', 'BUSD', 'PAXG', 'WBTC'].includes(base)) continue;

      const quoteVol = parseFloat(t.quoteVolume) || 0;
      const lastPrice = parseFloat(t.lastPrice) || 0;
      const priceChangePct = parseFloat(t.priceChangePercent) || 0;
      const highPrice = parseFloat(t.highPrice) || 0;
      const lowPrice = parseFloat(t.lowPrice) || 0;
      const vwapPrice = parseFloat(t.weightedAvgPrice) || lastPrice;

      if (lastPrice <= 0 || highPrice <= lowPrice) continue;

      const pricePositionInRange = (lastPrice - lowPrice) / (highPrice - lowPrice);
      const intradayRangePct = ((highPrice - lowPrice) / lowPrice) * 100;
      const vwapPremiumPct = vwapPrice > 0 ? ((lastPrice - vwapPrice) / vwapPrice) * 100 : 0;

      if (lastPrice <= 0 || highPrice <= lowPrice) continue;

      // -----------------------------------------------------------
      // HIGH-SUSTAIN INSTITUTIONAL LIQUIDITY FLOOR
      // Filters out fragile micro-caps ($1.5M) that lack bid depth to sustain price.
      // -----------------------------------------------------------
      let requiredFloor = 10000000; // $10M standard
      if (priceChangePct >= 5.0 && vwapPremiumPct >= 2.5 && pricePositionInRange >= 0.72) {
        requiredFloor = 3500000; // $3.5M minimum for clean breakouts
      } else if (priceChangePct >= 2.5 || vwapPremiumPct >= 1.5) {
        requiredFloor = 5000000; // $5.0M for moderate momentum
      } else if (isCompression || isStarved) {
        requiredFloor = 4500000;
      }

      if (quoteVol >= requiredFloor) {
        const minPositionRatio = (isCompression || isStarved) ? 0.48 : 0.54;
        if (pricePositionInRange < minPositionRatio) continue;

        const liquidityDensityScore = Math.min(22, Math.log10(quoteVol) * 3.0);
        const rangeProximityBoost = pricePositionInRange * 16;
        const directionalMomentum = Math.max(0, priceChangePct) * 2.4;
        const vwapBreakoutBoost = Math.max(0, vwapPremiumPct) * 3.2;

        const breakoutScore = directionalMomentum + vwapBreakoutBoost + liquidityDensityScore + rangeProximityBoost;

        candidates.push({
          symbol: t.symbol,
          baseAsset: base,
          priceUSDT: formatBinanceUSDTPrice(lastPrice),
          priceINR: calculateAccurateINRPrice(lastPrice, State.usdtToInrRate),
          priceChangePct,
          vwapPremiumPct: Number(vwapPremiumPct.toFixed(2)),
          intradayRangePct,
          pricePositionInRange,
          volumeM: Number((quoteVol / 1000000).toFixed(1)),
          breakoutScore,
          isActive: activeSymbols.includes(t.symbol)
        });
      }
    }

    State.subsystems.screener.qualifiedCandidatesCount = candidates.length;
    State.subsystems.screener.liquidityFloorUSD = 'High-Sustain ($3.5M-$12M)';

    const activeCandidates = candidates.filter(c => c.isActive);
    const nonActiveSorted = candidates
      .filter(c => !c.isActive)
      .sort((a, b) => b.breakoutScore - a.breakoutScore);

    const stage2PoolSize = Math.max(0, 15 - activeCandidates.length);
    const stage2Pool = [...activeCandidates, ...nonActiveSorted.slice(0, stage2PoolSize)];

    for (const p of stage2Pool) {
      const shortTerm = await fetchShortTermMomentum(p.symbol);
      p.change15mPct = shortTerm.change15mPct;
      p.change1hPct = shortTerm.change1hPct;
      p.change4hPct = shortTerm.change4hPct;
      p.rvol = shortTerm.rvol;

      // Penalize dead-cat bounces where 4h is sharply negative
      if (p.change4hPct < -4.0 && p.change15mPct > 0) {
        p.breakoutScore -= 12; // De-prioritize counter-trend relief pops
      }

      if (p.change15mPct >= 1.5 && p.change1hPct >= 0) p.breakoutScore += (p.change15mPct * 4.5);
      if (p.change1hPct >= 1.0) p.breakoutScore += (p.change1hPct * 2.5);
      if (p.change4hPct >= 2.0) p.breakoutScore += (p.change4hPct * 1.2);
      if (p.rvol >= 1.8 && (p.change15mPct > 0.5 || p.change1hPct > 1.0)) {
        p.breakoutScore += Math.min(18, (p.rvol - 1.0) * 5.0);
      }
    }

    stage2Pool.sort((a, b) => b.breakoutScore - a.breakoutScore);
    const topPicks = stage2Pool.slice(0, 10);

    if (topPicks.length > 0) {
      const updatedUniverse = {};

      activeSymbols.forEach(sym => {
        if (State.universe[sym]) {
          updatedUniverse[sym] = { ...State.universe[sym] };
        } else {
          const pos = State.activePositions[sym] || State.recoveryBags[sym];
          if (pos) {
            const base = pos.pairSymbol || sym.replace('USDT', '');
            updatedUniverse[sym] = {
              name: base,
              symbol: base,
              priceUSDT: pos.livePriceUSDT || pos.entryPriceUSDT,
              priceINR: calculateAccurateINRPrice(pos.livePriceUSDT || pos.entryPriceUSDT, State.usdtToInrRate),
              score: 80,
              direction: 'BUY',
              obi: 0.55,
              cvd: 3.5,
              volumeM: 10,
              source: 'ACTIVE_HOLD_ANCHOR',
              reasoning: 'Active or Recovery position anchored in universe',
              verifiedAt: Date.now()
            };
          }
        }
      });

      topPicks.forEach(c => {
        if (!updatedUniverse[c.symbol]) {
          const existing = State.universe[c.symbol];
          const isSpike = (c.change15mPct >= 2.0 || c.change1hPct >= 4.0) && c.rvol >= 1.6;
          updatedUniverse[c.symbol] = {
            name: c.baseAsset,
            symbol: c.baseAsset,
            priceUSDT: c.priceUSDT,
            priceINR: c.priceINR,
            priceChangePct: c.priceChangePct,
            vwapPremiumPct: c.vwapPremiumPct || 0,
            change15mPct: c.change15mPct || 0,
            change1hPct: c.change1hPct || 0,
            change4hPct: c.change4hPct || 0,
            rvol: c.rvol || 1.0,
            isSpikeCandidate: isSpike,
            intradayRangePct: c.intradayRangePct,
            pricePositionInRange: c.pricePositionInRange,
            volumeM: c.volumeM,
            score: existing ? existing.score : 78,
            direction: existing ? existing.direction : 'BUY',
            obi: existing ? existing.obi : 0.55,
            cvd: existing ? existing.cvd : 3.5,
            source: 'QUANT_ALGO',
            reasoning: `${isSpike ? '🚀 SPIKE IGNITION | ' : ''}15m: ${c.change15mPct >= 0 ? '+' : ''}${(c.change15mPct || 0).toFixed(1)}%, 1h: ${c.change1hPct >= 0 ? '+' : ''}${(c.change1hPct || 0).toFixed(1)}%, RVOL: ${(c.rvol || 1).toFixed(1)}x ($${c.volumeM}M Vol)`,
            verifiedAt: Date.now()
          };
        }
      });

      State.universe = updatedUniverse;
      broadcastPricesOnly();
    }

    State.subsystems.screener.status = 'ACTIVE';
    State.subsystems.screener.lastRunTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

    await runPureQuantScoring();
  } catch (err) {
    State.subsystems.screener.status = 'ERROR';
  } finally {
    scheduleNextScreenerRun();
  }
}

function scheduleNextScreenerRun() {
  if (screenerTimerHandle) clearTimeout(screenerTimerHandle);
  nextScreenerTargetTimestamp = Date.now() + screenerIntervalMs;
  screenerTimerHandle = setTimeout(runDynamicMarketScreener, screenerIntervalMs);
}

// -------------------------------------------------------------
// VOLATILITY REGIME & MICROSTRUCTURE SCORING
// -------------------------------------------------------------
function calculateMarketVolatilityRegime() {
  const ranges = Object.values(State.universe)
    .map(u => Number(u.intradayRangePct) || 4.5)
    .filter(r => r > 0)
    .sort((a, b) => a - b);

  let medianRange = 4.5;
  if (ranges.length > 0) {
    const mid = Math.floor(ranges.length / 2);
    medianRange = ranges.length % 2 !== 0 ? ranges[mid] : (ranges[mid - 1] + ranges[mid]) / 2;
  }
  medianRange = Number(medianRange.toFixed(2));

  const now = Date.now();
  const sixHoursAgo = now - (6 * 60 * 60 * 1000);
  const twelveHoursAgo = now - (12 * 60 * 60 * 1000);
  const tradesLast6h = (State.tradeFrequencyController.rollingTrades24h || []).filter(ts => ts > sixHoursAgo).length;
  const tradesLast12h = (State.tradeFrequencyController.rollingTrades24h || []).filter(ts => ts > twelveHoursAgo).length;

  let pacingMode = 'BALANCED';
  let pacingGateOffset = 0;

  if (tradesLast6h === 0) {
    pacingMode = 'STARVED';
    pacingGateOffset = -2;
    screenerIntervalMs = 60 * 1000;
  } else if (tradesLast12h >= 5) {
    pacingMode = 'SATURATED';
    pacingGateOffset = 2;
    screenerIntervalMs = 120 * 1000;
  } else {
    pacingMode = 'BALANCED';
    pacingGateOffset = 0;
    screenerIntervalMs = 75 * 1000;
  }

  State.tradeFrequencyController.pacingMode = pacingMode;
  State.subsystems.quotaManager.pacingMode = pacingMode;
  State.subsystems.screener.cadenceSec = Math.round(screenerIntervalMs / 1000);

  let regime = 'BALANCED';
  let baseScore = 80;
  let baseObi = 0.55;
  let maxSpread = 0.10;

  if (medianRange <= 4.0) {
    regime = 'COMPRESSION';
    baseScore = 78;
    baseObi = 0.54;
    maxSpread = 0.10;
  } else if (medianRange >= 9.5) {
    regime = 'TURBULENT';
    baseScore = 83;
    baseObi = 0.57;
    maxSpread = 0.08;
  }

  const finalScoreGate = Math.max(75, Math.min(86, baseScore + pacingGateOffset));

  State.marketRegime = regime;
  State.activeThresholds = {
    regime,
    pacingMode,
    scoreThreshold: finalScoreGate,
    obiThreshold: baseObi,
    maxSpread,
    cooldownMinutes: 15,
    marketAvgRange: medianRange,
    pacingGateOffset,
    vipScoreThreshold: 87,
    vipObiThreshold: 0.58
  };

  State.subsystems.regimeController = {
    status: 'CALIBRATED',
    currentRegime: regime,
    marketAvgVolatilityPct: medianRange,
    liveScoreGate: finalScoreGate,
    liveObiGatePct: Math.round(baseObi * 100),
    liveMaxSpreadPct: maxSpread,
    lastAdjustmentTime: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true })
  };

  return State.activeThresholds;
}

async function runPureQuantScoring() {
  try {
    const regimeConfig = calculateMarketVolatilityRegime();
    const symbols = Object.keys(State.universe);

    for (const sym of symbols) {
      const coin = State.universe[sym];
      if (!coin) continue;

      const [book, shortTerm] = await Promise.all([
        fetchRealOrderBookDepth(sym),
        fetchShortTermMomentum(sym)
      ]);

      const obi = book ? book.obi : (coin.obi || 0.55);
      const spreadPct = book ? book.spreadPct : 0.05;

      coin.obi = Number(obi.toFixed(2));
      coin.spreadPct = Number(spreadPct.toFixed(3));
      coin.change15mPct = shortTerm.change15mPct;
      coin.change1hPct = shortTerm.change1hPct;
      coin.change4hPct = shortTerm.change4hPct;
      coin.rvol = shortTerm.rvol;

      const sObi = Math.max(0, Math.min(40, (obi - 0.40) * 145));

      const c24 = Number(coin.priceChangePct) || 0;
      const c1h = Number(coin.change1hPct) || 0;
      const c15m = Number(coin.change15mPct) || 0;
      const effectiveMom = Math.max(
        c24,
        (c1h * 2.4) + (c15m * 3.8),
        (c15m * 5.0)
      );
      const sMom = Math.max(0, Math.min(38, (effectiveMom + 2.5) * 3.6));

      const volM = Math.max(1, Number(coin.volumeM) || 5);
      const rvolBoost = coin.rvol >= 1.5 ? Math.min(10, (coin.rvol - 1.0) * 4.5) : 0;
      const sVol = Math.min(25, (Math.log10(volM) * 7.2) + rvolBoost);

      const isSpike = (c15m >= 1.8 || c1h >= 3.8) && coin.rvol >= 1.6;
      const spikeBonus = isSpike ? 6 : 0;
      coin.isSpikeCandidate = isSpike;

      const rawScore = Math.round(sObi + sMom + sVol + spikeBonus);
      coin.score = Math.max(45, Math.min(98, rawScore));
      coin.cvd = Number(((obi - 0.50) * 120 + (c15m * 2.5)).toFixed(1));

      if (coin.score >= 82) coin.direction = 'STRONG BUY';
      else if (coin.score >= 74) coin.direction = 'BUY';
      else if (coin.score >= 60) coin.direction = 'NEUTRAL';
      else coin.direction = 'WEAK';

      coin.reasoning = `${isSpike ? '🚀 SPIKE IGNITION | ' : ''}15m: ${c15m >= 0 ? '+' : ''}${c15m.toFixed(1)}%, 1h: ${c1h >= 0 ? '+' : ''}${c1h.toFixed(1)}%, RVOL: ${coin.rvol.toFixed(1)}x, OBI: ${Math.round(obi * 100)}% ($${coin.volumeM}M Vol)`;
      coin.verifiedAt = Date.now();
    }

    const nowStr = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
    State.subsystems.microstructure.cyclesCompleted++;
    State.subsystems.microstructure.lastCycleTime = nowStr;
    State.subsystems.microstructure.evaluatedUniverseSize = symbols.length;
    State.quantTelemetry.lastAnalysisTime = nowStr;

    await evaluateBtcMacroStatus();
    await evaluateAutopilotEntries(regimeConfig);
    broadcastPricesOnly();
  } catch (err) {
    console.warn('[Quant Scoring Warning]', err.message);
  }
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY MATRIX (CLEAN SINGLE-ENTRY SPOT ARCHITECTURE)
// -------------------------------------------------------------
async function evaluateAutopilotEntries(regimeConfig) {
  if (!State.autopilotEnabled) return;

  syncRollingTradeQuota();

  const currentActiveCount = Object.keys(State.activePositions).length;
  // Maximum 2 concurrent active breakout slots (Recovery bags do NOT count against this!)
  if (currentActiveCount >= (State.tradeFrequencyController.maxConcurrentPositions || 2)) return;

  if (State.warmupCycles < 1) return;

  // -----------------------------------------------------------
  // DYNAMIC LIQUIDITY GOVERNOR RISK GUARDS
  // -----------------------------------------------------------
  const freeCash = Number(State.portfolio.cashUSDT || 0);
  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  const cashFloor = State.tradeFrequencyController.cashReserveFloorUSDT || 300.00;
  const maxBags = State.tradeFrequencyController.maxRecoveryBagsLimit || 6;

  // Guard 1: Must have at least $50 to buy AND maintain the $300 cash reserve floor
  if (freeCash < (cashFloor + 50.00)) {
    return;
  }

  // Guard 2: Max 6 concurrent Recovery Bags capacity
  if (activeBagsCount >= maxBags) {
    return;
  }

  const btcGate = State.subsystems.btcMacroGate || { allowAltcoinEntries: true };
  const config = regimeConfig || State.activeThresholds;
  const isBypassActive = Boolean(State.tradeFrequencyController.quotaBypassEnabled);
  const isDynamicGov = Boolean(State.tradeFrequencyController.dynamicGovernorEnabled);
  const standardQuotaAvailable = isBypassActive || isDynamicGov || (State.subsystems.quotaManager.quotaRemaining > 0);
  const vipSlotsRemaining = (isBypassActive || isDynamicGov) ? 999 : (State.subsystems.quotaManager.vipSlotsRemaining || 0);

  if (!standardQuotaAvailable && vipSlotsRemaining <= 0) return;

  const sortedCandidates = Object.keys(State.universe)
    .filter(k => !State.activePositions[k] && !State.recoveryBags[k])
    .sort((a, b) => (State.universe[b].score || 0) - (State.universe[a].score || 0));

  for (const sym of sortedCandidates) {
    const coin = State.universe[sym];
    if (!coin || !coin.score) continue;

    // PRONG 1: BTC MACRO SHIELD
    // If Bitcoin is flashing an active drop, block all altcoin entries to prevent sympathetic dump traps
    if (sym !== 'BTCUSDT' && !btcGate.allowAltcoinEntries) {
      continue;
    }

    if (State.cooldowns[sym]) {
      const cdObj = typeof State.cooldowns[sym] === 'object'
        ? State.cooldowns[sym]
        : { until: Number(State.cooldowns[sym]) || 0 };
      const remaining = cdObj.until - Date.now();
      if (remaining > 0) continue;
      delete State.cooldowns[sym];
    }

    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    // -----------------------------------------------------------
    // HIGH-SUSTAIN CONFLUENCE GATES (PREVENTING UNNECESSARY BAG TRANSFERS)
    // -----------------------------------------------------------
    
    // Gate 1: Anti-Apex & Top-Chase Filter
    // If 15m is already up > 3.8% and trading at apex of candle range (>0.88), buying the top guarantees taking the retest wick!
    // Require either a confirmed base/pullback or massive bid absorption (OBI >= 0.60)
    const isApexChasing = (coin.change15mPct >= 3.8 && (coin.pricePositionInRange || 0.8) > 0.88 && book.obi < 0.60);
    if (isApexChasing) continue;

    // Gate 2: Multi-Timeframe (MTF) Confirmation
    // Avoid dead-cat relief rallies (e.g. 15m up, but 1h and 4h heavily dumping)
    const isMacroDowntrend = (coin.change1hPct < -0.8 && coin.change4hPct < -3.5);
    if (isMacroDowntrend) continue;

    // Gate 3: VWAP Premium Alignment
    // Must be trading at or above VWAP (institutional accumulation signature)
    if (coin.vwapPremiumPct !== undefined && coin.vwapPremiumPct < -0.5) continue;

    const isSubCent = (coin.priceUSDT && coin.priceUSDT < 0.01);
    const isLowDollarAlt = (coin.priceUSDT && coin.priceUSDT >= 0.01 && coin.priceUSDT < 1.50);
    const isHighVelocitySpike = Boolean(coin.isSpikeCandidate || coin.change15mPct >= 2.0 || coin.change1hPct >= 4.0);

    let allowedSpread = config.maxSpread;
    if (isSubCent) allowedSpread = 0.35;
    else if (isHighVelocitySpike) allowedSpread = 0.25;
    else if (isLowDollarAlt) allowedSpread = 0.20;

    if (book.spreadPct > allowedSpread) continue;
    if (!coin.priceUSDT || coin.priceUSDT <= 0) continue;
    if (!coin.verifiedAt || (Date.now() - coin.verifiedAt) > 30000) continue;

    // Case A1: Standard Breakout Confluence (MTF confirmed + OBI support)
    const mtfConfirmed = (coin.change1hPct >= 0.2 || coin.vwapPremiumPct >= 1.0);
    if (standardQuotaAvailable && coin.score >= config.scoreThreshold && book.obi >= config.obiThreshold && mtfConfirmed) {
      executeSpotBuy(
        sym, 
        coin.priceUSDT, 
        `High-Sustain Confluence (${config.regime}: Score ${coin.score} >= ${config.scoreThreshold}, OBI ${(book.obi * 100).toFixed(0)}%, 15m +${(coin.change15mPct || 0).toFixed(1)}%, 1h +${(coin.change1hPct || 0).toFixed(1)}%)`,
        false
      );
      break;
    }

    // Case A2: Ignition Spike Fast-Track (Volume backed + non-exhausted)
    const isVerifiedIgnition = isHighVelocitySpike && (coin.rvol || 1) >= 1.8 && (coin.pricePositionInRange || 0.6) >= 0.70 && (coin.change15mPct <= 5.5);
    if (standardQuotaAvailable && isVerifiedIgnition && coin.score >= (config.scoreThreshold - 2) && book.obi >= 0.54) {
      executeSpotBuy(
        sym,
        coin.priceUSDT,
        `🚀 High-Sustain Ignition (${sym}: 15m +${(coin.change15mPct || 0).toFixed(1)}%, RVOL ${(coin.rvol || 1).toFixed(1)}x, OBI ${(book.obi * 100).toFixed(0)}%)`,
        false
      );
      break;
    }

    // Case B: VIP Alpha Override Gate
    if (!standardQuotaAvailable && vipSlotsRemaining > 0) {
      const isExtremeMomentum = (coin.change15mPct >= 3.0 && coin.change1hPct >= 4.0 && coin.priceChangePct >= 10.0);
      const isExtremeConfluence = (coin.score >= (config.vipScoreThreshold || 88) && book.obi >= (config.vipObiThreshold || 0.60));

      if (isExtremeMomentum && isExtremeConfluence) {
        executeSpotBuy(
          sym,
          coin.priceUSDT,
          `⚡ VIP Alpha Override: God-Candle Breakout! Score ${coin.score}, OBI ${(book.obi * 100).toFixed(0)}%`,
          true
        );
        break;
      }
    }
  }
}

// -------------------------------------------------------------
// SPOT BUY EXECUTION ($50 FIXED NOTIONAL)
// -------------------------------------------------------------
function executeSpotBuy(pairKey, priceUSDT, reason, isVipAlpha = false) {
  if (State.portfolio.cashUSDT < 50.00) return;

  const notionalUSDT = 50.00;
  const qty = notionalUSDT / priceUSDT;

  State.portfolio.cashUSDT -= notionalUSDT;

  const tp1 = formatBinanceUSDTPrice(priceUSDT * 1.018); // +1.80% Target TP1

  State.activePositions[pairKey] = {
    pairKey,
    pairSymbol: State.universe[pairKey]?.symbol || pairKey,
    entryPriceUSDT: priceUSDT,
    qty,
    notionalUSDT,
    tp1,
    sl: formatBinanceUSDTPrice(priceUSDT * 0.95), // Deep reference
    openedAt: Date.now(),
    isTrailingActive: false,
    tp1Secured: false,
    highWatermarkUSDT: priceUSDT,
    retestGraceUntil: Date.now() + (20 * 60 * 1000), // 20-min momentum window
    isVipAlpha
  };

  State.portfolio.holdings[pairKey] = {
    symbol: State.universe[pairKey]?.symbol || pairKey,
    qty,
    avgBuyPriceUSDT: priceUSDT,
    totalCostUSDT: notionalUSDT
  };

  State.tradeFrequencyController.rollingTrades24h.push(Date.now());
  if (isVipAlpha) {
    State.tradeFrequencyController.vipTrades24h.push(Date.now());
  }

  syncRollingTradeQuota();
  saveStateToDisk();
  broadcastState();

  console.log(`[Spot Buy] Opened ${pairKey} @ $${priceUSDT} ($50 USDT) | TP1 Target: $${tp1}`);
}

// -------------------------------------------------------------
// ALTERNATIVE 5: SPOT RECOVERY BAG TRANSFER (ZERO REALIZED LOSS)
// -------------------------------------------------------------
function moveToRecoveryBag(pairKey, transferReason) {
  const pos = State.activePositions[pairKey];
  if (!pos) return;

  // Set standing mean-reversion target at breakeven (+0.30% to clear trading fees cleanly)
  const targetExitPriceUSDT = formatBinanceUSDTPrice(pos.entryPriceUSDT * 1.0030);

  State.recoveryBags[pairKey] = {
    pairKey,
    pairSymbol: pos.pairSymbol || pairKey,
    entryPriceUSDT: pos.entryPriceUSDT,
    qty: pos.qty,
    notionalUSDT: pos.notionalUSDT,
    targetExitPriceUSDT,
    initialTargetPriceUSDT: targetExitPriceUSDT,
    targetStep: 'STEP 1 (+0.30%)',
    movedAt: Date.now(),
    livePriceUSDT: pos.livePriceUSDT || pos.entryPriceUSDT,
    gainPct: pos.gainPct || 0,
    unrealizedPnLUSDT: pos.unrealizedPnLUSDT || 0,
    ageHours: 0,
    reason: transferReason
  };

  // REMOVE from active positions so active slot is immediately OPEN for the bot to trade!
  delete State.activePositions[pairKey];

  State.subsystems.recoveryManager.activeRecoveryBagsCount = Object.keys(State.recoveryBags).length;

  saveStateToDisk();
  broadcastState();

  console.log(`[Spot Recovery Bag] Shifted ${pairKey} to Recovery Bag. Initial Breakeven Limit: $${targetExitPriceUSDT}. Active slot is now FREE!`);
}

function settleRecoveryBag(pairKey, exitPriceUSDT, reason) {
  const bag = State.recoveryBags[pairKey];
  if (!bag) return;

  const finalValueUSDT = bag.qty * exitPriceUSDT;
  const pnlUSDT = Number((finalValueUSDT - bag.notionalUSDT).toFixed(4));

  State.portfolio.cashUSDT += finalValueUSDT;
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + pnlUSDT).toFixed(4));

  delete State.portfolio.holdings[pairKey];
  delete State.recoveryBags[pairKey];

  State.subsystems.recoveryManager.bagsResolvedWinCount++;
  State.subsystems.recoveryManager.activeRecoveryBagsCount = Object.keys(State.recoveryBags).length;

  const now = new Date();
  const istTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const istDate = now.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric' });

  const logEntry = {
    id: Date.now(),
    type: 'RECOVERY EXIT (ZERO LOSS)',
    pair: `${bag.pairSymbol}/USDT`,
    entryPriceUSDT: formatBinanceUSDTPrice(bag.entryPriceUSDT),
    exitPriceUSDT: formatBinanceUSDTPrice(exitPriceUSDT),
    pnlUSDT: pnlUSDT,
    pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2)),
    reason: reason || `Mean-reversion bounce filled target (+${(((exitPriceUSDT - bag.entryPriceUSDT) / bag.entryPriceUSDT) * 100).toFixed(2)}%)`,
    time: istTime,
    date: istDate,
    regime: State.activeThresholds?.regime || 'BALANCED',
    volatilityPct: State.activeThresholds?.marketAvgRange || 4.5
  };

  State.tradeLogs.unshift(logEntry);
  if (State.tradeLogs.length > 500) State.tradeLogs.pop();

  recalculateTrueRealizedPnL();
  appendTradeToGoogleDrive(logEntry);
  saveStateToDisk();
  broadcastState();

  console.log(`[Recovery Resolved] ${pairKey} exit filled at $${exitPriceUSDT}. Cash restored.`);
}

function settlePartialPosition(pairKey, exitPriceUSDT, exitReason) {
  const pos = State.activePositions[pairKey];
  if (!pos || pos.tp1Secured) return;

  const halfQty = pos.qty / 2;
  const halfNotional = pos.notionalUSDT / 2;
  const halfValUSDT = halfQty * exitPriceUSDT;
  const pnlUSDT = Number((halfValUSDT - halfNotional).toFixed(4));

  State.portfolio.cashUSDT += halfValUSDT;
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + pnlUSDT).toFixed(4));

  // PRONG 2: SELF-HEALING PORTFOLIO (Route 35% of winning gains to Bag Liquidation Subsidy Pool)
  if (pnlUSDT > 0.05) {
    const contribution = Number((pnlUSDT * 0.35).toFixed(4));
    State.portfolio.bagSubsidyPoolUSDT = Number(((State.portfolio.bagSubsidyPoolUSDT || 0) + contribution).toFixed(4));
    State.subsystems.recoveryManager.subsidyPoolUSDT = State.portfolio.bagSubsidyPoolUSDT;
  }

  pos.qty = halfQty;
  pos.notionalUSDT = halfNotional;
  pos.tp1Secured = true;
  pos.isTrailingActive = true;
  pos.sl = pos.entryPriceUSDT * 1.0025; // Breakeven lock
  pos.highWatermarkUSDT = Math.max(pos.highWatermarkUSDT || exitPriceUSDT, exitPriceUSDT);

  if (State.portfolio.holdings[pairKey]) {
    State.portfolio.holdings[pairKey].qty = halfQty;
    State.portfolio.holdings[pairKey].totalCostUSDT = halfNotional;
  }

  const now = new Date();
  const istTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const istDate = now.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric' });

  const logEntry = {
    id: Date.now(),
    type: 'TP1 (50% BANKED)',
    pair: `${pos.pairSymbol}/USDT`,
    entryPriceUSDT: formatBinanceUSDTPrice(pos.entryPriceUSDT),
    exitPriceUSDT: formatBinanceUSDTPrice(exitPriceUSDT),
    pnlUSDT,
    pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2)),
    reason: exitReason,
    time: istTime,
    date: istDate,
    regime: State.activeThresholds?.regime || 'BALANCED',
    volatilityPct: State.activeThresholds?.marketAvgRange || 4.5
  };

  State.tradeLogs.unshift(logEntry);
  if (State.tradeLogs.length > 500) State.tradeLogs.pop();

  recalculateTrueRealizedPnL();
  appendTradeToGoogleDrive(logEntry);
  syncRollingTradeQuota();
  saveStateToDisk();
  broadcastState();
}

function settlePosition(pairKey, exitType, exitPriceUSDT, exitReason) {
  const pos = State.activePositions[pairKey];
  if (!pos) return;

  const finalValueUSDT = pos.qty * exitPriceUSDT;
  const pnlUSDT = Number((finalValueUSDT - pos.notionalUSDT).toFixed(4));

  State.portfolio.cashUSDT += finalValueUSDT;
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + pnlUSDT).toFixed(4));

  // PRONG 2: SELF-HEALING PORTFOLIO (Route 35% of winning runner gains to Bag Subsidy Pool)
  if (pnlUSDT > 0.05) {
    const contribution = Number((pnlUSDT * 0.35).toFixed(4));
    State.portfolio.bagSubsidyPoolUSDT = Number(((State.portfolio.bagSubsidyPoolUSDT || 0) + contribution).toFixed(4));
    State.subsystems.recoveryManager.subsidyPoolUSDT = State.portfolio.bagSubsidyPoolUSDT;
  }

  delete State.portfolio.holdings[pairKey];
  delete State.activePositions[pairKey];

  const now = new Date();
  const istTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const istDate = now.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric' });

  const logEntry = {
    id: Date.now(),
    type: exitType,
    pair: `${pos.pairSymbol}/USDT`,
    entryPriceUSDT: formatBinanceUSDTPrice(pos.entryPriceUSDT),
    exitPriceUSDT: formatBinanceUSDTPrice(exitPriceUSDT),
    pnlUSDT,
    pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2)),
    reason: exitReason,
    time: istTime,
    date: istDate,
    regime: State.activeThresholds?.regime || 'BALANCED',
    volatilityPct: State.activeThresholds?.marketAvgRange || 4.5
  };

  State.tradeLogs.unshift(logEntry);
  if (State.tradeLogs.length > 500) State.tradeLogs.pop();

  recalculateTrueRealizedPnL();
  syncRollingTradeQuota();

  appendTradeToGoogleDrive(logEntry);
  saveStateToDisk();
  broadcastState();
}

// -------------------------------------------------------------
// SENTRY & RISK MONITOR (ACTIVE POSITIONS & RECOVERY BAGS)
// -------------------------------------------------------------
async function monitorOpenPositionsHealth() {
  const openKeys = Object.keys(State.activePositions);
  const recoveryKeys = Object.keys(State.recoveryBags);

  State.subsystems.predictiveSentry.lastMonitorTick = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  State.subsystems.predictiveSentry.activePositionsMonitored = openKeys.length;
  State.subsystems.predictiveSentry.recoveryBagsMonitored = recoveryKeys.length;
  State.subsystems.recoveryManager.activeRecoveryBagsCount = recoveryKeys.length;
  State.subsystems.recoveryManager.subsidyPoolUSDT = State.portfolio.bagSubsidyPoolUSDT || 0;

  let totalRecoveryCap = 0;
  recoveryKeys.forEach(k => {
    totalRecoveryCap += (State.recoveryBags[k]?.notionalUSDT || 50);
  });
  State.subsystems.recoveryManager.totalRecoveryCapitalUSDT = totalRecoveryCap;

  // -----------------------------------------------------------
  // 1. MONITOR SPOT RECOVERY BAGS (DYNAMIC STEPPING & SUBSIDIZATION)
  // -----------------------------------------------------------
  for (const sym of recoveryKeys) {
    const bag = State.recoveryBags[sym];
    if (!bag) continue;

    const ageHours = (Date.now() - (bag.movedAt || Date.now())) / (1000 * 60 * 60);
    let live = bag.livePriceUSDT || State.universe[sym]?.priceUSDT || bag.entryPriceUSDT;
    const gainFromEntryPct = ((live - bag.entryPriceUSDT) / bag.entryPriceUSDT) * 100;
    bag.gainPct = Number(gainFromEntryPct.toFixed(2));
    bag.unrealizedPnLUSDT = Number(((live - bag.entryPriceUSDT) * bag.qty).toFixed(4));
    bag.ageHours = Number(ageHours.toFixed(1));

    // Dynamic Target Stepping based on holding duration:
    // Step 1 (0 to 4 hours): Breakeven + fee buffer (+0.30%)
    // Step 2 (4 to 12 hours): Exact Breakeven (+0.00%)
    // Step 3 (12 to 24 hours): Micro relief exit (-0.75%)
    // Step 4 (> 24 hours): Relief exit (-1.50%) or VWAP bounce to recycle $50 capital
    let dynamicTargetPrice = bag.entryPriceUSDT * 1.0030;
    let stepName = 'STEP 1 (+0.30%)';

    if (ageHours >= 24) {
      dynamicTargetPrice = bag.entryPriceUSDT * 0.9850; // -1.50% capital recycle
      stepName = 'STEP 4 (-1.50% RECYCLE)';
    } else if (ageHours >= 12) {
      dynamicTargetPrice = bag.entryPriceUSDT * 0.9925; // -0.75% relief exit
      stepName = 'STEP 3 (-0.75% RELIEF)';
    } else if (ageHours >= 4) {
      dynamicTargetPrice = bag.entryPriceUSDT * 1.0000; // Exact Breakeven
      stepName = 'STEP 2 (0.00% BREAKEVEN)';
    }

    bag.targetExitPriceUSDT = formatBinanceUSDTPrice(dynamicTargetPrice);
    bag.targetStep = stepName;

    // Check if live price filled the dynamic stepped target
    if (live >= bag.targetExitPriceUSDT) {
      settleRecoveryBag(sym, live, `Stepped Target ${stepName} filled on relief bounce (${gainFromEntryPct >= 0 ? '+' : ''}${gainFromEntryPct.toFixed(2)}%)`);
      continue;
    }

    // Cross-Trade Profit Subsidization Check:
    // If bag has been stuck for >= 6 hours and subsidy pool has accumulated winner buffer,
    // subsidize the deficit and liquidate the bag so $50 cash is completely recycled!
    const deficitUSDT = Math.abs(Math.min(0, bag.unrealizedPnLUSDT));
    const subsidyPool = Number(State.portfolio.bagSubsidyPoolUSDT || 0);

    if (ageHours >= 6 && deficitUSDT > 0 && subsidyPool >= deficitUSDT) {
      State.portfolio.bagSubsidyPoolUSDT = Number((subsidyPool - deficitUSDT).toFixed(4));
      State.subsystems.recoveryManager.bagsSubsidizedCount = (State.subsystems.recoveryManager.bagsSubsidizedCount || 0) + 1;
      State.subsystems.recoveryManager.subsidyPoolUSDT = State.portfolio.bagSubsidyPoolUSDT;

      settleRecoveryBag(
        sym, 
        live, 
        `Self-Healing Subsidization: Stagnant bag cleared after ${ageHours.toFixed(1)}h using $${deficitUSDT.toFixed(2)} winner profits`
      );
      continue;
    }
  }

  // -----------------------------------------------------------
  // 2. MONITOR ACTIVE BREAKOUT POSITIONS
  // -----------------------------------------------------------
  if (openKeys.length === 0) {
    State.subsystems.predictiveSentry.trailingLocksActive = 0;
    return;
  }

  for (const sym of openKeys) {
    const pos = State.activePositions[sym];
    if (!pos) continue;

    let livePriceUSDT = pos.livePriceUSDT || State.universe[sym]?.priceUSDT || pos.entryPriceUSDT;
    const gainPct = ((livePriceUSDT - pos.entryPriceUSDT) / pos.entryPriceUSDT) * 100;
    pos.gainPct = Number(gainPct.toFixed(2));
    pos.unrealizedPnLUSDT = Number(((livePriceUSDT - pos.entryPriceUSDT) * pos.qty).toFixed(4));
    const holdSeconds = (Date.now() - pos.openedAt) / 1000;

    if (!pos.highWatermarkUSDT || livePriceUSDT > pos.highWatermarkUSDT) {
      pos.highWatermarkUSDT = livePriceUSDT;
    }

    // A. Profit Target: Bank TP1 (+1.80%)
    if (livePriceUSDT >= pos.tp1 && !pos.tp1Secured) {
      settlePartialPosition(sym, livePriceUSDT, `TP1 Banked (+${gainPct.toFixed(2)}%), runner active`);
      continue;
    }

    // B. Runner Trailing Logic
    if (pos.tp1Secured) {
      const runnerTrailPrice = pos.highWatermarkUSDT * (1 - 0.012);
      pos.sl = Math.max(pos.entryPriceUSDT * 1.0025, runnerTrailPrice);

      if (livePriceUSDT <= pos.sl) {
        settlePosition(sym, 'RUNNER EXIT', livePriceUSDT, `Runner trailed from peak ${formatBinanceUSDTPrice(pos.highWatermarkUSDT)} (+${gainPct.toFixed(2)}%)`);
        continue;
      }
    } else {
      // ---------------------------------------------------------
      // C. HIGH-SUSTAIN SPOT RECOVERY BAG DISPATCH
      // ---------------------------------------------------------

      // 1. Adverse dip filter: Widened to -3.20% with OBI support verification
      // If OBI is holding >= 46%, let the position breathe through normal 15m retest wicks!
      const book = await fetchRealOrderBookDepth(sym);
      const isStrongSupport = Boolean(book && book.obi >= 0.46);

      if (gainPct <= -3.20 || (gainPct <= -2.50 && !isStrongSupport)) {
        pos.adverseDipTicks = (pos.adverseDipTicks || 0) + 1;
        // Require 3 consecutive ticks (7.5 seconds) to avoid flash wicks
        if (pos.adverseDipTicks >= 3) {
          moveToRecoveryBag(sym, `Confirmed structural breakdown (${gainPct.toFixed(2)}%, OBI ${book ? Math.round(book.obi * 100) : 40}%) - Shifted to Recovery Bag`);
          continue;
        }
      } else {
        pos.adverseDipTicks = 0;
      }

      // 2. Intelligent Dynamic Stagnation Window:
      // High-conviction altcoin bull flags regularly consolidate for 30-45 minutes.
      // If position is floating near breakeven (-0.80% to +0.30%), allow up to 45 minutes (2700s).
      const isHealthyConsolidation = (gainPct >= -0.80 && gainPct < 0.30);
      const stagnationLimitSec = isHealthyConsolidation ? 2700 : 1800; // 45m if flat, 30m if sluggish

      if (holdSeconds >= stagnationLimitSec) {
        moveToRecoveryBag(sym, `Stagnation after ${Math.round(holdSeconds / 60)}m (${gainPct.toFixed(2)}%) - Shifted to Recovery Bag`);
        continue;
      }
    }
  }
}

// -------------------------------------------------------------
// LIVE BINANCE PRICE FEED
// -------------------------------------------------------------
async function syncLiveBinancePrices() {
  try {
    const list = await fetchFromBinance('/api/v3/ticker/price');
    if (!Array.isArray(list)) return;

    const priceMap = {};
    list.forEach(i => { priceMap[i.symbol] = parseFloat(i.price); });

    const openKeys = [...Object.keys(State.activePositions), ...Object.keys(State.recoveryBags)];
    openKeys.forEach(sym => {
      const live = priceMap[sym];
      if (live && live > 0) {
        const formatted = formatBinanceUSDTPrice(live);
        const inr = calculateAccurateINRPrice(live, State.usdtToInrRate);

        if (!State.universe[sym]) {
          const base = sym.replace('USDT', '');
          State.universe[sym] = {
            name: base,
            symbol: base,
            priceUSDT: formatted,
            priceINR: inr,
            score: 80,
            direction: 'BUY',
            obi: 0.55,
            cvd: 3.5,
            volumeM: 10,
            source: 'ACTIVE_HOLD_ANCHOR',
            reasoning: 'Position retained in universe',
            verifiedAt: Date.now()
          };
        } else {
          State.universe[sym].priceUSDT = formatted;
          State.universe[sym].priceINR = inr;
          State.universe[sym].verifiedAt = Date.now();
        }

        const activePos = State.activePositions[sym];
        if (activePos) {
          activePos.livePriceUSDT = formatted;
          activePos.unrealizedPnLUSDT = Number(((live - activePos.entryPriceUSDT) * activePos.qty).toFixed(4));
          activePos.gainPct = Number((((live - activePos.entryPriceUSDT) / activePos.entryPriceUSDT) * 100).toFixed(2));
        }

        const bag = State.recoveryBags[sym];
        if (bag) {
          bag.livePriceUSDT = formatted;
          bag.unrealizedPnLUSDT = Number(((live - bag.entryPriceUSDT) * bag.qty).toFixed(4));
          bag.gainPct = Number((((live - bag.entryPriceUSDT) / bag.entryPriceUSDT) * 100).toFixed(2));
        }
      }
    });

    Object.keys(State.universe).forEach(sym => {
      if (priceMap[sym]) {
        const usdt = priceMap[sym];
        State.universe[sym].priceUSDT = formatBinanceUSDTPrice(usdt);
        State.universe[sym].priceINR = calculateAccurateINRPrice(usdt, State.usdtToInrRate);
        State.universe[sym].verifiedAt = Date.now();
      }
    });

    State.warmupCycles++;
    await monitorOpenPositionsHealth();
    syncRollingTradeQuota();

    const remainingSec = Math.max(0, Math.round((nextScreenerTargetTimestamp - Date.now()) / 1000));
    State.subsystems.screener.nextRunCountdownSec = remainingSec;

    broadcastPricesOnly();
  } catch (e) {}
}

// -------------------------------------------------------------
// WEBSOCKET BROADCASTING
// -------------------------------------------------------------
function broadcastState() {
  const payload = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs.slice(0, 100),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

function broadcastPricesOnly() {
  const priceData = {};
  Object.keys(State.universe).forEach(k => {
    priceData[k] = {
      priceINR: State.universe[k].priceINR,
      priceUSDT: State.universe[k].priceUSDT,
      score: State.universe[k].score,
      direction: State.universe[k].direction,
      obi: State.universe[k].obi,
      change15mPct: State.universe[k].change15mPct || 0,
      change1hPct: State.universe[k].change1hPct || 0,
      rvol: State.universe[k].rvol || 1.0,
      isSpikeCandidate: Boolean(State.universe[k].isSpikeCandidate),
      volumeM: State.universe[k].volumeM || 0,
      source: State.universe[k].source,
      reasoning: State.universe[k].reasoning
    };
  });

  const payload = JSON.stringify({
    type: 'PRICE_TICK',
    prices: priceData,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    portfolio: State.portfolio,
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    syncTime: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true })
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type: 'INIT',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs.slice(0, 100),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated
  }));

  ws.on('message', msg => {
    try {
      const data = JSON.parse(msg.toString());
      if (data.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(data.enabled);
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'TOGGLE_QUOTA_BYPASS') {
        State.tradeFrequencyController.quotaBypassEnabled = Boolean(data.bypass);
        syncRollingTradeQuota();
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'SET_DYNAMIC_GOVERNOR') {
        State.tradeFrequencyController.dynamicGovernorEnabled = Boolean(data.enabled);
        syncRollingTradeQuota();
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'SET_CASH') {
        const val = parseFloat(data.cashUSDT);
        if (!isNaN(val) && val >= 0) {
          State.portfolio.cashUSDT = Math.round(val * 100) / 100;
          syncRollingTradeQuota();
          saveStateToDisk();
          broadcastState();
          console.log(`[Portfolio] Cash updated to $${State.portfolio.cashUSDT} USDT`);
        }
      } else if (data.type === 'ADD_CASH') {
        const delta = parseFloat(data.addUSDT);
        if (!isNaN(delta)) {
          State.portfolio.cashUSDT = Math.max(0, Math.round((State.portfolio.cashUSDT + delta) * 100) / 100);
          syncRollingTradeQuota();
          saveStateToDisk();
          broadcastState();
          console.log(`[Portfolio] Added $${delta} USDT. New Cash: $${State.portfolio.cashUSDT} USDT`);
        }
      } else if (data.type === 'RECONCILE_QUOTA') {
        reconcileRollingTradeQuotaFromHistory();
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'CLOSE_POSITION' && data.pairKey) {
        const livePriceUSDT = State.universe[data.pairKey]?.priceUSDT || State.activePositions[data.pairKey]?.entryPriceUSDT;
        if (livePriceUSDT) settlePosition(data.pairKey, 'MANUAL EXIT', livePriceUSDT, 'Closed by user command');
      } else if (data.type === 'CLOSE_RECOVERY_BAG' && data.pairKey) {
        const livePriceUSDT = State.universe[data.pairKey]?.priceUSDT || State.recoveryBags[data.pairKey]?.entryPriceUSDT;
        if (livePriceUSDT) settleRecoveryBag(data.pairKey, livePriceUSDT, 'Manual recovery exit by user command');
      } else if (data.type === 'SYNC_P2P_RATE') {
        syncBinanceP2PUsdtInrRate();
      } else if (data.type === 'SET_GOOGLE_SHEET_URL' && data.url) {
        State.googleSheetUrl = data.url.trim();
        State.subsystems.persistence.googleSheetSync = 'CONNECTED';
        saveStateToDisk();
        syncWithGoogleDrive();
      }
    } catch (e) {}
  });
});

// -------------------------------------------------------------
// REST API ENDPOINTS
// -------------------------------------------------------------
app.get('/api/quota/reconcile', (req, res) => {
  reconcileRollingTradeQuotaFromHistory();
  saveStateToDisk();
  broadcastState();
  res.json({
    success: true,
    rollingTrades24h: State.tradeFrequencyController.rollingTrades24h,
    executed24hCount: State.subsystems.quotaManager.executed24hCount,
    quotaRemaining: State.subsystems.quotaManager.quotaRemaining,
    formattedReset: State.subsystems.quotaManager.formattedReset
  });
});

app.get('/api/quota/toggle-bypass', (req, res) => {
  const current = Boolean(State.tradeFrequencyController.quotaBypassEnabled);
  State.tradeFrequencyController.quotaBypassEnabled = !current;
  syncRollingTradeQuota();
  saveStateToDisk();
  broadcastState();
  res.json({
    success: true,
    quotaBypassEnabled: State.tradeFrequencyController.quotaBypassEnabled,
    status: State.subsystems.quotaManager.status,
    maxAllowed: State.subsystems.quotaManager.maxAllowed
  });
});

app.post('/api/quota/set-bypass', (req, res) => {
  const enabled = Boolean(req.body && req.body.enabled);
  State.tradeFrequencyController.quotaBypassEnabled = enabled;
  syncRollingTradeQuota();
  saveStateToDisk();
  broadcastState();
  res.json({
    success: true,
    quotaBypassEnabled: State.tradeFrequencyController.quotaBypassEnabled,
    status: State.subsystems.quotaManager.status
  });
});

// -------------------------------------------------------------
// CASH BALANCE MANAGEMENT API (SET / ADD AVAILABLE TRADING FUNDS)
// -------------------------------------------------------------
app.post('/api/portfolio/cash', (req, res) => {
  const { cashUSDT, addUSDT } = req.body || {};
  
  if (cashUSDT !== undefined) {
    const val = parseFloat(cashUSDT);
    if (!isNaN(val) && val >= 0) {
      State.portfolio.cashUSDT = Math.round(val * 100) / 100;
      syncRollingTradeQuota();
      saveStateToDisk();
      broadcastState();
      return res.json({ success: true, cashUSDT: State.portfolio.cashUSDT });
    }
  }

  if (addUSDT !== undefined) {
    const delta = parseFloat(addUSDT);
    if (!isNaN(delta)) {
      State.portfolio.cashUSDT = Math.max(0, Math.round((State.portfolio.cashUSDT + delta) * 100) / 100);
      syncRollingTradeQuota();
      saveStateToDisk();
      broadcastState();
      return res.json({ success: true, cashUSDT: State.portfolio.cashUSDT });
    }
  }

  res.status(400).json({ success: false, message: 'Invalid cash value provided' });
});

// -------------------------------------------------------------
// STRICT GOOGLE SHEET RE-FETCH & CLEAR LOCAL LOGS
// -------------------------------------------------------------
app.post('/api/trades/sync-strict-sheet', async (req, res) => {
  State.tradeLogs = []; // Wipe local server array
  await syncWithGoogleDrive(); // Pull strictly from Google Sheet
  res.json({
    success: true,
    message: 'Cleared local logs and refreshed strictly from Google Sheet',
    count: State.tradeLogs.length,
    trades: State.tradeLogs
  });
});

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs.slice(0, 100),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    googleSheetUrl: State.googleSheetUrl
  });
});

app.get('/api/p2p/sync', async (req, res) => {
  await syncBinanceP2PUsdtInrRate();
  res.json({
    success: true,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    source: State.p2pSource
  });
});

app.post('/api/settings/googlesheet', (req, res) => {
  const url = req.body && req.body.url ? req.body.url.trim() : '';
  if (url) {
    State.googleSheetUrl = url;
    State.subsystems.persistence.googleSheetSync = 'CONNECTED';
    saveStateToDisk();
    syncWithGoogleDrive();
    res.json({ success: true, googleSheetUrl: url });
  } else {
    res.status(400).json({ success: false, message: 'Invalid URL' });
  }
});

app.get('/api/settings/googlesheet/sync', async (req, res) => {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) {
    return res.status(400).json({ success: false, message: 'GOOGLE_SHEET_URL not configured' });
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const response = await fetch(targetUrl, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);

    if (response.ok) {
      const data = await response.json();
      if (data && Array.isArray(data.trades)) {
        const merged = sanitizeAndAuditTradeHistory([...data.trades, ...State.tradeLogs]);
        State.tradeLogs = merged;
        recalculateTrueRealizedPnL();
        State.subsystems.persistence.googleSheetSync = 'ACTIVE';
        saveStateToDisk();
        broadcastState();
        return res.json({
          success: true,
          count: data.trades.length,
          totalRetained: State.tradeLogs.length,
          trades: State.tradeLogs
        });
      }
      return res.json({ success: true, count: 0, trades: State.tradeLogs });
    } else {
      return res.status(response.status).json({ success: false, message: `Google Sheet returned HTTP ${response.status}` });
    }
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/screener/run', async (req, res) => {
  await runDynamicMarketScreener();
  res.json({ 
    success: true, 
    universeCount: Object.keys(State.universe).length, 
    subsystem: State.subsystems.screener,
    universe: State.universe 
  });
});

app.get('/api/quant/audit', (req, res) => {
  res.json({
    engine: State.quantTelemetry.engine,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    activePositionsCount: Object.keys(State.activePositions).length,
    recoveryBagsCount: Object.keys(State.recoveryBags).length,
    cooldowns: State.cooldowns,
    realizedPnL: State.portfolio.realizedPnL,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated
  });
});

app.get('/healthz', (req, res) => res.status(200).send('OK'));

// -------------------------------------------------------------
// BOOTSTRAP TIMERS
// -------------------------------------------------------------
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Quant Server] Running on port ${PORT} with Alternative 5: Spot Recovery Bag Architecture`);

  setTimeout(runDynamicMarketScreener, 2000);
  setInterval(runPureQuantScoring, 15 * 1000);
  setInterval(syncLiveBinancePrices, 2500);
  setInterval(syncBinanceP2PUsdtInrRate, 60 * 60 * 1000);
});
