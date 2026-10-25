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
// CORS & PRE-FLIGHT HEADERS (Allows mobile webviews & cross-origin browsers)
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

// Clean baseline universe in pure USDT
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
    quotaBypassEnabled: false, // Set to true to disable the 8-trade cap for full 48h stress-testing
    baseMaxTradesPer24h: 8,
    maxTradesPer24h: 8,
    vipAlphaSlotsAllowed: 2,
    vipAlphaSlotsUsed24h: 0,
    vipTrades24h: [],
    bonusSlotsEarned: 0,
    consecutiveWinStreak: 0,
    streakBonusReason: 'BASELINE',
    maxConcurrentPositions: 2,
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
    cooldownMinutes: 90,
    marketAvgRange: 4.5,
    pacingGateOffset: 0,
    vipScoreThreshold: 88,
    vipObiThreshold: 0.60
  },

  portfolio: {
    cashUSDT: 1000.00,
    holdings: {},
    realizedPnL: 0.00
  },

  activePositions: {},

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
      liquidityFloorUSD: 'Dynamic ($3.5M-$14M)',
      cadenceSec: 90
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
      trailingLocksActive: 0,
      highDecayAlerts: 0
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
    engine: 'Institutional 3-Stage Discovery & Active Quota Pacing Engine',
    source: 'PURE_QUANT_ALGO',
    frequencySeconds: 15,
    lastAnalysisTime: '--:--:--',
    screenerCadence: 'Dynamic 90s-180s (Active Pacing)',
    riskCadence: 'Every 2.5 Seconds',
    persistenceStatus: 'ACTIVE (Non-Destructive)',
    p2pRateStatus: 'Binance P2P Daily Sync Active',
    regimeReasoning: 'Actively pacing trades to consistently utilize 4-8 slots/24h while maintaining positive statistical expectancy.'
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
// LIVE BINANCE P2P USDT/INR RATE SYNC (HOURLY & DAILY)
// -------------------------------------------------------------
async function syncBinanceP2PUsdtInrRate() {
  try {
    let rateFound = 0;

    // 1. Try Binance Public C2C P2P Quote API
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

    // 2. Secondary fallback: Binance Price Reference endpoint
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

    // 3. Third fallback: Standard FX + P2P premium average
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
    const klines = await fetchFromBinance(`/api/v3/klines?symbol=${symbol}&interval=1h&limit=5`);
    if (Array.isArray(klines) && klines.length >= 4) {
      const latest = klines[klines.length - 1];
      const prev1h = klines[klines.length - 2];
      const prev4h = klines[0];

      const currentClose = parseFloat(latest[4]) || 0;
      const open1h = parseFloat(prev1h[1]) || currentClose;
      const open4h = parseFloat(prev4h[1]) || currentClose;

      const change1hPct = open1h > 0 ? ((currentClose - open1h) / open1h) * 100 : 0;
      const change4hPct = open4h > 0 ? ((currentClose - open4h) / open4h) * 100 : 0;

      return {
        change1hPct: Number(change1hPct.toFixed(2)),
        change4hPct: Number(change4hPct.toFixed(2))
      };
    }
  } catch (e) {}
  return { change1hPct: 0, change4hPct: 0 };
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

// -------------------------------------------------------------
// PNL INTEGRITY AUDITOR & INTELLIGENT DATE SANITIZATION
// -------------------------------------------------------------
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

// Reconstruct 24h rolling trade timestamps from trade logs and active positions
function reconcileRollingTradeQuotaFromHistory() {
  const now = Date.now();
  const dayAgo = now - (24 * 60 * 60 * 1000);
  const timestamps = new Set();
  const vipTimestamps = new Set();

  // 1. Add timestamps from all executed trade logs strictly within last 24h
  State.tradeLogs.forEach(l => {
    const tradeTs = parseTradeTimestamp(l);
    if (tradeTs > 0 && tradeTs > dayAgo && tradeTs <= now) {
      timestamps.add(tradeTs);
      if (l.type && l.type.includes('VIP')) {
        vipTimestamps.add(tradeTs);
      }
    }
  });

  // 2. Add timestamps from currently open active positions
  Object.values(State.activePositions || {}).forEach(pos => {
    const openTs = Number(pos.openedAt);
    if (!isNaN(openTs) && openTs > dayAgo && openTs <= now) {
      timestamps.add(openTs);
      if (pos.isVipAlpha) {
        vipTimestamps.add(openTs);
      }
    }
  });

  State.tradeFrequencyController.rollingTrades24h = Array.from(timestamps)
    .sort((a, b) => a - b);

  State.tradeFrequencyController.vipTrades24h = Array.from(vipTimestamps)
    .sort((a, b) => a - b);

  syncRollingTradeQuota();
}

function saveStateToDisk() {
  try {
    const dataToSave = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
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
  } catch (err) {
    State.subsystems.persistence.status = 'ERROR';
  }
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
        State.subsystems.persistence.googleSheetSync = 'ACTIVE';
        saveStateToDisk();
        broadcastState();
        console.log(`[GoogleSheet Sync] Successfully restored ${data.trades.length} trades from Google Drive with clean dates.`);
      }
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
// 24-HOUR ROLLING TRADE QUOTA MANAGER WITH DYNAMIC RATCHET & VIP ALPHA
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

  // 1. Calculate consecutive winning streak from recent settled trades in 24h
  let currentStreak = 0;
  let rollingNetGainUSDT = 0;
  const recentLogs24h = State.tradeLogs.filter(l => {
    const ts = parseTradeTimestamp(l);
    return ts > dayAgo && ts <= now;
  });

  for (const log of recentLogs24h) {
    const pnl = Number(log.pnlUSDT || 0);
    rollingNetGainUSDT += pnl;
    if (pnl > 0.10) {
      currentStreak++;
    } else if (pnl < -0.05) {
      break; // Streak terminates at first net loss
    }
  }

  // 2. Dynamic Earned Quota Ratchet:
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

  const baseMax = State.tradeFrequencyController.baseMaxTradesPer24h || 8;
  const dynamicMaxAllowed = isBypassActive ? 9999 : (baseMax + bonusSlots);

  State.tradeFrequencyController.bonusSlotsEarned = bonusSlots;
  State.tradeFrequencyController.consecutiveWinStreak = currentStreak;
  State.tradeFrequencyController.streakBonusReason = streakReason;
  State.tradeFrequencyController.maxTradesPer24h = dynamicMaxAllowed;

  const count = State.tradeFrequencyController.rollingTrades24h.length;
  const remaining = isBypassActive ? 999 : Math.max(0, dynamicMaxAllowed - count);

  // VIP Alpha Slots (Max 2 Reserve Overrides per 24h)
  const vipUsed = State.tradeFrequencyController.vipTrades24h.length;
  const vipMax = State.tradeFrequencyController.vipAlphaSlotsAllowed || 2;
  const vipRemaining = isBypassActive ? 999 : Math.max(0, vipMax - vipUsed);
  State.tradeFrequencyController.vipAlphaSlotsUsed24h = vipUsed;

  // Accurate Slot Expiration Tracking
  let resetTimeMin = 0;
  let nextReleaseTimestamp = 0;
  let formattedReset = isBypassActive ? 'UNLIMITED TRADES ACTIVE' : 'All Slots Open';

  if (count > 0 && !isBypassActive) {
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

  let circuitBreakerRemainingSec = 0;
  if (State.tradeFrequencyController.globalCircuitBreakerUntil > now) {
    circuitBreakerRemainingSec = Math.round((State.tradeFrequencyController.globalCircuitBreakerUntil - now) / 1000);
    State.tradeFrequencyController.circuitBreakerReason = 'POST_LOSS_FREEZE';
    State.subsystems.quotaManager.status = 'PAUSED_LOSS_COOLDOWN';
  } else if (isBypassActive) {
    State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    State.subsystems.quotaManager.status = 'UNLIMITED_BYPASS_ACTIVE';
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

  State.subsystems.quotaManager.quotaBypassEnabled = isBypassActive;
  State.subsystems.quotaManager.executed24hCount = count;
  State.subsystems.quotaManager.maxAllowed = isBypassActive ? 'UNLIMITED' : dynamicMaxAllowed;
  State.subsystems.quotaManager.baseMaxAllowed = baseMax;
  State.subsystems.quotaManager.bonusSlotsEarned = bonusSlots;
  State.subsystems.quotaManager.consecutiveWinStreak = currentStreak;
  State.subsystems.quotaManager.streakBonusReason = streakReason;
  State.subsystems.quotaManager.quotaRemaining = isBypassActive ? '∞' : remaining;
  State.subsystems.quotaManager.vipSlotsRemaining = isBypassActive ? '∞' : vipRemaining;
  State.subsystems.quotaManager.vipSlotsUsed = vipUsed;
  State.subsystems.quotaManager.resetOldestSlotInMin = resetTimeMin;
  State.subsystems.quotaManager.nextSlotReleaseTimestamp = nextReleaseTimestamp;
  State.subsystems.quotaManager.formattedReset = formattedReset;
  State.subsystems.quotaManager.circuitBreakerRemainingSec = circuitBreakerRemainingSec;
}

// -------------------------------------------------------------
// STAGE 1 & 2: DISCOVERY SCREENER (PACED RUN)
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
    const activeSymbols = Object.keys(State.activePositions);

    const isCompression = (State.marketRegime === 'COMPRESSION');
    const isStarved = (State.tradeFrequencyController.pacingMode === 'STARVED');

    for (const t of tickers) {
      if (!t.symbol.endsWith('USDT')) continue;
      const base = t.symbol.slice(0, -4);
      // Strictly guard single-letter tickers (like TUSDT) from matching pegged tokens
      if (['UP', 'DOWN', 'BEAR', 'BULL', 'USDC', 'FDUSD', 'TUSD', 'EUR', 'DAI', 'USDP', 'BUSD'].includes(base)) continue;

      const quoteVol = parseFloat(t.quoteVolume) || 0;
      const lastPrice = parseFloat(t.lastPrice) || 0;
      const priceChangePct = parseFloat(t.priceChangePercent) || 0;
      const highPrice = parseFloat(t.highPrice) || 0;
      const lowPrice = parseFloat(t.lowPrice) || 0;

      let requiredFloor = 14000000;
      if (priceChangePct >= 7.5) requiredFloor = 3500000;
      else if (priceChangePct >= 4.0) requiredFloor = 5000000;
      else if (priceChangePct >= 1.5) requiredFloor = 8000000;
      else if (isCompression || isStarved) requiredFloor = 6000000;

      if (quoteVol >= requiredFloor && lastPrice > 0 && highPrice > lowPrice) {
        const pricePositionInRange = (lastPrice - lowPrice) / (highPrice - lowPrice);
        const intradayRangePct = ((highPrice - lowPrice) / lowPrice) * 100;

        const minPositionRatio = (isCompression || isStarved) ? 0.48 : 0.55;
        if (pricePositionInRange < minPositionRatio) continue;

        const liquidityDensityScore = Math.min(30, Math.log10(quoteVol) * 3.8);
        const rangeProximityBoost = pricePositionInRange * 15;
        const directionalMomentum = Math.max(0, priceChangePct) * 2.2;

        const breakoutScore = directionalMomentum + liquidityDensityScore + rangeProximityBoost;

        candidates.push({
          symbol: t.symbol,
          baseAsset: base,
          priceUSDT: formatBinanceUSDTPrice(lastPrice),
          priceINR: calculateAccurateINRPrice(lastPrice, State.usdtToInrRate),
          priceChangePct,
          intradayRangePct,
          pricePositionInRange,
          volumeM: Math.round(quoteVol / 1000000),
          breakoutScore,
          isActive: activeSymbols.includes(t.symbol)
        });
      }
    }

    State.subsystems.screener.qualifiedCandidatesCount = candidates.length;

    const activeCandidates = candidates.filter(c => c.isActive);
    const nonActiveSorted = candidates
      .filter(c => !c.isActive)
      .sort((a, b) => b.breakoutScore - a.breakoutScore);

    const neededSlots = Math.max(0, 10 - activeCandidates.length);
    const topPicks = [...activeCandidates, ...nonActiveSorted.slice(0, neededSlots)];

    for (const p of topPicks) {
      const shortTerm = await fetchShortTermMomentum(p.symbol);
      p.change1hPct = shortTerm.change1hPct;
      p.change4hPct = shortTerm.change4hPct;

      if (p.change1hPct > 0.8) p.breakoutScore += (p.change1hPct * 2.2);
      if (p.change4hPct > 2.5) p.breakoutScore += (p.change4hPct * 1.4);
    }

    topPicks.sort((a, b) => b.breakoutScore - a.breakoutScore);

    if (topPicks.length > 0) {
      const updatedUniverse = {};
      topPicks.forEach(c => {
        const existing = State.universe[c.symbol];
        updatedUniverse[c.symbol] = {
          name: c.baseAsset,
          symbol: c.baseAsset,
          priceUSDT: c.priceUSDT,
          priceINR: c.priceINR,
          priceChangePct: c.priceChangePct,
          change1hPct: c.change1hPct || 0,
          change4hPct: c.change4hPct || 0,
          intradayRangePct: c.intradayRangePct,
          pricePositionInRange: c.pricePositionInRange,
          volumeM: c.volumeM,
          score: existing ? existing.score : 76,
          direction: existing ? existing.direction : 'BUY',
          obi: existing ? existing.obi : 0.55,
          cvd: existing ? existing.cvd : 3.5,
          source: 'QUANT_ALGO',
          reasoning: `Rank: ${c.breakoutScore.toFixed(1)} ($${c.volumeM}M Vol, 1h: ${c.change1hPct >= 0 ? '+' : ''}${(c.change1hPct || 0).toFixed(1)}%, ${(c.pricePositionInRange * 100).toFixed(0)}% Range)`,
          verifiedAt: Date.now()
        };
      });

      State.universe = updatedUniverse;
      broadcastPricesOnly();
    }

    State.subsystems.screener.status = 'ACTIVE';
    State.subsystems.screener.lastRunTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
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
// AUTOPILOT ENTRY MATRIX WITH VIP ALPHA OVERRIDE
// -------------------------------------------------------------
async function evaluateAutopilotEntries(regimeConfig) {
  if (!State.autopilotEnabled) return;

  syncRollingTradeQuota();

  if (State.tradeFrequencyController.circuitBreakerReason !== 'NORMAL') return;

  const currentCount = Object.keys(State.activePositions).length;
  if (currentCount >= State.tradeFrequencyController.maxConcurrentPositions) return;

  if (State.portfolio.cashUSDT < 30) return;
  if (State.warmupCycles < 1) return;

  const config = regimeConfig || State.activeThresholds;
  const isBypassActive = Boolean(State.tradeFrequencyController.quotaBypassEnabled);
  const standardQuotaAvailable = isBypassActive || (State.subsystems.quotaManager.quotaRemaining > 0);
  const vipSlotsRemaining = isBypassActive ? 999 : (State.subsystems.quotaManager.vipSlotsRemaining || 0);

  // If neither standard quota nor VIP reserve is available, abort
  if (!standardQuotaAvailable && vipSlotsRemaining <= 0) return;

  const sortedCandidates = Object.keys(State.universe)
    .filter(k => !State.activePositions[k])
    .sort((a, b) => (State.universe[b].score || 0) - (State.universe[a].score || 0));

  for (const sym of sortedCandidates) {
    const coin = State.universe[sym];
    if (!coin || !coin.score) continue;

    if (State.cooldowns[sym]) {
      const remaining = State.cooldowns[sym] - Date.now();
      if (remaining > 0) continue;
      delete State.cooldowns[sym];
    }

    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    const isMicroAsset = (coin.priceUSDT && coin.priceUSDT < 0.01);
    const allowedSpread = isMicroAsset ? 0.35 : config.maxSpread;
    if (book.spreadPct > allowedSpread) continue;

    if (!coin.priceUSDT || coin.priceUSDT <= 0) continue;
    if (!coin.verifiedAt || (Date.now() - coin.verifiedAt) > 30000) continue;

    // Check Case A: Standard Quota Available (or Quota Bypass is Active)
    if (standardQuotaAvailable && coin.score >= config.scoreThreshold && book.obi >= config.obiThreshold) {
      executeSimulatedBuy(
        sym, 
        coin.priceUSDT, 
        `AQPE High-Conviction Confluence (${config.regime}|${config.pacingMode}${isBypassActive ? '|UNLIMITED' : ''}: Score ${coin.score} >= ${config.scoreThreshold}, OBI ${(book.obi * 100).toFixed(0)}% >= ${(config.obiThreshold * 100).toFixed(0)}%, Spread ${book.spreadPct.toFixed(2)}%)`,
        false
      );
      break;
    }

    // Check Case B: VIP Alpha Override Gate (When standard quota exhausted)
    // Catches God-candle surges like T/USDT (+22%) or ANIME (+20%)
    if (!standardQuotaAvailable && vipSlotsRemaining > 0) {
      const isExtremeMomentum = (coin.change1hPct >= 5.0 || coin.priceChangePct >= 12.0);
      const isExtremeConfluence = (coin.score >= 88 && book.obi >= 0.60);

      if (isExtremeMomentum && isExtremeConfluence) {
        executeSimulatedBuy(
          sym,
          coin.priceUSDT,
          `⚡ VIP Alpha Override Slot (${vipSlotsRemaining} left): God-Candle Breakout! Score ${coin.score} >= 88, OBI ${(book.obi * 100).toFixed(0)}% >= 60%, 1h: +${(coin.change1hPct || 0).toFixed(1)}%, 24h: +${coin.priceChangePct.toFixed(1)}%`,
          true
        );
        break;
      }
    }
  }
}

// -------------------------------------------------------------
// ORDER EXECUTION & RISK MANAGEMENT
// -------------------------------------------------------------
function executeSimulatedBuy(pairKey, priceUSDT, reason, isVipAlpha = false) {
  if (State.portfolio.cashUSDT < 30) return;
  
  const notionalUSDT = Math.min(50, State.portfolio.cashUSDT);
  const qty = notionalUSDT / priceUSDT;

  State.portfolio.cashUSDT -= notionalUSDT;

  const isMicroAsset = (priceUSDT < 0.10);
  const slPct = isMicroAsset ? 0.0095 : 0.0080;
  const sl = priceUSDT * (1 - slPct);
  const tp1 = priceUSDT * (1 + 0.016);

  State.activePositions[pairKey] = {
    pairKey,
    pairSymbol: State.universe[pairKey]?.symbol || pairKey,
    entryPriceUSDT: priceUSDT,
    qty,
    originalQty: qty,
    notionalUSDT,
    originalNotionalUSDT: notionalUSDT,
    sl,
    tp1,
    openedAt: Date.now(),
    isTrailingActive: false,
    tp1Secured: false,
    highWatermarkUSDT: priceUSDT,
    decayRisk: 5,
    consecutiveDecayTicks: 0,
    predictiveStatus: 'SAFE',
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

  pos.qty = halfQty;
  pos.notionalUSDT = halfNotional;
  pos.tp1Secured = true;
  pos.isTrailingActive = true;
  pos.sl = pos.entryPriceUSDT * 1.0025;
  pos.highWatermarkUSDT = Math.max(pos.highWatermarkUSDT || exitPriceUSDT, exitPriceUSDT);

  if (State.portfolio.holdings[pairKey]) {
    State.portfolio.holdings[pairKey].qty = halfQty;
    State.portfolio.holdings[pairKey].totalCostUSDT = halfNotional;
  }

  const now = new Date();
  const istTime = now.toLocaleTimeString('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });
  const istDate = now.toLocaleDateString('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  });

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
    regime: State.activeThresholds?.regime || State.marketRegime || 'BALANCED',
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

  delete State.portfolio.holdings[pairKey];
  delete State.activePositions[pairKey];

  if (pnlUSDT < 0) {
    const cooldownMinutes = State.activeThresholds?.cooldownMinutes || 90;
    State.cooldowns[pairKey] = Date.now() + (cooldownMinutes * 60 * 1000);
    State.tradeFrequencyController.globalCircuitBreakerUntil = Date.now() + (20 * 60 * 1000);
    State.tradeFrequencyController.circuitBreakerReason = 'POST_LOSS_FREEZE';
    // Instant Retract Guard: Any single loss collapses bonus quota slots back to baseline
    State.tradeFrequencyController.consecutiveWinStreak = 0;
    State.tradeFrequencyController.bonusSlotsEarned = 0;
  }

  const now = new Date();
  const istTime = now.toLocaleTimeString('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });
  const istDate = now.toLocaleDateString('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  });

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
    regime: State.activeThresholds?.regime || State.marketRegime || 'BALANCED',
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
// SENTRY RISK MONITOR
// -------------------------------------------------------------
async function monitorOpenPositionsHealth() {
  const openKeys = Object.keys(State.activePositions);
  State.subsystems.predictiveSentry.lastMonitorTick = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  State.subsystems.predictiveSentry.activePositionsMonitored = openKeys.length;

  let trailingCount = 0;
  let highDecayCount = 0;

  if (openKeys.length === 0) {
    State.subsystems.predictiveSentry.trailingLocksActive = 0;
    State.subsystems.predictiveSentry.highDecayAlerts = 0;
    State.subsystems.predictiveSentry.status = 'WATCHING';
    return;
  }

  State.subsystems.predictiveSentry.status = 'ACTIVE_MONITORING';

  for (const sym of openKeys) {
    const pos = State.activePositions[sym];
    if (!pos) continue;

    if (pos.isTrailingActive) trailingCount++;

    const livePriceUSDT = State.universe[sym]?.priceUSDT || pos.entryPriceUSDT;
    const gainPct = ((livePriceUSDT - pos.entryPriceUSDT) / pos.entryPriceUSDT) * 100;
    const holdSeconds = (Date.now() - pos.openedAt) / 1000;
    const isMicroAsset = (pos.entryPriceUSDT < 0.10);

    if (!pos.highWatermarkUSDT || livePriceUSDT > pos.highWatermarkUSDT) {
      pos.highWatermarkUSDT = livePriceUSDT;
    }

    // 1. Partial TP1 Take-Profit (+1.60% Gross)
    if (livePriceUSDT >= pos.tp1 && !pos.tp1Secured) {
      settlePartialPosition(sym, livePriceUSDT, `TP1 Banked (+${gainPct.toFixed(2)}%), runner active`);
      continue;
    }

    // 2. High Watermark Runner Trail Logic
    if (pos.tp1Secured) {
      const runnerTrailPrice = pos.highWatermarkUSDT * (1 - 0.012);
      pos.sl = Math.max(pos.entryPriceUSDT * 1.0025, runnerTrailPrice);

      if (livePriceUSDT <= pos.sl) {
        settlePosition(sym, 'RUNNER EXIT', livePriceUSDT, `Runner trailed from peak ${formatBinanceUSDTPrice(pos.highWatermarkUSDT)} (+${gainPct.toFixed(2)}%)`);
        continue;
      }
    } else {
      // 3. Early Trailing Lock (Activates after +1.20% gain, avoiding spread traps)
      if (gainPct >= 1.20 && !pos.isTrailingActive) {
        pos.isTrailingActive = true;
        pos.sl = pos.entryPriceUSDT * 1.0025;
      }

      // 4. Hard Stop Loss Boundary
      if (livePriceUSDT <= pos.sl) {
        settlePosition(sym, pos.isTrailingActive ? 'TRAILING SL' : 'STOP LOSS', livePriceUSDT, `Hit boundary (${gainPct.toFixed(2)}%)`);
        continue;
      }

      // 5. Stagnation Exit: Extended to 12 minutes (720s) for high-momentum assets
      const maxStagnationSeconds = (State.universe[sym]?.priceChangePct >= 5.0) ? 720 : 540;
      if (holdSeconds >= maxStagnationSeconds && gainPct < 0.25) {
        settlePosition(sym, 'STAGNATION CUT', livePriceUSDT, `No momentum continuation within ${Math.round(maxStagnationSeconds/60)} minutes`);
        continue;
      }
    }

    // 6. Intelligent Order Book Decay Metric with Multi-Tick Confirmation
    let decay = 5;
    const realBook = await fetchRealOrderBookDepth(sym);

    if (realBook) {
      if (realBook.obi < 0.38) decay += 25;
      else if (realBook.obi < 0.46) decay += 10;
      
      const spreadPenaltyThreshold = isMicroAsset ? 0.30 : 0.12;
      if (realBook.spreadPct > spreadPenaltyThreshold) decay += 15;
    }

    const dipThreshold1 = isMicroAsset ? -0.45 : -0.30;
    const dipThreshold2 = isMicroAsset ? -0.65 : -0.45;
    if (gainPct <= dipThreshold1) decay += 15;
    if (gainPct <= dipThreshold2) decay += 25;

    pos.decayRisk = Math.min(100, decay);
    pos.predictiveStatus = pos.decayRisk >= 55 ? 'EXITING' : (pos.decayRisk >= 30 ? 'CAUTION' : 'SAFE');

    if (pos.decayRisk >= 55) {
      pos.consecutiveDecayTicks = (pos.consecutiveDecayTicks || 0) + 1;
      highDecayCount++;
    } else {
      pos.consecutiveDecayTicks = 0;
    }

    // 7. Predictive Cut Protection Window (Requires 150s grace + 3-tick decay)
    const predictiveDipGate = isMicroAsset ? -0.75 : -0.55;
    if (holdSeconds >= 150 && !pos.tp1Secured) {
      if (gainPct <= predictiveDipGate && (pos.consecutiveDecayTicks >= 3)) {
        settlePosition(sym, 'PREDICTIVE SL', livePriceUSDT, `Persistent decay confirmed over 3 ticks at ${gainPct.toFixed(2)}% (Decay: ${pos.decayRisk}%)`);
        continue;
      }
    }
  }

  State.subsystems.predictiveSentry.trailingLocksActive = trailingCount;
  State.subsystems.predictiveSentry.highDecayAlerts = highDecayCount;
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
      source: State.universe[k].source,
      reasoning: State.universe[k].reasoning
    };
  });

  const payload = JSON.stringify({
    type: 'PRICE_TICK',
    prices: priceData,
    activePositions: State.activePositions,
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
      } else if (data.type === 'RECONCILE_QUOTA') {
        reconcileRollingTradeQuotaFromHistory();
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'CLOSE_POSITION' && data.pairKey) {
        const livePriceUSDT = State.universe[data.pairKey]?.priceUSDT || State.activePositions[data.pairKey]?.entryPriceUSDT;
        if (livePriceUSDT) settlePosition(data.pairKey, 'MANUAL EXIT', livePriceUSDT, 'Closed by user command');
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

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
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

app.get('/api/settings/googlesheet/test', async (req, res) => {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) {
    return res.status(400).json({ success: false, message: 'GOOGLE_SHEET_URL is not set.' });
  }

  const now = new Date();
  const testPayload = {
    id: Date.now(),
    date: now.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric' }),
    time: now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true }),
    pair: 'TEST/USDT',
    type: 'SYNC TEST',
    entryPriceUSDT: 100.00,
    exitPriceUSDT: 101.60,
    pnlUSDT: 0.80,
    pnlINR: Number((0.80 * State.usdtToInrRate).toFixed(2)),
    reason: 'Manual diagnostic test from Render',
    regime: State.activeThresholds?.regime || 'BALANCED',
    volatilityPct: State.activeThresholds?.marketAvgRange || 4.5
  };

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const response = await fetch(targetUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(testPayload),
      signal: controller.signal
    });
    clearTimeout(timeout);

    const bodyText = await response.text();
    res.json({
      success: response.ok,
      status: response.status,
      response: bodyText,
      message: response.ok ? 'Test row posted to Google Sheet!' : 'Google Sheet returned an error.'
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
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
  console.log(`[Quant Server] Running on port ${PORT}`);

  setTimeout(runDynamicMarketScreener, 2000);
  setInterval(runPureQuantScoring, 15 * 1000);
  setInterval(syncLiveBinancePrices, 2500);
  setInterval(syncBinanceP2PUsdtInrRate, 60 * 60 * 1000);
});
