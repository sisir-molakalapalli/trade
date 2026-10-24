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
  marketRegime: 'BALANCED',
  warmupCycles: 0,
  cooldowns: {},
  googleSheetUrl: GOOGLE_SHEET_URL,

  tradeFrequencyController: {
    maxTradesPer24h: 8,
    maxConcurrentPositions: 2,
    rollingTrades24h: [],
    globalCircuitBreakerUntil: 0,
    circuitBreakerReason: 'NORMAL'
  },

  activeThresholds: {
    regime: 'BALANCED',
    scoreThreshold: 81,
    obiThreshold: 0.56,
    maxSpread: 0.08,
    cooldownMinutes: 90,
    marketAvgRange: 4.5
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
      nextRunCountdownSec: 180,
      scannedPairsCount: 0,
      qualifiedCandidatesCount: 0,
      liquidityFloorUSD: 'Dynamic ($8M-$18M)'
    },
    regimeController: {
      status: 'CALIBRATED',
      currentRegime: 'BALANCED',
      marketAvgVolatilityPct: 4.5,
      liveScoreGate: 81,
      liveObiGatePct: 56,
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
      executed24hCount: 0,
      maxAllowed: 8,
      quotaRemaining: 8,
      resetOldestSlotInMin: 0,
      formattedReset: 'None Pending',
      circuitBreakerRemainingSec: 0
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
    engine: 'Institutional 3-Stage Discovery & Low-Frequency Execution Engine',
    source: 'PURE_QUANT_ALGO',
    frequencySeconds: 15,
    lastAnalysisTime: '--:--:--',
    screenerCadence: 'Every 3 Mins ($8M-$18M Dynamic Floor)',
    riskCadence: 'Every 2.5 Seconds',
    persistenceStatus: 'ACTIVE (Non-Destructive)',
    regimeReasoning: 'Enforcing 4-8 trades/24h ceiling with dynamic liquidity floor and recalibrated score gate.'
  },

  universe: { ...DEFAULT_UNIVERSE },
  tradeLogs: []
};

let nextScreenerTargetTimestamp = Date.now() + (3 * 60 * 1000);

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
// PNL INTEGRITY AUDITOR & STATE PERSISTENCE
// -------------------------------------------------------------
function sanitizeAndAuditTradeHistory(logs) {
  if (!Array.isArray(logs)) return [];
  
  const map = new Map();
  logs.forEach(l => {
    if (!l || !l.id) return;
    
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
        State.subsystems.persistence.googleSheetSync = 'ACTIVE';
        saveStateToDisk();
        broadcastState();
        console.log(`[GoogleSheet Sync] Successfully restored ${data.trades.length} trades from Google Drive`);
      }
    } else {
      console.warn(`[GoogleSheet Sync] HTTP status: ${res.status}`);
    }
  } catch (err) {
    console.error('[GoogleSheet Sync Error]', err.message);
  }
}

async function appendTradeToGoogleDrive(tradeLog) {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) {
    console.warn('[GoogleSheet Append] No GOOGLE_SHEET_URL configured, skipping sheet sync.');
    return;
  }

  try {
    console.log(`[GoogleSheet Append] Sending trade ${tradeLog.pair} (${tradeLog.type}) to Google Sheet...`);
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
      console.log(`[GoogleSheet Append] Trade ${tradeLog.pair} recorded in Google Sheet.`);
    } else {
      console.warn(`[GoogleSheet Append] Received status ${res.status}`);
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
      if (parsed.googleSheetUrl) {
        State.googleSheetUrl = parsed.googleSheetUrl;
      }
    }
  } catch (err) {}
}

loadStateFromDisk();
syncWithGoogleDrive();

// -------------------------------------------------------------
// RECALIBRATED VOLATILITY REGIME DETECTOR
// -------------------------------------------------------------
function calculateMarketVolatilityRegime() {
  const coins = Object.values(State.universe);
  if (coins.length === 0) {
    return {
      regime: 'BALANCED',
      scoreThreshold: 81,
      obiThreshold: 0.56,
      maxSpread: 0.08,
      cooldownMinutes: 90,
      marketAvgRange: 4.5
    };
  }

  let totalRangePct = 0;
  let count = 0;

  coins.forEach(c => {
    let range = 0;
    if (typeof c.intradayRangePct === 'number' && c.intradayRangePct > 0) {
      range = c.intradayRangePct;
    } else if (typeof c.priceChangePct === 'number') {
      range = Math.abs(c.priceChangePct) * 1.4;
    }
    if (range > 0) {
      totalRangePct += range;
      count++;
    }
  });

  const avgRange = count > 0 ? (totalRangePct / count) : 4.5;
  const roundedRange = Math.round(avgRange * 10) / 10;

  let computedRegime = 'BALANCED';
  let scoreThreshold = 81;
  let obiThreshold = 0.56;
  let maxSpread = 0.08;
  let cooldownMinutes = 90;

  if (avgRange >= 9.5) {
    computedRegime = 'TURBULENT';
    scoreThreshold = 86;
    obiThreshold = 0.59;
    maxSpread = 0.06;
    cooldownMinutes = 120;
    State.tradeFrequencyController.circuitBreakerReason = 'TURBULENT_FREEZE';
  } else if (avgRange <= 4.0) {
    computedRegime = 'COMPRESSION';
    scoreThreshold = 79;
    obiThreshold = 0.55;
    maxSpread = 0.09;
    cooldownMinutes = 60;
    if (State.tradeFrequencyController.circuitBreakerReason === 'TURBULENT_FREEZE') {
      State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    }
  } else {
    if (State.tradeFrequencyController.circuitBreakerReason === 'TURBULENT_FREEZE') {
      State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    }
  }

  State.subsystems.regimeController.status = 'ACTIVE';
  State.subsystems.regimeController.currentRegime = computedRegime;
  State.subsystems.regimeController.marketAvgVolatilityPct = roundedRange;
  State.subsystems.regimeController.liveScoreGate = scoreThreshold;
  State.subsystems.regimeController.liveObiGatePct = Math.round(obiThreshold * 100);
  State.subsystems.regimeController.liveMaxSpreadPct = maxSpread;
  State.subsystems.regimeController.lastAdjustmentTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

  State.activeThresholds = {
    regime: computedRegime,
    scoreThreshold,
    obiThreshold,
    maxSpread,
    cooldownMinutes,
    marketAvgRange: roundedRange
  };

  return State.activeThresholds;
}

// -------------------------------------------------------------
// 24-HOUR ROLLING TRADE QUOTA MANAGER
// -------------------------------------------------------------
function syncRollingTradeQuota() {
  const now = Date.now();
  const dayAgo = now - (24 * 60 * 60 * 1000);
  
  State.tradeFrequencyController.rollingTrades24h = (State.tradeFrequencyController.rollingTrades24h || [])
    .filter(ts => ts > dayAgo);

  const count = State.tradeFrequencyController.rollingTrades24h.length;
  const maxAllowed = State.tradeFrequencyController.maxTradesPer24h;
  const remaining = Math.max(0, maxAllowed - count);

  let resetTimeMin = 0;
  let formattedReset = 'None Pending';
  if (count > 0) {
    const oldest = Math.min(...State.tradeFrequencyController.rollingTrades24h);
    const msRemaining = Math.max(0, (oldest + (24 * 60 * 60 * 1000)) - now);
    resetTimeMin = Math.round(msRemaining / 60000);
    const hrs = Math.floor(resetTimeMin / 60);
    const mins = resetTimeMin % 60;
    formattedReset = hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m`;
  }

  let circuitBreakerRemainingSec = 0;
  if (State.tradeFrequencyController.globalCircuitBreakerUntil > now) {
    circuitBreakerRemainingSec = Math.round((State.tradeFrequencyController.globalCircuitBreakerUntil - now) / 1000);
    State.tradeFrequencyController.circuitBreakerReason = 'POST_LOSS_FREEZE';
    State.subsystems.quotaManager.status = 'PAUSED_LOSS_COOLDOWN';
  } else if (State.marketRegime === 'TURBULENT') {
    State.tradeFrequencyController.circuitBreakerReason = 'TURBULENT_FREEZE';
    State.subsystems.quotaManager.status = 'PAUSED_TURBULENCE';
  } else if (remaining === 0) {
    State.tradeFrequencyController.circuitBreakerReason = 'QUOTA_EXHAUSTED';
    State.subsystems.quotaManager.status = 'QUOTA_FILLED';
  } else {
    State.tradeFrequencyController.circuitBreakerReason = 'NORMAL';
    State.subsystems.quotaManager.status = 'NORMAL';
  }

  State.subsystems.quotaManager.executed24hCount = count;
  State.subsystems.quotaManager.maxAllowed = maxAllowed;
  State.subsystems.quotaManager.quotaRemaining = remaining;
  State.subsystems.quotaManager.resetOldestSlotInMin = resetTimeMin;
  State.subsystems.quotaManager.formattedReset = formattedReset;
  State.subsystems.quotaManager.circuitBreakerRemainingSec = circuitBreakerRemainingSec;
}

// -------------------------------------------------------------
// STAGE 1 & 2: DISCOVERY SCREENER
// -------------------------------------------------------------
async function runDynamicMarketScreener() {
  State.subsystems.screener.status = 'SCANNING';
  try {
    const tickers = await fetchFromBinance('/api/v3/ticker/24hr');
    if (!Array.isArray(tickers) || tickers.length === 0) {
      State.subsystems.screener.status = 'IDLE';
      return;
    }

    State.subsystems.screener.scannedPairsCount = tickers.length;
    const candidates = [];
    const activeSymbols = Object.keys(State.activePositions);

    // Current regime determines the volume floor
    const isCompression = (State.marketRegime === 'COMPRESSION');

    for (const t of tickers) {
      if (!t.symbol.endsWith('USDT')) continue;
      const base = t.symbol.replace('USDT', '');
      if (['UP', 'DOWN', 'BEAR', 'BULL', 'USDC', 'FDUSD', 'TUSD', 'EUR', 'DAI'].some(x => base.endsWith(x))) continue;

      const quoteVol = parseFloat(t.quoteVolume) || 0;
      const lastPrice = parseFloat(t.lastPrice) || 0;
      const priceChangePct = parseFloat(t.priceChangePercent) || 0;
      const highPrice = parseFloat(t.highPrice) || 0;
      const lowPrice = parseFloat(t.lowPrice) || 0;

      // Dynamic floor adapts to market regime and velocity
      let requiredFloor = 14000000;
      if (priceChangePct >= 4.0) requiredFloor = 6000000;
      else if (priceChangePct >= 1.5) requiredFloor = 9000000;
      else if (isCompression) requiredFloor = 8000000;

      if (quoteVol >= requiredFloor && lastPrice > 0 && highPrice > lowPrice) {
        const pricePositionInRange = (lastPrice - lowPrice) / (highPrice - lowPrice);
        const intradayRangePct = ((highPrice - lowPrice) / lowPrice) * 100;

        // In compression regimes, allow coins trading in upper 50% of range
        const minPositionRatio = isCompression ? 0.50 : 0.58;
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

      if (p.change1hPct > 1.0) p.breakoutScore += (p.change1hPct * 2.0);
      if (p.change4hPct > 3.0) p.breakoutScore += (p.change4hPct * 1.3);
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
    nextScreenerTargetTimestamp = Date.now() + (3 * 60 * 1000);
  } catch (err) {
    State.subsystems.screener.status = 'ERROR';
  }
}

// -------------------------------------------------------------
// STAGE 3: 20-LEVEL MICROSTRUCTURE QUANT EVALUATOR
// -------------------------------------------------------------
async function runPureQuantScoring() {
  State.subsystems.microstructure.status = 'CALCULATING';
  const symbols = Object.keys(State.universe);
  const now = Date.now();

  for (const sym of symbols) {
    const coin = State.universe[sym];
    if (!coin) continue;

    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    coin.obi = book.obi;
    coin.spreadPct = book.spreadPct;

    const s_obi = Math.min(40, Math.max(0, Math.round((book.obi - 0.40) * 140)));
    const changePct = typeof coin.priceChangePct === 'number' ? coin.priceChangePct : 2.0;
    const change1h = typeof coin.change1hPct === 'number' ? coin.change1hPct : 0;
    
    const effectiveMom = (changePct * 0.6) + (change1h * 1.5);
    const s_mom = Math.min(35, Math.max(0, Math.round((effectiveMom + 2.5) * 3.6)));
    
    const volM = coin.volumeM || 25;
    const s_vol = Math.min(25, Math.max(5, Math.round(Math.log10(Math.max(1, volM)) * 8.5)));

    const compositeScore = Math.min(98, Math.max(45, s_obi + s_mom + s_vol));
    coin.score = compositeScore;

    if (compositeScore >= 81 && book.obi >= 0.56) {
      coin.direction = 'STRONG BUY';
      coin.reasoning = `High Conviction Confluence: OBI ${(book.obi * 100).toFixed(0)}% Bids, Score ${compositeScore}/100`;
    } else if (compositeScore >= 75) {
      coin.direction = 'BUY';
      coin.reasoning = `Moderate Bid Inflow: Score ${compositeScore}, OBI ${(book.obi * 100).toFixed(0)}%`;
    } else {
      coin.direction = 'NEUTRAL';
      coin.reasoning = `Consolidation Base: OBI ${(book.obi * 100).toFixed(0)}%`;
    }

    coin.verifiedAt = now;
  }

  State.subsystems.microstructure.cyclesCompleted++;
  State.subsystems.microstructure.lastCycleTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  State.subsystems.microstructure.status = 'ONLINE';
  State.quantTelemetry.lastAnalysisTime = State.subsystems.microstructure.lastCycleTime;

  const regimeConfig = calculateMarketVolatilityRegime();
  syncRollingTradeQuota();
  await evaluateAutopilotEntries(regimeConfig);
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY MATRIX
// -------------------------------------------------------------
async function evaluateAutopilotEntries(regimeConfig) {
  if (!State.autopilotEnabled) return;

  syncRollingTradeQuota();

  if (State.tradeFrequencyController.circuitBreakerReason !== 'NORMAL') return;
  if (State.subsystems.quotaManager.quotaRemaining <= 0) return;

  const currentCount = Object.keys(State.activePositions).length;
  if (currentCount >= State.tradeFrequencyController.maxConcurrentPositions) return;

  if (State.portfolio.cashUSDT < 30) return;
  if (State.warmupCycles < 1) return;

  const config = regimeConfig || State.activeThresholds;

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

    if (coin.score < config.scoreThreshold) continue;

    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    if (book.obi < config.obiThreshold) continue;

    const isMicroAsset = (coin.priceUSDT && coin.priceUSDT < 0.01);
    const allowedSpread = isMicroAsset ? 0.20 : config.maxSpread;
    if (book.spreadPct > allowedSpread) continue;

    if (!coin.priceUSDT || coin.priceUSDT <= 0) continue;
    if (!coin.verifiedAt || (Date.now() - coin.verifiedAt) > 30000) continue;

    executeSimulatedBuy(
      sym, 
      coin.priceUSDT, 
      `High-Conviction Confluence (${config.regime}: Score ${coin.score} >= ${config.scoreThreshold}, OBI ${(book.obi * 100).toFixed(0)}% >= ${(config.obiThreshold * 100).toFixed(0)}%, Spread ${book.spreadPct.toFixed(2)}%)`
    );
    break;
  }
}

// -------------------------------------------------------------
// ORDER EXECUTION & RISK MANAGEMENT
// -------------------------------------------------------------
function executeSimulatedBuy(pairKey, priceUSDT, reason) {
  if (State.portfolio.cashUSDT < 30) return;
  
  const notionalUSDT = Math.min(50, State.portfolio.cashUSDT);
  const qty = notionalUSDT / priceUSDT;

  State.portfolio.cashUSDT -= notionalUSDT;

  const sl = priceUSDT * (1 - 0.008);
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
    predictiveStatus: 'SAFE'
  };

  State.portfolio.holdings[pairKey] = {
    symbol: State.universe[pairKey]?.symbol || pairKey,
    qty,
    avgBuyPriceUSDT: priceUSDT,
    totalCostUSDT: notionalUSDT
  };

  State.tradeFrequencyController.rollingTrades24h.push(Date.now());
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

  // Bank half of notional plus profit back to free cash
  State.portfolio.cashUSDT += halfValUSDT;
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + pnlUSDT).toFixed(4));

  pos.qty = halfQty;
  pos.notionalUSDT = halfNotional;
  pos.tp1Secured = true;
  pos.isTrailingActive = true;
  // Move stop to breakeven + fee buffer (+0.25%)
  pos.sl = pos.entryPriceUSDT * 1.0025;
  pos.highWatermarkUSDT = Math.max(pos.highWatermarkUSDT || exitPriceUSDT, exitPriceUSDT);

  if (State.portfolio.holdings[pairKey]) {
    State.portfolio.holdings[pairKey].qty = halfQty;
    State.portfolio.holdings[pairKey].totalCostUSDT = halfNotional;
  }

  const istTime = new Date().toLocaleTimeString('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });
  const istDate = new Date().toLocaleDateString('en-IN', {
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
    date: istDate
  };

  State.tradeLogs.unshift(logEntry);
  if (State.tradeLogs.length > 500) State.tradeLogs.pop();

  recalculateTrueRealizedPnL();
  appendTradeToGoogleDrive(logEntry);
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

  // If trade ended in profit, do NOT quarantine the coin so we can ride continuation setups
  if (pnlUSDT < 0) {
    const cooldownMinutes = State.activeThresholds?.cooldownMinutes || 90;
    State.cooldowns[pairKey] = Date.now() + (cooldownMinutes * 60 * 1000);
    State.tradeFrequencyController.globalCircuitBreakerUntil = Date.now() + (20 * 60 * 1000);
    State.tradeFrequencyController.circuitBreakerReason = 'POST_LOSS_FREEZE';
  }

  const istTime = new Date().toLocaleTimeString('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });
  const istDate = new Date().toLocaleDateString('en-IN', {
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
    date: istDate
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

    // Track High Watermark for dynamic trailing runners
    if (!pos.highWatermarkUSDT || livePriceUSDT > pos.highWatermarkUSDT) {
      pos.highWatermarkUSDT = livePriceUSDT;
    }

    // STAGE 1 TAKE PROFIT: Bank 50% at +1.60%
    if (livePriceUSDT >= pos.tp1 && !pos.tp1Secured) {
      settlePartialPosition(sym, livePriceUSDT, `TP1 Banked (+${gainPct.toFixed(2)}%), runner active`);
      continue;
    }

    // RUNNER STAGE (After TP1): Dynamic Trailing Stop (-1.20% from high watermark)
    if (pos.tp1Secured) {
      const runnerTrailPrice = pos.highWatermarkUSDT * (1 - 0.012);
      // Stop is always at least entry + 0.25%
      pos.sl = Math.max(pos.entryPriceUSDT * 1.0025, runnerTrailPrice);

      if (livePriceUSDT <= pos.sl) {
        settlePosition(sym, 'RUNNER EXIT', livePriceUSDT, `Runner trailed from peak ${formatBinanceUSDTPrice(pos.highWatermarkUSDT)} (+${gainPct.toFixed(2)}%)`);
        continue;
      }
    } else {
      // PRE-TP1 TRAILING LOCK: Only activate when price has comfortable +1.20% cushion
      if (gainPct >= 1.20 && !pos.isTrailingActive) {
        pos.isTrailingActive = true;
        pos.sl = pos.entryPriceUSDT * 1.0025; // Lock breakeven + fees
      }

      // Hard or trailing stop triggered
      if (livePriceUSDT <= pos.sl) {
        settlePosition(sym, pos.isTrailingActive ? 'TRAILING SL' : 'STOP LOSS', livePriceUSDT, `Hit boundary (${gainPct.toFixed(2)}%)`);
        continue;
      }

      // Stagnation Cut: Only if trade remains dead after 8 minutes
      if (holdSeconds >= 480 && gainPct < 0.25) {
        settlePosition(sym, 'STAGNATION CUT', livePriceUSDT, 'No momentum impulse within 8 minutes');
        continue;
      }
    }

    // PREDICTIVE ORDERBOOK INTEGRITY MONITOR
    let decay = 5;
    const realBook = await fetchRealOrderBookDepth(sym);

    if (realBook) {
      if (realBook.obi < 0.40) decay += 25;
      else if (realBook.obi < 0.48) decay += 10;
      if (realBook.spreadPct > 0.12) decay += 15;
    }

    if (gainPct <= -0.30) decay += 15;
    if (gainPct <= -0.45) decay += 25;

    pos.decayRisk = Math.min(100, decay);
    pos.predictiveStatus = pos.decayRisk >= 55 ? 'EXITING' : (pos.decayRisk >= 30 ? 'CAUTION' : 'SAFE');

    if (pos.decayRisk >= 50) highDecayCount++;

    // 90-Second Retest Window: Never trigger premature predictive micro-cuts during initial entry retests
    if (holdSeconds >= 90 && !pos.tp1Secured) {
      if (gainPct <= -0.50 && pos.decayRisk >= 55) {
        settlePosition(sym, 'PREDICTIVE SL', livePriceUSDT, `Loss mitigated at ${gainPct.toFixed(2)}% (Decay: ${pos.decayRisk}%)`);
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
    usdtToInrRate: State.usdtToInrRate
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
      source: State.universe[k].source
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
    usdtToInrRate: State.usdtToInrRate
  }));

  ws.on('message', msg => {
    try {
      const data = JSON.parse(msg.toString());
      if (data.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(data.enabled);
        saveStateToDisk();
        broadcastState();
      } else if (data.type === 'CLOSE_POSITION' && data.pairKey) {
        const livePriceUSDT = State.universe[data.pairKey]?.priceUSDT || State.activePositions[data.pairKey]?.entryPriceUSDT;
        if (livePriceUSDT) settlePosition(data.pairKey, 'MANUAL EXIT', livePriceUSDT, 'Closed by user command');
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
    googleSheetUrl: State.googleSheetUrl
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

// Proxy endpoint to sync trades from Google Sheet without browser CORS blocks
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

// Diagnostic endpoint: Sends a test trade directly to verify sheet access
app.get('/api/settings/googlesheet/test', async (req, res) => {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl) {
    return res.status(400).json({ success: false, message: 'GOOGLE_SHEET_URL is not set.' });
  }

  const testPayload = {
    id: Date.now(),
    date: new Date().toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' }),
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }),
    pair: 'TEST/USDT',
    type: 'SYNC TEST',
    entryPriceUSDT: 100.00,
    exitPriceUSDT: 101.60,
    pnlUSDT: 0.80,
    pnlINR: Number((0.80 * State.usdtToInrRate).toFixed(2)),
    reason: 'Manual diagnostic test from Render'
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
    realizedPnL: State.portfolio.realizedPnL
  });
});

app.get('/healthz', (req, res) => res.status(200).send('OK'));

// -------------------------------------------------------------
// BOOTSTRAP TIMERS
// -------------------------------------------------------------
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Quant Server] Running on port ${PORT}`);

  setTimeout(runDynamicMarketScreener, 3000);
  setInterval(runDynamicMarketScreener, 3 * 60 * 1000);

  setInterval(runPureQuantScoring, 15 * 1000);
  setInterval(syncLiveBinancePrices, 2500);
});
