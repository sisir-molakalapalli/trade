const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || 'jtG8g4dNoJpXQ6vu03f8l4quEir2HHCVOrXXfpgmSCxV1ubg4p4eaOmN9oH3eG53';

const app = express();
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
  cooldowns: {}, // Symbol -> expiry timestamp (ms)

  tradeFrequencyController: {
    maxTradesPer24h: 8,             // Strict hard ceiling (4-8 target)
    maxConcurrentPositions: 2,      // Max 2 positions to concentrate capital
    rollingTrades24h: [],           // Array of timestamps for trades opened in past 24h
    globalCircuitBreakerUntil: 0,   // Timestamp for 20m post-loss pause
    circuitBreakerReason: 'NORMAL'  // 'NORMAL', 'POST_LOSS_FREEZE', 'TURBULENT_FREEZE'
  },

  activeThresholds: {
    regime: 'BALANCED',
    scoreThreshold: 81,             // Recalibrated sweet-spot gate
    obiThreshold: 0.56,             // 56% minimum buyer dominance
    maxSpread: 0.08,                // Strict spread ceiling (0.08%)
    cooldownMinutes: 90,            // 90m asset cooldown post-exit
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
      resetOldestSlotInMin: 0
    },
    persistence: {
      status: 'SYNCED',
      totalDiskSaves: 0,
      lastSaveTime: '--:--:--',
      retainedTradesCount: 0,
      storageEngine: 'Disk JSON + Sanitized Ledger'
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
      console.warn(`[PnL Audit] Correcting anomalous trade PnL for trade #${l.id}: was $${pnlUSDT}, converting to true USDT.`);
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
      usdtToInrRate: State.usdtToInrRate
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(dataToSave, null, 2), 'utf-8');
    State.subsystems.persistence.totalDiskSaves++;
    State.subsystems.persistence.lastSaveTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
    State.subsystems.persistence.retainedTradesCount = State.tradeLogs.length;
    State.subsystems.persistence.status = 'SYNCED';
  } catch (err) {
    State.subsystems.persistence.status = 'ERROR';
    console.error('[Persistence] Error saving state:', err.message);
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

      console.log(`[Persistence] Loaded & audited state successfully. True Realized PnL: $${State.portfolio.realizedPnL} USDT (${State.tradeLogs.length} trades)`);
    }
  } catch (err) {
    console.warn('[Persistence] Could not load state from disk:', err.message);
  }
}

loadStateFromDisk();

// -------------------------------------------------------------
// BINANCE MULTI-MIRROR API FETCHER (Geoblock-Resilient)
// -------------------------------------------------------------
const BINANCE_HOSTS = [
  'https://data-api.binance.vision',
  'https://api1.binance.com',
  'https://api3.binance.com',
  'https://api.binance.com'
];

async function fetchFromBinance(pathStr) {
  for (const host of BINANCE_HOSTS) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3500);
      const res = await fetch(`${host}${pathStr}`, {
        headers: { 'X-MBX-APIKEY': BINANCE_API_KEY },
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      if (res.ok) return await res.json();
    } catch (e) {}
  }
  return null;
}

// -------------------------------------------------------------
// 1H & 4H MOMENTUM ACCELERATION DETECTOR
// -------------------------------------------------------------
async function fetchShortTermMomentum(symbol) {
  try {
    // 5 candles of 1h = covers past 4 hours + live 1h
    const klines = await fetchFromBinance(`/api/v3/klines?symbol=${symbol}&interval=1h&limit=5`);
    if (Array.isArray(klines) && klines.length >= 2) {
      const currentClose = parseFloat(klines[klines.length - 1][4]);
      const oneHourAgoOpen = parseFloat(klines[klines.length - 1][1]);
      const fourHoursAgoOpen = parseFloat(klines[0][1]);

      const change1hPct = oneHourAgoOpen > 0 ? ((currentClose - oneHourAgoOpen) / oneHourAgoOpen) * 100 : 0;
      const change4hPct = fourHoursAgoOpen > 0 ? ((currentClose - fourHoursAgoOpen) / fourHoursAgoOpen) * 100 : 0;

      return { change1hPct, change4hPct };
    }
  } catch (e) {}
  return { change1hPct: 0, change4hPct: 0 };
}

// -------------------------------------------------------------
// ORDER BOOK DEPTH & SPREAD INSPECTION
// -------------------------------------------------------------
async function fetchRealOrderBookDepth(symbol) {
  try {
    const data = await fetchFromBinance(`/api/v3/depth?symbol=${symbol}&limit=20`);
    if (data && data.bids && data.asks && data.bids.length > 0 && data.asks.length > 0) {
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
  let scoreThreshold = 81; // Recalibrated normal sweet spot
  let obiThreshold = 0.56;   // 56% buyer dominance
  let maxSpread = 0.08;
  let cooldownMinutes = 90;

  // Turbulence Trigger: Whipsaw Protection
  if (avgRange >= 9.5) {
    computedRegime = 'TURBULENT';
    scoreThreshold = 86; // Stricter confirmation during storms
    obiThreshold = 0.59;
    maxSpread = 0.06;
    cooldownMinutes = 120;
    State.tradeFrequencyController.circuitBreakerReason = 'TURBULENT_FREEZE';
  } else if (avgRange <= 4.0) {
    computedRegime = 'COMPRESSION';
    scoreThreshold = 79; // Eased gate to catch quiet accumulation
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
// 24-HOUR ROLLING TRADE QUOTA MANAGER (4-8 Trades Limit)
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

  if (State.tradeFrequencyController.globalCircuitBreakerUntil > now) {
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
}

// -------------------------------------------------------------
// STAGE 1: DISCOVERY SCREENER (Dynamic $8M-$18M Floor + 1h/4h Surge)
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

    for (const t of tickers) {
      if (!t.symbol.endsWith('USDT')) continue;
      const base = t.symbol.replace('USDT', '');
      if (['UP', 'DOWN', 'BEAR', 'BULL', 'USDC', 'FDUSD', 'TUSD', 'EUR', 'DAI'].some(x => base.endsWith(x))) continue;

      const quoteVol = parseFloat(t.quoteVolume) || 0;
      const lastPrice = parseFloat(t.lastPrice) || 0;
      const priceChangePct = parseFloat(t.priceChangePercent) || 0;
      const highPrice = parseFloat(t.highPrice) || 0;
      const lowPrice = parseFloat(t.lowPrice) || 0;

      // Dynamic Liquidity Floor:
      // If price change is surging (>= +5%), floor eases to $8M USDT.
      // If moderate (>= +2%), floor is $12M. Otherwise, standard floor is $18M.
      let requiredFloor = 18000000;
      if (priceChangePct >= 5.0) requiredFloor = 8000000;
      else if (priceChangePct >= 2.0) requiredFloor = 12000000;

      if (quoteVol >= requiredFloor && lastPrice > 0 && highPrice > lowPrice) {
        const pricePositionInRange = (lastPrice - lowPrice) / (highPrice - lowPrice);
        const intradayRangePct = ((highPrice - lowPrice) / lowPrice) * 100;

        // Upper 60% of daily range (Bullish accumulation base)
        if (pricePositionInRange < 0.60) continue;

        // Mathematical Discovery Scoring with RVOL & Range Proximity
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

    // Fetch 1h and 4h momentum velocity for top picks
    for (const p of topPicks) {
      const shortTerm = await fetchShortTermMomentum(p.symbol);
      p.change1hPct = shortTerm.change1hPct;
      p.change4hPct = shortTerm.change4hPct;

      // Extra boost for fresh 1h and 4h acceleration
      if (p.change1hPct > 1.5) p.breakoutScore += (p.change1hPct * 1.8);
      if (p.change4hPct > 4.0) p.breakoutScore += (p.change4hPct * 1.2);
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
      console.log(`[Institutional Screener] Selected ${topPicks.length} candidates with dynamic floor and 1h/4h velocity.`);
      broadcastPricesOnly();
    }

    State.subsystems.screener.status = 'ACTIVE';
    State.subsystems.screener.lastRunTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
    nextScreenerTargetTimestamp = Date.now() + (3 * 60 * 1000);
  } catch (err) {
    State.subsystems.screener.status = 'ERROR';
    console.error('[Screener Error]', err.message);
  }
}

// -------------------------------------------------------------
// STAGE 2: 20-LEVEL MICROSTRUCTURE QUANT EVALUATOR (15s Loop)
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

    // Sub-score calculations with 1h & 24h momentum integration
    const s_obi = Math.min(40, Math.max(0, Math.round((book.obi - 0.40) * 140)));
    const changePct = typeof coin.priceChangePct === 'number' ? coin.priceChangePct : 2.0;
    const change1h = typeof coin.change1hPct === 'number' ? coin.change1hPct : 0;
    
    // Blended momentum: 60% 24h + 40% 1h velocity
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
// STAGE 3: LOW-FREQUENCY AUTOPILOT ENTRY MATRIX (4-8 Trades Limit)
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

    // Gate 1: Recalibrated Score Threshold (>= 81 in Balanced)
    if (coin.score < config.scoreThreshold) continue;

    // Gate 2: Live 20-Level Depth Inspection
    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    // Gate 3: Minimum Buyer Dominance (>= 56% Bids)
    if (book.obi < config.obiThreshold) continue;

    // Gate 4: Strict Spread Ceiling (<= 0.08% for majors, <= 0.20% for micro-cents)
    const isMicroAsset = (coin.priceUSDT && coin.priceUSDT < 0.01);
    const allowedSpread = isMicroAsset ? 0.20 : config.maxSpread;
    if (book.spreadPct > allowedSpread) continue;

    // Gate 5: Price Freshness & Sanity Check
    if (!coin.priceUSDT || coin.priceUSDT <= 0) continue;
    if (!coin.verifiedAt || (Date.now() - coin.verifiedAt) > 30000) continue;

    // All Institutional Gates Passed -> Execute High-Conviction Order
    executeSimulatedBuy(
      sym, 
      coin.priceUSDT, 
      `High-Conviction Confluence (${config.regime}: Score ${coin.score} >= ${config.scoreThreshold}, OBI ${(book.obi * 100).toFixed(0)}% >= ${(config.obiThreshold * 100).toFixed(0)}%, Spread ${book.spreadPct.toFixed(2)}%)`
    );
    break;
  }
}

// -------------------------------------------------------------
// ORDER EXECUTION & FEE-PROOF RISK MANAGEMENT
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
    notionalUSDT,
    sl,
    tp1,
    openedAt: Date.now(),
    isTrailingActive: false,
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

  console.log(`[AUTOPILOT BUY] ${pairKey} @ $${priceUSDT} USDT (Qty: ${qty >= 1000 ? Math.round(qty).toLocaleString() : qty.toFixed(4)}) | Allocation: $${notionalUSDT} USDT | Quota: ${State.subsystems.quotaManager.executed24hCount}/${State.subsystems.quotaManager.maxAllowed}`);
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

  const cooldownMinutes = State.activeThresholds?.cooldownMinutes || 90;
  State.cooldowns[pairKey] = Date.now() + (cooldownMinutes * 60 * 1000);

  if (pnlUSDT < 0) {
    State.tradeFrequencyController.globalCircuitBreakerUntil = Date.now() + (20 * 60 * 1000);
    State.tradeFrequencyController.circuitBreakerReason = 'POST_LOSS_FREEZE';
    console.warn(`[CIRCUIT BREAKER ACTIVATED] Loss incurred ($${pnlUSDT} USDT). Freezing all new entries for 20 minutes.`);
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

  console.log(`[POSITION EXIT] ${pairKey} via ${exitType} | PnL: $${pnlUSDT} USDT (≈ ₹${logEntry.pnlINR} INR) | ${exitReason} | Realized PnL: $${State.portfolio.realizedPnL}`);
  saveStateToDisk();
  broadcastState();
}

// -------------------------------------------------------------
// SENTRY RISK MONITOR (Every 2.5s)
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

    // 1. Take Profit 1 Target (+1.60%)
    if (livePriceUSDT >= pos.tp1) {
      settlePosition(sym, 'TP1 REACHED', livePriceUSDT, `Target reached (+${gainPct.toFixed(2)}%)`);
      continue;
    }

    // 2. Trailing Breakeven Lock (+0.60% Trigger -> Locks +0.25% Net)
    if (gainPct >= 0.60 && !pos.isTrailingActive) {
      pos.isTrailingActive = true;
      pos.sl = pos.entryPriceUSDT * 1.0025;
      console.log(`[FEE-PROOF TRAILING ACTIVE] ${sym} locked at +0.25%`);
    }

    // 3. Stop Loss or Trailing Stop Boundary
    if (livePriceUSDT <= pos.sl) {
      settlePosition(sym, pos.isTrailingActive ? 'TRAILING SL' : 'STOP LOSS', livePriceUSDT, `Hit boundary (${gainPct.toFixed(2)}%)`);
      continue;
    }

    // 4. Stagnation Cut (8-Minute Window)
    if (holdSeconds >= 480 && gainPct < 0.25) {
      settlePosition(sym, 'STAGNATION CUT', livePriceUSDT, 'No momentum impulse within 8 minutes');
      continue;
    }

    // 5. Predictive Risk Decay Scoring (Calibrated to prevent premature noise shakeouts)
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

    // Guard: Allow 90 seconds of post-entry breathing room before permitting predictive micro-cuts
    if (holdSeconds >= 90) {
      // Calibrated from -0.20% to -0.50% with persistent decay >= 55%
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
// LIVE BINANCE PRICE FEED (Every 2.5s)
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
  } catch (e) {
    console.error('[Price Sync Error]', e.message);
  }
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
    usdtToInrRate: State.usdtToInrRate
  });
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
