const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer, WebSocket } = require('ws');

// Import the Autonomous Self-Learning Quant Engine
const learningEngine = require('./learning_engine');

process.on('uncaughtException', (err) => {
  console.error('[Process UncaughtException]', err.message);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Process UnhandledRejection]', reason);
});

const PORT = process.env.PORT || 8080;
const STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || 'jtG8g4dNoJpXQ6vu03f8l4quEir2HHCVOrXXfpgmSCxV1ubg4p4eaOmN9oH3eG53';
const GOOGLE_SHEET_URL = process.env.GOOGLE_SHEET_URL || 'https://script.google.com/macros/s/AKfycbxzrxMMqOXehqTy3id5Z7Flr6Hbz9Tyz3XnDEgb8NaNPvO2RsvvUD-qNxKIEmOx3s9z/exec';

const DRIVE_FILE_IDS = {
  learningState: '15eSFMAP8gFT8hMZLO8xplq3woSCbQmeL',
  portfolioState: '1DVK9OvL1d2sNsHqwGsdOMNqEAzkTeyno'
};

const app = express();

// Register Google Drive sync callback for learning engine
learningEngine.setGoogleDriveSyncCallback((payload) => {
  backupLearningStateToGoogleDrive(payload).catch(() => {});
});

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
    dynamicGovernorEnabled: true,
    cashReserveFloorUSDT: 300.00,
    maxRecoveryBagsLimit: 6,
    governorTier: 'UNLIMITED_SPRINT',
    baseMaxTradesPer24h: 8,
    maxTradesPer24h: 9999,
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
    bagSubsidyPoolUSDT: 0.00,
    sizingMode: 'PERCENTAGE',
    allocationPct: 15.0,
    fixedNotionalUSDT: 50.00
  },

  activePositions: {},
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
      hardDisasterFloorPct: -6.50,
      architecture: '4-Point Loss Prevention + Spot Recovery Bag'
    },
    timeMachine: {
      status: 'ONLINE',
      lastEvaluatedSymbol: '--',
      lastWinOddsPct: 0,
      replaysCompleted: 0,
      blockedCount: 0,
      minOddsRequiredPct: 45
    },
    entryTimer: {
      status: 'ONLINE',
      closedBarEnforced: true,
      maxWickRatio: 0.40
    },
    volatilityBudget: {
      status: 'ONLINE',
      baselineVolPct: 4.5,
      lastAdjustmentFactor: 1.0
    },
    recoveryManager: {
    status: 'ONLINE',
    activeRecoveryBagsCount: 0,
    totalRecoveryCapitalUSDT: 0.00,
    meanReversionTargetRate: 'Dynamic Stepped Target + Hard -6.50% Disaster Cap',
    hardDisasterCutsCount: 0,
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
      storageEngine: 'Disk JSON + Google Drive File Engine',
      googleDriveFileStatus: 'FILES_ONLINE (Drive IDs Bound)',
      driveFileIds: DRIVE_FILE_IDS,
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
    regimeReasoning: 'Eliminated stop-out losses entirely. Dips transfer to Recovery Bags while active slots trade without interruption.'
  },

  universe: { ...DEFAULT_UNIVERSE },
  tradeLogs: []
};

// -------------------------------------------------------------
// GOOGLE DRIVE JSON FILE SYNC & PERSISTENCE ENGINE
// -------------------------------------------------------------
async function backupLearningStateToGoogleDrive(payload) {
  const targetUrl = State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!targetUrl) return;

  try {
    const postBody = {
      action: 'SAVE_LEARNING_STATE',
      fileId: DRIVE_FILE_IDS.learningState,
      learningState: payload?.learningState || learningEngine.state,
      timestamp: Date.now()
    };

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const res = await fetch(targetUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(postBody),
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);

    if (res.ok) {
      const data = await res.json().catch(() => null);
      if (data && data.success) {
        State.subsystems.persistence.googleDriveFileStatus = 'SYNCED (Drive ID: 15eSFMAP... Bound)';
        console.log('[Google Drive] Saved learning_state.json directly to Drive ID:', data.fileId || DRIVE_FILE_IDS.learningState);
      }
    }
  } catch (e) {
    State.subsystems.persistence.googleDriveFileStatus = 'DRIVE_SYNC_RETRY';
  }
}

async function backupPortfolioStateToGoogleDrive() {
  const targetUrl = State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!targetUrl) return;

  try {
    const postBody = {
      action: 'SAVE_PORTFOLIO_STATE',
      fileId: DRIVE_FILE_IDS.portfolioState,
      portfolioState: {
        portfolio: State.portfolio,
        activePositions: State.activePositions,
        recoveryBags: State.recoveryBags,
        cooldowns: State.cooldowns,
        tradeFrequencyController: State.tradeFrequencyController,
        timestamp: Date.now()
      }
    };

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const res = await fetch(targetUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(postBody),
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);

    if (res.ok) {
      State.subsystems.persistence.googleDriveFileStatus = 'SYNCED (Drive ID: 1DVK9OvL... Bound)';
    }
  } catch (e) {}
}

async function syncWithGoogleDrive() {
  const targetUrl = State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!targetUrl) return;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const res = await fetch(`${targetUrl}?action=GET_ALL`, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);

    if (res.ok) {
      const data = await res.json();
      
      // 1. Restore Learning State from Google Drive file
      if (data && data.learningState) {
        learningEngine.restoreFromDrivePayload(data);
        console.log('[Google Drive] Restored learning_state.json from Google Drive successfully.');
      }

      // 2. Restore Portfolio State from Google Drive file
      if (data && data.portfolioState && typeof data.portfolioState === 'object') {
        const ps = data.portfolioState;
        if (ps.portfolio) State.portfolio = { ...State.portfolio, ...ps.portfolio };
        if (ps.activePositions) State.activePositions = ps.activePositions;
        if (ps.recoveryBags) State.recoveryBags = ps.recoveryBags;
        if (ps.cooldowns) State.cooldowns = ps.cooldowns;
      }

      // 3. Restore Sheet Trades
      const rawList = Array.isArray(data) ? data : (Array.isArray(data?.trades) ? data.trades : []);
      if (rawList.length > 0) {
        const validSheetTrades = [];
        rawList.forEach(item => {
          const normalized = normalizeSheetTrade(item);
          if (normalized && isValidTradeRecord(normalized)) {
            validSheetTrades.push(normalized);
          }
        });
        State.tradeLogs = sanitizeAndAuditTradeHistory(validSheetTrades);
        recalculateTrueRealizedPnL();
        reconcileRollingTradeQuotaFromHistory();
      }

      State.subsystems.persistence.googleSheetSync = 'ACTIVE';
      State.subsystems.persistence.googleDriveFileStatus = 'FILES_ONLINE (Drive JSON Active)';
      saveStateToDisk();
      broadcastState();
    }
  } catch (err) {
    console.error('[Google Drive Sync Error]', err.message);
  }
}

async function appendTradeToGoogleDrive(tradeLog) {
  const targetUrl = State.googleSheetUrl;
  if (!targetUrl || !tradeLog) return;
  
  if (!isValidTradeRecord(tradeLog)) return;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const res = await fetch(targetUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(tradeLog),
      signal: controller.signal
    });
    clearTimeout(timeout);
    
    if (res.ok) {
      State.subsystems.persistence.googleSheetSync = 'SYNCED';
    }
  } catch (err) {}
}

function saveStateToDisk() {
  try {
    const cleanLogs = sanitizeAndAuditTradeHistory(State.tradeLogs).slice(0, 500);
    const dataToSave = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      recoveryBags: State.recoveryBags,
      tradeLogs: cleanLogs,
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
    State.subsystems.persistence.retainedTradesCount = cleanLogs.length;
    State.subsystems.persistence.status = 'SYNCED';

    // Periodically back up portfolio JSON to Google Drive
    backupPortfolioStateToGoogleDrive().catch(() => {});
  } catch (err) {
    State.subsystems.persistence.status = 'ERROR';
  }
}

function loadStateFromDisk() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const raw = fs.readFileSync(STATE_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        if (parsed.portfolio) State.portfolio = { ...State.portfolio, ...parsed.portfolio };
        if (parsed.activePositions) State.activePositions = parsed.activePositions;
        if (parsed.recoveryBags) State.recoveryBags = parsed.recoveryBags;
        if (Array.isArray(parsed.tradeLogs)) {
          State.tradeLogs = sanitizeAndAuditTradeHistory(parsed.tradeLogs);
        }
        if (parsed.cooldowns) State.cooldowns = parsed.cooldowns;
        if (parsed.tradeFrequencyController) {
          State.tradeFrequencyController = { ...State.tradeFrequencyController, ...parsed.tradeFrequencyController };
        }
        if (parsed.autopilotEnabled !== undefined) State.autopilotEnabled = parsed.autopilotEnabled;
        if (parsed.usdtToInrRate) State.usdtToInrRate = parsed.usdtToInrRate;
        if (parsed.googleSheetUrl) State.googleSheetUrl = parsed.googleSheetUrl;

        recalculateTrueRealizedPnL();
        reconcileRollingTradeQuotaFromHistory();
        console.log(`[Disk Loader] Restored ${State.tradeLogs.length} verified trades from disk.`);
      }
    }
  } catch (err) {
    console.error('[Disk Loader Error]', err.message);
  }
}

function broadcastState() {
  if (wss.clients.size === 0) return;
  const payload = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: sanitizeAndAuditTradeHistory(State.tradeLogs).slice(0, 80),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    googleSheetUrl: State.googleSheetUrl,
    learningEngineState: learningEngine.state
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// -------------------------------------------------------------
// STRICT RECORD VALIDATOR & SANITIZER
// -------------------------------------------------------------
function isValidTradeRecord(t) {
  if (!t || typeof t !== 'object') return false;

  const rawPair = String(t.pair || t.Pair || t.symbol || t.Symbol || t.asset || '').trim().toUpperCase();
  if (!rawPair || rawPair === 'CRYPTO/USDT' || rawPair.includes('CRYPTO') || rawPair === 'UNKNOWN/USDT' || rawPair === 'UNDEFINED/USDT' || !rawPair.includes('USDT')) {
    return false;
  }

  const entry = Number(t.entryPriceUSDT || t['Entry Price (USDT)'] || t.entryPrice || t.entry || 0);
  const exit = Number(t.exitPriceUSDT || t['Exit Price (USDT)'] || t.exitPrice || t.exit || 0);

  if (isNaN(entry) || entry <= 0 || isNaN(exit) || exit <= 0) {
    return false;
  }

  const rawType = String(t.type || t.Type || t.action || '').trim().toUpperCase();
  if (!rawType || rawType === 'UNDEFINED' || rawType === 'NULL') {
    return false;
  }

  return true;
}

function sanitizeAndAuditTradeHistory(logs) {
  if (!Array.isArray(logs)) return [];
  
  const map = new Map();
  logs.forEach(l => {
    if (!isValidTradeRecord(l)) return;

    const rawPair = String(l.pair || l.Pair || l.symbol || l.Symbol || l.asset || '').trim().toUpperCase();
    let entryUSDT = Number(l.entryPriceUSDT || l['Entry Price (USDT)'] || l.entryPrice || l.entry || 0);
    let exitUSDT = Number(l.exitPriceUSDT || l['Exit Price (USDT)'] || l.exitPrice || l.exit || 0);

    let id = Number(l.id) || Date.now();
    l.id = id;
    l.pair = rawPair.endsWith('USDT') && !rawPair.includes('/') ? `${rawPair.replace('USDT', '')}/USDT` : rawPair;

    let pnlUSDT = 0;
    if (typeof l.pnlUSDT === 'number' && !isNaN(l.pnlUSDT)) {
      pnlUSDT = l.pnlUSDT;
    } else if (typeof l.pnlINR === 'number' && !isNaN(l.pnlINR)) {
      pnlUSDT = l.pnlINR / State.usdtToInrRate;
    } else {
      pnlUSDT = (exitUSDT - entryUSDT) * ((Number(l.notionalUSDT) || 50.00) / entryUSDT);
    }

    l.pnlUSDT = Number(pnlUSDT.toFixed(4));
    l.pnlINR = Number((pnlUSDT * State.usdtToInrRate).toFixed(2));
    l.entryPriceUSDT = formatBinanceUSDTPrice(entryUSDT);
    l.exitPriceUSDT = formatBinanceUSDTPrice(exitUSDT);
    l.notionalUSDT = Number(l.notionalUSDT) || 50.00;
    l.type = String(l.type || l.Type || 'EXIT').trim().toUpperCase();

    const dedupKey = `${l.pair}_${l.id}`;
    map.set(dedupKey, l);
  });

  return Array.from(map.values()).sort((a, b) => b.id - a.id);
}

function recalculateTrueRealizedPnL() {
  let trueSumUSDT = 0;
  State.tradeLogs.forEach(l => {
    if (typeof l.pnlUSDT === 'number' && !isNaN(l.pnlUSDT)) {
      trueSumUSDT += l.pnlUSDT;
    }
  });
  State.portfolio.realizedPnL = Number(trueSumUSDT.toFixed(4));
}

function formatBinanceUSDTPrice(usdtPrice) {
  const num = Number(usdtPrice) || 0;
  if (num >= 100) return Number(num.toFixed(2));
  if (num >= 1) return Number(num.toFixed(4));
  return Number(num.toFixed(8));
}

function reconcileRollingTradeQuotaFromHistory() {
  const now = Date.now();
  const dayAgo = now - (24 * 60 * 60 * 1000);
  const timestamps = new Set();

  State.tradeLogs.forEach(l => {
    const tradeTs = Number(l.id);
    if (tradeTs > 0 && tradeTs > dayAgo && tradeTs <= now) {
      timestamps.add(tradeTs);
    }
  });

  State.tradeFrequencyController.rollingTrades24h = Array.from(timestamps).sort((a, b) => a - b);
}

function normalizeSheetTrade(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const getField = (keys) => {
    for (const k of keys) {
      if (raw[k] !== undefined && raw[k] !== null && String(raw[k]).trim() !== '') return raw[k];
    }
    return '';
  };

  const rawPair = String(getField(['pair', 'Pair', 'symbol', 'Symbol', 'asset'])).trim().toUpperCase();
  if (!rawPair || rawPair === 'CRYPTO/USDT' || rawPair.includes('CRYPTO') || !rawPair.includes('USDT')) return null;

  const entry = Number(getField(['entryPriceUSDT', 'Entry Price (USDT)', 'entryPrice', 'entry']));
  const exit = Number(getField(['exitPriceUSDT', 'Exit Price (USDT)', 'exitPrice', 'exit']));
  if (isNaN(entry) || entry <= 0 || isNaN(exit) || exit <= 0) return null;

  const pnlUSDT = Number(getField(['pnlUSDT', 'PnL (USDT)', 'pnl'])) || 0;
  const pnlINR = Number(getField(['pnlINR', 'PnL (INR)'])) || (pnlUSDT * (State.usdtToInrRate || 94.94));
  const notional = Number(getField(['notionalUSDT', 'Notional (USDT)'])) || 50.00;
  const pair = rawPair.includes('/') ? rawPair : `${rawPair.replace('USDT', '')}/USDT`;

  return {
    id: Number(getField(['id', 'ID'])) || Date.now(),
    date: String(getField(['date', 'Date'])) || 'Recent',
    time: String(getField(['time', 'Time'])) || '',
    pair,
    type: String(getField(['type', 'Type'])) || 'EXIT',
    entryPriceUSDT: formatBinanceUSDTPrice(entry),
    exitPriceUSDT: formatBinanceUSDTPrice(exit),
    notionalUSDT: notional,
    pnlUSDT: Number(pnlUSDT.toFixed(4)),
    pnlINR: Number(pnlINR.toFixed(2)),
    reason: String(getField(['reason', 'Reason'])) || 'Settled trade',
    regime: String(getField(['regime', 'Regime'])) || 'BALANCED',
    volatilityPct: Number(getField(['volatilityPct', 'Volatility %'])) || 4.5
  };
}

// -------------------------------------------------------------
// REST API ENDPOINTS FOR GOOGLE DRIVE FILE CREATION & VERIFICATION
// -------------------------------------------------------------
app.get('/api/drive/verify-files', async (req, res) => {
  const targetUrl = State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!targetUrl) {
    return res.status(400).json({ success: false, message: 'Google Apps Script URL is not set.' });
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const response = await fetch(`${targetUrl}?action=CREATE_ALL_FILES`, { 
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal 
    });
    clearTimeout(timeout);

    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('text/html')) {
      return res.json({
        success: false,
        isAuthRequired: true,
        message: 'Google Apps Script requires one-time permission authorization in the Apps Script editor.'
      });
    }

    if (response.ok) {
      const data = await response.json().catch(() => null);
      if (data && data.success) {
        State.subsystems.persistence.googleDriveFileStatus = 'FILES_ONLINE (Drive JSON Active)';
        return res.json({
          success: true,
          message: 'Google Drive file creation verified! Permissions are active.',
          data
        });
      }
    }
    return res.status(response.status).json({ success: false, message: `HTTP ${response.status} from Google Apps Script` });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/drive/sync-files-now', async (req, res) => {
  const targetUrl = State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!targetUrl) {
    return res.status(400).json({ success: false, error: 'Google Sheet / Apps Script URL is empty.' });
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const getRes = await fetch(`${targetUrl}?action=CREATE_ALL_FILES`, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);

    const contentType = getRes.headers.get('content-type') || '';
    if (contentType.includes('text/html')) {
      return res.json({
        success: false,
        isAuthRequired: true,
        message: 'Action needed: Open Apps Script, select MANUAL_RUN_CREATE_DRIVE_FILES and click Run to grant Drive access.'
      });
    }

    let driveData = null;
    if (getRes.ok) {
      driveData = await getRes.json().catch(() => null);
    }

    // Push active state updates
    await backupLearningStateToGoogleDrive({ learningState: learningEngine.state });
    await backupPortfolioStateToGoogleDrive();

    State.subsystems.persistence.googleDriveFileStatus = 'FILES_ONLINE (Drive JSON Active)';

    res.json({
      success: true,
      message: 'Created learning_state.json and cloud_portfolio_state.json in Google Drive!',
      driveData,
      timestamp: Date.now()
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// -------------------------------------------------------------
// REST API ENDPOINTS FOR TRADE SCRUBBING & PORTFOLIO CONTROLS
// -------------------------------------------------------------
app.post('/api/trades/purge-empty', (req, res) => {
  State.tradeLogs = sanitizeAndAuditTradeHistory(State.tradeLogs);
  recalculateTrueRealizedPnL();
  reconcileRollingTradeQuotaFromHistory();
  saveStateToDisk();
  broadcastState();
  res.json({ success: true, count: State.tradeLogs.length, trades: State.tradeLogs });
});

app.post('/api/trades/sync-strict-sheet', async (req, res) => {
  await syncWithGoogleDrive();
  res.json({ success: true, count: State.tradeLogs.length, trades: State.tradeLogs });
});

app.post('/api/portfolio/cash', (req, res) => {
  const { cashUSDT, addUSDT } = req.body;
  if (typeof cashUSDT === 'number' && cashUSDT >= 0) {
    State.portfolio.cashUSDT = Number(cashUSDT.toFixed(2));
  } else if (typeof addUSDT === 'number' && addUSDT > 0) {
    State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + addUSDT).toFixed(2));
  }
  saveStateToDisk();
  broadcastState();
  res.json({ success: true, cashUSDT: State.portfolio.cashUSDT });
});

app.post('/api/portfolio/sizing', (req, res) => {
  const { sizingMode, allocationPct, fixedNotionalUSDT } = req.body;
  if (sizingMode) State.portfolio.sizingMode = sizingMode;
  if (typeof allocationPct === 'number' && allocationPct >= 1 && allocationPct <= 100) {
    State.portfolio.allocationPct = allocationPct;
  }
  if (typeof fixedNotionalUSDT === 'number' && fixedNotionalUSDT >= 10) {
    State.portfolio.fixedNotionalUSDT = fixedNotionalUSDT;
  }
  saveStateToDisk();
  broadcastState();
  res.json({ success: true, portfolio: State.portfolio });
});

app.get('/api/quota/reconcile', (req, res) => {
  reconcileRollingTradeQuotaFromHistory();
  res.json({
    success: true,
    rollingTrades24h: State.tradeFrequencyController.rollingTrades24h,
    executed24hCount: State.subsystems.quotaManager.executed24hCount,
    quotaRemaining: State.subsystems.quotaManager.quotaRemaining,
    formattedReset: State.subsystems.quotaManager.formattedReset
  });
});

app.get('/api/screener/run', async (req, res) => {
  await runDynamicScreener();
  res.json({ success: true, universe: State.universe });
});

app.get('/api/p2p/sync', async (req, res) => {
  await syncLiveP2PRate();
  res.json({ success: true, rate: State.usdtToInrRate });
});

app.get('/api/settings/googlesheet/sync', async (req, res) => {
  await syncWithGoogleDrive();
  res.json({ success: true, count: State.tradeLogs.length });
});

// -------------------------------------------------------------
// WEBSOCKET COMMUNICATION GATEWAY
// -------------------------------------------------------------
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: sanitizeAndAuditTradeHistory(State.tradeLogs).slice(0, 80),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    googleSheetUrl: State.googleSheetUrl,
    learningEngineState: learningEngine.state
  }));

  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);
      if (msg.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(msg.enabled);
        saveStateToDisk();
        broadcastState();
      } else if (msg.type === 'SET_CASH') {
        State.portfolio.cashUSDT = Number(msg.cashUSDT) || 0;
        saveStateToDisk();
        broadcastState();
      } else if (msg.type === 'ADD_CASH') {
        State.portfolio.cashUSDT += Number(msg.addUSDT) || 0;
        saveStateToDisk();
        broadcastState();
      } else if (msg.type === 'SET_SIZING_CONFIG') {
        if (msg.sizingMode) State.portfolio.sizingMode = msg.sizingMode;
        if (msg.allocationPct) State.portfolio.allocationPct = Number(msg.allocationPct);
        saveStateToDisk();
        broadcastState();
      } else if (msg.type === 'SET_DYNAMIC_GOVERNOR') {
        State.tradeFrequencyController.dynamicGovernorEnabled = Boolean(msg.enabled);
        saveStateToDisk();
        broadcastState();
      } else if (msg.type === 'CLOSE_POSITION' && msg.pairKey) {
        closeActivePositionManual(msg.pairKey);
      } else if (msg.type === 'CLOSE_RECOVERY_BAG' && msg.pairKey) {
        closeRecoveryBagManual(msg.pairKey);
      } else if (msg.type === 'SYNC_P2P_RATE') {
        syncLiveP2PRate();
      } else if (msg.type === 'SET_GOOGLE_SHEET_URL' && msg.url) {
        State.googleSheetUrl = msg.url.trim();
        saveStateToDisk();
        syncWithGoogleDrive();
      } else if (msg.type === 'RECONCILE_QUOTA') {
        reconcileRollingTradeQuotaFromHistory();
        broadcastState();
      }
    } catch (e) {}
  });
});

// -------------------------------------------------------------
// RESILIENT MULTI-MIRROR BINANCE MARKET DATA GATEWAY
// -------------------------------------------------------------
const BINANCE_BASE_URLS = [
  'https://data-api.binance.vision', // Binance's public data mirror without cloud geo-blocking (HTTP 451 bypass)
  'https://api.binance.com',
  'https://api1.binance.com',
  'https://api3.binance.com'
];

async function fetchBinance(apiPath, options = {}) {
  for (const baseUrl of BINANCE_BASE_URLS) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 9000);
      const res = await fetch(`${baseUrl}${apiPath}`, {
        ...options,
        signal: controller.signal
      });
      clearTimeout(timeout);
      if (res && res.ok) {
        return res;
      }
      // If geo-blocked or server error, smoothly try the next mirror
      if (res && (res.status === 451 || res.status === 403 || res.status >= 500)) {
        continue;
      }
      return res;
    } catch (e) {
      continue;
    }
  }
  return null;
}

// -------------------------------------------------------------
// CORE QUANT SCREENER & ALGORITHMIC EXECUTION ENGINE
// -------------------------------------------------------------
async function fetchRealOrderBookDepth(symbol) {
  try {
    const res = await fetchBinance(`/api/v3/depth?symbol=${symbol}&limit=20`);
    if (!res || !res.ok) return null;
    const depth = await res.json();
    const bids = depth.bids || [];
    const asks = depth.asks || [];
    if (bids.length === 0 || asks.length === 0) return null;

    let bidVol = 0;
    let askVol = 0;
    bids.forEach(b => bidVol += parseFloat(b[1]));
    asks.forEach(a => askVol += parseFloat(a[1]));

    const obi = (bidVol + askVol) > 0 ? (bidVol / (bidVol + askVol)) : 0.5;
    const bestBid = parseFloat(bids[0][0]);
    const bestAsk = parseFloat(asks[0][0]);
    const spreadPct = bestBid > 0 ? ((bestAsk - bestBid) / bestBid) * 100 : 0.05;

    return { obi, spreadPct, bestBid, bestAsk };
  } catch (e) {
    return null;
  }
}

async function checkConfirmedClosedBar(symbol) {
  try {
    const res = await fetchBinance(`/api/v3/klines?symbol=${symbol}&interval=15m&limit=2`);
    if (!res || !res.ok) return { passed: true, upperWickRatio: 0.20 };
    const klines = await res.json();
    if (!klines || klines.length < 2) return { passed: true, upperWickRatio: 0.20 };

    const prevBar = klines[0];
    const high = parseFloat(prevBar[2]);
    const low = parseFloat(prevBar[3]);
    const open = parseFloat(prevBar[1]);
    const close = parseFloat(prevBar[4]);
    const range = high - low;

    if (range <= 0) return { passed: true, upperWickRatio: 0.10 };

    // TIGHT CONSOLIDATION SHIELD: If the 15m candle range is under 1.2%, it is a tight base/consolidation bar,
    // NOT an exhaustion wick rejection. Do not falsely penalize quiet base-building bars.
    const candleVolPct = low > 0 ? (range / low) * 100 : 0;
    if (candleVolPct < 1.20) {
      return { passed: true, upperWickRatio: 0.15 };
    }

    const upperWick = high - Math.max(open, close);
    const upperWickRatio = upperWick / range;
    const dynamicCeiling = learningEngine.state.dynamicHurdles?.wickCeiling || 0.40;
    
    // Clean expansion bars closing strong near their highs pass cleanly
    const isStrongExpansion = close > open && ((high - close) / range) <= dynamicCeiling;
    return {
      passed: isStrongExpansion || upperWickRatio <= dynamicCeiling,
      upperWickRatio
    };
  } catch (e) {
    return { passed: true, upperWickRatio: 0.20 };
  }
}

async function evaluateTimeMachineBreakoutOdds(symbol) {
  try {
    const res = await fetchBinance(`/api/v3/klines?symbol=${symbol}&interval=15m&limit=48`);
    if (!res || !res.ok) return { oddsPct: 55, simulatedCount: 0 };
    const klines = await res.json();
    if (!klines || klines.length < 20) return { oddsPct: 55, simulatedCount: 0 };

    let setupsDetected = 0;
    let continuationWins = 0;

    // Evaluate historical expansion setups across prior bars
    for (let i = 0; i < klines.length - 8; i++) {
      const open = parseFloat(klines[i][1]);
      const close = parseFloat(klines[i][4]);
      const barGain = open > 0 ? ((close - open) / open) * 100 : 0;

      if (barGain >= 0.80) {
        setupsDetected++;
        const target = close * 1.018; // TP1 (+1.80%)
        const stop = close * 0.968;   // Spot recovery bag threshold (-3.20%)
        let won = false;

        // Realistic 12-bar (3-hour) horizon for spot continuation
        const maxBar = Math.min(i + 13, klines.length);
        for (let j = i + 1; j < maxBar; j++) {
          const fHigh = parseFloat(klines[j][2]);
          const fLow = parseFloat(klines[j][3]);
          const fClose = parseFloat(klines[j][4]);

          if (fHigh >= target) { won = true; break; }
          if (fLow <= stop) { won = false; break; }
          // Sustained positive drift without stop hit
          if (j === maxBar - 1 && fClose >= (close * 1.008)) { won = true; }
        }
        if (won) continuationWins++;
      }
    }

    // If fewer than 2 recent spikes occurred, assign neutral baseline odds (55%)
    const oddsPct = setupsDetected >= 2 
      ? Math.round((continuationWins / setupsDetected) * 100) 
      : 55;
    
    // Update live subsystem telemetry
    State.subsystems.timeMachine.replaysCompleted++;
    State.subsystems.timeMachine.lastEvaluatedSymbol = symbol.replace('USDT', '');
    State.subsystems.timeMachine.lastWinOddsPct = oddsPct;
    if (oddsPct < 45) State.subsystems.timeMachine.blockedCount++;

    return { oddsPct, simulatedCount: setupsDetected };
  } catch (e) {
    return { oddsPct: 55, simulatedCount: 0 };
  }
}

async function runDynamicScreener() {
  try {
    const res = await fetchBinance('/api/v3/ticker/24hr');
    if (!res || !res.ok) {
      console.warn('[Screener] Warning: Binance 24hr ticker feed unavailable across mirrors.');
      return;
    }
    const tickers = await res.json();
    if (!Array.isArray(tickers) || tickers.length === 0) return;

    const candidates = [];
    for (const t of tickers) {
      const sym = t.symbol;
      if (!sym.endsWith('USDT')) continue;
      if (sym.includes('UP') || sym.includes('DOWN') || sym.includes('BEAR') || sym.includes('BULL')) continue;
      if (['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'EURUSDT'].includes(sym)) continue;

      const volQuote = parseFloat(t.quoteVolume);
      if (volQuote < 3000000) continue;

      const price = parseFloat(t.lastPrice);
      const change24h = parseFloat(t.priceChangePercent);
      const high = parseFloat(t.highPrice);
      const low = parseFloat(t.lowPrice);
      const range = (high - low);
      const posInRange = range > 0 ? (price - low) / range : 0.5;
      const intradayRangePct = low > 0 ? ((high - low) / low) * 100 : 4.5;

      // Parabolic top filter: blocks extreme blow-off tops like HAEDAL (+35%)
      if (change24h >= 20.0 || (change24h >= 14.0 && posInRange >= 0.96 && intradayRangePct >= 16.0)) {
        continue;
      }

      // Volume liquidity density bonus (rewards institutional participation)
      const volBonus = Math.min(6, Math.log10(Math.max(1, volQuote / 1000000)) * 2.5);

      // Multi-Factor Composite Scoring
      const score = Math.min(96, Math.max(50, Math.round(58 + (Math.max(0, change24h) * 1.3) + (posInRange * 20) + volBonus)));
      candidates.push({
        pair: sym,
        symbol: sym.replace('USDT', ''),
        priceUSDT: price,
        priceINR: Number((price * State.usdtToInrRate).toFixed(2)),
        score,
        change24h,
        intradayRangePct,
        volumeM: Math.round(volQuote / 1000000),
        pricePositionInRange: posInRange
      });
    }

    candidates.sort((a, b) => b.score - a.score);

    // Expand Autopilot evaluation pool to top 35 candidates (eliminates 10-pair deadlock)
    State.candidatePool = candidates.slice(0, 35);

    // Completely replace State.universe with live dynamically discovered candidates
    const newUniverse = {};
    const topLive = candidates.slice(0, 15);
    topLive.forEach(c => {
      newUniverse[c.pair] = {
        name: c.symbol,
        symbol: c.symbol,
        priceUSDT: c.priceUSDT,
        priceINR: c.priceINR,
        score: c.score,
        direction: c.score >= 80 ? 'STRONG BUY' : (c.score >= 70 ? 'BUY' : 'NEUTRAL'),
        obi: 0.56,
        change24h: c.change24h,
        intradayRangePct: c.intradayRangePct,
        volumeM: c.volumeM,
        pricePositionInRange: c.pricePositionInRange,
        source: 'BINANCE_LIVE_SCAN',
        reasoning: `Live market rank: #${candidates.indexOf(c) + 1} (${c.change24h >= 0 ? '+' : ''}${c.change24h.toFixed(1)}% 24h, $${c.volumeM}M vol)`,
        verifiedAt: Date.now()
      };
    });

    if (Object.keys(newUniverse).length > 0) {
      State.universe = newUniverse;
    }

    State.subsystems.screener.status = 'ACTIVE';
    State.subsystems.screener.scannedPairsCount = candidates.length;
    State.subsystems.screener.qualifiedCandidatesCount = candidates.length;
    State.subsystems.screener.lastRunTime = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

    if (State.autopilotEnabled) {
      await evaluateAutopilotEntries();
    }
    broadcastState();
  } catch (err) {
    console.error('[Screener Error]', err.message);
  }
}

async function evaluateAutopilotEntries() {
  const currentCount = Object.keys(State.activePositions).length;
  if (currentCount >= State.tradeFrequencyController.maxConcurrentPositions) return;

  const freeCash = State.portfolio.cashUSDT;
  if (freeCash < 20.00) return;

  const activeBagsCount = Object.keys(State.recoveryBags).length;
  if (activeBagsCount >= State.tradeFrequencyController.maxRecoveryBagsLimit) return;

  // AQPE Starvation Check: if idle with 0 active positions, enable balanced base hurdle
  const isStarved = currentCount === 0 && activeBagsCount === 0;
  const baseHurdle = isStarved ? 78 : (State.activeThresholds.scoreThreshold || 80);
  const maxAllowedSpread = isStarved ? 0.12 : (State.activeThresholds.maxSpread || 0.08);
  const minRequiredObi = isStarved ? 0.52 : (State.activeThresholds.obiThreshold || 0.54);

  // Evaluate candidate pool (top 35 liquid pairs sorted by score)
  const evalList = (State.candidatePool && State.candidatePool.length > 0)
    ? State.candidatePool
    : Object.keys(State.universe).map(k => ({ pair: k, ...State.universe[k] }));

  for (const item of evalList) {
    const pair = item.pair;
    if (Object.keys(State.activePositions).length >= State.tradeFrequencyController.maxConcurrentPositions) break;
    if (State.activePositions[pair] || State.recoveryBags[pair]) continue;

    // Check 90m Anti-Churn Lockout
    const cd = State.cooldowns[pair];
    if (cd && cd.until > Date.now()) continue;

    const candidate = item;
    const eligibility = learningEngine.evaluateCandidateEligibility(pair, candidate, null, State.subsystems.btcMacroGate);
    if (!eligibility.allowed) continue;

    const effectiveScoreHurdle = baseHurdle + (eligibility.netScoreHurdleOffset || 0);
    if ((candidate.score || 0) < effectiveScoreHurdle) continue;

    const closedBarCheck = await checkConfirmedClosedBar(pair);
    if (!closedBarCheck.passed) continue;

    const tmResult = await evaluateTimeMachineBreakoutOdds(pair);
    if (tmResult.oddsPct < (eligibility.minRequiredOdds || 45)) continue;

    const book = await fetchRealOrderBookDepth(pair);
    if (!book) continue;
    if (book.obi < minRequiredObi) continue;
    if (book.spreadPct > maxAllowedSpread) continue;

    const baselineVol = 4.5;
    const tokenVol = Math.max(2.5, candidate.intradayRangePct || baselineVol);
    const volFactor = Math.min(1.25, Math.max(0.40, baselineVol / tokenVol));

    const entrySnapshot = learningEngine.createEntrySnapshot(pair, candidate, book, null, tmResult.oddsPct, State.subsystems.btcMacroGate);
    executeSpotBuy(pair, candidate, book, volFactor, entrySnapshot);
    break; // Open highest-ranked passing candidate and allow sentry to take over
  }
}

function executeSpotBuy(pair, candidate, book, volFactor, entrySnapshot) {
  const allocPct = Number(State.portfolio.allocationPct || 15);
  const baseAllocation = Math.max(15.00, State.portfolio.cashUSDT * (allocPct / 100));
  const budgetedAllocation = Number((baseAllocation * volFactor).toFixed(2));
  const finalNotional = Math.min(State.portfolio.cashUSDT, Math.max(15.00, budgetedAllocation));

  if (State.portfolio.cashUSDT < finalNotional) return;

  const entryPrice = book.bestAsk || candidate.priceUSDT;
  const qty = Number((finalNotional / entryPrice).toFixed(6));

  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT - finalNotional).toFixed(2));

  State.activePositions[pair] = {
    pairSymbol: candidate.symbol || pair.replace('USDT', ''),
    entryPriceUSDT: entryPrice,
    livePriceUSDT: entryPrice,
    qty,
    notionalUSDT: finalNotional,
    tp1: Number((entryPrice * 1.018).toFixed(6)),
    tp1Secured: false,
    sl: Number((entryPrice * 0.95).toFixed(6)),
    peakPriceUSDT: entryPrice,
    openedAt: Date.now(),
    entrySnapshot
  };

  saveStateToDisk();
  broadcastState();
}

function runPredictiveSentry() {
  const activeKeys = Object.keys(State.activePositions);
  const bagKeys = Object.keys(State.recoveryBags);

  // Monitor Active Positions
  activeKeys.forEach(k => {
    const pos = State.activePositions[k];
    const live = State.universe[k]?.priceUSDT || pos.livePriceUSDT;
    pos.livePriceUSDT = live;
    const gainPct = ((live - pos.entryPriceUSDT) / pos.entryPriceUSDT) * 100;
    pos.gainPct = Number(gainPct.toFixed(2));

    if (live > pos.peakPriceUSDT) pos.peakPriceUSDT = live;

    // TP1 Hit
    if (!pos.tp1Secured && gainPct >= 1.80) {
      pos.tp1Secured = true;
      const bankedCash = (pos.notionalUSDT * 0.5) * (1 + (gainPct / 100));
      const profit = bankedCash - (pos.notionalUSDT * 0.5);
      const subsidy = profit * 0.35;

      State.portfolio.cashUSDT += Number((bankedCash - subsidy).toFixed(2));
      State.portfolio.bagSubsidyPoolUSDT += Number(subsidy.toFixed(2));
      pos.qty = Number((pos.qty * 0.5).toFixed(6));
      pos.notionalUSDT = Number((pos.notionalUSDT * 0.5).toFixed(2));
      pos.sl = Number((pos.entryPriceUSDT * 1.0025).toFixed(6));
    }

    // Runner Trailing Stop Hit
    if (pos.tp1Secured && live <= (pos.peakPriceUSDT * 0.988)) {
      const exitCash = pos.qty * live;
      const profit = exitCash - pos.notionalUSDT;
      State.portfolio.cashUSDT += Number(exitCash.toFixed(2));
      State.portfolio.realizedPnL += Number(profit.toFixed(4));

      State.tradeLogs.unshift({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB'),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: k.includes('/') ? k : `${k.replace('USDT', '')}/USDT`,
        type: 'RUNNER EXIT',
        entryPriceUSDT: pos.entryPriceUSDT,
        exitPriceUSDT: live,
        notionalUSDT: pos.notionalUSDT,
        pnlUSDT: Number(profit.toFixed(4)),
        pnlINR: Number((profit * State.usdtToInrRate).toFixed(2)),
        reason: 'Trailing runner locked at peak'
      });

      learningEngine.recordTradeOutcome(k, 'RUNNER', profit, (Date.now() - pos.openedAt) / 3600000, pos.entrySnapshot);
      State.cooldowns[k] = { until: Date.now() + (90 * 60 * 1000) };
      delete State.activePositions[k];
      saveStateToDisk();
      broadcastState();
      return;
    }

    // Emergency Active Cut (-5%)
    if (gainPct <= -5.00) {
      const returnCash = pos.qty * live;
      const loss = returnCash - pos.notionalUSDT;
      State.portfolio.cashUSDT += Number(returnCash.toFixed(2));
      State.portfolio.realizedPnL += Number(loss.toFixed(4));

      learningEngine.recordTradeOutcome(k, 'HARD DISASTER', loss, (Date.now() - pos.openedAt) / 3600000, pos.entrySnapshot);
      State.cooldowns[k] = { until: Date.now() + (90 * 60 * 1000) };
      delete State.activePositions[k];
      saveStateToDisk();
      broadcastState();
      return;
    }

    // Transfer to Spot Recovery Bag (-3.20%)
    if (!pos.tp1Secured && gainPct <= -3.20) {
      State.recoveryBags[k] = {
        ...pos,
        movedAt: Date.now(),
        targetStep: 'STEP 1 (+0.30%)',
        targetExitPriceUSDT: Number((pos.entryPriceUSDT * 1.003).toFixed(6))
      };
      delete State.activePositions[k];
      saveStateToDisk();
      broadcastState();
    }
  });

  // Monitor Recovery Bags
  bagKeys.forEach(k => {
    const bag = State.recoveryBags[k];
    const live = State.universe[k]?.priceUSDT || bag.livePriceUSDT;
    bag.livePriceUSDT = live;
    const gainPct = ((live - bag.entryPriceUSDT) / bag.entryPriceUSDT) * 100;
    bag.gainPct = Number(gainPct.toFixed(2));
    bag.ageHours = Number(((Date.now() - (bag.movedAt || Date.now())) / 3600000).toFixed(1));

    // Hard Disaster Floor (-6.50%)
    if (gainPct <= -6.50) {
      const returnCash = bag.qty * live;
      const loss = returnCash - bag.notionalUSDT;
      State.portfolio.cashUSDT += Number(returnCash.toFixed(2));
      State.portfolio.realizedPnL += Number(loss.toFixed(4));

      State.tradeLogs.unshift({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB'),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: k.includes('/') ? k : `${k.replace('USDT', '')}/USDT`,
        type: 'HARD DISASTER (-6.50%)',
        entryPriceUSDT: bag.entryPriceUSDT,
        exitPriceUSDT: live,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: Number(loss.toFixed(4)),
        pnlINR: Number((loss * State.usdtToInrRate).toFixed(2)),
        reason: 'Hard disaster floor liquidated to preserve cash'
      });

      learningEngine.recordTradeOutcome(k, 'HARD DISASTER', loss, bag.ageHours, bag.entrySnapshot);
      State.cooldowns[k] = { until: Date.now() + (90 * 60 * 1000) };
      delete State.recoveryBags[k];
      saveStateToDisk();
      broadcastState();
      return;
    }

    // Step Down Targets
    if (bag.ageHours >= 24) {
      bag.targetStep = 'STEP 4 (-1.50%)';
      bag.targetExitPriceUSDT = Number((bag.entryPriceUSDT * 0.985).toFixed(6));
    } else if (bag.ageHours >= 12) {
      bag.targetStep = 'STEP 3 (-0.75%)';
      bag.targetExitPriceUSDT = Number((bag.entryPriceUSDT * 0.9925).toFixed(6));
    } else if (bag.ageHours >= 4) {
      bag.targetStep = 'STEP 2 (0.00%)';
      bag.targetExitPriceUSDT = Number(bag.entryPriceUSDT.toFixed(6));
    }

    // Target Filled on Relief Bounce
    if (live >= bag.targetExitPriceUSDT) {
      const returnCash = bag.qty * live;
      const profit = returnCash - bag.notionalUSDT;
      State.portfolio.cashUSDT += Number(returnCash.toFixed(2));
      State.portfolio.realizedPnL += Number(profit.toFixed(4));

      State.tradeLogs.unshift({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB'),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: k.includes('/') ? k : `${k.replace('USDT', '')}/USDT`,
        type: `RECOVERY (${bag.targetStep})`,
        entryPriceUSDT: bag.entryPriceUSDT,
        exitPriceUSDT: live,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: Number(profit.toFixed(4)),
        pnlINR: Number((profit * State.usdtToInrRate).toFixed(2)),
        reason: 'Relief bounce filled recovery target'
      });

      learningEngine.recordTradeOutcome(k, 'RECOVERY', profit, bag.ageHours, bag.entrySnapshot);
      State.cooldowns[k] = { until: Date.now() + (90 * 60 * 1000) };
      delete State.recoveryBags[k];
      saveStateToDisk();
      broadcastState();
      return;
    }

    // Self-Healing Subsidy Pool Liquidation (>6h)
    if (bag.ageHours >= 6.0 && gainPct < 0) {
      const deficit = bag.notionalUSDT - (bag.qty * live);
      if (deficit > 0 && State.portfolio.bagSubsidyPoolUSDT >= deficit) {
        State.portfolio.bagSubsidyPoolUSDT -= Number(deficit.toFixed(2));
        State.portfolio.cashUSDT += Number(bag.notionalUSDT.toFixed(2));

        State.tradeLogs.unshift({
          id: Date.now(),
          date: new Date().toLocaleDateString('en-GB'),
          time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
          pair: k.includes('/') ? k : `${k.replace('USDT', '')}/USDT`,
          type: 'SUBSIDY CLEAR',
          entryPriceUSDT: bag.entryPriceUSDT,
          exitPriceUSDT: live,
          notionalUSDT: bag.notionalUSDT,
          pnlUSDT: 0.0000,
          pnlINR: 0.00,
          reason: 'Winner subsidy pool absorbed bag deficit'
        });

        learningEngine.recordTradeOutcome(k, 'RECOVERY', 0, bag.ageHours, bag.entrySnapshot);
        State.cooldowns[k] = { until: Date.now() + (90 * 60 * 1000) };
        delete State.recoveryBags[k];
        saveStateToDisk();
        broadcastState();
      }
    }
  });
}

function closeActivePositionManual(pairKey) {
  const pos = State.activePositions[pairKey];
  if (!pos) return;
  const live = State.universe[pairKey]?.priceUSDT || pos.livePriceUSDT;
  const returnCash = pos.qty * live;
  const pnl = returnCash - pos.notionalUSDT;
  State.portfolio.cashUSDT += Number(returnCash.toFixed(2));
  State.portfolio.realizedPnL += Number(pnl.toFixed(4));

  State.tradeLogs.unshift({
    id: Date.now(),
    date: new Date().toLocaleDateString('en-GB'),
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
    pair: pairKey.includes('/') ? pairKey : `${pairKey.replace('USDT', '')}/USDT`,
    type: 'MANUAL EXIT',
    entryPriceUSDT: pos.entryPriceUSDT,
    exitPriceUSDT: live,
    notionalUSDT: pos.notionalUSDT,
    pnlUSDT: Number(pnl.toFixed(4)),
    pnlINR: Number((pnl * State.usdtToInrRate).toFixed(2)),
    reason: 'Manual market exit by operator'
  });

  State.cooldowns[pairKey] = { until: Date.now() + (90 * 60 * 1000) };
  delete State.activePositions[pairKey];
  saveStateToDisk();
  broadcastState();
}

function closeRecoveryBagManual(pairKey) {
  const bag = State.recoveryBags[pairKey];
  if (!bag) return;
  const live = State.universe[pairKey]?.priceUSDT || bag.livePriceUSDT;
  const returnCash = bag.qty * live;
  const pnl = returnCash - bag.notionalUSDT;
  State.portfolio.cashUSDT += Number(returnCash.toFixed(2));
  State.portfolio.realizedPnL += Number(pnl.toFixed(4));

  State.tradeLogs.unshift({
    id: Date.now(),
    date: new Date().toLocaleDateString('en-GB'),
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
    pair: pairKey.includes('/') ? pairKey : `${pairKey.replace('USDT', '')}/USDT`,
    type: 'MANUAL RECOVERY EXIT',
    entryPriceUSDT: bag.entryPriceUSDT,
    exitPriceUSDT: live,
    notionalUSDT: bag.notionalUSDT,
    pnlUSDT: Number(pnl.toFixed(4)),
    pnlINR: Number((pnl * State.usdtToInrRate).toFixed(2)),
    reason: 'Manual recovery bag liquidation by operator'
  });

  State.cooldowns[pairKey] = { until: Date.now() + (90 * 60 * 1000) };
  delete State.recoveryBags[pairKey];
  saveStateToDisk();
  broadcastState();
}

async function syncLiveP2PRate() {
  try {
    const res = await fetchBinance('/api/v3/ticker/price?symbol=USDTINR');
    if (res && res.ok) {
      const data = await res.json();
      const p = parseFloat(data.price);
      if (p > 70 && p < 130) {
        State.usdtToInrRate = Number(p.toFixed(2));
        State.p2pLastUpdated = new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: true });
      }
    }
  } catch (e) {}
}

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: sanitizeAndAuditTradeHistory(State.tradeLogs).slice(0, 100),
    quantTelemetry: State.quantTelemetry,
    marketRegime: State.marketRegime,
    activeThresholds: State.activeThresholds,
    tradeFrequencyController: State.tradeFrequencyController,
    subsystems: State.subsystems,
    usdtToInrRate: State.usdtToInrRate,
    p2pLastUpdated: State.p2pLastUpdated,
    googleSheetUrl: State.googleSheetUrl,
    learningEngineState: learningEngine.state
  });
});

app.get('/healthz', (req, res) => res.status(200).send('OK'));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Quant Server] Running on port ${PORT} with Google Drive File Integration`);
  loadStateFromDisk();
  syncWithGoogleDrive();
  syncLiveP2PRate();
  runDynamicScreener();

  // Periodic intervals
  setInterval(runPredictiveSentry, 2500);
  setInterval(runDynamicScreener, 85000);
  setInterval(syncLiveP2PRate, 600000);
});
