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
// Allow serving index.html directly from root if placed there
app.use(express.static(__dirname));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Baseline universe with fallback anchors
const DEFAULT_UNIVERSE = {
  SOLUSDT:  { name: 'Solana', symbol: 'SOL', priceUSDT: 105.20, priceINR: 9927.72, score: 85, direction: 'STRONG BUY', cvd: 14.2, obi: 0.54, source: 'QUANT_ALGO', reasoning: 'High bid volume depth skew', verifiedAt: Date.now() },
  SUIUSDT:  { name: 'Sui Network', symbol: 'SUI', priceUSDT: 1.03, priceINR: 97.20, score: 58, direction: 'NEUTRAL', cvd: -4.5, obi: 0.44, source: 'QUANT_ALGO', reasoning: 'Balanced bid/ask distribution', verifiedAt: Date.now() },
  AVAXUSDT: { name: 'Avalanche', symbol: 'AVAX', priceUSDT: 11.10, priceINR: 1047.50, score: 72, direction: 'BUY', cvd: 6.4, obi: 0.51, source: 'QUANT_ALGO', reasoning: 'Spot baseline stabilization', verifiedAt: Date.now() },
  BTCUSDT:  { name: 'Bitcoin', symbol: 'BTC', priceUSDT: 84500.00, priceINR: 7974265.00, score: 84, direction: 'STRONG BUY', cvd: 12.3, obi: 0.53, source: 'QUANT_ALGO', reasoning: 'Institutional spot absorption', verifiedAt: Date.now() },
  ETHUSDT:  { name: 'Ethereum', symbol: 'ETH', priceUSDT: 2360.00, priceINR: 222713.20, score: 82, direction: 'STRONG BUY', cvd: 8.1, obi: 0.54, source: 'QUANT_ALGO', reasoning: 'Layer-1 transaction fee baseline', verifiedAt: Date.now() },
  NEARUSDT: { name: 'NEAR Protocol', symbol: 'NEAR', priceUSDT: 3.10, priceINR: 292.55, score: 80, direction: 'BUY', cvd: 10.4, obi: 0.55, source: 'QUANT_ALGO', reasoning: 'Compute narrative spot inflows', verifiedAt: Date.now() },
  LINKUSDT: { name: 'Chainlink', symbol: 'LINK', priceUSDT: 11.75, priceINR: 1108.85, score: 78, direction: 'BUY', cvd: 5.8, obi: 0.52, source: 'QUANT_ALGO', reasoning: 'Cross-chain protocol liquidity', verifiedAt: Date.now() },
  BNBUSDT:  { name: 'BNB', symbol: 'BNB', priceUSDT: 720.00, priceINR: 67946.40, score: 76, direction: 'BUY', cvd: 3.8, obi: 0.51, source: 'QUANT_ALGO', reasoning: 'Steady exchange utility baseline', verifiedAt: Date.now() },
  ADAUSDT:  { name: 'Cardano', symbol: 'ADA', priceUSDT: 0.35, priceINR: 33.03, score: 70, direction: 'BUY', cvd: 2.1, obi: 0.52, source: 'QUANT_ALGO', reasoning: 'Consolidation base', verifiedAt: Date.now() },
  XRPUSDT:  { name: 'Ripple', symbol: 'XRP', priceUSDT: 1.28, priceINR: 120.79, score: 62, direction: 'NEUTRAL', cvd: -1.5, obi: 0.48, source: 'QUANT_ALGO', reasoning: 'Resistance retest', verifiedAt: Date.now() }
};

const State = {
  autopilotEnabled: true,
  usdtToInrRate: 94.37,
  marketRegime: 'BALANCED',
  warmupCycles: 0,
  cooldowns: {}, // Symbol -> timestamp (ms)

  portfolio: {
    cashINR: 20000,
    holdings: {},
    realizedPnL: 0
  },

  activePositions: {},

  quantTelemetry: {
    engine: 'Deterministic Microstructure Quant Engine',
    source: 'PURE_QUANT_ALGO',
    frequencySeconds: 15,
    lastAnalysisTime: '--:--:--',
    regimeReasoning: 'Scoring active universe via 20-level OBI, momentum velocity, and turnover liquidity.'
  },

  universe: { ...DEFAULT_UNIVERSE },
  tradeLogs: []
};

// -------------------------------------------------------------
// PERSISTENCE ENGINE (Non-Destructive)
// -------------------------------------------------------------
function saveStateToDisk() {
  try {
    const dataToSave = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      tradeLogs: State.tradeLogs.slice(0, 500),
      autopilotEnabled: State.autopilotEnabled,
      cooldowns: State.cooldowns
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(dataToSave, null, 2), 'utf-8');
  } catch (err) {
    console.error('[Persistence] Error saving state:', err.message);
  }
}

function loadStateFromDisk() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const raw = fs.readFileSync(STATE_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed.portfolio && typeof parsed.portfolio.cashINR === 'number') {
        State.portfolio = parsed.portfolio;
      }
      if (parsed.activePositions && typeof parsed.activePositions === 'object') {
        State.activePositions = parsed.activePositions;
      }
      if (Array.isArray(parsed.tradeLogs)) {
        // Union merge to protect trade history
        const map = new Map();
        parsed.tradeLogs.forEach(l => { if (l && l.id) map.set(l.id, l); });
        State.tradeLogs = Array.from(map.values()).sort((a, b) => b.id - a.id);
      }
      if (typeof parsed.autopilotEnabled === 'boolean') {
        State.autopilotEnabled = parsed.autopilotEnabled;
      }
      if (parsed.cooldowns && typeof parsed.cooldowns === 'object') {
        State.cooldowns = parsed.cooldowns;
      }
      console.log(`[Persistence] Loaded state successfully. (${State.tradeLogs.length} historical trades restored)`);
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
      if (res.ok) {
        return await res.json();
      }
    } catch (e) {
      // Try next mirror
    }
  }
  return null;
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

// -------------------------------------------------------------
// VOLATILITY REGIME DETECTOR
// -------------------------------------------------------------
function calculateMarketVolatilityRegime() {
  const coins = Object.values(State.universe);
  if (coins.length === 0) return { regime: 'NORMAL', scoreThreshold: 85, obiThreshold: 0.56, maxSpread: 0.12 };

  let totalRangePct = 0;
  let count = 0;

  coins.forEach(c => {
    if (c.intradayRangePct) {
      totalRangePct += c.intradayRangePct;
      count++;
    }
  });

  const avgRange = count > 0 ? (totalRangePct / count) : 8.0;

  if (avgRange >= 14.0) {
    return {
      regime: 'TURBULENT',
      scoreThreshold: 88,
      obiThreshold: 0.58,
      maxSpread: 0.10,
      cooldownMinutes: 45
    };
  }

  if (avgRange <= 6.0) {
    return {
      regime: 'COMPRESSION',
      scoreThreshold: 82,
      obiThreshold: 0.54,
      maxSpread: 0.15,
      cooldownMinutes: 20
    };
  }

  return {
    regime: 'BALANCED',
    scoreThreshold: 85,
    obiThreshold: 0.56,
    maxSpread: 0.12,
    cooldownMinutes: 30
  };
}

// -------------------------------------------------------------
// STAGE 1: DYNAMIC VOLATILITY SCREENER (Every 3 minutes)
// -------------------------------------------------------------
async function runDynamicMarketScreener() {
  try {
    const tickers = await fetchFromBinance('/api/v3/ticker/24hr');
    if (!Array.isArray(tickers) || tickers.length === 0) return;

    const candidates = [];
    const activeSymbols = Object.keys(State.activePositions);

    for (const t of tickers) {
      if (!t.symbol.endsWith('USDT')) continue;
      const base = t.symbol.replace('USDT', '');
      if (['UP', 'DOWN', 'BEAR', 'BULL'].some(x => base.endsWith(x))) continue;

      const quoteVol = parseFloat(t.quoteVolume) || 0;
      const lastPrice = parseFloat(t.lastPrice) || 0;
      const priceChangePct = parseFloat(t.priceChangePercent) || 0;
      const highPrice = parseFloat(t.highPrice) || 0;
      const lowPrice = parseFloat(t.lowPrice) || 0;

      // Ensure asset has minimum institutional liquidity ($15M turnover)
      if (quoteVol >= 15000000 && lastPrice > 0 && highPrice > lowPrice) {
        const pricePositionInRange = (lastPrice - lowPrice) / (highPrice - lowPrice);
        const intradayRangePct = ((highPrice - lowPrice) / lowPrice) * 100;

        // Directional momentum breakout formula: rewards positive velocity and liquidity density
        const directionalMomentum = Math.max(0, priceChangePct);
        const breakoutScore = (directionalMomentum * 3.0) + (Math.log10(quoteVol) * 2.0) + (pricePositionInRange * 5.0);

        candidates.push({
          symbol: t.symbol,
          baseAsset: base,
          priceUSDT: lastPrice,
          priceINR: Math.round(lastPrice * State.usdtToInrRate * 100) / 100,
          priceChangePct,
          intradayRangePct,
          pricePositionInRange,
          volumeM: Math.round(quoteVol / 1000000),
          breakoutScore,
          isActive: activeSymbols.includes(t.symbol)
        });
      }
    }

    // Always keep active positions in the universe
    const activeCandidates = candidates.filter(c => c.isActive);
    const nonActiveSorted = candidates
      .filter(c => !c.isActive)
      .sort((a, b) => b.breakoutScore - a.breakoutScore);

    const neededSlots = Math.max(0, 10 - activeCandidates.length);
    const selected = [...activeCandidates, ...nonActiveSorted.slice(0, neededSlots)];

    if (selected.length > 0) {
      const updatedUniverse = {};
      selected.forEach(c => {
        const existing = State.universe[c.symbol];
        updatedUniverse[c.symbol] = {
          name: c.baseAsset,
          symbol: c.baseAsset,
          priceUSDT: c.priceUSDT,
          priceINR: c.priceINR,
          priceChangePct: c.priceChangePct,
          intradayRangePct: c.intradayRangePct,
          pricePositionInRange: c.pricePositionInRange,
          volumeM: c.volumeM,
          score: existing ? existing.score : 70,
          direction: existing ? existing.direction : 'BUY',
          obi: existing ? existing.obi : 0.52,
          cvd: existing ? existing.cvd : 2.5,
          source: 'QUANT_ALGO',
          reasoning: `Screener Breakout Score: ${c.breakoutScore.toFixed(1)}`,
          verifiedAt: Date.now()
        };
      });

      State.universe = updatedUniverse;
      console.log(`[Screener] Universe refreshed with ${selected.length} high-momentum assets.`);
      broadcastPricesOnly();
    }
  } catch (err) {
    console.error('[Screener Error]', err.message);
  }
}

// -------------------------------------------------------------
// STAGE 2: MATHEMATICAL QUANTITATIVE EVALUATION (Every 15s)
// -------------------------------------------------------------
async function runPureQuantScoring() {
  const symbols = Object.keys(State.universe);
  if (symbols.length === 0) return;

  const regimeConfig = calculateMarketVolatilityRegime();
  State.marketRegime = regimeConfig.regime;

  for (const sym of symbols) {
    const coin = State.universe[sym];
    if (!coin) continue;

    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    coin.obi = book.obi;
    coin.spreadPct = book.spreadPct;

    // Sub-score components
    const obiPts = Math.min(40, Math.max(0, Math.round((book.obi - 0.40) * 133)));
    const change = typeof coin.priceChangePct === 'number' ? coin.priceChangePct : 0;
    const momPts = Math.min(35, Math.max(0, Math.round((change + 3) * 3.5)));
    const volM = typeof coin.volumeM === 'number' ? coin.volumeM : 100;
    const volPts = Math.min(25, Math.max(5, Math.round(Math.log10(Math.max(1, volM)) * 8)));

    const compositeScore = Math.min(98, Math.max(50, obiPts + momPts + volPts));
    coin.score = compositeScore;

    if (compositeScore >= regimeConfig.scoreThreshold && book.obi >= regimeConfig.obiThreshold) {
      coin.direction = 'STRONG BUY';
      coin.reasoning = `High bid depth skew (${(book.obi * 100).toFixed(0)}% Bids) with positive volume momentum`;
    } else if (compositeScore >= 75) {
      coin.direction = 'BUY';
      coin.reasoning = 'Steady baseline accumulation';
    } else if (compositeScore <= 60 || book.obi < 0.46) {
      coin.direction = 'SELL';
      coin.reasoning = 'Ask overhead supply pressure';
    } else {
      coin.direction = 'NEUTRAL';
      coin.reasoning = 'Consolidation phase';
    }
  }

  State.quantTelemetry.lastAnalysisTime = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  State.quantTelemetry.regimeReasoning = `Market regime: ${regimeConfig.regime}. Dynamic entry threshold: ${regimeConfig.scoreThreshold} pts (OBI >= ${(regimeConfig.obiThreshold * 100).toFixed(0)}%).`;

  evaluateAutopilotEntries(regimeConfig);
  broadcastPricesOnly();
}

// -------------------------------------------------------------
// AUTOPILOT ENTRY MATRIX
// -------------------------------------------------------------
async function evaluateAutopilotEntries(regimeConfig) {
  if (!State.autopilotEnabled) return;

  const currentCount = Object.keys(State.activePositions).length;
  if (currentCount >= 3) return; // Max 3 concurrent positions
  if (State.portfolio.cashINR < 3500) return; // Min cash buffer
  if (State.warmupCycles < 2) return; // Guard against stale startup data

  const config = regimeConfig || calculateMarketVolatilityRegime();

  const sortedCandidates = Object.keys(State.universe)
    .filter(k => !State.activePositions[k])
    .sort((a, b) => (State.universe[b].score || 0) - (State.universe[a].score || 0));

  for (const sym of sortedCandidates) {
    const coin = State.universe[sym];
    if (!coin || !coin.score) continue;

    // Cooldown check
    if (State.cooldowns[sym]) {
      const remaining = State.cooldowns[sym] - Date.now();
      if (remaining > 0) continue;
      delete State.cooldowns[sym];
    }

    // Gate 1: Adaptive Score Threshold
    if (coin.score < config.scoreThreshold) continue;

    // Gate 2: Microstructure Order Book Inspection
    const book = await fetchRealOrderBookDepth(sym);
    if (!book) continue;

    // Gate 3: Adaptive OBI Gate
    if (book.obi < config.obiThreshold) continue;

    // Gate 4: Spread Ceiling Guard
    if (book.spreadPct > config.maxSpread) continue;

    // Gate 5: Price verification fresh within 10 seconds
    if (!coin.verifiedAt || (Date.now() - coin.verifiedAt) > 10000) continue;

    // Confluence Pass -> Execute Buy
    executeSimulatedBuy(sym, coin.priceINR, `Adaptive Confluence (${config.regime})`);
    break; // One entry per cycle
  }
}

// -------------------------------------------------------------
// ORDER EXECUTION & LIFECYCLE MANAGEMENT
// -------------------------------------------------------------
function executeSimulatedBuy(pairKey, priceINR, reason) {
  if (State.portfolio.cashINR < 3500) return;
  const notional = Math.min(5000, State.portfolio.cashINR);
  const qty = notional / priceINR;

  State.portfolio.cashINR -= notional;

  // Initial Stop Loss = -0.8%
  const sl = priceINR * (1 - 0.008);
  // Take Profit 1 = +1.2%
  const tp1 = priceINR * (1 + 0.012);

  State.activePositions[pairKey] = {
    pairKey,
    pairSymbol: State.universe[pairKey]?.symbol || pairKey,
    entryPrice: priceINR,
    qty,
    notionalINR: notional,
    sl,
    tp1,
    openedAt: Date.now(),
    isTrailingActive: false,
    decayRisk: 10,
    predictiveStatus: 'SAFE'
  };

  State.portfolio.holdings[pairKey] = {
    symbol: State.universe[pairKey]?.symbol || pairKey,
    qty,
    avgBuyPrice: priceINR,
    totalCostINR: notional
  };

  console.log(`[AUTOPILOT BUY] ${pairKey} @ ₹${priceINR.toFixed(2)} (${reason})`);
  saveStateToDisk();
  broadcastState();
}

function settlePosition(pairKey, exitType, exitPrice, exitReason) {
  const pos = State.activePositions[pairKey];
  if (!pos) return;

  const finalValue = pos.qty * exitPrice;
  const pnl = finalValue - pos.notionalINR;

  State.portfolio.cashINR += finalValue;
  State.portfolio.realizedPnL += pnl;

  delete State.portfolio.holdings[pairKey];
  delete State.activePositions[pairKey];

  // Set 30-minute cooldown on exited asset to prevent immediate churn
  State.cooldowns[pairKey] = Date.now() + (30 * 60 * 1000);

  const logEntry = {
    id: Date.now(),
    type: exitType,
    pair: `${pos.pairSymbol}/INR`,
    entryPrice: pos.entryPrice,
    exitPrice,
    pnlINR: pnl,
    reason: exitReason,
    time: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
    date: new Date().toLocaleDateString('en-IN')
  };

  State.tradeLogs.unshift(logEntry);
  if (State.tradeLogs.length > 500) State.tradeLogs.pop();

  console.log(`[POSITION EXIT] ${pairKey} via ${exitType} | PnL: ₹${Math.round(pnl)} | ${exitReason}`);
  saveStateToDisk();
  broadcastState();
}

// -------------------------------------------------------------
// HIGH-EFFICIENCY POSITION MONITOR (Every 2.5s)
// -------------------------------------------------------------
async function monitorOpenPositionsHealth() {
  const openKeys = Object.keys(State.activePositions);
  if (openKeys.length === 0) return;

  for (const sym of openKeys) {
    const pos = State.activePositions[sym];
    if (!pos) continue;

    const livePrice = State.universe[sym]?.priceINR || pos.entryPrice;
    const gainPct = ((livePrice - pos.entryPrice) / pos.entryPrice) * 100;
    const holdSeconds = (Date.now() - pos.openedAt) / 1000;

    // 1. Take Profit hit (+1.2%)
    if (livePrice >= pos.tp1) {
      settlePosition(sym, 'TP1 REACHED', livePrice, `Target reached (+${gainPct.toFixed(2)}%)`);
      continue;
    }

    // 2. Trailing Stop Ratchet: Lock in Break-Even (+0.1%) once gain touches +0.35%
    if (gainPct >= 0.35 && !pos.isTrailingActive) {
      pos.isTrailingActive = true;
      pos.sl = pos.entryPrice * 1.001; // Secure +0.1% net
      console.log(`[TRAILING SL RATCHET] ${sym} locked at Break-Even +0.1%`);
    }

    // 3. Hard Stop-Loss Hit
    if (livePrice <= pos.sl) {
      settlePosition(sym, pos.isTrailingActive ? 'TRAILING SL' : 'STOP LOSS', livePrice, `Price hit SL boundary (${gainPct.toFixed(2)}%)`);
      continue;
    }

    // 4. Stagnation Exit: If held for 3 minutes without reaching +0.15%, exit near flat
    if (holdSeconds >= 180 && gainPct < 0.15) {
      settlePosition(sym, 'STAGNATION CUT', livePrice, 'No momentum impulse within 3 minutes');
      continue;
    }

    // 5. Predictive Risk Decay Calculation
    let decay = 10;
    const realBook = await fetchRealOrderBookDepth(sym);

    if (realBook) {
      if (realBook.obi < 0.44) decay += 35;
      if (realBook.obi < 0.48) decay += 15;
      if (realBook.spreadPct > 0.12) decay += 15;
    }

    if (gainPct <= -0.15) decay += 20;
    if (gainPct <= -0.30) decay += 25;

    pos.decayRisk = Math.min(100, decay);
    pos.predictiveStatus = pos.decayRisk >= 45 ? 'EXITING' : (pos.decayRisk >= 25 ? 'CAUTION' : 'SAFE');

    // Fast Micro-Cut: If trade slips to -0.20% and decay is elevated (>= 40%), cut early
    if (gainPct <= -0.20 && pos.decayRisk >= 40) {
      settlePosition(sym, 'PREDICTIVE SL', livePrice, `Early loss mitigation at ${gainPct.toFixed(2)}% (Decay: ${pos.decayRisk}%)`);
      continue;
    }

    // Emergency Order Book Flip within first 90s
    if (holdSeconds <= 90 && realBook && realBook.obi < 0.42 && gainPct <= -0.10) {
      settlePosition(sym, 'ORDERBOOK FLIP', livePrice, 'Buyer depth collapsed immediately post-entry');
      continue;
    }
  }
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
        State.universe[sym].priceUSDT = usdt;
        State.universe[sym].priceINR = Math.round(usdt * State.usdtToInrRate * 100) / 100;
        State.universe[sym].verifiedAt = Date.now();
      }
    });

    State.warmupCycles++;
    await monitorOpenPositionsHealth();
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
    usdtToInrRate: State.usdtToInrRate
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
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
    syncTime: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
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
        const livePrice = State.universe[data.pairKey]?.priceINR || State.activePositions[data.pairKey]?.entryPrice;
        if (livePrice) settlePosition(data.pairKey, 'MANUAL EXIT', livePrice, 'Closed by user command');
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
    usdtToInrRate: State.usdtToInrRate
  });
});

app.get('/api/screener/run', async (req, res) => {
  await runDynamicMarketScreener();
  res.json({ success: true, universeCount: Object.keys(State.universe).length, universe: State.universe });
});

app.get('/api/quant/audit', (req, res) => {
  res.json({
    engine: State.quantTelemetry.engine,
    marketRegime: State.marketRegime,
    activePositionsCount: Object.keys(State.activePositions).length,
    cooldowns: State.cooldowns,
    sampleAsset: Object.values(State.universe)[0] || null
  });
});

app.get('/healthz', (req, res) => res.status(200).send('OK'));

// -------------------------------------------------------------
// BOOTSTRAP TIMERS
// -------------------------------------------------------------
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Quant Server] Running on port ${PORT}`);

  // 1. Dynamic Market Screener: runs 3s after boot, then every 3 minutes
  setTimeout(runDynamicMarketScreener, 3000);
  setInterval(runDynamicMarketScreener, 3 * 60 * 1000);

  // 2. Pure Quant Scoring Engine: runs every 15 seconds
  setInterval(runPureQuantScoring, 15 * 1000);

  // 3. Live Binance Price Polling & Position Risk Health: runs every 2.5 seconds
  setInterval(syncLiveBinancePrices, 2500);
});
