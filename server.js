/**
 * SISIR'S 100% PURE QUANT MICROSTRUCTURE TERMINAL & TRADING ENGINE (v12.0)
 * ------------------------------------------------------------------------------------
 * - ZERO AI / ZERO LLM DEPENDENCIES: 100% deterministic mathematical execution.
 * - Dynamic Two-Stage Screener scanning all 350+ Binance USDT pairs.
 * - Real-time 20-level Order Book Imbalance (OBI), Momentum, and Volume Liquidity scoring.
 * - Predictive Health & Trailing Stop Ratchet (+0.2% BE at +1.5% gain; early cut at -0.5%).
 * - Zero API key costs ($0 / ₹0 per day).
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || 'jtG8g4dNoJpXQ6vu03f8l4quEir2HHCVOrXXfpgmSCxV1ubg4p4eaOmN9oH3eG53';

const app = express();
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'X-MBX-APIKEY'] }));
app.options('*', cors());
app.use(express.json());

console.log('================================================================');
console.log("[Quant Engine v12.0] 100% Algorithmic Engine (Zero LLM / Zero External AI)");
console.log(`[Quant Engine v12.0] Port: ${PORT} | Render 24/7 Dedicated Server`);
console.log('================================================================');

// Multi-endpoint mirror array to bypass datacenter/cloud geoblocks
const BINANCE_HOSTS = [
  'https://data-api.binance.vision',
  'https://api1.binance.com',
  'https://api3.binance.com',
  'https://api.binance.com'
];
let currentHostIndex = 0;

async function fetchFromBinance(apiPath) {
  for (let i = 0; i < BINANCE_HOSTS.length; i++) {
    const hostIdx = (currentHostIndex + i) % BINANCE_HOSTS.length;
    const url = `${BINANCE_HOSTS[hostIdx]}${apiPath}`;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 6000);
      const res = await fetch(url, {
        signal: controller.signal,
        headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }
      });
      clearTimeout(timeout);
      if (res.ok) {
        currentHostIndex = hostIdx;
        return await res.json();
      }
    } catch (e) {}
  }
  return null;
}

const State = {
  autopilotEnabled: true,
  analysisActive: true,
  usdtToInrRate: Number(process.env.USDT_INR_RATE) || 94.37,
  lastMarketSyncTime: null,
  marketSyncCount: 0,
  lastScreenerRunTime: null,

  maxConcurrentPositions: 3,
  allocationPerTradeINR: 6000,
  marketRegime: 'EXPANSION',

  portfolio: {
    cashINR: 20000,
    holdings: {},
    realizedPnL: 0
  },

  activePositions: {},

  quantTelemetry: {
    engine: 'Deterministic Microstructure Engine (Pure Math)',
    source: 'PURE_QUANT_ALGO',
    frequencySeconds: 15,
    lastAnalysisTime: '--:--:--',
    cyclesExecuted: 0,
    regimeReasoning: 'Scoring all active pairs via 20-level OBI, 24h momentum velocity, and turnover liquidity.'
  },

  universe: {
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
  },

  tradeLogs: []
};

function loadPersistedState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      if (data.portfolio && typeof data.portfolio.cashINR === 'number') {
        State.portfolio = data.portfolio;
        State.portfolio.cashINR = Number(State.portfolio.cashINR);
      }
      if (data.activePositions && typeof data.activePositions === 'object') {
        State.activePositions = data.activePositions;
      }
      if (typeof data.autopilotEnabled === 'boolean') {
        State.autopilotEnabled = data.autopilotEnabled;
      }
      if (Array.isArray(data.tradeLogs)) {
        State.tradeLogs = data.tradeLogs;
      }
      console.log(`[Quant Engine] Restored state: Cash=₹${Math.round(State.portfolio.cashINR)}, Active Positions=${Object.keys(State.activePositions).length}`);
    } else {
      persistState();
    }
  } catch (err) {
    console.error('[Quant Engine] Error reading state:', err.message);
  }
}

function persistState() {
  try {
    const payload = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      autopilotEnabled: State.autopilotEnabled,
      marketRegime: State.marketRegime,
      tradeLogs: State.tradeLogs.slice(0, 100),
      timestamp: Date.now()
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(payload, null, 2));
  } catch (err) {
    console.error('[Quant Engine] Error saving state:', err.message);
  }
}

async function syncBinanceUsdtInrRate() {
  try {
    const data = await fetchFromBinance('/api/v3/ticker/price?symbol=USDTINR');
    if (data && data.price) {
      const liveRate = parseFloat(data.price);
      if (liveRate && liveRate > 80 && liveRate < 120) {
        State.usdtToInrRate = Math.round(liveRate * 100) / 100;
      }
    }
  } catch (e) {}
}
setInterval(syncBinanceUsdtInrRate, 60000);
syncBinanceUsdtInrRate();

async function fetchRealOrderBookDepth(symbol) {
  try {
    const data = await fetchFromBinance(`/api/v3/depth?symbol=${symbol}&limit=20`);
    if (data && data.bids && data.asks) {
      let bidVol = 0;
      let askVol = 0;
      data.bids.forEach(b => { bidVol += parseFloat(b[1]) || 0; });
      data.asks.forEach(a => { askVol += parseFloat(a[1]) || 0; });
      const total = bidVol + askVol;
      return total > 0 ? bidVol / total : 0.50;
    }
  } catch (e) {}
  return null;
}

/**
 * STAGE 1: DYNAMIC MARKET-WIDE SCREENER
 * Evaluates all 350+ Binance USDT pairs using geo-resilient mirrors.
 */
async function runDynamicMarketScreener() {
  try {
    console.log('[Dynamic Screener] Scanning all Binance USDT pairs for volume & momentum breakouts...');
    const allTickers = await fetchFromBinance('/api/v3/ticker/24hr');
    if (!allTickers || !Array.isArray(allTickers)) return { success: false, reason: 'Failed to fetch tickers' };

    const activeSymbols = Object.keys(State.activePositions);
    const eligible = [];

    allTickers.forEach(item => {
      const sym = item.symbol;
      if (!sym.endsWith('USDT')) return;
      if (sym.includes('UP') || sym.includes('DOWN') || sym.includes('BEAR') || sym.includes('BULL')) return;
      if (sym.startsWith('USDC') || sym.startsWith('FDUSD') || sym.startsWith('TUSD') || sym.startsWith('EUR')) return;

      const volumeUSDT = parseFloat(item.quoteVolume) || 0;
      const priceChangePct = parseFloat(item.priceChangePercent) || 0;
      const lastPrice = parseFloat(item.lastPrice) || 0;

      if (volumeUSDT >= 10000000 && lastPrice > 0) {
        const breakoutScore = (Math.abs(priceChangePct) * 2.2) + (Math.log10(volumeUSDT) * 1.5) + (priceChangePct > 0 ? 3.0 : 0);
        eligible.push({
          symbol: sym,
          baseAsset: sym.replace('USDT', ''),
          priceUSDT: lastPrice,
          priceChangePct,
          volumeUSDT,
          breakoutScore
        });
      }
    });

    eligible.sort((a, b) => b.breakoutScore - a.breakoutScore);

    const newUniverse = {};
    const addedSymbols = new Set();

    activeSymbols.forEach(sym => {
      if (State.universe[sym]) {
        newUniverse[sym] = State.universe[sym];
        addedSymbols.add(sym);
      }
    });

    for (const cand of eligible) {
      if (Object.keys(newUniverse).length >= 10) break;
      if (addedSymbols.has(cand.symbol)) continue;

      const sym = cand.symbol;
      const existing = State.universe[sym];

      newUniverse[sym] = {
        name: cand.baseAsset,
        symbol: cand.baseAsset,
        priceUSDT: cand.priceUSDT,
        priceINR: Math.round(cand.priceUSDT * State.usdtToInrRate * 100) / 100,
        score: existing ? existing.score : (cand.priceChangePct > 0 ? 75 : 65),
        direction: existing ? existing.direction : (cand.priceChangePct > 0 ? 'BUY' : 'NEUTRAL'),
        cvd: existing ? existing.cvd : (cand.priceChangePct * 1.2),
        obi: existing ? existing.obi : 0.52,
        priceChangePct: cand.priceChangePct,
        volumeM: Math.round(cand.volumeUSDT / 1e6),
        source: 'QUANT_ALGO',
        reasoning: existing ? existing.reasoning : `${cand.priceChangePct >= 0 ? '+' : ''}${cand.priceChangePct.toFixed(1)}% 24h ($${Math.round(cand.volumeUSDT / 1e6)}M Vol)`,
        verifiedAt: Date.now()
      };
      addedSymbols.add(sym);
    }

    if (Object.keys(newUniverse).length >= 5) {
      State.universe = newUniverse;
      State.lastScreenerRunTime = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata' });
      broadcastState();
      return { success: true, count: Object.keys(State.universe).length, assets: Object.keys(State.universe) };
    }
  } catch (err) {
    console.error('[Dynamic Screener] Error running stage 1:', err.message);
    return { success: false, error: err.message };
  }
}

setTimeout(runDynamicMarketScreener, 2000);
setInterval(runDynamicMarketScreener, 10 * 60 * 1000);

/**
 * STAGE 2: PURE MATHEMATICAL QUANTITATIVE EVALUATION (RUNS EVERY 15 SECONDS)
 * Completely replaces external LLM calls with institutional mathematical scoring.
 */
async function runPureQuantitativeAnalysis() {
  if (!State.analysisActive) return;

  State.quantTelemetry.cyclesExecuted++;
  State.quantTelemetry.lastAnalysisTime = new Date().toLocaleTimeString('en-IN', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata'
  });

  let ticker24hMap = {};
  try {
    const data = await fetchFromBinance('/api/v3/ticker/24hr');
    if (data && Array.isArray(data)) {
      data.forEach(item => {
        ticker24hMap[item.symbol] = {
          priceChangePercent: parseFloat(item.priceChangePercent) || 0,
          quoteVolume: parseFloat(item.quoteVolume) || 0
        };
      });
    }
  } catch (e) {}

  let strongBuyCount = 0;

  for (const sym of Object.keys(State.universe)) {
    const item = State.universe[sym];
    const liveObi = await fetchRealOrderBookDepth(sym);
    const obi = (liveObi !== null && liveObi !== undefined) ? liveObi : (item.obi || 0.50);
    const ticker = ticker24hMap[sym] || { priceChangePercent: item.priceChangePct || 0, quoteVolume: 50000000 };

    item.obi = obi;
    item.priceChangePct = ticker.priceChangePercent;
    item.volumeM = Math.round(ticker.quoteVolume / 1e6);

    // 1. Order Book Imbalance Component (Max 40 pts)
    const obiScore = Math.max(0, Math.min(40, (obi - 0.40) * 133));
    // 2. 24h Momentum Velocity Component (Max 35 pts)
    const momScore = Math.max(0, Math.min(35, (ticker.priceChangePercent + 3) * 3.5));
    // 3. Dollar Turnover Liquidity Component (Max 25 pts)
    const volScore = Math.min(25, Math.log10(Math.max(1, item.volumeM)) * 8);

    const computedScore = Math.min(98, Math.max(50, Math.round(obiScore + momScore + volScore)));

    item.score = computedScore;
    item.source = 'QUANT_ALGO';

    if (computedScore >= 82) {
      item.direction = 'STRONG BUY';
      item.reasoning = `Buyer skew (${Math.round(obi * 100)}%) with 24h momentum +${ticker.priceChangePercent.toFixed(1)}%`;
      strongBuyCount++;
    } else if (computedScore >= 75) {
      item.direction = 'BUY';
      item.reasoning = `Positive accumulation baseline (OBI: ${Math.round(obi * 100)}%)`;
    } else if (computedScore >= 62) {
      item.direction = 'NEUTRAL';
      item.reasoning = `Balanced liquidity equilibrium (OBI: ${Math.round(obi * 100)}%)`;
    } else {
      item.direction = 'SELL';
      item.reasoning = `Overhead ask pressure (OBI: ${Math.round(obi * 100)}%)`;
    }
  }

  // Pure Algorithmic Market Regime Detection
  if (strongBuyCount >= 3) {
    State.marketRegime = 'EXPANSION';
    State.quantTelemetry.regimeReasoning = `${strongBuyCount} coins exhibiting active order-book buyer skew and momentum expansion.`;
  } else if (strongBuyCount >= 1) {
    State.marketRegime = 'BALANCED';
    State.quantTelemetry.regimeReasoning = `Selective bid absorption across isolated assets with consolidation baseline.`;
  } else {
    State.marketRegime = 'DEFENSIVE';
    State.quantTelemetry.regimeReasoning = `Ask resistance dominance across universe. Defensive risk parameters active.`;
  }

  // Autopilot Execution Check
  if (State.autopilotEnabled && State.portfolio.cashINR >= 2500 && State.marketSyncCount >= 2) {
    const currentOpenCount = Object.keys(State.activePositions).length;
    const maxAllowed = State.marketRegime === 'EXPANSION' ? State.maxConcurrentPositions : 1;

    if (currentOpenCount < maxAllowed) {
      const now = Date.now();
      const availableCandidates = Object.keys(State.universe)
        .filter(key => {
          const item = State.universe[key];
          const isFresh = (now - (item.verifiedAt || 0)) <= 5000;
          return !State.activePositions[key] && item.priceINR > 0 && isFresh;
        })
        .sort((a, b) => State.universe[b].score - State.universe[a].score);

      const topKey = availableCandidates[0];
      const candidate = State.universe[topKey];

      if (candidate && candidate.score >= 78) {
        const slotsAvailable = maxAllowed - currentOpenCount;
        const targetAllocation = Math.min(State.allocationPerTradeINR, Math.floor(State.portfolio.cashINR / Math.max(1, slotsAvailable)));

        if (targetAllocation >= 2000) {
          const livePrice = candidate.priceINR;
          const qty = targetAllocation / livePrice;
          const slDist = livePrice * 0.015;

          State.activePositions[topKey] = {
            pairKey: topKey,
            pairName: candidate.name,
            pairSymbol: candidate.symbol,
            entryPrice: livePrice,
            notionalINR: targetAllocation,
            qty,
            sl: livePrice - slDist,
            tp1: livePrice + (slDist * 2.4),
            peakGainPct: 0,
            decayRisk: 15,
            predictiveStatus: 'SAFE',
            isTrailingActive: false,
            openedAt: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Kolkata' }),
            reasoning: candidate.reasoning
          };

          State.portfolio.cashINR -= targetAllocation;
          State.portfolio.holdings[topKey] = {
            name: candidate.name,
            symbol: candidate.symbol,
            qty,
            totalCostINR: targetAllocation,
            avgBuyPrice: livePrice
          };

          console.log(`[Quant Execution] BUY on ${candidate.symbol} @ ₹${livePrice.toLocaleString('en-IN')} | Score: ${candidate.score}/100 | Capital: ₹${targetAllocation}`);
          persistState();
        }
      }
    }
  }

  broadcastState();
}
setInterval(runPureQuantitativeAnalysis, 15000);

/**
 * LIVE BINANCE PRICE STREAM & POSITION HEALTH (EVERY 2.5 SECONDS)
 */
async function fetchLiveBinancePrices() {
  try {
    const allTickers = await fetchFromBinance('/api/v3/ticker/price');
    if (allTickers && Array.isArray(allTickers)) {
      const tickerMap = {};
      allTickers.forEach(t => { tickerMap[t.symbol] = parseFloat(t.price); });

      const now = Date.now();
      let anyUpdated = false;

      Object.keys(State.universe).forEach(sym => {
        if (tickerMap[sym]) {
          const usdt = tickerMap[sym];
          State.universe[sym].priceUSDT = usdt;
          State.universe[sym].priceINR = Math.round(usdt * State.usdtToInrRate * 100) / 100;
          State.universe[sym].verifiedAt = now;
          anyUpdated = true;
        }
      });

      if (anyUpdated) {
        State.marketSyncCount++;
        State.lastMarketSyncTime = new Date().toLocaleTimeString('en-IN', {
          hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata'
        });
        await monitorOpenPositionsHealth();
      }
    }
  } catch (e) {}
}
setInterval(fetchLiveBinancePrices, 2500);

async function monitorOpenPositionsHealth() {
  const activeKeys = Object.keys(State.activePositions);
  if (activeKeys.length === 0) {
    broadcastPricesOnly();
    return;
  }

  for (const pairKey of activeKeys) {
    const pos = State.activePositions[pairKey];
    if (!pos) continue;

    const livePrice = State.universe[pairKey]?.priceINR || pos.entryPrice;
    const gainPct = ((livePrice - pos.entryPrice) / pos.entryPrice) * 100;

    if (gainPct > (pos.peakGainPct || 0)) {
      pos.peakGainPct = gainPct;
    }

    if (gainPct >= 1.5 && !pos.isTrailingActive) {
      pos.isTrailingActive = true;
      pos.sl = pos.entryPrice * 1.002;
      console.log(`[Trailing Ratchet Triggered] ${pos.pairSymbol} locked at Break-Even (+0.2%)`);
    }

    const realObi = await fetchRealOrderBookDepth(pairKey);
    let decayRisk = 15;

    if (realObi !== null) {
      if (realObi < 0.45) decayRisk += 35;
      else if (realObi < 0.50) decayRisk += 20;
    }

    const dropFromPeak = (pos.peakGainPct || 0) - gainPct;
    if (dropFromPeak >= 0.8) decayRisk += 30;
    else if (dropFromPeak >= 0.4) decayRisk += 15;

    if (gainPct < -0.3) decayRisk += 25;

    pos.decayRisk = Math.min(95, decayRisk);
    pos.predictiveStatus = pos.decayRisk >= 65 ? 'EXITING' : (pos.decayRisk >= 40 ? 'CAUTION' : 'SAFE');

    if (livePrice <= pos.sl) {
      settlePosition(pairKey, 'HARD SL', livePrice, 'Hit designated Stop-Loss boundary');
      continue;
    }

    if (livePrice >= pos.tp1) {
      settlePosition(pairKey, 'HARD TP', livePrice, 'Target profit attained (+3.6%)');
      continue;
    }

    if (gainPct <= -0.5 && pos.decayRisk >= 70) {
      settlePosition(pairKey, 'PREDICTIVE SL', livePrice, `Loss mitigation cut: Decay ${pos.decayRisk}% (Closed at ${gainPct.toFixed(2)}%)`);
      continue;
    }

    if (gainPct >= 0.5 && pos.decayRisk >= 65) {
      settlePosition(pairKey, 'PREDICTIVE TP', livePrice, `Predictive profit capture: Decay ${pos.decayRisk}% (Locked +${gainPct.toFixed(2)}%)`);
      continue;
    }
  }

  broadcastPricesOnly();
}

function settlePosition(pairKey, type, exitPrice, reason) {
  const pos = State.activePositions[pairKey];
  if (!pos) return;

  const finalVal = pos.qty * exitPrice;
  const pnl = finalVal - pos.notionalINR;

  State.portfolio.cashINR += finalVal;
  State.portfolio.realizedPnL += pnl;

  delete State.portfolio.holdings[pairKey];
  delete State.activePositions[pairKey];

  const logEntry = {
    id: Date.now(),
    type,
    pair: `${pos.pairSymbol}/INR`,
    entryPrice: pos.entryPrice,
    exitPrice,
    pnlINR: pnl,
    reason,
    time: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata' }),
    date: new Date().toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' })
  };

  State.tradeLogs.unshift(logEntry);
  console.log(`[Trade Settle] ${type} on ${logEntry.pair} | Exit: ₹${exitPrice} | PnL: ₹${Math.round(pnl)} | Reason: ${reason}`);

  persistState();
  broadcastState();
}

function broadcastState() {
  const payload = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    autopilotEnabled: State.autopilotEnabled,
    marketRegime: State.marketRegime,
    quantTelemetry: State.quantTelemetry,
    universe: State.universe,
    tradeLogs: State.tradeLogs.slice(0, 50),
    timestamp: Date.now()
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

function broadcastPricesOnly() {
  const priceMap = {};
  Object.keys(State.universe).forEach(k => {
    priceMap[k] = {
      priceINR: State.universe[k].priceINR,
      priceUSDT: State.universe[k].priceUSDT,
      score: State.universe[k].score,
      obi: State.universe[k].obi,
      source: State.universe[k].source
    };
  });

  const payload = JSON.stringify({
    type: 'PRICE_TICK',
    prices: priceMap,
    activePositions: State.activePositions,
    portfolio: State.portfolio,
    marketRegime: State.marketRegime,
    quantTelemetry: State.quantTelemetry,
    usdtToInrRate: State.usdtToInrRate,
    syncTime: State.lastMarketSyncTime
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

app.get('/', (req, res) => {
  res.json({
    service: "Sisir's Dedicated 24/7 Pure Quant Microstructure Terminal",
    engineType: "100% Mathematical Quant (No External LLM / No AI)",
    status: 'online',
    binanceFeed: 'live',
    universeSize: Object.keys(State.universe).length,
    universeAssets: Object.keys(State.universe),
    lastScreenerRun: State.lastScreenerRunTime,
    lastQuantAnalysisTime: State.quantTelemetry.lastAnalysisTime,
    autopilotEnabled: State.autopilotEnabled,
    cashINR: State.portfolio.cashINR,
    activePositionsCount: Object.keys(State.activePositions).length,
    realizedPnL: State.portfolio.realizedPnL
  });
});

app.get('/api/screener/run', async (req, res) => {
  const result = await runDynamicMarketScreener();
  res.json({
    trigger: 'manual_screener_run',
    result,
    currentUniverse: Object.keys(State.universe),
    lastRun: State.lastScreenerRunTime
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    engine: 'PURE_MATHEMATICAL_QUANT',
    lastMarketSync: State.lastMarketSyncTime,
    lastQuantAnalysis: State.quantTelemetry.lastAnalysisTime,
    lastScreenerRun: State.lastScreenerRunTime,
    autopilot: State.autopilotEnabled,
    universeSize: Object.keys(State.universe).length
  });
});

app.get('/api/quant/audit', (req, res) => {
  res.json({
    status: 'operational',
    engine: State.quantTelemetry.engine,
    marketRegime: State.marketRegime,
    regimeReasoning: State.quantTelemetry.regimeReasoning,
    cyclesExecuted: State.quantTelemetry.cyclesExecuted,
    frequency: 'Every 15 seconds',
    scoringFormula: {
      OBI: 'Max 40 pts: clamp(0, 40, (OBI - 0.40) * 133)',
      Momentum: 'Max 35 pts: clamp(0, 35, (Pct + 3) * 3.5)',
      Volume: 'Max 25 pts: min(25, log10(Vol_M) * 8)',
      TriggerGate: '>= 78 Composite Points'
    },
    universeScores: Object.keys(State.universe).map(k => ({
      pair: k,
      score: State.universe[k].score,
      direction: State.universe[k].direction,
      obi: State.universe[k].obi,
      reasoning: State.universe[k].reasoning
    }))
  });
});

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    universe: State.universe,
    autopilotEnabled: State.autopilotEnabled,
    marketRegime: State.marketRegime,
    quantTelemetry: State.quantTelemetry,
    tradeLogs: State.tradeLogs.slice(0, 50)
  });
});

app.post('/api/autopilot/toggle', (req, res) => {
  State.autopilotEnabled = !State.autopilotEnabled;
  if (State.autopilotEnabled) runPureQuantitativeAnalysis();
  persistState();
  broadcastState();
  res.json({ success: true, autopilotEnabled: State.autopilotEnabled });
});

app.post('/api/trade/close-position', (req, res) => {
  const { pairKey } = req.body;
  if (pairKey && State.activePositions[pairKey]) {
    const curPrice = State.universe[pairKey]?.priceINR || State.activePositions[pairKey].entryPrice;
    settlePosition(pairKey, 'MANUAL', curPrice, 'Manual User Override');
    return res.json({ success: true, message: `Position ${pairKey} closed` });
  }
  res.status(404).json({ error: 'Position not found' });
});

app.post('/api/reset-portfolio', (req, res) => {
  State.portfolio = { cashINR: 20000, holdings: {}, realizedPnL: 0 };
  State.activePositions = {};
  persistState();
  broadcastState();
  res.json({ success: true, cashINR: State.portfolio.cashINR });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'INIT',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    universe: State.universe,
    marketRegime: State.marketRegime,
    quantTelemetry: State.quantTelemetry,
    autopilotEnabled: State.autopilotEnabled,
    tradeLogs: State.tradeLogs.slice(0, 50),
    lastMarketSync: State.lastMarketSyncTime
  }));

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(data.enabled);
        if (State.autopilotEnabled) runPureQuantitativeAnalysis();
        persistState();
        broadcastState();
      } else if (data.type === 'CLOSE_POSITION') {
        const pairKey = data.pairKey;
        if (pairKey && State.activePositions[pairKey]) {
          const curPrice = State.universe[pairKey]?.priceINR || State.activePositions[pairKey].entryPrice;
          settlePosition(pairKey, 'MANUAL', curPrice, 'Manual User Override');
        }
      }
    } catch (e) {}
  });
});

loadPersistedState();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`================================================================`);
  console.log(` Sisir's Pure Quant Engine v12.0 RUNNING on port ${PORT}`);
  console.log(` Screener: Geo-Resilient Dynamic Market-Wide Screener Active`);
  console.log(` Deterministic Math Loop: Active every 15s (Zero AI / $0 Cost)`);
  console.log(` Real-time Binance Depth & Health: Active every 2.5s`);
  console.log(`================================================================`);
});
