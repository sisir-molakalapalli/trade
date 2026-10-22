/**
 * SISIR'S CLOUD QUANT ENGINE & PAPER TRADING SERVER (v10.5)
 * ------------------------------------------------------------------------------------
 * Runs 24/7 on Render ($7 Dedicated Plan).
 * Features:
 *  - Official Google Gemini AI Quantitative Reasoning Pipeline
 *  - Live Binance Depth (/api/v3/depth) for Real-Time Order Book Imbalance (OBI)
 *  - Clear Source Attribution: GEMINI_2_5_FLASH vs. DETERMINISTIC_FALLBACK
 *  - Dedicated /api/llm/test diagnostic endpoint
 *  - Real-time Predictive Position Health (SAFE / CAUTION / EXITING)
 *  - Loss Mitigation Early Exit (Cuts bad trades at -0.5% if OBI decays >= 70%)
 *  - Trailing Stop Ratchet (+0.2% Break-Even lock upon crossing +1.5% unrealized gain)
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
// Strictly retrieved from Render Environment Settings - no hardcoded credentials
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';

const app = express();
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'X-MBX-APIKEY'] }));
app.options('*', cors());
app.use(express.json());

console.log('================================================================');
console.log("[Cloud Engine v10.5] 24/7 Autonomous Microstructure & Gemini AI Engine");
console.log(`[Cloud Engine v10.5] Port: ${PORT} | Render 24/7 Dedicated Server`);
console.log(`[Cloud Engine v10.5] Gemini AI Configured: ${GEMINI_API_KEY ? 'YES (Key Loaded from Env)' : 'NO (Using Microstructure Fallback)'}`);
console.log('================================================================');

const State = {
  autopilotEnabled: true,
  analysisActive: true,
  usdtToInrRate: Number(process.env.USDT_INR_RATE) || 94.37,
  lastMarketSyncTime: null,

  maxConcurrentPositions: 3,
  allocationPerTradeINR: 6000,
  marketRegime: 'MULTI_EXPANSION',

  portfolio: {
    cashINR: 20000,
    holdings: {},
    realizedPnL: 0
  },

  activePositions: {},

  llmTelemetry: {
    model: GEMINI_API_KEY ? 'Gemini 3.8 Flash (Live 24/7 Cloud API)' : 'Deterministic Microstructure Engine',
    source: GEMINI_API_KEY ? 'GEMINI_3_8_FLASH' : 'DETERMINISTIC_FALLBACK',
    frequencySeconds: 20,
    lastAnalysisTime: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }),
    promptExecutionCount: 0,
    lastLatencyMs: 0,
    lastError: null,
    regimeReasoning: "Initializing real-time quantitative microstructure and AI market evaluation...",
    weights: {
      orderBookImbalance: 0.35,
      cumulativeVolumeDelta: 0.30,
      momentumBreakout: 0.20,
      volatilityCompression: 0.15
    },
    rules: {
      hardStopLossPct: 1.5,
      hardTakeProfitPct: 3.6,
      trailingRatchetTriggerPct: 1.5,
      trailingRatchetLockPct: 0.2,
      predictiveTakeProfitThreshold: 65,
      predictiveLossMitigationThreshold: 70
    }
  },

  universe: {
    SOLUSDT:  { name: 'Solana', symbol: 'SOL', priceUSDT: 108.53, priceINR: 10241.97, score: 92, direction: 'STRONG BUY', cvd: 24.2, obi: 0.62, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Perpetual open interest high, strong spot taker delta' },
    SUIUSDT:  { name: 'Sui Network', symbol: 'SUI', priceUSDT: 1.03, priceINR: 97.20, score: 57, direction: 'NEUTRAL', cvd: -4.5, obi: 0.44, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Balanced bid/ask distribution, short-term consolidation' },
    AVAXUSDT: { name: 'Avalanche', symbol: 'AVAX', priceUSDT: 18.50, priceINR: 1745.85, score: 88, direction: 'STRONG BUY', cvd: 16.4, obi: 0.59, source: 'DETERMINISTIC_FALLBACK', reasoning: 'L1 volume rotation, strong buyer delta' },
    ETHUSDT:  { name: 'Ethereum', symbol: 'ETH', priceUSDT: 2360.00, priceINR: 222713.20, score: 84, direction: 'BUY', cvd: 8.1, obi: 0.56, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Spot accumulation, positive delta across aggregate trades' },
    BTCUSDT:  { name: 'Bitcoin', symbol: 'BTC', priceUSDT: 84500.00, priceINR: 7974265.00, score: 85, direction: 'BUY', cvd: 12.3, obi: 0.53, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Macro institutional spot absorption' },
    NEARUSDT: { name: 'NEAR Protocol', symbol: 'NEAR', priceUSDT: 3.10, priceINR: 292.55, score: 82, direction: 'BUY', cvd: 10.4, obi: 0.55, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Compute narrative spot inflows' },
    LINKUSDT: { name: 'Chainlink', symbol: 'LINK', priceUSDT: 11.75, priceINR: 1108.85, score: 79, direction: 'BUY', cvd: 5.8, obi: 0.52, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Cross-chain protocol liquidity spike' },
    BNBUSDT:  { name: 'BNB', symbol: 'BNB', priceUSDT: 720.00, priceINR: 67946.40, score: 78, direction: 'BUY', cvd: 3.8, obi: 0.51, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Steady exchange utility baseline' },
    ADAUSDT:  { name: 'Cardano', symbol: 'ADA', priceUSDT: 0.35, priceINR: 33.03, score: 71, direction: 'BUY', cvd: 2.1, obi: 0.52, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Low volatility consolidation breakout' },
    XRPUSDT:  { name: 'Ripple', symbol: 'XRP', priceUSDT: 1.28, priceINR: 120.79, score: 62, direction: 'NEUTRAL', cvd: -1.5, obi: 0.48, source: 'DETERMINISTIC_FALLBACK', reasoning: 'Resistance retest with balanced orderbook' }
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
      console.log(`[Cloud Engine] Restored state: Cash=₹${Math.round(State.portfolio.cashINR)}, Active Positions=${Object.keys(State.activePositions).length}`);
    } else {
      persistState();
    }
  } catch (err) {
    console.error('[Cloud Engine] Error reading state:', err.message);
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
    console.error('[Cloud Engine] Error saving state:', err.message);
  }
}

const BINANCE_HOSTS = [
  'https://api.binance.com',
  'https://data-api.binance.vision',
  'https://api1.binance.com',
  'https://api3.binance.com'
];
let currentHostIndex = 0;

async function syncBinanceUsdtInrRate() {
  const host = BINANCE_HOSTS[currentHostIndex];
  try {
    const res = await fetch(`${host}/api/v3/ticker/price?symbol=USDTINR`, {
      headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }
    });
    if (res.ok) {
      const data = await res.json();
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
  const host = BINANCE_HOSTS[currentHostIndex];
  try {
    const res = await fetch(`${host}/api/v3/depth?symbol=${symbol}&limit=20`, {
      headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }
    });
    if (res.ok) {
      const data = await res.json();
      let bidVol = 0;
      let askVol = 0;
      (data.bids || []).forEach(b => { bidVol += parseFloat(b[1]) || 0; });
      (data.asks || []).forEach(a => { askVol += parseFloat(a[1]) || 0; });
      const total = bidVol + askVol;
      return total > 0 ? bidVol / total : 0.50;
    }
  } catch (e) {
    currentHostIndex = (currentHostIndex + 1) % BINANCE_HOSTS.length;
  }
  return null;
}

async function fetchLiveBinancePrices() {
  const host = BINANCE_HOSTS[currentHostIndex];
  try {
    const res = await fetch(`${host}/api/v3/ticker/price`, {
      headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }
    });
    if (res.ok) {
      const allTickers = await res.json();
      const tickerMap = {};
      allTickers.forEach(t => { tickerMap[t.symbol] = parseFloat(t.price); });

      let anyUpdated = false;
      Object.keys(State.universe).forEach(sym => {
        if (tickerMap[sym]) {
          const usdt = tickerMap[sym];
          State.universe[sym].priceUSDT = usdt;
          State.universe[sym].priceINR = Math.round(usdt * State.usdtToInrRate * 100) / 100;
          anyUpdated = true;
        }
      });

      if (anyUpdated) {
        State.lastMarketSyncTime = new Date().toLocaleTimeString('en-IN', {
          hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata'
        });
        await monitorOpenPositionsHealth();
      }
    }
  } catch (e) {
    currentHostIndex = (currentHostIndex + 1) % BINANCE_HOSTS.length;
  }
}
setInterval(fetchLiveBinancePrices, 2500);

async function monitorOpenPositionsHealth() {
  const activeKeys = Object.keys(State.activePositions);
  if (activeKeys.length === 0) return;

  for (const pairKey of activeKeys) {
    const pos = State.activePositions[pairKey];
    if (!pos) continue;

    const livePrice = State.universe[pairKey]?.priceINR || pos.entryPrice;
    const gainPct = ((livePrice - pos.entryPrice) / pos.entryPrice) * 100;

    if (gainPct > (pos.peakGainPct || 0)) {
      pos.peakGainPct = gainPct;
    }

    // Trailing Stop Ratchet: Once up +1.5%, advance Stop-Loss to Break-Even (+0.2%)
    if (gainPct >= 1.5 && !pos.isTrailingActive) {
      pos.isTrailingActive = true;
      pos.sl = pos.entryPrice * 1.002;
      console.log(`[Trailing Ratchet Triggered] ${pos.pairSymbol} locked at Break-Even (+0.2%)`);
    }

    // Live Order Book Imbalance evaluation for active trade
    const realObi = await fetchRealOrderBookDepth(pairKey);
    let decayRisk = 15;

    if (realObi !== null) {
      if (realObi < 0.45) decayRisk += 35;
      else if (realObi < 0.50) decayRisk += 20;
    }

    // Drawdown from peak gain
    const dropFromPeak = (pos.peakGainPct || 0) - gainPct;
    if (dropFromPeak >= 0.8) decayRisk += 30;
    else if (dropFromPeak >= 0.4) decayRisk += 15;

    // Price slipping into negative territory
    if (gainPct < -0.3) decayRisk += 25;

    pos.decayRisk = Math.min(95, decayRisk);
    pos.predictiveStatus = pos.decayRisk >= 65 ? 'EXITING' : (pos.decayRisk >= 40 ? 'CAUTION' : 'SAFE');

    // 1. Hard Stop-Loss Hit
    if (livePrice <= pos.sl) {
      settlePosition(pairKey, 'HARD SL', livePrice, 'Hit designated Stop-Loss boundary');
      continue;
    }

    // 2. Hard Take-Profit Hit
    if (livePrice >= pos.tp1) {
      settlePosition(pairKey, 'HARD TP', livePrice, 'Target profit attained (+3.6%)');
      continue;
    }

    // 3. Early Loss Mitigation Exit (Cuts trade at -0.5% if order book deteriorates >= 70%)
    if (gainPct <= -0.5 && pos.decayRisk >= 70) {
      settlePosition(pairKey, 'PREDICTIVE SL', livePrice, `Loss mitigation cut: Decay ${pos.decayRisk}% (Closed at ${gainPct.toFixed(2)}%)`);
      continue;
    }

    // 4. Predictive Take-Profit Exit (Locks in positive gains if order flow flips >= 65%)
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

async function runLLMQuantitativeAnalysis() {
  if (!State.analysisActive) return;

  const startTime = Date.now();
  State.llmTelemetry.promptExecutionCount++;
  State.llmTelemetry.lastAnalysisTime = new Date().toLocaleTimeString('en-IN', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata'
  });

  // 1. Gather live 24h ticker performance from Binance
  const host = BINANCE_HOSTS[currentHostIndex];
  let ticker24hMap = {};
  try {
    const res = await fetch(`${host}/api/v3/ticker/24hr`, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY } });
    if (res.ok) {
      const data = await res.json();
      data.forEach(item => {
        ticker24hMap[item.symbol] = {
          priceChangePercent: parseFloat(item.priceChangePercent) || 0,
          quoteVolume: parseFloat(item.quoteVolume) || 0
        };
      });
    }
  } catch (e) {}

  // 2. Aggregate telemetry across all assets
  const marketTelemetry = [];
  for (const sym of Object.keys(State.universe)) {
    const item = State.universe[sym];
    const liveObi = await fetchRealOrderBookDepth(sym);
    const obi = (liveObi !== null && liveObi !== undefined) ? liveObi : (item.obi || 0.50);
    const ticker = ticker24hMap[sym] || { priceChangePercent: 0, quoteVolume: 50000000 };

    item.obi = obi;
    marketTelemetry.push({
      symbol: sym,
      name: item.name,
      priceUSDT: item.priceUSDT,
      orderBookImbalance: Math.round(obi * 100) / 100,
      priceChange24hPct: Math.round(ticker.priceChangePercent * 10) / 10,
      quoteVolume24h: Math.round(ticker.quoteVolume / 1000000)
    });
  }

  // 3. Query Google Gemini AI API if configured
  let geminiSuccess = false;
  if (GEMINI_API_KEY) {
    try {
      const promptText = `You are an institutional quant fund manager analyzing real-time crypto microstructure.
Analyze this live Binance market telemetry:
${JSON.stringify(marketTelemetry, null, 2)}

Active Open Positions in Portfolio: ${JSON.stringify(Object.keys(State.activePositions))}

Provide your quantitative market regime evaluation and per-asset scoring in strictly structured JSON matching this format:
{
  "marketRegime": "MULTI_EXPANSION",
  "reasoning": "Brief summary of market breadth, taker delta, and order depth",
  "ratings": {
    "SOLUSDT": {
      "score": 88,
      "direction": "STRONG BUY",
      "reasoning": "Strong bid depth dominance"
    },
    "SUIUSDT": {
      "score": 58,
      "direction": "NEUTRAL",
      "reasoning": "Ask depth resistance"
    }
  }
}`;

      // Current supported Google Gemini models
      const geminiModels = ['gemini-3.8-flash', 'gemini-3.6-flash', 'gemini-3-flash-preview', 'gemini-2.0-flash'];
      for (const model of geminiModels) {
        if (geminiSuccess) break;
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 8500);

          const geminiRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${GEMINI_API_KEY}`, {
            method: 'POST',
            signal: controller.signal,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: promptText }] }],
              generationConfig: {
                temperature: 0.1,
                maxOutputTokens: 1000,
                responseMimeType: "application/json"
              }
            })
          });
          clearTimeout(timeout);

          if (geminiRes.ok) {
            const geminiData = await geminiRes.json();
            let rawText = geminiData.candidates?.[0]?.content?.parts?.[0]?.text || '';
            rawText = rawText.replace(/```json/gi, '').replace(/```/g, '').trim();

            // Resilient JSON extractor: locate the outer enclosing braces
            const firstBrace = rawText.indexOf('{');
            const lastBrace = rawText.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
              rawText = rawText.substring(firstBrace, lastBrace + 1);
            }

            if (rawText) {
              const parsed = JSON.parse(rawText);
              if (parsed.marketRegime) State.marketRegime = parsed.marketRegime;
              if (parsed.reasoning) State.llmTelemetry.regimeReasoning = `Gemini AI (${model}): ${parsed.reasoning}`;
              State.llmTelemetry.model = `${model} (Live 24/7 Cloud API)`;
              State.llmTelemetry.source = 'GEMINI_3_8_FLASH';
              State.llmTelemetry.lastError = null;

              if (parsed.ratings && typeof parsed.ratings === 'object') {
                Object.keys(parsed.ratings).forEach(sym => {
                  if (State.universe[sym] && parsed.ratings[sym]) {
                    const r = parsed.ratings[sym];
                    if (typeof r.score === 'number') State.universe[sym].score = Math.min(98, Math.max(50, Math.round(r.score)));
                    if (r.direction) State.universe[sym].direction = r.direction;
                    if (r.reasoning) State.universe[sym].reasoning = r.reasoning;
                    State.universe[sym].source = 'GEMINI_3_8_FLASH';
                  }
                });
              }
              geminiSuccess = true;
              console.log(`[Gemini AI Live Evaluation] Success via ${model}: ${parsed.reasoning}`);
            }
          } else {
            const errBody = await geminiRes.text();
            State.llmTelemetry.lastError = `HTTP ${geminiRes.status} on ${model}: ${errBody.slice(0, 100)}`;
          }
        } catch (mErr) {
          State.llmTelemetry.lastError = mErr.message;
        }
      }
    } catch (err) {
      console.error('[Gemini AI] Call error:', err.message);
      State.llmTelemetry.lastError = err.message;
    }
  }

  // 4. Deterministic Microstructure Fallback if Gemini is not configured or fails
  if (!geminiSuccess) {
    for (const m of marketTelemetry) {
      const sym = m.symbol;
      const obiScore = Math.max(0, Math.min(40, (m.orderBookImbalance - 0.40) * 133));
      const momScore = Math.max(0, Math.min(35, (m.priceChange24hPct + 3) * 3.5));
      const volScore = Math.min(25, Math.log10(Math.max(1, m.quoteVolume24h)) * 8);
      const computed = Math.min(98, Math.max(50, Math.round(obiScore + momScore + volScore)));

      State.universe[sym].score = computed;
      State.universe[sym].source = 'DETERMINISTIC_FALLBACK';

      if (computed >= 82) {
        State.universe[sym].direction = 'STRONG BUY';
        State.universe[sym].reasoning = `High order book buyer skew (${Math.round(m.orderBookImbalance * 100)}%) with 24h momentum +${m.priceChange24hPct}%`;
      } else if (computed >= 72) {
        State.universe[sym].direction = 'BUY';
        State.universe[sym].reasoning = `Healthy accumulation baseline (OBI: ${Math.round(m.orderBookImbalance * 100)}%)`;
      } else {
        State.universe[sym].direction = 'NEUTRAL';
        State.universe[sym].reasoning = `Balanced or ask-skewed book (OBI: ${Math.round(m.orderBookImbalance * 100)}%)`;
      }
    }

    const qualified = Object.keys(State.universe).filter(k => State.universe[k].score >= 80);
    State.marketRegime = qualified.length >= 2 ? 'MULTI_EXPANSION' : 'TIGHT_DEFENSIVE';
    State.llmTelemetry.model = 'Deterministic Microstructure Engine';
    State.llmTelemetry.source = 'DETERMINISTIC_FALLBACK';
    State.llmTelemetry.regimeReasoning = qualified.length >= 2
      ? `Multi-Basket Expansion: Multiple assets (${qualified.slice(0, 3).map(k => State.universe[k].symbol).join(', ')}) exhibit verified buyer dominance.`
      : 'Tight Defensive: Narrow market breadth. Capital preserved for top setups.';
  }

  State.llmTelemetry.lastLatencyMs = Date.now() - startTime;

  // 5. Autonomous Trade Execution based on AI Conviction
  if (State.autopilotEnabled && State.portfolio.cashINR >= 2500) {
    const currentOpenCount = Object.keys(State.activePositions).length;
    const maxAllowed = State.marketRegime === 'MULTI_EXPANSION' ? State.maxConcurrentPositions : 1;

    if (currentOpenCount < maxAllowed) {
      const availableCandidates = Object.keys(State.universe)
        .filter(key => !State.activePositions[key] && State.universe[key].priceINR > 0)
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

          console.log(`[Autopilot Entry Executed] ${candidate.symbol} @ ₹${livePrice.toLocaleString('en-IN')} | Conviction Score: ${candidate.score}/100 | Capital: ₹${targetAllocation}`);
          persistState();
        }
      }
    }
  }

  // Push authoritative state to all connected devices
  broadcastState();
}
setInterval(runLLMQuantitativeAnalysis, 20000);

function broadcastState() {
  const payload = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    autopilotEnabled: State.autopilotEnabled,
    marketRegime: State.marketRegime,
    llmTelemetry: State.llmTelemetry,
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
    llmTelemetry: State.llmTelemetry,
    usdtToInrRate: State.usdtToInrRate,
    syncTime: State.lastMarketSyncTime
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

app.get('/', (req, res) => {
  res.json({
    service: "Sisir's Dedicated 24/7 Cloud Quant Engine",
    status: 'online',
    binanceFeed: 'live',
    geminiConfigured: Boolean(GEMINI_API_KEY),
    geminiSource: State.llmTelemetry.source,
    lastAiAnalysisTime: State.llmTelemetry.lastAnalysisTime,
    lastAiLatencyMs: State.llmTelemetry.lastLatencyMs,
    autopilotEnabled: State.autopilotEnabled,
    cashINR: State.portfolio.cashINR,
    activePositionsCount: Object.keys(State.activePositions).length,
    activePositions: Object.keys(State.activePositions).map(k => `${State.activePositions[k].pairSymbol} (₹${Math.round(State.activePositions[k].notionalINR)}, Risk: ${State.activePositions[k].decayRisk}%)`),
    realizedPnL: State.portfolio.realizedPnL
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    geminiActive: State.llmTelemetry.source === 'GEMINI_3_8_FLASH',
    geminiConfigured: Boolean(GEMINI_API_KEY),
    lastMarketSync: State.lastMarketSyncTime,
    lastAiAnalysis: State.llmTelemetry.lastAnalysisTime,
    autopilot: State.autopilotEnabled,
    positions: Object.keys(State.activePositions).length
  });
});

// Dedicated Diagnostic Route to verify Gemini AI connectivity with Binance data
app.get('/api/llm/test', async (req, res) => {
  const result = {
    geminiApiKeyPresent: Boolean(GEMINI_API_KEY),
    currentTelemetrySource: State.llmTelemetry.source,
    lastLatencyMs: State.llmTelemetry.lastLatencyMs,
    lastError: State.llmTelemetry.lastError,
    sampleAsset: {
      symbol: 'SUIUSDT',
      score: State.universe.SUIUSDT?.score,
      direction: State.universe.SUIUSDT?.direction,
      obi: State.universe.SUIUSDT?.obi,
      reasoning: State.universe.SUIUSDT?.reasoning,
      source: State.universe.SUIUSDT?.source
    }
  };

  if (!GEMINI_API_KEY) {
    result.diagnosis = 'GEMINI_API_KEY environment variable is missing on Render. Running on Microstructure Fallback.';
    return res.json(result);
  }

  try {
    const t0 = Date.now();
    const testModels = ['gemini-3.8-flash', 'gemini-3.6-flash', 'gemini-3-flash-preview', 'gemini-2.0-flash'];
    let connected = false;

    for (const m of testModels) {
      if (connected) break;
      const testRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${m}:generateContent?key=${GEMINI_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: 'Respond with exactly: {"status":"OK"}' }] }]
        })
      });

      result.geminiHttpStatus = testRes.status;
      result.geminiTestLatencyMs = Date.now() - t0;
      result.modelTested = m;

      if (testRes.ok) {
        result.geminiActive = true;
        result.diagnosis = `Gemini API verified and operational 24/7 using ${m}.`;
        connected = true;
      } else {
        result.geminiActive = false;
        result.geminiErrorDetails = await testRes.text();
      }
    }

    if (!connected) {
      result.diagnosis = 'Gemini API returned an error status across tested models.';
    }
  } catch (err) {
    result.geminiActive = false;
    result.geminiException = err.message;
    result.diagnosis = 'Network connection to Google Gemini API failed.';
  }

  res.json(result);
});

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    universe: State.universe,
    autopilotEnabled: State.autopilotEnabled,
    marketRegime: State.marketRegime,
    llmTelemetry: State.llmTelemetry,
    tradeLogs: State.tradeLogs.slice(0, 50)
  });
});

app.post('/api/autopilot/toggle', (req, res) => {
  State.autopilotEnabled = !State.autopilotEnabled;
  if (State.autopilotEnabled) runLLMQuantitativeAnalysis();
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
    llmTelemetry: State.llmTelemetry,
    autopilotEnabled: State.autopilotEnabled,
    tradeLogs: State.tradeLogs.slice(0, 50),
    lastMarketSync: State.lastMarketSyncTime
  }));

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(data.enabled);
        if (State.autopilotEnabled) runLLMQuantitativeAnalysis();
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
  console.log(` Sisir's Cloud Quant Engine v10.5 RUNNING on port ${PORT}`);
  console.log(` Real-time Binance Depth & Health: Active every 2.5s`);
  console.log(` Gemini AI Market Evaluation Loop: Active every 20s`);
  console.log(` Diagnostic Endpoint Available: /api/llm/test`);
  console.log(`================================================================`);
});
