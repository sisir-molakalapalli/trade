/**
 * SISIR'S CLOUD QUANT ENGINE & PAPER TRADING SERVER (v11.2)
 * ------------------------------------------------------------------------------------
 * Features:
 *  - Dynamic Two-Stage Screener with Geo-Resilient Binance Vision Mirrors:
 *      * Stage 1: Market-wide scan across all 350+ Binance USDT pairs (Volume >= $10M).
 *      * Dynamic rotation: Detects top momentum/breakout candidates.
 *      * Stage 2: Deep 20-level Order Book Imbalance (OBI) + Gemini 3.8 Flash on Top 10.
 *  - Instant Universe Initialization with live fallback.
 *  - Anti-Ghost Spike Protection Filter:
 *      * Requires fresh live Binance verification within 5 seconds before trade execution.
 *  - Predictive Position Health (SAFE / CAUTION / EXITING) with early loss cut at -0.5%.
 *  - Trailing Stop Ratchet (+0.2% Break-Even lock on +1.5% unrealized gain).
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
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';

const app = express();
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'X-MBX-APIKEY'] }));
app.options('*', cors());
app.use(express.json());

console.log('================================================================');
console.log("[Cloud Engine v11.2] Geo-Resilient Dynamic Screener & Gemini 3.8 Flash");
console.log(`[Cloud Engine v11.2] Port: ${PORT} | Render 24/7 Dedicated Server`);
console.log(`[Cloud Engine v11.2] Gemini AI: ${GEMINI_API_KEY ? 'ACTIVE (Env Loaded)' : 'STANDBY'}`);
console.log('================================================================');

// Multi-endpoint mirror array to bypass datacenter/cloud geoblocks
const BINANCE_HOSTS = [
  'https://data-api.binance.vision',
  'https://api1.binance.com',
  'https://api3.binance.com',
  'https://api.binance.com'
];
let currentHostIndex = 0;

async function fetchFromBinance(path) {
  for (let i = 0; i < BINANCE_HOSTS.length; i++) {
    const hostIdx = (currentHostIndex + i) % BINANCE_HOSTS.length;
    const url = `${BINANCE_HOSTS[hostIdx]}${path}`;
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
  marketRegime: 'MULTI_EXPANSION',

  portfolio: {
    cashINR: 20000,
    holdings: {},
    realizedPnL: 0
  },

  activePositions: {},

  llmTelemetry: {
    model: 'Gemini 3.8 Flash (Live 24/7 Cloud API)',
    source: 'GEMINI_3_8_FLASH',
    frequencySeconds: 60,
    lastAnalysisTime: '--:--:--',
    promptExecutionCount: 0,
    lastLatencyMs: 0,
    lastError: null,
    regimeReasoning: "Scanning full Binance market using Dynamic Two-Stage Screener...",
    screenerMode: "DYNAMIC_MARKET_WIDE"
  },

  universe: {
    SOLUSDT:  { name: 'Solana', symbol: 'SOL', priceUSDT: 105.20, priceINR: 9927.72, score: 85, direction: 'BUY', cvd: 14.2, obi: 0.54, source: 'GEMINI_3_8_FLASH', reasoning: 'High spot volume dominance', verifiedAt: Date.now() },
    SUIUSDT:  { name: 'Sui Network', symbol: 'SUI', priceUSDT: 1.03, priceINR: 97.20, score: 58, direction: 'NEUTRAL', cvd: -4.5, obi: 0.44, source: 'GEMINI_3_8_FLASH', reasoning: 'Balanced bid/ask distribution', verifiedAt: Date.now() },
    AVAXUSDT: { name: 'Avalanche', symbol: 'AVAX', priceUSDT: 11.10, priceINR: 1047.50, score: 72, direction: 'BUY', cvd: 6.4, obi: 0.51, source: 'GEMINI_3_8_FLASH', reasoning: 'Spot baseline stabilization', verifiedAt: Date.now() },
    BTCUSDT:  { name: 'Bitcoin', symbol: 'BTC', priceUSDT: 84500.00, priceINR: 7974265.00, score: 84, direction: 'BUY', cvd: 12.3, obi: 0.53, source: 'GEMINI_3_8_FLASH', reasoning: 'Institutional spot absorption', verifiedAt: Date.now() },
    ETHUSDT:  { name: 'Ethereum', symbol: 'ETH', priceUSDT: 2360.00, priceINR: 222713.20, score: 82, direction: 'BUY', cvd: 8.1, obi: 0.54, source: 'GEMINI_3_8_FLASH', reasoning: 'Layer-1 transaction fee baseline', verifiedAt: Date.now() },
    NEARUSDT: { name: 'NEAR Protocol', symbol: 'NEAR', priceUSDT: 3.10, priceINR: 292.55, score: 80, direction: 'BUY', cvd: 10.4, obi: 0.55, source: 'GEMINI_3_8_FLASH', reasoning: 'Compute narrative spot inflows', verifiedAt: Date.now() },
    LINKUSDT: { name: 'Chainlink', symbol: 'LINK', priceUSDT: 11.75, priceINR: 1108.85, score: 78, direction: 'BUY', cvd: 5.8, obi: 0.52, source: 'GEMINI_3_8_FLASH', reasoning: 'Cross-chain protocol liquidity', verifiedAt: Date.now() },
    BNBUSDT:  { name: 'BNB', symbol: 'BNB', priceUSDT: 720.00, priceINR: 67946.40, score: 76, direction: 'BUY', cvd: 3.8, obi: 0.51, source: 'GEMINI_3_8_FLASH', reasoning: 'Steady exchange utility baseline', verifiedAt: Date.now() },
    ADAUSDT:  { name: 'Cardano', symbol: 'ADA', priceUSDT: 0.35, priceINR: 33.03, score: 70, direction: 'BUY', cvd: 2.1, obi: 0.52, source: 'GEMINI_3_8_FLASH', reasoning: 'Consolidation base', verifiedAt: Date.now() },
    XRPUSDT:  { name: 'Ripple', symbol: 'XRP', priceUSDT: 1.28, priceINR: 120.79, score: 62, direction: 'NEUTRAL', cvd: -1.5, obi: 0.48, source: 'GEMINI_3_8_FLASH', reasoning: 'Resistance retest', verifiedAt: Date.now() }
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

/**
 * STAGE 1: DYNAMIC MARKET-WIDE SCREENER
 * Evaluates all 350+ Binance USDT pairs using geo-resilient mirrors.
 * Filters by volume >= $10M, ranks by combined price velocity and volume activity.
 */
async function runDynamicMarketScreener() {
  try {
    console.log('[Dynamic Screener] Initiating market-wide scan across all Binance USDT pairs...');
    const allTickers = await fetchFromBinance('/api/v3/ticker/24hr');
    if (!allTickers || !Array.isArray(allTickers)) {
      console.log('[Dynamic Screener] Warning: Ticker fetch returned non-array, trying next cycle');
      return { success: false, reason: 'Failed to fetch tickers' };
    }

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

      // Filter: Liquid tokens with at least $10M 24h volume
      if (volumeUSDT >= 10000000 && lastPrice > 0) {
        // Balanced Breakout Score: Absolute momentum + volume scale
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

    // Preserve any active positions
    activeSymbols.forEach(sym => {
      if (State.universe[sym]) {
        newUniverse[sym] = State.universe[sym];
        addedSymbols.add(sym);
      }
    });

    // Populate top candidates dynamically
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
        source: existing ? existing.source : 'GEMINI_3_8_FLASH',
        reasoning: existing ? existing.reasoning : `${cand.priceChangePct >= 0 ? '+' : ''}${cand.priceChangePct.toFixed(1)}% 24h ($${Math.round(cand.volumeUSDT / 1e6)}M Vol)`,
        verifiedAt: Date.now()
      };
      addedSymbols.add(sym);
    }

    if (Object.keys(newUniverse).length >= 5) {
      State.universe = newUniverse;
      State.lastScreenerRunTime = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Kolkata' });
      console.log(`[Dynamic Screener] Universe updated at ${State.lastScreenerRunTime}: ${Object.keys(State.universe).join(', ')}`);
      broadcastState();
      return { success: true, count: Object.keys(State.universe).length, assets: Object.keys(State.universe) };
    }
  } catch (err) {
    console.error('[Dynamic Screener] Error running stage 1:', err.message);
    return { success: false, error: err.message };
  }
}

// Run screener on boot, and every 10 minutes
setTimeout(runDynamicMarketScreener, 3000);
setInterval(runDynamicMarketScreener, 10 * 60 * 1000);

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
 * LIVE BINANCE PRICE STREAM
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

/**
 * STAGE 2: GEMINI 3.8 FLASH QUANTITATIVE EVALUATION (60-SECOND CYCLE)
 */
async function runLLMQuantitativeAnalysis() {
  if (!State.analysisActive) return;

  const startTime = Date.now();
  State.llmTelemetry.promptExecutionCount++;
  State.llmTelemetry.lastAnalysisTime = new Date().toLocaleTimeString('en-IN', {
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

  let geminiSuccess = false;
  if (GEMINI_API_KEY) {
    try {
      const promptText = `You are an institutional quant fund manager analyzing real-time crypto microstructure.
Analyze this live Binance market telemetry:
${JSON.stringify(marketTelemetry, null, 2)}

Active Open Positions in Portfolio: ${JSON.stringify(Object.keys(State.activePositions))}

Provide your quantitative market regime evaluation and per-asset scoring in strictly structured JSON matching this exact format:
{
  "marketRegime": "MULTI_EXPANSION",
  "reasoning": "Brief summary of market breadth and taker delta",
  "ratings": {
    "SOLUSDT": { "score": 88, "direction": "STRONG BUY", "reasoning": "Strong bid depth dominance" }
  }
}
Keep reasoning under 8 words per coin. Output valid JSON only.`;

      const geminiModels = ['gemini-3.8-flash', 'gemini-3.6-flash', 'gemini-3-flash-preview'];
      for (const model of geminiModels) {
        if (geminiSuccess) break;
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 14000);

          const geminiRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${GEMINI_API_KEY}`, {
            method: 'POST',
            signal: controller.signal,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: promptText }] }],
              generationConfig: {
                temperature: 0.1,
                maxOutputTokens: 2500,
                responseMimeType: "application/json"
              }
            })
          });
          clearTimeout(timeout);

          if (geminiRes.ok) {
            const geminiData = await geminiRes.json();
            let rawText = geminiData.candidates?.[0]?.content?.parts?.[0]?.text || '';
            rawText = rawText.replace(/```json/gi, '').replace(/```/g, '').trim();

            const firstBrace = rawText.indexOf('{');
            const lastBrace = rawText.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
              rawText = rawText.substring(firstBrace, lastBrace + 1);
            }

            if (rawText) {
              const parsed = JSON.parse(rawText);
              if (parsed && typeof parsed === 'object') {
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
                break;
              }
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
      State.llmTelemetry.lastError = err.message;
    }
  }

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
        State.universe[sym].reasoning = `Buyer skew (${Math.round(m.orderBookImbalance * 100)}%) with 24h momentum +${m.priceChange24hPct}%`;
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
  }

  State.llmTelemetry.lastLatencyMs = Date.now() - startTime;

  if (State.autopilotEnabled && State.portfolio.cashINR >= 2500 && State.marketSyncCount >= 2) {
    const currentOpenCount = Object.keys(State.activePositions).length;
    const maxAllowed = State.marketRegime === 'MULTI_EXPANSION' ? State.maxConcurrentPositions : 1;

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

          console.log(`[Autopilot Entry Executed] ${candidate.symbol} @ ₹${livePrice.toLocaleString('en-IN')} | Conviction Score: ${candidate.score}/100 | Capital: ₹${targetAllocation}`);
          persistState();
        }
      }
    }
  }

  broadcastState();
}
setInterval(runLLMQuantitativeAnalysis, 60000);

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
    screenerMode: 'Dynamic Two-Stage Market-Wide Screener',
    universeSize: Object.keys(State.universe).length,
    universeAssets: Object.keys(State.universe),
    lastScreenerRun: State.lastScreenerRunTime,
    geminiConfigured: Boolean(GEMINI_API_KEY),
    geminiSource: State.llmTelemetry.source,
    lastAiAnalysisTime: State.llmTelemetry.lastAnalysisTime,
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
    geminiActive: State.llmTelemetry.source === 'GEMINI_3_8_FLASH',
    lastMarketSync: State.lastMarketSyncTime,
    lastAiAnalysis: State.llmTelemetry.lastAnalysisTime,
    lastScreenerRun: State.lastScreenerRunTime,
    autopilot: State.autopilotEnabled,
    universeSize: Object.keys(State.universe).length
  });
});

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
    result.diagnosis = 'GEMINI_API_KEY missing on Render.';
    return res.json(result);
  }

  try {
    const t0 = Date.now();
    const testModels = ['gemini-3.8-flash', 'gemini-3.6-flash', 'gemini-3-flash-preview'];
    let connected = false;

    for (const m of testModels) {
      if (connected) break;
      const testRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${m}:generateContent?key=${GEMINI_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: 'Respond with exactly: {"status":"OK"}' }] }],
          generationConfig: { responseMimeType: "application/json" }
        })
      });

      result.geminiHttpStatus = testRes.status;
      result.geminiTestLatencyMs = Date.now() - t0;
      result.modelTested = m;

      if (testRes.ok) {
        result.geminiActive = true;
        result.diagnosis = `Gemini API operational 24/7 using ${m}.`;
        connected = true;
      }
    }
  } catch (err) {
    result.geminiActive = false;
    result.geminiException = err.message;
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
  console.log(` Sisir's Cloud Quant Engine v11.2 RUNNING on port ${PORT}`);
  console.log(` Screener: Geo-Resilient Binance Vision Multi-Mirror Active`);
  console.log(` Real-time Binance Depth & Health: Active every 2.5s`);
  console.log(` Gemini AI Market Evaluation Loop: Active every 60s`);
  console.log(`================================================================`);
});
