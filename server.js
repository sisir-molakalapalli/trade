/**
 * SISIR'S CLOUD QUANT ENGINE & PAPER TRADING SERVER
 * -------------------------------------------------------------
 * Runs 24/7 on any cloud instance (Render, Railway, Fly.io, etc.).
 * Connects directly to Binance streams, ranks crypto universe,
 * manages paper positions, and handles SL/TP settlements.
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');

const app = express();
app.use(cors());
app.use(express.json());

// Immediate console log to guarantee output in Render logs
console.log('=======================================================');
console.log('[Cloud Engine] Initializing Node process on Render...');
console.log(`[Cloud Engine] Environment Port: ${process.env.PORT || 'default 8080'}`);
console.log('=======================================================');

const State = {
  autopilotEnabled: true,
  analysisActive: true,
  usdtToInrRate: 90.50,
  
  portfolio: {
    cashINR: 300000,
    holdings: {},
    realizedPnL: 0
  },

  activePosition: null,

  universe: {
    BTCUSDT:  { name: 'Bitcoin', symbol: 'BTC', priceINR: 6428400, score: 86, direction: 'BUY', cvd: 14.2, obi: 0.54, volSpike: '1.2x' },
    ETHUSDT:  { name: 'Ethereum', symbol: 'ETH', priceINR: 284300, score: 88, direction: 'BUY', cvd: 8.6, obi: 0.58, volSpike: '1.6x' },
    SOLUSDT:  { name: 'Solana', symbol: 'SOL', priceINR: 18420, score: 92, direction: 'STRONG BUY', cvd: 22.4, obi: 0.61, volSpike: '2.4x' },
    BNBUSDT:  { name: 'BNB', symbol: 'BNB', priceINR: 52800, score: 79, direction: 'BUY', cvd: 4.1, obi: 0.52, volSpike: '1.1x' },
    XRPUSDT:  { name: 'Ripple', symbol: 'XRP', priceINR: 54.20, score: 62, direction: 'NEUTRAL', cvd: -1.8, obi: 0.48, volSpike: '0.9x' },
    ADAUSDT:  { name: 'Cardano', symbol: 'ADA', priceINR: 32.80, score: 71, direction: 'BUY', cvd: 2.3, obi: 0.53, volSpike: '1.0x' },
    AVAXUSDT: { name: 'Avalanche', symbol: 'AVAX', priceINR: 2540, score: 89, direction: 'STRONG BUY', cvd: 16.5, obi: 0.59, volSpike: '2.1x' },
    NEARUSDT: { name: 'NEAR Protocol', symbol: 'NEAR', priceINR: 448.00, score: 87, direction: 'BUY', cvd: 11.2, obi: 0.56, volSpike: '1.8x' },
    SUIUSDT:  { name: 'Sui Network', symbol: 'SUI', priceINR: 192.40, score: 94, direction: 'STRONG BUY', cvd: 28.9, obi: 0.65, volSpike: '3.2x' },
    LINKUSDT: { name: 'Chainlink', symbol: 'LINK', priceINR: 1180, score: 81, direction: 'BUY', cvd: 5.7, obi: 0.53, volSpike: '1.3x' }
  },

  tradeLogs: []
};

// Auto-load state from disk
function loadPersistedState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      if (data.portfolio) State.portfolio = data.portfolio;
      if (data.activePosition !== undefined) State.activePosition = data.activePosition;
      if (data.autopilotEnabled !== undefined) State.autopilotEnabled = data.autopilotEnabled;
      if (data.tradeLogs) State.tradeLogs = data.tradeLogs;
      console.log('[Cloud Engine] Restored portfolio state successfully.');
    }
  } catch (err) {
    console.error('[Cloud Engine] Failed loading state:', err.message);
  }
}

// Auto-save state to disk
function persistState() {
  try {
    const payload = {
      portfolio: State.portfolio,
      activePosition: State.activePosition,
      autopilotEnabled: State.autopilotEnabled,
      tradeLogs: State.tradeLogs.slice(-50),
      timestamp: Date.now()
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(payload, null, 2));
  } catch (err) {
    console.error('[Cloud Engine] Error persisting state:', err.message);
  }
}

let binanceSocket = null;

function connectBinanceStream() {
  const streams = Object.keys(State.universe)
    .map(symbol => `${symbol.toLowerCase()}@aggTrade/${symbol.toLowerCase()}@ticker`)
    .join('/');
  
  const wsUrl = `wss://stream.binance.com:9443/stream?streams=${streams}`;
  console.log('[Binance Stream] Connecting to multi-stream socket...');

  try {
    binanceSocket = new WebSocket(wsUrl);

    binanceSocket.on('open', () => {
      console.log('[Binance Stream] Connected. Live market feed active 24/7.');
    });

    binanceSocket.on('message', (raw) => {
      if (!State.analysisActive) return;
      try {
        const payload = JSON.parse(raw);
        const stream = payload.stream;
        const data = payload.data;

        if (!data) return;

        if (data.e === 'aggTrade') {
          const symbol = data.s;
          const usdtPrice = parseFloat(data.p);
          const inrPrice = usdtPrice * State.usdtToInrRate;
          const isBuyerMaker = data.m;
          const qty = parseFloat(data.q);

          if (State.universe[symbol]) {
            State.universe[symbol].priceINR = inrPrice;
            const delta = isBuyerMaker ? -qty : qty;
            State.universe[symbol].cvd += (delta * 0.1);
          }

          checkPositionExits(symbol, inrPrice);
        }
      } catch (e) {}
    });

    binanceSocket.on('error', (err) => {
      console.error('[Binance Stream] Socket error:', err.message);
    });

    binanceSocket.on('close', () => {
      console.warn('[Binance Stream] Connection lost. Reconnecting in 5s...');
      setTimeout(connectBinanceStream, 5000);
    });
  } catch (err) {
    console.error('[Binance Stream] Failed to initialize socket:', err.message);
    setTimeout(connectBinanceStream, 5000);
  }
}

function checkPositionExits(symbol, currentPrice) {
  if (!State.activePosition) return;
  const pos = State.activePosition;
  if (pos.pairKey !== symbol) return;

  if (currentPrice >= pos.tp1) {
    settlePosition('TP', currentPrice, 'Take-Profit Target Hit');
  } else if (currentPrice <= pos.sl) {
    settlePosition('SL', currentPrice, 'Stop-Loss Protected');
  }
}

function settlePosition(type, exitPrice, reason) {
  if (!State.activePosition) return;
  const pos = State.activePosition;
  const finalVal = pos.qty * exitPrice;
  const pnl = finalVal - pos.notionalINR;

  State.portfolio.cashINR += finalVal;
  State.portfolio.realizedPnL += pnl;

  delete State.portfolio.holdings[pos.pairKey];

  const logEntry = {
    id: Date.now(),
    type,
    pair: `${pos.pairSymbol}/INR`,
    entryPrice: pos.entryPrice,
    exitPrice,
    pnlINR: pnl,
    reason,
    time: new Date().toISOString()
  };

  State.tradeLogs.unshift(logEntry);
  State.activePosition = null;

  console.log(`[Trade Executed] Position closed (${type}) on ${logEntry.pair}. PnL: ₹${Math.round(pnl)}`);
  persistState();
  broadcastState();
}

// Background scheduler running every 5 seconds for radar scoring & auto-entry
setInterval(() => {
  if (!State.analysisActive) return;

  Object.keys(State.universe).forEach(sym => {
    const item = State.universe[sym];
    const scoreDrift = (Math.random() - 0.49) * 2;
    item.score = Math.min(98, Math.max(50, Math.round(item.score + scoreDrift)));
  });

  if (State.autopilotEnabled && !State.activePosition && State.portfolio.cashINR >= 15000) {
    const sorted = Object.keys(State.universe).sort((a, b) => State.universe[b].score - State.universe[a].score);
    const topKey = sorted[0];
    const candidate = State.universe[topKey];

    if (candidate && candidate.score >= 89) {
      const allocateINR = Math.min(State.portfolio.cashINR, 50000);
      const price = candidate.priceINR;
      const qty = allocateINR / price;
      const slDist = price * 0.015;

      State.activePosition = {
        pairKey: topKey,
        pairName: candidate.name,
        pairSymbol: candidate.symbol,
        entryPrice: price,
        notionalINR: allocateINR,
        qty,
        sl: price - slDist,
        tp1: price + (slDist * 2.4),
        openedAt: new Date().toISOString()
      };

      State.portfolio.cashINR -= allocateINR;
      State.portfolio.holdings[topKey] = {
        name: candidate.name,
        symbol: candidate.symbol,
        qty,
        totalCostINR: allocateINR,
        avgBuyPrice: price
      };

      console.log(`[Autopilot Action] Bought ${candidate.name} (${candidate.symbol}) for ₹${allocateINR}`);
      persistState();
      broadcastState();
    }
  }

  persistState();
}, 5000);

// Root test endpoint
app.get('/', (req, res) => {
  res.send({ status: 'online', service: "Sisir's Cloud Quant Engine", version: '1.0' });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', uptime: process.uptime(), time: new Date() });
});

app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePosition: State.activePosition,
    universe: State.universe,
    autopilotEnabled: State.autopilotEnabled,
    analysisActive: State.analysisActive,
    tradeLogs: State.tradeLogs.slice(0, 15)
  });
});

app.post('/api/autopilot/toggle', (req, res) => {
  State.autopilotEnabled = !State.autopilotEnabled;
  persistState();
  broadcastState();
  res.json({ success: true, autopilotEnabled: State.autopilotEnabled });
});

app.post('/api/analysis/toggle', (req, res) => {
  State.analysisActive = !State.analysisActive;
  persistState();
  broadcastState();
  res.json({ success: true, analysisActive: State.analysisActive });
});

app.post('/api/trade/manual-buy', (req, res) => {
  const { pairKey, amountINR } = req.body;
  if (!pairKey || !amountINR || amountINR <= 0) {
    return res.status(400).json({ error: 'Invalid trade inputs' });
  }
  if (amountINR > State.portfolio.cashINR) {
    return res.status(400).json({ error: 'Insufficient funds' });
  }

  const candidate = State.universe[pairKey];
  if (!candidate) return res.status(404).json({ error: 'Pair not supported' });

  const price = candidate.priceINR;
  const qty = amountINR / price;
  const slDist = price * 0.015;

  State.activePosition = {
    pairKey,
    pairName: candidate.name,
    pairSymbol: candidate.symbol,
    entryPrice: price,
    notionalINR: amountINR,
    qty,
    sl: price - slDist,
    tp1: price + (slDist * 2.4),
    openedAt: new Date().toISOString()
  };

  State.portfolio.cashINR -= amountINR;
  State.portfolio.holdings[pairKey] = {
    name: candidate.name,
    symbol: candidate.symbol,
    qty,
    totalCostINR: amountINR,
    avgBuyPrice: price
  };

  persistState();
  broadcastState();
  res.json({ success: true, position: State.activePosition });
});

app.post('/api/trade/close-all', (req, res) => {
  let liquidated = 0;
  Object.keys(State.portfolio.holdings).forEach(k => {
    const h = State.portfolio.holdings[k];
    const p = State.universe[k]?.priceINR || h.avgBuyPrice;
    liquidated += h.qty * p;
  });

  State.portfolio.cashINR += liquidated;
  State.portfolio.holdings = {};
  State.activePosition = null;

  persistState();
  broadcastState();
  res.json({ success: true, cashINR: State.portfolio.cashINR });
});

app.post('/api/deposit', (req, res) => {
  const { amountINR } = req.body;
  if (!amountINR || amountINR <= 0) return res.status(400).json({ error: 'Invalid deposit' });

  State.portfolio.cashINR += Number(amountINR);
  persistState();
  broadcastState();
  res.json({ success: true, cashINR: State.portfolio.cashINR });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcastState() {
  const payload = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePosition: State.activePosition,
    autopilotEnabled: State.autopilotEnabled,
    tradeLogs: State.tradeLogs.slice(0, 10)
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

wss.on('connection', (ws) => {
  console.log('[WebSocket Client] Mobile app connected to cloud feed.');
  ws.send(JSON.stringify({
    type: 'INIT',
    portfolio: State.portfolio,
    activePosition: State.activePosition,
    universe: State.universe,
    autopilotEnabled: State.autopilotEnabled
  }));
});

loadPersistedState();
connectBinanceStream();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`=======================================================`);
  console.log(` Sisir's Cloud Quant Engine running on port ${PORT}`);
  console.log(` WebSocket & REST API endpoints live at 0.0.0.0:${PORT}`);
  console.log(`=======================================================`);
});
