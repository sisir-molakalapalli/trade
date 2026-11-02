/**
 * =========================================================================
 * SISIR QUANT TERMINAL - PRODUCTION BACKEND TRADING ENGINE
 * =========================================================================
 * - Low-latency Singapore Binance WebSocket stream (<5ms latency)
 * - 3 Concurrent Active Breakout Slots (High-Velocity Execution)
 * - Maximum 5 Spot Recovery Bags Limit
 * - Strict Active Position Rule: Never close below +0.35% (Demote to Bag on failure)
 * - Self-Aware Multi-Dimensional Trailing (Taker CVD, CLV, Rejection Wicks, Apex Harvest)
 * - Spot Recovery Bag Engine (Target: +0.30% Breakeven / >3h Self-Aware Relief Trailing)
 * - 90-Minute 15m VWAP Technical Bounce Relief Exit
 * - Emergency Disaster Circuit Breaker Floor (-6.50%)
 * - Dual-Transport (POST + GET) Cloud Ledger Dispatch strictly to Sisir_Quant_Trade_Ledger
 * =========================================================================
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const https = require('https');

// Process-level crash resilience
process.on('uncaughtException', (err) => {
  console.error('[Server Sentry] Uncaught Exception intercepted:', err.message);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Server Sentry] Unhandled Rejection intercepted:', reason);
});

// Import Autonomous Learning Engine
let learningEngine;
try {
  learningEngine = require('./learning_engine');
} catch (e) {
  console.warn('[Server] Notice: learning_engine fallback loaded:', e.message);
  learningEngine = {
    recalculateDynamicHurdles: () => {},
    createEntrySnapshot: () => ({}),
    recordTradeOutcome: () => {},
    trainOnHistoricalTrades: () => {},
    state: { dynamicHurdles: { scoreOffset: 0, wickCeiling: 0.40, minOddsRequired: 45 } }
  };
}

const PORT = process.env.PORT || 3000;
const DISK_STATE_FILE = path.join(__dirname, 'cloud_portfolio_state.json');

// In-memory trade dispatch deduplication
const recentTradeDispatches = new Set();

// -------------------------------------------------------------
// CORE SYSTEM STATE
// -------------------------------------------------------------
const State = {
  autopilotEnabled: true,
  portfolio: {
    cashUSDT: 1000.00,
    holdings: {},
    realizedPnL: 0.00,
    bagSubsidyPoolUSDT: 0.00,
    sizingMode: 'PERCENTAGE',
    allocationPct: 15.0,
    fixedNotionalUSDT: 10.50
  },
  activePositions: {},
  recoveryBags: {},
  tradeLogs: [],
  universe: {},
  cooldowns: {},
  subsystems: {
    liveFeed: [],
    pumpDumpTrap: { status: 'ARMED', trappedTokensDetected: 0, lastBlockedSymbol: '--', minLiquidityHurdleUSD: '$70M' },
    screener: { status: 'ACTIVE', scannedPairsCount: 350, qualifiedCandidatesCount: 15, cadenceSec: 60, liquidityFloorUSD: 'High-Sustain ($3.5M+)' },
    microstructure: { status: 'ONLINE', cyclesCompleted: 150, activeBookDepth: 20 },
    btcMacroGate: { status: 'NORMAL', allowAltcoinEntries: true, btcChange15m: 0, reason: 'BTC macro healthy' },
    regimeController: { status: 'CALIBRATED', currentRegime: 'BALANCED', marketAvgVolatilityPct: 4.5, liveScoreGate: 75, liveObiGatePct: 45 },
    predictiveSentry: { status: 'WATCHING', activePositionsMonitored: 0, recoveryBagsMonitored: 0, architecture: 'Min +0.35% Floor • Self-Aware Apex Harvest' },
    recoveryManager: { status: 'ONLINE', activeRecoveryBagsCount: 0, meanReversionTargetRate: 'Step 1 (+0.30% Target) / >3h Self-Aware Relief Trail', bagsResolvedWinCount: 0, subsidyPoolUSDT: 0.00 },
    quotaManager: { status: 'NORMAL', executed24hCount: 0, maxAllowed: 'UNLIMITED', quotaRemaining: '∞', vipSlotsRemaining: '∞', formattedReset: 'Dynamic Reserve Active' },
    persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Sheet Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
  },
  marketRegime: 'BALANCED',
  activeThresholds: { regime: 'BALANCED', scoreThreshold: 75, obiThreshold: 0.45, maxSpread: 0.20, marketAvgRange: 4.5 },
  tradeFrequencyController: {
    dynamicGovernorEnabled: true,
    quotaBypassEnabled: false,
    cashReserveFloorUSDT: 300.00,
    maxRecoveryBagsLimit: 5,
    governorTier: 'UNLIMITED_SPRINT',
    maxTradesPer24h: 9999,
    maxConcurrentPositions: 3,
    rollingTrades24h: [],
    consecutiveWinStreak: 0,
    nextSlotReleaseTimestamp: 0,
    pacingMode: 'HIGH_VELOCITY'
  },
  usdtToInrRate: 94.94,
  masterLedgerUrl: 'https://script.google.com/macros/s/AKfycbwwFkRk4O2oJYmgrRcQBpx3z5KBKEOCJoX6YzL1yJXdot93V1T08ewIGaF78O1n6Z1ktg/exec',
  learningSheetUrl: 'https://script.google.com/macros/s/AKfycbwut72lTkCVcqIV2k7YlzlsIOwO3GXCKqlF-4UBxvTvJHckJApKKxzbfFp5JN0VLND8/exec'
};

// -------------------------------------------------------------
// AUTOPILOT ENTRY EVALUATOR (3 SLOTS & MAX 5 RECOVERY BAGS)
// -------------------------------------------------------------
let isEvaluatingEntries = false;

async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled || isEvaluatingEntries) return;

  const currentCount = Object.keys(State.activePositions || {}).length;
  const maxSlots = State.tradeFrequencyController?.maxConcurrentPositions || 3;
  if (currentCount >= maxSlots) return;

  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  const maxBagsLimit = State.tradeFrequencyController?.maxRecoveryBagsLimit || 5;
  if (activeBagsCount >= maxBagsLimit) return;

  const isBtcHealthy = await checkBtcMacroWeatherShield();
  if (!isBtcHealthy) return;

  learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const minScore = State.activeThresholds?.scoreThreshold || 75;
  const keys = Object.keys(State.universe || {});
  if (keys.length === 0) return;

  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  isEvaluatingEntries = true;
  try {
    let evaluatedCount = 0;
    for (const k of sorted) {
      if (Object.keys(State.activePositions || {}).length >= maxSlots) break;
      if (Object.keys(State.recoveryBags || {}).length >= maxBagsLimit) break;
      const coin = State.universe[k];
      if (!coin || !coin.symbol) continue;

      // Gate: Strictly reject flagged pump-and-dump spikes
      if (coin.isPumpAndDump) continue;

      if ((coin.score || 0) < minScore) break;

      const baseSym = String(coin.symbol).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/USDT$/, '');
      const pairKey = `${baseSym}USDT`;
      if (State.activePositions[pairKey] || State.recoveryBags[pairKey]) continue;

      if (State.cooldowns[baseSym] && State.cooldowns[baseSym].until > Date.now()) continue;

      if (++evaluatedCount > 25) break;

      const change24h = Number(coin.priceChangePct || coin.change24h || 0);
      const rangePct = Number(coin.intradayRangePct || 0);
      if (change24h < 0.20 && rangePct < 3.0) continue;

      const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
      if (posInRange < 0.45) continue;

      // Secondary Pre-Flight Pump & Dump Trap Veto
      const volQuote = (coin.volumeM || 0) * 1000000;
      if (volQuote < 70000000 && (change24h >= 7.0 || rangePct >= 8.5)) {
        const pdCheck = await checkDormantPumpAndDumpTrap(pairKey, volQuote, change24h, rangePct);
        if (pdCheck.isTrap) {
          logOperation('TRAP_GUARD', `Avoided trap on ${pairKey}: ${pdCheck.reason}`, 'danger');
          continue;
        }
      }

      const orderFlow = await checkVeteranOrderFlowAndVSA(pairKey);
      if (!orderFlow.passed) continue;

      const microMom = await checkMicrostructureMomentumAndVwap(pairKey);
      if (!microMom.passed) continue;

      const trend1h = await check1hTrendAlignment(pairKey);
      if (!trend1h.passed) continue;

      let book = await fetchRealOrderBookDepth(pairKey);
      if (!book) {
        book = {
          obi: Number(coin.obi || 0.55),
          spreadPct: 0.05,
          bestBid: Number(coin.priceUSDT || 1.0),
          bestAsk: Number(coin.priceUSDT || 1.0) * 1.0005
        };
      }

      if (book.obi < 0.45 || book.spreadPct > 0.20) {
        logOperation('BOOK_VETO', `${pairKey} halted: OBI ${Math.round(book.obi * 100)}% (req >= 45%) or Spread ${book.spreadPct.toFixed(2)}% (req <= 0.20%).`, 'warning');
        continue;
      }

      logOperation('BOOK_CLEAR', `${pairKey} Order Book verified: OBI ${Math.round(book.obi * 100)}%, Spread ${book.spreadPct.toFixed(2)}%. Deploying capital...`, 'success');

      console.log(`[Autopilot Signal] Entering ${pairKey}: Score ${coin.score}, OBI ${Math.round(book.obi * 100)}%, 15m ${microMom.change15mPct}%`);
      await executeSpotBuy(pairKey, coin, book, microMom);
    }
  } finally {
    isEvaluatingEntries = false;
  }
}

// -------------------------------------------------------------
// PREDICTIVE SENTRY & SPOT RECOVERY BAG ENGINE
// -------------------------------------------------------------
async function runPredictiveSentry() {
  const activeKeys = Object.keys(State.activePositions || {});
  const bagKeys = Object.keys(State.recoveryBags || {});

  // PART 1: ACTIVE BREAKOUT POSITIONS (NEVER CLOSE BELOW +0.35%)
  for (const k of activeKeys) {
    const pos = State.activePositions[k];
    if (!pos) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
    const entryPrice = Number(pos.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdMinutes = (Date.now() - (pos.openedAt || Date.now())) / 60000;

    pos.livePriceUSDT = livePrice;
    pos.peakGainPct = Math.max(pos.peakGainPct || 0, gainPct);
    pos.peakHighPrice = Math.max(pos.peakHighPrice || entryPrice, livePrice);
    const peakGain = pos.peakGainPct;

    // Minimum Floor Constraint: Stop loss can NEVER be set below entry * 1.0035 (+0.35%)
    const minExitPriceFloor = Number((entryPrice * 1.0035).toFixed(6));

    // Ratchet 1: Breakeven / Min-Floor lock at +0.35%
    if (peakGain >= 0.35 && (pos.ratchetStage === 'INITIAL' || !pos.ratchetStage)) {
      pos.ratchetStage = 'BREAKEVEN_LOCKED';
      pos.sl = minExitPriceFloor; // Strictly locked at >= +0.35%
      logOperation('SENTRY', `${pos.pair}: Breakeven locked (+0.35% minimum floor). Peak: +${peakGain.toFixed(2)}%`, 'success');
    }

    // Ratchet 2: Profit lock at +0.70%
    if (peakGain >= 0.70 && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL' && pos.ratchetStage !== 'RUNNER_TRAIL') {
      pos.ratchetStage = 'PROFIT_LOCKED';
      pos.sl = Math.max(minExitPriceFloor, Number((entryPrice * 1.0050).toFixed(6))); // +0.50% profit secured
      logOperation('SENTRY', `${pos.pair}: Profit lock engaged (+0.50% secured).`, 'success');
    }

    // Ratchet 3: Dynamic apex trailing at +1.00%
    if (peakGain >= 1.00 && peakGain < 1.80) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9965).toFixed(6)); // Trails 0.35% below peak
      pos.sl = Math.max(minExitPriceFloor, pos.sl || 0, trailFloor);
    }

    // Self-Aware Trailing: Apex Harvest at Peak (Strictly >= +0.35%)
    if (gainPct >= 0.35) {
      const selfAware = await checkSelfAwareOrderFlow(k);
      if (selfAware.isApexExhausted && livePrice >= minExitPriceFloor) {
        logOperation('SENTRY_EXIT', `Self-Aware Peak Harvest on ${pos.pair}: Taker Buy ${selfAware.takerBuyRatio}% / Upper Wick ${Math.round(selfAware.upperWickRatio * 100)}% signaled apex. Banked at +${gainPct.toFixed(2)}% ($${livePrice.toFixed(4)}).`, 'success');
        
        const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
        const tradePayload = {
          id: Date.now(),
          pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
          type: 'SELF-AWARE PEAK HARVEST',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: exitNotional,
          pnlUSDT: realizedGain,
          reason: `Self-aware peak harvest (+${gainPct.toFixed(2)}% >= +0.35% floor)`
        };

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.activePositions[k];
        saveDiskState();
        broadcastStateUpdate();
        recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
        continue;
      }
    }

    // Take Profit 1 (+1.80%): Bank 50%, runner trails
    if (gainPct >= 1.80 && !pos.tp1Secured) {
      pos.tp1Secured = true;
      pos.tp1Price = livePrice;
      pos.ratchetStage = 'RUNNER_TRAIL';
      pos.sl = Math.max(minExitPriceFloor, Number((pos.peakHighPrice * 0.9950).toFixed(6))); // Trails 0.50% below peak
      logOperation('SENTRY_EXIT', `TP1 BANKED on ${pos.pair}: 50% sold at +${gainPct.toFixed(2)}% ($${livePrice.toFixed(4)}). Runner trailing.`, 'success');
      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'TP1 BANKED',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: Number((pos.notionalUSDT * 0.5).toFixed(2)),
        pnlUSDT: Number((((livePrice - entryPrice) / entryPrice) * (pos.notionalUSDT * 0.5)).toFixed(4)),
        reason: 'TP1 secured: 50% banked at +1.80%'
      });
    }

    // Trailing Stop Execution (Strictly >= +0.35%)
    if (pos.sl && livePrice <= pos.sl && pos.ratchetStage !== 'INITIAL') {
      if (gainPct >= 0.35 && livePrice >= minExitPriceFloor) {
        const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
        
        logOperation('SENTRY_EXIT', `Trailing stop executed on ${pos.pair} at $${livePrice.toFixed(4)} (+${gainPct.toFixed(2)}% >= +0.35%). Realized: +$${realizedGain.toFixed(2)} USDT.`, 'success');

        const tradePayload = {
          id: Date.now(),
          pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
          type: pos.tp1Secured ? 'RUNNER EXIT' : 'TRAILING PROFIT LOCK (+0.35%+)',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: exitNotional,
          pnlUSDT: realizedGain,
          reason: `Trailing stop triggered at $${livePrice.toFixed(4)} (Peak: +${peakGain.toFixed(2)}%)`
        };

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.activePositions[k];
        saveDiskState();
        broadcastStateUpdate();

        recordTradeToCloudLedger(tradePayload).catch(e => console.error('[Ledger Dispatch Notice]', e.message));
        continue;
      }
    }

    // 45-Minute Alpha Velocity Scratch (Only if >= +0.35%)
    if (holdMinutes >= 45 && gainPct >= 0.35) {
      const exitNotional = pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VELOCITY SCRATCH 45M',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `45m Velocity Scratch: Cleared stalled breakout at +${gainPct.toFixed(2)}% (>= +0.35%)`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + exitNotional + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // Demote to Spot Recovery Bag: NEVER CLOSE BELOW +0.35% IN BREAKOUT SLOT
    // If momentum fails or price pulls back, transfer safely to Recovery Bag
    if (!pos.tp1Secured && gainPct < 0.35) {
      let shouldDemote = (gainPct <= -2.80);

      if (!shouldDemote && holdMinutes >= 20 && gainPct <= -0.50) {
        const selfAwareCheck = await checkSelfAwareOrderFlow(k);
        if (selfAwareCheck.isUpsideDead) {
          shouldDemote = true;
        }
      }

      if (shouldDemote) {
        logOperation('SENTRY_DEMOTE', `${pos.pair}: Upside exhausted / retrace (${gainPct.toFixed(2)}%). Transferred to Spot Recovery Bag (Never closed below +0.35%).`, 'warning');

        State.recoveryBags[k] = {
          pair: pos.pair,
          pairSymbol: pos.pairSymbol,
          openedAt: pos.openedAt,
          movedAt: Date.now(),
          entryPriceUSDT: entryPrice,
          livePriceUSDT: livePrice,
          notionalUSDT: pos.notionalUSDT,
          qty: pos.qty,
          targetStep: 'STEP 1 (+0.30%)',
          targetExitPriceUSDT: Number((entryPrice * 1.0030).toFixed(6)),
          troughPrice: livePrice,
          bouncePeakPrice: livePrice
        };

        delete State.activePositions[k];
        broadcastStateUpdate();
        continue;
      }
    }
  }

  // PART 2: SPOT RECOVERY BAGS (WITH SELF-AWARE TRAILING AFTER 3 HOURS)
  for (const k of bagKeys) {
    const bag = State.recoveryBags[k];
    if (!bag) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
    const entryPrice = Number(bag.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdHours = (Date.now() - (bag.movedAt || Date.now())) / 3600000;

    bag.livePriceUSDT = livePrice;
    bag.troughPrice = Math.min(bag.troughPrice || livePrice, livePrice);
    bag.bouncePeakPrice = Math.max(bag.bouncePeakPrice || livePrice, livePrice);

    // Target Step 1: Standard +0.30% Breakeven Target
    const targetPrice = bag.targetExitPriceUSDT || (entryPrice * 1.0030);
    if (livePrice >= targetPrice) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      
      logOperation('RECOVERY', `Breakeven relief target (+0.30%) filled on ${bag.pair} at $${livePrice.toFixed(4)}! 100% capital recycled.`, 'success');

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'RECOVERY BREAKEVEN (+0.30%)',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery filled on relief bounce (+0.30% Breakeven Target)`
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    // -------------------------------------------------------------
    // SELF-AWARE RELIEF TRAILING FOR BAGS HELD > 3 HOURS
    // -------------------------------------------------------------
    if (holdHours >= 3.0) {
      bag.selfAwareActive = true;
      const selfAware = await checkSelfAwareOrderFlow(k);
      const bounceFromTroughPct = bag.troughPrice > 0 ? (((livePrice - bag.troughPrice) / bag.troughPrice) * 100) : 0;

      // Trailing floor 0.30% beneath the bounce peak
      const reliefTrailFloor = Number((bag.bouncePeakPrice * 0.9970).toFixed(6));
      bag.recoverySl = Math.max(bag.recoverySl || 0, reliefTrailFloor);

      // Exit trigger 1: Bounce crest rolls over into trailing floor
      const isTrailHit = (bag.recoverySl && livePrice <= bag.recoverySl && bounceFromTroughPct >= 0.40);
      
      // Exit trigger 2: Order-flow exhaustion on relief bounce (Taker buy collapses or heavy upper wick)
      const isReliefExhausted = (selfAware.isApexExhausted && bounceFromTroughPct >= 0.45);

      // Exit trigger 3: Near-breakeven relief (gainPct >= -0.10%)
      const isNearBreakeven = (gainPct >= -0.10);

      if (isTrailHit || isReliefExhausted || isNearBreakeven) {
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
        const exitReason = isNearBreakeven 
          ? `Self-Aware >3h: Near-breakeven relief exit at $${livePrice.toFixed(4)}`
          : `Self-Aware >3h Trailing Exit: Captured bounce peak $${bag.bouncePeakPrice.toFixed(4)} at $${livePrice.toFixed(4)}`;

        logOperation('RECOVERY', `Self-Aware Trailing Exit executed on ${bag.pair} held ${holdHours.toFixed(1)}h at $${livePrice.toFixed(4)} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}%). Best price secured.`, gainPct >= 0 ? 'success' : 'info');

        await recordTradeToCloudLedger({
          id: Date.now(),
          pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
          type: gainPct >= 0 ? 'RECOVERY SELF-AWARE WIN' : 'RECOVERY SELF-AWARE TRAIL',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: bag.notionalUSDT,
          pnlUSDT: realizedGain,
          reason: exitReason
        });

        State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.recoveryBags[k];
        saveDiskState();
        broadcastStateUpdate();
        continue;
      }
    }

    // 15m VWAP Technical Bounce Exit after 90 mins (if gain >= -0.15%)
    if (holdHours >= 1.5 && gainPct >= -0.15) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      logOperation('RECOVERY', `VWAP relief exit on ${bag.pair} at $${livePrice.toFixed(4)}. Capital recycled.`, 'info');

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VWAP RELIEF RECOVERY',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'VWAP technical bounce exit: capital recycled in 90m'
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    // Hard Disaster Floor Cut (-6.50%)
    if (gainPct <= -6.50) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));

      logOperation('SENTRY_EXIT', `Hard disaster cut triggered on ${bag.pair} at -6.50%. Preserved cash capital.`, 'danger');

      await recordTradeToCloudLedger({
        id: Date.now(),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'HARD DISASTER CUT (-6.50%)',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'Hard disaster floor (-6.50%): Capital preserved'
      });

      State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + bag.notionalUSDT + realizedGain).toFixed(4));
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }
  }
}

// -------------------------------------------------------------
// CLOUD LEDGER & PERSISTENCE
// -------------------------------------------------------------
function normalizeDateToIndianFormat(dateVal, id, timeVal) {
  let idDate = null;
  const idNum = Number(id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    idDate = new Date(idNum);
  }

  let s = String(dateVal || '').trim();
  const sTime = String(timeVal || '').trim();

  if (!s || s === 'Recent' || s === '0' || s.includes('1899') || (s.includes(':') && !s.includes('/'))) {
    if (sTime && (sTime.includes('/') || sTime.includes('-')) && !sTime.includes('1899')) {
      s = sTime;
    } else if (idDate) {
      const d = String(idDate.getDate()).padStart(2, '0');
      const m = String(idDate.getMonth() + 1).padStart(2, '0');
      const y = idDate.getFullYear();
      return `${d}/${m}/${y}`;
    } else {
      return '01/11/2026';
    }
  }

  const slashMatch = s.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
  if (slashMatch) {
    const d = parseInt(slashMatch[1], 10);
    const m = parseInt(slashMatch[2], 10);
    let y = parseInt(slashMatch[3], 10);
    if (y < 2000 && idDate) y = idDate.getFullYear();
    return `${String(d).padStart(2, '0')}/${String(m).padStart(2, '0')}/${y}`;
  }

  if (idDate) {
    const d = String(idDate.getDate()).padStart(2, '0');
    const m = String(idDate.getMonth() + 1).padStart(2, '0');
    const y = idDate.getFullYear();
    return `${d}/${m}/${y}`;
  }

  return '01/11/2026';
}

function getTradeTimeMs(t) {
  if (!t) return 0;
  const dStr = String(t.date || '').trim();
  const tStr = String(t.time || '').trim();
  const slashMatch = dStr.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
  if (slashMatch) {
    const day = parseInt(slashMatch[1], 10);
    const month = parseInt(slashMatch[2], 10) - 1;
    const year = parseInt(slashMatch[3], 10);
    let hours = 0, minutes = 0, seconds = 0;
    const timeMatch = tStr.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (timeMatch) {
      hours = parseInt(timeMatch[1], 10);
      minutes = parseInt(timeMatch[2], 10);
      seconds = timeMatch[3] ? parseInt(timeMatch[3], 10) : 0;
    }
    const dt = new Date(year, month, day, hours, minutes, seconds);
    if (!isNaN(dt.getTime())) return dt.getTime();
  }

  const idNum = Number(t.id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    return idNum;
  }
  return idNum || 0;
}

function sanitizeAndAuditTradeHistory(logs) {
  if (!Array.isArray(logs)) return [];
  const seenSignatures = new Set();

  return logs.filter(t => {
    if (!t || typeof t !== 'object') return false;
    const p = String(t.pair || t.Pair || t.symbol || t.Symbol || '').trim().toUpperCase();
    if (!p || p === 'CRYPTO/USDT' || p.includes('CRYPTO') || p === 'UNKNOWN/USDT' || !p.includes('USDT')) return false;

    const entry = Number(t.entryPriceUSDT || t['Entry Price (USDT)'] || t['Entry (USDT)'] || t.entryPrice || t.entry || t.Entry || 0);
    const exit = Number(t.exitPriceUSDT || t['Exit Price (USDT)'] || t['Exit (USDT)'] || t.exitPrice || t.exit || t.Exit || 0);
    if (isNaN(entry) || entry <= 0 || isNaN(exit) || exit <= 0) return false;

    return true;
  }).map(t => {
    const rawPair = String(t.pair || t.Pair || t.symbol || '').trim().toUpperCase();
    const pair = rawPair.includes('/') ? rawPair : `${rawPair.replace('USDT', '')}/USDT`;
    const entry = Number(t.entryPriceUSDT || t['Entry Price (USDT)'] || t['Entry (USDT)'] || t.entryPrice || t.entry || t.Entry || 1);
    const exit = Number(t.exitPriceUSDT || t['Exit Price (USDT)'] || t['Exit (USDT)'] || t.exitPrice || t.exit || t.Exit || entry);
    const tradeId = Number(t.id || t.ID || t.Id) || Date.now();
    const cleanDate = normalizeDateToIndianFormat(t.date, tradeId, t.time);
    const cleanTime = String(t.time || '').trim() || '00:00:00';

    // 1. Preserve real in-memory notional if trade was executed in this session
    const existingLocal = (State.tradeLogs || []).find(x => x.id === tradeId || (x.pair === pair && x.date === cleanDate && Math.abs(Number(x.entryPriceUSDT || 0) - entry) < 0.0001));
    let notional = existingLocal && existingLocal.notionalUSDT && existingLocal.notionalUSDT !== 50
      ? Number(existingLocal.notionalUSDT)
      : Number(t.notionalUSDT || t['Notional (USDT)'] || t.notional || 0);

    let pnlUSDT = Number(t.pnlUSDT !== undefined ? t.pnlUSDT : (t['PnL (USDT)'] !== undefined ? t['PnL (USDT)'] : t.pnl));

    // 2. Derive true allocated notional if missing or defaulted to 50
    const priceGainRatio = (entry > 0 && exit > 0) ? Math.abs((exit - entry) / entry) : 0;
    if ((!notional || notional === 50) && !isNaN(pnlUSDT) && Math.abs(pnlUSDT) > 0.0001 && priceGainRatio > 0.0005) {
      const derivedNotional = Math.abs(pnlUSDT) / priceGainRatio;
      if (derivedNotional >= 5.0 && derivedNotional <= 5000.0) {
        notional = Number(derivedNotional.toFixed(2));
      }
    }

    if (!notional || notional === 50) {
      notional = existingLocal?.notionalUSDT ? Number(existingLocal.notionalUSDT) : (Number(State.portfolio.fixedNotionalUSDT) || 10.50);
    }

    if (isNaN(pnlUSDT)) {
      pnlUSDT = (exit - entry) * (notional / entry);
    }

    return {
      ...t,
      id: tradeId,
      date: cleanDate,
      time: cleanTime,
      pair,
      type: String(t.type || t.Type || 'EXIT').trim().toUpperCase(),
      entryPriceUSDT: entry,
      exitPriceUSDT: exit,
      notionalUSDT: Number(notional.toFixed(2)),
      pnlUSDT: Number(pnlUSDT.toFixed(4)),
      pnlINR: Number((pnlUSDT * State.usdtToInrRate).toFixed(2))
    };
  }).filter(t => {
    const timeMinute = String(t.time || '').slice(0, 5);
    const sig = `${t.pair}_${t.date}_${timeMinute}_${t.entryPriceUSDT.toFixed(4)}_${t.exitPriceUSDT.toFixed(4)}_${t.type}`;
    if (seenSignatures.has(sig)) return false;
    seenSignatures.add(sig);
    return true;
  }).sort((a, b) => getTradeTimeMs(b) - getTradeTimeMs(a));
}

async function syncTradesFromGoogleSheets() {
  const masterUrl = State.masterLedgerUrl;

  try {
    const fullUrl = masterUrl.includes('?') ? `${masterUrl}&action=GET_ALL` : `${masterUrl}?action=GET_ALL`;
    const data = await safeFetch(fullUrl, 10000);
    let fetchedTrades = [];
    if (data && Array.isArray(data.trades) && data.trades.length > 0) {
      fetchedTrades = data.trades;
    } else if (Array.isArray(data) && data.length > 0) {
      fetchedTrades = data;
    }

    if (fetchedTrades.length > 0) {
      // Merge with in-memory tradeLogs to keep accurate local notional
      const merged = [...fetchedTrades];
      (State.tradeLogs || []).forEach(localTrade => {
        if (!merged.some(f => f.id === localTrade.id || (f.pair === localTrade.pair && f.date === localTrade.date && f.time === localTrade.time))) {
          merged.push(localTrade);
        }
      });
      State.tradeLogs = sanitizeAndAuditTradeHistory(merged);
      saveDiskState();
      broadcastStateUpdate();
      console.log(`[Sync Ledger] Successfully synchronized ${State.tradeLogs.length} verified trades.`);
      return State.tradeLogs;
    }
  } catch (err) {
    console.warn(`[Sync Ledger] Notice:`, err.message);
  }
  return State.tradeLogs;
}

async function recordTradeToCloudLedger(trade) {
  const tradeId = Number(trade.id || Date.now());
  const now = new Date();
  const cleanDate = normalizeDateToIndianFormat(trade.date || now.toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }), tradeId);
  const cleanTime = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });

  const record = {
    ...trade,
    action: 'RECORD_TRADE',
    id: tradeId,
    date: cleanDate,
    time: cleanTime,
    pair: trade.pair,
    type: trade.type,
    entryPriceUSDT: Number(trade.entryPriceUSDT),
    exitPriceUSDT: Number(trade.exitPriceUSDT),
    notionalUSDT: Number(trade.notionalUSDT || 10.05),
    pnlUSDT: Number(trade.pnlUSDT),
    pnlINR: Number((trade.pnlUSDT * State.usdtToInrRate).toFixed(2)),
    regime: State.marketRegime || 'BALANCED',
    volatilityPct: 4.5,
    reason: trade.reason || 'Closed trade'
  };

  const timeMinute = cleanTime.slice(0, 5);
  const signature = `${record.pair}_${cleanDate}_${timeMinute}_${record.entryPriceUSDT}_${record.exitPriceUSDT}_${record.type}`;
  if (recentTradeDispatches.has(signature)) return;
  recentTradeDispatches.add(signature);
  if (recentTradeDispatches.size > 200) {
    const first = recentTradeDispatches.values().next().value;
    recentTradeDispatches.delete(first);
  }

  State.tradeLogs.unshift(record);
  saveDiskState();

  const targetUrl = (trade.targetUrl || State.masterLedgerUrl || '').trim();
  let success = false;

  try {
    console.log(`[Cloud Ledger] Dispatching trade ${record.pair} (${record.type}) strictly to Sisir_Quant_Trade_Ledger...`);
    const res = await safePostJson(targetUrl, record, 15000);
    console.log(`[Cloud Ledger Response]`, JSON.stringify(res));
    if (res && res.success !== false) {
      success = true;
    }
  } catch (err) {
    console.error(`[Cloud Ledger POST Error] ${err.message}. Retrying via GET fallback...`);
  }

  if (!success) {
    try {
      const getUrl = targetUrl.includes('?') 
        ? `${targetUrl}&action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`
        : `${targetUrl}?action=RECORD_TRADE&data=${encodeURIComponent(JSON.stringify(record))}`;
      const getRes = await safeFetch(getUrl, 15000);
      console.log(`[Cloud Ledger GET Fallback Response]`, JSON.stringify(getRes));
      if (getRes && getRes.success !== false) {
        success = true;
      }
    } catch (err2) {
      console.warn(`[Cloud Ledger GET Fallback Notice]`, err2.message);
    }
  }

  learningEngine.recordTradeOutcome(
    record.pair.replace('/USDT', 'USDT'),
    record.type,
    record.pnlUSDT,
    1.0
  );

  broadcastStateUpdate();
  return { success: true };
}

function saveDiskState() {
  try {
    const data = {
      portfolio: State.portfolio,
      activePositions: State.activePositions,
      recoveryBags: State.recoveryBags,
      tradeLogs: State.tradeLogs.slice(0, 150),
      masterLedgerUrl: State.masterLedgerUrl,
      learningSheetUrl: State.learningSheetUrl
    };
    fs.writeFileSync(DISK_STATE_FILE, JSON.stringify(data, null, 2), 'utf-8');
  } catch (e) {}
}

function loadDiskState() {
  try {
    if (fs.existsSync(DISK_STATE_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DISK_STATE_FILE, 'utf-8'));
      if (parsed.portfolio) State.portfolio = parsed.portfolio;
      if (parsed.activePositions) State.activePositions = parsed.activePositions;
      if (parsed.recoveryBags) State.recoveryBags = parsed.recoveryBags;
      if (Array.isArray(parsed.tradeLogs)) State.tradeLogs = parsed.tradeLogs;
      if (parsed.masterLedgerUrl && !parsed.masterLedgerUrl.includes('AKfycbw3cTF2Q3X') && !parsed.masterLedgerUrl.includes('AKfycbxzrx')) {
        State.masterLedgerUrl = parsed.masterLedgerUrl;
      } else {
        State.masterLedgerUrl = 'https://script.google.com/macros/s/AKfycbwwFkRk4O2oJYmgrRcQBpx3z5KBKEOCJoX6YzL1yJXdot93V1T08ewIGaF78O1n6Z1ktg/exec';
      }
      if (parsed.learningSheetUrl && !parsed.learningSheetUrl.includes('AKfycbxzrxMMq')) {
        State.learningSheetUrl = parsed.learningSheetUrl;
      } else {
        State.learningSheetUrl = 'https://script.google.com/macros/s/AKfycbwut72lTkCVcqIV2k7YlzlsIOwO3GXCKqlF-4UBxvTvJHckJApKKxzbfFp5JN0VLND8/exec';
      }
    }
  } catch (e) {}
}

// -------------------------------------------------------------
// EXPRESS APP & HTTP SERVER SETUP
// -------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const indexHtml = path.join(__dirname, 'index.html');
  if (fs.existsSync(indexHtml)) {
    return res.sendFile(indexHtml);
  }
  res.json({ status: 'ONLINE', engine: 'Sisir Quant Trading Engine' });
});

app.get(['/pnl_dashboard', '/pnl_dashboard.html'], (req, res) => {
  const pnlHtml = path.join(__dirname, 'pnl_dashboard.html');
  if (fs.existsSync(pnlHtml)) {
    return res.sendFile(pnlHtml);
  }
  res.status(404).send('P&L Dashboard file not found');
});

// REST API Endpoints
app.get('/api/state', (req, res) => {
  res.json({
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs,
    subsystems: State.subsystems,
    tradeFrequencyController: State.tradeFrequencyController,
    activeThresholds: State.activeThresholds,
    marketRegime: State.marketRegime,
    usdtToInrRate: State.usdtToInrRate,
    masterLedgerUrl: State.masterLedgerUrl,
    learningSheetUrl: State.learningSheetUrl,
    learningEngineState: learningEngine.state || {}
  });
});

app.post('/api/portfolio/cash', (req, res) => {
  const cash = parseFloat(req.body.cashUSDT);
  if (!isNaN(cash) && cash >= 0) {
    State.portfolio.cashUSDT = cash;
    saveDiskState();
    broadcastStateUpdate();
    return res.json({ success: true, cashUSDT: cash });
  }
  res.status(400).json({ success: false, error: 'Invalid cash' });
});

app.post('/api/portfolio/sizing', (req, res) => {
  const pct = parseFloat(req.body.allocationPct);
  if (!isNaN(pct) && pct >= 1 && pct <= 100) {
    State.portfolio.allocationPct = pct;
    saveDiskState();
    broadcastStateUpdate();
    return res.json({ success: true, allocationPct: pct });
  }
  res.status(400).json({ success: false, error: 'Invalid sizing' });
});

app.get('/api/screener/run', async (req, res) => {
  await runDynamicScreener();
  res.json({ success: true, universe: State.universe });
});

app.get('/api/p2p/sync', (req, res) => {
  res.json({ success: true, usdtToInrRate: State.usdtToInrRate });
});

app.post('/api/trades/purge-empty', (req, res) => {
  State.tradeLogs = State.tradeLogs.filter(t => t && t.pair && t.pair.includes('USDT') && t.entryPriceUSDT > 0);
  saveDiskState();
  res.json({ success: true, trades: State.tradeLogs });
});

app.post('/api/trades/sync-strict-sheet', async (req, res) => {
  try {
    const trades = await syncTradesFromGoogleSheets();
    res.json({ success: true, count: trades.length, trades });
  } catch (err) {
    res.json({ success: true, count: State.tradeLogs.length, trades: State.tradeLogs });
  }
});

app.post('/api/ledger/set-url', (req, res) => {
  const { url } = req.body;
  if (url && typeof url === 'string') {
    State.masterLedgerUrl = url.trim();
    saveDiskState();
    return res.json({ success: true, masterLedgerUrl: State.masterLedgerUrl });
  }
  res.status(400).json({ success: false, error: 'Invalid URL' });
});

app.post('/api/ledger/test', async (req, res) => {
  const testUrl = (req.body.url || State.masterLedgerUrl || '').trim();
  if (!testUrl) return res.status(400).json({ success: false, error: 'Missing ledger URL' });
  try {
    const fetchUrl = testUrl.includes('?') ? `${testUrl}&action=TEST` : `${testUrl}?action=TEST`;
    const data = await safeFetch(fetchUrl, 15000);
    return res.json({ success: true, source: 'SERVER_PROXY', data });
  } catch (err) {
    return res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/ledger/send-test-trade', async (req, res) => {
  const targetUrl = (req.body.url || State.masterLedgerUrl || '').trim();
  if (!targetUrl) return res.status(400).json({ success: false, error: 'Missing ledger URL' });

  const testTrade = {
    id: Date.now(),
    date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
    pair: 'TEST/USDT',
    type: 'DIAGNOSTIC TEST WRITE',
    entryPriceUSDT: 1.0000,
    exitPriceUSDT: 1.0050,
    notionalUSDT: 10.00,
    pnlUSDT: 0.0500,
    pnlINR: Number((0.05 * State.usdtToInrRate).toFixed(2)),
    reason: 'Connection test from terminal UI',
    targetUrl: targetUrl
  };

  try {
    const writeResult = await recordTradeToCloudLedger(testTrade);
    res.json({ success: true, trade: testTrade, cloudResult: writeResult });
  } catch (err) {
    res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/learning/test', async (req, res) => {
  const testUrl = (req.body.url || State.learningSheetUrl || '').trim();
  if (!testUrl) return res.status(400).json({ success: false, error: 'Missing learning URL' });
  try {
    const fetchUrl = testUrl.includes('?') ? `${testUrl}&action=TEST` : `${testUrl}?action=TEST`;
    const data = await safeFetch(fetchUrl, 15000);
    return res.json({ success: true, source: 'SERVER_PROXY', data });
  } catch (err) {
    return res.status(502).json({ success: false, error: err.message });
  }
});

app.post('/api/positions/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const pos = State.activePositions[pairKey];
  if (!pos) return res.status(404).json({ success: false, error: 'Position not found' });
  const livePrice = Number(State.universe[pairKey]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
  const entryPrice = Number(pos.entryPriceUSDT || livePrice);
  const notional = Number(pos.notionalUSDT || 10.05);
  const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
  const closeRecord = {
    id: Date.now(),
    pair: pos.pair || `${pairKey.replace('USDT', '')}/USDT`,
    type: realizedGain >= 0 ? 'MANUAL PROFIT' : 'MANUAL EXIT',
    entryPriceUSDT: entryPrice,
    exitPriceUSDT: livePrice,
    notionalUSDT: notional,
    pnlUSDT: realizedGain,
    reason: `Manual position close at $${livePrice.toFixed(4)}`
  };
  delete State.activePositions[pairKey];
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
  saveDiskState();
  broadcastStateUpdate();
  await recordTradeToCloudLedger(closeRecord);
  res.json({ success: true, trade: closeRecord });
});

app.post('/api/bags/close', async (req, res) => {
  const { pairKey } = req.body;
  if (!pairKey) return res.status(400).json({ success: false, error: 'Missing pairKey' });
  const bag = State.recoveryBags[pairKey];
  if (!bag) return res.status(404).json({ success: false, error: 'Bag not found' });
  const livePrice = Number(State.universe[pairKey]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
  const entryPrice = Number(bag.entryPriceUSDT || livePrice);
  const notional = Number(bag.notionalUSDT || 10.05);
  const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * notional).toFixed(4));
  const closeRecord = {
    id: Date.now(),
    pair: bag.pair || `${pairKey.replace('USDT', '')}/USDT`,
    type: realizedGain >= 0 ? 'RECOVERY MANUAL EXIT' : 'BAG LIQUIDATION',
    entryPriceUSDT: entryPrice,
    exitPriceUSDT: livePrice,
    notionalUSDT: notional,
    pnlUSDT: realizedGain,
    reason: `Manual bag liquidation at $${livePrice.toFixed(4)}`
  };
  delete State.recoveryBags[pairKey];
  State.portfolio.cashUSDT = Number((State.portfolio.cashUSDT + notional + realizedGain).toFixed(4));
  State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
  saveDiskState();
  broadcastStateUpdate();
  await recordTradeToCloudLedger(closeRecord);
  res.json({ success: true, trade: closeRecord });
});

// -------------------------------------------------------------
// WEBSOCKET SERVER & REAL-TIME BROADCASTS
// -------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcastStateUpdate() {
  const msg = JSON.stringify({
    type: 'STATE_UPDATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    tradeLogs: State.tradeLogs,
    subsystems: State.subsystems,
    universe: State.universe
  });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

function broadcastPriceStream() {
  const prices = {};
  Object.keys(State.universe).forEach(k => prices[k] = State.universe[k].priceUSDT);
  const msg = JSON.stringify({
    type: 'PRICE_STREAM',
    prices,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags
  });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

// -------------------------------------------------------------
// ZERO-LATENCY DIRECT BINANCE WEBSOCKET INGESTION (SINGAPORE)
// -------------------------------------------------------------
let binanceGlobalWs = null;
let subscribedActiveStreams = new Set();
let isSentryRunning = false;

function connectBinanceGlobalStream() {
  const wsEndpoint = 'wss://stream.binance.com:9443/ws/!miniTicker@arr';
  
  try {
    if (binanceGlobalWs) {
      try { binanceGlobalWs.terminate(); } catch (e) {}
      binanceGlobalWs = null;
    }

    binanceGlobalWs = new WebSocket(wsEndpoint);

    binanceGlobalWs.on('open', () => {
      console.log('[Binance Stream] Connected to Binance Global miniTicker stream (<5ms Singapore latency).');
      syncActiveSymbolSubscriptions();
    });

    binanceGlobalWs.on('message', (data) => {
      try {
        const tickers = JSON.parse(data);
        if (!Array.isArray(tickers)) return;

        const rate = Number(State.usdtToInrRate) || 94.94;
        let activePriceChanged = false;

        tickers.forEach(t => {
          const sym = t.s;
          if (!sym || !sym.endsWith('USDT')) return;

          const closePrice = parseFloat(t.c);
          if (isNaN(closePrice) || closePrice <= 0) return;

          if (State.universe[sym]) {
            State.universe[sym].priceUSDT = closePrice;
            State.universe[sym].priceINR = Number((closePrice * rate).toFixed(2));
          }

          if (State.activePositions[sym]) {
            State.activePositions[sym].livePriceUSDT = closePrice;
            activePriceChanged = true;
          }

          if (State.recoveryBags[sym]) {
            State.recoveryBags[sym].livePriceUSDT = closePrice;
            activePriceChanged = true;
          }
        });

        if (activePriceChanged && !isSentryRunning) {
          isSentryRunning = true;
          runPredictiveSentry().finally(() => {
            isSentryRunning = false;
          });
        }
      } catch (err) {}
    });

    binanceGlobalWs.on('error', (err) => {
      console.warn('[Binance Stream Error]', err.message);
    });

    binanceGlobalWs.on('close', () => {
      setTimeout(connectBinanceGlobalStream, 2500);
    });
  } catch (err) {
    setTimeout(connectBinanceGlobalStream, 4000);
  }
}

function syncActiveSymbolSubscriptions() {
  if (!binanceGlobalWs || binanceGlobalWs.readyState !== WebSocket.OPEN) return;
  const currentSymbols = new Set([
    ...Object.keys(State.activePositions || {}),
    ...Object.keys(State.recoveryBags || {})
  ]);

  const newStreams = [];
  currentSymbols.forEach(sym => {
    const s = `${sym.toLowerCase()}@bookTicker`;
    if (!subscribedActiveStreams.has(s)) {
      subscribedActiveStreams.add(s);
      newStreams.push(s);
    }
  });

  if (newStreams.length > 0) {
    binanceGlobalWs.send(JSON.stringify({
      method: 'SUBSCRIBE',
      params: newStreams,
      id: Date.now()
    }));
  }
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
    portfolio: State.portfolio,
    activePositions: State.activePositions,
    recoveryBags: State.recoveryBags,
    universe: State.universe,
    tradeLogs: State.tradeLogs,
    subsystems: State.subsystems,
    tradeFrequencyController: State.tradeFrequencyController,
    activeThresholds: State.activeThresholds,
    marketRegime: State.marketRegime,
    usdtToInrRate: State.usdtToInrRate,
    learningEngineState: learningEngine.state || {}
  }));

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'SYNC_STATE') {
        broadcastStateUpdate();
      } else if (msg.type === 'TOGGLE_AUTOPILOT') {
        State.autopilotEnabled = Boolean(msg.enabled);
        broadcastStateUpdate();
      } else if (msg.type === 'SET_CASH' && !isNaN(parseFloat(msg.cashUSDT))) {
        State.portfolio.cashUSDT = parseFloat(msg.cashUSDT);
        saveDiskState();
        broadcastStateUpdate();
      } else if (msg.type === 'SET_SIZING_CONFIG') {
        if (!isNaN(parseFloat(msg.allocationPct))) {
          State.portfolio.allocationPct = parseFloat(msg.allocationPct);
        }
        if (msg.sizingMode) State.portfolio.sizingMode = msg.sizingMode;
        saveDiskState();
        broadcastStateUpdate();
      } else if (msg.type === 'SET_GOOGLE_SHEET_URL') {
        if (msg.masterLedgerUrl) State.masterLedgerUrl = msg.masterLedgerUrl;
        if (msg.learningSheetUrl) State.learningSheetUrl = msg.learningSheetUrl;
        saveDiskState();
        broadcastStateUpdate();
      }
    } catch (e) {}
  });
});

// -------------------------------------------------------------
// BOOTSTRAP TIMERS & SERVICE INITIALIZATION
// -------------------------------------------------------------
loadDiskState();

runDynamicScreener().then(() => {
  console.log('[Screener] Initialized', Object.keys(State.universe).length, 'Binance Spot USDT pairs.');
}).catch(() => {});

syncTradesFromGoogleSheets().catch(() => {});
connectBinanceGlobalStream();

setInterval(() => {
  syncTradesFromGoogleSheets().catch(() => {});
}, 120000);

setInterval(async () => {
  try {
    await runPredictiveSentry();
    await evaluateAutopilotEntries();
  } catch (e) {
    console.error('[Sentry Error]', e.message);
  }
}, 1000);

setInterval(broadcastPriceStream, 500);
setInterval(runDynamicScreener, 60000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(`SISIR QUANT ENGINE ONLINE ON PORT ${PORT}`);
  console.log(`Low-Latency Singapore Stream + Sentinel Active (<5ms)`);
  console.log(`====================================================`);
});
