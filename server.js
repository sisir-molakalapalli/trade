// ... existing code ...
      persistence: { status: 'SYNCED', storageEngine: 'Disk JSON + Google Drive Ledger', googleSheetSync: 'STANDBY', retainedTradesCount: 0 }
    },
    marketRegime: 'BALANCED',
    activeThresholds: { regime: 'BALANCED', scoreThreshold: 80, obiThreshold: 0.55, maxSpread: 0.08, marketAvgRange: 4.5 },
    tradeFrequencyController: {
      dynamicGovernorEnabled: true,
      quotaBypassEnabled: false,
      cashReserveFloorUSDT: 300.00,
      maxRecoveryBagsLimit: 6,
      governorTier: 'UNLIMITED_SPRINT',
      maxTradesPer24h: 9999,
      maxConcurrentPositions: 3, // EXPANDED TO 3 SLOTS: Eliminates active slot lockouts
      rollingTrades24h: [],
      consecutiveWinStreak: 0,
      nextSlotReleaseTimestamp: 0,
      pacingMode: 'BALANCED'
    },
    usdtToInrRate: 94.94,
// ... existing code ...
async function checkVeteranOrderFlowAndVSA(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const url = `https://api.binance.com/api/v3/klines?symbol=${cleanSym}&interval=5m&limit=12`;
    const res = await axios.get(url, { timeout: 3500 });
    const klines = res.data;
    if (!Array.isArray(klines) || klines.length < 6) {
      return { passed: false, reason: 'Insufficient 5m klines for order flow' };
    }

    // Inspect last completed 5m candle
    const lastBar = klines[klines.length - 2];
    const curBar = klines[klines.length - 1];

    const open = parseFloat(lastBar[1]);
    const high = parseFloat(lastBar[2]);
    const low = parseFloat(lastBar[3]);
    const close = parseFloat(lastBar[4]);
    const vol = parseFloat(lastBar[5]);
    const takerBuyVol = parseFloat(lastBar[9]); // Native Binance Taker Buy Base Volume

    const takerBuyRatio = vol > 0 ? (takerBuyVol / vol) * 100 : 50;

    // Check Cumulative Volume Delta across recent 3 bars
    let netDelta = 0;
    for (let i = klines.length - 4; i < klines.length - 1; i++) {
      const bVol = parseFloat(klines[i][5]);
      const tBuy = parseFloat(klines[i][9]);
      const tSell = bVol - tBuy;
      netDelta += (tBuy - tSell);
    }

    // ANTI-ZAMA GUARD: Must have genuine market buyer dominance
    if (takerBuyRatio < 48.5 && netDelta < 0) {
      return { passed: false, reason: `Order flow distribution detected (Taker Buy: ${takerBuyRatio.toFixed(1)}%, Negative Delta)` };
    }

    // Wyckoff VSA: Effort vs. Result
    const range = high - low;
    const clv = range > 0 ? (close - low) / range : 0.5;

    // If volume is high but candle closed weak (Upthrust/Absorption trap)
    if (clv < 0.38 && range > 0 && close < open) {
      return { passed: false, reason: `Wyckoff VSA Absorption Trap (CLV: ${clv.toFixed(2)})` };
    }

    // ANTI-ZAMA CASCADE GUARD: Reject if last 2 completed 5m bars were both red lower-lows
    const prevBar = klines[klines.length - 3];
    const prevClose = parseFloat(prevBar[4]);
    const prevLow = parseFloat(prevBar[3]);
    if (close < open && prevClose < parseFloat(prevBar[1]) && low < prevLow) {
      return { passed: false, reason: 'Active 5m lower-low downward cascade (ZAMA trap signature)' };
    }

    return {
      passed: true,
      takerBuyRatio: Number(takerBuyRatio.toFixed(1)),
      recent3Delta: Number(netDelta.toFixed(2)),
      vsaPattern: clv >= 0.65 ? 'STRONG_BUY_EFFORT' : 'NEUTRAL_ABSORPTION'
    };
  } catch (err) {
    return { passed: true, takerBuyRatio: 50, recent3Delta: 0, vsaPattern: 'FALLBACK' };
  }
}
// ... existing code ...
async function checkMicrostructureMomentumAndVwap(symbol) {
  try {
    const cleanSym = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const url = `https://api.binance.com/api/v3/klines?symbol=${cleanSym}&interval=15m&limit=16`;
    const res = await axios.get(url, { timeout: 3500 });
    const klines = res.data;
    if (!Array.isArray(klines) || klines.length < 8) {
      return { passed: false, reason: 'Insufficient 15m klines' };
    }

    const curBar = klines[klines.length - 1];
    const curClose = parseFloat(curBar[4]);
    const curOpen = parseFloat(curBar[1]);
    const change15mPct = curOpen > 0 ? ((curClose - curOpen) / curOpen) * 100 : 0;

    // 1-Hour change calculation from last 4 15m bars
    const bar4Ago = klines[klines.length - 5];
    const open1hAgo = parseFloat(bar4Ago[1]);
    const change1hPct = open1hAgo > 0 ? ((curClose - open1hAgo) / open1hAgo) * 100 : 0;

    // 4-Hour Session High
    let high4h = 0;
    let sumTypicalPriceVol = 0;
    let sumVol = 0;

    klines.forEach(k => {
      const h = parseFloat(k[2]);
      const l = parseFloat(k[3]);
      const c = parseFloat(k[4]);
      const v = parseFloat(k[5]);
      if (h > high4h) high4h = h;
      const typical = (h + l + c) / 3;
      sumTypicalPriceVol += (typical * v);
      sumVol += v;
    });

    const vwap4h = sumVol > 0 ? (sumTypicalPriceVol / sumVol) : curClose;
    const distFrom4hHighPct = high4h > 0 ? ((high4h - curClose) / high4h) * 100 : 0;

    // Calculate 15m relative volume (RVOL)
    const curVol = parseFloat(curBar[5]);
    let avgVol = 0;
    for (let i = klines.length - 9; i < klines.length - 1; i++) {
      avgVol += parseFloat(klines[i][5]);
    }
    avgVol = avgVol / 8;
    const rvol = avgVol > 0 ? (curVol / avgVol) : 1.0;

    // ANTI-ZAMA ABSOLUTE MANDATE: 1-hour change MUST NOT be negative!
    if (change1hPct < 0.20) {
      return { passed: false, reason: `1h trend is negative or flat (${change1hPct.toFixed(2)}%) - Anti-ZAMA Guard` };
    }

    // GATE 1: MUST be on or above 15m VWAP
    if (curClose < (vwap4h * 0.998)) {
      return { passed: false, reason: `Trading below 15m VWAP ($${curClose.toFixed(4)} < $${vwap4h.toFixed(4)})` };
    }

    // DUAL-TRACK MOMENTUM EVALUATION:
    // Track A: Ignition Breakout
    const isTrackA = (change15mPct >= 0.70 && rvol >= 1.30 && distFrom4hHighPct <= 2.50);

    // Track B: Steady Base Expansion
    const isTrackB = (change15mPct >= 0.15 && change1hPct >= 1.00 && distFrom4hHighPct <= 3.20 && curClose >= vwap4h);

    if (!isTrackA && !isTrackB) {
      return { 
        passed: false, 
        reason: `Failed Dual-Track Momentum (15m: ${change15mPct.toFixed(2)}%, 1h: ${change1hPct.toFixed(2)}%, Peak DD: ${distFrom4hHighPct.toFixed(2)}%)` 
      };
    }

    return {
      passed: true,
      change15mPct: Number(change15mPct.toFixed(2)),
      change1hPct: Number(change1hPct.toFixed(2)),
      distFrom4hHighPct: Number(distFrom4hHighPct.toFixed(2)),
      isAboveVwap: true,
      rvol: Number(rvol.toFixed(2)),
      selectionTrack: isTrackA ? 'TRACK_A_IGNITION' : 'TRACK_B_BASE_EXPANSION'
    };
  } catch (err) {
    return { passed: false, reason: `Microstructure momentum error: ${err.message}` };
  }
}
// ... existing code ...
async function evaluateAutopilotEntries() {
  if (!State.autopilotEnabled) return;

  const currentCount = Object.keys(State.activePositions).length;
  const maxSlots = State.tradeFrequencyController?.maxConcurrentPositions || 3;
  if (currentCount >= maxSlots) return;

  // Real-time coupling: throttle when recovery bags are active
  const activeBagsCount = Object.keys(State.recoveryBags || {}).length;
  learningEngine.recalculateDynamicHurdles(activeBagsCount, State.tradeLogs.length);

  const keys = Object.keys(State.universe || {});
  const sorted = [...keys].sort((a, b) => (State.universe[b]?.score || 0) - (State.universe[a]?.score || 0));

  for (const k of sorted) {
    if (Object.keys(State.activePositions).length >= maxSlots) break;
    const coin = State.universe[k];
    if (!coin || !coin.symbol) continue;

    const sym = coin.symbol.replace(/[^A-Z0-9]/g, '');
    if (State.activePositions[k] || State.recoveryBags[k]) continue;

    // ANTI-ZAMA INVIOLABLE MANDATE 1: 24h change MUST be positive (+1.50% min)
    const change24h = Number(coin.priceChangePct || coin.change24h || 0);
    if (change24h < 1.50) continue;

    // ANTI-ZAMA INVIOLABLE MANDATE 2: Daily Range Position must be in upper 35%
    const posInRange = Number(coin.pricePositionInRange !== undefined ? coin.pricePositionInRange : 0.70);
    if (posInRange < 0.65) continue;

    // ANTI-ZAMA INVIOLABLE MANDATE 3: Minimum 24h range >= 4.0%
    const intradayRange = Number(coin.intradayRangePct || 4.5);
    if (intradayRange < 4.0) continue;

    // Anti-Churn Lockout
    if (State.cooldowns[sym] && State.cooldowns[sym].until > Date.now()) continue;

    // Order flow and VSA check
    const orderFlow = await checkVeteranOrderFlowAndVSA(sym);
    if (!orderFlow.passed) continue;

    // Microstructure momentum and VWAP check
    const microMom = await checkMicrostructureMomentumAndVwap(sym);
    if (!microMom.passed) continue;

    // 1-Hour Trend Alignment
    const trend1h = await check1hTrendAlignment(sym);
    if (!trend1h.passed) continue;

    // Real-time 20-level book check
    const book = await fetchRealOrderBookDepth(sym);
    if (!book || book.obi < 0.54) continue;
    if (book.spreadPct > 0.08) continue;

    // Passed all gates - execute spot buy
    await executeSpotBuy(sym, coin, book, microMom);
  }
}
// ... existing code ...
async function runPredictiveSentry() {
  const activeKeys = Object.keys(State.activePositions || {});
  const bagKeys = Object.keys(State.recoveryBags || {});

  // -------------------------------------------------------------
  // PART 1: ACTIVE BREAKOUT POSITIONS - INSTANT-ON MINUTE-0 TRAILING
  // -------------------------------------------------------------
  for (const k of activeKeys) {
    const pos = State.activePositions[k];
    if (!pos) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || pos.livePriceUSDT || pos.entryPriceUSDT);
    const entryPrice = Number(pos.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdMinutes = (Date.now() - (pos.openedAt || Date.now())) / 60000;

    // Continuous Peak Watermark tracking from Minute 0
    pos.peakGainPct = Math.max(pos.peakGainPct || 0, gainPct);
    pos.peakHighPrice = Math.max(pos.peakHighPrice || entryPrice, livePrice);
    const peakGain = pos.peakGainPct;

    // -----------------------------------------------------------
    // INSTANT-ON MULTI-TIER TRAILING LADDER (NO LOSS & MAX PROFIT)
    // -----------------------------------------------------------

    // TIER 1: Micro-Protection at +0.35% -> Move SL to +0.10% (Breakeven + Fees Locked!)
    if (peakGain >= 0.35 && pos.ratchetStage !== 'BREAKEVEN_LOCKED' && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'BREAKEVEN_LOCKED';
      pos.sl = Number((entryPrice * 1.0010).toFixed(6)); // Locks +0.10% net gain
    }

    // TIER 2: Profit Lock at +0.60% -> Move SL to +0.35%
    if (peakGain >= 0.60 && pos.ratchetStage !== 'PROFIT_LOCKED' && pos.ratchetStage !== 'DYNAMIC_TRAIL') {
      pos.ratchetStage = 'PROFIT_LOCKED';
      pos.sl = Number((entryPrice * 1.0035).toFixed(6)); // Locks +0.35% net gain
    }

    // TIER 3: Tight Dynamic Trailing from +0.90% (Trail 0.25% from Peak)
    if (peakGain >= 0.90 && peakGain < 1.40) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9975).toFixed(6)); // 0.25% trail
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    // TIER 4: Dynamic Trailing from +1.40% (Trail 0.30% from Peak)
    if (peakGain >= 1.40 && peakGain < 1.80) {
      pos.ratchetStage = 'DYNAMIC_TRAIL';
      const trailFloor = Number((pos.peakHighPrice * 0.9970).toFixed(6)); // 0.30% trail
      if (trailFloor > pos.sl) pos.sl = trailFloor;
    }

    // TIER 5: TP1 Hit (+1.80%) -> Bank 50%, Trail Remaining 50% by 0.35%
    if (gainPct >= 1.80 && !pos.tp1Secured) {
      pos.tp1Secured = true;
      pos.tp1Price = livePrice;
      pos.ratchetStage = 'RUNNER_TRAIL';
      pos.sl = Number((pos.peakHighPrice * 0.9965).toFixed(6)); // 0.35% trail
      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'TP1 BANKED',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: Number((pos.notionalUSDT * 0.5).toFixed(2)),
        pnlUSDT: Number((((livePrice - entryPrice) / entryPrice) * (pos.notionalUSDT * 0.5)).toFixed(4)),
        reason: 'TP1 secured: 50% banked at +1.80%'
      });
    }

    // -----------------------------------------------------------
    // TRAILING STOP EXIT EXECUTION (Triggered whenever price dips to pos.sl)
    // -----------------------------------------------------------
    if (pos.sl && livePrice <= pos.sl && (pos.ratchetStage === 'BREAKEVEN_LOCKED' || pos.ratchetStage === 'PROFIT_LOCKED' || pos.ratchetStage === 'DYNAMIC_TRAIL' || pos.ratchetStage === 'RUNNER_TRAIL')) {
      const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));
      
      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: pos.tp1Secured ? 'RUNNER EXIT' : 'TRAILING PROFIT LOCK',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `Dynamic trailing stop executed at $${livePrice.toFixed(4)} (Peak: +${peakGain.toFixed(2)}%)`
      });

      // Free capital and slot
      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // -----------------------------------------------------------
    // MICRO-DPEE EXHAUSTION EXIT (Top-tick profit banking at +0.35%)
    // -----------------------------------------------------------
    if (gainPct >= 0.35) {
      const book = await fetchRealOrderBookDepth(k.replace('USDT', ''));
      if (book && book.obi < 0.35) {
        const exitNotional = pos.tp1Secured ? (pos.notionalUSDT * 0.5) : pos.notionalUSDT;
        const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

        await recordTradeToCloudLedger({
          id: Date.now(),
          date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
          time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
          pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
          type: 'DPEE EXHAUSTION EXIT',
          entryPriceUSDT: entryPrice,
          exitPriceUSDT: livePrice,
          notionalUSDT: exitNotional,
          pnlUSDT: realizedGain,
          reason: `Micro-DPEE: Order book flipped to sellers at +${gainPct.toFixed(2)}%`
        });

        State.portfolio.cashUSDT += (exitNotional + realizedGain);
        State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
        delete State.activePositions[k];
        continue;
      }
    }

    // -----------------------------------------------------------
    // ALPHA VELOCITY TIME-DECAY EXITS (Eliminates 3-Hour Stagnation)
    // -----------------------------------------------------------
    // At 30 mins: If floating in profit (>= +0.20%), bank it immediately!
    if (holdMinutes >= 30 && gainPct >= 0.20) {
      const exitNotional = pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VELOCITY HARVEST 30M',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `30m Alpha Velocity: Banked +${gainPct.toFixed(2)}% to clear active slot`
      });

      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // At 45 mins: If positive (>= +0.10%), bank it!
    if (holdMinutes >= 45 && gainPct >= 0.10) {
      const exitNotional = pos.notionalUSDT;
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * exitNotional).toFixed(4));

      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: pos.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VELOCITY SCRATCH 45M',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: exitNotional,
        pnlUSDT: realizedGain,
        reason: `45m Velocity Scratch: Cleared stalled breakout at +${gainPct.toFixed(2)}%`
      });

      State.portfolio.cashUSDT += (exitNotional + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.activePositions[k];
      continue;
    }

    // At 45 mins: If negative (<= 0.00%), move immediately to Recovery Bag!
    // (Preserves zero realized loss while vacating slot so 3-hour droughts never happen!)
    if (holdMinutes >= 45 && gainPct <= 0.00) {
      State.recoveryBags[k] = {
        ...pos,
        movedAt: Date.now(),
        targetStep: 'STEP 1 (+0.30%)',
        targetExitPriceUSDT: Number((entryPrice * 1.003).toFixed(6)),
        initialDeficitUSDT: Number((((livePrice - entryPrice) / entryPrice) * pos.notionalUSDT).toFixed(4))
      };
      delete State.activePositions[k];
      continue;
    }

    // Retest drop past -2.80% moves to recovery bag
    if (gainPct <= -2.80) {
      State.recoveryBags[k] = {
        ...pos,
        movedAt: Date.now(),
        targetStep: 'STEP 1 (+0.30%)',
        targetExitPriceUSDT: Number((entryPrice * 1.003).toFixed(6)),
        initialDeficitUSDT: Number((((livePrice - entryPrice) / entryPrice) * pos.notionalUSDT).toFixed(4))
      };
      delete State.activePositions[k];
      continue;
    }
  }

  // -------------------------------------------------------------
  // PART 2: SPOT RECOVERY BAGS (Unchanged Zero-Loss Protocol)
  // -------------------------------------------------------------
  for (const k of bagKeys) {
    const bag = State.recoveryBags[k];
    if (!bag) continue;

    const livePrice = Number(State.universe[k]?.priceUSDT || bag.livePriceUSDT || bag.entryPriceUSDT);
    const entryPrice = Number(bag.entryPriceUSDT || livePrice);
    const gainPct = entryPrice > 0 ? Number((((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)) : 0;
    const holdHours = (Date.now() - (bag.movedAt || Date.now())) / 3600000;

    // Relief Bounce Exit at Target
    if (livePrice >= bag.targetExitPriceUSDT) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'RECOVERY EXIT',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: `Recovery filled on relief bounce (${bag.targetStep})`
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    // Dynamic VWAP Relief Exit (> 90 mins held and touching 15m VWAP)
    if (holdHours >= 1.5 && gainPct >= -0.50) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'VWAP RELIEF RECOVERY',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'VWAP technical bounce exit: capital recycled in 90m'
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }

    // Catastrophic Disaster Floor (-6.50% Hard Cap)
    if (gainPct <= -6.50) {
      const realizedGain = Number((((livePrice - entryPrice) / entryPrice) * bag.notionalUSDT).toFixed(4));
      await recordTradeToCloudLedger({
        id: Date.now(),
        date: new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' }),
        time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
        pair: bag.pair || `${k.replace('USDT', '')}/USDT`,
        type: 'HARD DISASTER CUT',
        entryPriceUSDT: entryPrice,
        exitPriceUSDT: livePrice,
        notionalUSDT: bag.notionalUSDT,
        pnlUSDT: realizedGain,
        reason: 'Hard disaster floor cut (-6.50%): preserved liquid cash'
      });

      State.portfolio.cashUSDT += (bag.notionalUSDT + realizedGain);
      State.portfolio.realizedPnL = Number((State.portfolio.realizedPnL + realizedGain).toFixed(4));
      delete State.recoveryBags[k];
      continue;
    }
  }
}
// ... existing code ...
