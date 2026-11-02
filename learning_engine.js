/**
 * =========================================================================
 * AUTONOMOUS SELF-LEARNING ENGINE (BAYESIAN FEEDBACK & HURDLE CONTROLLER)
 * =========================================================================
 * - Real-time active bag saturation coupling (throttles hurdles on dead inventory)
 * - Asset reputation credit scoring (rewards clean runners, penalizes bleeders)
 * - Closed-loop disk persistence to learning_state.json
 * =========================================================================
 */

const fs = require('fs');
const path = require('path');

const STATE_FILE = path.join(__dirname, 'learning_state.json');

class LearningEngine {
  constructor() {
    this.state = {
      assetReputations: {},
      bagSaturation24h: 0.0,
      dynamicHurdles: {
        scoreOffset: 0,
        wickCeiling: 0.40,
        minOddsRequired: 45
      },
      failureClusters: [],
      recentAttributions: [],
      avgPeakEfficiencyPct: 90,
      lastUpdated: Date.now()
    };

    this.googleDriveSyncFn = null;
    this.loadState();
    this.initializeUniverseTokens();
  }

  setGoogleDriveSyncCallback(fn) {
    this.googleDriveSyncFn = fn;
  }

  initializeUniverseTokens(universeKeys = []) {
    const defaultTokens = [
      'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'AVAXUSDT', 'NEARUSDT',
      'LINKUSDT', 'BNBUSDT', 'DASHUSDT', 'SUIUSDT', 'PEPEUSDT',
      'HYPEUSDT', 'BEMXUSDT', 'ZROUSDT', 'HAEDALUSDT'
    ];
    const allKeys = new Set([...universeKeys, ...defaultTokens]);
    allKeys.forEach(k => {
      const clean = String(k).replace(/[^A-Z0-9]/g, '');
      if (clean && clean.endsWith('USDT') && clean !== 'CRYPTOUSDT') {
        this.getAssetReputation(clean);
      }
    });
  }

  trainOnHistoricalTrades(tradeLogs, universeKeys = []) {
    this.initializeUniverseTokens(universeKeys);

    if (!Array.isArray(tradeLogs) || tradeLogs.length === 0) {
      this.recalculateDynamicHurdles();
      this.saveState();
      return;
    }

    const chronological = [...tradeLogs].sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));

    chronological.forEach(trade => {
      const rawPair = String(trade.pair || trade.symbol || '').trim().toUpperCase();
      const cleanSym = rawPair.replace(/[^A-Z0-9]/g, '');
      if (!cleanSym || cleanSym === 'CRYPTOUSDT' || !cleanSym.endsWith('USDT')) return;

      const pnl = Number(trade.pnlUSDT) || 0;
      const type = String(trade.type || 'EXIT').toUpperCase();

      let outcomeType = 'OTHER';
      if (type.includes('TP1') || type.includes('RUNNER') || type.includes('PROFIT') || type.includes('VELOCITY')) {
        outcomeType = 'TP1';
      } else if (type.includes('DISASTER') || type.includes('-6.50%') || pnl < -1.5) {
        outcomeType = 'HARD DISASTER';
      } else if (type.includes('RECOVERY') || type.includes('STEP')) {
        outcomeType = 'RECOVERY';
      } else if (pnl >= 0) {
        outcomeType = 'TP1';
      } else {
        outcomeType = 'RECOVERY';
      }

      const rep = this.getAssetReputation(cleanSym);
      let multiplier = 1.0;
      if (outcomeType === 'TP1') {
        multiplier = 1.10;
        rep.wins = (rep.wins || 0) + 1;
      } else if (outcomeType === 'HARD DISASTER' || pnl < -2.0) {
        multiplier = 0.40;
        rep.disasters = (rep.disasters || 0) + 1;
      } else {
        multiplier = 0.95;
        rep.bags = (rep.bags || 0) + 1;
      }

      rep.repScore = Number(Math.max(0.20, Math.min(1.50, rep.repScore * multiplier)).toFixed(3));
      rep.lastUpdated = Number(trade.id) || Date.now();
    });

    this.recalculateDynamicHurdles();
    this.saveState();
  }

  loadState() {
    try {
      if (fs.existsSync(STATE_FILE)) {
        const raw = fs.readFileSync(STATE_FILE, 'utf-8');
        if (raw && raw.trim().length > 0) {
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed === 'object') {
            if (parsed.assetReputations) this.state.assetReputations = parsed.assetReputations;
            if (parsed.bagSaturation24h !== undefined) this.state.bagSaturation24h = parsed.bagSaturation24h;
            if (parsed.dynamicHurdles) this.state.dynamicHurdles = { ...this.state.dynamicHurdles, ...parsed.dynamicHurdles };
            if (Array.isArray(parsed.failureClusters)) this.state.failureClusters = parsed.failureClusters;
            if (Array.isArray(parsed.recentAttributions)) this.state.recentAttributions = parsed.recentAttributions;
            if (parsed.avgPeakEfficiencyPct !== undefined) this.state.avgPeakEfficiencyPct = parsed.avgPeakEfficiencyPct;
          }
        }
      }
    } catch (e) {
      console.error('[LearningEngine] Notice: starting fresh state:', e.message);
    }
  }

  saveState() {
    try {
      this.state.lastUpdated = Date.now();
      fs.writeFileSync(STATE_FILE, JSON.stringify(this.state, null, 2), 'utf-8');

      if (typeof this.googleDriveSyncFn === 'function') {
        this.googleDriveSyncFn({
          action: 'SAVE_LEARNING_STATE',
          learningState: this.state,
          timestamp: Date.now()
        });
      }
    } catch (e) {
      console.error('[LearningEngine] Error saving state:', e.message);
    }
  }

  createEntrySnapshot(sym, coinData, book, shortTerm, btcMacro) {
    return {
      timestamp: Date.now(),
      symbol: sym,
      entryPriceUSDT: coinData?.priceUSDT || 0,
      features: {
        compositeScore: coinData?.score || 78,
        obi: book?.obi ? Number(book.obi.toFixed(2)) : 0.55,
        spreadPct: book?.spreadPct ? Number(book.spreadPct.toFixed(3)) : 0.05,
        change15mPct: shortTerm?.change15mPct || 0,
        change1hPct: shortTerm?.change1hPct || 0,
        isAbove1hEma: shortTerm?.isAbove1hEma !== undefined ? shortTerm.isAbove1hEma : true,
        distFrom4hHighPct: shortTerm?.distFrom4hHighPct || 0,
        isAboveVwap: shortTerm?.isAboveVwap !== undefined ? shortTerm.isAboveVwap : true,
        selectionTrack: shortTerm?.selectionTrack || 'STANDARD',
        takerBuyRatio: shortTerm?.takerBuyRatio || 50,
        recent3Delta: shortTerm?.recent3Delta || 0,
        vsaPattern: shortTerm?.vsaPattern || 'ACCUMULATION',
        change24hPct: coinData?.priceChangePct || coinData?.change24h || 0,
        pricePositionInRange: coinData?.pricePositionInRange || 0.7,
        rvol: shortTerm?.rvol || 1.0,
        btc15mMomentum: btcMacro?.btcChange15m || 0
      }
    };
  }

  getAssetReputation(sym) {
    const cleanSym = String(sym).replace(/[^A-Z0-9]/g, '');
    if (!this.state.assetReputations[cleanSym]) {
      this.state.assetReputations[cleanSym] = {
        repScore: 1.0,
        wins: 0,
        bags: 0,
        disasters: 0,
        avgPeakEfficiency: 90,
        lastUpdated: Date.now()
      };
    }
    return this.state.assetReputations[cleanSym];
  }

  recordTradeOutcome(sym, outcomeType, pnlUSDT, holdHours, entrySnapshot, peakEfficiencyPct) {
    const cleanSym = String(sym).replace(/[^A-Z0-9]/g, '');
    const rep = this.getAssetReputation(cleanSym);
    const pnl = Number(pnlUSDT) || 0;
    const hours = Number(holdHours) || 0;
    const eff = (typeof peakEfficiencyPct === 'number' && !isNaN(peakEfficiencyPct)) ? peakEfficiencyPct : null;

    let multiplier = 1.0;
    let attributionReason = '';

    if (outcomeType.includes('TP1') || outcomeType.includes('RUNNER') || outcomeType.includes('VELOCITY') || outcomeType.includes('TRAILING')) {
      multiplier = 1.12;
      rep.wins++;
      attributionReason = `Trailing / velocity win (+${pnl.toFixed(2)}$). Credit score boosted.`;
    } else if (outcomeType.includes('HARD DISASTER') || outcomeType.includes('-6.50%') || pnl < -2.0) {
      multiplier = 0.50;
      rep.disasters++;
      attributionReason = `Catastrophic breakdown cut ($${pnl.toFixed(2)}). Severe penalty applied.`;
    } else if (outcomeType.includes('RECOVERY') || outcomeType.includes('STEP') || outcomeType.includes('VWAP RELIEF')) {
      multiplier = hours >= 12 ? 0.88 : 0.96;
      attributionReason = `Routine bag resolution (${hours.toFixed(1)}h). Capital preserved.`;
    }

    if (eff !== null) {
      rep.avgPeakEfficiency = Math.round((Number(rep.avgPeakEfficiency || 90) * 0.7) + (eff * 0.3));
    }

    rep.repScore = Number(Math.max(0.20, Math.min(1.50, rep.repScore * multiplier)).toFixed(3));
    rep.lastUpdated = Date.now();

    this.state.recentAttributions.push({
      id: Date.now(),
      timestamp: Date.now(),
      symbol: cleanSym,
      pnlUSDT: pnl,
      type: outcomeType,
      peakEfficiencyPct: eff,
      repScoreAfter: rep.repScore,
      reason: attributionReason
    });

    if (this.state.recentAttributions.length > 100) {
      this.state.recentAttributions = this.state.recentAttributions.slice(-100);
    }

    this.recalculateDynamicHurdles();
    this.saveState();
  }

  recalculateDynamicHurdles(activeBagsCount = 0, rollingTrades24hCount = 0) {
    let offset = 0;
    if (activeBagsCount > 0) {
      offset = Math.min(6, activeBagsCount * 2);
    }
    this.state.dynamicHurdles = {
      scoreOffset: offset,
      wickCeiling: 0.40,
      minOddsRequired: 45
    };
  }

  evaluateCandidateEligibility(sym, candidate, book, btcMacro) {
    const cleanSym = String(sym).replace(/[^A-Z0-9]/g, '');
    const rep = this.getAssetReputation(cleanSym);

    const repPenaltyOffset = rep.repScore < 1.0 
      ? Math.round((1.0 - rep.repScore) * 12)
      : 0;

    const repBonus = rep.repScore > 1.15 ? -2 : 0;
    const netScoreHurdleOffset = this.state.dynamicHurdles.scoreOffset + repPenaltyOffset + repBonus;

    if (rep.repScore <= 0.40 && rep.disasters >= 2) {
      return {
        allowed: false,
        reason: `Blacklist: ${cleanSym} Credit Rating too low (${rep.repScore.toFixed(2)})`
      };
    }

    return {
      allowed: true,
      repScore: rep.repScore,
      netScoreHurdleOffset,
      effectiveWickCeiling: this.state.dynamicHurdles.wickCeiling,
      minRequiredOdds: this.state.dynamicHurdles.minOddsRequired
    };
  }
}

module.exports = new LearningEngine();
