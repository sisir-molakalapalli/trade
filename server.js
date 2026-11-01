// ... existing code ...
function parseCleanNumber(val) {
  if (typeof val === 'number') return isNaN(val) ? 0 : val;
  if (!val) return 0;
  const s = String(val).replace(/[^0-9.-]/g, '');
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

/**
 * Standardizes dates to Indian DD/MM/YYYY format.
 * Eliminates "Dec 30 1899" by recovering the true calendar date from the trade ID timestamp.
 */
function normalizeDateToIndianFormat(dateVal, id, timeVal) {
  let idDate = null;
  const idNum = Number(id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    idDate = new Date(idNum);
  }

  let s = String(dateVal || '').trim();
  const sTime = String(timeVal || '').trim();

  // 1. Intercept 1899 or empty or time-like date strings and recover directly from idDate
  if (!s || s === 'Recent' || s === '0' || s.includes('1899') || (s.includes(':') && !s.includes('/'))) {
    if (sTime && (sTime.includes('/') || sTime.includes('-')) && !sTime.includes('1899')) {
      s = sTime;
    } else if (idDate) {
      const d = String(idDate.getDate()).padStart(2, '0');
      const m = String(idDate.getMonth() + 1).padStart(2, '0');
      const y = idDate.getFullYear();
      return `${d}/${m}/${y}`;
    } else {
      return new Date().toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata' });
    }
  }

  // 2. Correct inverted US date strings from Date objects (e.g. "Sun Jan 11 2026...")
  if (s.includes('Jan 11') && idDate && idDate.getMonth() === 10) {
    const d = String(idDate.getDate()).padStart(2, '0');
    const m = String(idDate.getMonth() + 1).padStart(2, '0');
    const y = idDate.getFullYear();
    return `${d}/${m}/${y}`;
  }

  // 3. Slash or dot or dash separated dates (e.g. "1/11/2026", "01/11/2026", "31/10/2026")
  const slashMatch = s.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
  if (slashMatch) {
    const d = parseInt(slashMatch[1], 10);
    const m = parseInt(slashMatch[2], 10);
    let y = parseInt(slashMatch[3], 10);
    if (y < 2000 && idDate) y = idDate.getFullYear();
    return `${String(d).padStart(2, '0')}/${String(m).padStart(2, '0')}/${y}`;
  }

  // 4. ISO formatted dates (e.g. "2026-11-01" or inverted "2026-01-11")
  const isoMatch = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (isoMatch) {
    let year = parseInt(isoMatch[1], 10);
    let month = parseInt(isoMatch[2], 10);
    let day = parseInt(isoMatch[3], 10);
    if (year < 2000 && idDate) year = idDate.getFullYear();
    if (idDate && month === 1 && idDate.getMonth() === 10) {
      month = 11;
      day = 1;
    }
    return `${String(day).padStart(2, '0')}/${String(month).padStart(2, '0')}/${year}`;
  }

  // 5. Standard parseable date string
  const parsed = new Date(s);
  if (!isNaN(parsed.getTime())) {
    if (parsed.getFullYear() < 2000 && idDate) {
      const d = String(idDate.getDate()).padStart(2, '0');
      const m = String(idDate.getMonth() + 1).padStart(2, '0');
      const y = idDate.getFullYear();
      return `${d}/${m}/${y}`;
    }
    if (parsed.getMonth() === 0 && idDate && idDate.getMonth() === 10) {
      return `01/11/${parsed.getFullYear()}`;
    }
    const d = String(parsed.getDate()).padStart(2, '0');
    const m = String(parsed.getMonth() + 1).padStart(2, '0');
    const y = parsed.getFullYear();
    return `${d}/${m}/${y}`;
  }

  if (idDate) {
    const d = String(idDate.getDate()).padStart(2, '0');
    const m = String(idDate.getMonth() + 1).padStart(2, '0');
    const y = idDate.getFullYear();
    return `${d}/${m}/${y}`;
  }

  return s;
}

function normalizeTimeTo24h(timeVal, id) {
  const s = String(timeVal || '').trim();
  const timeMatch = s.match(/(\d{1,2}:\d{2}(?::\d{2})?)/);
  if (timeMatch) return timeMatch[1];

  const idNum = Number(id);
  if (!isNaN(idNum) && idNum > 1600000000000 && idNum < 2500000000000) {
    return new Date(idNum).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });
  }
  return s || '00:00:00';
}

async function fetchTradesFromLedgerWebApp() {
// ... existing code ...
  const headers = parseLine(lines[0]).map(h => h.toLowerCase().replace(/[^a-z0-9]/g, ''));
    for (let i = 1; i < lines.length; i++) {
      const cols = parseLine(lines[i]);
      const rowObj = {};
      headers.forEach((h, idx) => { rowObj[h] = cols[idx] || ''; });

      const rowId = cols[0] || Date.now() - i * 1000;
      const cleanDate = normalizeDateToIndianFormat(cols[1], rowId, cols[2]);
      const cleanTime = normalizeTimeTo24h(cols[2], rowId);

      const normalized = normalizeSheetTrade({
        id: rowId,
        date: cleanDate,
        time: cleanTime,
        pair: cols[3] || '',
        type: cols[4] || 'EXIT',
        entryPriceUSDT: cols[5],
        exitPriceUSDT: cols[6],
        pnlUSDT: cols[7],
        pnlINR: cols[8],
        regime: cols[9] || 'BALANCED',
        volatilityPct: cols[10] || 4.5,
        reason: cols[11] || '',
        ...rowObj
      });

      if (normalized && isValidTradeRecord(normalized)) {
        trades.push(normalized);
      }
    }
  }
  return trades;
}
// ... existing code ...
  logs.forEach(l => {
    if (!isValidTradeRecord(l)) return;
    const rawPair = String(l.pair || l.Pair || l.symbol || l.Symbol || l.asset || '').trim().toUpperCase();
    let entryUSDT = Number(l.entryPriceUSDT || l['Entry Price (USDT)'] || l.entryPrice || l.entry || 0);
    let exitUSDT = Number(l.exitPriceUSDT || l['Exit Price (USDT)'] || l.exitPrice || l.exit || 0);
    let id = Number(l.id) || Date.now();
    l.id = id;
    l.pair = rawPair.endsWith('USDT') && !rawPair.includes('/') ? `${rawPair.replace('USDT', '')}/USDT` : rawPair;
    l.date = normalizeDateToIndianFormat(l.date, l.id, l.time);
    l.time = normalizeTimeTo24h(l.time, l.id);

    let pnlUSDT = 0;
// ... existing code ...
  const pnlUSDT = parseCleanNumber(getField(['pnlUSDT', 'PnL (USDT)', 'pnl', 'pnlusdt']));
  const pnlINR = parseCleanNumber(getField(['pnlINR', 'PnL (INR)', 'pnlinr'])) || (pnlUSDT * (State.usdtToInrRate || 94.94));
  const notional = parseCleanNumber(getField(['notionalUSDT', 'Notional (USDT)', 'notional'])) || 50.00;
  const pair = rawPair.includes('/') ? rawPair : `${rawPair.replace('USDT', '')}/USDT`;

  const tradeId = Number(getField(['id', 'ID'])) || Date.now();
  const rawDate = String(getField(['date', 'Date'])) || 'Recent';
  const rawTime = String(getField(['time', 'Time'])) || '';
  const cleanDate = normalizeDateToIndianFormat(rawDate, tradeId, rawTime);
  const cleanTime = normalizeTimeTo24h(rawTime, tradeId);

  return {
    id: tradeId,
    date: cleanDate,
    time: cleanTime,
    pair,
    type: String(getField(['type', 'Type', 'action'])) || 'EXIT',
// ... existing code ...
  const primaryUrl = TRADE_LEDGER_URL || State.googleSheetUrl || GOOGLE_SHEET_URL;
  if (!primaryUrl) return;

  const tradeId = trade.id || Date.now();
  const payload = {
    id: tradeId,
    date: normalizeDateToIndianFormat(trade.date, tradeId),
    time: trade.time || new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }),
    pair: trade.pair || '',
    type: trade.type || 'EXIT',
// ... existing code ...
