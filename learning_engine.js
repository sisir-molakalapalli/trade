/**
 * =========================================================================
 * SISIR QUANT TERMINAL - GOOGLE DRIVE JSON & SPREADSHEET LEDGER ENGINE
 * =========================================================================
 * Placed inside spreadsheet: "learning"
 * Web App URL: https://script.google.com/macros/s/AKfycbxzrxMMqOXehqTy3id5Z7Flr6Hbz9Tyz3XnDEgb8NaNPvO2RsvvUD-qNxKIEmOx3s9z/exec
 */

var LEARNING_STATE_FILE = "learning_state.json";
var PORTFOLIO_STATE_FILE = "cloud_portfolio_state.json";
var SHEET_NAME = "learning";

/**
 * Automatically locates the folder containing the "learning" spreadsheet
 * so JSON files are created right next to your spreadsheet in Google Drive.
 */
function getTargetFolder() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) {
      var ssFile = DriveApp.getFileById(ss.getId());
      var parents = ssFile.getParents();
      if (parents.hasNext()) {
        return parents.next();
      }
    }
  } catch (e) {}
  return DriveApp.getRootFolder();
}

/**
 * Searches the folder first, then Drive. Updates if existing, creates if missing.
 */
function saveOrUpdateDriveJsonFile(fileName, dataObj) {
  var content = (typeof dataObj === 'string') ? dataObj : JSON.stringify(dataObj, null, 2);
  var targetFolder = getTargetFolder();

  // 1. Check in target folder
  var folderFiles = targetFolder.getFilesByName(fileName);
  if (folderFiles.hasNext()) {
    var file = folderFiles.next();
    file.setContent(content);
    return file;
  }

  // 2. Check globally in Drive
  var globalFiles = DriveApp.getFilesByName(fileName);
  if (globalFiles.hasNext()) {
    var gFile = globalFiles.next();
    gFile.setContent(content);
    return gFile;
  }

  // 3. Create fresh file in the folder alongside the "learning" spreadsheet
  return targetFolder.createFile(fileName, content, MimeType.PLAIN_TEXT);
}

/**
 * Reads and parses a JSON file from Google Drive by name
 */
function readDriveJsonFile(fileName) {
  var targetFolder = getTargetFolder();
  var folderFiles = targetFolder.getFilesByName(fileName);
  if (folderFiles.hasNext()) {
    var file = folderFiles.next();
    try {
      return JSON.parse(file.getBlob().getDataAsString());
    } catch (e) {
      return null;
    }
  }

  var globalFiles = DriveApp.getFilesByName(fileName);
  if (globalFiles.hasNext()) {
    var gFile = globalFiles.next();
    try {
      return JSON.parse(gFile.getBlob().getDataAsString());
    } catch (e) {
      return null;
    }
  }
  return null;
}

/**
 * Handle incoming GET requests (Reading data & instant file creation)
 */
function doGet(e) {
  try {
    var action = (e && e.parameter && e.parameter.action) ? e.parameter.action : "GET_ALL";

    // Instant Creation & Verification of both JSON files in Google Drive
    if (action === "CREATE_ALL_FILES" || action === "TEST_DRIVE_PERMISSION" || action === "INIT_DRIVE_FILES") {
      var initialLearning = {
        assetReputations: {},
        bagSaturation24h: 0.0,
        dynamicHurdles: { scoreOffset: 0, wickCeiling: 0.40, minOddsRequired: 45 },
        failureClusters: [],
        recentAttributions: [],
        createdAt: new Date().toISOString(),
        lastUpdated: Date.now()
      };

      var initialPortfolio = {
        portfolio: { cashUSDT: 1000.00, realizedPnL: 0.00, bagSubsidyPoolUSDT: 0.00, sizingMode: "PERCENTAGE", allocationPct: 15.0 },
        activePositions: {},
        recoveryBags: {},
        cooldowns: {},
        createdAt: new Date().toISOString(),
        lastUpdated: Date.now()
      };

      var existingL = readDriveJsonFile(LEARNING_STATE_FILE);
      var file1 = saveOrUpdateDriveJsonFile(LEARNING_STATE_FILE, existingL || initialLearning);

      var existingP = readDriveJsonFile(PORTFOLIO_STATE_FILE);
      var file2 = saveOrUpdateDriveJsonFile(PORTFOLIO_STATE_FILE, existingP || initialPortfolio);

      return createJsonResponse({
        success: true,
        message: "Successfully created both JSON files in Google Drive!",
        folder: getTargetFolder().getName(),
        files: {
          learningState: { name: LEARNING_STATE_FILE, id: file1.getId(), url: file1.getUrl() },
          portfolioState: { name: PORTFOLIO_STATE_FILE, id: file2.getId(), url: file2.getUrl() }
        }
      });
    }

    if (action === "GET_LEARNING_STATE") {
      var learningData = readDriveJsonFile(LEARNING_STATE_FILE);
      return createJsonResponse({ success: true, source: "GOOGLE_DRIVE", fileName: LEARNING_STATE_FILE, data: learningData });
    }

    if (action === "GET_PORTFOLIO_STATE") {
      var portfolioData = readDriveJsonFile(PORTFOLIO_STATE_FILE);
      return createJsonResponse({ success: true, source: "GOOGLE_DRIVE", fileName: PORTFOLIO_STATE_FILE, data: portfolioData });
    }

    // Default: Return trades + Drive states
    var trades = readSheetTrades();
    var learningState = readDriveJsonFile(LEARNING_STATE_FILE);
    var portfolioState = readDriveJsonFile(PORTFOLIO_STATE_FILE);

    return createJsonResponse({
      success: true,
      trades: trades,
      learningState: learningState,
      portfolioState: portfolioState,
      driveFiles: {
        learningStateExists: learningState !== null,
        portfolioStateExists: portfolioState !== null
      }
    });

  } catch (err) {
    return createJsonResponse({ success: false, error: err.toString() });
  }
}

/**
 * Handle incoming POST requests (Writing data to Google Drive & Sheet)
 */
function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return createJsonResponse({ success: false, message: "Empty post body" });
    }

    var payload = JSON.parse(e.postData.contents);
    var action = payload.action || "";

    if (action === "SAVE_LEARNING_STATE" || action === "BACKUP_LEARNING_STATE") {
      var lData = payload.learningState || payload.data || payload;
      var lFile = saveOrUpdateDriveJsonFile(LEARNING_STATE_FILE, lData);
      return createJsonResponse({
        success: true,
        message: "Saved " + LEARNING_STATE_FILE + " in Drive",
        fileId: lFile.getId(),
        fileUrl: lFile.getUrl()
      });
    }

    if (action === "SAVE_PORTFOLIO_STATE" || action === "BACKUP_STATE") {
      var pData = payload.portfolioState || payload.portfolio || payload;
      var pFile = saveOrUpdateDriveJsonFile(PORTFOLIO_STATE_FILE, pData);
      return createJsonResponse({
        success: true,
        message: "Saved " + PORTFOLIO_STATE_FILE + " in Drive",
        fileId: pFile.getId(),
        fileUrl: pFile.getUrl()
      });
    }

    if (action === "CREATE_ALL_FILES" || action === "TEST_DRIVE_PERMISSION") {
      var f1 = saveOrUpdateDriveJsonFile(LEARNING_STATE_FILE, payload.learningState || { initialized: true });
      var f2 = saveOrUpdateDriveJsonFile(PORTFOLIO_STATE_FILE, payload.portfolioState || { initialized: true });
      return createJsonResponse({
        success: true,
        message: "Files created in Drive",
        files: [f1.getName(), f2.getName()]
      });
    }

    if (isValidTradePayload(payload)) {
      var sheetResult = appendTradeToSpreadsheet(payload);
      return createJsonResponse({ success: true, message: "Trade recorded to spreadsheet", trade: sheetResult });
    }

    return createJsonResponse({ success: true, message: "Handled without polluting sheet" });
  } catch (err) {
    return createJsonResponse({ success: false, error: err.toString() });
  }
}

function getTargetSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName("learning") || ss.getSheetByName("TradeLedger") || ss.getActiveSheet() || ss.getSheets()[0];
}

function isValidTradePayload(p) {
  if (!p || typeof p !== "object") return false;
  var pair = String(p.pair || p.Pair || p.symbol || "").trim().toUpperCase();
  if (!pair || pair.indexOf("USDT") === -1 || pair === "CRYPTO/USDT") return false;
  var entry = Number(p.entryPriceUSDT || p["Entry Price (USDT)"] || 0);
  var exit = Number(p.exitPriceUSDT || p["Exit Price (USDT)"] || 0);
  return (!isNaN(entry) && entry > 0 && !isNaN(exit) && exit > 0);
}

function appendTradeToSpreadsheet(trade) {
  var sheet = getTargetSheet();
  if (sheet.getLastRow() === 0) {
    sheet.appendRow([
      "ID", "Date", "Time", "Pair", "Type", 
      "Entry Price (USDT)", "Exit Price (USDT)", "Notional (USDT)", 
      "PnL (USDT)", "PnL (INR)", "Reason", "Regime", "Volatility %"
    ]);
  }

  sheet.appendRow([
    trade.id || Date.now(),
    trade.date || "Recent",
    trade.time || "",
    trade.pair || "",
    trade.type || "EXIT",
    Number(trade.entryPriceUSDT || 0),
    Number(trade.exitPriceUSDT || 0),
    Number(trade.notionalUSDT || 50),
    Number(trade.pnlUSDT || 0),
    Number(trade.pnlINR || 0),
    trade.reason || "Settled by quant engine",
    trade.regime || "BALANCED",
    trade.volatilityPct || 4.5
  ]);
  return trade;
}

function readSheetTrades() {
  var sheet = getTargetSheet();
  var data = sheet.getDataRange().getValues();
  if (!data || data.length <= 1) return [];

  var results = [];
  for (var i = 1; i < data.length; i++) {
    var row = data[i];
    var pair = String(row[3] || "").trim();
    var entry = Number(row[5]);
    var exit = Number(row[6]);

    if (pair && pair.indexOf("USDT") !== -1 && !isNaN(entry) && entry > 0 && !isNaN(exit) && exit > 0) {
      results.push({
        id: row[0] || i,
        date: String(row[1] || ""),
        time: String(row[2] || ""),
        pair: pair,
        type: String(row[4] || "EXIT"),
        entryPriceUSDT: entry,
        exitPriceUSDT: exit,
        notionalUSDT: Number(row[7] || 50),
        pnlUSDT: Number(row[8] || 0),
        pnlINR: Number(row[9] || 0),
        reason: String(row[10] || ""),
        regime: String(row[11] || "BALANCED"),
        volatilityPct: Number(row[12] || 4.5)
      });
    }
  }
  return results;
}

function createJsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
