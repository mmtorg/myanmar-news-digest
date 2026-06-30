/**
 * =========================
 * GitHub Actions Dispatcher
 * =========================
 *
 * 使い方:
 * - Script Properties に以下を設定:
 *   GITHUB_OWNER
 *   GITHUB_REPO
 *   GITHUB_WORKFLOW_FILE   例: sheet-pipeline-send.yml
 *   GITHUB_TOKEN
 *
 * - schedule-watch-build-send のGAS監視でスプレッドシートを読むため、
 *   可能であれば以下も設定する:
 *   MNA_SHEET_ID           対象スプレッドシートID
 *   ※スプレッドシートに紐づいたApps Scriptの場合は、未設定でもActiveSpreadsheetを使用する。
 *
 * - 本番: main ブランチ → production 環境 → prod シート
 * - テスト: develop ブランチ → development 環境 → dev シート
 *
 * 運用:
 * - Apps Script の時間主導型トリガーは prodScheduleTick を「1分おき」に1つだけ設定する。
 * - 指定時刻のみ記事収集を workflow_dispatch する。
 * - schedule-watch-build-send は、02:05〜11:00 MMT の間、GAS側でK列を監視する。
 * - K列に a があり、config!A1 が当日未送信の場合だけ watch-build-send を workflow_dispatch する。
 * - 12:30 は collect16 と同じく、既存シートを整理してから当日分を収集する。
 * - 13:30〜23:20 は当日分、00:10 は前日分を GitHub Actions 側で判定する。
 * - 00:45 / 01:15 / 01:45 / 02:00 は前日分の Khit Thit Media のみ収集する。
 * - Irrawaddy は sheet_pipeline.py 側で 21:30 / 22:30 / 23:20 のみ収集する。
 */

const TZ = "Asia/Yangon";

/**
 * 収集枠の定義。
 * cron は GitHub Actions の schedule_cron 入力として渡す識別子。
 * 実際の時刻制御は Apps Script 側の prodScheduleTick / devScheduleTick で行う。
 */
const COLLECT_SLOTS = [
  // 初回は既存 collect16 と同じ処理（シート整理あり）
  { hhmm: "12:30", cron: "0 6 * * *", mode: "collect16" },

  // 当日分取得
  // 一時停止: 13:30の記事収集は停止中。再開する場合は次の行のコメントアウトを解除する。
  // { hhmm: "13:30", cron: "0 7 * * *", mode: "collect" },
  { hhmm: "14:30", cron: "0 8 * * *", mode: "collect" },
  { hhmm: "15:30", cron: "0 9 * * *", mode: "collect" },
  { hhmm: "16:30", cron: "0 10 * * *", mode: "collect" },
  { hhmm: "17:30", cron: "0 11 * * *", mode: "collect" },
  { hhmm: "18:30", cron: "0 12 * * *", mode: "collect" },
  { hhmm: "19:30", cron: "0 13 * * *", mode: "collect" },
  { hhmm: "20:30", cron: "0 14 * * *", mode: "collect" },
  { hhmm: "21:30", cron: "0 15 * * *", mode: "collect" },
  { hhmm: "22:30", cron: "0 16 * * *", mode: "collect" },
  { hhmm: "23:20", cron: "50 16 * * *", mode: "collect" },

  // 前日分取得
  { hhmm: "00:10", cron: "40 17 * * *", mode: "collect" },

  // 前日分取得（Khit Thit Media のみ）
  { hhmm: "00:45", cron: "15 18 * * *", mode: "collect" },
  { hhmm: "01:15", cron: "45 18 * * *", mode: "collect" },
  { hhmm: "01:45", cron: "15 19 * * *", mode: "collect" },
  { hhmm: "02:00", cron: "30 19 * * *", mode: "collect" },
];

/**
 * bundle生成・送信処理。
 * GitHub Actions の schedule ではなく、GAS から workflow_dispatch する。
 * 監視自体は WATCH_BUILD_SEND_START_HHMM〜WATCH_BUILD_SEND_END_HHMM の間、GAS側で行う。
 */
const WATCH_BUILD_SEND_SLOT = {
  hhmm: "02:05",
  cron: "35 19 * * *",
  mode: "watch-build-send",
};

// 既存の手動テスト関数互換のため配列も残す。
const WATCH_BUILD_SEND_SLOTS = [WATCH_BUILD_SEND_SLOT];

const SLOT_WINDOW_MINUTES = 5;

// schedule-watch-build-send の監視時間帯（MMT）
const WATCH_BUILD_SEND_START_HHMM = "02:05";
const WATCH_BUILD_SEND_END_HHMM = "11:00";

// watch-build-send のトリガー条件
const WATCH_TRIGGER_SHEET_PROD = "prod";
const WATCH_TRIGGER_SHEET_DEV = "dev";
const WATCH_TRIGGER_COLUMN = 11; // K列
const WATCH_TRIGGER_VALUE = "a";

// GitHub Actions側と同じ送信済みガード
const WATCH_SENT_MARK_CELL = "config!A1";

/** GitHub リクエスト共通 */
function callGithubWorkflowDispatch_(ref, mode, scheduleCron) {
  const props = PropertiesService.getScriptProperties();
  const owner = mustGetProp_(props, "GITHUB_OWNER");
  const repo = mustGetProp_(props, "GITHUB_REPO");
  const workflowFile = mustGetProp_(props, "GITHUB_WORKFLOW_FILE");
  const token = mustGetProp_(props, "GITHUB_TOKEN");

  const url =
    `https://api.github.com/repos/${encodeURIComponent(owner)}/` +
    `${encodeURIComponent(repo)}/actions/workflows/` +
    `${encodeURIComponent(workflowFile)}/dispatches`;

  const payload = {
    ref: ref,
    inputs: {
      mode: mode,
      schedule_cron: scheduleCron || "",
    },
  };

  const res = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  });

  const code = res.getResponseCode();
  const body = res.getContentText();

  Logger.log(`dispatch status=${code}`);
  Logger.log(body);

  if (code !== 204) {
    throw new Error(`GitHub workflow_dispatch failed: HTTP ${code} / ${body}`);
  }
}

/** Script Properties 必須取得 */
function mustGetProp_(props, key) {
  const value = props.getProperty(key);
  if (!value) {
    throw new Error(`Script Property "${key}" is not set.`);
  }
  return value;
}

/** 重複実行防止つき dispatch */
function dispatchOncePerDay_(ref, slot, slotKey) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const props = PropertiesService.getScriptProperties();
    const today = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd");
    const propKey = `LAST_DISPATCH_${slotKey}`;
    const last = props.getProperty(propKey);

    if (last === today) {
      Logger.log(`skip duplicate: ${propKey} already dispatched for ${today}`);
      return;
    }

    callGithubWorkflowDispatch_(ref, slot.mode, slot.cron);
    props.setProperty(propKey, today);
    Logger.log(
      `dispatched: ${slotKey} ${today} ref=${ref} mode=${slot.mode} cron=${slot.cron}`,
    );
  } finally {
    lock.releaseLock();
  }
}

function dispatchProdCollect_(slot) {
  dispatchOncePerDay_(
    "main",
    slot,
    `prod_${slot.mode}_${slot.hhmm.replace(":", "")}`,
  );
}

function dispatchDevCollect_(slot) {
  dispatchOncePerDay_(
    "develop",
    slot,
    `dev_${slot.mode}_${slot.hhmm.replace(":", "")}`,
  );
}

function dispatchProdWatchBuildSend_(slot) {
  dispatchOncePerDay_(
    "main",
    slot,
    `prod_${slot.mode}_${slot.hhmm.replace(":", "")}`,
  );
}

function dispatchDevWatchBuildSend_(slot) {
  dispatchOncePerDay_(
    "develop",
    slot,
    `dev_${slot.mode}_${slot.hhmm.replace(":", "")}`,
  );
}

function currentMinutesInTz_() {
  const now = new Date();
  return (
    Number(Utilities.formatDate(now, TZ, "H")) * 60 +
    Number(Utilities.formatDate(now, TZ, "m"))
  );
}

function hhmmToMinutes_(hhmm) {
  const parts = hhmm.split(":");
  return Number(parts[0]) * 60 + Number(parts[1]);
}

function isNowInSlot_(slot) {
  const currentMinutes = currentMinutesInTz_();
  const slotMinutes = hhmmToMinutes_(slot.hhmm);

  return (
    currentMinutes >= slotMinutes &&
    currentMinutes < slotMinutes + SLOT_WINDOW_MINUTES
  );
}

function isNowInTimeRange_(startHhmm, endHhmm) {
  const current = currentMinutesInTz_();
  const start = hhmmToMinutes_(startHhmm);
  const end = hhmmToMinutes_(endHhmm);

  if (start <= end) {
    return current >= start && current < end;
  }

  // 日付をまたぐ時間帯にも対応
  return current >= start || current < end;
}

function isNowInWatchBuildSendWindow_() {
  return isNowInTimeRange_(
    WATCH_BUILD_SEND_START_HHMM,
    WATCH_BUILD_SEND_END_HHMM,
  );
}

function getWatchSpreadsheet_() {
  const props = PropertiesService.getScriptProperties();

  const ssId =
    props.getProperty("MNA_SHEET_ID") ||
    props.getProperty("SPREADSHEET_ID") ||
    "";

  if (ssId) {
    return SpreadsheetApp.openById(ssId);
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error("MNA_SHEET_ID or active spreadsheet is required.");
  }
  return ss;
}

function getRangeByA1_(ss, a1Notation) {
  const parts = a1Notation.split("!");
  if (parts.length === 2) {
    const sh = ss.getSheetByName(parts[0]);
    if (!sh) {
      throw new Error(`sheet not found: ${parts[0]}`);
    }
    return sh.getRange(parts[1]);
  }
  return ss.getRange(a1Notation);
}

function isAlreadySentToday_() {
  try {
    const ss = getWatchSpreadsheet_();
    const today = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd");
    const mark = String(
      getRangeByA1_(ss, WATCH_SENT_MARK_CELL).getDisplayValue() || "",
    ).trim();

    return mark === today;
  } catch (e) {
    Logger.log(`[watch-build-send] sent mark check failed: ${e}`);
    return false;
  }
}

function hasWatchBuildSendTrigger_(sheetName) {
  const ss = getWatchSpreadsheet_();
  const sh = ss.getSheetByName(sheetName);

  if (!sh) {
    Logger.log(`[watch-build-send] sheet not found: ${sheetName}`);
    return false;
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 1) {
    return false;
  }

  const values = sh
    .getRange(1, WATCH_TRIGGER_COLUMN, lastRow, 1)
    .getDisplayValues();

  return values.some(function (row) {
    return String(row[0] || "").trim() === WATCH_TRIGGER_VALUE;
  });
}

/** 現在時刻が収集枠の許容範囲内なら、その slot を返す */
function findCurrentSlot_() {
  return COLLECT_SLOTS.find(isNowInSlot_);
}

/**
 * 現在時刻が watch-build-send の監視時間帯で、かつK列に a があれば slot を返す。
 * 監視時間帯外・送信済み・K列未選定の場合は null を返す。
 */
function findCurrentWatchBuildSendSlotForSheet_(sheetName) {
  if (!isNowInWatchBuildSendWindow_()) {
    return null;
  }

  if (isAlreadySentToday_()) {
    Logger.log("[watch-build-send] already sent today; skip dispatch.");
    return null;
  }

  if (!hasWatchBuildSendTrigger_(sheetName)) {
    Logger.log(`[watch-build-send] no trigger in ${sheetName}; skip dispatch.`);
    return null;
  }

  return WATCH_BUILD_SEND_SLOT;
}

// 手動テスト互換用。現在時刻が監視時間帯かだけを見る。
function findCurrentWatchBuildSendSlot_() {
  return isNowInWatchBuildSendWindow_() ? WATCH_BUILD_SEND_SLOT : null;
}

/** 本番用: 1分おきトリガーから呼ぶ */
function prodScheduleTick() {
  const collectSlot = findCurrentSlot_();
  if (collectSlot) {
    dispatchProdCollect_(collectSlot);
  }

  const watchSlot = findCurrentWatchBuildSendSlotForSheet_(
    WATCH_TRIGGER_SHEET_PROD,
  );
  if (watchSlot) {
    dispatchProdWatchBuildSend_(watchSlot);
  }
}

/** 開発用: 必要な場合だけ1分おきトリガーから呼ぶ */
function devScheduleTick() {
  const collectSlot = findCurrentSlot_();
  if (collectSlot) {
    dispatchDevCollect_(collectSlot);
  }

  const watchSlot = findCurrentWatchBuildSendSlotForSheet_(
    WATCH_TRIGGER_SHEET_DEV,
  );
  if (watchSlot) {
    dispatchDevWatchBuildSend_(watchSlot);
  }
}

/** 手動テスト: 現在時刻の slot を本番 dispatch する */
function testProdCurrentSlotNow() {
  const slot = findCurrentSlot_();
  if (!slot) {
    throw new Error("現在時刻は収集枠ではありません。");
  }
  dispatchProdCollect_(slot);
}

/** 手動テスト: 12:30枠（collect16）を本番 dispatch する */
function testProd1230Now() {
  const slot = COLLECT_SLOTS.find(
    (slot) => slot.hhmm === "12:30" && slot.mode === "collect16",
  );
  if (!slot) {
    throw new Error("12:30枠（collect16）が見つかりません。");
  }
  dispatchProdCollect_(slot);
}

/** 手動テスト: 15:30枠（当日分 collect）を本番 dispatch する */
function testProd1530Now() {
  const slot = COLLECT_SLOTS.find(
    (slot) => slot.hhmm === "15:30" && slot.mode === "collect",
  );
  if (!slot) {
    throw new Error("15:30枠（collect）が見つかりません。");
  }
  dispatchProdCollect_(slot);
}

/** 手動テスト: 00:10枠（前日分）を本番 dispatch する */
function testProd0010Now() {
  const slot = COLLECT_SLOTS.find((slot) => slot.hhmm === "00:10");
  if (!slot) {
    throw new Error("00:10枠が見つかりません。");
  }
  dispatchProdCollect_(slot);
}

/** 手動テスト: watch-build-send 条件確認（本番/prod） */
function testProdWatchBuildSendConditionNow() {
  const slot = findCurrentWatchBuildSendSlotForSheet_(WATCH_TRIGGER_SHEET_PROD);
  Logger.log(slot ? JSON.stringify(slot) : "no watch-build-send trigger now");
  return slot;
}

/** 手動テスト: watch-build-send 条件確認（開発/dev） */
function testDevWatchBuildSendConditionNow() {
  const slot = findCurrentWatchBuildSendSlotForSheet_(WATCH_TRIGGER_SHEET_DEV);
  Logger.log(slot ? JSON.stringify(slot) : "no watch-build-send trigger now");
  return slot;
}

/** 手動テスト: 監視条件を見ずに watch-build-send を本番 dispatch する */
function testProdWatchBuildSendNow() {
  dispatchProdWatchBuildSend_(WATCH_BUILD_SEND_SLOT);
}

/** 手動テスト: 監視条件を見ずに watch-build-send を開発 dispatch する */
function testDevWatchBuildSendNow() {
  dispatchDevWatchBuildSend_(WATCH_BUILD_SEND_SLOT);
}
