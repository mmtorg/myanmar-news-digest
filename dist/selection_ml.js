/**
 * Selection ML GitHub Actions dispatcher.
 *
 * 目的:
 * - selectionMlWatcher を5分おきに実行するGASトリガーに設定する。
 * - コード側で 23:00 / 00:00 / 01:00 の各10分間だけ検知し、
 *   各スロットにつき1回だけ prod 用 Selection ML を起動する。
 *
 * Script Properties:
 * - GITHUB_OWNER
 * - GITHUB_REPO
 * - GITHUB_TOKEN
 * - SELECTION_ML_GITHUB_WORKFLOW_FILE optional, default: selection-ml.yml
 * - ARCHIVE_DRIVE_FOLDER_ID
 */

const SELECTION_ML_TIMEZONE = "Asia/Yangon";
const SELECTION_ML_WATCH_WINDOW_MINUTES = 10;
const SELECTION_ML_LAST_RUN_PREFIX = "SELECTION_ML_LAST_RUN_SLOT_";

/**
 * 実行したい時刻。
 * selectionMlWatcher が5分おきに起動され、各時刻から10分間の範囲に入った場合だけ
 * GitHub Actions の selection-ml.yml を dispatch する。
 */
const SELECTION_ML_TARGET_SLOTS = [
  { hour: 23, minute: 0 },
  { hour: 0, minute: 0 },
  { hour: 1, minute: 0 },
];

/**
 * 既存の selectionMlWatcher トリガーを置き換え、5分おきに実行する。
 *
 * GASの画面から手動でトリガー設定する場合、この関数は実行不要。
 * 手動設定する場合は、以下の内容で設定する。
 * - 実行する関数: selectionMlWatcher
 * - イベントのソース: 時間主導型
 * - 時間ベースのトリガーのタイプ: 分ベースのタイマー
 * - 間隔: 5分おき
 */
function installSelectionMlWatcherTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (trigger) {
    if (trigger.getHandlerFunction() === "selectionMlWatcher") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp.newTrigger("selectionMlWatcher")
    .timeBased()
    .everyMinutes(5)
    .create();
}

/**
 * 23:00 / 00:00 / 01:00 の各10分間に入ったら、
 * 各スロットにつき1回だけ prod 用 Selection ML を起動する。
 */
function selectionMlWatcher() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    Logger.log("[selection-ml] lock busy -> skip");
    return;
  }

  try {
    const props = PropertiesService.getScriptProperties();
    const now = new Date();
    const ymd = Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "yyyyMMdd");
    const currentMinutes =
      Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "H")) * 60 +
      Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "m"));

    for (const slot of SELECTION_ML_TARGET_SLOTS) {
      const targetMinutes = slot.hour * 60 + slot.minute;

      if (!isSelectionMlCurrentSlot_(currentMinutes, targetMinutes)) {
        continue;
      }

      const slotKey = buildSelectionMlSlotKey_(ymd, slot);
      const lastRunPropKey = SELECTION_ML_LAST_RUN_PREFIX + slotKey;

      if (props.getProperty(lastRunPropKey) === "done") {
        Logger.log("[selection-ml] already dispatched for slot " + slotKey);
        return;
      }

      triggerSelectionMlGitHubActions_("prod");
      props.setProperty(lastRunPropKey, "done");
      Logger.log("[selection-ml] prod dispatched for slot " + slotKey);
      return;
    }
  } finally {
    lock.releaseLock();
  }
}

/**
 * prod手動実行用。
 * 時刻・スロット実行済み判定を無視して即時起動する。
 * 通常の定時実行用プロパティは更新しない。
 */
function triggerSelectionMlProdNow() {
  triggerSelectionMlGitHubActions_("prod");
}

/**
 * dev手動実行用。
 * devは定時実行しない。
 */
function triggerSelectionMlDevNow() {
  triggerSelectionMlGitHubActions_("dev");
}

function triggerSelectionMlGitHubActions_(targetSheet) {
  if (targetSheet !== "prod" && targetSheet !== "dev") {
    throw new Error("Selection ML target sheet must be prod or dev.");
  }

  const props = PropertiesService.getScriptProperties();
  const owner = mustGetProp_(props, "GITHUB_OWNER");
  const repo = mustGetProp_(props, "GITHUB_REPO");
  const token = mustGetProp_(props, "GITHUB_TOKEN");
  const workflowFile =
    props.getProperty("SELECTION_ML_GITHUB_WORKFLOW_FILE") ||
    "selection-ml.yml";
  const archiveFolderId = mustGetProp_(props, "ARCHIVE_DRIVE_FOLDER_ID");
  const spreadsheetId = SpreadsheetApp.getActive().getId();

  const url =
    "https://api.github.com/repos/" +
    encodeURIComponent(owner) +
    "/" +
    encodeURIComponent(repo) +
    "/actions/workflows/" +
    encodeURIComponent(workflowFile) +
    "/dispatches";

  const response = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + token,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    payload: JSON.stringify({
      ref: "main",
      inputs: {
        mode: "predict",
        spreadsheet_id: spreadsheetId,
        archive_folder_id: archiveFolderId,
        target_sheet: targetSheet,
      },
    }),
    muteHttpExceptions: true,
  });

  const code = response.getResponseCode();
  const body = response.getContentText();
  if (code !== 204) {
    throw new Error(
      "Selection ML workflow_dispatch failed: HTTP " + code + " / " + body,
    );
  }
}

function isSelectionMlCurrentSlot_(currentMinutes, targetMinutes) {
  return (
    currentMinutes >= targetMinutes &&
    currentMinutes < targetMinutes + SELECTION_ML_WATCH_WINDOW_MINUTES
  );
}

function buildSelectionMlSlotKey_(ymd, slot) {
  return (
    ymd +
    "_" +
    String(slot.hour).padStart(2, "0") +
    String(slot.minute).padStart(2, "0")
  );
}

function mustGetProp_(props, key) {
  const value = props.getProperty(key);
  if (value === null || String(value).trim() === "") {
    throw new Error('Script Property "' + key + '" is required.');
  }
  return String(value).trim();
}
