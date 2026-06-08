/**
 * Selection ML GitHub Actions dispatcher.
 *
 * Script Properties:
 * - GITHUB_OWNER
 * - GITHUB_REPO
 * - GITHUB_TOKEN
 * - SELECTION_ML_GITHUB_WORKFLOW_FILE (default: selection-ml.yml)
 * - ARCHIVE_DRIVE_FOLDER_ID
 * - SELECTION_ML_TARGET_HOUR (default: 2)
 * - SELECTION_ML_TARGET_MINUTE (default: 0)
 */

const SELECTION_ML_TIMEZONE = "Asia/Yangon";
const SELECTION_ML_WATCH_WINDOW_MINUTES = 10;
const SELECTION_ML_LAST_RUN_PROP = "SELECTION_ML_LAST_RUN_YMD";
const SELECTION_ML_DEFAULT_TARGET_HOUR = 2;
const SELECTION_ML_DEFAULT_TARGET_MINUTE = 0;

/**
 * 既存のwatcherトリガーを置き換え、5分おきに実行する。
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
 * prod用Selection MLの実行時刻を午前2時に固定する。
 * 既にScript Propertiesに1:30が残っている場合は、この関数を1回実行する。
 */
function setSelectionMlScheduleTo2AM() {
  const props = PropertiesService.getScriptProperties();
  props.setProperty(
    "SELECTION_ML_TARGET_HOUR",
    String(SELECTION_ML_DEFAULT_TARGET_HOUR),
  );
  props.setProperty(
    "SELECTION_ML_TARGET_MINUTE",
    String(SELECTION_ML_DEFAULT_TARGET_MINUTE),
  );
}

/**
 * 指定時刻から10分間の範囲で、1日1回だけprod用Selection MLを起動する。
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

    if (props.getProperty(SELECTION_ML_LAST_RUN_PROP) === ymd) {
      return;
    }

    const targetHour = _selectionMlIntegerProp_(
      props,
      "SELECTION_ML_TARGET_HOUR",
      SELECTION_ML_DEFAULT_TARGET_HOUR,
    );
    const targetMinute = _selectionMlIntegerProp_(
      props,
      "SELECTION_ML_TARGET_MINUTE",
      SELECTION_ML_DEFAULT_TARGET_MINUTE,
    );
    const currentMinutes =
      Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "H")) * 60 +
      Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "m"));
    const targetMinutes = targetHour * 60 + targetMinute;

    if (
      currentMinutes < targetMinutes ||
      currentMinutes >= targetMinutes + SELECTION_ML_WATCH_WINDOW_MINUTES
    ) {
      return;
    }

    triggerSelectionMlGitHubActions_("prod");
    props.setProperty(SELECTION_ML_LAST_RUN_PROP, ymd);
    Logger.log("[selection-ml] prod dispatched for " + ymd);
  } finally {
    lock.releaseLock();
  }
}

/**
 * prod手動実行用。時刻・当日実行済み判定を無視して即時起動する。
 * SELECTION_ML_LAST_RUN_YMD は更新しないため、通常の定時実行には影響しない。
 */
function triggerSelectionMlProdNow() {
  triggerSelectionMlGitHubActions_("prod");
}

/**
 * dev手動実行用。devは定時実行しない。
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

function _selectionMlIntegerProp_(props, key, defaultValue) {
  const raw = props.getProperty(key);
  if (raw === null || raw === "") return defaultValue;

  const value = Number(raw);
  if (!Number.isInteger(value)) {
    throw new Error('Script Property "' + key + '" must be an integer.');
  }
  return value;
}
