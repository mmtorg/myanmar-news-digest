/**
 * Selection ML GitHub Actions dispatcher.
 *
 * Script Properties:
 * - GITHUB_OWNER
 * - GITHUB_REPO
 * - GITHUB_TOKEN
 * - SELECTION_ML_GITHUB_WORKFLOW_FILE (default: selection-ml.yml)
 * - ARCHIVE_DRIVE_FOLDER_ID
 * - SELECTION_ML_TARGET_HOUR (default: 1)
 * - SELECTION_ML_TARGET_MINUTE (default: 30)
 * - SELECTION_ML_TARGET_SHEETS (default: prod,dev)
 */

const SELECTION_ML_TIMEZONE = "Asia/Yangon";
const SELECTION_ML_WATCH_WINDOW_MINUTES = 10;
const SELECTION_ML_LAST_RUN_PROP = "SELECTION_ML_LAST_RUN_YMD";

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
 * 指定時刻から10分間の範囲で、1日1回だけSelection MLを起動する。
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

    const targetHour = _selectionMlIntegerProp_(props, "SELECTION_ML_TARGET_HOUR", 1);
    const targetMinute = _selectionMlIntegerProp_(
      props,
      "SELECTION_ML_TARGET_MINUTE",
      30,
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

    triggerSelectionMlGitHubActions_();
    props.setProperty(SELECTION_ML_LAST_RUN_PROP, ymd);
    Logger.log("[selection-ml] dispatched for " + ymd);
  } finally {
    lock.releaseLock();
  }
}

/**
 * テスト用。時刻・当日実行済み判定を無視して即時起動する。
 * SELECTION_ML_LAST_RUN_YMD は更新しないため、通常の定時実行には影響しない。
 */
function triggerSelectionMlGitHubActionsNow() {
  triggerSelectionMlGitHubActions_();
}

function triggerSelectionMlGitHubActions_() {
  const props = PropertiesService.getScriptProperties();
  const owner = mustGetProp_(props, "GITHUB_OWNER");
  const repo = mustGetProp_(props, "GITHUB_REPO");
  const token = mustGetProp_(props, "GITHUB_TOKEN");
  const workflowFile =
    props.getProperty("SELECTION_ML_GITHUB_WORKFLOW_FILE") ||
    "selection-ml.yml";
  const archiveFolderId = mustGetProp_(props, "ARCHIVE_DRIVE_FOLDER_ID");
  const targetSheets =
    props.getProperty("SELECTION_ML_TARGET_SHEETS") || "prod,dev";
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
        target_sheets: targetSheets,
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
