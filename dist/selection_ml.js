/**
 * Selection ML GitHub Actions dispatcher.
 *
 * 目的:
 * - selectionMlWatcher を10分おきに実行するGASトリガーに設定する。
 * - コード側で 21:00以上 / 00:00未満 の時間帯だけ検知する。
 * - prodシートに「A列に日付あり、AA列が空」の未スコア新規記事がある場合だけ
 *   GitHub Actions の selection-ml.yml を dispatch する。
 * - 新規記事がない場合は GitHub API / Gemini API を呼ばずにスキップする。
 *
 * Script Properties:
 * - GITHUB_OWNER
 * - GITHUB_REPO
 * - GITHUB_TOKEN
 * - SELECTION_ML_GITHUB_WORKFLOW_FILE optional, default: selection-ml.yml
 * - ARCHIVE_DRIVE_FOLDER_ID
 */

const SELECTION_ML_TIMEZONE = "Asia/Yangon";
const SELECTION_ML_RUN_START_MINUTES = 21 * 60; // 21:00
const SELECTION_ML_RUN_END_MINUTES = 24 * 60; // 00:00 は含めない
const SELECTION_ML_SLOT_MINUTES = 10;
const SELECTION_ML_STALE_WAITING_RUN_MINUTES = 45;
const SELECTION_ML_LAST_RUN_PREFIX = "SELECTION_ML_LAST_RUN_SLOT_";
const SELECTION_ML_LAST_PENDING_SIGNATURE_KEY =
  "SELECTION_ML_LAST_PENDING_SIGNATURE_PROD";

/**
 * 既存の selectionMlWatcher トリガーを置き換え、10分おきに実行する。
 *
 * GASの画面から手動でトリガー設定する場合、この関数は実行不要。
 * 手動設定する場合は、以下の内容で設定する。
 * - 実行する関数: selectionMlWatcher
 * - イベントのソース: 時間主導型
 * - 時間ベースのトリガーのタイプ: 分ベースのタイマー
 * - 間隔: 10分おき
 */
function installSelectionMlWatcherTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (trigger) {
    if (trigger.getHandlerFunction() === "selectionMlWatcher") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp.newTrigger("selectionMlWatcher")
    .timeBased()
    .everyMinutes(SELECTION_ML_SLOT_MINUTES)
    .create();
}

/**
 * 21:00〜23:59の間だけ、10分単位のスロットにつき最大1回、
 * かつ未スコア行がある場合だけ prod 用 Selection ML を起動する。
 */
function selectionMlWatcher() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    Logger.log("[selection-ml] lock busy -> skip");
    return;
  }

  try {
    const now = new Date();
    const currentMinutes = selectionMlCurrentMinutes_(now);
    if (!isSelectionMlRunWindow_(currentMinutes)) {
      Logger.log("[selection-ml] outside 21:00-00:00 window -> skip");
      return;
    }

    const targetSheet = "prod";
    const pendingInfo = getSelectionMlPendingInfo_(targetSheet);
    if (!pendingInfo.hasPending) {
      Logger.log(
        "[selection-ml] no pending rows: A has value and AA is blank -> skip without GitHub API",
      );
      return;
    }

    const props = PropertiesService.getScriptProperties();
    const ymd = Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "yyyyMMdd");
    const slotKey = buildSelectionMlSlotKey_(ymd, currentMinutes);
    const lastRunPropKey = SELECTION_ML_LAST_RUN_PREFIX + slotKey;

    if (props.getProperty(lastRunPropKey) === "done") {
      Logger.log("[selection-ml] already dispatched for slot " + slotKey);
      return;
    }

    const dispatched = triggerSelectionMlGitHubActions_(targetSheet);
    if (!dispatched) {
      Logger.log("[selection-ml] active GitHub Actions run exists -> skip");
      return;
    }

    props.setProperty(lastRunPropKey, "done");
    Logger.log(
      "[selection-ml] prod dispatched slot=" +
        slotKey +
        " pending_rows=" +
        pendingInfo.pendingRows.join(","),
    );
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

/**
 * pending signature をリセットする。
 * GitHub Actions が失敗し、同じ未スコア行を定時処理で再dispatchしたい場合だけ手動実行する。
 */
function resetSelectionMlPendingSignature() {
  PropertiesService.getScriptProperties().deleteProperty(
    SELECTION_ML_LAST_PENDING_SIGNATURE_KEY,
  );
  Logger.log("[selection-ml] pending signature reset");
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

  if (hasBlockingSelectionMlWorkflowRun_(owner, repo, token, workflowFile)) {
    return false;
  }

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
  return true;
}

function hasBlockingSelectionMlWorkflowRun_(owner, repo, token, workflowFile) {
  const cancelableWaitingStatuses = {
    queued: true,
    waiting: true,
    requested: true,
    pending: true,
  };
  const url =
    "https://api.github.com/repos/" +
    encodeURIComponent(owner) +
    "/" +
    encodeURIComponent(repo) +
    "/actions/workflows/" +
    encodeURIComponent(workflowFile) +
    "/runs?branch=main&event=workflow_dispatch&per_page=20";

  const response = UrlFetchApp.fetch(url, {
    method: "get",
    headers: {
      Authorization: "Bearer " + token,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    muteHttpExceptions: true,
  });

  const code = response.getResponseCode();
  const body = response.getContentText();
  if (code !== 200) {
    throw new Error(
      "Selection ML workflow run lookup failed: HTTP " + code + " / " + body,
    );
  }

  const payload = JSON.parse(body);
  const runs = Array.isArray(payload.workflow_runs) ? payload.workflow_runs : [];
  for (const run of runs) {
    const status = String(run.status || "");
    if (status === "in_progress") {
      Logger.log(
        "[selection-ml] active workflow run id=" +
          run.id +
          " status=" +
          run.status,
      );
      return true;
    }
    if (cancelableWaitingStatuses[status]) {
      if (isSelectionMlStaleRun_(run)) {
        cancelSelectionMlWorkflowRun_(owner, repo, token, run);
        continue;
      }
      Logger.log(
        "[selection-ml] recent workflow run id=" +
          run.id +
          " status=" +
          status +
          " -> skip dispatch",
      );
      return true;
    }
  }
  return false;
}

function isSelectionMlStaleRun_(run) {
  const createdAtMs = Date.parse(run.created_at || "");
  if (!createdAtMs) {
    return false;
  }
  const ageMinutes = (Date.now() - createdAtMs) / 60000;
  return ageMinutes >= SELECTION_ML_STALE_WAITING_RUN_MINUTES;
}

function cancelSelectionMlWorkflowRun_(owner, repo, token, run) {
  const url =
    "https://api.github.com/repos/" +
    encodeURIComponent(owner) +
    "/" +
    encodeURIComponent(repo) +
    "/actions/runs/" +
    encodeURIComponent(run.id) +
    "/cancel";

  const response = UrlFetchApp.fetch(url, {
    method: "post",
    headers: {
      Authorization: "Bearer " + token,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    muteHttpExceptions: true,
  });

  const code = response.getResponseCode();
  const body = response.getContentText();
  if (code !== 202 && code !== 409) {
    throw new Error(
      "Selection ML workflow run cancel failed: HTTP " + code + " / " + body,
    );
  }
  Logger.log(
    "[selection-ml] canceled stale workflow run id=" +
      run.id +
      " status=" +
      run.status +
      " created_at=" +
      run.created_at,
  );
}

function getSelectionMlPendingInfo_(sheetName) {
  const sheet = SpreadsheetApp.getActive().getSheetByName(sheetName);
  if (!sheet) {
    throw new Error('Sheet "' + sheetName + '" was not found.');
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return {
      hasPending: false,
      pendingRows: [],
      signature: "none",
    };
  }

  // A:AA を読む。A列=日付、AA列=最終スコア。
  const values = sheet.getRange(2, 1, lastRow - 1, 27).getValues();
  const pendingRows = [];

  values.forEach(function (row, offset) {
    const sheetRow = offset + 2;
    const dateValue = row[0];
    const finalScoreValue = row[26];
    if (
      hasSelectionMlCellValue_(dateValue) &&
      !hasSelectionMlCellValue_(finalScoreValue)
    ) {
      pendingRows.push(sheetRow);
    }
  });

  return {
    hasPending: pendingRows.length > 0,
    pendingRows: pendingRows,
    signature: pendingRows.length > 0 ? pendingRows.join(",") : "none",
  };
}

function hasSelectionMlCellValue_(value) {
  if (value === null || value === undefined) {
    return false;
  }
  return String(value).trim() !== "";
}

function selectionMlCurrentMinutes_(now) {
  return (
    Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "H")) * 60 +
    Number(Utilities.formatDate(now, SELECTION_ML_TIMEZONE, "m"))
  );
}

function isSelectionMlRunWindow_(currentMinutes) {
  return (
    currentMinutes >= SELECTION_ML_RUN_START_MINUTES &&
    currentMinutes < SELECTION_ML_RUN_END_MINUTES
  );
}

function buildSelectionMlSlotKey_(ymd, currentMinutes) {
  const slotStartMinutes =
    Math.floor(currentMinutes / SELECTION_ML_SLOT_MINUTES) *
    SELECTION_ML_SLOT_MINUTES;
  const hour = Math.floor(slotStartMinutes / 60);
  const minute = slotStartMinutes % 60;
  return (
    ymd + "_" + String(hour).padStart(2, "0") + String(minute).padStart(2, "0")
  );
}

function mustGetProp_(props, key) {
  const value = props.getProperty(key);
  if (value === null || String(value).trim() === "") {
    throw new Error('Script Property "' + key + '" is required.');
  }
  return String(value).trim();
}
