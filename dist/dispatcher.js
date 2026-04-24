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
 * - 本番: main ブランチ → production 環境 → prod シート
 * - テスト: develop ブランチ → development 環境 → dev シート
 */

const TZ = "Asia/Yangon";

/** 実行枠の定義 */
const SLOT_CRONS = {
  collect1550: "17 9 * * *",
  collect1750: "17 11 * * *",
  collect2050: "17 14 * * *",
  collect2150: "17 15 * * *",
  collect2250: "17 16 * * *",
  collect2330: "57 16 * * *",
  collect0010: "37 17 * * *",

  // 前日分取得（00:05 は全メディア、以降は Khit Thit Media のみ）
  // 00:05 MMT（= 17:35 UTC 前日）
  collect0005: "35 17 * * *",
  // 00:35 MMT（= 18:05 UTC 前日）
  collect0035: "5 18 * * *",
  // 01:05 MMT（= 18:35 UTC 前日）
  collect0105: "35 18 * * *",
  // 01:35 MMT（= 19:05 UTC 前日）
  collect0135: "5 19 * * *",
};

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

/** 重複実行防止つき dispatch
 * slotKey 例: prod_collect1750 / dev_collect1750
 */
function dispatchOncePerDay_(ref, mode, scheduleCron, slotKey) {
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

    callGithubWorkflowDispatch_(ref, mode, scheduleCron);
    props.setProperty(propKey, today);
    Logger.log(
      `dispatched: ${slotKey} ${today} ref=${ref} mode=${mode} cron=${scheduleCron}`,
    );
  } finally {
    lock.releaseLock();
  }
}

/** 本番 dispatch 共通 */
function dispatchProdCollect_(scheduleCron) {
  const mode =
    scheduleCron === SLOT_CRONS.collect1550 ? "collect16" : "collect";
  const slotKey = `prod_${scheduleCron.replace(/\s+/g, "_")}`;
  dispatchOncePerDay_("main", mode, scheduleCron, slotKey);
}

/** テスト dispatch 共通 */
function dispatchDevCollect_(scheduleCron) {
  const mode =
    scheduleCron === SLOT_CRONS.collect1550 ? "collect16" : "collect";
  const slotKey = `dev_${scheduleCron.replace(/\s+/g, "_")}`;
  dispatchOncePerDay_("develop", mode, scheduleCron, slotKey);
}

/**
 * 本番用スケジュール関数
 */
function prodCollect1550() {
  dispatchProdCollect_(SLOT_CRONS.collect1550);
}
function prodCollect1750() {
  dispatchProdCollect_(SLOT_CRONS.collect1750);
}
function prodCollect2050() {
  dispatchProdCollect_(SLOT_CRONS.collect2050);
}
function prodCollect2150() {
  dispatchProdCollect_(SLOT_CRONS.collect2150);
}
function prodCollect2250() {
  dispatchProdCollect_(SLOT_CRONS.collect2250);
}
function prodCollect2330() {
  dispatchProdCollect_(SLOT_CRONS.collect2330);
}
function prodCollect0005() {
  dispatchProdCollect_(SLOT_CRONS.collect0005);
}
function prodCollect0035() {
  dispatchProdCollect_(SLOT_CRONS.collect0035);
}
function prodCollect0105() {
  dispatchProdCollect_(SLOT_CRONS.collect0105);
}
function prodCollect0135() {
  dispatchProdCollect_(SLOT_CRONS.collect0135);
}

/**
 * 開発用スケジュール関数
 */
function devCollect1550() {
  dispatchDevCollect_(SLOT_CRONS.collect1550);
}
function devCollect1750() {
  dispatchDevCollect_(SLOT_CRONS.collect1750);
}
function devCollect2050() {
  dispatchDevCollect_(SLOT_CRONS.collect2050);
}
function devCollect2150() {
  dispatchDevCollect_(SLOT_CRONS.collect2150);
}
function devCollect2250() {
  dispatchDevCollect_(SLOT_CRONS.collect2250);
}
function devCollect2330() {
  dispatchDevCollect_(SLOT_CRONS.collect2330);
}
function devCollect0005() {
  dispatchDevCollect_(SLOT_CRONS.collect0005);
}
function devCollect0035() {
  dispatchDevCollect_(SLOT_CRONS.collect0035);
}
function devCollect0105() {
  dispatchDevCollect_(SLOT_CRONS.collect0105);
}
function devCollect0135() {
  dispatchDevCollect_(SLOT_CRONS.collect0135);
}

/**
 * 手動関数
 */
function testProd1750Now() {
  prodCollect1750();
}

function testDev1750Now() {
  devCollect1750();
}

function testProd1550Now() {
  prodCollect1550();
}

function testDev1550Now() {
  devCollect1550();
}

function testProd0005Now() {
  prodCollect0005();
}

function testDev0005Now() {
  devCollect0005();
}
