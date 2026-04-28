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
 *
 * 運用:
 * - Apps Script の時間主導型トリガーは prodScheduleTick を「1分おき」に1つだけ設定する。
 * - 指定時刻のみ記事収集を workflow_dispatch する。
 * - 15:30 は collect16 と同じく、既存シートを整理してから当日分を収集する。
 * - 18:30〜23:20 は当日分、00:10 は前日分を GitHub Actions 側で判定する。
 * - 00:45 / 01:15 / 01:45 は前日分の Khit Thit Media のみ収集する。
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
  { hhmm: "15:30", cron: "0 9 * * *", mode: "collect16" },

  // 当日分取得
  { hhmm: "18:30", cron: "0 12 * * *", mode: "collect" },
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
];

const SLOT_WINDOW_MINUTES = 5;

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

/** 現在時刻が収集枠の許容範囲内なら、その slot を返す */
function findCurrentSlot_() {
  const now = new Date();
  const currentMinutes =
    Number(Utilities.formatDate(now, TZ, "H")) * 60 +
    Number(Utilities.formatDate(now, TZ, "m"));

  return COLLECT_SLOTS.find((slot) => {
    const parts = slot.hhmm.split(":");
    const slotMinutes = Number(parts[0]) * 60 + Number(parts[1]);
    return (
      currentMinutes >= slotMinutes &&
      currentMinutes < slotMinutes + SLOT_WINDOW_MINUTES
    );
  });
}

/** 本番用: 1分おきトリガーから呼ぶ */
function prodScheduleTick() {
  const slot = findCurrentSlot_();
  if (!slot) return;
  dispatchProdCollect_(slot);
}

/** 開発用: 必要な場合だけ1分おきトリガーから呼ぶ */
function devScheduleTick() {
  const slot = findCurrentSlot_();
  if (!slot) return;
  dispatchDevCollect_(slot);
}

/** 手動テスト: 現在時刻の slot を本番 dispatch する */
function testProdCurrentSlotNow() {
  const slot = findCurrentSlot_();
  if (!slot) {
    throw new Error("現在時刻は収集枠ではありません。");
  }
  dispatchProdCollect_(slot);
}

/** 手動テスト: 15:30枠（collect16）を本番 dispatch する */
function testProd1530Now() {
  dispatchProdCollect_(COLLECT_SLOTS[0]);
}

/** 手動テスト: 00:10枠（前日分）を本番 dispatch する */
function testProd0010Now() {
  const slot = COLLECT_SLOTS.find((slot) => slot.hhmm === "00:10");
  if (!slot) {
    throw new Error("00:10枠が見つかりません。");
  }
  dispatchProdCollect_(slot);
}
