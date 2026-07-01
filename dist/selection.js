/************************************************************
 * selection.js
 *
 * 記事選定スコア処理 v4.5.8-country-yangon-company-civilian-death
 *
 * 前提:
 * - processRowsBatch() 済み
 * - E/F/G列が生成済み
 * - I列（本文要約）も選定入力として使用
 * - L列が OK / OK(FLASH) / OK(GPT)
 * - prodはarchive_prod、devはarchive_devのK列=a採用済み記事を参照（日付制限なし）
 *
 * 出力:
 * - R〜Y列に最終採用確率スコアと補助情報を左詰めで出力（旧selection.js出力）
 * - 現在のML/Gemini列構成では、同日同一内容判定はAA列・AD列だけを更新
 *
 * 重要:
 * - K列には一切書き込まない
 * - P列・Q列には一切書き込まない
 * - P列はselection.jsでは参照しない
 * - Q列は記事重複判定キーとして読み取り専用
 *
 * Gemini:
 * - selection.js専用のAPIキーを使用する
 * - Script Properties に GEMINI_SELECTION_API_KEY を設定する
 * - モデル名はこのファイル内の GEMINI_SELECTION_MODEL で管理する
 ************************************************************/

/************************************************************
 * Gemini selection.js 専用設定
 ************************************************************/

const GEMINI_SELECTION_MODEL = "gemini-3.1-flash-lite";

// 429 / 503 / high demand 時のGemini側フォールバック
const GEMINI_SELECTION_FALLBACK_MODEL = "gemini-2.5-flash";

// Geminiがだめな場合の最終フォールバック

// Gemini APIコール節約・安定化設定
const SELECT_GEMINI_BATCH_SIZE = 3;
const SELECT_GEMINI_TARGET_RPM = 12;
const SELECT_GEMINI_RATE_LAST_CALL_PROP = "GEMINI_SELECTION_LAST_CALL_AT_MS";
const SELECT_MODEL_FALLBACK_WAIT_MS = 2000;
const SELECT_MAX_OUTPUT_TOKENS = 8192;

// 国名・地域名重みの調整
// 優先度1の国・地域、ヤンゴン、ミャンマー個別企業名は強い採用候補として扱う。
const SELECT_TOP_COUNTRY_WEIGHT_SCORE = 10;
const SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE = 7;
const SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE = 4;
const SELECT_KOREA_COUNTRY_WEIGHT_SCORE =
  SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE; // 後方互換用
const SELECT_TOP_COUNTRY_SCORE_FLOOR = 75;
const SELECT_SECOND_PRIORITY_COUNTRY_ORG_SCORE_FLOOR = 60;
const SELECT_NEIGHBOR_COUNTRY_SCORE_FLOOR = 48;
const SELECT_KOREA_COUNTRY_SCORE_FLOOR =
  SELECT_SECOND_PRIORITY_COUNTRY_ORG_SCORE_FLOOR; // 後方互換用
const SELECT_YANGON_SCORE_FLOOR = SELECT_TOP_COUNTRY_SCORE_FLOOR;
const SELECT_MYANMAR_INDIVIDUAL_COMPANY_SCORE_FLOOR = 75;
const SELECT_CONFLICT_CIVILIAN_DEATH_SCORE_FLOOR = 68;
const SELECT_CONFLICT_WITHOUT_CIVILIAN_DEATH_SCORE_CAP = 34;

// 重要トピックの基礎スコア係数
const SELECT_PRIORITY_TOPIC_WEIGHT = 3.2;

// ミャンマー直接関連が確認できない記事は、R列の最終スコアでも必ず抑える。
// 基礎スコア段階だけでなく、同日トピック・Q列重複の同点化後にも再適用する。
const SELECT_NON_MYANMAR_FINAL_SCORE_CAP = 20;

// ミンアウンフライン、国軍総司令官、ミャンマー政府・省庁などによる
// 政策提案・発表・声明記事を、通常の声明記事より強く扱うための下限。
const SELECT_MYANMAR_LEADERSHIP_POLICY_SCORE_FLOOR = 76;
const SELECT_MYANMAR_LEADERSHIP_POLICY_STRONG_SCORE_FLOOR = 82;

// archive採用済み記事との比較。
// prodはarchive_prod、devはarchive_devを参照し、K列がaの行だけを比較対象にする（日付制限なし）。
const SELECT_ARCHIVE_ADOPTED_MAX_ITEMS = 30;
const SELECT_ARCHIVE_ADOPTED_MAX_PROMPT_CHARS = 6500;
const SELECT_ARCHIVE_ADOPTED_SUMMARY_MAX_CHARS = 420;
const SELECT_ARCHIVE_ADOPTED_DUPLICATE_SCORE_CAP = 42;
const SELECT_ARCHIVE_ADOPTED_DUPLICATE_SCORE_PENALTY = 32;
const SELECT_ARCHIVE_ADOPTED_CONTINUATION_BONUS = 10;
const SELECT_ARCHIVE_ADOPTED_DIFFERENT_ANGLE_BONUS = 7;
const SELECT_ARCHIVE_ADOPTED_RELATED_DIFFERENT_BONUS = 3;

// 法案提出系、港湾・コンテナ船入港系は、他の重要トピックと同じく
// priorityTopicTags / priorityTopicScore の中で評価する。
// 個別の基礎スコア下限補正は設けない。

/************************************************************
 * 列定義
 ************************************************************/

const SELECT_COL_O_PAST_TOPIC_COUNT = 15; // O: 過去2日同一TOPIC数。過去採用記事との差分判定の実行条件にのみ使用
const SELECT_COL_P_PAST_TOPIC_TITLES = 16; // P: 過去2日同一トピック記事タイトル。selection.jsでは未使用
const SELECT_COL_Q_DUPLICATE_KEY = 17; // Q: 記事重複判定キー。読み取り専用

const SELECT_COL_R_SCORE = 18; // R: AI最終採用確率スコア
const SELECT_COL_S_RATIONALE = 19; // S: AI説明・補足
const SELECT_COL_T_SAME_DAY_TOPIC_KEY = 20; // T: 同日トピックキー
const SELECT_COL_U_STATUS = 21; // U: AI選定ステータス
const SELECT_COL_V_TOPIC_IMPORTANCE = 22; // V: 基礎採用スコア
const SELECT_COL_W_REPRESENTATIVE = 23; // W: 代表記事スコア
const SELECT_COL_X_TOPIC_RANK = 24; // X: 同日トピック内順位（未使用）
const SELECT_COL_Y_RECOMMEND_FLAG = 25; // Y: AI採用判定

const SELECT_OUTPUT_LAST_COL = SELECT_COL_Y_RECOMMEND_FLAG;

// 現在のSelection ML/Gemini出力列（0701.xlsxの列構成に合わせる）
// R〜AC列は既存のML/Gemini出力領域として扱い、同日同一内容判定ではAA列とAD列だけを書き換える。
const SELECT_COL_AA_FINAL_SCORE = 27; // AA: 最終スコア
const SELECT_COL_AC_GEMINI_DETAIL_JSON = 29; // AC: Gemini詳細JSON
const SELECT_COL_AD_DUPLICATE_GROUP = 30; // AD: 同日重複グループ
const SELECT_DUPLICATE_GROUP_HEADER = "同日重複グループ";
const SELECT_DUPLICATE_GROUP_PREFIX = "重複";

const SELECT_MAX_ROWS_PER_RUN = 150;

// v4: 指定5基準のみのランキング仕様。
const SELECT_RANKING_MODE_VERSION =
  "v4.5.8-country-yangon-company-civilian-death";

/************************************************************
 * 実行入口
 ************************************************************/

/**
 * prod用入口
 */
function runArticleSelectionScoreProdBatch() {
  _runArticleSelectionScoreBatchBySheetName_("prod");
}

/**
 * dev用入口
 */
function runArticleSelectionScoreDevBatch() {
  _runArticleSelectionScoreBatchBySheetName_("dev");
}

/**
 * prod/devまとめて実行
 */
function runArticleSelectionScoreBatch() {
  ["prod", "dev"].forEach(function (sheetName) {
    _runArticleSelectionScoreBatchBySheetName_(sheetName);
  });
}

/**
 * 1分おきトリガー用。
 * 23:00 / 0:00 / 1:00 の分だけ prod の選定スコア処理を実行する。
 *
 * 使い方:
 * - GASのトリガーで、この関数を「分ベースのタイマー」「1分おき」に設定する。
 * - runArticleSelectionScoreProdBatch を23時台/0時台/1時台で直接起動する既存トリガーがある場合は削除する。
 *
 * 注意:
 * - GASの時間主導型トリガー自体は秒単位の厳密実行を保証しない。
 * - この関数は、GAS内で可能な範囲で 23:00 / 0:00 / 1:00 の分だけ本処理を通すための監視入口。
 */
function runArticleSelectionScoreProdBatchMinuteWatcher() {
  const tz = Session.getScriptTimeZone();
  const now = new Date();

  const hour = Number(Utilities.formatDate(now, tz, "H"));
  const minute = Number(Utilities.formatDate(now, tz, "m"));
  const dateKey = Utilities.formatDate(now, tz, "yyyyMMdd");

  const targetHours = {
    23: true,
    0: true,
    1: true,
  };

  // 対象時刻以外は何もしない。
  if (!targetHours[hour]) return;

  // 23:00 / 0:00 / 1:00 の「0分」のときだけ実行する。
  if (minute !== 0) return;

  const props = PropertiesService.getScriptProperties();
  const runKey =
    "ARTICLE_SELECTION_PROD_BATCH_RAN_" +
    dateKey +
    "_" +
    _zeroPadSelectionNumber_(hour, 2);

  const lock = LockService.getScriptLock();

  try {
    if (!lock.tryLock(10000)) {
      Logger.log("[selection-minute-watcher] lock busy -> skip");
      return;
    }

    // 同じ日付・同じ対象時刻で二重実行しない。
    if (props.getProperty(runKey) === "DONE") {
      Logger.log("[selection-minute-watcher] already done: " + runKey);
      return;
    }

    // 本処理が長時間化しても、同じ対象時刻の重複起動を避けるため先に記録する。
    props.setProperty(runKey, "DONE");

    Logger.log(
      "[selection-minute-watcher] run prod batch at " +
        dateKey +
        " " +
        _zeroPadSelectionNumber_(hour, 2) +
        ":00",
    );

    runArticleSelectionScoreProdBatch();
  } catch (e) {
    Logger.log("[selection-minute-watcher] error: " + e);

    // エラー時はDONEを戻し、同じ分内に再実行機会があれば再試行できるようにする。
    props.deleteProperty(runKey);

    throw e;
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/**
 * prodのAI選定列をリセットする
 * K列・P列・Q列は触らない
 */
function resetArticleSelectionScoreProd() {
  _resetArticleSelectionScoreColumnsBySheetName_("prod");
}

/**
 * devのAI選定列をリセットする
 * K列・P列・Q列は触らない
 */
function resetArticleSelectionScoreDev() {
  _resetArticleSelectionScoreColumnsBySheetName_("dev");
}

/************************************************************
 * ヘッダー・リセット
 ************************************************************/

/**
 * R〜Y列のヘッダー整備
 */
function _ensureSelectionScoreHeaders_(sheet) {
  // 現在のシートではR〜AC列がML/Gemini出力領域として使われているため、
  // この関数ではR〜AC列のヘッダーを上書きしない。
  // 同日同一内容判定で新たに使うAD列だけを整備する。
  _ensureSameDayDuplicateGroupHeader_(sheet);
}

function _ensureSameDayDuplicateGroupHeader_(sheet) {
  if (!sheet) return;
  sheet
    .getRange(1, SELECT_COL_AD_DUPLICATE_GROUP)
    .setValue(SELECT_DUPLICATE_GROUP_HEADER);
}

/**
 * R〜Y列をクリアする
 * AA〜AC列はSelection MLの出力領域なので触らない
 * K列・P列・Q列は触らない
 */
function _resetArticleSelectionScoreColumnsBySheetName_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return;

  _ensureSelectionScoreHeaders_(sheet);

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  _clearSelectionUsedColumns_(sheet, 2, lastRow - 1);
}

function _clearSelectionUsedColumns_(sheet, startRow, rowCount) {
  if (!sheet || !rowCount || rowCount <= 0) return;

  // 現在の列構成ではR〜AC列が既存のML/Gemini出力領域のため、
  // reset系関数でもR〜AC列は消さない。
  // 同日同一内容判定の出力先であるAD列だけをクリアする。
  sheet
    .getRange(startRow, SELECT_COL_AD_DUPLICATE_GROUP, rowCount, 1)
    .clearContent();
}

/************************************************************
 * メイン処理
 ************************************************************/

/**
 * 選定スコア判定の対象行かどうか
 */
function _isArticleSelectionScoreTarget_(row) {
  const dateVal = row[0]; // A
  const media = String(row[2] || "").trim(); // C
  const e = String(row[4] || "").trim(); // E
  const f = String(row[5] || "").trim(); // F
  const g = String(row[6] || "").trim(); // G
  const summary = String(row[8] || "").trim(); // I
  const statusL = String(row[11] || "").trim(); // L

  const scoreR = row[SELECT_COL_R_SCORE - 1]; // R
  const statusZ = String(row[SELECT_COL_U_STATUS - 1] || "").trim(); // U

  if (!dateVal) return false;
  if (!media) return false;

  // Businessプラン限定は記事選定AIの対象外。
  // 必要な場合は、別枠の固定採用・マーケット情報処理として扱う。
  if (media === "(Businessプラン限定)") return false;

  // 見出し・要約処理が完了していること
  if (!(statusL === "OK" || statusL === "OK(FLASH)" || statusL === "OK(GPT)")) {
    return false;
  }

  if (!e && !f && !g && !summary) return false;

  // すでにR列スコアがある行はスキップ
  if (scoreR !== "" && scoreR !== null && scoreR !== undefined) return false;

  // RUNNING行はスキップ
  if (statusZ.startsWith("RUNNING")) return false;

  return true;
}

/**
 * 1回あたり最大 SELECT_MAX_ROWS_PER_RUN 件をバッチでスコア判定
 *
 * 処理後に以下を実行:
 * - Q列完全一致・sameDayTopicKey一致のR列同点化
 * - AI採用判定フラグ付け
 */
function _runArticleSelectionScoreBatchBySheetName_(sheetName) {
  const lock = LockService.getDocumentLock();

  try {
    if (!lock.tryLock(5000)) {
      Logger.log("[selection-score] lock busy -> skip");
      return;
    }

    const ss = SpreadsheetApp.getActive();
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) return;

    _ensureSelectionScoreHeaders_(sheet);

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return;

    const lastCol = Math.max(SELECT_OUTPUT_LAST_COL, sheet.getLastColumn());
    const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    const allAdoptedArchiveArticles = _buildAdoptedArchiveListForSelection_(
      ss,
      sheetName,
    );

    let processed = 0;
    let batch = [];

    for (let i = 0; i < values.length; i++) {
      if (processed + batch.length >= SELECT_MAX_ROWS_PER_RUN) break;

      const rowIndex = i + 2;
      const row = values[i];

      if (!_isArticleSelectionScoreTarget_(row)) continue;

      batch.push(
        _articleFromSelectionScoreRow_(
          row,
          rowIndex,
          allAdoptedArchiveArticles,
        ),
      );

      if (batch.length >= SELECT_GEMINI_BATCH_SIZE) {
        _processSelectionScoreArticleBatch_(sheetName, sheet, batch);
        processed += batch.length;
        batch = [];
      }
    }

    if (batch.length > 0 && processed < SELECT_MAX_ROWS_PER_RUN) {
      _processSelectionScoreArticleBatch_(sheetName, sheet, batch);
      processed += batch.length;
    }

    // まだ選定スコア未処理の対象行が残っている場合は、
    // R列同点化・採用判定フラグ付けを実行しない。
    // これにより、T列/U列が埋まっていない行を除外した状態で
    // 先に同日重複判定が走ることを防ぐ。
    const remainingTargets =
      _countRemainingArticleSelectionScoreTargets_(sheet);
    if (remainingTargets > 0) {
      Logger.log(
        "[selection-score] %s processed=%s post-processing skipped; remainingTargets=%s",
        sheetName,
        processed,
        remainingTargets,
      );
      return;
    }

    // 全対象行のスコア判定が完了した最後の実行回だけ、
    // 同日同一内容判定を一番最後に実行する。
    // 現在の列構成ではT列ではなくAC列のGemini詳細JSONを参照し、
    // AA列（最終スコア）とAD列（重複グループ）だけを書き換える。
    _assignDailySelectionRecommendations_(sheet);

    Logger.log(
      "[selection-score] %s processed=%s post-processing done",
      sheetName,
      processed,
    );
  } catch (e) {
    Logger.log("[selection-score] error: " + e);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/**
 * 現在のシート上に、まだ選定スコア判定の対象として残っている行が何件あるか数える。
 *
 * 重要:
 * - この関数は、Geminiバッチ処理後にシートを再読み込みして確認する。
 * - そのため、今回の実行でR/T/U列などに書き込まれた最新状態を前提に判定できる。
 * - 1件でも残っている間は、R列同点化・採用判定フラグ付けを保留する。
 */
function _countRemainingArticleSelectionScoreTargets_(sheet) {
  if (!sheet) return 0;

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return 0;

  const lastCol = Math.max(SELECT_OUTPUT_LAST_COL, sheet.getLastColumn());
  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  let count = 0;

  for (let i = 0; i < values.length; i++) {
    if (_isArticleSelectionScoreTarget_(values[i])) {
      count += 1;
    }
  }

  return count;
}

/**
 * シート行から記事オブジェクト化
 */
function _articleFromSelectionScoreRow_(
  row,
  rowIndex,
  allAdoptedArchiveArticles,
) {
  const shouldCompareAdoptedArchive =
    _shouldCompareAdoptedArchiveForSelection_(row);
  const adoptedArchiveCandidates = shouldCompareAdoptedArchive
    ? _selectAdoptedArchiveCandidatesForSelection_(
        row,
        allAdoptedArchiveArticles || [],
      )
    : [];

  return {
    rowIndex: rowIndex,
    date: row[0],
    media: String(row[2] || "").trim(),

    headlineA: String(row[4] || "").trim(), // E
    headlineFinal: String(row[5] || "").trim(), // F
    headlineBody: String(row[6] || "").trim(), // G
    summary: String(row[8] || "").trim(), // I
    url: String(row[9] || "").trim(), // J

    // selection.js の評価入力は E/F/G/I 列を使う。
    // M列の原文タイトル・N列の原文本文は使わない。
    // O列は、過去採用記事との差分判定を行うかどうかのゲートとしてのみ使う。
    // P列の「過去2日同一トピック記事タイトル」は使わない。
    pastTwoDaysSameTopicCount: String(
      row[SELECT_COL_O_PAST_TOPIC_COUNT - 1] || "",
    ).trim(), // O
    shouldCompareAdoptedArchive: shouldCompareAdoptedArchive,
    duplicateKey: String(row[SELECT_COL_Q_DUPLICATE_KEY - 1] || "").trim(), // Q

    // archive_prod/archive_devでK列=aとして手動採用済みだった記事。
    // 同行O列が2の場合だけ、日付では絞らず、Q列一致・見出し/要約類似度で候補を絞って比較入力にする。
    // O列が2以外の場合は空配列にし、過去採用記事との差分判定・スコア補正を行わない。
    adoptedArchiveArticles: adoptedArchiveCandidates,
  };
}

function _shouldCompareAdoptedArchiveForSelection_(row) {
  if (!row) return false;

  const raw = row[SELECT_COL_O_PAST_TOPIC_COUNT - 1];
  if (raw === null || raw === undefined || raw === "") return false;

  const n = Number(raw);
  return !isNaN(n) && n === 2;
}

/************************************************************
 * archive採用済み記事の取得
 ************************************************************/

function _archiveSheetNameForSelection_(sheetName) {
  const s = String(sheetName || "").trim();
  if (s === "prod") return "archive_prod";
  if (s === "dev") return "archive_dev";
  return "archive_" + s;
}

function _buildAdoptedArchiveListForSelection_(ss, sheetName) {
  const out = [];
  if (!ss) return out;

  const archiveSheetName = _archiveSheetNameForSelection_(sheetName);
  const archiveSheet = ss.getSheetByName(archiveSheetName);
  if (!archiveSheet) {
    Logger.log(
      "[selection-score] archive sheet not found: %s",
      archiveSheetName,
    );
    return out;
  }

  const lastRow = archiveSheet.getLastRow();
  if (lastRow < 2) return out;

  const lastCol = Math.max(
    SELECT_OUTPUT_LAST_COL,
    SELECT_COL_Q_DUPLICATE_KEY,
    archiveSheet.getLastColumn(),
  );
  const values = archiveSheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  values.forEach(function (row, i) {
    const adoptedFlag = String(row[10] || "").trim(); // K
    if (adoptedFlag !== "a") return;

    const item = _archiveAdoptedArticleFromSelectionRow_(row, i + 2);
    if (
      String(
        item.headlineFinal ||
          item.headlineA ||
          item.headlineBody ||
          item.summary ||
          "",
      ).trim() === ""
    ) {
      return;
    }

    out.push(item);
  });

  return out;
}

function _archiveAdoptedArticleFromSelectionRow_(row, rowIndex) {
  row = row || [];
  return {
    archiveRowIndex: rowIndex,
    date: row[0],
    dateKey: _selectionDateKey_(row[0]),
    media: String(row[2] || "").trim(), // C
    headlineA: String(row[4] || "").trim(), // E
    headlineFinal: String(row[5] || "").trim(), // F
    headlineBody: String(row[6] || "").trim(), // G
    summary: String(row[8] || "").trim(), // I
    url: String(row[9] || "").trim(), // J
    adoptedFlag: String(row[10] || "").trim(), // K
    duplicateKey: String(row[SELECT_COL_Q_DUPLICATE_KEY - 1] || "").trim(), // Q
    sameDayTopicKey: _normalizeSelectionTopicKey_(
      String(row[SELECT_COL_T_SAME_DAY_TOPIC_KEY - 1] || ""),
    ),
  };
}

function _selectAdoptedArchiveCandidatesForSelection_(
  currentRow,
  allAdoptedArchiveArticles,
) {
  const list = Array.isArray(allAdoptedArchiveArticles)
    ? allAdoptedArchiveArticles
    : [];
  if (!list.length) return [];

  const currentText = _selectionInputTextFromSheetRow_(currentRow);
  const currentDuplicateKey = _selectionExactDuplicateKeyFromQ_(
    currentRow[SELECT_COL_Q_DUPLICATE_KEY - 1],
  );
  const currentDateKey = _selectionDateKey_(currentRow[0]);

  const scored = [];

  list.forEach(function (item) {
    const archiveText = _archiveAdoptedArticleTextForSelection_(item);
    let score = 0;

    if (currentDuplicateKey && item.duplicateKey === currentDuplicateKey) {
      score += 1000;
    }

    const currentTitle = _normalizeSelectionComparableText_(
      [currentRow[4], currentRow[5], currentRow[6]].join(" "),
    );
    const archiveTitle = _normalizeSelectionComparableText_(
      [item.headlineA, item.headlineFinal, item.headlineBody].join(" "),
    );

    if (currentTitle && archiveTitle && currentTitle === archiveTitle) {
      score += 700;
    }

    score += _archiveAdoptedTextSimilarityScoreForSelection_(
      currentText,
      archiveText,
    );

    const currentUrl = String(currentRow[9] || "").trim();
    if (currentUrl && item.url && String(item.url).trim() === currentUrl) {
      score += 900;
    }

    // 日付制限はしない。日付一致は小さな補助点に留める。
    if (currentDateKey && item.dateKey && currentDateKey === item.dateKey) {
      score += 2;
    }

    if (score <= 0) return;

    scored.push({
      item: item,
      score: score,
    });
  });

  scored.sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return (
      Number(b.item.archiveRowIndex || 0) - Number(a.item.archiveRowIndex || 0)
    );
  });

  // Q列完全一致・タイトル完全一致などの強一致は必ず残し、
  // それ以外は類似度の高い候補だけをGeminiに渡す。
  return scored
    .filter(function (x, i) {
      return x.score >= 10 || i < 8;
    })
    .slice(0, SELECT_ARCHIVE_ADOPTED_MAX_ITEMS)
    .map(function (x) {
      const item = {};
      Object.keys(x.item).forEach(function (key) {
        item[key] = x.item[key];
      });
      item.candidateScore = x.score;
      return item;
    });
}

function _archiveAdoptedArticleTextForSelection_(item) {
  item = item || {};
  return [
    item.headlineA,
    item.headlineFinal,
    item.headlineBody,
    item.summary,
    item.duplicateKey,
  ].join("\n");
}

function _normalizeSelectionComparableText_(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[\s　]+/g, "")
    .replace(/[|｜:：,，.。()（）\[\]「」『』、"'’‘“”]/g, "")
    .trim();
}

function _archiveAdoptedTextSimilarityScoreForSelection_(a, b) {
  const tokensA = _selectionSimilarityTokens_(a);
  const tokensB = _selectionSimilarityTokens_(b);
  if (!tokensA.length || !tokensB.length) return 0;

  const setB = {};
  tokensB.forEach(function (t) {
    setB[t] = true;
  });

  let overlap = 0;
  tokensA.forEach(function (t) {
    if (setB[t]) overlap += 1;
  });

  const ratio = overlap / Math.max(1, Math.min(tokensA.length, tokensB.length));
  return Math.round(overlap * 2 + ratio * 20);
}

function _selectionSimilarityTokens_(value) {
  const s = String(value || "")
    .toLowerCase()
    .replace(/[|｜:：,，.。()（）\[\]「」『』、"'’‘“”]/g, " ")
    .replace(/[\s　]+/g, " ")
    .trim();

  if (!s) return [];

  const raw = s.split(" ");
  const stop = {
    ミャンマー: true,
    ビルマ: true,
    myanmar: true,
    burma: true,
    記事: true,
    発表: true,
    報道: true,
    述べ: true,
    した: true,
    する: true,
    される: true,
    について: true,
    など: true,
    news: true,
    report: true,
    update: true,
  };

  const out = [];
  const seen = {};

  raw.forEach(function (t) {
    t = String(t || "").trim();
    if (!t) return;
    if (t.length < 2) return;
    if (/^[0-9０-９]+$/.test(t)) return;
    if (stop[t]) return;
    if (seen[t]) return;
    seen[t] = true;
    out.push(t);
  });

  return out.slice(0, 80);
}

function _hasAdoptedArchiveArticlesForSelection_(article) {
  return (
    article &&
    Array.isArray(article.adoptedArchiveArticles) &&
    article.adoptedArchiveArticles.length > 0
  );
}

function _adoptedArchiveArticlesForPrompt_(article) {
  const items =
    article && Array.isArray(article.adoptedArchiveArticles)
      ? article.adoptedArchiveArticles
      : [];

  if (!items.length) return "なし";

  let totalChars = 0;
  const blocks = [];

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const block = [
      "archive採用記事#" + (i + 1),
      "archiveRowIndex: " + item.archiveRowIndex,
      "候補抽出スコア: " + (item.candidateScore || 0),
      "日付: " + item.date,
      "媒体: " + item.media,
      "見出しA: " + _truncateSelectionPromptText_(item.headlineA, 180),
      "確定寄せ見出し: " +
        _truncateSelectionPromptText_(item.headlineFinal, 180),
      "本文ベース見出し: " +
        _truncateSelectionPromptText_(item.headlineBody, 180),
      "本文要約（I列）: " +
        _truncateSelectionPromptText_(
          item.summary,
          SELECT_ARCHIVE_ADOPTED_SUMMARY_MAX_CHARS,
        ),
      "Q列 記事重複判定キー: " +
        _truncateSelectionPromptText_(item.duplicateKey, 220),
      "URL: " + item.url,
    ].join("\n");

    if (
      blocks.length > 0 &&
      totalChars + block.length > SELECT_ARCHIVE_ADOPTED_MAX_PROMPT_CHARS
    ) {
      blocks.push(
        "（archive採用済み記事候補が多いため、以降はプロンプト長制限により省略）",
      );
      break;
    }

    blocks.push(block);
    totalChars += block.length;
  }

  return blocks.join("\n\n");
}

function _truncateSelectionPromptText_(value, maxChars) {
  const s = String(value || "").trim();
  const max = Number(maxChars || 0);
  if (!max || s.length <= max) return s;
  return s.slice(0, Math.max(0, max - 1)) + "…";
}

function _adoptedArchiveRelationFromModel_(obj, article) {
  if (!_hasAdoptedArchiveArticlesForSelection_(article)) {
    return "no_archive_adopted";
  }

  const relation = String(
    (obj && obj.adoptedArchiveRelation) || "unknown",
  ).trim();

  const allowed = {
    no_archive_adopted: true,
    duplicate_same_content: true,
    continuation_update: true,
    different_angle: true,
    related_but_different: true,
    unrelated: true,
    unknown: true,
  };

  return allowed[relation] ? relation : "unknown";
}

function _applyAdoptedArchiveCriteriaAdjustment_(score, feature, obj, article) {
  let adjusted = Number(score || 0);

  if (!_hasAdoptedArchiveArticlesForSelection_(article)) {
    return adjusted;
  }

  const relation = _adoptedArchiveRelationFromModel_(obj, article);

  if (relation === "duplicate_same_content") {
    adjusted -= SELECT_ARCHIVE_ADOPTED_DUPLICATE_SCORE_PENALTY;
    adjusted = Math.min(adjusted, SELECT_ARCHIVE_ADOPTED_DUPLICATE_SCORE_CAP);
  } else if (relation === "continuation_update") {
    adjusted += SELECT_ARCHIVE_ADOPTED_CONTINUATION_BONUS;
  } else if (relation === "different_angle") {
    adjusted += SELECT_ARCHIVE_ADOPTED_DIFFERENT_ANGLE_BONUS;
  } else if (relation === "related_but_different") {
    adjusted += SELECT_ARCHIVE_ADOPTED_RELATED_DIFFERENT_BONUS;
  } else if (relation === "unknown") {
    // archive採用済み記事の候補があるのに差分が不明な場合は、重複リスクとして軽く抑える。
    adjusted -= 6;
  }

  return adjusted;
}

/************************************************************
 * Geminiスコア判定
 ************************************************************/

/**
 * 単一記事をスコア化する。バッチJSON崩れ・欠損時の個別フォールバックにも使う。
 */
function _scoreArticleForSelection_(sheetName, article) {
  try {
    const prompt = _buildArticleSelectionScorePrompt_(article);
    const tag = sheetName + "#row" + article.rowIndex + ":selectionScoreV2";

    const bundle = getSelectionGeminiApiKeyBundle_(
      sheetName,
      article.media,
      tag,
    );

    const resp = callSelectionGeminiWithKey_(
      bundle && bundle.apiKey,
      prompt,
      tag,
      bundle && bundle.propName,
    );

    if (!resp || String(resp).indexOf("ERROR:") === 0) {
      return {
        error: "SELNG(1): " + String(resp || "empty response").slice(0, 180),
      };
    }

    const obj = _parseSelectionJsonObjectFromModelResponse_(resp);
    return _selectionResultFromModelObject_(obj, article);
  } catch (e) {
    return {
      error: "SELNG(1): " + String(e).slice(0, 180),
    };
  }
}

/**
 * 複数記事をまとめてGeminiへ投げ、結果を行番号キーの連想配列で返す。
 */
function _processSelectionScoreArticleBatch_(sheetName, sheet, articles) {
  if (!articles || !articles.length) return;

  articles.forEach(function (article) {
    sheet
      .getRange(article.rowIndex, SELECT_COL_U_STATUS)
      .setValue("RUNNING(BATCH)");
  });

  const resultByRowIndex = _scoreArticlesForSelectionBatch_(
    sheetName,
    articles,
  );

  if (resultByRowIndex && resultByRowIndex.error) {
    if (resultByRowIndex.retryIndividually) {
      Logger.log(
        "[selection-score] batch failed, retry individually: %s",
        resultByRowIndex.error,
      );

      articles.forEach(function (article) {
        const single = _scoreArticleForSelection_(sheetName, article);

        if (single.error) {
          sheet
            .getRange(article.rowIndex, SELECT_COL_U_STATUS)
            .setValue(single.error);
        } else {
          _writeSelectionScoreResultToRow_(sheet, article.rowIndex, single);
        }
      });
      return;
    }

    articles.forEach(function (article) {
      sheet
        .getRange(article.rowIndex, SELECT_COL_U_STATUS)
        .setValue(String(resultByRowIndex.error).slice(0, 220));
    });
    return;
  }

  articles.forEach(function (article) {
    const rowKey = String(article.rowIndex);
    const result = resultByRowIndex && resultByRowIndex[rowKey];

    if (!result) {
      // 一部行だけ返ってこない場合は、その行だけ個別再試行する。
      const single = _scoreArticleForSelection_(sheetName, article);

      if (single.error) {
        sheet
          .getRange(article.rowIndex, SELECT_COL_U_STATUS)
          .setValue("SELNG(BATCH_MISSING): " + single.error.slice(0, 180));
      } else {
        _writeSelectionScoreResultToRow_(sheet, article.rowIndex, single);
      }
      return;
    }

    if (result.error) {
      sheet
        .getRange(article.rowIndex, SELECT_COL_U_STATUS)
        .setValue(result.error);
    } else {
      _writeSelectionScoreResultToRow_(sheet, article.rowIndex, result);
    }
  });
}

/**
 * 5記事単位などでまとめて選定スコア判定する。
 */
function _scoreArticlesForSelectionBatch_(sheetName, articles) {
  try {
    const prompt = _buildArticleSelectionScoreBatchPrompt_(articles);
    const tag =
      sheetName +
      "#rows" +
      articles
        .map(function (a) {
          return a.rowIndex;
        })
        .join("-") +
      ":selectionScoreV2Batch";

    const bundle = getSelectionGeminiApiKeyBundle_(sheetName, "batch", tag);

    const resp = callSelectionGeminiWithKey_(
      bundle && bundle.apiKey,
      prompt,
      tag,
      bundle && bundle.propName,
    );

    if (!resp || String(resp).indexOf("ERROR:") === 0) {
      return {
        error:
          "SELNG(BATCH): " + String(resp || "empty response").slice(0, 180),
        retryIndividually: false,
      };
    }

    let parsed;
    try {
      parsed = _parseSelectionJsonObjectFromModelResponse_(resp);
    } catch (e) {
      return {
        error: "SELNG(BATCH_PARSE): " + String(e).slice(0, 180),
        retryIndividually: true,
      };
    }

    const items = Array.isArray(parsed.results) ? parsed.results : [];
    if (!items.length) {
      return {
        error: "SELNG(BATCH_EMPTY_RESULTS): results array is empty",
        retryIndividually: true,
      };
    }

    const articleByRowIndex = {};
    articles.forEach(function (article) {
      articleByRowIndex[String(article.rowIndex)] = article;
    });

    const resultByRowIndex = {};

    items.forEach(function (obj) {
      const rowIndex = String(obj.rowIndex || "").trim();
      const article = articleByRowIndex[rowIndex];
      if (!article) return;

      resultByRowIndex[rowIndex] = _selectionResultFromModelObject_(
        obj,
        article,
      );
    });

    return resultByRowIndex;
  } catch (e) {
    return {
      error: "SELNG(BATCH): " + String(e).slice(0, 180),
      retryIndividually: true,
    };
  }
}

/************************************************************
 * スコア計算
 ************************************************************/

/**
 * トピック重要度スコア
 *
 * 記事単体の出来ではなく、
 * 「この話題自体が採用対象として重要か」を見る。
 */

function _coalesceSelectionScore_() {
  for (let i = 0; i < arguments.length; i++) {
    const n = Number(arguments[i]);
    if (
      !isNaN(n) &&
      arguments[i] !== "" &&
      arguments[i] !== null &&
      arguments[i] !== undefined
    ) {
      return _clampRound_(n, 0, 10);
    }
  }
  return 0;
}

/************************************************************
 * 同日トピックキー補正・重複抑制
 ************************************************************/

/**
 * モデルのsameDayTopicKeyが揺れる/粗すぎるケースを、編集上の同一事象キーに寄せる。
 */
function _sameDayTopicKeyForSelection_(article, obj) {
  obj = obj || {};
  const modelKey = _normalizeSelectionTopicKey_(
    obj.sameDayTopicKey || obj.topicKey || "",
  );

  // 固有トピックの手書きマッピングは避ける。
  // モデルが返したキーを基本にし、空・粗すぎる・媒体名/日付っぽい場合だけE・F・G・I列から汎用キーを作る。
  if (modelKey && !_looksLikeWeakSelectionTopicKey_(modelKey)) {
    return modelKey.slice(0, 120);
  }

  return _genericSelectionTopicKeyFromArticle_(article, obj);
}

function _looksLikeWeakSelectionTopicKey_(key) {
  const s = String(key || "");
  if (!s || s === "none" || s === "other" || s === "topic") return true;
  if (s.length < 8) return true;
  if (/^\d+$/.test(s)) return true;
  if (
    _hasAny_(s, ["unknown", "general-news", "misc", "article", "news-update"])
  )
    return true;
  return false;
}

function _genericSelectionTopicKeyFromArticle_(article, obj) {
  const category = String((obj && obj.mainCategory) || "other").toLowerCase();
  const source = String(
    article.headlineFinal ||
      article.headlineA ||
      article.headlineBody ||
      article.summary ||
      article.url ||
      "topic",
  );

  const normalized = source
    .toLowerCase()
    .replace(/https?:\/\//g, "")
    .replace(/[0-9０-９]+/g, "n")
    .replace(/\s+/g, "-")
    .replace(/[|｜:：,，.。()（）\[\]「」『』、]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);

  return _normalizeSelectionTopicKey_(
    category + "-" + (normalized || "topic"),
  ).slice(0, 120);
}

function _domesticRegionTierForSelection_(text) {
  const s = String(text || "");
  if (_hasAny_(s, ["ヤンゴン", "Yangon"])) return "yangon";
  if (
    _hasAny_(s, [
      "エーヤワディー",
      "エーヤワディ",
      "エヤワディー",
      "エヤワディ",
      "Ayeyarwady",
      "Ayeyawady",
    ])
  ) {
    return "ayeyarwady";
  }
  if (_hasAny_(s, ["バゴー", "Bago", "Pegu"])) return "bago";
  if (_hasOtherMyanmarDomesticRegionSignal_(s)) return "other_myanmar_region";
  return "none";
}

function _domesticRegionPriorityScoreForSelection_(text) {
  const tier = _domesticRegionTierForSelection_(text);
  if (tier === "yangon") return 10;
  if (tier === "ayeyarwady") return 8;
  if (tier === "bago") return 6;
  if (tier === "other_myanmar_region") return 3;
  return 0;
}

function _hasOtherMyanmarDomesticRegionSignal_(text) {
  return _hasAny_(text, [
    "ネピドー",
    "Naypyidaw",
    "マンダレー",
    "Mandalay",
    "ラカイン",
    "Rakhine",
    "カレン",
    "Kayin",
    "カチン",
    "Kachin",
    "チン州",
    "Chin",
    "シャン",
    "Shan",
    "モン州",
    "Mon",
    "サガイン",
    "Sagaing",
    "マグウェー",
    "Magway",
    "タニンダーリ",
    "Tanintharyi",
    "カヤー",
    "Kayah",
    "郡区",
    "村",
  ]);
}

function _civilianCasualtyFocusScoreForSelection_(text) {
  const s = String(text || "");
  if (!_hasConflictDamageSignalForSelection_(s)) return 0;

  // 空爆・衝突・戦闘・砲撃系は、市民・民間人・住民・子ども・避難民などの
  // 死亡被害が確認できる場合だけ選定候補側のスコアにする。
  if (_hasCivilianDeathVictimSignalForSelection_(s)) {
    return _hasAirstrikeOrClashSignalForSelection_(s) ? 10 : 9;
  }

  return 0;
}

function _militaryCasualtyFocusScoreForSelection_(text) {
  const s = String(text || "");
  if (!_hasCasualtyOrDamageSignalForSelection_(s)) return 0;

  const patterns = [
    /(?:国軍兵士|軍兵士|兵士|軍人|警察官|軍側|国軍側|治安部隊|junta troops|soldiers).{0,28}(?:死亡|死傷|戦死|殺害|負傷|被害|損害|捕虜|拘束|投降|降伏|killed|dead|wounded|casualties)/i,
    /(?:死亡|死傷|戦死|殺害|負傷|被害|損害|捕虜|拘束|投降|降伏|killed|dead|wounded|casualties).{0,28}(?:国軍兵士|軍兵士|兵士|軍人|警察官|軍側|国軍側|治安部隊|junta troops|soldiers)/i,
  ];

  for (let i = 0; i < patterns.length; i++) {
    if (patterns[i].test(s)) return 9;
  }

  return 0;
}

function _resistanceCasualtyFocusScoreForSelection_(text) {
  const s = String(text || "");
  if (!_hasCasualtyOrDamageSignalForSelection_(s)) return 0;

  const patterns = [
    /(?:抵抗勢力|民主派武装勢力|反軍政勢力|人民防衛隊|PDF|民族武装勢力|武装組織|EAO|AA|TNLA|MNDAA|KIA|KNLA|KNDF|BPLA).{0,30}(?:死亡|死傷|戦死|殺害|負傷|被害|損害|捕虜|拘束|killed|dead|wounded|casualties)/i,
    /(?:死亡|死傷|戦死|殺害|負傷|被害|損害|捕虜|拘束|killed|dead|wounded|casualties).{0,30}(?:抵抗勢力|民主派武装勢力|反軍政勢力|人民防衛隊|PDF|民族武装勢力|武装組織|EAO|AA|TNLA|MNDAA|KIA|KNLA|KNDF|BPLA)/i,
  ];

  for (let i = 0; i < patterns.length; i++) {
    if (patterns[i].test(s)) return 7;
  }

  return 0;
}

function _hasAirstrikeOrClashSignalForSelection_(text) {
  const s = String(text || "");

  if (
    _hasAny_(s, [
      "空爆",
      "爆撃",
      "砲撃",
      "衝突",
      "戦闘",
      "交戦",
      "掃討作戦",
      "airstrike",
      "bombing",
      "shelling",
      "clash",
      "clashes",
      "battle",
      "fighting",
    ])
  ) {
    return true;
  }

  return (
    _hasAny_(s, ["攻撃", "襲撃", "attack", "attacked"]) &&
    _hasAny_(s, [
      "国軍",
      "軍",
      "PDF",
      "抵抗勢力",
      "民族武装勢力",
      "住民",
      "村",
      "民間人",
      "junta",
      "troops",
      "civilian",
      "village",
    ])
  );
}

function _hasCivilianDeathVictimSignalForSelection_(text) {
  const s = String(text || "");
  if (!_hasConflictDamageSignalForSelection_(s)) return false;

  const civilianActorPattern =
    "(?:市民|民間人|住民|村民|子ども|子供|児童|女性|高齢者|避難民|難民|農民|労働者|患者|civilian|civilians|resident|residents|villager|villagers|child|children|displaced|refugee|refugees)";
  const deathPattern =
    "(?:死亡|殺害|死者|犠牲|遺体|命を落と|亡くな|killed|dead|died|death|fatalit(?:y|ies))";

  const actorThenDeath = new RegExp(
    civilianActorPattern + ".{0,45}" + deathPattern,
    "i",
  );
  const deathThenActor = new RegExp(
    deathPattern + ".{0,45}" + civilianActorPattern,
    "i",
  );

  if (actorThenDeath.test(s) || deathThenActor.test(s)) return true;

  // 「子どもを含む8人死亡」「避難民キャンプで10人死亡」のような表現を拾う。
  if (
    _hasCivilianActorSignalForSelection_(s) &&
    _hasAny_(s, [
      "死亡",
      "殺害",
      "死者",
      "犠牲",
      "命を落と",
      "亡くな",
      "killed",
      "dead",
      "died",
    ]) &&
    /[0-9０-９]+\s*(?:人|名|people|persons)?/i.test(s)
  ) {
    return true;
  }

  return false;
}

function _isConflictWithoutCivilianDeathExclusionForSelection_(text) {
  const s = String(text || "");
  return (
    _hasAirstrikeOrClashSignalForSelection_(s) &&
    !_hasCivilianDeathVictimSignalForSelection_(s)
  );
}

function _hasKnownMyanmarCompanyNameSignalForSelection_(text) {
  const s = String(text || "");

  return _hasAny_(s, [
    "KBZ",
    "Kanbawza",
    "カンボーザ",
    "CB Bank",
    "AYA Bank",
    "Yoma Bank",
    "ヨマ銀行",
    "Yoma Strategic",
    "Max Myanmar",
    "Shwe Taung",
    "Shwe Thanlwin",
    "City Mart",
    "MPT",
    "Mytel",
    "Wave Money",
    "uab Bank",
    "UAB Bank",
    "AGD Bank",
    "MAB",
    "Myanmar Apex Bank",
    "MAI",
    "Myanmar Airways International",
    "Air KBZ",
    "FMI",
    "First Myanmar Investment",
    "Myanmar Brewery",
    "Dagon Beverages",
    "Htoo Group",
    "Eden Group",
    "IGE",
    "Asia World",
    "Shwe Byain Phyu",
    "Denko",
  ]);
}

function _hasMyanmarIndividualCompanySignalForSelection_(text) {
  const s = String(text || "");

  if (_hasKnownMyanmarCompanyNameSignalForSelection_(s)) return true;

  // ミャンマー直接関連がある文脈で、個別社名らしい固有名詞＋会社・銀行・グループ等を拾う。
  // 「企業」「会社」という一般語だけでは真にしない。
  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  const latinCompanyPattern =
    /\b[A-Z][A-Za-z0-9&.,'’\- ]{2,}\s+(?:Co\.?\s*,?\s*Ltd\.?|Company|Ltd\.?|Limited|Group|Holdings?|Corporation|Corp\.?|PLC|Pte\.?\s*Ltd\.?)\b/;
  if (latinCompanyPattern.test(s)) return true;

  const japaneseCompanyPattern =
    /[一-龥々ァ-ヶA-Za-z0-9・ー＆&（）()]{2,}(?:社|会社|銀行|商社|工業|製造|物流|建設|ホテル|航空|通信|電力|鉱業|石油|ガス|グループ)/;
  if (japaneseCompanyPattern.test(s)) {
    if (
      !_hasAny_(s, [
        "中央銀行",
        "国営紙",
        "国営メディア",
        "人民銀行",
        "世界銀行",
      ])
    ) {
      return true;
    }
  }

  return false;
}

function _hasConflictDamageSignalForSelection_(text) {
  return _hasAny_(text, [
    "戦闘",
    "攻撃",
    "空爆",
    "砲撃",
    "爆撃",
    "衝突",
    "襲撃",
    "攻勢",
    "殺害",
    "放火",
    "airstrike",
    "attack",
    "shelling",
    "clash",
  ]);
}

function _hasCasualtyOrDamageSignalForSelection_(text) {
  return _hasAny_(text, [
    "死亡",
    "死者",
    "死傷",
    "殺害",
    "負傷",
    "犠牲",
    "被害",
    "損害",
    "捕虜",
    "拘束",
    "投降",
    "降伏",
    "killed",
    "dead",
    "wounded",
    "casualties",
  ]);
}

function _hasCivilianActorSignalForSelection_(text) {
  return _hasAny_(text, [
    "市民",
    "民間人",
    "住民",
    "村民",
    "子ども",
    "子供",
    "女性",
    "高齢者",
    "避難民",
    "難民",
    "農民",
    "労働者",
    "患者",
    "学校",
    "病院",
    "civilian",
    "residents",
    "villagers",
    "children",
  ]);
}

function _hasQuantitativeEvidence_(text) {
  const s = String(text || "");
  if (/[0-9０-９]+/.test(s)) return true;
  return _hasAny_(s, [
    "％",
    "%",
    "万",
    "千",
    "億",
    "トン",
    "キロ",
    "人",
    "世帯",
    "社",
    "件",
    "チャット",
    "ドル",
  ]);
}

/**
 * 処理に必要な列だけへ書き込む。
 * 使用列:
 * - R: 最終スコア
 * - S: 説明・補足（重複/過去記事差分/判定理由を集約）
 * - T: 同日トピックキー
 * - U: ステータス
 * - V: 基礎採用スコア（R列を最終採用確率で上書きしても再計算できるよう保持）
 * - W: 代表記事スコア
 */
function _writeSelectionScoreResultToRow_(sheet, rowIndex, result) {
  sheet.getRange(rowIndex, SELECT_COL_R_SCORE).setValue(result.score);
  sheet.getRange(rowIndex, SELECT_COL_S_RATIONALE).setValue(result.rationale);
  sheet
    .getRange(rowIndex, SELECT_COL_T_SAME_DAY_TOPIC_KEY)
    .setValue(result.sameDayTopicKey);
  sheet.getRange(rowIndex, SELECT_COL_U_STATUS).setValue("OK");
  sheet
    .getRange(rowIndex, SELECT_COL_V_TOPIC_IMPORTANCE)
    .setValue(result.score);
  sheet
    .getRange(rowIndex, SELECT_COL_W_REPRESENTATIVE)
    .setValue(result.representativeScore);
}

/************************************************************
 * Gemini APIキー取得・API呼び出し
 ************************************************************/

/**
 * selection.js 専用APIキー取得
 *
 * Script Properties:
 * - GEMINI_SELECTION_API_KEY
 */
function getSelectionGeminiApiKeyBundle_(sheetName, source, tag) {
  const props = PropertiesService.getScriptProperties();
  const apiKey = String(
    props.getProperty("GEMINI_SELECTION_API_KEY") || "",
  ).trim();

  if (!apiKey) {
    throw new Error(
      "Selection Gemini API key is not set. Set GEMINI_SELECTION_API_KEY in Script Properties.",
    );
  }

  return {
    apiKey: apiKey,
    propName: "GEMINI_SELECTION_API_KEY",
  };
}

/**
 * selection.js 専用Gemini API呼び出し
 */
function callSelectionGeminiWithKey_(apiKey, prompt, tag, propName) {
  if (!apiKey) {
    return "ERROR: Selection Gemini API key is empty";
  }

  // 1. primary Gemini
  const primary = _callSelectionGeminiModelWithKey_(
    apiKey,
    GEMINI_SELECTION_MODEL,
    prompt,
    tag + ":primaryGemini",
    propName,
  );

  if (primary.ok) {
    return primary.text;
  }

  Logger.log(
    "[selection-score] primary Gemini failed model=%s code=%s fallback=%s error=%s",
    GEMINI_SELECTION_MODEL,
    primary.code,
    _shouldSelectionFallbackToOtherModel_(primary),
    String(primary.error || "").slice(0, 300),
  );

  // 429 / 503 / high demand 系だけ、別モデルへ逃がす。
  if (!_shouldSelectionFallbackToOtherModel_(primary)) {
    return primary.error;
  }

  Utilities.sleep(SELECT_MODEL_FALLBACK_WAIT_MS);

  // 2. Gemini 2.5 Flash に1回だけフォールバック
  const gemini25 = _callSelectionGeminiModelWithKey_(
    apiKey,
    GEMINI_SELECTION_FALLBACK_MODEL,
    prompt,
    tag + ":fallbackGemini25Flash",
    propName,
  );

  if (gemini25.ok) {
    Logger.log(
      "[selection-score] fallback Gemini succeeded model=%s tag=%s",
      GEMINI_SELECTION_FALLBACK_MODEL,
      tag,
    );
    return gemini25.text;
  }

  Logger.log(
    "[selection-score] fallback Gemini failed model=%s code=%s error=%s",
    GEMINI_SELECTION_FALLBACK_MODEL,
    gemini25.code,
    String(gemini25.error || "").slice(0, 300),
  );

  // 3. Gemini 2.5 Flash も失敗したら OpenAI gpt-5-mini へ1回だけフォールバック
  const openai = _callSelectionOpenAiGpt5Mini_(prompt, tag + ":fallbackOpenAI");

  if (openai.ok) {
    Logger.log("[selection-score] OpenAI fallback succeeded tag=%s", tag);
    return openai.text;
  }

  return (
    "ERROR: Selection all model fallbacks failed. " +
    "primary=" +
    String(primary.error || "").slice(0, 180) +
    " / gemini25=" +
    String(gemini25.error || "").slice(0, 180) +
    " / openai=" +
    String(openai.error || "").slice(0, 180)
  );
}

/**
 * 指定したGeminiモデルへ1回だけ呼び出す。
 */
function _callSelectionGeminiModelWithKey_(
  apiKey,
  modelName,
  prompt,
  tag,
  propName,
) {
  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/" +
    encodeURIComponent(modelName) +
    ":generateContent";

  const payload = {
    contents: [
      {
        role: "user",
        parts: [
          {
            text: String(prompt || ""),
          },
        ],
      },
    ],
    generationConfig: {
      temperature: 0.2,
      topP: 0.9,
      maxOutputTokens: SELECT_MAX_OUTPUT_TOKENS,
      responseMimeType: "application/json",
    },
  };

  const options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "x-goog-api-key": apiKey,
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    _waitBeforeSelectionGeminiCall_();

    const res = UrlFetchApp.fetch(url, options);
    const code = res.getResponseCode();
    const body = res.getContentText();

    if (code < 200 || code >= 300) {
      return {
        ok: false,
        code: code,
        provider: "gemini",
        model: modelName,
        body: body,
        error:
          "ERROR: Selection Gemini HTTP " +
          code +
          " model=" +
          modelName +
          " prop=" +
          String(propName || "") +
          " body=" +
          body.slice(0, 500),
      };
    }

    const json = JSON.parse(body);

    const text =
      json &&
      json.candidates &&
      json.candidates[0] &&
      json.candidates[0].content &&
      json.candidates[0].content.parts &&
      json.candidates[0].content.parts[0] &&
      json.candidates[0].content.parts[0].text;

    if (!text) {
      return {
        ok: false,
        code: code,
        provider: "gemini",
        model: modelName,
        body: body,
        error:
          "ERROR: Selection Gemini empty response model=" +
          modelName +
          " body=" +
          body.slice(0, 500),
      };
    }

    return {
      ok: true,
      code: code,
      provider: "gemini",
      model: modelName,
      text: text,
    };
  } catch (e) {
    return {
      ok: false,
      code: 0,
      provider: "gemini",
      model: modelName,
      error:
        "ERROR: Selection Gemini fetch failed model=" +
        modelName +
        " " +
        String(e).slice(0, 500),
    };
  }
}

/**
 * Gemini側で429 / 503 / high demand系なら別モデル・別プロバイダへ逃がす。
 */
function _shouldSelectionFallbackToOtherModel_(result) {
  if (!result) return false;

  const code = Number(result.code || 0);
  if (code === 429 || code === 503) return true;

  const text = [result.error, result.body].join("\n");

  if (text.indexOf('"code": 429') !== -1) return true;
  if (text.indexOf('"code": 503') !== -1) return true;
  if (text.indexOf("high demand") !== -1) return true;
  if (text.indexOf("currently experiencing high demand") !== -1) return true;
  if (text.indexOf("rate limit") !== -1) return true;
  if (text.indexOf("RESOURCE_EXHAUSTED") !== -1) return true;
  if (text.indexOf("UNAVAILABLE") !== -1) return true;

  return false;
}

/**
 * Gemini APIの1分あたり呼び出し回数を抑制する。
 * Script Properties の GEMINI_SELECTION_TARGET_RPM があればそれを優先する。
 */
function _waitBeforeSelectionGeminiCall_() {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const props = PropertiesService.getScriptProperties();

    const rpmFromProp = Number(
      props.getProperty("GEMINI_SELECTION_TARGET_RPM") || "",
    );

    const targetRpm =
      !isNaN(rpmFromProp) && rpmFromProp > 0
        ? rpmFromProp
        : SELECT_GEMINI_TARGET_RPM;

    const minIntervalMs = Math.ceil(60000 / targetRpm);
    const now = Date.now();
    const last = Number(
      props.getProperty(SELECT_GEMINI_RATE_LAST_CALL_PROP) || 0,
    );

    const waitMs = Math.max(0, last + minIntervalMs - now);

    if (waitMs > 0) {
      Utilities.sleep(waitMs);
    }

    props.setProperty(SELECT_GEMINI_RATE_LAST_CALL_PROP, String(Date.now()));
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}

/**
 * Gemini fallbackも失敗した場合にOpenAI gpt-5-miniへ1回だけ投げる。
 */
function _callSelectionOpenAiGpt5Mini_(prompt, tag) {
  const props = PropertiesService.getScriptProperties();
  const apiKey = String(props.getProperty(OPENAI_API_KEY_PROP) || "").trim();

  if (!apiKey) {
    return {
      ok: false,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      error:
        "ERROR: OpenAI API key is not set. Set " +
        OPENAI_API_KEY_PROP +
        " in Script Properties.",
    };
  }

  const url = "https://api.openai.com/v1/responses";

  const payload = {
    model: GPT5_MINI_MODEL,
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text: String(prompt || ""),
          },
        ],
      },
    ],
    text: {
      format: {
        type: "json_object",
      },
    },
    reasoning: {
      effort: "minimal",
    },
    max_output_tokens: SELECT_MAX_OUTPUT_TOKENS,
    store: false,
  };

  const options = {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + apiKey,
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    const res = UrlFetchApp.fetch(url, options);
    const code = res.getResponseCode();
    const body = res.getContentText();

    if (code < 200 || code >= 300) {
      return {
        ok: false,
        code: code,
        provider: "openai",
        model: GPT5_MINI_MODEL,
        body: body,
        error:
          "ERROR: OpenAI HTTP " +
          code +
          " model=" +
          GPT5_MINI_MODEL +
          " body=" +
          body.slice(0, 500),
      };
    }

    const json = JSON.parse(body);
    const text = _extractSelectionOpenAiResponseText_(json);

    if (!text) {
      return {
        ok: false,
        code: code,
        provider: "openai",
        model: GPT5_MINI_MODEL,
        body: body,
        error:
          "ERROR: OpenAI empty response model=" +
          GPT5_MINI_MODEL +
          " body=" +
          body.slice(0, 500),
      };
    }

    return {
      ok: true,
      code: code,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      text: text,
    };
  } catch (e) {
    return {
      ok: false,
      code: 0,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      error:
        "ERROR: OpenAI fetch failed model=" +
        GPT5_MINI_MODEL +
        " " +
        String(e).slice(0, 500),
    };
  }
}

/**
 * OpenAI Responses APIの返却JSONからテキストを取り出す。
 */
function _extractSelectionOpenAiResponseText_(json) {
  if (!json) return "";

  if (typeof json.output_text === "string" && json.output_text.trim()) {
    return json.output_text.trim();
  }

  const output = Array.isArray(json.output) ? json.output : [];

  for (let i = 0; i < output.length; i++) {
    const item = output[i];
    if (!item) continue;

    const content = Array.isArray(item.content) ? item.content : [];

    for (let j = 0; j < content.length; j++) {
      const part = content[j];
      if (!part) continue;

      if (typeof part.text === "string" && part.text.trim()) {
        return part.text.trim();
      }

      if (
        part.type === "output_text" &&
        typeof part.text === "string" &&
        part.text.trim()
      ) {
        return part.text.trim();
      }
    }
  }

  return "";
}

/**
 * selection.js 専用Gemini設定の確認用
 * APIキー本体はログに出さない
 */
function checkSelectionGeminiScriptProperties_() {
  const props = PropertiesService.getScriptProperties();

  const geminiApiKey = String(
    props.getProperty("GEMINI_SELECTION_API_KEY") || "",
  ).trim();

  const openAiApiKey = String(
    props.getProperty(OPENAI_API_KEY_PROP) || "",
  ).trim();

  const rpm = String(
    props.getProperty("GEMINI_SELECTION_TARGET_RPM") ||
      SELECT_GEMINI_TARGET_RPM,
  ).trim();

  Logger.log("Selection Gemini primary model: " + GEMINI_SELECTION_MODEL);
  Logger.log(
    "Selection Gemini fallback model: " + GEMINI_SELECTION_FALLBACK_MODEL,
  );
  Logger.log("Selection OpenAI fallback model: " + GPT5_MINI_MODEL);
  Logger.log("Selection target RPM: " + rpm);
  Logger.log("Selection Gemini API key: " + (geminiApiKey ? "set" : "not set"));
  Logger.log("OpenAI API key: " + (openAiApiKey ? "set" : "not set"));
}

/************************************************************
 * 共通ヘルパー
 ************************************************************/

/**
 * Gemini応答からJSONオブジェクトを取り出す
 */
function _parseSelectionJsonObjectFromModelResponse_(resp) {
  let text = String(resp || "").trim();

  text = text
    .replace(/^\uFEFF/, "")
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(text);
  } catch (e1) {}

  const jsonText = _extractFirstBalancedSelectionJsonObject_(text);

  if (!jsonText) {
    throw new Error("JSON object not found. head=" + text.slice(0, 160));
  }

  try {
    return JSON.parse(jsonText);
  } catch (e2) {
    throw new Error(
      String(e2) +
        " / jsonHead=" +
        jsonText.slice(0, 160) +
        " / jsonTail=" +
        jsonText.slice(-160),
    );
  }
}

function _extractFirstBalancedSelectionJsonObject_(text) {
  const s = String(text || "");
  const start = s.indexOf("{");
  if (start === -1) return "";

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = start; i < s.length; i++) {
    const ch = s.charAt(i);

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "{") {
      depth++;
    } else if (ch === "}") {
      depth--;
      if (depth === 0) {
        return s.slice(start, i + 1);
      }
      if (depth < 0) return "";
    }
  }

  return "";
}

/**
 * topicKey正規化
 */
function _normalizeSelectionTopicKey_(key) {
  return String(key || "")
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^\p{L}\p{N}-]/gu, "")
    .slice(0, 120);
}

/**
 * Q列の記事重複判定キーを、正規化せずに比較用へ渡す。
 *
 * 重要:
 * - URLの正規化・記号除去・120文字切り詰めは行わない。
 * - 同じ日付で、この返り値が完全一致した場合だけ
 *   S列に「同日重複あり（同一内容グループ）」を付ける。
 */
function _selectionExactDuplicateKeyFromQ_(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

/**
 * sameDayTopicKey を日次グルーピング用に軽く正規化する。
 *
 * 方針:
 * - 固定トピック辞書で広くマージしすぎない。
 * - モデルキーに含まれがちな一般語・媒体性の低い語だけを落とす。
 * - トピックの具体性を残し、初見記事でも過度に同一化しない。
 */
function _mergeRelatedTopicKeys_(normalizedKey) {
  const key = String(normalizedKey || "").trim();
  if (!key) return "";

  // 固定トピック辞書ではなく、モデルキーのノイズだけを落とす。
  // 目的は「広くまとめすぎること」を避けること。
  const stopTokens = [
    "myanmar",
    "burma",
    "junta",
    "sac",
    "news",
    "article",
    "report",
    "update",
    "latest",
    "today",
    "ミャンマー",
    "ビルマ",
    "軍政",
    "国軍",
    "記事",
    "報道",
    "発表",
    "関連",
    "最新",
  ];

  const tokens = key
    .split("-")
    .map(function (x) {
      return String(x || "").trim();
    })
    .filter(function (x) {
      if (!x) return false;
      if (/^[0-9０-９]+$/.test(x)) return false;
      if (/^n+$/.test(x)) return false;
      return stopTokens.indexOf(x) === -1;
    });

  if (!tokens.length) return key;

  // 具体性を残すため、短くしすぎない。
  return tokens.slice(0, 10).join("-").slice(0, 120);
}

/**
 * 日付キー化
 */
function _selectionDateKey_(dateVal) {
  if (!dateVal) return "";

  const tz = Session.getScriptTimeZone();

  if (dateVal instanceof Date) {
    return Utilities.formatDate(dateVal, tz, "yyyyMMdd");
  }

  const s = String(dateVal || "").trim();
  if (!s) return "";

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return Utilities.formatDate(d, tz, "yyyyMMdd");
  }

  return s.replace(/[^\d]/g, "").slice(0, 8);
}

/**
 * いずれかのキーワードを含むか
 */
function _hasAny_(text, keywords) {
  const s = String(text || "");

  return keywords.some(function (kw) {
    return s.indexOf(kw) !== -1;
  });
}

/**
 * 正規表現用エスケープ
 */
function _escapeRegExpForSelection_(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * カタカナ国名の部分一致誤爆を避ける。
 * 例: 「エインドラ」の中の「インド」は国名として扱わない。
 * 一方で「インド国境」「タイ政府」「バングラデシュから」は拾う。
 */
function _hasStandaloneKatakanaTermForSelection_(text, term) {
  const s = String(text || "");
  const word = _escapeRegExpForSelection_(term);

  // 前後がカタカナ/半角カナ/長音なら、人名・外来語の一部である可能性が高いので除外。
  const pattern = new RegExp(
    "(^|[^\u30A0-\u30FF\u31F0-\u31FF\uFF66-\uFF9Fー])" +
      word +
      "(?=$|[^\u30A0-\u30FF\u31F0-\u31FF\uFF66-\uFF9Fー])",
  );

  return pattern.test(s);
}

function _hasAnyStandaloneKatakanaTermForSelection_(text, terms) {
  return (terms || []).some(function (term) {
    return _hasStandaloneKatakanaTermForSelection_(text, term);
  });
}

function _hasAnyLatinCountryTermForSelection_(text, terms) {
  const s = String(text || "");
  return (terms || []).some(function (term) {
    const pattern = new RegExp(
      "(^|[^A-Za-z0-9_])" +
        _escapeRegExpForSelection_(term) +
        "(?=$|[^A-Za-z0-9_])",
      "i",
    );
    return pattern.test(s);
  });
}

/**
 * 数値化
 */
function _safeNumber_(v) {
  const n = Number(v);
  if (isNaN(n)) return 0;
  return n;
}

/**
 * 丸めて範囲内に収める
 */
function _clampRound_(v, min, max) {
  const n = Math.round(Number(v) || 0);
  return Math.max(min, Math.min(max, n));
}

/************************************************************
 * v4: criteria-only automatic selection
 *
 * 変更方針:
 * - 自動選定のスコア基準を指定された5項目に限定する。
 * - O列・P列による過去2日同一トピックの扱いは使わない。
 * - 過去採用済み記事との差分判定はarchive側K列=aの記事だけで行う。
 * - 同日重複記事はS列メモとR列同点化で扱う。
 * - 採用候補では媒体偏り補正を行わず、Q列重複キーはR列同点化で扱う。
 * - R列は日次順位・同日トピック内順位では上下させない。
 *
 * 旧v3の教師データ補正・汎用キーワード判定は削除済み。
 ************************************************************/

function _selectionResultFromModelObject_(obj, article) {
  obj = obj || {};

  const feature = _selectionCriteriaFeatureVector_(obj, article);
  const topicImportanceScore = _calculateSelectionCriteriaScore_(
    feature,
    obj,
    article,
  );
  const representativeScore = _calculateCriteriaRepresentativeScore_(
    obj,
    article,
    feature,
  );
  const finalScore = _applySelectionPostCriteriaAdjustment_(
    topicImportanceScore,
    feature,
    obj,
    article,
  );

  return {
    score: finalScore,
    rationale: _criteriaRationale_(feature, obj, article, finalScore),
    sameDayTopicKey: _sameDayTopicKeyForSelection_(article, obj),
    topicImportanceScore: topicImportanceScore,
    representativeScore: representativeScore,
  };
}

function _buildArticleSelectionScorePrompt_(article) {
  return _buildSelectionCriteriaPrompt_([article], false);
}

function _buildArticleSelectionScoreBatchPrompt_(articles) {
  return _buildSelectionCriteriaPrompt_(articles || [], true);
}

function _buildSelectionCriteriaPrompt_(articles, isBatch) {
  const articleBlocks = (articles || [])
    .map(function (article) {
      return `
---
rowIndex: ${article.rowIndex}
日付: ${article.date}

見出しA:
${article.headlineA}

確定寄せ見出し:
${article.headlineFinal}

本文ベース見出し:
${article.headlineBody}

本文要約（I列）:
${article.summary || ""}

Q列 記事重複判定キー:
${article.duplicateKey || "なし"}

同行O列（過去2日同一TOPIC判定）:
${article.pastTwoDaysSameTopicCount || ""}

archive採用済み記事候補（prodはarchive_prod、devはarchive_dev。K列=a。同行O列が2の場合のみ、日付制限なしでQ列一致・見出し/要約類似度により最大30件を渡す）:
${_adoptedArchiveArticlesForPrompt_(article)}

URL:
${article.url}
---`;
    })
    .join("\n");

  const outputShape = isBatch
    ? `{
  "results": [
    {
      "rowIndex": 0,
      "countryWeightScore": 0,
      "countryWeightTier": "none | top_priority_country_region | second_priority_country_org | other_neighbor_country",
      "domesticRegionPriorityScore": 0,
      "domesticRegionTier": "none | yangon | ayeyarwady | bago | other_myanmar_region",
      "civilianCasualtyFocusScore": 0,
      "militaryCasualtyFocusScore": 0,
      "resistanceCasualtyFocusScore": 0,
      "conflictDamageTarget": "none | civilian | military | resistance | mixed | unclear",
      "strategicMeaningScore": 0,
      "priorityTopicScore": 0,
      "priorityTopicTags": ["prices_fuel_forex", "central_bank_forex_sale_allocation", "official_policy_regulation_announcement", "myanmar_leadership_policy_statement", "law_revision", "business_sme", "power_demand_project_plan", "port_container_shipping_logistics"],
      "adoptedArchiveRelation": "no_archive_adopted | duplicate_same_content | continuation_update | different_angle | related_but_different | unrelated | unknown",
      "adoptedArchiveDiffJa": "archive採用済み記事候補との差分。差分がない場合は空文字",
      "adoptedMatchedArchiveRowIndex": 0,
      "sameDayTopicKey": "short-same-day-topic-key",
      "sameDayDuplicateCandidate": true,
      "representativeScore": 0,
      "rationaleJa": "80〜160字で、指定された基準だけに基づく説明"
    }
  ]
}`
    : `{
  "countryWeightScore": 0,
  "countryWeightTier": "none | top_priority_country_region | second_priority_country_org | other_neighbor_country",
  "domesticRegionPriorityScore": 0,
  "domesticRegionTier": "none | yangon | ayeyarwady | bago | other_myanmar_region",
  "civilianCasualtyFocusScore": 0,
  "militaryCasualtyFocusScore": 0,
  "resistanceCasualtyFocusScore": 0,
  "conflictDamageTarget": "none | civilian | military | resistance | mixed | unclear",
  "strategicMeaningScore": 0,
  "priorityTopicScore": 0,
  "priorityTopicTags": ["prices_fuel_forex", "central_bank_forex_sale_allocation", "official_policy_regulation_announcement", "myanmar_leadership_policy_statement", "law_revision", "business_sme", "power_demand_project_plan", "port_container_shipping_logistics"],
  "adoptedArchiveRelation": "no_archive_adopted | duplicate_same_content | continuation_update | different_angle | related_but_different | unrelated | unknown",
  "adoptedArchiveDiffJa": "archive採用済み記事候補との差分。差分がない場合は空文字",
  "adoptedMatchedArchiveRowIndex": 0,
  "sameDayTopicKey": "short-same-day-topic-key",
  "sameDayDuplicateCandidate": true,
  "representativeScore": 0,
  "rationaleJa": "80〜160字で、指定された基準だけに基づく説明"
}`;

  return `
あなたはミャンマー関連記事の編集選定担当です。
自動選定の評価基準を、以下の項目だけに限定して構造化してください。
旧仕様にあった一般的な公共性、教師データ補正、ソフトニュース補正、媒体の好み、代表性によるスコア加点は使いません。

大前提:
- この処理はミャンマー関連記事の自動選定です。
- E/F/G/I列からミャンマーとの直接関係が確認できない記事は、原則として採用しません。
- ミャンマーに直接関係ない海外一般ニュースは、どれだけ話題性があっても低スコアにします。
- メディア名に Myanmar / ミャンマー が含まれていても、それだけではミャンマー関連とは判定しません。
- 中国、米国、日本、タイ、インド、ロシア、ASEAN、欧州/EU、バングラデシュ、韓国、国連、UNHCR、世界銀行、UNDP、UNFPAなどが含まれていても、ミャンマーとの関係が入力情報上確認できない場合は countryWeightScore を高くしません。
- 海外政府の人事、海外の法改正、海外の紙幣・通貨・選挙・芸能・スポーツなどは、ミャンマーへの直接影響が書かれていない限り、優先トピックにしません。

変更しない重要事項:
- K列には何も書きません。
- O列の過去2日同一TOPIC判定は、過去採用記事との差分判定を行うかどうかの実行条件としてのみ使います。
- 過去採用記事との差分判定は、同行O列の値が2の記事だけで行います。O列が2以外の記事では、archive採用済み記事候補を渡さず、adoptedArchiveRelation は no_archive_adopted として扱います。
- P列の過去2日同一トピック記事タイトルは、このselection.jsでは使いません。
- 重複・続編・別観点の判定は、下記のarchive採用済み記事候補だけを使います。

archive採用済み記事候補の扱い:
- prodシートの判定ではarchive_prod、devシートの判定ではarchive_devを参照します。
- 「archive採用済み記事候補」には、同行O列の値が2の場合だけ、archive側でK列に a が入っている採用済み記事を日付制限なしで走査し、Q列一致・見出し/要約の類似度で絞った候補だけが渡されています。
- この入力が「なし」の場合、adoptedArchiveRelation は no_archive_adopted にします。
- 現在記事とarchive採用済み記事候補の「いつ・誰が・どこで・何を」がほぼ同じで、新しい進展・新しい数字・新しい反応・新しい被害情報・新しい制度変更・新しい観点がなければ adoptedArchiveRelation は duplicate_same_content にします。これは過去に採用済みの同内容を繰り返し選ばないため、選定確率を下げる前提です。
- 同じトピックでも、過去採用済み記事から進展がある、数字が更新された、被害や政策に新情報がある、関係者の反応が追加された、実務上の影響が広がった場合は continuation_update にし、adoptedArchiveDiffJa に差分を書きます。これは過去採用済み記事の続編として選定確率を上げる前提です。
- 同じ大きなトピックでも、焦点・当事者・地域・政策面・経済面・市民被害面など観点が異なる場合は different_angle にし、adoptedArchiveDiffJa に観点差を書きます。これは単純な重複ではなく、選定確率を上げる前提です。
- 関連はあるが別事象なら related_but_different、関係が薄ければ unrelated にします。
- Q列が完全一致する、または見出し・要約の中核が同一なら duplicate_same_content を優先します。ただし、Q列が違っても中身が同じなら duplicate_same_content にします。

自動選定の基準:

1. 国名・国際機関による重みづけ
- 優先度1: 中国、アメリカ/米国、日本、タイ、インド、ロシア、ASEAN、欧州/EU、バングラデシュを含むミャンマー関連記事は countryWeightTier を top_priority_country_region とし、countryWeightScore は10前後を基本にします。これらを含む記事は強い選定候補です。
- 優先度2: 韓国、国連、UNHCR、世界銀行、UNDP、UNFPAを含むミャンマー関連記事は countryWeightTier を second_priority_country_org とし、countryWeightScore は7前後を基本にします。これらを含む記事は選定候補です。
- その他の周辺国・関係国（マレーシア、ラオス、カンボジア、ベトナム、シンガポール、インドネシア、フィリピン等）を含むミャンマー関連記事は other_neighbor_country とし、countryWeightScore は4前後を基本にします。
- ただし、人名・地名・商品名などの一部に偶然「インド」「タイ」等の文字列が含まれるだけの場合は、国名による重みづけに入れません。
- 国名・国際機関・地域名の優先度にヒットしたこと自体を理由に減点しません。外交会談・表敬訪問・訪問などの外交儀礼的側面が強いという判断だけでスコアを下げません。
- ミャンマーとの関係が薄い海外一般ニュースは、国名・国際機関だけで過大評価しません。

2. 紛争・空爆・衝突の被害対象
- 空爆、衝突、戦闘、砲撃などに関する記事で、市民、民間人、住民、子ども、避難民などの死亡被害が含まれている場合は civilianCasualtyFocusScore を高くし、選定候補にします。
- 空爆、衝突、戦闘、砲撃などに関する記事でも、市民、民間人、住民、子ども、避難民などの死亡被害が確認できない場合は選定候補から外す前提で評価します。避難、負傷、住宅破壊だけの場合は civilianCasualtyFocusScore を高くしません。
- 国軍兵士、軍側、治安部隊の死傷・損害だけが中心なら militaryCasualtyFocusScore を高くします。これは選定確率を極めて低くする要因です。
- PDF、抵抗勢力、民族武装勢力側の死傷だけが中心なら resistanceCasualtyFocusScore を中程度から高めにします。これは五分五分の要因です。

3. ミャンマー国内の地域・企業による重みづけ
- ヤンゴンを含むミャンマー関連記事は、国名優先度1位と同列の強い選定候補です。domesticRegionTier を yangon、domesticRegionPriorityScore を10前後にします。
- ミャンマーの個別企業名を含む記事は、強い選定候補です。会社名・銀行名・グループ名・企業名が具体的に示されている場合は高く評価します。
- エーヤワディー、バゴー、それ以外のミャンマー国内地域は、同じ事象・出来事を複数記事が報じている場合の代表記事スコア用の補助基準として扱います。
- 国名・地域名の優先度にヒットしたこと自体を理由に減点しません。ただし、ミャンマーに直接関係がない記事は既存仕様通り採用候補から外します。
- 同一事象内の地域優先順位は、1. ヤンゴン、2. エーヤワディー、3. バゴー、4. それ以外です。
- domesticRegionPriorityScore はこの同一事象内の優先順位も示す補助スコアとして使います。

4. 戦略的な意味合い
- 軍または抵抗勢力側の戦略的な意味合いがある記事は strategicMeaningScore を高くします。
- 対象は戦術ではなく、補給路、主要都市、国境、港湾、空港、経済回廊、支配地域、選挙、同盟関係、停戦、行政支配などの戦略です。

5. 選定確率が高いトピック
以下に該当する記事は priorityTopicScore を高くし、priorityTopicTags に該当タグを入れます。
特に、ミャンマー政府、省庁、地方当局、委員会、局・庁、国営機関などの公的機関が、政策・施策、計画、規制、制度変更、法改正、法案提出、議会提出、上程、審議入り、可決、成立、許認可、行政手続き、税制・関税、輸出入・出入国・労働・企業活動に関わる変更を発表・公表・通達・告示・承認・決定・提出・上程・開始・導入・停止・廃止した記事は、重要トピックとして扱い、priorityTopicTags に official_policy_regulation_announcement を入れます。
ただし、読者に影響する政策・制度・規制・計画の中身がある場合だけ official_policy_regulation_announcement の対象にします。外交儀礼的側面が強いこと自体を理由に減点はしません。
ただし「開発計画・インフラ・地域開発」は例外です。このトピックは、単に開発・インフラ関連語があるだけでは高評価にしません。ミャンマー政府、省庁、地方当局、公的機関、国営機関などが発表・公表・承認・決定・開始・入札公告・説明した内容である場合だけ、選定確率が高いトピックとして扱います。
- 公的機関による政策・制度・規制発表: ミャンマー政府、省庁、当局、委員会、局・庁、国営機関などが、施策、計画、規制、法改正、法案提出、議会提出、上程、審議入り、可決、成立、規則改定、制度変更、許認可、行政手続き、税制、関税、貿易・出入国・労働・企業活動に関わる変更を発表・公表・通達・告示・承認・決定・提出・上程・開始・導入・停止・廃止した記事。該当する場合は priorityTopicTags に official_policy_regulation_announcement を入れます。政策・制度・規制・計画の中身が確認できない場合はこのタグの対象外ですが、外交儀礼的側面だけを理由に減点はしません。
- 物価・燃料・為替・外貨管理: 物価、燃料価格、為替、外貨規制、価格統制、外貨使用制限。チャット/ドルなどの通貨単位が出ていても、寄付額・支援額・賞金・売上など単なる金額表示なら、このトピックには入れません。
- 中央銀行による外貨売却・外貨配分: 中央銀行、CBM、ミャンマー中央銀行などが、CMP企業・輸出企業・市場などから買い取った外貨を、食用油、燃料、医薬品、輸入業者、生活必需品輸入、輸入決済などへ売却・配分・供給する記事。これは選定確率が高い重要トピックとして扱い、priorityTopicTags に central_bank_forex_sale_allocation を入れます。単なる寄付額・売上額・賞金額などの金額表示は対象外です。
- 税・関税・貿易規制: 税制、関税、輸出入規制、貿易許認可、輸入制限、輸出制限
- 外国投資・国内投資・事業許認可: 外国投資、海外投資、国内投資、投資認可、MIC関連、事業許可、企業登録、投資制限
- 輸出入・国境物流: 輸出入実務、通関、国境物流、港湾、陸路物流、越境輸送
- 港湾・コンテナ船入港・海上物流: ヤンゴン港、ミャンマー港湾局、港湾ターミナル、コンテナ船、貨物船、入港予定、寄港、船舶スケジュール、海上貿易ルート、港湾能力、浚渫、大型船受け入れ、輸入増加・輸出促進に関する記事。特にミャンマー港湾局など公的機関が入港予定や港湾能力、海上物流の状況を発表・共有した記事は、priorityTopicTags に port_container_shipping_logistics を入れ、重要トピックとして扱います。単なる港の風景、一般的な船舶事故、式典だけの記事は、このタグで過大評価しません。
- 行政システム・手続き変更: 新システム導入、オンライン申請、システム仕様変更、システム廃止、行政手続きの電子化
- 海外就労者・出入国・旅券・ビザ関連: 海外就労者、出国、海外在住ミャンマー人、旅券、OWIC、ビザ、相互ビザ免除協定、入国・滞在制度、海外就労者・出国・入国に関する制限や規制
- 雇用・労働政策・労使関係: 雇用創出、職業訓練、労働者支援、労働組合、ストライキ、賃金・労働条件、労使紛争、労働力不足
- 開発計画・インフラ・地域開発: 国家開発計画、都市開発、工業団地、インフラ整備、地域開発、経済特区、公共事業。ただし、政府・省庁・当局・公的機関などによる発表、決定、承認、着工・開始、入札公告、説明がある場合のみ priorityTopicTags に development_infrastructure を入れ、priorityTopicScore を高くします。民間企業の一般的な開発案件や、開発・道路・橋・電力などの単語が出るだけの記事は、このタグで高評価にしません。
- 電力需要増対応・電力供給計画: 電力需要増、電力不足への対応、発電所、送電網、変電所、配電網、電力供給拡大、電力プロジェクト、発電・送配電設備の増強、電力計画。ただし、政府・省庁・電力省・当局・公的機関などによる発表、決定、承認、開始、入札公告、説明がある場合のみ priorityTopicTags に power_demand_project_plan を入れ、priorityTopicScore を高くします。単に電力、停電、発電などの単語が出るだけの記事や、民間企業の一般的な案件は、このタグで高評価にしません。
- ビジネス・中小企業・企業支援: ビジネス環境、中小企業、零細企業、MSME/SME、小規模事業者、商工業者、企業支援、事業支援、融資・資金繰り、起業・スタートアップ、商工会議所、企業活動に影響する制度や政策。ミャンマーの個別企業名が具体的に含まれる記事は強い選定候補です。ただし、個別企業の宣伝、慈善活動、芸能・イベント性だけで実務影響が薄い場合は business_sme タグでは過大評価しません。
- ビジネス促進・展示会・イベント: 博覧会、展示会、商談会、投資フォーラム、ビジネスマッチング、産業振興イベント
- 政府・政権運営・人事・声明: 政権人事、就任、異動、解任、役職任命、軍事政権トップ・大統領・政府機関による声明
- ミャンマー指導部・政府・省庁による政策提案・発表・声明: ミンアウンフライン、ミンアウンフライン大統領、国軍総司令官、ミャンマー政府、ミャンマー省庁、省庁、政府機関が、政策・制度・税制・燃料・物価・環境・森林・防災・経済・労働・電力・インフラ・貿易など読者に影響する内容を提案・発表・声明・表明・指示・提出・承認・決定した記事は priorityTopicTags に myanmar_leadership_policy_statement を入れ、通常の単なる声明より高く評価します。政策・制度・経済・実務影響の中身が確認できる記事を対象にします。外交儀礼的側面だけを理由に減点はしません。
- 法制度・法改正・法案提出: 法案、法律案、改正案、法案提出、議会提出、上程、審議入り、可決、成立、法律改正、規則改定、制度変更、厳罰化、罰則強化
- 通信・監視・情報統制: 通信規制、監視、インターネット制限、SNS規制、情報統制
- 食品・医薬品・品質基準: 食品、医薬品、品質基準、衛生基準、認証、検査、流通規制

同日同一トピック・重複:
- 同日に「いつ・誰が・どこで・何を」が一致する記事は、媒体、URL、表現、見出しの違いがあっても sameDayTopicKey を必ず同じにします。
- sameDayTopicKey は、媒体名やURLではなく、具体的な出来事の中核に基づく短いキーにします。
- Q列完全一致は同一内容グループです。
- sameDayTopicKey一致は同一出来事グループです。
- 同日に内容が重複している可能性が高い場合は sameDayDuplicateCandidate を true にします。
- 大きなテーマが同じだけの記事は同じキーにしません。たとえば「インド訪問」だけでは同じキーにせず、「モディ首相と会談」「インド企業家に投資呼びかけ」「ムンバイへ移動」「芳名録に署名・報奨金授与」は別キーにします。
- 例: 「ミンアウンフラインがニューデリーでモディ首相と会談」は媒体が違っても同じキーにします。
- 例: 「インド訪問中にムンバイへ移動」は、「モディ首相と会談」とは別キーにします。

対象記事:
${articleBlocks}

出力はJSONのみ。コードブロック禁止。先頭文字は {、末尾文字は } にしてください。
${isBatch ? "入力された各 rowIndex について必ず1件ずつ results に含めてください。" : ""}

${outputShape}

各スコア項目は0〜10で評価します。高スコアほど、その特徴が強いことを示します。militaryCasualtyFocusScore は高いほど選定確率を下げるリスクが強いことを示します。
`.trim();
}

function _selectionCriteriaFeatureVector_(obj, article) {
  obj = obj || {};

  // 判定には媒体名を含めない。
  // Popular Myanmar など、媒体名だけでミャンマー関連と誤判定しないため。
  const text = _selectionInputText_(article);
  const hasDirectMyanmarRelevance =
    _hasDirectMyanmarRelevanceForSelection_(text);

  const officialBillSubmission =
    _hasOfficialBillSubmissionSignalForSelection_(text);
  const portContainerShippingLogistics =
    _hasPortContainerShippingLogisticsSignalForSelection_(text);
  const myanmarLeadershipPolicyStatement =
    _hasMyanmarLeadershipPolicyProposalAnnouncementSignalForSelection_(text);
  const myanmarIndividualCompany =
    _hasMyanmarIndividualCompanySignalForSelection_(text);
  const conflictWithoutCivilianDeath =
    _isConflictWithoutCivilianDeathExclusionForSelection_(text);
  const conflictWithCivilianDeath =
    _hasCivilianDeathVictimSignalForSelection_(text);

  const detectedCountryTier = _criteriaCountryWeightTierForSelection_(text);
  const detectedCountryScore = _criteriaCountryWeightScoreForSelection_(text);
  const modelCountryTier = _validatedModelCountryWeightTierForSelection_(
    obj,
    text,
  );
  const modelCountryScore = _validatedModelCountryWeightScoreForSelection_(
    obj,
    text,
  );

  return {
    // 国名は本文側の明示シグナルで裏取りできる場合だけ強い採用シグナルとして使う。
    // 「エインドラ」内の「インド」など、カタカナ人名・外来語の部分一致は除外する。
    countryWeight: Math.max(modelCountryScore, detectedCountryScore),
    countryTier: _strongerCountryWeightTierForSelection_(
      modelCountryTier,
      detectedCountryTier,
    ),
    domesticRegionPriority: _coalesceSelectionScore_(
      obj.domesticRegionPriorityScore,
      _domesticRegionPriorityScoreForSelection_(text),
    ),
    domesticRegionTier: String(
      obj.domesticRegionTier || _domesticRegionTierForSelection_(text),
    ),
    civilianCasualtyFocus: _coalesceSelectionScore_(
      obj.civilianCasualtyFocusScore,
      _civilianCasualtyFocusScoreForSelection_(text),
    ),
    militaryCasualtyFocus: _coalesceSelectionScore_(
      obj.militaryCasualtyFocusScore,
      _militaryCasualtyFocusScoreForSelection_(text),
    ),
    resistanceCasualtyFocus: _coalesceSelectionScore_(
      obj.resistanceCasualtyFocusScore,
      _resistanceCasualtyFocusScoreForSelection_(text),
    ),
    conflictDamageTarget: String(obj.conflictDamageTarget || "unclear"),
    strategicMeaning: _coalesceSelectionScore_(
      obj.strategicMeaningScore,
      _strategicMeaningScoreForSelection_(text),
    ),
    priorityTopic: _priorityTopicScoreFromModelOrText_(obj, text),
    priorityTopicTags: _priorityTopicTagsFromModelOrText_(obj, text),
    officialBillSubmission: officialBillSubmission,
    portContainerShippingLogistics: portContainerShippingLogistics,
    myanmarLeadershipPolicyStatement: myanmarLeadershipPolicyStatement,
    myanmarIndividualCompany: myanmarIndividualCompany,
    conflictWithoutCivilianDeath: conflictWithoutCivilianDeath,
    conflictWithCivilianDeath: conflictWithCivilianDeath,
    hasAdoptedArchive: _hasAdoptedArchiveArticlesForSelection_(article),
    adoptedArchiveRelation: _adoptedArchiveRelationFromModel_(obj, article),
    hasDirectMyanmarRelevance: hasDirectMyanmarRelevance,
  };
}

function _calculateSelectionCriteriaScore_(feature, obj, article) {
  feature = feature || {};
  obj = obj || {};

  const text = _selectionInputText_(article);
  let score = 18;

  const country = _safeNumber_(feature.countryWeight);
  const region = _safeNumber_(feature.domesticRegionPriority);
  const civilian = _safeNumber_(feature.civilianCasualtyFocus);
  const military = _safeNumber_(feature.militaryCasualtyFocus);
  const resistance = _safeNumber_(feature.resistanceCasualtyFocus);
  const strategic = _safeNumber_(feature.strategicMeaning);
  const priority = _safeNumber_(feature.priorityTopic);
  const myanmarLeadershipPolicyStatement =
    feature.myanmarLeadershipPolicyStatement === true;

  const countryTier = _canonicalCountryWeightTierForSelection_(
    feature.countryTier,
  );
  const isTopCountry =
    countryTier === "top_priority_country_region" ||
    country >= SELECT_TOP_COUNTRY_WEIGHT_SCORE;
  const isSecondPriorityCountryOrg =
    countryTier === "second_priority_country_org" ||
    country >= SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE;
  const isOtherNeighborCountry =
    countryTier === "other_neighbor_country" ||
    country >= SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE;
  const isYangonRegion =
    String(feature.domesticRegionTier || "") === "yangon" || region >= 10;
  const hasMyanmarIndividualCompany = feature.myanmarIndividualCompany === true;
  const conflictWithoutCivilianDeath =
    feature.conflictWithoutCivilianDeath === true;
  const conflictWithCivilianDeath = feature.conflictWithCivilianDeath === true;
  const hasDirectMyanmarRelevance = feature.hasDirectMyanmarRelevance === true;

  // 国名重みは重要な補助基準だが、国名だけで高得点に張り付かないようにする。
  // 地域は記事単体の重要度ではなく、同一事象内の代表記事選びの補助に寄せる。
  score += country * 3.0;
  score += priority * SELECT_PRIORITY_TOPIC_WEIGHT;
  score += region * 0.2;
  score += strategic * 1.6;

  if (civilian >= 7) score += 22;
  else if (civilian >= 5) score += 14;

  // 法案提出系、港湾・コンテナ船入港系は、
  // 他の重要トピックと同じく priorityTopicScore の加点に含める。
  // 個別の下限補正や専用加点は行わない。

  // ミャンマー指導部・政府・省庁による政策提案・発表・声明は、
  // 単なる人事・儀礼声明とは区別し、読者に影響する政策・制度・経済・環境等の内容がある場合に下限を設ける。
  if (myanmarLeadershipPolicyStatement && hasDirectMyanmarRelevance) {
    score = Math.max(score, SELECT_MYANMAR_LEADERSHIP_POLICY_SCORE_FLOOR);
    if (_hasStrongPublicImpactPolicySignalForSelection_(text)) {
      score = Math.max(
        score,
        SELECT_MYANMAR_LEADERSHIP_POLICY_STRONG_SCORE_FLOOR,
      );
    }
  }

  if (military >= 7 && civilian < 5 && strategic < 7) {
    score -= 36;
    score = Math.min(score, 35);
  } else if (military >= 7 && civilian < 5) {
    score -= 22;
    score = Math.min(score, 48);
  }

  // 抵抗勢力側の被害中心は「五分五分」とし、単独では大きく上げない。
  if (resistance >= 7 && civilian < 5 && strategic < 7) {
    score = Math.min(score, 62);
  }

  // 優先度1の国・地域、ヤンゴン、ミャンマー個別企業名は強い選定候補として下限を設ける。
  // 国名・地域名の優先度にヒットしたこと自体を理由に減点しない。
  if (isTopCountry) {
    score = Math.max(score, SELECT_TOP_COUNTRY_SCORE_FLOOR);
  } else if (isSecondPriorityCountryOrg) {
    score = Math.max(score, SELECT_SECOND_PRIORITY_COUNTRY_ORG_SCORE_FLOOR);
  } else if (isOtherNeighborCountry) {
    score = Math.max(score, SELECT_NEIGHBOR_COUNTRY_SCORE_FLOOR);
  }

  if (isYangonRegion) {
    score = Math.max(score, SELECT_YANGON_SCORE_FLOOR);
  }

  if (hasMyanmarIndividualCompany) {
    score = Math.max(score, SELECT_MYANMAR_INDIVIDUAL_COMPANY_SCORE_FLOOR);
  }

  if (conflictWithCivilianDeath) {
    score = Math.max(score, SELECT_CONFLICT_CIVILIAN_DEATH_SCORE_FLOOR);
  }

  const anyPositiveCriterion =
    isTopCountry ||
    isSecondPriorityCountryOrg ||
    isOtherNeighborCountry ||
    isYangonRegion ||
    hasMyanmarIndividualCompany ||
    conflictWithCivilianDeath ||
    priority >= 7 ||
    civilian >= 7 ||
    strategic >= 7 ||
    resistance >= 7;

  if (!anyPositiveCriterion) score = Math.min(score, 42);

  // 大前提:
  // ミャンマーに直接関係ない記事は、ニュース配信でほぼ採用しない。
  // 中国・米国・日本、法改正、政府人事、海外一般ニュース等の加点があっても、
  // ミャンマーとの直接関係が確認できない場合は上位に入れない。
  if (!hasDirectMyanmarRelevance) {
    score = Math.min(score, 20);
  }

  if (conflictWithoutCivilianDeath) {
    score = Math.min(score, SELECT_CONFLICT_WITHOUT_CIVILIAN_DEATH_SCORE_CAP);
  }

  return _clampRound_(score, 0, 100);
}

function _applySelectionPostCriteriaAdjustment_(score, feature, obj, article) {
  let adjusted = Number(score || 0);
  feature = feature || {};
  obj = obj || {};

  const country = _safeNumber_(feature.countryWeight);
  const countryTier = _canonicalCountryWeightTierForSelection_(
    feature.countryTier,
  );
  const myanmarLeadershipPolicyStatement =
    feature.myanmarLeadershipPolicyStatement === true;
  const text = _selectionInputText_(article);
  const isTopCountry =
    countryTier === "top_priority_country_region" ||
    country >= SELECT_TOP_COUNTRY_WEIGHT_SCORE;
  const isSecondPriorityCountryOrg =
    countryTier === "second_priority_country_org" ||
    country >= SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE;
  const isOtherNeighborCountry =
    countryTier === "other_neighbor_country" ||
    country >= SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE;
  const isYangonRegion =
    String(feature.domesticRegionTier || "") === "yangon" ||
    _safeNumber_(feature.domesticRegionPriority) >= 10;
  const hasMyanmarIndividualCompany = feature.myanmarIndividualCompany === true;
  const conflictWithoutCivilianDeath =
    feature.conflictWithoutCivilianDeath === true;
  const conflictWithCivilianDeath = feature.conflictWithCivilianDeath === true;
  const hasDirectMyanmarRelevance = feature.hasDirectMyanmarRelevance === true;

  // O列・P列の過去2日同一トピック補正は使わない。
  // ただし、過去採用記事との差分判定は、同行O列が2の記事だけで行う。
  // O列が2以外の場合は article.adoptedArchiveArticles が空になり、ここではスコア補正されない。

  if (hasDirectMyanmarRelevance) {
    if (isTopCountry)
      adjusted = Math.max(adjusted, SELECT_TOP_COUNTRY_SCORE_FLOOR);
    else if (isSecondPriorityCountryOrg)
      adjusted = Math.max(
        adjusted,
        SELECT_SECOND_PRIORITY_COUNTRY_ORG_SCORE_FLOOR,
      );
    else if (isOtherNeighborCountry)
      adjusted = Math.max(adjusted, SELECT_NEIGHBOR_COUNTRY_SCORE_FLOOR);

    if (isYangonRegion)
      adjusted = Math.max(adjusted, SELECT_YANGON_SCORE_FLOOR);
    if (hasMyanmarIndividualCompany)
      adjusted = Math.max(
        adjusted,
        SELECT_MYANMAR_INDIVIDUAL_COMPANY_SCORE_FLOOR,
      );
    if (conflictWithCivilianDeath)
      adjusted = Math.max(adjusted, SELECT_CONFLICT_CIVILIAN_DEATH_SCORE_FLOOR);
  } else {
    adjusted = Math.min(adjusted, 20);
  }

  if (myanmarLeadershipPolicyStatement && hasDirectMyanmarRelevance) {
    adjusted = Math.max(
      adjusted,
      _hasStrongPublicImpactPolicySignalForSelection_(text)
        ? SELECT_MYANMAR_LEADERSHIP_POLICY_STRONG_SCORE_FLOOR
        : SELECT_MYANMAR_LEADERSHIP_POLICY_SCORE_FLOOR,
    );
  }

  adjusted = _applyAdoptedArchiveCriteriaAdjustment_(
    adjusted,
    feature,
    obj,
    article,
  );

  if (!hasDirectMyanmarRelevance) {
    adjusted = Math.min(adjusted, 20);
  }

  if (conflictWithoutCivilianDeath) {
    adjusted = Math.min(
      adjusted,
      SELECT_CONFLICT_WITHOUT_CIVILIAN_DEATH_SCORE_CAP,
    );
  }

  return _clampRound_(adjusted, 0, 100);
}

function _calculateCriteriaRepresentativeScore_(obj, article, feature) {
  obj = obj || {};
  feature = feature || {};

  let score = 45;
  score += _safeNumber_(obj.representativeScore) * 4;

  // 代表性は最終スコア基準には使わない。
  // 地域優先順位も、参考用の代表記事スコアにだけ反映する。
  const region = _safeNumber_(feature.domesticRegionPriority);
  score += region * 1.4;

  if (_hasQuantitativeEvidence_(_selectionInputText_(article))) score += 8;

  return _clampRound_(score, 0, 100);
}

function _criteriaReasonTags_(feature, obj, article) {
  feature = feature || {};
  const tags = ["criteria_only_v4"];

  if (feature.hasDirectMyanmarRelevance === false) {
    tags.push("not_direct_myanmar_related_cap");
  }

  const countryWeight = _safeNumber_(feature.countryWeight);
  const countryTier = String(feature.countryTier || "none");

  const canonicalCountryTier =
    _canonicalCountryWeightTierForSelection_(countryTier);
  if (
    canonicalCountryTier === "top_priority_country_region" ||
    countryWeight >= SELECT_TOP_COUNTRY_WEIGHT_SCORE
  )
    tags.push("criteria_country_priority_1");
  else if (
    canonicalCountryTier === "second_priority_country_org" ||
    countryWeight >= SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE
  )
    tags.push("criteria_country_org_priority_2");
  else if (
    canonicalCountryTier === "other_neighbor_country" ||
    countryWeight >= SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE
  )
    tags.push("criteria_country_other_neighbor");

  // 地域は記事単体の重要度ではなく、同一事象内の代表記事選びの補助として記録する。
  if (_safeNumber_(feature.domesticRegionPriority) >= 9)
    tags.push("criteria_region_yangon_priority_1");
  else if (_safeNumber_(feature.domesticRegionPriority) >= 7)
    tags.push("tie_break_region_ayeyarwady");
  else if (_safeNumber_(feature.domesticRegionPriority) >= 5)
    tags.push("tie_break_region_bago");

  if (feature.myanmarIndividualCompany === true)
    tags.push("criteria_myanmar_individual_company");
  if (feature.conflictWithoutCivilianDeath === true)
    tags.push("conflict_without_civilian_death_cap");
  if (feature.conflictWithCivilianDeath === true)
    tags.push("criteria_conflict_civilian_death");
  if (_safeNumber_(feature.civilianCasualtyFocus) >= 7)
    tags.push("criteria_civilian_death_focus");
  if (_safeNumber_(feature.resistanceCasualtyFocus) >= 7)
    tags.push("criteria_resistance_casualty_mixed_priority");
  if (_safeNumber_(feature.strategicMeaning) >= 7)
    tags.push("criteria_strategic_meaning");
  if (_safeNumber_(feature.priorityTopic) >= 7)
    tags.push("criteria_priority_topic");
  if (
    (feature.priorityTopicTags || []).indexOf("development_infrastructure") !==
    -1
  )
    tags.push("criteria_gov_authority_development_infrastructure");
  if (
    (feature.priorityTopicTags || []).indexOf("power_demand_project_plan") !==
    -1
  )
    tags.push("criteria_gov_authority_power_demand_project");
  if (
    (feature.priorityTopicTags || []).indexOf(
      "official_policy_regulation_announcement",
    ) !== -1
  )
    tags.push("criteria_official_policy_regulation_announcement");
  if (
    (feature.priorityTopicTags || []).indexOf(
      "central_bank_forex_sale_allocation",
    ) !== -1
  )
    tags.push("criteria_central_bank_forex_sale_allocation");
  if (feature.officialBillSubmission === true)
    tags.push("criteria_official_bill_submission");
  if (feature.portContainerShippingLogistics === true)
    tags.push("criteria_port_container_shipping_logistics");
  if (feature.myanmarLeadershipPolicyStatement === true)
    tags.push("criteria_myanmar_leadership_policy_statement");

  (feature.priorityTopicTags || []).forEach(function (tag) {
    tags.push("topic_" + tag);
  });

  if (_hasAdoptedArchiveArticlesForSelection_(article)) {
    const archiveRelation = _adoptedArchiveRelationFromModel_(obj, article);
    if (archiveRelation === "duplicate_same_content")
      tags.push("archive_adopted_duplicate_downrank");
    else if (archiveRelation === "continuation_update")
      tags.push("archive_adopted_continuation_update");
    else if (archiveRelation === "different_angle")
      tags.push("archive_adopted_different_angle");
    else if (archiveRelation === "related_but_different")
      tags.push("archive_adopted_related_but_different");
    else if (archiveRelation === "unrelated")
      tags.push("archive_adopted_unrelated");
    else tags.push("archive_adopted_unknown");
  }

  return tags;
}

function _criteriaRationale_(feature, obj, article, finalScore) {
  const modelText = String((obj && obj.rationaleJa) || "").trim();
  const archiveDiff = String((obj && obj.adoptedArchiveDiffJa) || "").trim();
  const tags = _criteriaReasonTags_(feature, obj, article)
    .filter(function (tag) {
      return tag !== "criteria_only_v4";
    })
    .slice(0, 6)
    .join("、");
  const prefix = "指定5基準のみで評価。";
  const basis = tags ? "該当: " + tags + "。" : "強い該当基準は限定的。";
  const scoreText = "基礎スコア" + finalScore + "。";
  const archiveDiffText = archiveDiff
    ? " archive採用記事との差分: " + archiveDiff
    : "";

  return (
    prefix +
    basis +
    scoreText +
    archiveDiffText +
    (modelText ? " " + modelText : "")
  ).slice(0, 260);
}

/**
 * selection.js の評価入力としてE/F/G/I列を結合する。
 * - E: 見出し日本語訳
 * - F: 確定寄せ見出し
 * - G: 本文ベース見出し
 * - I: 本文要約
 *
 * M列の原文タイトル・N列の原文本文は使わない。
 */
function _selectionInputText_(article) {
  article = article || {};
  return [
    article.headlineA,
    article.headlineFinal,
    article.headlineBody,
    article.summary,
  ].join("\n");
}

function _hasDirectMyanmarRelevanceForSelection_(text) {
  const s = String(text || "");

  if (
    _hasAny_(s, [
      "ミャンマー",
      "ビルマ",
      "Myanmar",
      "Burma",
      "Burmese",
      "မြန်မာ",
      "ဗမာ",

      "ヤンゴン",
      "Yangon",
      "ရန်ကုန်",
      "ネピドー",
      "Naypyidaw",
      "နေပြည်တော်",
      "マンダレー",
      "Mandalay",
      "မန္တလေး",
      "エーヤワディ",
      "Ayeyarwady",
      "ဧရာဝတီ",
      "バゴー",
      "Bago",
      "ပဲခူး",
      "サガイン",
      "Sagaing",
      "စစ်ကိုင်း",
      "ラカイン",
      "Rakhine",
      "ရခိုင်",
      "カレン",
      "Kayin",
      "ကရင်",
      "カチン",
      "Kachin",
      "ကချင်",
      "シャン",
      "Shan",
      "ရှမ်း",
      "モン",
      "Mon",
      "မွန်",
      "チン",
      "Chin",
      "ချင်း",
      "カヤー",
      "Kayah",
      "ကယား",
      "マグウェー",
      "Magway",
      "မကွေး",
      "タニンダーリ",
      "Tanintharyi",
      "တနင်္သာရီ",
      "ミャワディ",
      "Myawaddy",
      "မြဝတီ",
      "ムセ",
      "Muse",
      "မူဆယ်",
      "メーソット",
      "Mae Sot",

      "国軍",
      "軍事政権",
      "軍政",
      "SAC",
      "NUG",
      "PDF",
      "抵抗勢力",
      "民主派",
      "民族武装勢力",
      "ミンアウンフライン",
      "アウンサンスーチー",
      "Aung San Suu Kyi",
      "Min Aung Hlaing",
      "အောင်ဆန်းစုကြည်",
      "မင်းအောင်လှိုင်",
    ])
  ) {
    return true;
  }

  // 「ミャンマー」という語がなくても、ミャンマー固有の企業・機関・制度・経済語があれば直接関連とみなす。
  if (_hasKnownMyanmarCompanyNameSignalForSelection_(s)) return true;

  return _hasMyanmarSpecificInstitutionSystemEconomicSignalForSelection_(s);
}

function _hasMyanmarSpecificInstitutionSystemEconomicSignalForSelection_(text) {
  const s = String(text || "");

  const strongSpecificTerms = [
    "CBM",
    "中央銀行",
    "ミャンマー中央銀行",
    "チャット",
    "キヤット",
    "Kyat",
    "MMK",
    "CMP",
    "CMP企業",
    "MIC",
    "DICA",
    "UMFCCI",
    "OWIC",
    "UID",
    "YCDC",
    "MCDC",
    "連邦議会",
    "人民代表院",
    "民族代表院",
    "Pyidaungsu Hluttaw",
    "Hluttaw",
    "လွှတ်တော်",
    "管区政府",
    "州政府",
    "地方政府",
    "市開発委員会",
    "ネピドー評議会",
    "ミャンマー港湾局",
    "港湾局",
    "陸運局",
    "労働局",
    "入国管理局",
    "投資委員会",
    "工業省",
    "商業省",
    "電力省",
    "建設省",
    "運輸・通信省",
    "投資・対外経済関係省",
    "国営紙",
    "国営メディア",
  ];

  if (_hasAny_(s, strongSpecificTerms)) return true;

  const hasEconomicSystem = _hasAny_(s, [
    "外貨規制",
    "外貨管理",
    "外貨売却",
    "外貨配分",
    "外貨供給",
    "ドル売却",
    "ドル供給",
    "外貨購入",
    "食用油輸入",
    "輸入ライセンス",
    "輸出ライセンス",
    "国境貿易",
    "CMP企業から買い取った外貨",
    "燃料輸入",
    "CNG車両",
    "CNG",
  ]);

  const hasAdministrativeSystem = _hasAny_(s, [
    "電子年金",
    "年金引き出し",
    "オンライン申請",
    "行政手続き",
    "認証印",
    "Viber通報",
    "交通ルール違反",
    "旅券申請",
    "海外就労者証明書",
    "労働許可",
  ]);

  const hasMyanmarContextLite = _hasAny_(s, [
    "大統領",
    "政府",
    "当局",
    "省",
    "省庁",
    "委員会",
    "局",
    "庁",
    "大臣",
    "副大臣",
    "長官",
    "議会",
    "法案",
    "規制",
    "通達",
    "告示",
    "公告",
  ]);

  return (
    (hasEconomicSystem || hasAdministrativeSystem) && hasMyanmarContextLite
  );
}

function _canonicalPriorityTopicTagForSelection_(tag) {
  const t = String(tag || "").trim();

  const aliases = {
    overseas_workers_visa: "migration_passport_visa",
    communication_surveillance_control:
      "telecom_surveillance_information_control",
    government_administration_statement: "government_personnel_statement",
    government_statement: "government_personnel_statement",
    government_personnel_statement: "government_personnel_statement",
    gov_official_announcement: "official_policy_regulation_announcement",
    official_government_announcement: "official_policy_regulation_announcement",
    policy_regulation_announcement: "official_policy_regulation_announcement",
    official_policy_regulation_announcement:
      "official_policy_regulation_announcement",
    law_revision: "law_revision",
    port_shipping_logistics: "port_container_shipping_logistics",
    port_container_shipping_logistics: "port_container_shipping_logistics",
    maritime_logistics: "port_container_shipping_logistics",
    central_bank_forex_sale: "central_bank_forex_sale_allocation",
    cbm_forex_sale: "central_bank_forex_sale_allocation",
    central_bank_forex_sale_allocation: "central_bank_forex_sale_allocation",
    myanmar_leadership_policy_statement: "myanmar_leadership_policy_statement",
    leadership_policy_statement: "myanmar_leadership_policy_statement",
    official_leadership_statement: "myanmar_leadership_policy_statement",
  };

  return aliases[t] || t;
}

function _priorityTopicRequiresMyanmarRelevanceForSelection_(tag) {
  const t = _canonicalPriorityTopicTagForSelection_(tag);

  return (
    [
      "official_policy_regulation_announcement",
      "myanmar_leadership_policy_statement",
      "tax_tariff_trade_regulation",
      "investment_business_permit",
      "import_export_border_logistics",
      "port_container_shipping_logistics",
      "administrative_system_procedure",
      "migration_passport_visa",
      "labor_policy_relations",
      "development_infrastructure",
      "power_demand_project_plan",
      "business_sme",
      "business_event_promotion",
      "government_personnel_statement",
      "law_revision",
      "telecom_surveillance_information_control",
      "food_medicine_quality_standard",
    ].indexOf(t) !== -1
  );
}

function _canonicalCountryWeightTierForSelection_(tier) {
  const t = String(tier || "none").trim();

  const aliases = {
    none: "none",
    top_china_us_japan: "top_priority_country_region",
    top_priority_country: "top_priority_country_region",
    top_priority_country_region: "top_priority_country_region",
    priority_1_country_region: "top_priority_country_region",
    neighbor_country: "other_neighbor_country",
    other_neighbor_country: "other_neighbor_country",
    korea_country: "second_priority_country_org",
    second_priority_country: "second_priority_country_org",
    second_priority_org: "second_priority_country_org",
    second_priority_country_org: "second_priority_country_org",
    priority_2_country_org: "second_priority_country_org",
  };

  return aliases[t] || "none";
}

function _countryWeightTierRankForSelection_(tier) {
  const t = _canonicalCountryWeightTierForSelection_(tier);
  const rank = {
    none: 0,
    other_neighbor_country: 1,
    second_priority_country_org: 2,
    top_priority_country_region: 3,
  };
  return rank[t] || 0;
}

function _strongerCountryWeightTierForSelection_(modelTier, detectedTier) {
  const model = _canonicalCountryWeightTierForSelection_(modelTier);
  const detected = _canonicalCountryWeightTierForSelection_(detectedTier);
  return _countryWeightTierRankForSelection_(detected) >
    _countryWeightTierRankForSelection_(model)
    ? detected
    : model;
}

function _validatedModelCountryWeightTierForSelection_(obj, text) {
  obj = obj || {};
  const modelTier = _canonicalCountryWeightTierForSelection_(
    obj.countryWeightTier || "none",
  );
  const detectedTier = _criteriaCountryWeightTierForSelection_(text);

  // E・F・G・I列から明示国名・国際機関を確認できない場合、モデルの国名判定だけでは下限補正を発火させない。
  if (detectedTier === "none") return "none";

  if (
    _countryWeightTierRankForSelection_(modelTier) <=
    _countryWeightTierRankForSelection_(detectedTier)
  ) {
    return modelTier;
  }

  return "none";
}

function _validatedModelCountryWeightScoreForSelection_(obj, text) {
  obj = obj || {};
  const detectedTier = _criteriaCountryWeightTierForSelection_(text);
  if (detectedTier === "none") return 0;

  const modelScore = _coalesceSelectionScore_(obj.countryWeightScore);

  if (detectedTier === "top_priority_country_region") {
    return Math.min(modelScore, SELECT_TOP_COUNTRY_WEIGHT_SCORE);
  }

  if (detectedTier === "second_priority_country_org") {
    return Math.min(
      modelScore,
      SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE,
    );
  }

  if (detectedTier === "other_neighbor_country") {
    return Math.min(modelScore, SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE);
  }

  return 0;
}

function _criteriaCountryWeightTierForSelection_(text) {
  if (_hasTopPriorityCountryRegionSignalForSelection_(text)) {
    return "top_priority_country_region";
  }

  if (_hasSecondPriorityCountryOrgSignalForSelection_(text)) {
    return "second_priority_country_org";
  }

  if (_hasOtherNeighborCountrySignalForSelection_(text)) {
    return "other_neighbor_country";
  }

  return "none";
}

function _hasKoreaCountrySignalForSelection_(text) {
  const s = String(text || "");

  if (
    _hasAny_(s, [
      "韓国",
      "大韓民国",
      "韓国政府",
      "韓国企業",
      "韓国大使館",
      "ソウル",
      "韓国ウォン",
    ])
  ) {
    return true;
  }

  return _hasAnyLatinCountryTermForSelection_(s, [
    "South Korea",
    "Republic of Korea",
    "ROK",
    "Seoul",
    "Korean",
  ]);
}

function _hasSecondPriorityCountryOrgSignalForSelection_(text) {
  const s = String(text || "");

  if (_hasKoreaCountrySignalForSelection_(s)) return true;

  if (
    _hasAny_(s, [
      "国連",
      "国際連合",
      "国連機関",
      "UNHCR",
      "国連難民高等弁務官事務所",
      "世界銀行",
      "世銀",
      "UNDP",
      "国連開発計画",
      "UNFPA",
      "国連人口基金",
    ])
  ) {
    return true;
  }

  return _hasAnyLatinCountryTermForSelection_(s, [
    "United Nations",
    "UN",
    "UNHCR",
    "World Bank",
    "UNDP",
    "UNFPA",
  ]);
}

function _hasTopPriorityCountryRegionSignalForSelection_(text) {
  const s = String(text || "");

  if (
    _hasAny_(s, [
      "中国",
      "北京",
      "人民解放軍",
      "人民元",
      "一帯一路",
      "アメリカ",
      "米国",
      "米政府",
      "米軍",
      "ワシントン",
      "日本",
      "日本政府",
      "東京",
      "日本大使館",
      "ロシア",
      "ロシア政府",
      "モスクワ",
      "ASEAN",
      "東南アジア諸国連合",
      "欧州",
      "ヨーロッパ",
      "欧州連合",
      "EU",
      "欧州委員会",
      "ブリュッセル",
      "バングラデシュ",
      "ダッカ",
      "コックスバザール",
      "バンコク",
      "メーソット",
      "ターク県",
      "チェンマイ",
      "ニューデリー",
      "ミゾラム",
      "マニプール",
    ])
  ) {
    return true;
  }

  if (_hasAnyStandaloneKatakanaTermForSelection_(s, ["タイ", "インド"])) {
    return true;
  }

  return _hasAnyLatinCountryTermForSelection_(s, [
    "China",
    "Beijing",
    "PLA",
    "USA",
    "U.S.",
    "United States",
    "Washington",
    "Japan",
    "Tokyo",
    "JICA",
    "Thailand",
    "Thai",
    "Bangkok",
    "Mae Sot",
    "India",
    "New Delhi",
    "Mizoram",
    "Manipur",
    "Russia",
    "Moscow",
    "ASEAN",
    "European Union",
    "Europe",
    "EU",
    "European Commission",
    "Bangladesh",
    "Dhaka",
    "Cox's Bazar",
  ]);
}

// 後方互換用。旧関数名で参照されても優先度1の判定を返す。
function _hasTopCountrySignalForSelection_(text) {
  return _hasTopPriorityCountryRegionSignalForSelection_(text);
}

function _hasOtherNeighborCountrySignalForSelection_(text) {
  const s = String(text || "");

  if (
    _hasAnyStandaloneKatakanaTermForSelection_(s, [
      "マレーシア",
      "ラオス",
      "カンボジア",
      "ベトナム",
      "シンガポール",
      "インドネシア",
      "フィリピン",
    ])
  ) {
    return true;
  }

  if (_hasAny_(s, ["クアラルンプール", "ASEAN加盟国", "周辺国", "隣国"])) {
    return true;
  }

  return _hasAnyLatinCountryTermForSelection_(s, [
    "Malaysia",
    "Laos",
    "Cambodia",
    "Vietnam",
    "Singapore",
    "Indonesia",
    "Philippines",
  ]);
}

// 後方互換用。旧関数名で参照されてもその他周辺国の判定を返す。
function _hasNeighborCountrySignalForSelection_(text) {
  return _hasOtherNeighborCountrySignalForSelection_(text);
}

function _criteriaCountryWeightScoreForSelection_(text) {
  const tier = _criteriaCountryWeightTierForSelection_(text);

  if (tier === "top_priority_country_region") {
    return SELECT_TOP_COUNTRY_WEIGHT_SCORE;
  }

  if (tier === "second_priority_country_org") {
    return SELECT_SECOND_PRIORITY_COUNTRY_ORG_WEIGHT_SCORE;
  }

  if (tier === "other_neighbor_country") {
    return SELECT_NEIGHBOR_COUNTRY_WEIGHT_SCORE;
  }

  return 0;
}

function _priorityTopicDefinitionsForSelection_() {
  return [
    {
      tag: "official_policy_regulation_announcement",
      keywords: [
        "施策",
        "政策",
        "方針",
        "計画",
        "規制",
        "制限",
        "禁止",
        "緩和",
        "解除",
        "法改正",
        "法律改正",
        "法案",
        "法律",
        "規則改定",
        "制度変更",
        "制度",
        "行政手続き",
        "許認可",
        "許可",
        "認可",
        "ライセンス",
        "登録",
        "税制",
        "関税",
        "輸出入",
        "出国",
        "入国",
        "労働",
        "企業活動",
        "発表",
        "公表",
        "通達",
        "告示",
        "公告",
        "policy",
        "measure",
        "plan",
        "regulation",
        "restriction",
        "law",
        "amendment",
        "rule",
        "procedure",
        "permit",
        "license",
        "approval",
        "notification",
        "directive",
        "announced",
      ],
    },
    {
      tag: "prices_fuel_forex",
      keywords: [
        "物価",
        "燃料価格",
        "燃料",
        "ガソリン",
        "軽油",
        "為替",
        "外貨規制",
        "外貨管理",
        "価格統制",
        "外貨使用制限",
        "ドル",
        "チャット",
        "中央銀行",
        "CBM",
      ],
    },
    {
      tag: "central_bank_forex_sale_allocation",
      keywords: [
        "中央銀行",
        "ミャンマー中央銀行",
        "CBM",
        "外貨売却",
        "外貨を売却",
        "外貨配分",
        "外貨を配分",
        "外貨供給",
        "外貨を供給",
        "外貨を販売",
        "外貨を割り当て",
        "外貨オークション",
        "ドル売却",
        "ドルを売却",
        "ドル供給",
        "ドルを供給",
        "為替市場に売却",
        "CMP企業",
        "輸出企業",
        "食用油輸入",
        "燃料輸入",
        "医薬品輸入",
        "生活必需品輸入",
        "輸入業者",
        "輸入決済",
      ],
    },
    {
      tag: "tax_tariff_trade_regulation",
      keywords: [
        "税制",
        "税",
        "関税",
        "輸出入規制",
        "貿易許認可",
        "輸入制限",
        "輸出制限",
        "輸入ライセンス",
        "輸出ライセンス",
      ],
    },
    {
      tag: "investment_business_permit",
      keywords: [
        "外国投資",
        "海外投資",
        "国内投資",
        "投資認可",
        "MIC",
        "事業許可",
        "企業登録",
        "投資制限",
        "投資委員会",
      ],
    },
    {
      tag: "import_export_border_logistics",
      keywords: [
        "輸出入実務",
        "通関",
        "国境物流",
        "港湾",
        "陸路物流",
        "越境輸送",
        "輸出",
        "輸入",
        "貿易",
        "物流",
        "国境貿易",
      ],
    },
    {
      tag: "port_container_shipping_logistics",
      keywords: [
        "ヤンゴン港",
        "ティラワ港",
        "ミャンマー港湾局",
        "港湾局",
        "港湾",
        "港",
        "港湾ターミナル",
        "コンテナ船",
        "コンテナ貨物",
        "貨物船",
        "船舶",
        "入港",
        "寄港",
        "着岸",
        "入港予定",
        "寄港予定",
        "船舶スケジュール",
        "航路",
        "海上貿易",
        "海上物流",
        "港湾能力",
        "浚渫",
        "大型船",
        "輸入増加",
        "輸出促進",
        "Yangon Port",
        "Myanmar Port Authority",
        "MPA",
        "container vessel",
        "container ship",
        "cargo vessel",
        "vessel schedule",
        "port terminal",
        "maritime trade",
        "shipping route",
        "dredging",
      ],
    },
    {
      tag: "administrative_system_procedure",
      keywords: [
        "新システム導入",
        "オンライン申請",
        "システム仕様変更",
        "システム廃止",
        "行政手続きの電子化",
        "電子化",
        "オンライン",
        "申請システム",
      ],
    },
    {
      tag: "migration_passport_visa",
      keywords: [
        "海外就労者",
        "出国",
        "海外在住ミャンマー人",
        "旅券",
        "パスポート",
        "OWIC",
        "ビザ",
        "相互ビザ免除協定",
        "入国",
        "滞在制度",
        "海外就労",
      ],
    },
    {
      tag: "labor_policy_relations",
      keywords: [
        "雇用創出",
        "職業訓練",
        "労働者支援",
        "労働組合",
        "ストライキ",
        "賃金",
        "労働条件",
        "労使紛争",
        "労働力不足",
        "雇用",
      ],
    },
    {
      tag: "development_infrastructure",
      keywords: [
        "国家開発計画",
        "都市開発",
        "工業団地",
        "インフラ整備",
        "地域開発",
        "経済特区",
        "公共事業",
        "道路",
        "橋",
        "鉄道",
        "電力",
      ],
    },
    {
      tag: "power_demand_project_plan",
      keywords: [
        "電力需要増",
        "電力需要",
        "電力不足",
        "電力供給",
        "電力需給",
        "発電所",
        "発電",
        "送電",
        "配電",
        "変電所",
        "送電網",
        "配電網",
        "電力網",
        "グリッド",
        "水力発電",
        "太陽光発電",
        "天然ガス発電",
        "LNG発電",
        "電力プロジェクト",
        "電力計画",
        "power demand",
        "electricity demand",
        "power supply",
        "power generation",
        "transmission",
        "grid",
      ],
    },
    {
      tag: "business_sme",
      keywords: [
        "ビジネス環境",
        "ビジネス",
        "中小企業",
        "零細企業",
        "小規模事業者",
        "小規模企業",
        "MSME",
        "MSMEs",
        "SME",
        "SMEs",
        "商工業者",
        "商工会議所",
        "企業支援",
        "事業支援",
        "事業者支援",
        "融資",
        "資金繰り",
        "企業活動",
        "民間企業",
        "起業",
        "スタートアップ",
        "business environment",
        "small and medium",
        "micro small and medium",
        "chamber of commerce",
      ],
    },
    {
      tag: "business_event_promotion",
      keywords: [
        "博覧会",
        "展示会",
        "商談会",
        "投資フォーラム",
        "ビジネスマッチング",
        "産業振興イベント",
      ],
    },
    {
      tag: "government_personnel_statement",
      keywords: [
        "政権人事",
        "就任",
        "異動",
        "解任",
        "役職任命",
        "声明",
        "任命",
        "大統領",
        "政府機関",
        "軍事政権トップ",
        "ミンアウンフライン",
      ],
    },
    {
      tag: "myanmar_leadership_policy_statement",
      keywords: [
        "ミンアウンフライン",
        "ミンアウンフライン大統領",
        "ミンアウンフライン",
        "ミンアウンフライン大統領",
        "国軍総司令官",
        "ミャンマー政府",
        "ミャンマー省庁",
        "政府",
        "省庁",
        "政策",
        "提案",
        "発表",
        "声明",
        "表明",
        "方針",
        "計画",
        "指示",
        "承認",
        "決定",
      ],
    },
    {
      tag: "law_revision",
      keywords: [
        "法案",
        "法律案",
        "改正案",
        "法案提出",
        "議会提出",
        "法律改正",
        "規則改定",
        "制度変更",
        "法改正",
        "厳罰化",
        "罰則強化",
        "刑罰強化",
        "処罰強化",
        "draft law",
        "draft bill",
      ],
    },
    {
      tag: "telecom_surveillance_information_control",
      keywords: [
        "通信規制",
        "監視",
        "インターネット制限",
        "SNS規制",
        "情報統制",
        "通信遮断",
      ],
    },
    {
      tag: "food_medicine_quality_standard",
      keywords: [
        "食品",
        "医薬品",
        "品質基準",
        "衛生基準",
        "認証",
        "検査",
        "流通規制",
      ],
    },
  ];
}

function _priorityTopicTagsForSelection_(text) {
  const s = String(text || "");
  const out = [];
  _priorityTopicDefinitionsForSelection_().forEach(function (def) {
    if (def.tag === "official_policy_regulation_announcement") {
      if (_hasOfficialPolicyRegulationAnnouncementSignalForSelection_(s)) {
        out.push(def.tag);
      }
      return;
    }

    if (def.tag === "prices_fuel_forex") {
      if (_hasPricesFuelForexSignalForSelection_(s)) out.push(def.tag);
      return;
    }

    if (def.tag === "myanmar_leadership_policy_statement") {
      if (
        _hasMyanmarLeadershipPolicyProposalAnnouncementSignalForSelection_(s)
      ) {
        out.push(def.tag);
      }
      return;
    }

    if (def.tag === "central_bank_forex_sale_allocation") {
      if (_hasCentralBankForexSaleAllocationSignalForSelection_(s))
        out.push(def.tag);
      return;
    }

    if (def.tag === "power_demand_project_plan") {
      if (_hasPowerDemandProjectAuthorityAnnouncementSignalForSelection_(s)) {
        out.push(def.tag);
      }
      return;
    }

    if (def.tag === "business_sme") {
      if (_hasBusinessSmeSignalForSelection_(s)) out.push(def.tag);
      return;
    }

    if (def.tag === "port_container_shipping_logistics") {
      if (_hasPortContainerShippingLogisticsSignalForSelection_(s)) {
        out.push(def.tag);
      }
      return;
    }

    if (!_hasAny_(s, def.keywords)) return;

    // 開発計画・インフラ・地域開発は、単なるトピック該当では高評価にしない。
    // ミャンマー政府・省庁・当局などからの発表/決定/承認/公告等がある場合だけ、優先トピックタグにする。
    if (
      def.tag === "development_infrastructure" &&
      !_hasDevelopmentInfrastructureAuthorityAnnouncementSignalForSelection_(s)
    ) {
      return;
    }

    out.push(def.tag);
  });
  return out;
}

function _hasPortContainerShippingLogisticsSignalForSelection_(text) {
  const s = String(text || "");

  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  const hasPortSignal = _hasAny_(s, [
    "ヤンゴン港",
    "ティラワ港",
    "港湾局",
    "ミャンマー港湾局",
    "港湾ターミナル",
    "港湾",
    "Yangon Port",
    "Thilawa Port",
    "Myanmar Port Authority",
    "MPA",
    "port terminal",
  ]);

  const hasContainerOrVesselSignal = _hasAny_(s, [
    "コンテナ船",
    "コンテナ貨物",
    "貨物船",
    "船舶",
    "大型船",
    "隻",
    "入港",
    "寄港",
    "着岸",
    "container vessel",
    "container ship",
    "cargo vessel",
    "vessel",
    "ship",
  ]);

  const hasShippingLogisticsContext = _hasAny_(s, [
    "入港予定",
    "寄港予定",
    "入港スケジュール",
    "船舶スケジュール",
    "航路",
    "海上貿易",
    "海上物流",
    "港湾能力",
    "浚渫",
    "輸入増加",
    "輸出促進",
    "輸出",
    "輸入",
    "貿易",
    "物流",
    "需要",
    "ルート",
    "schedule",
    "shipping route",
    "maritime trade",
    "logistics",
    "port capacity",
    "dredging",
    "import",
    "export",
    "trade",
  ]);

  const hasOfficialOrOperationalUpdate =
    _hasMyanmarGovernmentAuthoritySignalForSelection_(s) ||
    _hasOfficialAnnouncementDecisionSignalForSelection_(s) ||
    _hasQuantitativeEvidence_(s);

  if (!hasPortSignal) return false;
  if (!hasContainerOrVesselSignal) return false;
  if (!hasShippingLogisticsContext) return false;

  // 港・船舶という語だけの一般記事ではなく、入港予定や港湾能力などの実務影響がある記事に限定する。
  return hasOfficialOrOperationalUpdate;
}

function _hasPricesFuelForexSignalForSelection_(text) {
  const s = String(text || "");

  // ミャンマーに直接関係ない海外一般ニュースの物価・燃料・為替話題を
  // ミャンマー記事の重要トピックとして扱わない。
  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  // これらは単独でも、物価・燃料・為替・外貨管理トピックとして十分に強い。
  if (
    _hasAny_(s, [
      "物価",
      "インフレ",
      "燃料価格",
      "ガソリン価格",
      "軽油価格",
      "為替",
      "為替レート",
      "外貨規制",
      "外貨管理",
      "価格統制",
      "外貨使用制限",
      "中央銀行",
      "CBM",
      "exchange rate",
      "foreign currency",
      "fuel price",
      "inflation",
    ])
  ) {
    return true;
  }

  // 「チャット」「ドル」は寄付額・支援額・賞金などにも頻出するため、単独では使わない。
  const hasCurrencyAmount = _hasAny_(s, [
    "チャット",
    "ドル",
    "MMK",
    "Kyat",
    "kyat",
    "USD",
    "dollar",
    "Dollar",
  ]);
  const hasMarketOrRegulationContext = _hasAny_(s, [
    "レート",
    "外貨",
    "通貨",
    "両替",
    "価格",
    "市場",
    "銀行",
    "規制",
    "制限",
    "使用制限",
    "輸入",
    "輸出",
    "貿易",
    "決済",
    "送金",
    "下落",
    "上昇",
    "高騰",
    "急騰",
    "急落",
    "売買",
    "取引",
    "market",
    "rate",
    "currency",
    "forex",
    "bank",
    "restriction",
  ]);
  if (hasCurrencyAmount && hasMarketOrRegulationContext) return true;

  const hasFuel = _hasAny_(s, [
    "燃料",
    "ガソリン",
    "軽油",
    "diesel",
    "petrol",
    "fuel",
  ]);
  const hasFuelMarketContext = _hasAny_(s, [
    "価格",
    "値上げ",
    "値下げ",
    "不足",
    "供給",
    "販売",
    "輸入",
    "輸出",
    "規制",
    "制限",
    "配給",
    "市場",
    "高騰",
    "price",
    "shortage",
    "supply",
    "import",
    "export",
  ]);

  return hasFuel && hasFuelMarketContext;
}

function _hasCentralBankForexSaleAllocationSignalForSelection_(text) {
  const s = String(text || "");

  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  const hasCentralBank = _hasAny_(s, [
    "中央銀行",
    "ミャンマー中央銀行",
    "CBM",
    "Central Bank of Myanmar",
  ]);

  const hasForexSaleOrAllocation = _hasAny_(s, [
    "外貨売却",
    "外貨を売却",
    "外貨販売",
    "外貨を販売",
    "外貨配分",
    "外貨を配分",
    "外貨供給",
    "外貨を供給",
    "外貨を割り当て",
    "外貨を充当",
    "ドル売却",
    "ドルを売却",
    "ドル販売",
    "ドルを販売",
    "ドル供給",
    "ドルを供給",
    "為替市場に売却",
    "外貨オークション",
    "売却した外貨",
    "買い取った外貨",
    "購入した外貨",
    "purchased foreign currency",
    "sold foreign currency",
    "sell foreign currency",
    "foreign currency sale",
    "foreign exchange sale",
    "foreign currency allocation",
    "USD sale",
    "dollar sale",
  ]);

  const hasUseOrRecipientContext = _hasAny_(s, [
    "CMP企業",
    "輸出企業",
    "輸出業者",
    "輸入業者",
    "食用油輸入",
    "燃料輸入",
    "医薬品輸入",
    "生活必需品輸入",
    "輸入決済",
    "輸入代金",
    "輸入に充て",
    "輸入向け",
    "輸入業者へ",
    "食用油",
    "燃料",
    "医薬品",
    "生活必需品",
    "essential goods",
    "edible oil",
    "fuel import",
    "medicine import",
    "importers",
    "CMP",
  ]);

  // 中央銀行が関与し、外貨を売却・配分・供給する実務影響記事に限定する。
  // 単なる寄付額・売上額・賞金額などの金額表示は対象外。
  return hasCentralBank && hasForexSaleOrAllocation && hasUseOrRecipientContext;
}

function _priorityTopicScoreForSelection_(text) {
  const tags = _priorityTopicTagsForSelection_(text);
  return _priorityTopicScoreFromTags_(tags);
}

function _priorityTopicScoreFromModelOrText_(obj, text) {
  const tags = _priorityTopicTagsFromModelOrText_(obj, text);
  if (!tags.length) return 0;

  const modelScore = _coalesceSelectionScore_(obj && obj.priorityTopicScore);
  const tagScore = _priorityTopicScoreFromTags_(tags);

  // モデルが開発・インフラを過大評価しても、政府・当局発表シグナルがない場合は
  // _priorityTopicTagsFromModelOrText_ 側でタグを除去するため、ここで高スコア化しない。
  return Math.max(tagScore, modelScore);
}

function _priorityTopicScoreFromTags_(tags) {
  const list = Array.isArray(tags) ? tags : [];
  if (!list.length) return 0;
  if (list.length >= 3) return 10;
  if (list.length === 2) return 9;
  return 8;
}

function _isPriorityTopicTagSupportedByTextForSelection_(tag, text) {
  const s = String(text || "");
  const t = _canonicalPriorityTopicTagForSelection_(tag);

  if (!t) return false;

  // ミャンマー関連性がない場合、海外政府・海外法改正・海外制度変更などを
  // ミャンマー記事の優先トピックとして扱わない。
  if (
    _priorityTopicRequiresMyanmarRelevanceForSelection_(t) &&
    !_hasDirectMyanmarRelevanceForSelection_(s)
  ) {
    return false;
  }

  if (t === "official_policy_regulation_announcement") {
    return _hasOfficialPolicyRegulationAnnouncementSignalForSelection_(s);
  }

  if (t === "prices_fuel_forex") {
    return _hasPricesFuelForexSignalForSelection_(s);
  }

  if (t === "myanmar_leadership_policy_statement") {
    return _hasMyanmarLeadershipPolicyProposalAnnouncementSignalForSelection_(
      s,
    );
  }

  if (t === "central_bank_forex_sale_allocation") {
    return _hasCentralBankForexSaleAllocationSignalForSelection_(s);
  }

  if (t === "development_infrastructure") {
    return _hasDevelopmentInfrastructureAuthorityAnnouncementSignalForSelection_(
      s,
    );
  }

  if (t === "power_demand_project_plan") {
    return _hasPowerDemandProjectAuthorityAnnouncementSignalForSelection_(s);
  }

  if (t === "business_sme") {
    return _hasBusinessSmeSignalForSelection_(s);
  }

  if (t === "port_container_shipping_logistics") {
    return _hasPortContainerShippingLogisticsSignalForSelection_(s);
  }

  const defs = _priorityTopicDefinitionsForSelection_();
  for (let i = 0; i < defs.length; i++) {
    if (defs[i].tag === t) {
      return _hasAny_(s, defs[i].keywords);
    }
  }

  // 未定義タグはモデルが勝手に作った可能性が高いため、採用しない。
  return false;
}

function _priorityTopicTagsFromModelOrText_(obj, text) {
  const sourceText = String(text || "");
  const out = [];
  if (obj && Array.isArray(obj.priorityTopicTags)) {
    obj.priorityTopicTags.forEach(function (tag) {
      const s = _canonicalPriorityTopicTagForSelection_(tag);
      if (!s) return;
      if (!_isPriorityTopicTagSupportedByTextForSelection_(s, sourceText)) {
        return;
      }
      if (out.indexOf(s) === -1) out.push(s);
    });
  }
  _priorityTopicTagsForSelection_(sourceText).forEach(function (tag) {
    const s = _canonicalPriorityTopicTagForSelection_(tag);
    if (!s) return;
    if (!_isPriorityTopicTagSupportedByTextForSelection_(s, sourceText)) return;
    if (out.indexOf(s) === -1) out.push(s);
  });
  return out.slice(0, 8);
}

function _hasMyanmarLeadershipPolicyProposalAnnouncementSignalForSelection_(
  text,
) {
  const s = String(text || "");

  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  const hasLeadershipOrOfficialInstitution = _hasAny_(s, [
    "ミンアウンフライン",
    "ミンアウンフライン大統領",
    "ミンアウンフライン",
    "ミンアウンフライン大統領",
    "国軍総司令官",
    "ミャンマー政府",
    "ミャンマー省庁",
    "ミャンマー当局",
    "政府",
    "省庁",
    "省",
    "当局",
    "委員会",
    "局",
    "庁",
    "大統領",
    "大臣",
    "副大臣",
    "長官",
    "連邦議会",
    "人民代表院",
    "民族代表院",
    "議会",
    "SAC",
    "Pyidaungsu Hluttaw",
    "Hluttaw",
    "မင်းအောင်လှိုင်",
  ]);

  if (!hasLeadershipOrOfficialInstitution) return false;

  const hasProposalAnnouncementStatementAction = _hasAny_(s, [
    "政策",
    "提案",
    "発表",
    "公表",
    "声明",
    "表明",
    "方針",
    "計画",
    "指示",
    "要請",
    "呼びかけ",
    "強調",
    "説明",
    "通達",
    "告示",
    "公告",
    "提出",
    "上程",
    "審議",
    "承認",
    "決定",
    "実施",
    "導入",
    "開始",
    "推進",
    "促進",
    "proposal",
    "policy",
    "announced",
    "statement",
    "directive",
    "submitted",
    "approved",
    "decided",
  ]);

  if (!hasProposalAnnouncementStatementAction) return false;

  if (!_hasStrongPublicImpactPolicySignalForSelection_(s)) return false;

  // 単なる視察・式典・表敬訪問・挨拶だけの記事は除外する。
  const looksLikeCeremonyOrCourtesyOnly =
    _hasAny_(s, [
      "表敬訪問",
      "視察",
      "式典",
      "記念式典",
      "開会式",
      "閉会式",
      "挨拶",
      "祝辞",
      "芳名録",
      "報奨金",
      "courtesy call",
      "ceremony",
      "visited",
      "inspection",
    ]) &&
    !_hasAny_(s, [
      "政策",
      "制度",
      "規制",
      "税",
      "関税",
      "免除",
      "法案",
      "法律",
      "燃料",
      "物価",
      "環境",
      "森林",
      "植林",
      "気候変動",
      "災害",
      "輸出",
      "輸入",
      "労働",
      "電力",
      "インフラ",
      "投資",
      "企業",
      "農業",
      "製造業",
    ]);

  return !looksLikeCeremonyOrCourtesyOnly;
}

function _hasStrongPublicImpactPolicySignalForSelection_(text) {
  return _hasAny_(text, [
    "政策",
    "制度",
    "規制",
    "制限",
    "禁止",
    "緩和",
    "解除",
    "法案",
    "法律",
    "法改正",
    "改正案",
    "税",
    "税制",
    "関税",
    "免税",
    "免除",
    "特別物品税",
    "商業税",
    "燃料",
    "ディーゼル",
    "ガソリン",
    "物価",
    "為替",
    "外貨",
    "輸出",
    "輸入",
    "貿易",
    "物流",
    "港湾",
    "コンテナ船",
    "投資",
    "企業",
    "中小企業",
    "事業",
    "労働",
    "雇用",
    "海外就労",
    "旅券",
    "ビザ",
    "出国",
    "入国",
    "電力",
    "発電",
    "送電",
    "インフラ",
    "開発計画",
    "道路",
    "橋",
    "農業",
    "製造業",
    "産業",
    "環境",
    "森林",
    "植林",
    "マングローブ",
    "気候変動",
    "自然災害",
    "災害",
    "防災",
    "教育",
    "医療",
    "保健",
    "通信",
    "監視",
    "情報統制",
    "品質基準",
    "衛生基準",
    "policy",
    "regulation",
    "law",
    "tax",
    "fuel",
    "trade",
    "export",
    "import",
    "investment",
    "labour",
    "labor",
    "employment",
    "electricity",
    "infrastructure",
    "environment",
    "climate",
  ]);
}

function _hasOfficialPolicyRegulationAnnouncementSignalForSelection_(text) {
  const s = String(text || "");

  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;
  if (!_hasMyanmarGovernmentAuthoritySignalForSelection_(s)) return false;
  if (!_hasOfficialAnnouncementDecisionSignalForSelection_(s)) return false;
  if (!_hasOfficialPolicyRegulationTopicSignalForSelection_(s)) return false;

  // 公的機関が出ていても、単なる式典・視察・表敬訪問・PRだけなら重要トピック扱いしない。
  const looksLikeCeremonyOrVisitOnly =
    _hasAny_(s, [
      "表敬訪問",
      "視察",
      "submitted",
      "tabled",
      "passed",
      "adopted",
      "式典",
      "記念式典",
      "開会式",
      "閉会式",
      "挨拶",
      "祝辞",
      "会合",
      "協議",
      "meeting",
      "ceremony",
      "visited",
      "inspection",
    ]) &&
    !_hasAny_(s, [
      "規制",
      "制限",
      "禁止",
      "法改正",
      "法律改正",
      "法案提出",
      "議会提出",
      "改正案",
      "厳罰化",
      "罰則強化",
      "規則改定",
      "制度変更",
      "通達",
      "告示",
      "公告",
      "提出",
      "上程",
      "審議",
      "可決",
      "成立",
      "許認可",
      "許可",
      "認可",
      "ライセンス",
      "税制",
      "関税",
      "輸出入",
      "出国",
      "入国",
      "労働",
      "企業活動",
      "開始",
      "導入",
      "停止",
      "廃止",
      "施行",
      "実施",
      "regulation",
      "restriction",
      "amendment",
      "bill",
      "submitted",
      "tabled",
      "passed",
      "adopted",
      "notification",
      "directive",
      "permit",
      "license",
    ]);

  if (looksLikeCeremonyOrVisitOnly) return false;

  return true;
}

function _hasOfficialBillSubmissionSignalForSelection_(text) {
  const s = String(text || "");

  if (!_hasDirectMyanmarRelevanceForSelection_(s)) return false;

  const hasAuthority =
    _hasMyanmarGovernmentAuthoritySignalForSelection_(s) ||
    _hasAny_(s, [
      "連邦議会",
      "人民代表院",
      "民族代表院",
      "議会",
      "Pyidaungsu Hluttaw",
      "Hluttaw",
      "လွှတ်တော်",
    ]);

  if (!hasAuthority) return false;

  const hasBillOrLegalChange =
    _hasAny_(s, [
      "法案",
      "法律案",
      "改正案",
      "法案提出",
      "議会提出",
      "法改正",
      "法律改正",
      "規則改定",
      "制度変更",
      "厳罰化",
      "罰則強化",
      "刑罰強化",
      "処罰強化",
      "draft law",
      "draft bill",
    ]) || /(^|[^A-Za-z])bill(s)?(?=$|[^A-Za-z])/i.test(s);

  const hasLegislativeAction = _hasAny_(s, [
    "提出",
    "議会に提出",
    "連邦議会に提出",
    "上程",
    "審議",
    "審議入り",
    "可決",
    "成立",
    "採択",
    "承認",
    "submitted",
    "tabled",
    "approved",
    "passed",
    "adopted",
  ]);

  return hasBillOrLegalChange && hasLegislativeAction;
}

function _hasOfficialPolicyRegulationTopicSignalForSelection_(text) {
  return _hasAny_(text, [
    "施策",
    "政策",
    "方針",
    "計画",
    "実施計画",
    "ロードマップ",
    "規制",
    "制限",
    "禁止",
    "緩和",
    "解除",
    "法改正",
    "法律改正",
    "法案",
    "法律案",
    "改正案",
    "法案提出",
    "議会提出",
    "厳罰化",
    "罰則強化",
    "刑罰強化",
    "処罰強化",
    "法律",
    "規則改定",
    "規則",
    "制度変更",
    "制度",
    "行政手続き",
    "手続き",
    "オンライン申請",
    "許認可",
    "許可",
    "認可",
    "ライセンス",
    "免許",
    "登録",
    "承認",
    "税制",
    "税率",
    "関税",
    "輸出入",
    "港湾",
    "コンテナ船",
    "入港",
    "寄港",
    "航路",
    "海上貿易",
    "船舶スケジュール",
    "港湾能力",
    "輸出制限",
    "輸入制限",
    "貿易許認可",
    "出国",
    "入国",
    "ビザ",
    "旅券",
    "海外就労",
    "労働",
    "雇用",
    "企業活動",
    "事業許可",
    "投資認可",
    "品質基準",
    "衛生基準",
    "認証",
    "検査",
    "流通規制",
    "policy",
    "measure",
    "plan",
    "roadmap",
    "regulation",
    "restriction",
    "ban",
    "relaxation",
    "law",
    "amendment",
    "bill",
    "rule",
    "procedure",
    "permit",
    "license",
    "approval",
    "tax",
    "tariff",
    "import",
    "export",
    "visa",
    "passport",
    "labour",
    "labor",
    "employment",
    "business permit",
    "quality standard",
    "certification",
    "draft law",
    "draft bill",
  ]);
}

function _hasPowerDemandProjectAuthorityAnnouncementSignalForSelection_(text) {
  const s = String(text || "");
  if (!_hasPowerDemandProjectTopicSignalForSelection_(s)) return false;
  return (
    _hasMyanmarGovernmentAuthoritySignalForSelection_(s) &&
    _hasOfficialAnnouncementDecisionSignalForSelection_(s)
  );
}

function _hasPowerDemandProjectTopicSignalForSelection_(text) {
  const s = String(text || "");

  const hasPowerSignal = _hasAny_(s, [
    "電力需要増",
    "電力需要",
    "電力需給",
    "電力不足",
    "電力供給",
    "電力",
    "停電",
    "発電",
    "発電所",
    "送電",
    "配電",
    "変電",
    "変電所",
    "送電網",
    "配電網",
    "電力網",
    "グリッド",
    "水力発電",
    "太陽光発電",
    "天然ガス発電",
    "LNG発電",
    "electricity demand",
    "power demand",
    "power supply",
    "power generation",
    "transmission",
    "distribution",
    "grid",
  ]);

  const hasDemandProjectPlanSignal = _hasAny_(s, [
    "需要増",
    "需要増加",
    "需要に対応",
    "不足に対応",
    "供給拡大",
    "供給増",
    "増強",
    "整備",
    "拡張",
    "新設",
    "建設",
    "改修",
    "計画",
    "プロジェクト",
    "事業計画",
    "実施計画",
    "ロードマップ",
    "入札",
    "開発",
    "投資",
    "rising demand",
    "growing demand",
    "shortage",
    "expansion",
    "upgrade",
    "project",
    "plan",
    "tender",
  ]);

  return hasPowerSignal && hasDemandProjectPlanSignal;
}

function _hasBusinessSmeSignalForSelection_(text) {
  const s = String(text || "");

  const hasStrongSmeSignal = _hasAny_(s, [
    "中小企業",
    "零細企業",
    "小規模事業者",
    "小規模企業",
    "MSME",
    "MSMEs",
    "SME",
    "SMEs",
    "small and medium",
    "micro small and medium",
  ]);

  const hasBusinessSignal = _hasAny_(s, [
    "ビジネス環境",
    "ビジネス",
    "商工業者",
    "商工会議所",
    "企業支援",
    "事業支援",
    "事業者支援",
    "企業活動",
    "民間企業",
    "起業",
    "スタートアップ",
    "business environment",
    "chamber of commerce",
    "private sector",
    "businesses",
  ]);

  const hasPolicyOrEconomicContext = _hasAny_(s, [
    "支援",
    "融資",
    "資金繰り",
    "資金",
    "信用保証",
    "補助金",
    "税",
    "関税",
    "規制",
    "制度",
    "許可",
    "認可",
    "登録",
    "投資",
    "輸出",
    "輸入",
    "貿易",
    "商談",
    "市場",
    "経済",
    "雇用",
    "産業",
    "振興",
    "育成",
    "競争力",
    "会議",
    "協議",
    "説明会",
    "フォーラム",
    "商工",
    "policy",
    "support",
    "loan",
    "credit",
    "tax",
    "regulation",
    "investment",
    "trade",
    "market",
    "employment",
  ]);

  if (!hasStrongSmeSignal && !hasBusinessSignal) return false;

  const looksLikePrOrCharityOnly =
    _hasAny_(s, [
      "寄付",
      "慈善",
      "誕生日",
      "俳優",
      "女優",
      "芸能",
      "映画",
      "歌手",
      "charity",
      "donation",
      "birthday",
      "actress",
      "actor",
    ]) &&
    !hasStrongSmeSignal &&
    !hasPolicyOrEconomicContext;

  if (looksLikePrOrCharityOnly) return false;

  return (
    hasStrongSmeSignal || (hasBusinessSignal && hasPolicyOrEconomicContext)
  );
}

function _hasDevelopmentInfrastructureAuthorityAnnouncementSignalForSelection_(
  text,
) {
  const s = String(text || "");
  if (!_hasDevelopmentInfrastructureTopicSignalForSelection_(s)) return false;
  return (
    _hasMyanmarGovernmentAuthoritySignalForSelection_(s) &&
    _hasOfficialAnnouncementDecisionSignalForSelection_(s)
  );
}

function _hasDevelopmentInfrastructureTopicSignalForSelection_(text) {
  return _hasAny_(text, [
    "国家開発計画",
    "開発計画",
    "都市開発",
    "工業団地",
    "インフラ整備",
    "地域開発",
    "経済特区",
    "公共事業",
    "道路",
    "橋",
    "鉄道",
    "電力",
    "発電",
    "送電",
    "配電",
    "変電所",
    "港湾",
    "空港",
  ]);
}

function _hasMyanmarGovernmentAuthoritySignalForSelection_(text) {
  return _hasAny_(text, [
    "ミャンマー政府",
    "政府",
    "当局",
    "省",
    "省庁",
    "委員会",
    "局",
    "庁",
    "大臣",
    "副大臣",
    "長官",
    "管区政府",
    "州政府",
    "地方政府",
    "市開発委員会",
    "ネピドー評議会",
    "YCDC",
    "MCDC",
    "MIC",
    "DICA",
    "建設省",
    "運輸・通信省",
    "電力省",
    "工業省",
    "投資・対外経済関係省",
    "ミャンマー港湾局",
    "港湾局",
    "Myanmar Port Authority",
    "MPA",
    "国営紙",
    "国営メディア",
    "軍事政権",
    "SAC",
    "連邦議会",
    "人民代表院",
    "民族代表院",
    "議会",
    "Pyidaungsu Hluttaw",
    "Hluttaw",
    "လွှတ်တော်",
  ]);
}

function _hasOfficialAnnouncementDecisionSignalForSelection_(text) {
  return _hasAny_(text, [
    "発表",
    "公表",
    "声明",
    "通達",
    "告示",
    "公告",
    "入札公告",
    "明らかに",
    "述べ",
    "説明",
    "承認",
    "提出",
    "上程",
    "審議",
    "審議入り",
    "可決",
    "成立",
    "採択",
    "許可",
    "認可",
    "決定",
    "指示",
    "命令",
    "開始",
    "着工",
    "開通",
    "開業",
    "再開",
    "共有",
    "予定",
    "スケジュール",
    "停止",
    "廃止",
    "実施",
    "導入",
    "署名",
    "合意",
    "会合",
    "協議",
    "式典",
    "視察",
  ]);
}

function _assignDailySelectionRecommendations_(sheet) {
  // 現在のExcel列構成では、同日同一内容判定に使えるキーは
  // T列ではなくAC列（Gemini詳細JSON）内の same_day_event_key です。
  // そのため、この後処理ではR〜Y列を更新せず、AA列・AD列だけを更新します。
  _applySameDayDuplicateGroupsFromGeminiDetailJson_(sheet);
}

/**
 * prod用: AC列のGemini詳細JSONを使って、AA列・AD列だけを更新する。
 * 既存スコア処理とは別トリガーで動かす場合はこの関数を設定する。
 */
function runSameDayDuplicateGroupingProd() {
  _runSameDayDuplicateGroupingBySheetName_("prod");
}

/**
 * dev用: AC列のGemini詳細JSONを使って、AA列・AD列だけを更新する。
 * 既存スコア処理とは別トリガーで動かす場合はこの関数を設定する。
 */
function runSameDayDuplicateGroupingDev() {
  _runSameDayDuplicateGroupingBySheetName_("dev");
}

/**
 * prod/devまとめて、AC列のGemini詳細JSONを使ってAA列・AD列だけを更新する。
 */
function runSameDayDuplicateGroupingBatch() {
  ["prod", "dev"].forEach(function (sheetName) {
    _runSameDayDuplicateGroupingBySheetName_(sheetName);
  });
}

function _runSameDayDuplicateGroupingBySheetName_(sheetName) {
  const lock = LockService.getDocumentLock();

  try {
    if (!lock.tryLock(5000)) {
      Logger.log("[same-day-duplicate] lock busy -> skip");
      return;
    }

    const ss = SpreadsheetApp.getActive();
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) return;

    const count = _applySameDayDuplicateGroupsFromGeminiDetailJson_(sheet);
    Logger.log("[same-day-duplicate] %s duplicateGroups=%s", sheetName, count);
  } catch (e) {
    Logger.log("[same-day-duplicate] error: " + e);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/**
 * 現在のExcel列構成に合わせた同日同一内容判定。
 *
 * 入力:
 * - A列: 日付
 * - C列: メディア
 * - AA列: 最終スコア
 * - AC列: Gemini詳細JSON
 *
 * 出力:
 * - AA列: 同じADグループ内の最大スコアに上書き
 * - AD列: 重複001、重複002... のグループID
 *
 * 仕様:
 * - T列はMLモデルバージョンなので使わない。
 * - Q列一致は起きない前提のため、同日同一内容判定には使わない。
 * - AD列は毎回全行クリアしてから再生成する。
 * - AC列の same_day_event_key が同じ同日記事を同一内容グループとして扱う。
 */
function _applySameDayDuplicateGroupsFromGeminiDetailJson_(sheet) {
  if (!sheet) return 0;

  _ensureSameDayDuplicateGroupHeader_(sheet);

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return 0;

  const rowCount = lastRow - 1;
  const lastCol = Math.max(
    SELECT_COL_AD_DUPLICATE_GROUP,
    sheet.getLastColumn(),
  );
  const values = sheet.getRange(2, 1, rowCount, lastCol).getValues();

  const finalScoreOutputs = values.map(function (row) {
    return [row[SELECT_COL_AA_FINAL_SCORE - 1]];
  });

  // 1日複数回動かす前提のため、AD列は毎回作り直す。
  const duplicateGroupOutputs = values.map(function () {
    return [""];
  });

  const groups = {};

  values.forEach(function (row, i) {
    const dateKey = _selectionDateKey_(row[0]); // A
    const media = String(row[2] || "").trim(); // C
    if (!dateKey || !media) return;
    if (media === "(Businessプラン限定)") return;

    const detail = _parseGeminiDetailJsonForSameDayDuplicate_(
      row[SELECT_COL_AC_GEMINI_DETAIL_JSON - 1],
    );
    if (!detail) return;

    const eventKeyRaw = _sameDayEventKeyFromGeminiDetail_(detail);
    const eventKey = _normalizeSameDayEventKeyForDuplicateGroup_(eventKeyRaw);
    if (_looksLikeWeakSameDayEventKeyForDuplicateGroup_(eventKey)) return;

    if (!_isSameDayDuplicateCandidateFromGeminiDetail_(detail)) return;

    const groupKey = dateKey + "||" + eventKey;
    if (!groups[groupKey]) groups[groupKey] = [];

    groups[groupKey].push({
      index: i,
      rowIndex: i + 2,
      dateKey: dateKey,
      eventKey: eventKey,
      score: _scoreNumberForSameDayDuplicate_(
        row[SELECT_COL_AA_FINAL_SCORE - 1],
      ),
    });
  });

  const duplicateGroups = Object.keys(groups)
    .map(function (key) {
      const items = groups[key] || [];
      if (items.length <= 1) return null;

      let maxScore = null;
      items.forEach(function (item) {
        if (item.score === null || item.score === undefined) return;
        if (maxScore === null || item.score > maxScore) maxScore = item.score;
      });

      if (maxScore === null) return null;

      const firstRowIndex = Math.min.apply(
        null,
        items.map(function (item) {
          return item.rowIndex;
        }),
      );

      return {
        key: key,
        items: items,
        maxScore: maxScore,
        firstRowIndex: firstRowIndex,
      };
    })
    .filter(function (x) {
      return !!x;
    })
    .sort(function (a, b) {
      return a.firstRowIndex - b.firstRowIndex;
    });

  duplicateGroups.forEach(function (group, groupIndex) {
    const groupId =
      SELECT_DUPLICATE_GROUP_PREFIX +
      _zeroPadSelectionNumber_(groupIndex + 1, 3);

    group.items.forEach(function (item) {
      finalScoreOutputs[item.index] = [group.maxScore];
      duplicateGroupOutputs[item.index] = [groupId];
    });
  });

  sheet
    .getRange(2, SELECT_COL_AA_FINAL_SCORE, rowCount, 1)
    .setValues(finalScoreOutputs);
  sheet
    .getRange(2, SELECT_COL_AD_DUPLICATE_GROUP, rowCount, 1)
    .setValues(duplicateGroupOutputs);

  return duplicateGroups.length;
}

function _parseGeminiDetailJsonForSameDayDuplicate_(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "object") return value;

  let text = String(value || "").trim();
  if (!text) return null;

  text = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(text);
  } catch (e1) {}

  const jsonText = _extractFirstBalancedSelectionJsonObject_(text);
  if (!jsonText) return null;

  try {
    return JSON.parse(jsonText);
  } catch (e2) {
    return null;
  }
}

function _sameDayEventKeyFromGeminiDetail_(detail) {
  detail = detail || {};

  const directCandidates = [
    detail.same_day_event_key,
    detail.sameDayEventKey,
    detail.same_day_topic_key,
    detail.sameDayTopicKey,
    detail.sameDayTopicKeyForSelection,
    detail.same_day_key,
    detail.event_key,
    detail.eventKey,
  ];

  for (let i = 0; i < directCandidates.length; i++) {
    const v = String(directCandidates[i] || "").trim();
    if (v) return v;
  }

  const nestedCandidates = [
    detail.same_day_event,
    detail.sameDayEvent,
    detail.same_day,
    detail.sameDay,
    detail.duplicate,
    detail.same_day_duplicate,
  ];

  for (let j = 0; j < nestedCandidates.length; j++) {
    const obj = nestedCandidates[j];
    if (!obj || typeof obj !== "object") continue;

    const v = String(
      obj.same_day_event_key ||
        obj.sameDayEventKey ||
        obj.same_day_topic_key ||
        obj.sameDayTopicKey ||
        obj.event_key ||
        obj.eventKey ||
        obj.key ||
        "",
    ).trim();

    if (v) return v;
  }

  return "";
}

function _sameDayEventRelationFromGeminiDetail_(detail) {
  detail = detail || {};

  const directCandidates = [
    detail.same_day_event_relation,
    detail.sameDayEventRelation,
    detail.same_day_relation,
    detail.sameDayRelation,
    detail.duplicate_relation,
    detail.duplicateRelation,
  ];

  for (let i = 0; i < directCandidates.length; i++) {
    const v = String(directCandidates[i] || "").trim();
    if (v) return v;
  }

  const nestedCandidates = [
    detail.same_day_event,
    detail.sameDayEvent,
    detail.same_day,
    detail.sameDay,
    detail.duplicate,
    detail.same_day_duplicate,
  ];

  for (let j = 0; j < nestedCandidates.length; j++) {
    const obj = nestedCandidates[j];
    if (!obj || typeof obj !== "object") continue;

    const v = String(
      obj.same_day_event_relation ||
        obj.sameDayEventRelation ||
        obj.relation ||
        obj.type ||
        "",
    ).trim();

    if (v) return v;
  }

  return "";
}

function _isSameDayDuplicateCandidateFromGeminiDetail_(detail) {
  detail = detail || {};

  const boolCandidates = [
    detail.same_day_duplicate_candidate,
    detail.sameDayDuplicateCandidate,
    detail.is_same_day_duplicate,
    detail.isSameDayDuplicate,
  ];

  for (let i = 0; i < boolCandidates.length; i++) {
    if (boolCandidates[i] === true) return true;
  }

  const relation = _sameDayEventRelationFromGeminiDetail_(detail);
  const s = String(relation || "")
    .trim()
    .toLowerCase();

  if (!s) {
    // relationがない場合でも、same_day_event_keyが同じ行が複数あれば重複扱いできるよう候補に残す。
    return true;
  }

  const positive = [
    "same_event_duplicate",
    "duplicate_same_content",
    "same_day_duplicate",
    "same_event",
    "same_content",
    "duplicate",
    "duplicated",
  ];

  for (let j = 0; j < positive.length; j++) {
    if (s.indexOf(positive[j]) !== -1) return true;
  }

  const negative = [
    "different_event",
    "related_but_different",
    "different_angle",
    "unrelated",
    "not_duplicate",
    "no_duplicate",
    "no_same_event",
    "unique",
    "none",
  ];

  for (let k = 0; k < negative.length; k++) {
    if (s === negative[k] || s.indexOf(negative[k]) !== -1) return false;
  }

  // 未知のrelationでも、同じevent_keyが複数行で一致する場合は拾えるよう候補に残す。
  return true;
}

function _normalizeSameDayEventKeyForDuplicateGroup_(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/[\s　]+/g, " ")
    .trim()
    .toLowerCase();
}

function _looksLikeWeakSameDayEventKeyForDuplicateGroup_(key) {
  const s = String(key || "")
    .trim()
    .toLowerCase();
  if (!s) return true;
  if (s.length < 6) return true;

  const weak = {
    none: true,
    null: true,
    undefined: true,
    unknown: true,
    other: true,
    misc: true,
    topic: true,
    news: true,
    article: true,
    update: true,
    general: true,
    一般: true,
    その他: true,
    不明: true,
  };

  if (weak[s]) return true;
  if (/^\d+$/.test(s)) return true;
  if (/^\d{8}\|?$/.test(s)) return true;

  return false;
}

function _scoreNumberForSameDayDuplicate_(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(String(value).replace(/,/g, "").trim());
  if (isNaN(n)) return null;
  return n;
}

function _zeroPadSelectionNumber_(value, width) {
  const s = String(Math.max(0, Number(value || 0)));
  const w = Number(width || 0);
  if (!w || s.length >= w) return s;
  return new Array(w - s.length + 1).join("0") + s;
}

function _finalAdoptionProbabilityScore_(item) {
  item = item || {};

  const baseScore = Number(item.baseScore || item.score || 0);
  let score = baseScore;

  // R列の最終スコア計算直前でも、E/F/G/I列からミャンマー直接関連を再確認する。
  // 基礎スコア側で誤って高くなっていても、ここで20点以下へ抑える。
  const rowText = item.row ? _selectionInputTextFromSheetRow_(item.row) : "";

  if (item.row && !_hasDirectMyanmarRelevanceForSelection_(rowText)) {
    score = Math.min(score, SELECT_NON_MYANMAR_FINAL_SCORE_CAP);
  }

  if (
    item.row &&
    _isConflictWithoutCivilianDeathExclusionForSelection_(rowText)
  ) {
    score = Math.min(score, SELECT_CONFLICT_WITHOUT_CIVILIAN_DEATH_SCORE_CAP);
  }

  return _clampRound_(score, 0, 100);
}

function _selectionInputTextFromSheetRow_(row) {
  row = row || [];
  return [
    row[4], // E
    row[5], // F
    row[6], // G
    row[8], // I
  ].join("\n");
}

function _enforceNoDirectMyanmarFinalScoreCapForDate_(
  items,
  finalScoreOutputs,
  rationaleOutputs,
) {
  (items || []).forEach(function (item) {
    if (!item || !item.row) return;

    const text = _selectionInputTextFromSheetRow_(item.row);
    if (_hasDirectMyanmarRelevanceForSelection_(text)) return;

    const current = Number(
      (finalScoreOutputs[item.index] && finalScoreOutputs[item.index][0]) || 0,
    );
    const capped = _clampRound_(
      Math.min(current, SELECT_NON_MYANMAR_FINAL_SCORE_CAP),
      0,
      100,
    );

    finalScoreOutputs[item.index] = [capped];
    rationaleOutputs[item.index] = [
      _prependSelectionNote_(
        String(
          (rationaleOutputs[item.index] && rationaleOutputs[item.index][0]) ||
            "",
        ),
        "ミャンマー直接関連なしのためR列を20点以下に強制。",
      ),
    ];
  });
}

function _enforceConflictWithoutCivilianDeathFinalScoreCapForDate_(
  items,
  finalScoreOutputs,
  rationaleOutputs,
) {
  (items || []).forEach(function (item) {
    if (!item || !item.row) return;

    const text = _selectionInputTextFromSheetRow_(item.row);
    if (!_isConflictWithoutCivilianDeathExclusionForSelection_(text)) return;

    const current = Number(
      (finalScoreOutputs[item.index] && finalScoreOutputs[item.index][0]) || 0,
    );
    const capped = _clampRound_(
      Math.min(current, SELECT_CONFLICT_WITHOUT_CIVILIAN_DEATH_SCORE_CAP),
      0,
      100,
    );

    finalScoreOutputs[item.index] = [capped];
    rationaleOutputs[item.index] = [
      _prependSelectionNote_(
        String(
          (rationaleOutputs[item.index] && rationaleOutputs[item.index][0]) ||
            "",
        ),
        "空爆・衝突等で市民等の死亡被害が確認できないためR列を34点以下に強制。",
      ),
    ];
  });
}

function _equalizeSameDayTopicFinalScoresForDate_(
  items,
  finalScoreOutputs,
  rationaleOutputs,
) {
  const groups = {};

  (items || []).forEach(function (item) {
    const topicKey = item && item.topicKey;
    if (!topicKey) return;
    if (_looksLikeWeakSelectionTopicKey_(topicKey)) return;

    if (!groups[topicKey]) groups[topicKey] = [];
    groups[topicKey].push(item);
  });

  Object.keys(groups).forEach(function (topicKey) {
    const group = groups[topicKey];
    if (!group || group.length <= 1) return;

    const groupScore = Math.max.apply(
      null,
      group.map(function (item) {
        const v =
          finalScoreOutputs[item.index] && finalScoreOutputs[item.index][0];
        return Number(v || 0);
      }),
    );

    group.forEach(function (item) {
      finalScoreOutputs[item.index] = [groupScore];
      rationaleOutputs[item.index] = [
        _prependSelectionNote_(
          String(
            (rationaleOutputs[item.index] && rationaleOutputs[item.index][0]) ||
              "",
          ),
          "同一出来事グループ（いつ・誰が・どこで・何を一致・R列同点）。",
        ),
      ];
    });
  });
}

function _equalizeExactDuplicateFinalScoresForDate_(
  items,
  finalScoreOutputs,
  rationaleOutputs,
) {
  const groups = {};

  (items || []).forEach(function (item) {
    const duplicateKey = item && item.duplicateKey;
    if (!duplicateKey) return;

    if (!groups[duplicateKey]) groups[duplicateKey] = [];
    groups[duplicateKey].push(item);
  });

  Object.keys(groups).forEach(function (duplicateKey) {
    const group = groups[duplicateKey];
    if (!group || group.length <= 1) return;

    const groupScore = Math.max.apply(
      null,
      group.map(function (item) {
        const v =
          finalScoreOutputs[item.index] && finalScoreOutputs[item.index][0];
        return Number(v || 0);
      }),
    );

    group.forEach(function (item) {
      finalScoreOutputs[item.index] = [groupScore];
      rationaleOutputs[item.index] = [
        _prependSelectionNote_(
          String(
            (rationaleOutputs[item.index] && rationaleOutputs[item.index][0]) ||
              "",
          ),
          "同日重複あり（同一内容グループ・R列同点）。",
        ),
      ];
    });
  });
}

function _refreshFinalScoreFlagsForOrderedItems_(
  ordered,
  finalScoreOutputs,
  flagOutputs,
) {
  (ordered || []).forEach(function (item) {
    const finalScore = Number(
      (finalScoreOutputs[item.index] && finalScoreOutputs[item.index][0]) || 0,
    );
    flagOutputs[item.index] = [
      _selectionRecommendationFlagFromFinalScore_(finalScore, item),
    ];
  });
}

function _selectionRecommendationFlagFromFinalScore_(finalScore, item) {
  if (finalScore >= 75) return "TOP_CANDIDATE";
  if (finalScore >= 60) return "CANDIDATE";
  if (finalScore >= 45) return "BACKUP";
  if (finalScore < 35) return "LOW";
  return "RANKED";
}

function _isSelectionOkStatusForRanking_(status) {
  const s = String(status || "").trim();
  return (
    s === "OK" ||
    s === "OK(SAME_DAY_RANKED)" ||
    s === "OK(DAILY_RANKED)" ||
    s.indexOf("OK(") === 0
  );
}

function _prependSelectionNote_(value, note) {
  const v = String(value || "").trim();
  const n = String(note || "").trim();
  if (!n) return v.slice(0, 220);
  if (v.indexOf(n) === 0) return v.slice(0, 220);
  return (n + (v ? " " + v : "")).slice(0, 220);
}

function _appendSelectionNote_(value, note) {
  const v = String(value || "").trim();
  const n = String(note || "").trim();
  if (!n) return v.slice(0, 260);
  if (v.indexOf(n) !== -1) return v.slice(0, 260);
  return ((v ? v + " " : "") + n).slice(0, 260);
}

/************************************************************
 * v4 cleanup overrides: keep only criteria-required keyword logic
 ************************************************************/
function _strategicMeaningScoreForSelection_(text) {
  const s = String(text || "");
  const conflictOrPower = _hasAny_(s, [
    "軍",
    "国軍",
    "抵抗勢力",
    "PDF",
    "民族武装勢力",
    "戦闘",
    "攻撃",
    "空爆",
    "砲撃",
    "衝突",
    "制圧",
    "占拠",
    "撤退",
    "選挙",
    "停戦",
    "同盟",
  ]);
  const strategic = _hasAny_(s, [
    "戦略",
    "戦略的",
    "補給路",
    "主要都市",
    "主要道路",
    "国境",
    "越境",
    "港湾",
    "港",
    "空港",
    "経済回廊",
    "回廊",
    "支配地域",
    "行政支配",
    "拠点",
    "基地",
    "選挙区",
    "同盟関係",
    "停戦",
    "物流",
    "経済特区",
    "中国",
    "タイ",
    "インド",
  ]);
  if (conflictOrPower && strategic) return 8;
  if (
    strategic &&
    _hasAny_(s, ["大規模", "多数", "数千", "万人", "国際", "ASEAN", "国連"])
  )
    return 7;
  return 0;
}
