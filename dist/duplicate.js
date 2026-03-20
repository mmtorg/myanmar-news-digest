/**
 * 同一トピック判定処理 & アーカイブ管理
 *
 * 処理フロー:
 * 1. updateArchiveProd() / updateArchiveDev()
 *    - 対象シートの当日分を archive_* シートに追記し、
 *      archive_* を「今日を含む過去 ARCHIVE_DAYS 日分」に整理する（日次トリガー）
 *
 * 2. checkDuplicateTopicsProd() / checkDuplicateTopicsDev()
 *    - O列が空、かつ必要列が埋まっている行だけを対象に、
 *      Gemini で同一トピック数を O列へ、
 *      一致した archive 記事一覧を P列へ書き込む（定期トリガー）
 *
 * 3. 必要なら updateArchive() / checkDuplicateTopics() で prod/dev をまとめて実行可能
 */

// ====================================================
// 定数
// ====================================================

/** アーカイブシート名のプレフィックス */
const ARCHIVE_SHEET_PREFIX = "archive_";

/** O列の列番号（1始まり） */
const COL_O_DUPLICATE_COUNT = 15;

/** P列の列番号（1始まり） */
const COL_P_DUPLICATE_MATCHES = 16;

/** アーカイブに保持する日数（今日を除く過去N日） */
const ARCHIVE_DAYS = 2;

/**
 * 事前フィルタ後、1ターゲットあたり Gemini に渡す候補記事数。
 * まず全アーカイブを機械的にスコアリングし、上位候補のみを Gemini 判定に回す。
 */
const PREFILTER_CANDIDATES_PER_TARGET = 24;

/**
 * 1回のGemini呼び出しで比較するアーカイブ記事の最大件数。
 * アーカイブ側は E/F/G 列のみを渡すため、やや大きめに設定する。
 */
const TOPIC_CHECK_BATCH_SIZE = 24;

/** 1回のGemini呼び出しでまとめて判定するターゲット行数 */
const TOPIC_TARGETS_PER_CALL = 4;

// 列インデックス（0始まり）
const _DIDX_A = 0; // A: 日付
const _DIDX_C = 2; // C: メディア
const _DIDX_E = 4; // E: 見出しA
const _DIDX_F = 5; // F: 日本語見出し（few-shot版）
const _DIDX_G = 6; // G: 見出しB'（本文のみ）
const _DIDX_I = 8; // I: 本文要約
const _DIDX_J = 9; // J: URL
const _DIDX_K = 10; // K: 採用フラグ
const _DIDX_M = 12; // M: 原文タイトル
const _DIDX_N = 13; // N: 原文本文
const _DIDX_O = 14; // O: 同一トピック数（COL_O_DUPLICATE_COUNT - 1）

// ====================================================
// 1. アーカイブ更新
// ====================================================

const TARGET_SHEET_NAMES = ["prod", "dev"];

/**
 * prod シートの当日分を archive_prod に追加し、
 * archive_prod を「今日を除く過去ARCHIVE_DAYS日分」だけ残す。
 */
function updateArchiveProd() {
  _appendAndPruneArchiveBySheetName_("prod");
}

/**
 * dev シートの当日分を archive_dev に追加し、
 * archive_dev を「今日を除く過去ARCHIVE_DAYS日分」だけ残す。
 */
function updateArchiveDev() {
  _appendAndPruneArchiveBySheetName_("dev");
}

/**
 * prod / dev をまとめて更新する。
 */
function updateArchive() {
  TARGET_SHEET_NAMES.forEach(function (sheetName) {
    _appendAndPruneArchiveBySheetName_(sheetName);
  });
}

/**
 * 対象シート1つ分の archive_* を更新する。
 *
 * 処理:
 * 1. 元シートのデータを archive に重複なしで追加
 * 2. archive から保持期限外（今日を除く過去ARCHIVE_DAYS日分より前 / 今日以降）の行を削除
 *
 * @param {string} sheetName
 */
function _appendAndPruneArchiveBySheetName_(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tz = Session.getScriptTimeZone();

  const srcSheet = ss.getSheetByName(sheetName);
  if (!srcSheet) {
    Logger.log("[updateArchive] sheet not found: " + sheetName);
    return;
  }

  const archiveName = ARCHIVE_SHEET_PREFIX + sheetName;
  let archiveSheet = ss.getSheetByName(archiveName);
  if (!archiveSheet) {
    archiveSheet = ss.insertSheet(archiveName);
    Logger.log("[updateArchive] created archive sheet: " + archiveName);
  }

  _ensureArchiveHeader_(srcSheet, archiveSheet);
  const appendResult = _appendRowsToArchive_(srcSheet, archiveSheet);
  const pruneResult = _pruneArchiveSheet_(archiveSheet, tz);

  Logger.log(
    "[updateArchive] completed: %s appended=%s skipped=%s pruned=%s remaining=%s",
    sheetName,
    appendResult.appended,
    appendResult.skipped,
    pruneResult.deleted,
    pruneResult.remaining,
  );
}

/**
 * archive シートにヘッダーが無い場合、srcSheet のヘッダーをコピーする。
 * 既にヘッダーがある場合は何もしない。
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} srcSheet
 * @param {GoogleAppsScript.Spreadsheet.Sheet} archiveSheet
 */
function _ensureArchiveHeader_(srcSheet, archiveSheet) {
  const srcLastCol = srcSheet.getLastColumn();
  if (srcLastCol < 1) return;

  const srcHeader = srcSheet.getRange(1, 1, 1, srcLastCol).getValues()[0];
  const archiveLastRow = archiveSheet.getLastRow();
  const archiveLastCol = archiveSheet.getLastColumn();

  if (archiveLastRow === 0) {
    archiveSheet.getRange(1, 1, 1, srcHeader.length).setValues([srcHeader]);
    return;
  }

  const archiveHeader = archiveSheet
    .getRange(1, 1, 1, Math.max(srcHeader.length, archiveLastCol || 1))
    .getValues()[0];

  const isHeaderEmpty = archiveHeader.every(function (v) {
    return String(v || "").trim() === "";
  });

  if (isHeaderEmpty) {
    archiveSheet.getRange(1, 1, 1, srcHeader.length).setValues([srcHeader]);
  }
}

/**
 * srcSheet のデータ行を archiveSheet に追加する。
 *
 * 追加しない条件:
 * - archive 追加対象の最低条件を満たさない
 * - C列が "(Businessプラン限定)" の行
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} srcSheet
 * @param {GoogleAppsScript.Spreadsheet.Sheet} archiveSheet
 * @returns {{appended:number, skipped:number}}
 */
function _appendRowsToArchive_(srcSheet, archiveSheet) {
  const srcValues = srcSheet.getDataRange().getValues();
  if (srcValues.length < 2) {
    return { appended: 0, skipped: 0 };
  }

  const header = srcValues[0];
  const srcRows = srcValues.slice(1);

  const rowsToAppend = [];
  let skipped = 0;

  srcRows.forEach(function (row) {
    if (!_isArchivableRow_(row)) {
      skipped += 1;
      return;
    }

    const normalizedRow = row.slice(0, header.length);
    while (normalizedRow.length < header.length) {
      normalizedRow.push("");
    }

    rowsToAppend.push(normalizedRow);
  });

  if (rowsToAppend.length > 0) {
    archiveSheet
      .getRange(
        archiveSheet.getLastRow() + 1,
        1,
        rowsToAppend.length,
        header.length,
      )
      .setValues(rowsToAppend);
  }

  return {
    appended: rowsToAppend.length,
    skipped: skipped,
  };
}

/**
 * archiveSheet から、保持期間外の行を削除する。
 *
 * 残す条件:
 *   cutoffStr <= A列の日付
 *
 * つまり
 * - 今日を含む
 * - 今日から ARCHIVE_DAYS 日前以上
 *
 * 例:
 *   ARCHIVE_DAYS = 2 の場合
 *   「今日・昨日・一昨日」を残す
 *
 * ※ ファイル先頭の概要コメントも、この実装に合わせている。
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} archiveSheet
 * @returns {{deleted:number, remaining:number}}
 */
function _pruneArchiveSheet_(archiveSheet, tz) {
  const values = archiveSheet.getDataRange().getValues();
  if (values.length < 2) {
    return { deleted: 0, remaining: 0 };
  }

  const header = values[0];
  const dataRows = values.slice(1);

  const now = new Date();
  const cutoffMs = now.getTime() - ARCHIVE_DAYS * 24 * 60 * 60 * 1000;
  const cutoffStr = Utilities.formatDate(new Date(cutoffMs), tz, "yyyyMMdd");

  const keptRows = dataRows.filter(function (row) {
    const dateVal = row[_DIDX_A];
    if (!(dateVal instanceof Date)) return false;

    const dateStr = Utilities.formatDate(dateVal, tz, "yyyyMMdd");
    return dateStr >= cutoffStr;
  });

  const deleted = dataRows.length - keptRows.length;

  archiveSheet.clearContents();
  archiveSheet.getRange(1, 1, 1, header.length).setValues([header]);

  if (keptRows.length > 0) {
    archiveSheet
      .getRange(2, 1, keptRows.length, header.length)
      .setValues(keptRows);
  }

  return {
    deleted: deleted,
    remaining: keptRows.length,
  };
}

/**
 * archive 追加対象の最低条件。
 *
 * 条件:
 * - A列に日付がある
 * - C列にメディアがある
 * - C列が "(Businessプラン限定)" ではない
 * - E/F/G のいずれかに値がある
 *
 * @param {any[]} row
 * @returns {boolean}
 */
function _isArchivableRow_(row) {
  const dateVal = row[_DIDX_A];
  if (!(dateVal instanceof Date)) return false;

  const media = String(row[_DIDX_C] || "").trim();
  if (!media) return false;
  if (media === "(Businessプラン限定)") return false;

  const e = String(row[_DIDX_E] || "").trim();
  const f = String(row[_DIDX_F] || "").trim();
  const g = String(row[_DIDX_G] || "").trim();
  if (!e && !f && !g) return false;

  return true;
}

// ====================================================
// 2. 同一トピック判定
// ====================================================

/**
 * prod シートの判定対象行に同一トピック数を O列へ書き込む。
 */
function checkDuplicateTopicsProd() {
  _checkDuplicateTopicsBySheetName_("prod");
}

/**
 * dev シートの判定対象行に同一トピック数を O列へ書き込む。
 */
function checkDuplicateTopicsDev() {
  _checkDuplicateTopicsBySheetName_("dev");
}

/**
 * prod / dev をまとめて判定する。
 */
function checkDuplicateTopics() {
  TARGET_SHEET_NAMES.forEach(function (sheetName) {
    _checkDuplicateTopicsBySheetName_(sheetName);
  });
}

/**
 * 対象シート1つ分の同一トピック判定を実行する。
 *
 * @param {string} sheetName
 */
function _checkDuplicateTopicsBySheetName_(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    Logger.log("[checkDuplicateTopics] sheet not found: " + sheetName);
    return;
  }

  const archiveName = ARCHIVE_SHEET_PREFIX + sheetName;
  const archiveSheet = ss.getSheetByName(archiveName);
  if (!archiveSheet) {
    Logger.log(
      "[checkDuplicateTopics] archive not found: " +
        archiveName +
        ". Run updateArchive first.",
    );
    return;
  }

  _processTopicCheckForSheet_(sheet, archiveSheet, sheetName);
}

/**
 * archive 側の同一トピック判定候補行を選別する。
 *
 * ルール:
 * - E/F/G のいずれかに値がある行だけを対象にする
 * - J列(URL)が空の行はそのまま残す
 * - J列(URL)が重複していない行はそのまま残す
 * - J列(URL)が重複している場合は、K列(採用フラグ)='a' の行だけ残す
 *
 * @param {any[][]} rows archive シートのデータ行（ヘッダー除く）
 * @returns {{row:any[], sheetRowIndex:number}[]}
 */
function _selectArchiveRowsForDuplicateCheck_(rows) {
  if (!rows || rows.length === 0) return [];

  const rowsByUrl = {};
  const rowsWithoutUrl = [];

  rows.forEach(function (row, idx) {
    const e = String(row[_DIDX_E] || "").trim();
    const f = String(row[_DIDX_F] || "").trim();
    const g = String(row[_DIDX_G] || "").trim();
    if (!e && !f && !g) return;

    const item = {
      row: row,
      sheetRowIndex: idx + 2,
    };

    const url = String(row[_DIDX_J] || "").trim();
    if (!url) {
      rowsWithoutUrl.push(item);
      return;
    }

    if (!rowsByUrl[url]) rowsByUrl[url] = [];
    rowsByUrl[url].push(item);
  });

  const selected = rowsWithoutUrl.slice();

  Object.keys(rowsByUrl).forEach(function (url) {
    const group = rowsByUrl[url];

    if (group.length === 1) {
      selected.push(group[0]);
      return;
    }

    const adopted = group.filter(function (item) {
      return (
        String(item.row[_DIDX_K] || "")
          .trim()
          .toLowerCase() === "a"
      );
    });

    adopted.forEach(function (item) {
      selected.push(item);
    });
  });

  return selected;
}

/**
 * 1シート分の同一トピック判定を実行する。
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet  prod または dev シート
 * @param {GoogleAppsScript.Spreadsheet.Sheet} archiveSheet  対応するアーカイブシート
 * @param {string} sheetName  "prod" または "dev"
 */
function _processTopicCheckForSheet_(sheet, archiveSheet, sheetName) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const values = sheet.getDataRange().getValues();
  const dataRows = values.slice(1);

  const archiveValues = archiveSheet.getDataRange().getValues();
  const archiveRows =
    archiveValues.length > 1
      ? _selectArchiveRowsForDuplicateCheck_(archiveValues.slice(1))
      : [];

  const archiveArticles = archiveRows.map(function (item, idx) {
    const row = item.row;
    const e = String(row[_DIDX_E] || "").trim();
    const f = String(row[_DIDX_F] || "").trim();
    const g = String(row[_DIDX_G] || "").trim();

    return {
      archiveIndex: idx + 1,
      sheetRowIndex: item.sheetRowIndex,
      e: e,
      f: f,
      g: g,
      prefilter: _buildTopicPrefilterFeatures_([e, f, g].join("\n")),
    };
  });

  const archiveArticleMapById = {};
  archiveArticles.forEach(function (article) {
    archiveArticleMapById[String(article.archiveIndex)] = article;
  });

  const targets = [];
  for (let idx = 0; idx < dataRows.length; idx++) {
    const row = dataRows[idx];
    if (_isDuplicateCheckTarget_(row)) {
      const media = String(row[_DIDX_C] || "").trim();
      const e = String(row[_DIDX_E] || "").trim();
      const f = String(row[_DIDX_F] || "").trim();
      const g = String(row[_DIDX_G] || "").trim();

      targets.push({
        rowIndex: idx + 2,
        media: media,
        e: e,
        f: f,
        g: g,
        prefilter: _buildTopicPrefilterFeatures_([e, f, g].join("\n")),
      });
    }
  }

  if (targets.length === 0) {
    Logger.log("[checkDuplicateTopics] no targets in: " + sheetName);
    return;
  }

  Logger.log(
    "[checkDuplicateTopics] %s targets, %s archive rows in %s",
    targets.length,
    archiveArticles.length,
    sheetName,
  );

  if (archiveArticles.length === 0) {
    for (const t of targets) {
      sheet.getRange(t.rowIndex, COL_O_DUPLICATE_COUNT).setValue(0);
      sheet.getRange(t.rowIndex, COL_P_DUPLICATE_MATCHES).setValue("");
    }
    Logger.log(
      "[checkDuplicateTopics] archive is empty, wrote 0 / blank for all targets.",
    );
    return;
  }

  const pendingByMedia = {};

  for (const target of targets) {
    const media = _normalizeMediaName_(target.media);
    if (!media) {
      Logger.log(
        "[checkDuplicateTopics] row=%s skipped: media(C列) is empty",
        target.rowIndex,
      );
      continue;
    }

    const narrowedCandidates = _selectArchiveCandidatesForTarget_(
      target,
      archiveArticles,
      PREFILTER_CANDIDATES_PER_TARGET,
    );

    if (!pendingByMedia[media]) {
      pendingByMedia[media] = [];
    }

    for (
      let start = 0;
      start < narrowedCandidates.length;
      start += TOPIC_CHECK_BATCH_SIZE
    ) {
      const candidates = narrowedCandidates
        .slice(start, start + TOPIC_CHECK_BATCH_SIZE)
        .map(function (a) {
          return {
            id: String(a.archiveIndex),
            e: a.e,
            f: a.f,
            g: a.g,
          };
        });

      pendingByMedia[media].push({
        target: target,
        candidates: candidates,
      });
    }
  }

  const resultsByRowTotal = {};

  Object.keys(pendingByMedia).forEach(function (media) {
    const apiKey = _getDuplicateGeminiApiKeyByMedia_(
      media,
      sheetName + "#topicCheck#" + media,
    );

    if (!apiKey) {
      Logger.log(
        "[checkDuplicateTopics] no API key: sheet=%s media=%s",
        sheetName,
        media,
      );
      return;
    }

    const pending = pendingByMedia[media];

    for (
      let start = 0;
      start < pending.length;
      start += TOPIC_TARGETS_PER_CALL
    ) {
      const batchItems = pending.slice(start, start + TOPIC_TARGETS_PER_CALL);

      const usageTag =
        sheetName +
        "#topicCheck#" +
        media +
        ":rows" +
        batchItems
          .map(function (it) {
            return it.target.rowIndex;
          })
          .join("-");

      const batchResults = _countSameTopicArticlesBatch_(
        batchItems,
        apiKey,
        usageTag,
      );

      for (const item of batchItems) {
        const rowIndex = item.target.rowIndex;
        const r = batchResults[rowIndex] || { count: 0, matched: [] };

        if (!resultsByRowTotal[rowIndex]) {
          resultsByRowTotal[rowIndex] = { count: 0, matched: [] };
        }

        resultsByRowTotal[rowIndex].count += r.count;
        resultsByRowTotal[rowIndex].matched = resultsByRowTotal[
          rowIndex
        ].matched.concat(r.matched || []);
      }
    }
  });

  const countWriteValues = targets.map(function (target) {
    const rowIndex = target.rowIndex;
    const result = resultsByRowTotal[rowIndex] || { count: 0, matched: [] };

    Logger.log(
      "[checkDuplicateTopics] row=%s media=%s → O列=%s",
      rowIndex,
      target.media,
      result.count,
    );

    return [result.count];
  });

  const matchWriteValues = targets.map(function (target) {
    const rowIndex = target.rowIndex;
    const result = resultsByRowTotal[rowIndex] || { count: 0, matched: [] };

    const uniqueMatched = Array.from(
      new Set((result.matched || []).map(String)),
    );

    const labels = uniqueMatched.map(function (id) {
      const article = archiveArticleMapById[String(id)];
      if (!article) return "候補ID:" + id;

      const title = String(article.f || article.e || article.g || "")
        .replace(/\s+/g, " ")
        .trim();
      const shortTitle =
        title.length > 60 ? title.substring(0, 60) + "…" : title;

      return "archive行" + article.sheetRowIndex + ": " + shortTitle;
    });

    return [labels.join("\n")];
  });

  const rowIndexes = targets.map(function (target) {
    return target.rowIndex;
  });

  const isContiguous =
    rowIndexes.length > 0 &&
    rowIndexes[rowIndexes.length - 1] - rowIndexes[0] + 1 === rowIndexes.length;

  if (isContiguous) {
    sheet
      .getRange(
        rowIndexes[0],
        COL_O_DUPLICATE_COUNT,
        countWriteValues.length,
        1,
      )
      .setValues(countWriteValues);

    sheet
      .getRange(
        rowIndexes[0],
        COL_P_DUPLICATE_MATCHES,
        matchWriteValues.length,
        1,
      )
      .setValues(matchWriteValues);
  } else {
    for (let i = 0; i < targets.length; i++) {
      sheet
        .getRange(targets[i].rowIndex, COL_O_DUPLICATE_COUNT)
        .setValue(countWriteValues[i][0]);

      sheet
        .getRange(targets[i].rowIndex, COL_P_DUPLICATE_MATCHES)
        .setValue(matchWriteValues[i][0]);
    }
  }
}

/**
 * 日本語/英語混在テキストから、事前フィルタ用の特徴量を作る。
 * Gemini に渡す前の粗い候補絞り込み専用であり、厳密一致は目的としない。
 *
 * @param {string} text
 * @returns {{tokens:string[], tokenSet:Object<string, boolean>, numbers:string[], numberSet:Object<string, boolean>, charGramSet:Object<string, boolean>}}
 */
function _buildTopicPrefilterFeatures_(text) {
  const normalized = _normalizeTopicText_(text);
  const tokens = _tokenizeTopicText_(normalized);

  const tokenSet = {};
  for (const token of tokens) tokenSet[token] = true;

  const numbers = normalized.match(/\b\d{1,6}\b/g) || [];
  const numberSet = {};
  for (const num of numbers) numberSet[num] = true;

  const charBigrams = _buildCharNgrams_(normalized, 2);
  const charTrigrams = _buildCharNgrams_(normalized, 3);
  const charGramSet = {};
  charBigrams.concat(charTrigrams).forEach(function (g) {
    charGramSet[g] = true;
  });

  return {
    tokens: tokens,
    tokenSet: tokenSet,
    numbers: numbers,
    numberSet: numberSet,
    charGramSet: charGramSet,
  };
}

function _buildCharNgrams_(text, n) {
  const s = String(text || "").replace(/[　\s]+/g, "");
  const out = [];
  const seen = {};
  for (let i = 0; i <= s.length - n; i++) {
    const g = s.substring(i, i + n);
    if (!seen[g]) {
      seen[g] = true;
      out.push(g);
    }
  }
  return out;
}

/**
 * 事前フィルタ用にテキストを正規化する。
 *
 * @param {string} text
 * @returns {string}
 */
function _normalizeTopicText_(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[‐-―]/g, "-")
    .replace(/[　\s]+/g, " ")
    .trim();
}

/**
 * 事前フィルタ用の簡易トークナイズ。
 * 日本語・英数字の連続列を抽出し、短すぎる一般語を落とす。
 *
 * @param {string} text
 * @returns {string[]}
 */
function _tokenizeTopicText_(text) {
  if (!text) return [];

  var normalized = _normalizeTopicText_(String(text));

  var stopwords = {
    について: true,
    に対して: true,
    により: true,
    による: true,
    として: true,
    において: true,
    に関して: true,
    に関する: true,
    など: true,
    などの: true,
    こと: true,
    もの: true,
    ため: true,
    さん: true,
    ほか: true,
    ほかの: true,
    一方: true,
    今回: true,
    速報: true,
    記事: true,
    報道: true,
    発表: true,
    表明: true,
    開始: true,
    実施: true,
    判明: true,
    明らか: true,
    する: true,
    した: true,
    して: true,
    され: true,
    された: true,
    なる: true,
    なった: true,
    いる: true,
    いた: true,
    ある: true,
    いう: true,
    受け: true,
    受けて: true,
    めぐり: true,
    the: true,
    and: true,
    for: true,
    with: true,
    from: true,
    this: true,
    that: true,
    report: true,
    article: true,
    news: true,
  };

  var dedup = {};
  var tokens = [];

  function addToken(token) {
    if (!token) return;
    token = token.trim();
    if (!token) return;
    if (stopwords[token]) return;

    if (/^[\p{Script=Hiragana}ー]+$/u.test(token) && token.length <= 3) return;
    if (token.length <= 1) return;
    if (/^\d$/.test(token)) return;

    if (!dedup[token]) {
      dedup[token] = true;
      tokens.push(token);
    }
  }

  function splitJapaneseChunk(chunk) {
    if (!chunk) return [];

    var parts = chunk.split(
      /(?:について|に対して|による|により|として|において|に関する|に関して|など|などの|へ|に|で|と|が|を|は|も|や|から|まで|より|など|ため|こと|もの|そして|また|ただし|一方|受けて|めぐり)+/u,
    );

    var out = [];
    for (var i = 0; i < parts.length; i++) {
      var p = parts[i].trim();
      if (!p) continue;

      var kanjiMatches = p.match(/[\p{Script=Han}]{2,}/gu) || [];
      for (var k = 0; k < kanjiMatches.length; k++) out.push(kanjiMatches[k]);

      var kataMatches = p.match(/[\p{Script=Katakana}ー]{2,}/gu) || [];
      for (var j = 0; j < kataMatches.length; j++) out.push(kataMatches[j]);

      var latinMatches = p.match(/[a-z]{3,}(?:[a-z0-9\-]{0,20})/g) || [];
      for (var l = 0; l < latinMatches.length; l++) out.push(latinMatches[l]);

      var numMatches = p.match(/\d{2,}/g) || [];
      for (var n = 0; n < numMatches.length; n++) out.push(numMatches[n]);

      var mixedMatches =
        p.match(
          /[\p{Script=Han}\p{Script=Katakana}][\p{Script=Han}\p{Script=Katakana}ー]{1,}/gu,
        ) || [];
      for (var m = 0; m < mixedMatches.length; m++) out.push(mixedMatches[m]);
    }
    return out;
  }

  var roughChunks =
    normalized.match(
      /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}ー]{2,}|[a-z0-9\-]{3,}/gu,
    ) || [];

  for (var i = 0; i < roughChunks.length; i++) {
    var chunk = roughChunks[i];

    if (/[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]/u.test(chunk)) {
      var finer = splitJapaneseChunk(chunk);
      for (var f = 0; f < finer.length; f++) {
        addToken(finer[f]);
      }
    } else {
      addToken(chunk);
    }
  }

  return tokens;
}

/**
 * ターゲット記事とアーカイブ候補の近さを簡易スコア化する。
 * 固有語重複を重く、数値一致をさらに重く評価する。
 *
 * @param {{prefilter:{tokens:string[], tokenSet:Object<string, boolean>, numbers:string[], numberSet:Object<string, boolean>}}} target
 * @param {{prefilter:{tokens:string[], tokenSet:Object<string, boolean>, numbers:string[], numberSet:Object<string, boolean>}}} archiveArticle
 * @returns {number}
 */
function _scoreTopicCandidate_(target, archiveArticle) {
  const targetFeatures = target.prefilter || _buildTopicPrefilterFeatures_("");
  const archiveFeatures =
    archiveArticle.prefilter || _buildTopicPrefilterFeatures_("");

  let overlap = 0;
  let strongOverlap = 0;

  for (const token of targetFeatures.tokens) {
    let matched = false;

    if (archiveFeatures.tokenSet[token]) {
      matched = true;
    } else {
      for (const aToken of archiveFeatures.tokens) {
        if (_isLooseTokenMatch_(token, aToken)) {
          matched = true;
          break;
        }
      }
    }

    if (matched) {
      overlap += 1;
      if (
        token.length >= 4 ||
        /\d/.test(token) ||
        /[\p{Script=Han}\p{Script=Katakana}]/u.test(token)
      ) {
        strongOverlap += 1;
      }
    }
  }

  let numberMatches = 0;
  for (const num of targetFeatures.numbers) {
    if (archiveFeatures.numberSet[num]) numberMatches += 1;
  }

  return overlap + strongOverlap * 2 + numberMatches * 3;
}

function _scoreTopicCandidateFuzzy_(target, archiveArticle) {
  const tf = target.prefilter || _buildTopicPrefilterFeatures_("");
  const af = archiveArticle.prefilter || _buildTopicPrefilterFeatures_("");

  let charOverlap = 0;
  let targetGramCount = 0;

  for (const gram in tf.charGramSet) {
    targetGramCount += 1;
    if (af.charGramSet[gram]) charOverlap += 1;
  }

  const charRatio = targetGramCount > 0 ? charOverlap / targetGramCount : 0;

  let numberMatches = 0;
  for (const num of tf.numbers) {
    if (af.numberSet[num]) numberMatches += 1;
  }

  return charRatio * 10 + numberMatches * 3;
}

function _isLooseTokenMatch_(a, b) {
  if (!a || !b) return false;
  if (a === b) return true;

  if (a.length >= 3 && b.length >= 3) {
    if (a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return true;
  }

  const aa = a.replace(/ー/g, "");
  const bb = b.replace(/ー/g, "");
  if (aa === bb) return true;

  return false;
}

/**
 * 全アーカイブを対象に機械的な事前フィルタを行い、Gemini に渡す上位候補だけを返す。
 * スコア0でも最低限の件数は返し、取りこぼしを抑える。
 *
 * @param {{prefilter:any}} target
 * @param {{archiveIndex:number,sheetRowIndex:number,e:string,f:string,g:string,prefilter:any}[]} archiveArticles
 * @param {number} limit
 * @returns {{archiveIndex:number,sheetRowIndex:number,e:string,f:string,g:string,prefilter:any}[]}
 */
function _selectArchiveCandidatesForTarget_(target, archiveArticles, limit) {
  const scored = archiveArticles.map(function (article) {
    return {
      article: article,
      score: _scoreTopicCandidate_(target, article),
      fuzzy: _scoreTopicCandidateFuzzy_(target, article),
    };
  });

  const primarySorted = scored.slice().sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    if (b.fuzzy !== a.fuzzy) return b.fuzzy - a.fuzzy;
    return a.article.archiveIndex - b.article.archiveIndex;
  });

  const primaryCount = Math.max(1, Math.floor(limit * 0.75));
  const rescueCount = Math.max(1, limit - primaryCount);

  const primary = primarySorted.slice(0, primaryCount);

  const used = {};
  primary.forEach(function (x) {
    used[x.article.archiveIndex] = true;
  });

  const rescue = scored
    .slice()
    .sort(function (a, b) {
      if (b.fuzzy !== a.fuzzy) return b.fuzzy - a.fuzzy;
      if (b.score !== a.score) return b.score - a.score;
      return a.article.archiveIndex - b.article.archiveIndex;
    })
    .filter(function (x) {
      return !used[x.article.archiveIndex];
    })
    .slice(0, rescueCount);

  return primary.concat(rescue).map(function (x) {
    return x.article;
  });
}

// ====================================================
// duplicate.js 用 Gemini APIキー切り替え
// ====================================================

const DUP_GEMINI_KEY_PROP_MAP = {
  DVB: "DUP_GEMINI_DVB",
  POPULAR_MYANMAR: "DUP_GEMINI_POPMY",
  GLOBAL_NEW_LIGHT_OF_MYANMAR: "DUP_GEMINI_GNLM",
  KHIT_THIT_MEDIA: "DUP_GEMINI_KHIT",
  OTHER: "DUP_GEMINI_OTHER",
};

function _normalizeMediaName_(media) {
  return String(media || "")
    .trim()
    .replace(/\s+/g, " ");
}

function _resolveDuplicateGeminiKeyPropertyNameByMedia_(media) {
  const m = _normalizeMediaName_(media);

  if (m === "DVB") {
    return DUP_GEMINI_KEY_PROP_MAP.DVB;
  }

  if (m === "Popular Myanmar" || m === "Popular Myanmar (国軍系メディア)") {
    return DUP_GEMINI_KEY_PROP_MAP.POPULAR_MYANMAR;
  }

  if (
    m === "Global New Light Of Myanmar" ||
    m === "Global New Light Of Myanmar (国営紙)"
  ) {
    return DUP_GEMINI_KEY_PROP_MAP.GLOBAL_NEW_LIGHT_OF_MYANMAR;
  }

  if (m === "Khit Thit Media") {
    return DUP_GEMINI_KEY_PROP_MAP.KHIT_THIT_MEDIA;
  }

  if (
    m === "Mizzima (Burmese)" ||
    m === "Irrawaddy" ||
    m === "Myanmar Now" ||
    m === "BBC Burmese" ||
    m === "Frontier Myanmar" ||
    m === "JETROビジネス短信"
  ) {
    return DUP_GEMINI_KEY_PROP_MAP.OTHER;
  }

  return "";
}

function _getDuplicateGeminiApiKeyByMedia_(media, usageTag) {
  const propName = _resolveDuplicateGeminiKeyPropertyNameByMedia_(media);
  if (!propName) {
    Logger.log(
      "[_getDuplicateGeminiApiKeyByMedia_] unsupported media: %s (usage=%s)",
      media,
      usageTag || "",
    );
    return "";
  }

  const apiKey = PropertiesService.getScriptProperties().getProperty(propName);
  if (!apiKey) {
    Logger.log(
      "[_getDuplicateGeminiApiKeyByMedia_] missing property: %s (media=%s, usage=%s)",
      propName,
      media,
      usageTag || "",
    );
    return "";
  }

  return apiKey;
}

/**
 * 行が同一トピック判定の対象かどうかを確認する。
 *
 * 条件:
 *   1. A列が空でない（日付あり）
 *   2. C列が空でない（メディアあり）
 *   3. O列が空（未判定）
 *   4. E列・F列・G列がすべて空でない
 *
 * @param {any[]} row 0始まりの行データ配列
 * @returns {boolean}
 */
function _isDuplicateCheckTarget_(row) {
  if (!row[_DIDX_A]) return false;

  const valC = String(row[_DIDX_C] || "").trim();
  if (!valC) return false;

  const valE = String(row[_DIDX_E] || "").trim();
  const valF = String(row[_DIDX_F] || "").trim();
  const valG = String(row[_DIDX_G] || "").trim();

  const valO = row[_DIDX_O];
  const isOEmpty =
    valO === null || valO === undefined || String(valO).trim() === "";

  if (!isOEmpty) return false;

  // E/F/G だけを判定材料にする
  if (!valE || !valF || !valG) return false;

  return true;
}

/**
 * 複数ターゲットを1回のGemini呼び出しで判定する。
 *
 * @param {{ target:{rowIndex:number,e:string,f:string,g:string}, candidates:{id:string,e:string,f:string,g:string}[] }[]} batchItems
 * @param {string} apiKey Gemini APIキー
 * @param {string} usageTag ログ用タグ
 * @returns {Object<number, {count:number, matched:string[]}>} rowIndex -> 判定結果
 */
function _countSameTopicArticlesBatch_(batchItems, apiKey, usageTag) {
  const resultsByRow = {};
  if (!batchItems || batchItems.length === 0) return resultsByRow;

  const prompt = _buildTopicCheckPromptForBatch_(batchItems);
  const result = callGeminiWithKey_(apiKey, prompt, usageTag);

  if (!result || String(result).indexOf("ERROR:") === 0) {
    Logger.log("[_countSameTopicArticlesBatch_] Gemini error: " + result);
    return resultsByRow;
  }

  return _parseTopicCheckBatchResult_(result, batchItems);
}

/**
 * Geminiレスポンス（JSON文字列）から rowIndex ごとの count / matched を取り出す。
 * 期待するレスポンス形式:
 * {"results":[{"rowIndex":12,"count":1,"matched":["3"]}, ...]}
 *
 * @param {string} result Gemini レスポンステキスト
 * @param {any[]} batchItems 判定対象バッチ
 * @returns {Object<number, {count:number, matched:string[]}>}
 */
function _parseTopicCheckBatchResult_(result, batchItems) {
  const out = {};

  try {
    const s = String(result || "").trim();
    const objStart = s.indexOf("{");
    const objEnd = s.lastIndexOf("}");

    if (objStart === -1 || objEnd <= objStart) {
      Logger.log(
        "[_parseTopicCheckBatchResult_] no JSON object found: " +
          s.substring(0, 200),
      );
      return out;
    }

    const obj = JSON.parse(s.slice(objStart, objEnd + 1));
    const results = Array.isArray(obj.results) ? obj.results : [];

    for (const r of results) {
      const rowIndex = parseInt(r && r.rowIndex, 10);
      const count = parseInt(r && r.count, 10);
      const matched = Array.isArray(r && r.matched)
        ? r.matched.map(String)
        : [];

      if (!isNaN(rowIndex)) {
        out[rowIndex] = {
          count: isNaN(count) ? 0 : Math.max(0, count),
          matched: matched,
        };
      }
    }

    for (const item of batchItems || []) {
      const rowIndex = item.target.rowIndex;
      if (!(rowIndex in out)) {
        out[rowIndex] = { count: 0, matched: [] };
      }
    }

    return out;
  } catch (e) {
    Logger.log(
      "[_parseTopicCheckBatchResult_] parse error: " +
        e +
        " raw=" +
        String(result || "").substring(0, 200),
    );
    return out;
  }
}

/**
 * 同一トピック判定用（複数ターゲット一括）のGeminiプロンプトを生成する。
 *
 * @param {any[]} batchItems 判定対象バッチ
 * @returns {string} Gemini送信用プロンプト
 */
function _buildTopicCheckPromptForBatch_(batchItems) {
  const sections = batchItems
    .map(function (item) {
      const t = item.target;
      const targetLines = [];
      if (t.e) targetLines.push("見出しE: " + t.e);
      if (t.f) targetLines.push("見出しF: " + t.f);
      if (t.g) targetLines.push("見出しG: " + t.g);

      const archiveSection = item.candidates
        .slice(0, TOPIC_CHECK_BATCH_SIZE)
        .map(function (a) {
          const lines = [];
          if (a.e) lines.push("見出しE: " + a.e);
          if (a.f) lines.push("見出しF: " + a.f);
          if (a.g) lines.push("見出しG: " + a.g);
          return "【候補ID:" + a.id + "】\n" + lines.join("\n");
        })
        .join("\n\n");

      return (
        "==== 判定対象 rowIndex=" +
        t.rowIndex +
        " ====\n" +
        targetLines.join("\n") +
        "\n\n【アーカイブ候補リスト（" +
        Math.min(item.candidates.length, TOPIC_CHECK_BATCH_SIZE) +
        "件）】\n" +
        archiveSection
      );
    })
    .join("\n\n");

  return `以下の複数「判定対象記事」ごとに、対応する「アーカイブ候補リスト」と比較し、同一トピックの記事件数を算出してください。

比較に使用してよい情報は、判定対象記事・アーカイブ候補の両方とも E列, F列, G列の内容のみです。
本文要約、原文タイトル、原文本文、その他の列の情報は判定に使用しないでください。

【同一トピックの判定基準】

◆ 必須条件（以下のうち少なくとも1つが成立すること）
1. 同一事象: E/F/G の見出し情報から見て、同じ具体的な事件・攻撃・逮捕・会議・法案・声明・裁判・交渉を扱っている
   ※ 同じ出来事の「続報」「別メディアの報道」も同一トピックとする

◆ 補強条件（事象が一致する前提で以下が一致するほど確度が高い）
2. 同一地域: 同じ地名（州・管区・都市・郡区レベル）が共通して登場する
3. 同一主体: 同じ組織・人物（国軍・PDF・KNUなど）が主語・関与者として共通する
4. 同一時点: 同じ日付・時期（「3月12日」「先週」等）を指している
5. 同一数値: 死者数・被害額・人数などの具体的数値が共通して登場する
6. 同一発言: 同じ人物・組織による同じ声明・発言が示されている

◆ 同一トピックではない例
- 同じ地域で起きた別の事件
- 同じ組織・人物による別の活動や別の発言
- 同じテーマ（人権・徴兵など）だが具体的な事象が異なる記事
- 関連はあるが続報でも重複でもない記事（別の地域での同種の事件など）

◆ 判定上の注意
- メディアが異なっても同一事象なら同一トピックとする
- 原文言語が違っても構わない
- 微妙に異なる数値（報道機関による差異）は同一トピックとして扱う
- 判定対象記事は E列, F列, G列のみを参照する
- アーカイブ候補も E列, F列, G列のみを参照して判定する
- E/F/G に明示されていない本文情報を推測して補わないこと
- 単なるテーマの近さではなく、具体的に同じ出来事かどうかを優先して判定すること

${sections}

【出力形式（必須）】
以下のJSON形式のみで出力してください。それ以外の文字は一切含めないでください。
{"results":[{"rowIndex": 行番号, "count": 同一トピック数, "matched": [一致した候補IDの配列]}]}

出力例: {"results":[{"rowIndex":12,"count":2,"matched":["3","9"]},{"rowIndex":18,"count":0,"matched":[]}]}`;
}

/**
 * prod シートに「同一トピック判定の対象行」が1件でもあるかを返す
 *
 * 目的:
 * - 時間トリガー入口で先に軽く判定し、
 *   対象行が無ければロック取得や重い処理に入らず即終了する
 *
 * 判定条件:
 * - _isDuplicateCheckTarget_() と同じ条件を使う
 *   （A/C/F/I/O などの条件は既存関数に委譲）
 *
 * @returns {boolean}
 */
function hasDuplicateCheckTargetsInProd_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName("prod");
  if (!sheet) return false;

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return false; // データ行なし

  // A〜O まで読めば _isDuplicateCheckTarget_() の判定に必要な列を満たせる
  const values = sheet.getRange(2, 1, lastRow - 1, _DIDX_O + 1).getValues();

  for (let i = 0; i < values.length; i++) {
    if (_isDuplicateCheckTarget_(values[i])) {
      return true;
    }
  }

  return false;
}

/**
 * 同一トピック判定の実行許可時間かどうかを返す
 *
 * 想定運用:
 * - 時間主導トリガーで 10 分ごとに起動する
 * - 実際に処理を走らせるのは 17:00〜翌 07:00 の間だけ
 */
function isWithinDuplicateProcessingWindow_() {
  const now = new Date();
  const h = now.getHours();
  const m = now.getMinutes();
  const t = h * 60 + m;

  const START = 17 * 60; // 17:00
  const END = 7 * 60; // 07:00

  return t >= START || t <= END;
}

/**
 * prod シート専用:
 * 10分ごとの時間トリガーから呼ぶ入口関数
 *
 * 挙動:
 * 1. 時間帯外なら即スキップ
 * 2. 判定対象行が無ければ即スキップ
 * 3. 対象行があるときだけロック取得
 * 4. ロック取得できたときだけ prod の同一トピック判定を実行
 */
function runDuplicateTopicsProdBatch() {
  // 実行時間帯外なら何もしない
  if (!isWithinDuplicateProcessingWindow_()) {
    Logger.log(
      "[runDuplicateTopicsProdBatch] outside allowed time window -> skip",
    );
    return;
  }

  // 判定対象が1件も無ければ何もしない
  if (!hasDuplicateCheckTargetsInProd_()) {
    Logger.log(
      "[runDuplicateTopicsProdBatch] no duplicate-check targets in prod -> skip",
    );
    return;
  }

  const lock = LockService.getDocumentLock();

  try {
    // 他処理がロック中ならこの回はスキップ
    if (!lock.tryLock(5000)) {
      Logger.log("[runDuplicateTopicsProdBatch] lock busy -> skip");
      return;
    }

    // 念のため、ロック取得後にも再確認
    // （ロック取得までの間に別処理が O 列を書き終えて対象ゼロになる可能性がある）
    if (!hasDuplicateCheckTargetsInProd_()) {
      Logger.log("[runDuplicateTopicsProdBatch] no targets after lock -> skip");
      return;
    }

    checkDuplicateTopicsProd();
  } catch (err) {
    Logger.log("[runDuplicateTopicsProdBatch] error: " + err);
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}
