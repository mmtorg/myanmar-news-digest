/**
 * prodシートのK列が "a" の行だけを、A列の日付の「月」ごとのCSVに追記する。
 * スプレッドシート側のデータは一切変更しない。
 */
function exportProdRowsToMonthlyCsv() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("prod");
  if (!sheet) {
    throw new Error('シート "prod" が見つかりません。');
  }

  const tz = Session.getScriptTimeZone(); // 例: Asia/Yangon
  const range = sheet.getDataRange();
  const values = range.getValues();
  if (values.length < 2) {
    // ヘッダーのみ・または空
    return;
  }

  const header = values[0];
  const dataRows = values.slice(1);

  // 月ごとの行をまとめる: key = "YYYYMM" , value = rows[]
  /** @type {{[monthKey: string]: any[][]}} */
  const monthBuckets = {};

  for (let i = 0; i < dataRows.length; i++) {
    const row = dataRows[i];

    const status = row[10]; // K列 (0始まりで index=10)
    if (status !== "a") {
      continue;
    }

    const dateValue = row[0]; // A列 日付
    if (!(dateValue instanceof Date)) {
      // 日付でない場合はスキップ
      continue;
    }

    const monthKey = Utilities.formatDate(dateValue, tz, "yyyyMM"); // 例: 202512

    if (!monthBuckets[monthKey]) {
      monthBuckets[monthKey] = [];
    }
    monthBuckets[monthKey].push(row);
  }

  // 何も対象がなければ終了
  const monthKeys = Object.keys(monthBuckets);
  if (monthKeys.length === 0) {
    return;
  }

  // 各月ごとにCSVを書き出し
  monthKeys.forEach(function (monthKey) {
    appendRowsToMonthlyCsv_(monthKey, header, monthBuckets[monthKey]);
  });
}

// スクリプトプロパティからCSV出力先フォルダを取得
function getCsvOutputFolder_() {
  const folderId = PropertiesService.getScriptProperties().getProperty(
    "CSV_OUTPUT_FOLDER_ID"
  );

  if (!folderId) {
    throw new Error("スクリプトプロパティ CSV_OUTPUT_FOLDER_ID が未設定です");
  }

  return DriveApp.getFolderById(folderId);
}

/**
 * 指定した月キー(YYYYMM)のCSVファイルに、ヘッダー付きで行を追記する。
 * 既に同じ行が存在する場合は重複保存しない。
 *
 * @param {string} monthKey 例: "202512"
 * @param {any[]} headerRow prodシートのヘッダー行
 * @param {any[][]} rows    追記したい行
 */
function appendRowsToMonthlyCsv_(monthKey, headerRow, rows) {
  if (!rows || rows.length === 0) return;

  const fileName = "prod_" + monthKey + ".csv";

  // 既存ファイルを探す（同名が複数ある想定はしない）
  let file;
  const folder = getCsvOutputFolder_();
  const files = folder.getFilesByName(fileName);

  if (files.hasNext()) {
    file = files.next();
  } else {
    file = folder.createFile(fileName, "", MimeType.CSV);
  }

  // 既存内容を読み込み
  const existingContent = file.getBlob().getDataAsString("UTF-8");
  let lines = [];
  if (existingContent) {
    lines = existingContent.split("\n").filter(function (l) {
      return l !== "";
    });
  }

  const headerLine = headerRow.map(csvEscape_).join(",");

  // 既存行のセットを作って重複チェックに使う
  const existingSet = new Set(lines);

  // 1行目がヘッダーでない場合のみ、ヘッダーを先頭に差し込む
  if (lines.length === 0) {
    // 完全に空 → ヘッダーを入れてからデータを追加
    lines.push(headerLine);
    existingSet.add(headerLine);
  } else {
    const firstLine = lines[0];
    if (firstLine !== headerLine) {
      // ヘッダーが存在しないとみなして、先頭に挿入
      lines.unshift(headerLine);
      existingSet.add(headerLine);
    }
  }

  // 新規行をCSV形式に変換して、既存にないものだけ追加
  rows.forEach(function (row) {
    const line = row.map(csvEscape_).join(",");
    if (!existingSet.has(line)) {
      lines.push(line);
      existingSet.add(line);
    }
  });

  const newContent = lines.join("\n");
  // 上書き保存（CSVとして維持される）
  file.setContent(newContent);
}

/**
 * CSV用エスケープ
 * - null/undefinedは空文字
 * - " を "" にエスケープ
 * - カンマ/ダブルクオート/改行を含む場合はダブルクオートで囲む
 *
 * @param {any} value
 * @returns {string}
 */
function csvEscape_(value) {
  if (value === null || value === undefined) return "";
  let str = String(value);
  let needsQuote = false;

  if (str.indexOf('"') !== -1) {
    str = str.replace(/"/g, '""');
    needsQuote = true;
  }
  if (
    str.indexOf(",") !== -1 ||
    str.indexOf("\n") !== -1 ||
    str.indexOf("\r") !== -1
  ) {
    needsQuote = true;
  }
  if (needsQuote) {
    str = '"' + str + '"';
  }
  return str;
}
