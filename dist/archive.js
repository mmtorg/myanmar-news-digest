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

  // 各月ごとにスプレッドシートを書き出し
  monthKeys.forEach(function (monthKey) {
    appendRowsToMonthlySpreadsheet_(monthKey, header, monthBuckets[monthKey]);
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
 * セル内の「空行」を維持するために、空行部分にゼロ幅スペースを入れる。
 * 例: "a\n\nb" -> "a\n\u200B\nb"
 */
function preserveBlankLinesInCell_(v) {
  if (typeof v !== "string") return v;
  // 改行コードの揺れを統一
  const s = v.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  // 空行（= 連続改行で生まれる空の行）にゼロ幅スペースを注入
  // splitして空行要素を検知するのが堅い
  const lines = s.split("\n");
  const fixed = lines.map((line) => (line === "" ? "\u200B" : line)).join("\n");
  return fixed;
}

/**
 * 指定した月キー(YYYYMM)のスプレッドシートファイルに、ヘッダー付きで行を追記する。
 *
 * @param {string} monthKey 例: "202512"
 * @param {any[]} headerRow prodシートのヘッダー行
 * @param {any[][]} rows    追記したい行
 */
function appendRowsToMonthlySpreadsheet_(monthKey, headerRow, rows) {
  if (!rows || rows.length === 0) return;

  const fileName = "prod_" + monthKey;
  const folder = getCsvOutputFolder_();

  let file;
  const files = folder.getFilesByName(fileName);
  if (files.hasNext()) {
    file = files.next();
  } else {
    const ss = SpreadsheetApp.create(fileName);
    file = DriveApp.getFileById(ss.getId());
    folder.addFile(file);
    DriveApp.getRootFolder().removeFile(file);
  }

  const outSs = SpreadsheetApp.openById(file.getId());

  // アーカイブ先スプレッドシートのTZを固定
  outSs.setSpreadsheetTimeZone("Asia/Yangon");

  const sheetName = "prod";
  let outSheet = outSs.getSheetByName(sheetName);
  if (!outSheet) outSheet = outSs.insertSheet(sheetName);

  // ヘッダー確認＆設定
  const lastRow = outSheet.getLastRow();
  if (lastRow === 0) {
    outSheet.getRange(1, 1, 1, headerRow.length).setValues([headerRow]);
  }

  // ★ setValues 前に I列だけ空行保持加工（I列=9列目 index=8）
  const rowsFixed = rows.map((r) => {
    const rr = r.slice(); // 行をコピー（元データを壊さない）
    rr[8] = preserveBlankLinesInCell_(rr[8]);
    return rr;
  });

  // ★ 重複チェックなしでそのまま追記
  outSheet
    .getRange(outSheet.getLastRow() + 1, 1, rowsFixed.length, headerRow.length)
    .setValues(rowsFixed);
}
