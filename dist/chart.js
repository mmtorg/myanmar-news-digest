/**
 * 設定
 */

const spreadsheetId = PropertiesService.getScriptProperties().getProperty(
  "DEST_SPREADSHEET_ID"
);

const SOURCE_FOLDER_ID = folderId;
const DEST_SPREADSHEET_ID = spreadsheetId;
const OUTPUT_SHEET_NAME = "output";

// 元シートの列（1始まり）
const COL_PLAN = 3; // C列
const COL_I = 9; // I列

/**
 * メイン：フォルダ配下の全スプレッドシートから抽出して集計先へ書き出す
 */
function exportBusinessPlanRows() {
  const outSs = SpreadsheetApp.openById(DEST_SPREADSHEET_ID);
  const outSh =
    outSs.getSheetByName(OUTPUT_SHEET_NAME) ||
    outSs.insertSheet(OUTPUT_SHEET_NAME);

  // ヘッダが無ければ作成
  if (outSh.getLastRow() === 0) {
    outSh.appendRow([
      "日付",
      "1米ドル",
      "1円",
      "金1ティカル",
      "レギュラーガソリン1L",
      "軽油1L",
    ]);
  }

  // 既存データ（重複防止用キー）
  const existingKeys = buildExistingKeys_(outSh);

  const folder = DriveApp.getFolderById(SOURCE_FOLDER_ID);
  const files = folder.getFilesByType(MimeType.GOOGLE_SHEETS);

  const toAppend = [];

  while (files.hasNext()) {
    const file = files.next();
    const ss = SpreadsheetApp.openById(file.getId());
    const sheets = ss.getSheets();

    for (const sh of sheets) {
      const lastRow = sh.getLastRow();
      if (lastRow < 2) continue;

      // B〜Iまで取れば十分（B=2, I=9）
      const range = sh.getRange(2, 1, lastRow - 1, 9); // 2行目〜、A〜I
      const values = range.getValues();

      for (let r = 0; r < values.length; r++) {
        const plan = values[r][COL_PLAN - 1];
        const iText = values[r][COL_I - 1];

        if (!String(plan).includes("Businessプラン限定")) continue;
        if (!iText || String(iText).trim() === "") continue;

        const parsed = parseRateFuelText_(String(iText));
        if (!parsed) continue;

        // 重複防止キー：日付 + 主要数値 + 元ファイル名（必要なら）
        const key = [
          parsed.date,
          parsed.usd,
          parsed.jpy,
          parsed.gold,
          parsed.gas,
          parsed.diesel,
        ].join("|");

        if (existingKeys.has(key)) continue;
        existingKeys.add(key);

        toAppend.push([
          parsed.date,
          parsed.usd,
          parsed.jpy,
          parsed.gold,
          parsed.gas,
          parsed.diesel,
        ]);
      }
    }
  }

  if (toAppend.length > 0) {
    outSh
      .getRange(outSh.getLastRow() + 1, 1, toAppend.length, toAppend[0].length)
      .setValues(toAppend);
  }
}

/**
 * I列の文章をパースして必要項目を抜き出す
 * 例:
 * 【2025年12月25日 MMT23:00時点】
 * 1米ドル=3,985ks ...
 * 1円=25.36ks ...
 * 金1ティカル=928万ks ...
 * レギュラーガソリン1L=2,520ks ...
 * 軽油1L=2,230ks ...
 */
function parseRateFuelText_(text) {
  // 日付：2025年12月25日 → 2025/12/25
  const mDate = text.match(/【\s*(\d{4})年\s*(\d{1,2})年?(\d{1,2})日/); // 保険で "年" 重複も許容
  let y, mo, d;
  if (mDate) {
    y = mDate[1];
    mo = String(parseInt(mDate[2], 10)).padStart(2, "0");
    d = String(parseInt(mDate[3], 10)).padStart(2, "0");
  } else {
    // 別表記があり得るならここに追加
    const mDate2 = text.match(/【\s*(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日/);
    if (!mDate2) return null;
    y = mDate2[1];
    mo = String(parseInt(mDate2[2], 10)).padStart(2, "0");
    d = String(parseInt(mDate2[3], 10)).padStart(2, "0");
  }
  const date = `${y}/${mo}/${d}`;

  // 値抽出（カンマ付き数値・小数OK）
  const usd = pickNumber_(text, /1米ドル\s*=\s*([0-9,]+(?:\.[0-9]+)?)\s*ks/i);
  const jpy = pickNumber_(text, /1円\s*=\s*([0-9,]+(?:\.[0-9]+)?)\s*ks/i);

  // 金：928万ks → "928万" をそのまま（要望に合わせて）
  const goldText = pickText_(
    text,
    /金1ティカル\s*=\s*([0-9,]+(?:\.[0-9]+)?)(万|億)?\s*ks/i
  );
  const gold = jpUnitToNumber_(goldText); // 928万 → 9280000

  const gas = pickNumber_(
    text,
    /レギュラーガソリン1L\s*=\s*([0-9,]+(?:\.[0-9]+)?)\s*ks/i
  );
  const diesel = pickNumber_(text, /軽油1L\s*=\s*([0-9,]+(?:\.[0-9]+)?)\s*ks/i);

  if (!usd || !jpy || !gold || !gas || !diesel) return null;

  return { date, usd, jpy, gold, gas, diesel };
}

function pickNumber_(text, regex) {
  const m = text.match(regex);
  if (!m) return "";
  return m[1].replace(/,/g, "");
}

function pickText_(text, regex) {
  const m = text.match(regex);
  if (!m) return "";
  const num = m[1].replace(/,/g, "");
  const unit = m[2] ? m[2] : "";
  return `${num}${unit}`;
}

/**
 * 既存の出力シートから重複チェック用キーを作る
 */
function buildExistingKeys_(outSh) {
  const set = new Set();
  const lastRow = outSh.getLastRow();
  if (lastRow < 2) return set;

  const values = outSh.getRange(2, 1, lastRow - 1, 6).getValues();
  for (const row of values) {
    const key = row.join("|");
    set.add(key);
  }
  return set;
}

/**
 * "928万" → "9280000"
 * "9.28万" → "92800"
 * "1.2億" → "120000000"
 * "9280000" → "9280000"
 */
function jpUnitToNumber_(s) {
  if (!s) return "";
  let str = String(s).trim().replace(/,/g, "");

  // 末尾の単位を拾う（万/億）
  const m = str.match(/^([0-9]+(?:\.[0-9]+)?)(万|億)?$/);
  if (!m) return str; // 想定外フォーマットはそのまま

  const num = parseFloat(m[1]);
  const unit = m[2] || "";

  let mul = 1;
  if (unit === "万") mul = 10000;
  if (unit === "億") mul = 100000000;

  // 小数対応：整数化して文字列で返す（小数点は出さない）
  const val = Math.round(num * mul);
  return String(val);
}
