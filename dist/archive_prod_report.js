const SPREADSHEET_ID = ""; // コンテナバインドGASなら空でOK。単独GASならスプレッドシートIDを入れる
const SHEET_NAME = "archive_prod";

const NOTIFY_EMAIL_TO_PROD_PROP = "NOTIFY_EMAIL_TO_PROD";
const REPORT_SUBJECT = "【MNA】収集報告";

const MEDIA_PATTERN_1 = [
  "Popular Myanmar (国軍系メディア)",
  "DVB",
  "Global New Light Of Myanmar (国営紙)",
  "Khit Thit Media",
  "News Eleven",
  "Mizzima (Burmese)",
  "BBC Burmese",
];

const MEDIA_PATTERN_2 = ["Myanmar Now", "Irrawaddy", "Frontier Myanmar"];

const GNLM = "Global New Light Of Myanmar (国営紙)";

/**
 * archive_prod収集報告メールの送信先を取得する。
 * prompt.js と同じ Script Properties の NOTIFY_EMAIL_TO_PROD を使う。
 *
 * 複数宛先はカンマ区切りで設定可能。
 * 例: aaa@example.com, bbb@example.com
 */
function getArchiveProdReportEmailTo_() {
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty(NOTIFY_EMAIL_TO_PROD_PROP) || "";

  return raw
    .split(",")
    .map((email) => email.trim())
    .filter(Boolean)
    .join(",");
}

/**
 * archive_prodをチェックし、報告事項がある場合のみメール送信する。
 */
function checkArchiveProdAndSendMail() {
  const ss = SPREADSHEET_ID
    ? SpreadsheetApp.openById(SPREADSHEET_ID)
    : SpreadsheetApp.getActiveSpreadsheet();

  const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  const sheet = ss.getSheetByName(SHEET_NAME);

  if (!sheet) {
    throw new Error(`シートが見つかりません: ${SHEET_NAME}`);
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  // A:N まで取得
  const values = sheet.getRange(2, 1, lastRow - 1, 14).getValues();

  const yesterdayKey = getRelativeDateKey_(-1, tz);

  const previousDayMissingMedia = [];
  const pastTwoDaysMissingMedia = [];
  const noBodyMedia = [];

  // 前日対象行
  const yesterdayRows = values.filter((row) => {
    return toDateKey_(row[0], tz) === yesterdayKey;
  });

  // パターン1：前日のA列日付に対象メディアが存在しない場合
  const yesterdayMediaSet = new Set(
    yesterdayRows
      .map((row) => normalizeText_(row[2])) // C列
      .filter(Boolean),
  );

  MEDIA_PATTERN_1.forEach((media) => {
    if (!yesterdayMediaSet.has(media)) {
      previousDayMissingMedia.push(media);
    }
  });

  // パターン2：archive_prodシート全体で対象メディアが存在しない場合
  // archive_prodが過去2日分を保持している前提
  const allMediaSet = new Set(
    values
      .map((row) => normalizeText_(row[2])) // C列
      .filter(Boolean),
  );

  MEDIA_PATTERN_2.forEach((media) => {
    if (!allMediaSet.has(media)) {
      pastTwoDaysMissingMedia.push(media);
    }
  });

  // 全メディア共通：前日対象でN列が空白のもの
  // 同じメディアで複数件あっても、メール文面はメディア単位で1回に集約
  const noBodyMediaSet = new Set();

  yesterdayRows.forEach((row) => {
    const media = normalizeText_(row[2]); // C列
    const titleOriginal = row[12]; // M列
    const bodyOriginal = row[13]; // N列

    if (!media) return;
    if (!isBlank_(bodyOriginal)) return;

    // 例外：
    // C列がGlobal New Light Of Myanmar (国営紙)で、
    // M列が "14 May 2026" のような日付を表す場合は報告しない
    if (media === GNLM && isEnglishDateLike_(titleOriginal)) {
      return;
    }

    noBodyMediaSet.add(media);
  });

  noBodyMediaSet.forEach((media) => {
    noBodyMedia.push(media);
  });

  // 報告事項がなければメール送信しない
  const hasReport =
    previousDayMissingMedia.length > 0 ||
    pastTwoDaysMissingMedia.length > 0 ||
    noBodyMedia.length > 0;

  if (!hasReport) return;

  const reportTo = getArchiveProdReportEmailTo_();

  if (!reportTo) {
    Logger.log(
      `[checkArchiveProdAndSendMail] missing script property: ${NOTIFY_EMAIL_TO_PROD_PROP}`,
    );
    return;
  }

  const body = buildReportBody_(
    previousDayMissingMedia,
    pastTwoDaysMissingMedia,
    noBodyMedia,
  );

  MailApp.sendEmail({
    to: reportTo,
    subject: REPORT_SUBJECT,
    body,
  });
}

/**
 * メール本文を作成する。
 */
function buildReportBody_(
  previousDayMissingMedia,
  pastTwoDaysMissingMedia,
  noBodyMedia,
) {
  return [
    "【前日未収集メディア】",
    ...previousDayMissingMedia,
    "",
    "【過去2日未収集メディア】",
    ...pastTwoDaysMissingMedia,
    "",
    "【本文未取得メディア】",
    ...noBodyMedia,
  ].join("\n");
}

/**
 * 毎日実行したい場合のトリガー作成用。
 * 1回だけ手動実行してください。
 */
function createDailyArchiveProdCheckTrigger() {
  ScriptApp.newTrigger("checkArchiveProdAndSendMail")
    .timeBased()
    .everyDays(1)
    .atHour(8)
    .create();
}

function getRelativeDateKey_(offsetDays, tz) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return Utilities.formatDate(d, tz, "yyyy-MM-dd");
}

function toDateKey_(value, tz) {
  if (isValidDate_(value)) {
    return Utilities.formatDate(value, tz, "yyyy-MM-dd");
  }

  const text = normalizeText_(value);
  if (!text) return "";

  const parsed = new Date(text);
  if (isValidDate_(parsed)) {
    return Utilities.formatDate(parsed, tz, "yyyy-MM-dd");
  }

  return "";
}

function isBlank_(value) {
  return normalizeText_(value) === "";
}

function normalizeText_(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isValidDate_(value) {
  return (
    Object.prototype.toString.call(value) === "[object Date]" &&
    !isNaN(value.getTime())
  );
}

function isEnglishDateLike_(value) {
  if (isValidDate_(value)) return true;

  const text = normalizeText_(value);
  if (!text) return false;

  const datePattern =
    /^\d{1,2}\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{4}$/i;

  if (!datePattern.test(text)) return false;

  const parsed = new Date(text.replace(/\bSept\b/i, "Sep"));
  return isValidDate_(parsed);
}
