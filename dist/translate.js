/**
 * 設定
 */

const CONFIG = {
  FOLDER_ID: folderId,
  BATCH_SIZE: 30, // 1回のAPI呼び出しでまとめて翻訳する行数
  START_ROW: 2, // 1行目はヘッダ想定
};

// 列番号（1始まり）
const COL = {
  H_TITLE: 8,
  I_BODY: 9,
  P_EN_TITLE: 16,
  Q_EN_BODY: 17,
  R_ZH_TITLE: 18,
  S_ZH_BODY: 19,
};

/**
 * エントリポイント：フォルダ配下の全スプレッドシートを巡回して未翻訳行を埋める
 */
function translateFolderSpreadsheets() {
  const lock = LockService.getScriptLock(); // 同時実行衝突を避ける :contentReference[oaicite:1]{index=1}
  if (!lock.tryLock(30000)) {
    console.log("Could not acquire lock. Skip this run.");
    return;
  }

  try {
    const folder = DriveApp.getFolderById(CONFIG.FOLDER_ID);
    const files = folder.getFilesByType(MimeType.GOOGLE_SHEETS);

    while (files.hasNext()) {
      const file = files.next();
      try {
        const ss = SpreadsheetApp.openById(file.getId());
        processSpreadsheet_(ss);
      } catch (e) {
        console.error(`Failed file: ${file.getName()} (${file.getId()})`, e);
      }
    }
  } finally {
    lock.releaseLock();
  }
}

/**
 * 1つのスプレッドシート内の全シートを処理
 * （もし「必ず1枚だけ」なら getSheets()[0] のみに絞ってもOK）
 */
function processSpreadsheet_(ss) {
  const sheets = ss.getSheets();
  for (const sh of sheets) {
    try {
      processSheet_(sh);
    } catch (e) {
      console.error(`Failed sheet: ${ss.getName()} / ${sh.getName()}`, e);
    }
  }
}

/**
 * 1シート分：H/Iに値があり、P〜Sが全部空の行だけ翻訳して「値」で書き込む
 */
function processSheet_(sh) {
  const lastRow = sh.getLastRow();
  if (lastRow < CONFIG.START_ROW) return;

  // 必要範囲をまとめて取得（H〜S = 8〜19）
  const numRows = lastRow - CONFIG.START_ROW + 1;
  const range = sh.getRange(
    CONFIG.START_ROW,
    COL.H_TITLE,
    numRows,
    COL.S_ZH_BODY - COL.H_TITLE + 1,
  );
  const values = range.getValues();

  // 未翻訳行を集める
  const targets = [];
  for (let i = 0; i < values.length; i++) {
    const rowIndex = CONFIG.START_ROW + i;

    const hTitle = values[i][0]; // H
    const iBody = values[i][COL.I_BODY - COL.H_TITLE]; // I (H基準のオフセット)

    const pEnTitle = values[i][COL.P_EN_TITLE - COL.H_TITLE]; // P
    const qEnBody = values[i][COL.Q_EN_BODY - COL.H_TITLE]; // Q
    const rZhTitle = values[i][COL.R_ZH_TITLE - COL.H_TITLE]; // R
    const sZhBody = values[i][COL.S_ZH_BODY - COL.H_TITLE]; // S

    const hasSource =
      String(hTitle).trim() !== "" && String(iBody).trim() !== "";
    const alreadyTranslated =
      String(pEnTitle).trim() !== "" ||
      String(qEnBody).trim() !== "" ||
      String(rZhTitle).trim() !== "" ||
      String(sZhBody).trim() !== "";

    if (hasSource && !alreadyTranslated) {
      targets.push({ rowIndex, title: String(hTitle), body: String(iBody) });
    }
  }

  if (targets.length === 0) return;

  // バッチでLLMに投げて埋める
  // Claude は出力（英語・中国語の本文）が長くなるため、1回の件数を減らして時間切れを避ける
  const batchSize = isClaudePrimary_() ? CLAUDE_TRANSLATE_BATCH_SIZE : CONFIG.BATCH_SIZE;
  for (let start = 0; start < targets.length; start += batchSize) {
    const batch = targets.slice(start, start + batchSize);
    const translations = callGeminiTranslateBatch_(batch);

    // 書き込み用2次元配列（P〜Sの4列）
    // 行ごとに setValues したいので、各行に対して個別Rangeで書き込み（小さめバッチなら十分速い）
    for (const t of translations) {
      const row = batch[t.idx].rowIndex;
      sh.getRange(row, COL.P_EN_TITLE, 1, 4).setValues([
        [t.en_title, t.en_body, t.zh_title, t.zh_body],
      ]);
    }
  }
}

/**
 * Gemini APIで複数行をまとめて翻訳して JSON配列で返す
 * 返却形式:
 * [
 *  {"idx":0,"en_title":"...","en_body":"...","zh_title":"...","zh_body":"..."},
 *  ...
 * ]
 */
const CLAUDE_TRANSLATE_BATCH_SIZE = 10;

const CLAUDE_TRANSLATE_SCHEMA_ = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          idx: { type: "integer" },
          en_title: { type: "string" },
          en_body: { type: "string" },
          zh_title: { type: "string" },
          zh_body: { type: "string" },
        },
        required: ["idx", "en_title", "en_body", "zh_title", "zh_body"],
      },
    },
  },
  required: ["items"],
};

/**
 * Claude Haiku 5.5 で複数行をまとめて翻訳する（返却形式は callGeminiTranslateBatch_ と同じ配列）
 */
function callClaudeTranslateBatch_(batch) {
  const items = batch.map((x, idx) => ({
    idx,
    title: x.title,
    body: x.body,
  }));

  const prompt = [
    "あなたはプロの翻訳者です。",
    "以下の日本語テキストを (1) 英語、(2) 中国語（簡体字）に翻訳してください。",
    "入力の各要素について、idx をそのまま返し、en_title / en_body / zh_title / zh_body を埋めてください。",
    "本文の改行/空行/括弧は保持してください。",
    "固有名詞は必要に応じて原文を維持してください。",
    "",
    "入力(JSON):",
    JSON.stringify(items),
  ].join("\n");

  const resp = callClaude_(prompt, "translate", {
    model: CLAUDE_MODEL_LIGHT,
    schema: CLAUDE_TRANSLATE_SCHEMA_,
    maxTokens: 32000,
  });

  if (String(resp).indexOf("ERROR:") === 0) {
    throw new Error("Claude translate error: " + resp);
  }

  const obj = JSON.parse(resp);
  const arr = obj && Array.isArray(obj.items) ? obj.items : null;
  if (!arr) throw new Error("Claude output has no items array.");
  return arr;
}

function callGeminiTranslateBatch_(batch) {
  if (isClaudePrimary_()) {
    return callClaudeTranslateBatch_(batch);
  }

  const apiKey = PropertiesService.getScriptProperties().getProperty(
    "GEMINI_TRANSLATE_API_KEY",
  );
  if (!apiKey)
    throw new Error("Missing GEMINI_TRANSLATE_API_KEY in Script Properties.");

  const items = batch.map((x, idx) => ({
    idx,
    title: x.title,
    body: x.body,
  }));

  const prompt = [
    "あなたはプロの翻訳者です。",
    "以下の日本語テキストを (1) 英語、(2) 中国語（簡体字）に翻訳してください。",
    "出力は **必ず有効なJSONのみ** にしてください。",
    "マークダウン、説明文、前置き、コメントは一切含めないでください。",
    "出力形式は JSON配列 で、各要素は以下のキーを持つオブジェクトです：",
    "idx, en_title, en_body, zh_title, zh_body",
    "本文の改行/空行/括弧は保持してください。",
    "固有名詞は必要に応じて原文を維持してください。",
    "",
    "入力(JSON):",
    JSON.stringify(items),
  ].join("\n");

  const payload = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
  };

  const request = (model) => {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      model,
    )}:generateContent?key=${encodeURIComponent(apiKey)}`;
    return UrlFetchApp.fetch(url, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({
        ...payload,
        generationConfig:
          model === GEMINI_MODEL
            ? { responseMimeType: "application/json" }
            : { temperature: 0.0 },
      }),
      muteHttpExceptions: true,
    });
  };

  let res = request(GEMINI_MODEL);
  if (res.getResponseCode() === 429) {
    res = request(GEMINI_FALLBACK_MODEL);
  }

  const code = res.getResponseCode();
  const text = res.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error(`Gemini API error: HTTP ${code}\n${text}`);
  }

  const data = JSON.parse(text);

  // 候補テキストを取り出す（Gemini APIの一般的なレスポンス構造）
  const outText =
    data?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") ?? "";

  // 念のため JSON 部分だけ抜き出す（前後に空白などが混ざるケース対策）
  const json = extractFirstJsonArray_(outText);
  const arr = JSON.parse(json);

  // 軽いバリデーション
  if (!Array.isArray(arr)) throw new Error("Gemini output is not an array.");
  for (const o of arr) {
    for (const k of ["idx", "en_title", "en_body", "zh_title", "zh_body"]) {
      if (!(k in o))
        throw new Error(`Missing key ${k} in output: ${JSON.stringify(o)}`);
    }
  }
  return arr;
}

/**
 * 文字列から最初の JSON配列 [...] を抽出
 */
function extractFirstJsonArray_(s) {
  const start = s.indexOf("[");
  const end = s.lastIndexOf("]");
  if (start === -1 || end === -1 || end <= start) {
    throw new Error("Could not find JSON array in Gemini output:\n" + s);
  }
  return s.slice(start, end + 1).trim();
}

/**
 * 初回だけ実行：15分ごとの時間主導トリガーを作る
 */
function createTimeTrigger_15min() {
  ScriptApp.newTrigger("translateFolderSpreadsheets")
    .timeBased()
    .everyMinutes(15)
    .create();
}
