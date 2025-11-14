/************************************************************
 * 0. 共通ルール（Python側の COMMON_TRANSLATION_RULES / TITLE_OUTPUT_RULES / HEADLINE_PROMPT_x 相当）
 ************************************************************/

// 用語統一＋通貨変換ルール（COMMON_TRANSLATION_RULES 相当）

const COMMON_TRANSLATION_RULES = `
【翻訳時の用語統一ルール（必ず従うこと）】
このルールは記事タイトルと本文の翻訳に必ず適用してください。
クーデター指導者⇒総司令官
テロリスト指導者ミン・アウン・フライン⇒ミン・アウン・フライン
テロリストのミン・アウン・フライン⇒ミン・アウン・フライン
テロリスト軍事指導者⇒総司令官
テロリスト軍事政権⇒軍事政権
テロリスト軍事評議会⇒軍事政権
テロリスト軍⇒国軍
軍事評議会⇒軍事政権
軍事委員会⇒軍事政権
徴用⇒徴兵
軍事評議会軍⇒国軍
アジア道路⇒アジアハイウェイ
来客登録⇒宿泊登録
来客登録者⇒宿泊登録者
タウンシップ⇒郡区
北オークカラパ⇒北オカラッパ
北オカラパ⇒北オカラッパ
サリンギ郡区⇒タンリン郡区
ネーピードー⇒ネピドー
ファシスト国軍⇒国軍
クーデター軍⇒国軍
ミャンマー国民⇒ミャンマー人
タディンユット⇒ダディンジュ
ティティンジュット⇒ダディンジュ

【翻訳時の特別ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。
「ဖမ်းဆီး」の訳語は文脈によって使い分けること。
- 犯罪容疑や法律違反に対する文脈の場合は「逮捕」とする。
- 犯罪容疑や法律違反に基づかない文脈の場合は「拘束」とする。

【通貨換算ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。
ミャンマー通貨「チャット（Kyat、ကျပ်）」が出てきた場合は、日本円に換算して併記してください。
- 換算レートは 1チャット = 0.037円 を必ず使用すること。
- 記事中にチャットが出た場合は必ず「◯チャット（約◯円）」の形式に翻訳してください。
- 日本円の表記は小数点以下は四捨五入してください（例: 16,500円）。
- 他のレートは使用禁止。
- チャット以外の通貨（例：タイの「バーツ」や米ドルなど）には適用しない。換算は行わないこと。
`;

// タイトルの出力ルール（TITLE_OUTPUT_RULES 相当）
const TITLE_OUTPUT_RULES = `
出力は見出し文だけを1行で返してください。
【翻訳】や【日本語見出し案】、## 翻訳 などのラベル・注釈タグ・見出しは出力しないでください。
文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。
`;

// HEADLINE_PROMPT_1：原題ベースの日本語見出し（A）
const HEADLINE_PROMPT_1 = `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
あなたは報道見出しの専門翻訳者です。
以下の英語/ビルマ語のニュース見出しタイトルを、
自然で簡潔な日本語見出しに翻訳してください。
固有名詞は一般的な日本語表記を優先し、
意訳しすぎず要点を保ち、記号の乱用は避けてください。
`;

// HEADLINE_PROMPT_3：本文を読んで作る見出し（B/B’）
const HEADLINE_PROMPT_3 = `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
あなたは新聞社の見出しデスクです。
以下の本文（原文／機械翻訳含む可能性あり）を読み、
記事の要点（誰／どこ／何が起きた／規模・数値／結果／時点）を抽出し、
自然で簡潔な日本語の報道見出しを1行で作成してください。

ルール：
- 主語と動作を明確に（曖昧表現や冗長な修飾は削る）
- 重要な固有名詞・数値は優先して残す
- 「〜と述べた」「〜が行われた」等の婉曲表現は避ける
- 事実関係が曖昧な断定は避ける（必要な場合のみ推定語を最小限に使う）
`;

// make_headline_prompt_2_from 相当：案1 から案2を作るプロンプトを生成
function buildHeadlinePrompt2From_(variant1Ja) {
  return `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
以下は先に作成した日本語見出し（案1）です。
【案1】${variant1Ja}

この案1を素材に、次の要件で新しい別案（案2）を1行で出力してください。
・直訳ではなく、ニュース見出しとして自然な日本語にする
・30文字以内で要点を端的に
・主語・動作を明確に
・重複語を避ける
・報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）
・「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける
`;
}

// 本文要約タスク（Python の STEP3_TASK 相当）
const SUMMARY_TASK = `
Step 3: 翻訳と要約処理
以下のルールに従って、本文を要約してください。

${COMMON_TRANSLATION_RULES}
本文要約：
- 以下の記事本文について重要なポイントをまとめ、最大500字で具体的に要約する（500字を超えない）。
- 自然な日本語に翻訳する。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。
- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。
- レスポンスでは要約のみを返してください、それ以外の文言は不要です。

本文要約の出力条件：
- 1行目は\`【要約】\`とだけしてください。
- 2行目以降が全て空行になってはいけません。
- 見出しや箇条書きを適切に使って整理してください。
- 見出しや箇条書きにはマークダウン記号（#, *, - など）を使わず、単純なテキストとして出力してください。
- 見出しは \`[ ]\` で囲んでください。
- 空行は作らないでください。
- 特殊記号は使わないでください（全体をHTMLとして送信するわけではないため）。
- 箇条書きは\`・\`を使ってください。
- 「【要約】」は1回だけ書き、途中や本文の末尾には繰り返さないでください。
- 思考用の手順（Step 1/2/3、Q1/Q2、→ など）は出力に含めないこと。
- 本文要約の合計は最大500文字以内に収める。超えそうな場合は重要情報を優先して削る（日時・主体・行為・規模・結果を優先）。
`;

/************************************************************
 * スプレッドシート用語集
 ************************************************************/

// 正規表現用に特殊文字をエスケープ
function escapeRegExp_(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// ==== 用語集（州・管区訳）: A:mm / B:en / C:本文訳 / D:見出し訳 ====
let REGION_GLOSSARY_CACHE = null;

function loadRegionGlossary_() {
  if (REGION_GLOSSARY_CACHE) return REGION_GLOSSARY_CACHE;

  // ★必要に応じて書き換え：同じスプレッドシート内の "regions" シートを想定
  const ss = SpreadsheetApp.getActive();
  const sheetName = "regions"; // Python 側の MNA_REGION_SHEET_NAME と合わせる
  const sh = ss.getSheetByName(sheetName);
  if (!sh) {
    Logger.log("[region-glossary] sheet not found: " + sheetName);
    REGION_GLOSSARY_CACHE = [];
    return REGION_GLOSSARY_CACHE;
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    REGION_GLOSSARY_CACHE = [];
    return REGION_GLOSSARY_CACHE;
  }

  const values = sh.getRange(2, 1, lastRow - 1, 4).getValues(); // A:D
  const out = [];
  values.forEach(function (r) {
    const mm = (r[0] || "").toString().trim(); // A
    const en = (r[1] || "").toString().trim(); // B
    const jaBody = (r[2] || "").toString().trim(); // C
    const jaHead = (r[3] || "").toString().trim(); // D
    if (!mm && !en && !jaBody && !jaHead) return;
    const ja = jaHead || jaBody; // 後方互換
    out.push({
      mm: mm,
      en: en,
      ja: ja,
      ja_body: jaBody,
      ja_headline: jaHead,
    });
  });

  Logger.log("[region-glossary] loaded " + out.length + " entries");
  REGION_GLOSSARY_CACHE = out;
  return REGION_GLOSSARY_CACHE;
}

function selectRegionEntriesForText_(text, entries) {
  if (!text || !entries || !entries.length) return [];
  const t = text.toString(); // GAS の文字列

  const picked = [];
  const seen = {};

  entries.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    let hit = false;

    // Myanmar 語は単純な部分文字列マッチ（Python 側とほぼ同等）
    if (mm) {
      if (t.indexOf(mm) !== -1) {
        hit = true;
      }
    }

    // 英語は \bword\b の単語境界で大文字小文字無視
    if (!hit && en) {
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(t)) {
        hit = true;
      }
    }

    if (hit) {
      const key = mm + "|" + en;
      if (!seen[key]) {
        seen[key] = true;
        picked.push(e);
      }
    }
  });

  return picked;
}

function buildRegionGlossaryPromptFor_(entries, useHeadlineJa) {
  if (!entries || !entries.length) return "";

  const lines = [];
  entries.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    const ja = useHeadlineJa ? e.ja_headline || e.ja : e.ja_body || e.ja;
    if (!ja) return;

    if (mm && en) {
      lines.push(
        "- 「" +
          mm +
          "」または「" +
          en +
          "」が出たら、必ず「" +
          ja +
          "」と訳す。"
      );
    } else if (mm) {
      lines.push("- 「" + mm + "」が出たら、必ず「" + ja + "」と訳す。");
    } else if (en) {
      lines.push("- 「" + en + "」が出たら、必ず「" + ja + "」と訳す。");
    }
  });

  if (!lines.length) return "";
  return "【用語固定（必須）】\n" + lines.join("\n") + "\n";
}

// タイトル用（D列=見出し訳）
function buildRegionRulesForTitle_(title) {
  const entries = selectRegionEntriesForText_(
    title || "",
    loadRegionGlossary_()
  );
  return buildRegionGlossaryPromptFor_(entries, true);
}

// 本文用（C列=本文訳）
function buildRegionRulesForBody_(body) {
  const entries = selectRegionEntriesForText_(
    body || "",
    loadRegionGlossary_()
  );
  return buildRegionGlossaryPromptFor_(entries, false);
}

/************************************************************
 * 1. メディアごとの API キー切り替え（prod / dev）
 ************************************************************/

// メディア名 → ベースキー名（末尾）のマップ
// 例: "mizzima" → "MIZZIMA"
// prod: GEMINI_API_KEY_MIZZIMA
// dev : GEMINI_API_TEST_KEY_MIZZIMA
const SOURCE_KEY_BASE_MAP = {
  bbc: "BBC",
  "bbc burmese": "BBC",
  mizzima: "MIZZIMA",
  "mizzima burmese": "MIZZIMA",
  "mizzima (burmese)": "MIZZIMA",
  "khit thit": "KHITTHIT",
  "khit thit media": "KHITTHIT",
  "myanmar now": "MYANMARNOW",
  dvb: "DVB",
  irrawaddy: "IRRAWADDY",
};

const DEFAULT_BASE_KEY = "MIZZIMA"; // マップにない場合のフォールバック

// シート名ごとのプレフィックス
const SHEET_KEY_PREFIX_MAP = {
  prod: "GEMINI_API_KEY_", // 例: GEMINI_API_KEY_MIZZIMA
  dev: "GEMINI_API_TEST_KEY_", // 例: GEMINI_API_TEST_KEY_MIZZIMA
};

const DEFAULT_PREFIX = "GEMINI_API_KEY_"; // prod/dev以外のシート用

function normalizeSourceName_(s) {
  if (!s) return "";
  let out = s.toString().trim();
  try {
    out = out.normalize("NFKC");
  } catch (e) {}
  out = out.replace(/\s+/g, " ");
  return out.toLowerCase();
}

// シート名 & メディア名から API キーを取得
function getApiKeyFromSheetAndSource_(sheetName, sourceRaw) {
  const scriptProps = PropertiesService.getScriptProperties();

  const prefix = SHEET_KEY_PREFIX_MAP[sheetName] || DEFAULT_PREFIX;

  const norm = normalizeSourceName_(sourceRaw || "");
  const baseKey = SOURCE_KEY_BASE_MAP[norm] || DEFAULT_BASE_KEY;

  const propName = prefix + baseKey;
  const apiKey = scriptProps.getProperty(propName);
  return apiKey || null;
}

/************************************************************
 * 2. Gemini 呼び出し共通
 ************************************************************/

function callGeminiWithKey_(apiKey, prompt) {
  if (!apiKey) {
    return "ERROR: API key not set";
  }

  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent" +
    "?key=" +
    encodeURIComponent(apiKey);

  const payload = {
    contents: [
      {
        parts: [{ text: prompt }],
      },
    ],
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  const res = UrlFetchApp.fetch(url, options);
  const text = res.getContentText();

  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    return "ERROR: invalid JSON: " + text;
  }

  try {
    return (data.candidates[0].content.parts[0].text || "").trim();
  } catch (e) {
    return "ERROR: " + text;
  }
}

/************************************************************
 * 3. 1行分の処理
 *
 * 入力:
 *   - C列: メディア
 *   - M列: タイトル原文
 *   - N列: 本文原文
 *   - J列: URL（あれば）
 *
 * 出力:
 *   - E列: HEADLINE_PROMPT_1(タイトル)         → 見出しA
 *   - F列: make_headline_prompt_2_from(E)       → 見出しA'
 *   - G列: HEADLINE_PROMPT_3(本文のみ)         → 見出しB'
 *   - I列: 本文要約（STEP3_TASK）
 ************************************************************/

function processRow_(sheet, row) {
  const colC = 3; // メディア
  const colM = 13; // タイトル原文
  const colN = 14; // 本文原文

  const colE = 5; // 見出しA
  const colF = 6; // 見出しA'
  const colG = 7; // 見出しB'（本文のみ）
  const colI = 9; // 本文要約（STEP3_TASK）
  const colJ = 10; // URL（任意）

  const sourceVal = sheet.getRange(row, colC).getValue();
  const titleRaw = sheet.getRange(row, colM).getValue();
  const bodyRaw = sheet.getRange(row, colN).getValue();
  const urlVal = sheet.getRange(row, colJ).getValue();

  if (!titleRaw && !bodyRaw) {
    // 何もなければ何もしない
    return;
  }

  // === ここで行ごとの用語固定ルールを作る ===
  // タイトルに出た語 → D列（見出し訳）を採用
  const regionRulesTitle = buildRegionRulesForTitle_(titleRaw || "");
  // 本文に出た語 → C列（本文訳）を採用
  const regionRulesBody = buildRegionRulesForBody_(bodyRaw || "");

  // タイトル翻訳用（見出しA/A'）
  const titleGlossaryRules = regionRulesTitle;

  // 本文を読む見出し・要約用（タイトル＋本文両方を対象）
  const bodyGlossaryRules = regionRulesTitle + regionRulesBody;

  const sheetName = sheet.getName();
  const apiKey = getApiKeyFromSheetAndSource_(sheetName, sourceVal);

  /***************
   * E列：見出しA (HEADLINE_PROMPT_1)
   ***************/
  let headlineA = "";
  if (titleRaw) {
    const prompt1 =
      HEADLINE_PROMPT_1 +
      "\n\n" +
      titleGlossaryRules +
      "\n原題: " +
      titleRaw +
      "\nsource: " +
      (sourceVal || "") +
      "\nurl: " +
      (urlVal || "") +
      "\n";

    headlineA = callGeminiWithKey_(apiKey, prompt1);
    sheet.getRange(row, colE).setValue(headlineA);
  } else {
    sheet.getRange(row, colE).setValue("");
  }

  /***************
   * F列：見出しA' (make_headline_prompt_2_from)
   ***************/
  if (headlineA) {
    const prompt2 = buildHeadlinePrompt2From_(headlineA);
    const headlineA2 = callGeminiWithKey_(apiKey, prompt2);
    sheet.getRange(row, colF).setValue(headlineA2);
  } else {
    sheet.getRange(row, colF).setValue("");
  }

  /********************************************
   * G列：見出しB' (HEADLINE_PROMPT_3 with body only)
   ********************************************/
  if (bodyRaw) {
    const bodyOnlyBlock = "【本文】\n" + bodyRaw + "\n";

    const prompt3 =
      HEADLINE_PROMPT_3 + "\n\n" + bodyGlossaryRules + "\n" + bodyOnlyBlock;

    const headlineB2 = callGeminiWithKey_(apiKey, prompt3);
    sheet.getRange(row, colG).setValue(headlineB2);
  } else {
    sheet.getRange(row, colG).setValue("");
  }

  /********************************************
   * I列：本文要約（STEP3_TASK）
   ********************************************/
  if (bodyRaw) {
    const summaryInput =
      "入力データ：\n" +
      "###\n[記事タイトル]\n###\n" +
      (titleRaw || "") +
      "\n\n" +
      "[記事本文]\n###\n" +
      (bodyRaw || "") +
      "\n###\n";

    const summaryPrompt =
      SUMMARY_TASK + "\n\n" + bodyGlossaryRules + "\n" + summaryInput;

    const summaryJa = callGeminiWithKey_(apiKey, summaryPrompt);
    sheet.getRange(row, colI).setValue(summaryJa);
  } else {
    sheet.getRange(row, colI).setValue("");
  }
}

/************************************************************
 * 4. onEditHead トリガー
 *   - M列 or N列 が編集されたとき、その行の E〜G を再計算
 ************************************************************/

function onEditHead(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const row = range.getRow();
  const col = range.getColumn();

  if (row === 1) return; // ヘッダー行は無視

  // M列(13) or N列(14) が編集されたときだけ実行
  if (col === 13 || col === 14) {
    processRow_(sheet, row);
  }
}

function manualInit() {
  const dummyApiKey = "DUMMY";
  const prompt = "test";
  try {
    callGeminiWithKey_(dummyApiKey, prompt);
  } catch (e) {
    Logger.log(e);
  }
}
