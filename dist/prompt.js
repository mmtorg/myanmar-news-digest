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
- 換算レートは 1チャット = 0.039円 を必ず使用すること。
- 記事中にチャットが出た場合は必ず「◯チャット（約◯円）」の形式に翻訳してください。
- 日本円の表記は小数点以下は四捨五入してください（例: 16,500円）。
- 他のレートは使用禁止。
- チャット以外の通貨（例：タイの「バーツ」や米ドルなど）には適用しない。換算は行わないこと。

【ミャンマー語の数詞・単位ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。

■ 共通ルール
ミャンマー語の金額単位は、以下のように「数字の前後どちらに置かれてもよい」ものとして扱うこと。
また、数字と単位の間にスペースがある／ないの両方を許容すること。
- 例： 「၅၀ သိန်း」「သိန်း ၅၀」「၅၀သိန်း」「သိန်း၅၀」は、すべて同じ意味として扱う。

--------------------------------
■ 1) 「သိန်း」：10万チャットの単位
--------------------------------
- ミャンマー語の「သိန်း」は 100,000（10万）チャットを表す単位である。
- 「数字 + သိန်း」「သိန်း + 数字」のどちらの並びも有効とみなす。
- 数字と「သိန်း」の間にスペースがない場合も同様に解釈する。

＜解釈ルール＞
- 「◯◯ သိန်း」「သိန်း ◯◯」「◯◯သိန်း」「သိန်း◯◯」
  → いずれも「◯◯ × 10万チャット」と解釈する。

＜例＞
- 「သိန်း ၅၀」「၅၀ သိန်း」「၅၀သိန်း」「သိန်း၅၀」
  → 50 × 10万 ＝ 5,000,000 チャット（500万チャット）
- 「သိန်း ၃၀၀၀」「၃၀၀၀ သိန်း」「၃၀၀၀သိန်း」「သိန်း٣၀၀၀」
  → 3,000 × 10万 ＝ 300,000,000 チャット（3億チャット）

--------------------------------
■ 2) 「သန်း」：100万チャット（100万）の単位
--------------------------------
- ミャンマー語の「သန်း」は 1,000,000（100万）チャットを表す単位である。
- 「数字 + သန်း」「သန်း + 数字」のどちらの並びも有効とみなす。
- 数字と「သန်း」の間にスペースがない場合も同様に解釈する。

＜解釈ルール＞
- 「◯◯ သန်း」「သန်း ◯◯」「◯◯သန်း」「သန်း◯◯」
  → いずれも「◯◯ × 100万チャット」と解釈する。

＜例＞
- 「သန်း ၅၀」「၅၀ သန်း」「၅၀သန်း」「သန်း၅０」
  → 50 × 100万 ＝ 50,000,000 チャット（5,000万チャット）
- 「သန်း ၃၀၀၀」「၃၀၀၀ သန်း」「၃၀၀၀သန်း」「သန်း٣၀၀၀」
  → 3,000 × 100万 ＝ 3,000,000,000 チャット（30億チャット）

--------------------------------
■ 3) 語尾が付く場合の扱い
--------------------------------
以下のような語尾がついていても、前述のルールで数値部分と単位を認識し、金額を解釈すること。
- လောက်（〜くらい）
- ကျော်（〜超）
- ခန့်（およそ）

＜例＞
- 「သိန်း ၅၀ ကျော်」「၅၀သိန်းလောက်」など
  → まず「၅０ × 10万チャット」として解釈し、その上で「〜超」「〜くらい」といったニュアンスを日本語に反映する。
- 「သန်း ၅０ လောက်」「၅０သန်းခန့်」など
  → まず「၅０ × 100万チャット」として解釈し、その上で「〜くらい」「およそ」といったニュアンスを日本語に反映する。
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
【追加要件】
- 直訳ではなく、ニュース見出しとして自然な日本語にする
- 30文字以内で要点を端的に
- 主語・動作を明確に
- 重複語を避ける
- 報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）
- 「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける
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

// 3タスク（見出しA / 見出しB' / 本文要約）を1回で投げるまとめプロンプト
function buildMultiTaskPromptForRow_(params) {
  const {
    titleRaw,
    bodyRaw,
    sourceVal,
    urlVal,
    titleGlossaryRules,
    bodyGlossaryRules,
  } = params;

  return `
以下は1つのニュース記事です。
あなたはこの1記事から、次の3つの結果を同時に生成してください。

[記事タイトル]
${titleRaw || ""}

[記事本文]
${bodyRaw || ""}

====================
[Task1: 見出しA（タイトル翻訳ベース）]
- 記事タイトルをインプットとしてください。
- 次のプロンプトとルールに従って、日本語見出しAを1行で作成してください。
--- HEADLINE_PROMPT_1 ---
${HEADLINE_PROMPT_1}
-------------------------
【タイトル用 用語固定ルール】
${titleGlossaryRules || "(なし)"}

====================
[Task2: 見出しB'（本文を読んで作る見出し）]
- 記事本文をインプットとしてください。
- 次のプロンプトとルールに従って、日本語見出しB'を1行で作成してください。
--- HEADLINE_PROMPT_3 ---
${HEADLINE_PROMPT_3}
---------------------------
【本文用 用語固定ルール】
${bodyGlossaryRules || "(なし)"}
本文を主な根拠としつつ、必要であればタイトルも補助情報として用いて構いません。

====================
[Task3: 本文要約]
- 記事本文をインプットとしてください。
- 次のプロンプトとルールに従って、本文要約を作成してください。
--- SUMMARY_TASK ---
${SUMMARY_TASK}
--------------------
【本文用 用語固定ルール】
${bodyGlossaryRules || "(なし)"}

====================
【最終出力フォーマット（必須）】

3つのタスク結果だけを含む JSON オブジェクトを 1 つだけ出力してください。

{
  "headlineA": "ここにTask1の見出しAを入れる",
  "headlineBPrime": "ここにTask2の見出しB'を入れる",
  "summary": "ここにTask3の本文要約を入れる"
}

制約:
- 上記の JSON オブジェクト以外の文字（解説・ラベル・マークダウンなど）は一切出力しないこと。
- 特に、\`\`\`json 〜 \`\`\` のようなコードブロックで囲まず
  純粋な JSON テキストのみを出力すること。
- JSON 全体としては 1 つのオブジェクトだけを出力すればよい。summary の値の中では、
  「【要約】」「[見出し]」「・箇条書き」などのために改行（\n）を自由に使ってよい。
`;
}

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
  "global new light of myanmar": "GNLM",
  "global new light": "GNLM",
  gnlm: "GNLM",
  "global new light of myanmar (国営紙)": "GNLM",
  "news eleven": "NEWSELEVEN",
  "news eleven burmese": "NEWSELEVEN",
  newseleven: "NEWSELEVEN",
  "popular myanmar": "POPULARMYANMAR",
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
function getApiKeyFromSheetAndSource_(sheetName, sourceRaw, usageTagOpt) {
  const scriptProps = PropertiesService.getScriptProperties();

  const prefix = SHEET_KEY_PREFIX_MAP[sheetName] || DEFAULT_PREFIX;

  const norm = normalizeSourceName_(sourceRaw || "");
  const baseKey = SOURCE_KEY_BASE_MAP[norm] || DEFAULT_BASE_KEY;

  const propName = prefix + baseKey;
  const apiKey = scriptProps.getProperty(propName);

  // ★ ここで「どのキー名を使ったか」をログ出力（値そのものは出さない）
  const tag = usageTagOpt || sheetName || "unknown";
  const msg =
    "use apiKeyProp=" +
    propName +
    " (baseKey=" +
    baseKey +
    ", sourceRaw=" +
    sourceRaw +
    ", norm=" +
    norm +
    ")";

  Logger.log("[gemini-key] " + msg);
  _appendGeminiLog_("INFO", tag, msg);

  return apiKey || null;
}

/************************************************************
 * Gemini 共通設定（リトライ＆ログ）
 ************************************************************/

// リトライ設定（少し控えめに）
const GEMINI_JS_MAX_RETRIES = 2; // 3 → 2
const GEMINI_JS_BASE_DELAY_SEC = 8; // 5 → 8
const GEMINI_JS_MAX_DELAY_SEC = 90; // 60 → 90

// 乱数ジッター付き指数バックオフ: attempt=0,1,2,... → 待機ミリ秒
function _expBackoffMs_(attempt) {
  const baseMs = GEMINI_JS_BASE_DELAY_SEC * 1000;
  const maxMs = GEMINI_JS_MAX_DELAY_SEC * 1000;
  let delay = Math.min(maxMs, Math.pow(2, attempt) * baseMs); // 2^attempt * base
  delay += Math.floor(Math.random() * 1000); // 0〜999ms のジッター
  return delay;
}

// Gemini RESTレスポンスから usage を取り出す（snake/camel両対応）
function _usageFromData_(data) {
  if (!data) return null;
  const usage = data.usageMetadata || data.usage_metadata;
  if (!usage) return null;

  function _get(obj, key, fallback) {
    return obj && key in obj ? obj[key] : fallback;
  }

  const promptTokens = _get(
    usage,
    "prompt_token_count",
    _get(usage, "input_token_count", _get(usage, "input_tokens", 0))
  );
  const candTokens = _get(
    usage,
    "candidates_token_count",
    _get(usage, "output_token_count", _get(usage, "output_tokens", 0))
  );
  const totalTokens = _get(
    usage,
    "total_token_count",
    _get(usage, "total_tokens", (promptTokens || 0) + (candTokens || 0))
  );
  const cacheCreate = _get(usage, "cache_creation_input_token_count", 0);
  const cacheRead = _get(usage, "cache_read_input_token_count", 0);

  return {
    prompt_token_count: promptTokens || 0,
    candidates_token_count: candTokens || 0,
    total_token_count: totalTokens || 0,
    cache_creation_input_token_count: cacheCreate || 0,
    cache_read_input_token_count: cacheRead || 0,
  };
}

// usage ログ（標準出力＝Apps Script 実行ログ）
function _logGeminiUsage_(data, usageTag, model) {
  const u = _usageFromData_(data);
  if (!u) return;
  const tag = usageTag || "gen";
  const m = model || "gemini-2.5-flash";
  Logger.log(
    "📊 TOKENS[%s] in=%s out=%s total=%s (cache create/read=%s/%s) model=%s",
    tag,
    u.prompt_token_count,
    u.candidates_token_count,
    u.total_token_count,
    u.cache_creation_input_token_count,
    u.cache_read_input_token_count,
    m
  );
}

// Free tier の「generate_content_free_tier_requests」系 429 を判定
function _isFreeTierQuotaErrorData_(data) {
  try {
    const err = data && data.error;
    if (!err) return false;
    const msg = (err.message || "").toString();
    return msg.indexOf("generate_content_free_tier_requests") !== -1;
  } catch (e) {
    return false;
  }
}

// 503/429 などリトライ対象かどうか判定（HTTPコード + エラー内容から）
function _isRetriableError_(httpCode, data) {
  const err = data && data.error;
  const status = err && err.status ? String(err.status) : "";
  const msg = err && err.message ? String(err.message) : "";

  if (httpCode === 503 || httpCode === 500) return true;
  if (httpCode === 429) return true;

  const lower = (status + " " + msg).toLowerCase();
  const hints = [
    "unavailable",
    "resource_exhausted",
    "timeout",
    "temporar",
    "overload",
    "server error",
    "internal",
  ];
  return hints.some(function (h) {
    return lower.indexOf(h) !== -1;
  });
}

/************************************************************
 * Gemini 呼び出しログ用シート出力
 ************************************************************/

const GEMINI_LOG_SHEET_NAME_PROD = "gemini_logs_prod";
const GEMINI_LOG_SHEET_NAME_DEV = "gemini_logs_dev";

// usageTag からどのログシートに書くか判定する
// 例: "prod#row5:E(...)" → "gemini_logs_prod"
//     "dev#row10:I(...)" → "gemini_logs_dev"
function _getLogSheetNameForTag_(tag) {
  if (!tag) return null;
  const s = String(tag);
  const sharpIndex = s.indexOf("#");
  const head = sharpIndex >= 0 ? s.substring(0, sharpIndex) : s;

  if (head === "prod") return GEMINI_LOG_SHEET_NAME_PROD;
  if (head === "dev") return GEMINI_LOG_SHEET_NAME_DEV;

  // prod/dev 以外（manualInit など）はログを残さない
  return null;
}

// 実際にログシートに1行追加する
function _appendGeminiLog_(level, tag, message) {
  try {
    const logSheetName = _getLogSheetNameForTag_(tag);
    if (!logSheetName) {
      // prod/dev 以外のタグは無視
      return;
    }

    const ss = SpreadsheetApp.getActive();
    let sh = ss.getSheetByName(logSheetName);

    // 初回のみシート作成
    if (!sh) {
      sh = ss.insertSheet(logSheetName);
    }

    sh.appendRow([new Date(), level || "", tag || "", message || ""]);
  } catch (e) {
    // ログ書き込み失敗は本体処理に影響させない
    Logger.log("[gemini-log] failed to append log: " + e);
  }
}

/************************************************************
 * 2. Gemini 呼び出し共通
 ************************************************************/

function callGeminiWithKey_(apiKey, prompt, usageTagOpt) {
  if (!apiKey) {
    Logger.log("[gemini] ERROR: API key not set");
    return "ERROR: API key not set";
  }

  const usageTag = usageTagOpt || "generic";
  const model = "gemini-2.5-flash";
  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/" +
    model +
    ":generateContent?key=" +
    encodeURIComponent(apiKey);

  const payload = {
    contents: [
      {
        parts: [{ text: prompt }],
      },
    ],
    generationConfig: {
      response_mime_type: "application/json",
      // 必要なら温度も指定
      // temperature: 0.1,
    },
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true, // 非200もレスポンスを返させる
  };

  let lastErrorText = "";

  for (let attempt = 0; attempt < GEMINI_JS_MAX_RETRIES; attempt++) {
    Logger.log(
      "[gemini-call] try %s/%s tag=%s model=%s prompt_chars=%s",
      attempt + 1,
      GEMINI_JS_MAX_RETRIES,
      usageTag,
      model,
      (prompt || "").length
    );

    _appendGeminiLog_(
      "INFO",
      usageTag,
      "try " +
        (attempt + 1) +
        "/" +
        GEMINI_JS_MAX_RETRIES +
        " model=" +
        model +
        " prompt_chars=" +
        (prompt || "").length
    );

    let res;
    try {
      res = UrlFetchApp.fetch(url, options);
    } catch (e) {
      // ネットワーク例外など
      lastErrorText = (e && e.toString()) || "fetch error";
      Logger.log("[gemini] fetch exception: %s", lastErrorText);

      _appendGeminiLog_("ERROR", usageTag, "fetch exception: " + lastErrorText);

      if (attempt === GEMINI_JS_MAX_RETRIES - 1) {
        return "ERROR: " + lastErrorText;
      }

      const sleepMs = _expBackoffMs_(attempt);
      Logger.log(
        "[gemini] retry after %sms (fetch exception, attempt=%s)",
        sleepMs,
        attempt + 1
      );

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "retry after " +
          sleepMs +
          "ms (fetch exception, attempt=" +
          (attempt + 1) +
          ")"
      );

      Utilities.sleep(sleepMs);
      continue;
    }

    const code = res.getResponseCode();
    const text = res.getContentText();

    let data;
    try {
      data = JSON.parse(text);
    } catch (e) {
      lastErrorText = "invalid JSON: " + text.substring(0, 500);
      Logger.log("[gemini] invalid JSON (code=%s): %s", code, lastErrorText);

      if (attempt === GEMINI_JS_MAX_RETRIES - 1) {
        return "ERROR: " + lastErrorText;
      }

      const sleepMs = _expBackoffMs_(attempt);
      Utilities.sleep(sleepMs);
      continue;
    }

    // Free tier の日次上限エラーは即諦める
    if (_isFreeTierQuotaErrorData_(data)) {
      const errMsg =
        (data.error && data.error.message) ||
        "free tier quota exceeded (generate_content_free_tier_requests)";
      Logger.log("🚫 [gemini] free tier quota exceeded: %s", errMsg);

      _appendGeminiLog_(
        "ERROR",
        usageTag,
        "free tier quota exceeded: " + errMsg
      );

      return "ERROR: " + errMsg;
    }

    // 2xx かつ error 無し → 成功とみなす
    if (code >= 200 && code < 300 && !(data && data.error)) {
      try {
        // usage ログ
        _logGeminiUsage_(data, usageTag, model);
      } catch (e) {
        // usage ログ失敗は致命的ではないので無視
      }

      let out = "";
      try {
        out =
          (((data.candidates || [])[0] || {}).content.parts || [])[0].text ||
          "";
      } catch (e) {
        out = "";
      }
      out = (out || "").trim();

      Logger.log(
        "[gemini] success tag=%s model=%s len(resp)=%s",
        usageTag,
        model,
        out.length
      );

      _appendGeminiLog_(
        "SUCCESS",
        usageTag,
        "success model=" + model + " len(resp)=" + out.length
      );

      return out;
    }

    // error オブジェクトがあれば詳細ログ
    if (data && data.error) {
      const err = data.error;
      const status = String(err.status || "");
      const message = String(err.message || "");
      Logger.log(
        "[gemini] HTTP %s error status=%s message=%s",
        code,
        status,
        message
      );
      lastErrorText = message || "HTTP " + code;

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "HTTP " + code + " error status=" + status + " message=" + message
      );
    } else {
      Logger.log("[gemini] HTTP %s unexpected response body: %s", code, text);
      lastErrorText = "HTTP " + code;

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "HTTP " + code + " unexpected response: " + text.substring(0, 200)
      );
    }

    // リトライ対象か判定
    const retriable = _isRetriableError_(code, data);
    if (!retriable || attempt === GEMINI_JS_MAX_RETRIES - 1) {
      Logger.log(
        "[gemini] give up (retriable=%s): %s",
        retriable,
        lastErrorText
      );

      _appendGeminiLog_(
        "ERROR",
        usageTag,
        "give up (retriable=" + retriable + "): " + lastErrorText
      );

      return "ERROR: " + lastErrorText;
    }

    const sleepMs = _expBackoffMs_(attempt);
    Logger.log(
      "⚠️ [gemini] retry %s/%s after %sms (HTTP %s)",
      attempt + 1,
      GEMINI_JS_MAX_RETRIES,
      sleepMs,
      code
    );

    _appendGeminiLog_(
      "WARN",
      usageTag,
      "retry " +
        (attempt + 1) +
        "/" +
        GEMINI_JS_MAX_RETRIES +
        " after " +
        sleepMs +
        "ms (HTTP " +
        code +
        ")"
    );

    Utilities.sleep(sleepMs);
  }

  // ここまで来ることはほぼ無い想定
  return "ERROR: " + (lastErrorText || "Gemini call failed");
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

function processRow_(sheet, row, prevStatus) {
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

  /********************************************
   * E / G / I を 1回の Gemini 呼び出しでまとめて生成
   ********************************************/
  let headlineA = "";
  let headlineB2 = "";
  let summaryJa = "";

  if (titleRaw || bodyRaw) {
    const multiParams = {
      titleRaw: titleRaw || "",
      bodyRaw: bodyRaw || "",
      sourceVal: sourceVal || "",
      urlVal: urlVal || "",
      titleGlossaryRules: titleGlossaryRules || "",
      bodyGlossaryRules: bodyGlossaryRules || "",
    };

    const multiPrompt = buildMultiTaskPromptForRow_(multiParams);
    const tagMulti = sheetName + "#row" + row + ":EGI(multi)";

    // ★ tagMulti を渡して APIキー名ログも紐付ける
    const apiKey = getApiKeyFromSheetAndSource_(sheetName, sourceVal, tagMulti);

    const resp = callGeminiWithKey_(apiKey, multiPrompt, tagMulti);

    if (typeof resp === "string" && resp.indexOf("ERROR:") === 0) {
      // callGeminiWithKey_ 自体がエラーを返した場合 → そのまま3列とも同じエラー扱い
      headlineA = resp;
      headlineB2 = resp;
      summaryJa = resp;
    } else {
      try {
        let cleaned = (resp || "").trim();

        // もし ``` で始まっていたら、コードブロックを剥がす
        if (cleaned.startsWith("```")) {
          // 先頭の ```json / ``` を削除
          cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
          // 最後の ``` 以降を削る
          const lastFence = cleaned.lastIndexOf("```");
          if (lastFence !== -1) {
            cleaned = cleaned.substring(0, lastFence);
          }
          cleaned = cleaned.trim();
        }

        const obj = JSON.parse(cleaned);

        headlineA = (obj.headlineA || "").toString().trim();
        headlineB2 = (obj.headlineBPrime || obj.headlineB || "")
          .toString()
          .trim();
        summaryJa = (obj.summary || "").toString().trim();

        headlineA = headlineA || "";
        headlineB2 = headlineB2 || "";
        summaryJa = summaryJa || "";
      } catch (e) {
        const errMsg =
          "ERROR: invalid JSON from Gemini: " + String(resp).substring(0, 200);
        headlineA = errMsg;
        headlineB2 = errMsg;
        summaryJa = errMsg;
      }
    }
  }

  // ★ここで地域名ログを出す
  logRegionUsageForRow_(sheet, row, {
    sourceVal,
    urlVal,
    titleRaw,
    bodyRaw,
    headlineA,
    headlineB2,
    summaryJa,
  });

  // シートに書き込み
  sheet.getRange(row, colE).setValue(headlineA); // 見出しA
  // F列（見出しA'）は従来どおり一時停止のまま
  sheet.getRange(row, colG).setValue(headlineB2); // 見出しB'
  sheet.getRange(row, colI).setValue(summaryJa); // 本文要約

  /********************************************
   * L列：ステータス判定（詳細エラー + 複数記録）
   ********************************************/
  const colL = 12;

  function isError_(val) {
    return typeof val === "string" && val.indexOf("ERROR:") === 0;
  }

  // タイトルも本文も無い場合は EMPTY
  if (!titleRaw && !bodyRaw) {
    sheet.getRange(row, colL).setValue("EMPTY");
    return;
  }

  const vE = sheet.getRange(row, colE).getValue();
  // F列（案2）は一時停止中のためステータス判定対象から外す
  // const vF = sheet.getRange(row, colF).getValue();
  const vG = sheet.getRange(row, colG).getValue();
  const vI = sheet.getRange(row, colI).getValue();

  const errors = [];

  if (isError_(vE)) {
    errors.push("E=" + String(vE));
  }
  // if (isError_(vF)) {
  //   errors.push("F=" + String(vF));
  // }
  if (isError_(vG)) {
    errors.push("G=" + String(vG));
  }
  if (isError_(vI)) {
    errors.push("I=" + String(vI));
  }

  let statusText = "";

  if (errors.length === 0) {
    statusText = "OK";
  } else {
    // 呼び出し元から渡された「前回までのステータス」から回数を計算
    const prevCount = parseRetryCount_(prevStatus || "");
    const newCount = prevCount + 1;

    statusText = `NG(${newCount}): ` + errors.join(" / ");
  }

  sheet.getRange(row, colL).setValue(statusText);
}

/************************************************************
 * 4. トリガー
 *   - M列 or N列 が編集されたとき、その行の E〜G を再計算
 ************************************************************/

// prod / dev シート用のログシート内容をクリア
function _clearLogSheetFor_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  let logSheetName = null;

  if (sheetName === "prod") {
    logSheetName = GEMINI_LOG_SHEET_NAME_PROD;
  } else if (sheetName === "dev") {
    logSheetName = GEMINI_LOG_SHEET_NAME_DEV;
  } else {
    return; // 対象外
  }

  const sh = ss.getSheetByName(logSheetName);
  if (!sh) return;

  // ★シート全体の中身をクリア（ヘッダーも残さない）
  sh.clearContents();
  // ※書式も消したければ sh.clear(); に変更
}

// ミャンマー時間 16:00〜翌 2:30 の間だけ true を返す
function isWithinProcessingWindow_() {
  // appsscript.json の timeZone が "Asia/Yangon" になっている前提
  const now = new Date();
  const h = now.getHours(); // 0〜23
  const m = now.getMinutes(); // 0〜59
  const t = h * 60 + m; // その日の 0:00 からの経過分数

  const START = 16 * 60; // 16:00 → 960 分
  const END = 2 * 60 + 30; // 02:30 → 150 分

  // 日付をまたぐウィンドウの判定:
  // 16:00〜24:00 か 0:00〜2:30 のどちらかなら OK
  return t >= START || t <= END;
}

// "NG(3): xxxx" のような形式から試行回数を取り出す
function parseRetryCount_(status) {
  if (!status) return 0;
  const m = status.match(/^NG\((\d+)\)/);
  if (!m) return 0;
  return Number(m[1]);
}

// ★ 古い RUNNING ステータスを NG(1): timeout に置き換える
function cleanupStaleRunningStatuses_() {
  const ss = SpreadsheetApp.getActive();
  const sheetNames = ["prod", "dev"]; // 対象シート

  sheetNames.forEach(function (sheetName) {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) return;

    const lastRow = sh.getLastRow();
    if (lastRow < 2) return; // データ行なし

    const numRows = lastRow - 1;
    const statusRange = sh.getRange(2, STATUS_COL, numRows, 1); // L列
    const values = statusRange.getValues();

    let changed = false;

    for (let i = 0; i < numRows; i++) {
      const status = (values[i][0] || "").toString();

      // 前回実行で RUNNING のまま残った行とみなす
      if (status.startsWith("RUNNING")) {
        // 1回目の失敗として扱う
        values[i][0] = "NG(1): timeout";
        changed = true;
      }
    }

    if (changed) {
      statusRange.setValues(values);
      Logger.log(
        "[cleanupStaleRunningStatuses_] sheet=%s cleaned RUNNING rows",
        sheetName
      );
    }
  });
}

// pythonで操作した時にも動く
const MAX_ROWS_PER_RUN = 5; // 1回の実行で処理する最大行数
const STATUS_COL = 12; // L列 (ステータス列の列番号)

// NG の最大試行回数（これ以上失敗したら「打ち切り完了」とみなす）
const MAX_RETRY_COUNT = 3;

/************************************************************
 * メール通知用設定
 ************************************************************/

// 送信先アドレス
function getNotifyEmailListForSheet_(sheetName) {
  const props = PropertiesService.getScriptProperties();

  let raw = "";
  if (sheetName === "prod") {
    raw = props.getProperty("NOTIFY_EMAIL_TO_PROD") || "";
  } else if (sheetName === "dev") {
    raw = props.getProperty("NOTIFY_EMAIL_TO_DEV") || "";
  }

  // 空なら空配列を返す
  if (!raw) return [];

  // カンマ区切り → トリム → 空要素除去
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

// ★件名のベース
const NOTIFY_EMAIL_SUBJECT_BASE = "【MNA 記事収集完了】";

// ★ 統合版：この関数だけを時間トリガーで動かす
function processRowsBatch() {
  const lock = LockService.getDocumentLock();
  try {
    // 同時実行防止
    lock.waitLock(30 * 1000);

    // ★ ここで「前回の RUNNING」を NG に戻す
    cleanupStaleRunningStatuses_();

    // ★ 時間帯外なら即スキップ（16:00〜翌2:30だけ動かす）
    if (!isWithinProcessingWindow_()) {
      Logger.log("[processRowsBatch] outside allowed time window → skip");
      return;
    }

    const ss = SpreadsheetApp.getActive();
    const sheetNames = ["prod", "dev"]; // 対象シート
    let remaining = MAX_ROWS_PER_RUN; // 1回の実行で処理する最大行数（既存の定数）

    for (let s = 0; s < sheetNames.length; s++) {
      if (remaining <= 0) break;

      const sheetName = sheetNames[s];
      const sh = ss.getSheetByName(sheetName);
      if (!sh) continue;

      const lastRow = sh.getLastRow();
      if (lastRow < 2) continue; // データ行なし

      const startRow = 2;
      const numRows = lastRow - 1;
      const numCols = 14; // A〜N まで読む
      const values = sh.getRange(startRow, 1, numRows, numCols).getValues();

      for (let i = 0; i < numRows; i++) {
        if (remaining <= 0) break;

        const rowIndex = startRow + i;
        const row = values[i];

        const titleRaw = row[13 - 1]; // M列 (13)
        const bodyRaw = row[14 - 1]; // N列 (14)
        const status = (row[STATUS_COL - 1] || "").toString(); // L列 (STATUS_COL=12)

        // タイトルも本文も空 → 処理不要
        if (!titleRaw && !bodyRaw) {
          continue;
        }

        // すでに成功している行はスキップ
        if (status.startsWith("OK")) {
          continue;
        }

        // 前回 RUNNING のまま残っている行は、とりあえずスキップ
        // （タイムアウトで止まっている可能性もあるが、安全寄り）
        if (status.startsWith("RUNNING")) {
          continue;
        }

        // ★ NG の再試行回数チェック
        const retryCount = parseRetryCount_(status);

        // すでに MAX_RETRY_COUNT 回以上失敗している行は再試行しない
        if (retryCount >= MAX_RETRY_COUNT) {
          Logger.log(
            "[processRowsBatch] skip row %s (retryCount=%s >= %s)",
            rowIndex,
            retryCount,
            MAX_RETRY_COUNT
          );
          continue;
        }

        // ここまで来たら「未処理 or 失敗 or PENDING」なので処理対象
        Logger.log(
          "[processRowsBatch] processing %s row %s (status=%s)",
          sheetName,
          rowIndex,
          status
        );

        // この行の「処理前ステータス」を保持（NG(1) など）
        const prevStatus = status;

        // 処理開始マーク
        sh.getRange(rowIndex, STATUS_COL).setValue("RUNNING");

        // 実際の1行処理（prevStatus を渡す）
        processRow_(sh, rowIndex, prevStatus);

        remaining--;
      }
    }

    Logger.log(
      "[processRowsBatch] done, processed rows=%s",
      MAX_ROWS_PER_RUN - remaining
    );

    // ★ prod / dev それぞれについて、完了していればメール通知
    checkAndNotifyAllDoneIfNeeded_();

    _cleanupOldGeminiLogs_(); // ← 5分ごとに必ず上詰め＆24時間整理
  } catch (err) {
    Logger.log("[processRowsBatch] lock error: " + err);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/************************************************************
 * 5. 全行完了時のメール通知
 *
 *   完了の定義（対象行）:
 *   - A列が埋まっている
 *   - M列・N列が両方埋まっている
 *   - L列が OK または NG(x) かつ x >= MAX_RETRY_COUNT
 *
 *   prod / dev それぞれで、
 *   「対象行のすべてが上記を満たした時点」でメール送信。
 *   そのときの「最終行のB列の値」をメールに含める。
 *   同じ最終行(B)まで完了している状態では二重送信しない。
 ************************************************************/

function checkAndNotifyAllDoneIfNeededForSheet_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return;

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    // データ行が無い
    return;
  }

  const lastCol = Math.max(STATUS_COL, 14); // 少なくとも L〜N までは読む
  const numRows = lastRow - 1;
  const values = sh.getRange(2, 1, numRows, lastCol).getValues();

  let allDone = true;
  let targetRowCount = 0;
  let lastTargetRowIndex = 0;
  let lastTargetBValue = null;

  for (let i = 0; i < numRows; i++) {
    const row = values[i];

    const colAVal = row[0]; // A列
    const colBVal = row[1]; // B列（日時）
    const titleRaw = row[13 - 1]; // M列
    const bodyRaw = row[14 - 1]; // N列
    const status = (row[STATUS_COL - 1] || "").toString(); // L列

    // A列が空なら対象外
    if (!colAVal) {
      continue;
    }

    // M・N のどちらかでも空なら対象外（その行は「翻訳・要約対象」ではない）
    if (!titleRaw || !bodyRaw) {
      continue;
    }

    // ここまで来たら対象行
    targetRowCount++;
    const absRowIndex = 2 + i;
    lastTargetRowIndex = absRowIndex;
    lastTargetBValue = colBVal;

    // 完了かどうか判定
    let isDone = false;

    if (status.startsWith("OK")) {
      isDone = true;
    } else if (status.startsWith("NG(")) {
      const retryCount = parseRetryCount_(status);
      if (retryCount >= MAX_RETRY_COUNT) {
        isDone = true;
      }
    }

    if (!isDone) {
      allDone = false;
      break;
    }
  }

  // 対象行が1つもないなら通知しない
  if (targetRowCount === 0) {
    return;
  }

  // まだ完了していない
  if (!allDone) {
    return;
  }

  // ここまで来たら「そのシートの対象行がすべて完了」状態

  // ─ 同じ「最終行(B)」に対しては二重送信しないためのキーを作成 ─
  const props = PropertiesService.getScriptProperties();
  const tz = Session.getScriptTimeZone() || "Asia/Yangon";

  let bStr = "";
  if (lastTargetBValue instanceof Date) {
    bStr = Utilities.formatDate(lastTargetBValue, tz, "yyyy-MM-dd HH:mm:ss");
  } else {
    bStr = String(lastTargetBValue || "");
  }

  const notifyKey = sheetName + "#row" + lastTargetRowIndex + "#B=" + bStr;
  const propName = "LAST_NOTIFIED_KEY_" + sheetName;
  const alreadyKey = props.getProperty(propName);

  if (alreadyKey === notifyKey) {
    // この最終行(B)まではすでに通知済み
    Logger.log(
      "[notify] sheet=%s already notified for key=%s",
      sheetName,
      notifyKey
    );
    return;
  }

  // ─ メール送信先の決定（Script Propertiesから複数取得） ─
  const emailList = getNotifyEmailListForSheet_(sheetName);

  if (emailList.length === 0) {
    Logger.log("[notify] no email configured for sheet=" + sheetName);
    return;
  }

  // ─ メール件名・本文を先に作る ─
  const subject = NOTIFY_EMAIL_SUBJECT_BASE + bStr;

  const ssUrl = ss.getUrl();
  const body =
    "シート「" +
    sheetName +
    "」で" +
    bStr +
    "分の記事収集が完了しました。\n\n" +
    "翌 02:30 までにスプレッドシートを更新してください。\n\n" +
    "スプレッドシートURL:\n" +
    ssUrl +
    "\n";

  // ─ 複数アドレスに順次送る ─
  emailList.forEach(function (emailTo) {
    GmailApp.sendEmail(emailTo, subject, body);
    Logger.log(
      "[notify] sent mail to %s for sheet=%s key=%s",
      emailTo,
      sheetName,
      notifyKey
    );
  });

  // 最後に通知した状態を記録
  props.setProperty(propName, notifyKey);
}

// prod / dev まとめてチェックするヘルパー
function checkAndNotifyAllDoneIfNeeded_() {
  const sheetNames = ["prod", "dev"];
  sheetNames.forEach(function (name) {
    checkAndNotifyAllDoneIfNeededForSheet_(name);
  });
}

/************************************************************
 * 6. ログシートクリア
 ************************************************************/
// スプレッドシート主導（インストール型）の「編集時」トリガー用
function onEditClearGeminiLogs(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const sheetName = sheet.getName();

  // 対象は prod / dev シートのみ
  if (sheetName !== "prod" && sheetName !== "dev") return;

  // A2 の変更だけを監視
  if (range.getRow() !== 2 || range.getColumn() !== 1) return;

  const newValue = range.getValue();

  // 「クリアされた（空になった）」ときだけログシートをクリア
  if (newValue === "" || newValue === null) {
    _clearLogSheetFor_(sheetName); // prod or dev に応じてログシート全クリア
    Logger.log("[onEditClearGeminiLogs] cleared logs for sheet=%s", sheetName);
  }
}

/************************************************************
 * 12時間より古いログを削除しつつ、値のある行だけ上に詰める（ヘッダー無し版）
 * 対象シート: gemini_logs_prod / gemini_logs_dev
 ************************************************************/
function _cleanupOldGeminiLogs_() {
  const ss = SpreadsheetApp.getActive();
  const logSheetNames = [GEMINI_LOG_SHEET_NAME_PROD, GEMINI_LOG_SHEET_NAME_DEV];

  const now = new Date();
  const cutoffMs = now.getTime() - 12 * 60 * 60 * 1000; // 12時間前

  logSheetNames.forEach(function (logSheetName) {
    const sh = ss.getSheetByName(logSheetName);
    if (!sh) return;

    const lastRow = sh.getLastRow();
    if (lastRow < 1) return; // データ無し

    const numRows = lastRow;
    const numCols = sh.getLastColumn() || 4; // 念のため自動検出（なければ4）

    const range = sh.getRange(1, 1, numRows, numCols);
    const values = range.getValues();

    const keptRows = [];

    for (let i = 0; i < numRows; i++) {
      const row = values[i];

      const ts = row[0];
      const level = row[1];
      const tag = row[2];
      const msg = row[3];

      // 行全体が空ならスキップ
      const isAllEmpty = !ts && !level && !tag && !msg;
      if (isAllEmpty) continue;

      // timestamp をパース
      let tsDate = null;
      if (ts instanceof Date) {
        tsDate = ts;
      } else if (ts) {
        const parsed = new Date(ts);
        if (!isNaN(parsed.getTime())) tsDate = parsed;
      }

      // timestamp 無し or 不明 → 安全側で残す
      if (!tsDate) {
        keptRows.push(row);
        continue;
      }

      // 24時間以内 → 残す
      if (tsDate.getTime() >= cutoffMs) {
        keptRows.push(row);
      }
    }

    // 元データ消去（書式は保持）
    range.clearContent();

    // 上から詰めて書き戻し
    if (keptRows.length > 0) {
      sh.getRange(1, 1, keptRows.length, numCols).setValues(keptRows);
    }

    Logger.log(
      "[_cleanupOldGeminiLogs_] sheet=%s kept_rows=%s deleted_rows=%s",
      logSheetName,
      keptRows.length,
      numRows - keptRows.length
    );
  });
}

/************************************************************
 * 地名ログ出力用
 ************************************************************/
function openRegionLogSheet_() {
  const ss = SpreadsheetApp.getActive();
  const name = "region_logs";
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.appendRow([
      "timestamp",
      "sheet",
      "row",
      "source",
      "url",
      "part",
      "type",
      "mm",
      "en",
      "dict_ja",
      "used_in_output",
      "output_ja",
      "note",
    ]);
  }
  return sh;
}

function logRegionUsageForRow_(sheet, row, ctx) {
  const logSheet = openRegionLogSheet_();
  const entriesAll = loadRegionGlossary_();

  const sheetName = sheet.getName();
  const {
    sourceVal,
    urlVal,
    titleRaw,
    bodyRaw,
    headlineA,
    headlineB2,
    summaryJa,
  } = ctx;

  const now = new Date();

  // 判定用：元テキスト
  const titleText = (titleRaw || "").toString();
  const bodyText = (bodyRaw || "").toString();

  // タイトル／本文それぞれで regions マッチ（既知地名）
  const entriesTitle = selectRegionEntriesForText_(titleRaw || "", entriesAll);
  const entriesBody = selectRegionEntriesForText_(bodyRaw || "", entriesAll);

  // --- known（regions にある地名）をログ ---
  // タイトル用：見出しA に dict_ja が含まれているか
  entriesTitle.forEach(function (e) {
    const ja = e.ja_headline || e.ja || "";
    const used = ja && headlineA && headlineA.indexOf(ja) !== -1;

    // 出力で使われていないものはログしない
    if (!used) return;

    const mm = e.mm || "";
    const en = e.en || "";

    // この記事タイトルで mm / en のどちらが実際に出ているかを判定
    let mmHit = false;
    let enHit = false;

    if (mm) {
      if (titleText.indexOf(mm) !== -1) {
        mmHit = true;
      }
    }
    if (en) {
      // 英語は単語境界で判定
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(titleText)) {
        enHit = true;
      }
    }

    // ログに書き込む mm / en を決定
    let mmOut = "";
    let enOut = "";
    if (mmHit && !enHit) {
      mmOut = mm;
    } else if (enHit && !mmHit) {
      enOut = en;
    } else if (mmHit && enHit) {
      // 両方出ているケースは、とりあえず mm を優先
      mmOut = mm;
    } else {
      // 念のため、どちらも検出できない場合は従来通り両方入れておく
      mmOut = mm;
      enOut = en;
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      "title", // part
      "known", // type
      mmOut, // mm
      enOut, // en
      ja, // dict_ja
      true, // used_in_output は必ず TRUE
      ja, // output_ja は常に ja
      "",
    ]);
  });

  // 本文用：見出しB' + 要約 に dict_ja が含まれているか（日本語側のかたまり）
  const blobBodyJa = (headlineB2 || "") + "\n" + (summaryJa || "");

  entriesBody.forEach(function (e) {
    const ja = e.ja_body || e.ja || "";
    const used = ja && blobBodyJa.indexOf(ja) !== -1;

    // 出力で使われていないものはログしない
    if (!used) return;

    const mm = e.mm || "";
    const en = e.en || "";

    // この記事本文(bodyText)で mm / en のどちらが出ているかを判定
    let mmHit = false;
    let enHit = false;

    if (mm) {
      if (bodyText.indexOf(mm) !== -1) {
        mmHit = true;
      }
    }
    if (en) {
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(bodyText)) {
        enHit = true;
      }
    }

    let mmOut = "";
    let enOut = "";
    if (mmHit && !enHit) {
      mmOut = mm;
    } else if (enHit && !mmHit) {
      enOut = en;
    } else if (mmHit && enHit) {
      // 両方出ている場合は mm を優先
      mmOut = mm;
    } else {
      // 念のため両方なしのときは元の値をそのまま入れておく
      mmOut = mm;
      enOut = en;
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      "body",
      "known",
      mmOut, // mm
      enOut, // en
      ja, // dict_ja
      true, // used_in_output
      ja, // output_ja
      "",
    ]);
  });

  // ★ unknown 判定でも使う日本語出力のかたまり
  const blobTitleJa = headlineA || "";

  // --- unknown（regions にない地名）を 1 回の呼び出しで検出 ---
  const unknownList = detectUnknownRegionsForArticle_(
    titleRaw || "",
    bodyRaw || "",
    headlineA || "",
    headlineB2 || "",
    summaryJa || "",
    entriesTitle,
    entriesBody
  );

  unknownList.forEach(function (item) {
    const part = (item.part || "").toString().toLowerCase();
    const normalizedPart = part === "title" ? "title" : "body"; // 不正値は body 扱い

    const jaOut = (item.ja || "").toString();
    let used = false;
    if (jaOut) {
      if (normalizedPart === "title") {
        // タイトル用: headlineA の中に含まれているか
        used = blobTitleJa.indexOf(jaOut) !== -1;
      } else {
        // 本文用: 見出しB' + 要約 の中に含まれているか
        used = blobBodyJa.indexOf(jaOut) !== -1;
      }
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      normalizedPart, // part
      "unknown", // type
      item.src || "", // mm 列に原文を入れてしまう（unknown は両言語区別困難なので現状このまま）
      "", // en は不明なので空
      "", // dict_ja は無し
      used, // used_in_output を TRUE / FALSE で記録
      jaOut, // output_ja は常に jaOut
      "",
    ]);
  });
}

function getRegionLogApiKey_() {
  const props = PropertiesService.getScriptProperties();
  const v = props.getProperty("GEMINI_API_KEY_REGION_LOG");
  return v || ""; // 空なら呼び出し側でフォールバック
}

// 記事単位で未知地名を検出する関数
function detectUnknownRegionsForArticle_(
  titleRaw,
  bodyRaw,
  headlineA,
  headlineB2,
  summaryJa,
  knownEntriesTitle,
  knownEntriesBody
) {
  const apiKey = getRegionLogApiKey_();
  if (!apiKey) return []; // ログ専用キーが無ければスキップ

  // 原文 or 出力どちらも何も無ければスキップ
  if (!(titleRaw || bodyRaw)) return [];
  if (!(headlineA || headlineB2 || summaryJa)) return [];

  // 既知エントリ(mm/en)をマージ＋重複除去
  const allKnown = []
    .concat(knownEntriesTitle || [], knownEntriesBody || [])
    .filter(Boolean);

  const seen = {};
  const knownList = [];
  allKnown.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    const key = mm + "|" + en;
    if (seen[key]) return;
    seen[key] = true;
    knownList.push({ mm: mm, en: en });
  });

  // 本文側日本語（見出しB' + 要約）
  const bodyJa = [headlineB2 || "", summaryJa || ""].join("\n").trim();

  const prompt = [
    "あなたは対訳ペアから地名の対応を抽出するツールです。",
    "",
    "与えられた原文タイトル・本文と、その日本語タイトル・本文から、",
    "regions 用語集には載っていないミャンマー国内の地名のみを抽出してください。",
    "",
    "出力は JSON 配列1つのみとし、フォーマットは次の通りです（日本語以外は英数字）：",
    '[{"part":"titleまたはbody","src":"...元の地名...","ja":"...日本語訳..."}]',
    "",
    "制約:",
    "- regions 用語集に含まれている mm/en は抽出しないこと",
    "- 「src」は原文（ミャンマー語または英語）側の地名をそのまま出すこと",
    "- 「ja」は対応する日本語訳をできるだけ短く自然な形で出すこと",
    "- 地名以外（人名・肩書き・一般名詞など）は含めないこと",
    "",
    "【既知の地名（regionsに既に存在）】",
    JSON.stringify(knownList),
    "",
    "【原文タイトル】",
    titleRaw || "(なし)",
    "",
    "【原文本文】",
    bodyRaw || "(なし)",
    "",
    "【日本語タイトル】",
    headlineA || "(なし)",
    "",
    "【日本語本文（見出しB' + 要約）】",
    bodyJa || "(なし)",
  ].join("\n");

  const resp = callGeminiWithKey_(apiKey, prompt, "regionlog#article");
  if (typeof resp !== "string" || resp.indexOf("ERROR:") === 0) return [];

  let cleaned = resp.trim();
  // ```json ... ``` のガード
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
    const last = cleaned.lastIndexOf("```");
    if (last !== -1) cleaned = cleaned.substring(0, last);
    cleaned = cleaned.trim();
  }

  try {
    const arr = JSON.parse(cleaned);
    if (!Array.isArray(arr)) return [];
    // part が title/body のものだけ返す
    return arr.filter(function (item) {
      if (!item || typeof item !== "object") return false;
      const p = (item.part || "").toString().toLowerCase();
      return p === "title" || p === "body";
    });
  } catch (e) {
    return [];
  }
}
