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
 * Gemini 共通設定（リトライ＆ログ）
 ************************************************************/

// リトライ設定（Python版に揃えた値）
const GEMINI_JS_MAX_RETRIES = 7; // 最大リトライ回数
const GEMINI_JS_BASE_DELAY_SEC = 10; // 初回待機（秒）
const GEMINI_JS_MAX_DELAY_SEC = 120; // 最大待機（秒）

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

    // 初回のみシート作成 & ヘッダー行
    if (!sh) {
      sh = ss.insertSheet(logSheetName);
      sh.appendRow(["timestamp", "level", "tag", "message"]);
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

    const tagE = sheetName + "#row" + row + ":E(headlineA)";
    headlineA = callGeminiWithKey_(apiKey, prompt1, tagE);
    sheet.getRange(row, colE).setValue(headlineA);
  } else {
    sheet.getRange(row, colE).setValue("");
  }

  /***************
   * F列：見出しA' (make_headline_prompt_2_from)
   ***************/
  if (headlineA) {
    const prompt2 = buildHeadlinePrompt2From_(headlineA);
    const tagF = sheetName + "#row" + row + ":F(headlineA2)";
    const headlineA2 = callGeminiWithKey_(apiKey, prompt2, tagF);
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

    const tagG = sheetName + "#row" + row + ":G(headlineB2)";
    const headlineB2 = callGeminiWithKey_(apiKey, prompt3, tagG);
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

    const tagI = sheetName + "#row" + row + ":I(summary)";
    const summaryJa = callGeminiWithKey_(apiKey, summaryPrompt, tagI);
    sheet.getRange(row, colI).setValue(summaryJa);
  } else {
    sheet.getRange(row, colI).setValue("");
  }

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
  const vF = sheet.getRange(row, colF).getValue();
  const vG = sheet.getRange(row, colG).getValue();
  const vI = sheet.getRange(row, colI).getValue();

  const errors = [];

  if (isError_(vE)) {
    errors.push("E=" + String(vE));
  }
  if (isError_(vF)) {
    errors.push("F=" + String(vF));
  }
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
    // NGが複数ある場合は / 区切りで全部記録
    // 例: NG: E=ERROR: ... / G=ERROR: ...
    statusText = "NG: " + errors.join(" / ");
  }

  sheet.getRange(row, colL).setValue(statusText);
}

/************************************************************
 * 4. onEditHead トリガー
 *   - M列 or N列 が編集されたとき、その行の E〜G を再計算
 ************************************************************/

// prod / dev シート用のログシート内容をクリア（ヘッダー行は残す）
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

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn() || 1;
  if (lastRow <= 1) return; // ヘッダーのみ

  // 2行目以降をクリア
  sh.getRange(2, 1, lastRow - 1, lastCol).clearContent();
}

const STATUS_PENDING = "PENDING";

function enqueueRowForProcess_(sheet, row) {
  // ヘッダー行は無視
  if (row === 1) return;

  const titleRaw = sheet.getRange(row, 13).getValue(); // M列
  const bodyRaw = sheet.getRange(row, 14).getValue(); // N列

  // タイトルも本文も空なら何もしない
  if (!titleRaw && !bodyRaw) {
    return;
  }

  const statusCell = sheet.getRange(row, STATUS_COL);
  const statusVal = (statusCell.getValue() || "").toString();

  // すでに OK / NG など何かしら結果が入っている場合、
  // 上書きして再処理させたいなら常に PENDING にしてよい。
  // 「一度処理したものは二度とやらない」なら、ここで return してもよい。
  statusCell.setValue(STATUS_PENDING);
}

// pythonで操作した時にも動く
const MAX_ROWS_PER_RUN = 3; // 1回の実行で処理する最大行数
const STATUS_COL = 12; // L列 (ステータス列の列番号)

function processDirtyRows() {
  const lock = LockService.getDocumentLock();
  try {
    lock.waitLock(30 * 1000);

    const ss = SpreadsheetApp.getActive();
    const sheetNames = ["prod", "dev"];
    let remaining = MAX_ROWS_PER_RUN;

    for (let s = 0; s < sheetNames.length; s++) {
      if (remaining <= 0) break;

      const sheetName = sheetNames[s];
      const sh = ss.getSheetByName(sheetName);
      if (!sh) continue;

      const lastRow = sh.getLastRow();
      if (lastRow < 2) continue;

      const startRow = 2;
      const numRows = lastRow - 1;
      const numCols = 14;
      const values = sh.getRange(startRow, 1, numRows, numCols).getValues();

      for (let i = 0; i < numRows; i++) {
        if (remaining <= 0) break;

        const rowIndex = startRow + i;
        const row = values[i];

        const titleRaw = row[13 - 1]; // M列
        const bodyRaw = row[14 - 1]; // N列
        const status = (row[STATUS_COL - 1] || "").toString(); // L列

        // M・N が空 → 処理しない
        if (!titleRaw && !bodyRaw) continue;

        // OK の行はスキップ（再処理させない）
        if (status.startsWith("OK")) continue;

        // ★ RUNNING の行はスキップ（前回の処理途中で止まった場合）
        if (status.startsWith("RUNNING")) continue;

        Logger.log(
          "[processDirtyRows_] processing %s row %s (status=%s)",
          sheetName,
          rowIndex,
          status
        );

        // ★ 処理開始をマーク
        sh.getRange(rowIndex, STATUS_COL).setValue("RUNNING");

        // ★ 実際の1行処理
        processRow_(sh, rowIndex);

        remaining--;
      }
    }

    Logger.log(
      "[processDirtyRows_] done, processed rows=%s",
      MAX_ROWS_PER_RUN - remaining
    );
  } catch (err) {
    Logger.log("[processDirtyRows_] lock error: " + err);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

// 手で操作した時しか動かない
function onEditHead(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const sheetName = sheet.getName();

  // === 1) prod / dev シート以外は一切処理しない ===
  if (sheetName !== "prod" && sheetName !== "dev") {
    return;
  }

  const startRow = range.getRow();
  const startCol = range.getColumn();
  const numRows = range.getNumRows();
  const numCols = range.getNumColumns();
  const endRow = startRow + numRows - 1;
  const endCol = startCol + numCols - 1;

  // === 2) A2 以降がクリアされたら、そのシート用ログをクリア ===
  // 貼り付け・削除の範囲に A列(1) が含まれていて、かつ 2行目以降ならログクリア
  if (startCol <= 1 && endCol >= 1 && endRow >= 2) {
    const fromRow = Math.max(2, startRow);
    const rows = endRow - fromRow + 1;
    const aValues = sheet.getRange(fromRow, 1, rows, 1).getValues();

    // どこか1つでも A列セルが空になっていたらログクリア（ざっくりなルール）
    let clearedA = false;
    for (let i = 0; i < aValues.length; i++) {
      if (!aValues[i][0]) {
        clearedA = true;
        break;
      }
    }
    if (clearedA) {
      _clearLogSheetFor_(sheetName);
    }
    // A列編集時はここで終了（要約・翻訳は走らせない）
    // ※必要なら、「A列と同時にM/Nも貼った場合も処理したい」ように拡張できます
  }

  // === 3) M列(13) / N列(14) が範囲にまったく含まれていなければ何もしない ===
  const touchesM = startCol <= 13 && endCol >= 13;
  const touchesN = startCol <= 14 && endCol >= 14;
  if (!touchesM && !touchesN) {
    return;
  }

  // === 4) ヘッダー行(1行目)は無視、2行目以降を対象 ===
  const firstRow = Math.max(2, startRow);

  // === 5) 貼り付け範囲内で M/N 列が含まれる各行を処理 ===
  for (let r = firstRow; r <= endRow; r++) {
    // この行で実際に見るのは M(13) と N(14) だけ
    const titleRaw = sheet.getRange(r, 13).getValue(); // M列
    const bodyRaw = sheet.getRange(r, 14).getValue(); // N列

    // 両方空なら要約・翻訳処理は走らせない
    if (!titleRaw && !bodyRaw) {
      // 必要ならここで E/F/G/I/L を消す:
      // sheet.getRange(r, 5, 1, 1).clearContent();  // E
      // sheet.getRange(r, 6, 1, 1).clearContent();  // F
      // sheet.getRange(r, 7, 1, 1).clearContent();  // G
      // sheet.getRange(r, 9, 1, 1).clearContent();  // I
      // sheet.getRange(r, 12, 1, 1).clearContent(); // L
      continue;
    }

    // この行は M/N が埋まっているので、1行分処理
    processRow_(sheet, r);
  }
}

function runGeminiBatch() {
  const lock = LockService.getDocumentLock();
  try {
    // 同時実行防止（最大30秒待つ）
    lock.waitLock(30 * 1000);

    const ss = SpreadsheetApp.getActive();
    const sheetNames = ["prod", "dev"]; // 対象シート
    let remaining = MAX_ROWS_PER_RUN;

    for (let s = 0; s < sheetNames.length; s++) {
      if (remaining <= 0) break;

      const sheetName = sheetNames[s];
      const sh = ss.getSheetByName(sheetName);
      if (!sh) continue;

      const lastRow = sh.getLastRow();
      if (lastRow < 2) continue; // データ無し

      const startRow = 2;
      const numRows = lastRow - 1;
      const numCols = 14; // A〜N までを読んでおく
      const values = sh.getRange(startRow, 1, numRows, numCols).getValues();

      for (let i = 0; i < numRows; i++) {
        if (remaining <= 0) break;

        const rowIndex = startRow + i;
        const row = values[i];

        const titleRaw = row[13 - 1]; // M列 (13)
        const bodyRaw = row[14 - 1]; // N列 (14)
        const status = (row[STATUS_COL - 1] || "").toString(); // L列

        // タイトルも本文も空 → スキップ
        if (!titleRaw && !bodyRaw) {
          continue;
        }

        // 「PENDING」の行だけ処理対象にする
        if (status !== STATUS_PENDING) {
          continue;
        }

        Logger.log(
          "[runGeminiBatch] processing %s row %s",
          sheetName,
          rowIndex
        );

        // この行を実際に処理（見出し/要約作成＋L列にOK/NG書き込み）
        processRow_(sh, rowIndex);

        remaining--;
      }
    }

    Logger.log(
      "[runGeminiBatch] done, processed rows=%s",
      MAX_ROWS_PER_RUN - remaining
    );
  } catch (err) {
    Logger.log("[runGeminiBatch] lock error: " + err);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
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
