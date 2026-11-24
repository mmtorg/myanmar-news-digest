/************************************************************
 * claude.js
 *
 * Claude API (Anthropic Messages API) 呼び出しの共通処理。
 *
 * - LLM_PROVIDER を "claude" にすると、prompt.js / duplicate.js /
 *   selection.js / translate.js の Gemini 呼び出しが Claude に切り替わる。
 *   "gemini" に戻すと従来の Gemini 経路（Flash → OpenAI フォールバック含む）に戻る。
 * - Claude 経路では OpenAI へのフォールバックは行わない（OpenAI 実装は残してある）。
 *
 * Script Properties:
 * - ANTHROPIC_API_KEY : Claude API キー（必須）
 *
 * モデル:
 * - 要約・見出し・要約圧縮（読者向けの日本語生成）: Claude Sonnet 5.5
 * - 同一トピック判定・記事選定スコア・翻訳（判定/定型処理）: Claude Haiku 5.5
 *
 * GAS には公式 SDK が無いため、UrlFetchApp で REST API を直接呼ぶ。
 ************************************************************/

// "claude" | "gemini"
const LLM_PROVIDER = "claude";

const CLAUDE_API_KEY_PROP = "ANTHROPIC_API_KEY";
const CLAUDE_API_URL = "https://api.anthropic.com/v1/messages";
const CLAUDE_API_VERSION = "2026-10-21";

// 読者向けの日本語生成（要約・見出し・圧縮）
const CLAUDE_MODEL_MAIN = "claude-sonnet-5-5";
// 判定・分類・翻訳などの軽い処理
const CLAUDE_MODEL_LIGHT = "claude-haiku-5-5";

// 要約・見出しは深い推論が不要なので low（思考トークン＝出力課金を抑える）
const CLAUDE_DEFAULT_EFFORT = "low";

// 固定プロンプト（system）のキャッシュ TTL。
// 5分ごとのトリガー実行なので、5分TTLだと実行間で切れやすい。1時間TTLにする。
const CLAUDE_CACHE_TTL = "1h";

// 追加リトライ回数（429 / 5xx / 529 overloaded / 通信エラー）
const CLAUDE_MAX_RETRIES = 2;
const CLAUDE_RETRY_BASE_MS = 8000;
const CLAUDE_RETRY_MAX_MS = 60000;

// Sonnet 5.5 の安全分類器による拒否時、Anthropic 側で別モデルに再実行させる（Claude内のみ）
const CLAUDE_SERVER_FALLBACK_BETA = "server-side-fallback-2026-07-01";

function isClaudePrimary_() {
  return LLM_PROVIDER === "claude";
}

// 実行ログ + ログシート（prod/dev タグのみ。prompt.js の _appendGeminiLog_ を流用）
function _claudeLog_(level, tag, message) {
  Logger.log("[claude][%s] %s %s", level, tag, message);
  _appendGeminiLog_(level, tag, message);
}

function getClaudeApiKey_() {
  return String(
    PropertiesService.getScriptProperties().getProperty(CLAUDE_API_KEY_PROP) ||
      "",
  ).trim();
}

// 1M トークンあたりの USD（cacheWrite は 1時間TTL = 入力単価の2倍）
function _claudePricePerMillion_(model) {
  const m = String(model || "");
  if (m.indexOf("claude-sonnet-5-5") === 0) {
    return { input: 2.0, cacheRead: 0.1, cacheWrite1h: 4.0, cacheWrite5m: 2.5, output: 10.0 };
  }
  if (m.indexOf("claude-haiku-5-5") === 0) {
    return { input: 0.1, cacheRead: 0.01, cacheWrite1h: 0.2, cacheWrite5m: 0.125, output: 0.5 };
  }
  return null;
}

function _claudeUsageSummary_(data, model) {
  const u = (data && data.usage) || {};
  const input = Number(u.input_tokens || 0);
  const cacheRead = Number(u.cache_read_input_tokens || 0);
  const cacheWrite = Number(u.cache_creation_input_tokens || 0);
  const output = Number(u.output_tokens || 0);
  const cc = u.cache_creation || {};
  const write1h = Number(cc.ephemeral_1h_input_tokens || 0);
  const write5m = Math.max(0, cacheWrite - write1h);

  const price = _claudePricePerMillion_(model);
  let usd = null;
  if (price) {
    usd =
      (input * price.input +
        cacheRead * price.cacheRead +
        write1h * price.cacheWrite1h +
        write5m * price.cacheWrite5m +
        output * price.output) /
      1000000;
  }
  return {
    input: input,
    cacheRead: cacheRead,
    cacheWrite: cacheWrite,
    output: output,
    usd: usd,
  };
}

function _isRetriableClaudeError_(code) {
  return (
    code === 408 ||
    code === 409 ||
    code === 429 ||
    code === 500 ||
    code === 502 ||
    code === 503 ||
    code === 504 ||
    code === 529
  );
}

function _claudeRetryWaitMs_(attempt, res) {
  let ms = Math.min(CLAUDE_RETRY_MAX_MS, Math.pow(2, attempt) * CLAUDE_RETRY_BASE_MS);
  try {
    const headers = res && res.getHeaders ? res.getHeaders() : {};
    const ra = Number(headers["retry-after"] || headers["Retry-After"] || 0);
    if (ra > 0) ms = Math.min(CLAUDE_RETRY_MAX_MS, ra * 1000);
  } catch (e) {}
  return ms + Math.floor(Math.random() * 1000);
}

/**
 * Claude を呼び出し、テキスト（JSON 指定時は JSON 文字列）を返す。
 * 失敗時は既存コードと同じく "ERROR: ..." で始まる文字列を返す。
 *
 * @param {string|{developerText:string,userText:string}} inputSpec
 *   - 文字列: そのまま user メッセージにする
 *   - {developerText,userText}: developerText を system（キャッシュ対象）、userText を user にする
 * @param {string} usageTag ログ用タグ
 * @param {Object} optionsOpt
 *   - model        : 既定 CLAUDE_MODEL_MAIN
 *   - effort       : 既定 CLAUDE_DEFAULT_EFFORT
 *   - maxTokens    : 既定 8000（思考トークン込み）
 *   - formatType   : prompt.js の OpenAI 用スキーマ名（json_schema_summary_batch 等）
 *   - schema       : JSON Schema を直接渡す場合
 *   - cacheSystem  : system をキャッシュするか（既定 true）
 */
function callClaude_(inputSpec, usageTag, optionsOpt) {
  const tag = usageTag || "claude";
  const apiKey = getClaudeApiKey_();
  if (!apiKey) {
    _claudeLog_("ERROR", tag, "missing " + CLAUDE_API_KEY_PROP);
    return "ERROR: missing " + CLAUDE_API_KEY_PROP;
  }

  const cfg = optionsOpt || {};
  const model = String(cfg.model || CLAUDE_MODEL_MAIN);
  const effort = String(cfg.effort || CLAUDE_DEFAULT_EFFORT);
  const maxTokens = Number(cfg.maxTokens || 8000);
  const cacheSystem = cfg.cacheSystem !== false;

  let systemText = "";
  let userText = "";
  if (
    inputSpec &&
    typeof inputSpec === "object" &&
    (inputSpec.developerText != null || inputSpec.userText != null)
  ) {
    systemText = String(inputSpec.developerText || "");
    userText = String(inputSpec.userText || "");
  } else {
    userText = String(inputSpec || "");
  }

  const payload = {
    model: model,
    max_tokens: maxTokens,
    messages: [{ role: "user", content: userText }],
    output_config: { effort: effort },
  };

  if (systemText) {
    const sysBlock = { type: "text", text: systemText };
    if (cacheSystem) {
      sysBlock.cache_control = { type: "ephemeral", ttl: CLAUDE_CACHE_TTL };
    }
    payload.system = [sysBlock];
  }

  let schema = cfg.schema || null;
  if (!schema && cfg.formatType && typeof _openAIJsonSchemaForFormat_ === "function") {
    const def = _openAIJsonSchemaForFormat_(cfg.formatType);
    if (def) schema = def.schema;
  }
  if (schema) {
    payload.output_config.format = { type: "json_schema", schema: schema };
  }

  const headers = {
    "x-api-key": apiKey,
    "anthropic-version": CLAUDE_API_VERSION,
  };

  // Haiku 5.5 には server-side fallback が無いので Sonnet 系だけに付ける
  if (model.indexOf("claude-sonnet-5-5") === 0) {
    payload.fallbacks = "default";
    headers["anthropic-beta"] = CLAUDE_SERVER_FALLBACK_BETA;
  }

  const options = {
    method: "post",
    contentType: "application/json",
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  const promptChars = systemText.length + userText.length;
  let lastError = "";

  for (let attempt = 0; attempt <= CLAUDE_MAX_RETRIES; attempt++) {
    _claudeLog_(
      "INFO",
      tag,
      "Claude try " +
        (attempt + 1) +
        "/" +
        (CLAUDE_MAX_RETRIES + 1) +
        " model=" +
        model +
        " effort=" +
        effort +
        " prompt_chars=" +
        promptChars,
    );

    let res = null;
    try {
      res = UrlFetchApp.fetch(CLAUDE_API_URL, options);
    } catch (e) {
      lastError = "fetch exception: " + e;
      _claudeLog_("WARN", tag, "Claude " + lastError);
      if (attempt < CLAUDE_MAX_RETRIES) {
        Utilities.sleep(_claudeRetryWaitMs_(attempt, null));
        continue;
      }
      break;
    }

    const code = res.getResponseCode();
    const bodyText = res.getContentText() || "";
    let data = null;
    try {
      data = bodyText ? JSON.parse(bodyText) : null;
    } catch (e) {
      data = null;
    }

    if (code >= 200 && code < 300 && data) {
      const u = _claudeUsageSummary_(data, data.model || model);
      const usageMsg =
        "Claude usage model=" +
        (data.model || model) +
        " effort=" +
        effort +
        " input=" +
        u.input +
        " cache_write=" +
        u.cacheWrite +
        " cache_read=" +
        u.cacheRead +
        " output=" +
        u.output +
        " stop=" +
        data.stop_reason +
        (u.usd == null ? " estimated_usd=n/a" : " estimated_usd=" + u.usd.toFixed(6));

      if (data.stop_reason === "refusal") {
        const cat =
          data.stop_details && data.stop_details.category
            ? data.stop_details.category
            : "unknown";
        _claudeLog_("ERROR", tag, usageMsg + " refusal_category=" + cat);
        return "ERROR: Claude refusal (category=" + cat + ")";
      }

      if (data.stop_reason === "max_tokens") {
        _claudeLog_("ERROR", tag, usageMsg);
        return "ERROR: Claude max_tokens reached (max_tokens=" + maxTokens + ")";
      }

      // content は thinking / fallback / text などが混在し得るので type で拾う
      const text = (data.content || [])
        .filter(function (b) {
          return b && b.type === "text";
        })
        .map(function (b) {
          return b.text || "";
        })
        .join("")
        .trim();

      if (!text) {
        lastError = "empty text (stop=" + data.stop_reason + ")";
        _claudeLog_("WARN", tag, usageMsg + " " + lastError);
        if (attempt < CLAUDE_MAX_RETRIES) {
          Utilities.sleep(_claudeRetryWaitMs_(attempt, null));
          continue;
        }
        break;
      }

      _claudeLog_("SUCCESS", tag, usageMsg + " len(resp)=" + text.length);
      return text;
    }

    const errType = data && data.error ? String(data.error.type || "") : "";
    const errMsg = data && data.error ? String(data.error.message || "") : bodyText.slice(0, 300);
    lastError = "HTTP " + code + " " + errType + " " + errMsg;
    _claudeLog_("WARN", tag, "Claude " + lastError);

    if (_isRetriableClaudeError_(code) && attempt < CLAUDE_MAX_RETRIES) {
      Utilities.sleep(_claudeRetryWaitMs_(attempt, res));
      continue;
    }
    break;
  }

  _claudeLog_("ERROR", tag, "Claude give up: " + lastError);
  return "ERROR: Claude " + lastError;
}
