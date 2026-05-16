/************************************************************
 * selection.js
 *
 * 記事選定スコア処理 v2
 *
 * 前提:
 * - processRowsBatch() 済み
 * - E/F/G/I列が生成済み
 * - L列が OK / OK(FLASH) / OK(GPT)
 * - P列に「過去2日同一トピック記事タイトル」が出力済み
 *
 * 出力:
 * - R列以降に記事選定スコアと補助情報を出力
 *
 * 重要:
 * - K列には一切書き込まない
 * - P列・Q列には一切書き込まない
 * - P列は過去2日同一トピック、Q列は記事重複判定キーとして読み取り専用
 *
 * Gemini:
 * - selection.js専用のAPIキーを使用する
 * - Script Properties に GEMINI_SELECTION_API_KEY を設定する
 * - モデル名はこのファイル内の GEMINI_SELECTION_MODEL で管理する
 ************************************************************/

/************************************************************
 * Gemini selection.js 専用設定
 ************************************************************/

const GEMINI_SELECTION_MODEL = "gemini-3.1-flash-lite";

// 429 / 503 / high demand 時のGemini側フォールバック
const GEMINI_SELECTION_FALLBACK_MODEL = "gemini-2.5-flash";

// Geminiがだめな場合の最終フォールバック

// Gemini APIコール節約・安定化設定
const SELECT_GEMINI_BATCH_SIZE = 5;
const SELECT_GEMINI_TARGET_RPM = 12;
const SELECT_GEMINI_RATE_LAST_CALL_PROP = "GEMINI_SELECTION_LAST_CALL_AT_MS";
const SELECT_MODEL_FALLBACK_WAIT_MS = 2000;
const SELECT_MAX_OUTPUT_TOKENS = 8192;

/************************************************************
 * 列定義
 ************************************************************/

const SELECT_COL_O_PAST_TOPIC_COUNT = 15; // O: 過去2日同一TOPIC数。読み取り専用
const SELECT_COL_P_PAST_TOPIC_TITLES = 16; // P: 過去2日同一トピック記事タイトル。読み取り専用
const SELECT_COL_Q_DUPLICATE_KEY = 17; // Q: 記事重複判定キー。読み取り専用

const SELECT_COL_R_SCORE = 18; // R: AI最終選定スコア
const SELECT_COL_S_LABEL = 19; // S: AI判定ラベル
const SELECT_COL_T_CATEGORY = 20; // T: AI主カテゴリ
const SELECT_COL_U_REASON_TAGS = 21; // U: AI採用理由タグ
const SELECT_COL_V_REJECT_TAGS = 22; // V: AI除外理由タグ
const SELECT_COL_W_CONTINUATION_DIFF = 23; // W: 過去記事との差分・続編理由
const SELECT_COL_X_RATIONALE = 24; // X: AI説明
const SELECT_COL_Y_SAME_DAY_TOPIC_KEY = 25; // Y: 同日トピックキー
const SELECT_COL_Z_STATUS = 26; // Z: AI選定ステータス

const SELECT_COL_AA_TOPIC_IMPORTANCE = 27; // AA: トピック重要度スコア
const SELECT_COL_AB_REPRESENTATIVE = 28; // AB: 代表記事スコア
const SELECT_COL_AC_TOPIC_RANK = 29; // AC: 同日トピック内順位
const SELECT_COL_AD_DAILY_RANK = 30; // AD: 日次代表記事順位
const SELECT_COL_AE_DAILY_QUOTA = 31; // AE: 日次採用目安件数
const SELECT_COL_AF_RECOMMEND_FLAG = 32; // AF: AI採用候補フラグ

const SELECT_MAX_ROWS_PER_RUN = 150;

/************************************************************
 * 実行入口
 ************************************************************/

/**
 * prod用入口
 */
function runArticleSelectionScoreProdBatch() {
  _runArticleSelectionScoreBatchBySheetName_("prod");
}

/**
 * dev用入口
 */
function runArticleSelectionScoreDevBatch() {
  _runArticleSelectionScoreBatchBySheetName_("dev");
}

/**
 * prod/devまとめて実行
 */
function runArticleSelectionScoreBatch() {
  ["prod", "dev"].forEach(function (sheetName) {
    _runArticleSelectionScoreBatchBySheetName_(sheetName);
  });
}

/**
 * prodのAI選定列をリセットする
 * K列・P列・Q列は触らない
 */
function resetArticleSelectionScoreProd() {
  _resetArticleSelectionScoreColumnsBySheetName_("prod");
}

/**
 * devのAI選定列をリセットする
 * K列・P列・Q列は触らない
 */
function resetArticleSelectionScoreDev() {
  _resetArticleSelectionScoreColumnsBySheetName_("dev");
}

/************************************************************
 * ヘッダー・リセット
 ************************************************************/

/**
 * R〜AF列のヘッダー整備
 */
function _ensureSelectionScoreHeaders_(sheet) {
  const headers = [
    "AI最終選定スコア",
    "AI判定ラベル",
    "AI主カテゴリ",
    "AI採用理由タグ",
    "AI除外理由タグ",
    "過去記事との差分・続編理由",
    "AI説明",
    "同日トピックキー",
    "AI選定ステータス",
    "トピック重要度スコア",
    "代表記事スコア",
    "同日トピック内順位",
    "日次代表記事順位",
    "日次採用目安件数",
    "AI採用候補フラグ",
  ];

  sheet.getRange(1, SELECT_COL_R_SCORE, 1, headers.length).setValues([headers]);
}

/**
 * R〜AF列をクリアする
 * K列・P列・Q列は触らない
 */
function _resetArticleSelectionScoreColumnsBySheetName_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return;

  _ensureSelectionScoreHeaders_(sheet);

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const colCount = SELECT_COL_AF_RECOMMEND_FLAG - SELECT_COL_R_SCORE + 1;

  sheet.getRange(2, SELECT_COL_R_SCORE, lastRow - 1, colCount).clearContent();
}

/************************************************************
 * メイン処理
 ************************************************************/

/**
 * 選定スコア判定の対象行かどうか
 */
function _isArticleSelectionScoreTarget_(row) {
  const dateVal = row[0]; // A
  const media = String(row[2] || "").trim(); // C
  const e = String(row[4] || "").trim(); // E
  const f = String(row[5] || "").trim(); // F
  const g = String(row[6] || "").trim(); // G
  const summary = String(row[8] || "").trim(); // I
  const statusL = String(row[11] || "").trim(); // L

  const scoreR = row[SELECT_COL_R_SCORE - 1]; // R
  const statusZ = String(row[SELECT_COL_Z_STATUS - 1] || "").trim(); // Z

  if (!dateVal) return false;
  if (!media) return false;

  // Businessプラン限定は記事選定AIの対象外。
  // 必要な場合は、別枠の固定採用・マーケット情報処理として扱う。
  if (media === "(Businessプラン限定)") return false;

  // 見出し・要約処理が完了していること
  if (!(statusL === "OK" || statusL === "OK(FLASH)" || statusL === "OK(GPT)")) {
    return false;
  }

  if (!e && !f && !g) return false;
  if (!summary) return false;

  // すでにR列スコアがある行はスキップ
  if (scoreR !== "" && scoreR !== null && scoreR !== undefined) return false;

  // RUNNING行はスキップ
  if (statusZ.startsWith("RUNNING")) return false;

  return true;
}

/**
 * 1回あたり最大 SELECT_MAX_ROWS_PER_RUN 件をバッチでスコア判定
 *
 * 処理後に以下を実行:
 * - 同日同一トピック内の代表記事順位付け
 * - 日次採用候補ランキング
 */
function _runArticleSelectionScoreBatchBySheetName_(sheetName) {
  const lock = LockService.getDocumentLock();

  try {
    if (!lock.tryLock(5000)) {
      Logger.log("[selection-score] lock busy -> skip");
      return;
    }

    const ss = SpreadsheetApp.getActive();
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) return;

    _ensureSelectionScoreHeaders_(sheet);

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return;

    const lastCol = Math.max(
      SELECT_COL_AF_RECOMMEND_FLAG,
      sheet.getLastColumn(),
    );
    const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

    let processed = 0;
    let batch = [];

    for (let i = 0; i < values.length; i++) {
      if (processed + batch.length >= SELECT_MAX_ROWS_PER_RUN) break;

      const rowIndex = i + 2;
      const row = values[i];

      if (!_isArticleSelectionScoreTarget_(row)) continue;

      batch.push(_articleFromSelectionScoreRow_(row, rowIndex));

      if (batch.length >= SELECT_GEMINI_BATCH_SIZE) {
        _processSelectionScoreArticleBatch_(sheetName, sheet, batch);
        processed += batch.length;
        batch = [];
      }
    }

    if (batch.length > 0 && processed < SELECT_MAX_ROWS_PER_RUN) {
      _processSelectionScoreArticleBatch_(sheetName, sheet, batch);
      processed += batch.length;
    }

    // 同日同一トピック内で代表記事順位を付ける
    _rankSameDayTopicRepresentatives_(sheet);

    // 日次の採用候補順位と採用候補フラグを付ける
    _assignDailySelectionRecommendations_(sheet);

    Logger.log("[selection-score] %s processed=%s", sheetName, processed);
  } catch (e) {
    Logger.log("[selection-score] error: " + e);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/**
 * シート行から記事オブジェクト化
 */
function _articleFromSelectionScoreRow_(row, rowIndex) {
  return {
    rowIndex: rowIndex,
    date: row[0],
    media: String(row[2] || "").trim(),

    headlineA: String(row[4] || "").trim(), // E
    headlineFinal: String(row[5] || "").trim(), // F
    headlineBody: String(row[6] || "").trim(), // G
    summary: String(row[8] || "").trim(), // I
    url: String(row[9] || "").trim(), // J

    originalTitle: String(row[12] || "").trim(), // M
    originalBody: String(row[13] || "").trim(), // N

    pastTopicCount: Number(row[SELECT_COL_O_PAST_TOPIC_COUNT - 1] || 0), // O
    pastTopicTitles: String(
      row[SELECT_COL_P_PAST_TOPIC_TITLES - 1] || "",
    ).trim(), // P
    duplicateKey: String(row[SELECT_COL_Q_DUPLICATE_KEY - 1] || "").trim(), // Q
  };
}

/************************************************************
 * Geminiスコア判定
 ************************************************************/

/**
 * 単一記事をスコア化する。バッチJSON崩れ・欠損時の個別フォールバックにも使う。
 */
function _scoreArticleForSelection_(sheetName, article) {
  try {
    const prompt = _buildArticleSelectionScorePrompt_(article);
    const tag = sheetName + "#row" + article.rowIndex + ":selectionScoreV2";

    const bundle = getSelectionGeminiApiKeyBundle_(
      sheetName,
      article.media,
      tag,
    );

    const resp = callSelectionGeminiWithKey_(
      bundle && bundle.apiKey,
      prompt,
      tag,
      bundle && bundle.propName,
    );

    if (!resp || String(resp).indexOf("ERROR:") === 0) {
      return {
        error: "SELNG(1): " + String(resp || "empty response").slice(0, 180),
      };
    }

    const obj = _parseSelectionJsonObjectFromModelResponse_(resp);
    return _selectionResultFromModelObject_(obj, article);
  } catch (e) {
    return {
      error: "SELNG(1): " + String(e).slice(0, 180),
    };
  }
}

/**
 * 複数記事をまとめてGeminiへ投げ、結果を行番号キーの連想配列で返す。
 */
function _processSelectionScoreArticleBatch_(sheetName, sheet, articles) {
  if (!articles || !articles.length) return;

  articles.forEach(function (article) {
    sheet
      .getRange(article.rowIndex, SELECT_COL_Z_STATUS)
      .setValue("RUNNING(BATCH)");
  });

  const resultByRowIndex = _scoreArticlesForSelectionBatch_(
    sheetName,
    articles,
  );

  if (resultByRowIndex && resultByRowIndex.error) {
    if (resultByRowIndex.retryIndividually) {
      Logger.log(
        "[selection-score] batch failed, retry individually: %s",
        resultByRowIndex.error,
      );

      articles.forEach(function (article) {
        const single = _scoreArticleForSelection_(sheetName, article);

        if (single.error) {
          sheet
            .getRange(article.rowIndex, SELECT_COL_Z_STATUS)
            .setValue(single.error);
        } else {
          _writeSelectionScoreResultToRow_(sheet, article.rowIndex, single);
        }
      });
      return;
    }

    articles.forEach(function (article) {
      sheet
        .getRange(article.rowIndex, SELECT_COL_Z_STATUS)
        .setValue(String(resultByRowIndex.error).slice(0, 220));
    });
    return;
  }

  articles.forEach(function (article) {
    const rowKey = String(article.rowIndex);
    const result = resultByRowIndex && resultByRowIndex[rowKey];

    if (!result) {
      // 一部行だけ返ってこない場合は、その行だけ個別再試行する。
      const single = _scoreArticleForSelection_(sheetName, article);

      if (single.error) {
        sheet
          .getRange(article.rowIndex, SELECT_COL_Z_STATUS)
          .setValue("SELNG(BATCH_MISSING): " + single.error.slice(0, 180));
      } else {
        _writeSelectionScoreResultToRow_(sheet, article.rowIndex, single);
      }
      return;
    }

    if (result.error) {
      sheet
        .getRange(article.rowIndex, SELECT_COL_Z_STATUS)
        .setValue(result.error);
    } else {
      _writeSelectionScoreResultToRow_(sheet, article.rowIndex, result);
    }
  });
}

/**
 * 5記事単位などでまとめて選定スコア判定する。
 */
function _scoreArticlesForSelectionBatch_(sheetName, articles) {
  try {
    const prompt = _buildArticleSelectionScoreBatchPrompt_(articles);
    const tag =
      sheetName +
      "#rows" +
      articles
        .map(function (a) {
          return a.rowIndex;
        })
        .join("-") +
      ":selectionScoreV2Batch";

    const bundle = getSelectionGeminiApiKeyBundle_(sheetName, "batch", tag);

    const resp = callSelectionGeminiWithKey_(
      bundle && bundle.apiKey,
      prompt,
      tag,
      bundle && bundle.propName,
    );

    if (!resp || String(resp).indexOf("ERROR:") === 0) {
      return {
        error:
          "SELNG(BATCH): " + String(resp || "empty response").slice(0, 180),
        retryIndividually: false,
      };
    }

    let parsed;
    try {
      parsed = _parseSelectionJsonObjectFromModelResponse_(resp);
    } catch (e) {
      return {
        error: "SELNG(BATCH_PARSE): " + String(e).slice(0, 180),
        retryIndividually: true,
      };
    }

    const items = Array.isArray(parsed.results) ? parsed.results : [];
    if (!items.length) {
      return {
        error: "SELNG(BATCH_EMPTY_RESULTS): results array is empty",
        retryIndividually: true,
      };
    }

    const articleByRowIndex = {};
    articles.forEach(function (article) {
      articleByRowIndex[String(article.rowIndex)] = article;
    });

    const resultByRowIndex = {};

    items.forEach(function (obj) {
      const rowIndex = String(obj.rowIndex || "").trim();
      const article = articleByRowIndex[rowIndex];
      if (!article) return;

      resultByRowIndex[rowIndex] = _selectionResultFromModelObject_(
        obj,
        article,
      );
    });

    return resultByRowIndex;
  } catch (e) {
    return {
      error: "SELNG(BATCH): " + String(e).slice(0, 180),
      retryIndividually: true,
    };
  }
}

/**
 * モデル出力JSONと記事情報から、R〜ABへ書ける結果オブジェクトを作る。
 */
function _selectionResultFromModelObject_(obj, article) {
  const ruleScore = _selectionRuleScore_(article);

  const topicImportanceScore = _calculateTopicImportanceScore_(
    obj,
    ruleScore,
    article,
  );
  const representativeScore = _clampRound_(
    _calculateRepresentativeScore_(obj, article) +
      _teacherRepresentativeAdjustmentScore_(article, obj),
    0,
    100,
  );
  let finalScore = _calculateSelectionPossibilityScore_(
    obj,
    topicImportanceScore,
    representativeScore,
    article,
  );
  finalScore = _applyTeacherCalibratedFinalScore_(finalScore, obj, article);

  const reasonTags = _mergeSelectionTags_(
    Array.isArray(obj.reasonTags) ? obj.reasonTags : [],
    _teacherReasonTags_(article, obj),
  );
  const rejectTags = _mergeSelectionTags_(
    Array.isArray(obj.rejectTags) ? obj.rejectTags : [],
    _teacherRejectTags_(article, obj),
  );

  return {
    score: finalScore,
    label: _selectionLabelFromScore_(finalScore),
    category: String(obj.mainCategory || "other"),
    reasonTags: reasonTags,
    rejectTags: rejectTags,
    continuationDiff: String(obj.continuationDiffJa || "").slice(0, 220),
    rationale: String(obj.rationaleJa || "").slice(0, 220),
    sameDayTopicKey: _teacherSameDayTopicKey_(article, obj),
    topicImportanceScore: topicImportanceScore,
    representativeScore: representativeScore,
  };
}

/**
 * Gemini用プロンプト
 */
function _buildArticleSelectionScorePrompt_(article) {
  return `
あなたはミャンマー関連記事の編集選定担当です。
毎日収集されるミャンマー関連ニュースの中から、
日本語読者にとって継続的な把握価値が高い記事を見極めます。

目的:
各記事について、以下を0〜100点相当に変換できるように評価する。
1. トピック自体の重要度
2. 記事単体の代表性
3. 過去2日同一トピック記事との差分
4. 同日同一トピック内で代表記事になり得るか

重要:
- これは最終採用フラグではない。
- K列には何も書かない。
- P列は入力情報であり、過去2日に収集した同一トピック記事のタイトルである。
- P列の内容を見て、過去記事と重複するか、続編として価値があるかを判定する。
- Businessプラン限定の記事は原則として記事選定AIの対象外である。

教師データからの補正:
- 経済・生活物価・燃料・貿易・農業・港湾物流・中央銀行関連は評価するが、定例会議・一般的な投資促進・成果不明の協議だけなら上げすぎない。
- 特に、為替、ドル供給、燃料価格、食用油、米・豆・塩などの生活必需品、輸出入、国境貿易、港湾、物流、農家収入、雇用・投資は高評価にする。
- 国営紙・軍政系メディアの記事でも、市場統計、制度変更、貿易、港湾、農業、投資、雇用、公共サービスに関する具体的な数字がある場合は採用候補にする。
- 公式発表系の記事は、生活への影響、制度変更、価格変動、拘束・制限・停止、国際的な政治意味が明確な場合に評価する。単なる会議開催・協力確認・投資誘致の一般論は低めにする。
- 地域の公共交通、罰金、営業停止、通信・排水・市場整備などは、住民生活や行政運用の変化が具体的なら社会・インフラ記事として拾う。
- 安全保障記事は、攻撃・死亡・負傷という語だけでは高評価にしない。主要戦線、主要都市、国境、戦略拠点、民間人多数被害、インフラ・物流・国際社会への波及がある場合に高評価にする。
- 文化・観光・スポーツは原則低評価。ただし、UNESCO、国際機関、全国規模、経済効果、観光収入、軍政プロパガンダ、外交的意味がある場合は例外的に評価する。

採用可能性が高い記事:
- 政治、統治、選挙、憲法、政権中枢、NUG、CRPH、NUCC、AFTA、SCEF
- 外交、国連、ASEAN、EU、ILO、制裁、周辺国との関係
- 経済、為替、燃料、物価、中央銀行、税制、輸出入、国境貿易
- 農業、食料、生活必需品、港湾、物流、雇用、投資、市場統計
- 国民生活に影響する制度変更、交通、電力、通信、公共サービス
- 人権、人道、政治犯、報道の自由、労働者、難民、ロヒンギャ
- 国境情勢、主要戦線、主要武装勢力、戦略拠点、主要インフラ
- 民間人被害が大きい、または国際社会への波及がある安全保障記事

採用可能性が低い記事:
- 地方の単発戦闘、単発攻撃、少数の死傷のみの記事
- 式典、祝電、表敬訪問、会議出席など儀礼的な公式発表
- 通常の文化、宗教、スポーツ、芸能、観光記事
- 通常の犯罪、事故、火災などで政策的・社会的波及が小さい記事
- 過去2日の同一トピック記事と、要点・結論・新情報がほぼ同じ記事

過去2日同一トピック判定の使い方:
1. P列に過去2日の同一トピック記事タイトルがある場合、現在の記事と比較する。
2. 同一トピックかつ記事の要点が同じなら、過去記事と重複するため低スコアにする。
3. 同じテーマでも、過去2日の記事から進展がある、数字が更新された、新しい反応がある、政策・被害・国際反応などの要点が異なる場合は、続編として評価する。
4. 続編として評価する場合は、過去2日の同一トピック記事と何が違うのかを continuationDiffJa に明記する。
5. P列が空の場合は、過去2日重複なしとして扱う。

同日同一トピック:
- 同日に同一トピックの記事が複数ある場合、同じ具体的事象なら必ず同じ sameDayTopicKey を返す。
- sameDayTopicKey は、媒体名やURLではなく、出来事の中核に基づく短いキーにする。
- 重要: 同じ出来事を異なる媒体・角度から報じた記事は、必ず同一のsameDayTopicKeyにする。
- 例えば、恩赦・囚人釈放を報じる記事が複数あれば、すべて amnesty-prisoner-release にする。
- 燃料価格・燃料不足を報じる複数記事は、すべて fuel-price-shortage にする。
- キーを細かく分けすぎない。以下は悪い例: junta-amnesty-march / peasants-day-amnesty / political-prisoner-release（同じ事象なのにキーが分散）
- 例: amnesty-prisoner-release / fuel-price-shortage / cbm-forex-policy / rakhine-aa-conflict / thai-border-refugees
- 同日同一トピック内では、代表記事になりやすい記事を高い representativeScore にする。
- 代表記事として強い記事とは、要約が具体的で、数字・当事者・政策内容・影響範囲が明確な記事である。
- 単なる転載、要点が薄い記事、儀礼的な記事、過去記事とほぼ同じ記事は representativeScore を下げる。

評価上の注意:
- 「死亡」「攻撃」「空爆」という語があるだけで高得点にしない。
- 国民生活、経済、制度、外交、国境、主要勢力、民間人多数被害への影響を重視する。
- 続編の場合は、過去記事との差分が明確なら重複として減点しすぎない。
- 直近で話題化しているテーマは採用候補に残すが、同じ話を毎日繰り返すだけの記事は落とす。
- 要点が同じ重複記事は、重要テーマであっても低スコアにする。
- 同じURLの記事は同一記事として扱われる可能性が高い。
- ニュース番組、動画配信、ニュースまとめ、ライブ配信など、複数ニュースの入口・番組告知に見える記事は、単一の独立記事としての代表性を低く評価する。
- sameDayTopicKey は「同じ事実・同じ政策・同じ事件・同じ発表」を報じる場合だけ共有する。ジャンル、地域、媒体、登場主体が似ているだけの記事を同じキーにしない。
- 判断に迷う場合は、広すぎるキーではなく、見出しの中核事象に基づくやや狭いキーを返す。

記事:
行番号: ${article.rowIndex}
日付: ${article.date}
媒体: ${article.media}

見出しA:
${article.headlineA}

確定寄せ見出し:
${article.headlineFinal}

本文ベース見出し:
${article.headlineBody}

本文要約:
${article.summary}

原文タイトル:
${article.originalTitle}

P列 過去2日同一トピック記事タイトル:
${article.pastTopicTitles || "なし"}

O列 過去2日同一TOPIC数:
${article.pastTopicCount || 0}

URL:
${article.url}

出力はJSONのみ。コードブロック禁止。

{
  "mainCategory": "politics | diplomacy | economy | security | human_rights | border | infrastructure | society | culture_event | crime_accident | other",
  "importanceScore": 0,
  "economicLivelihoodScore": 0,
  "lifeImpactScore": 0,
  "internationalImpactScore": 0,
  "policyImpactScore": 0,
  "conflictImpactScore": 0,
  "strategicConflictScore": 0,
  "noveltyScore": 0,
  "representativeScore": 0,
  "specificityScore": 0,
  "pastTopicRelation": "no_past_topic | duplicate_same_point | continuation_update | related_but_different | unknown",
  "softNewsException": "none | international_recognition | national_scale | economic_impact | propaganda_signal | diplomatic_signal | military_regime_signal",
  "reasonTags": ["economic_policy"],
  "rejectTags": ["local_single_combat"],
  "continuationDiffJa": "過去2日記事との差分。続編でない場合は空文字",
  "sameDayTopicKey": "short-same-day-topic-key",
  "sameDayRepresentativeReasonJa": "同日同一トピック内で代表記事として強い/弱い理由",
  "rationaleJa": "80〜160字で、なぜそのスコアになり得るかを説明"
}

各スコア項目は0〜10で評価する。
`.trim();
}

/**
 * Gemini / OpenAI バッチ用プロンプト
 */
function _buildArticleSelectionScoreBatchPrompt_(articles) {
  const articleBlocks = articles
    .map(function (article) {
      return `
---
rowIndex: ${article.rowIndex}
日付: ${article.date}
媒体: ${article.media}

見出しA:
${article.headlineA}

確定寄せ見出し:
${article.headlineFinal}

本文ベース見出し:
${article.headlineBody}

本文要約:
${article.summary}

原文タイトル:
${article.originalTitle}

P列 過去2日同一トピック記事タイトル:
${article.pastTopicTitles || "なし"}

O列 過去2日同一TOPIC数:
${article.pastTopicCount || 0}

URL:
${article.url}
---`;
    })
    .join("\n");

  return `
あなたはミャンマー関連記事の編集選定担当です。
毎日収集されるミャンマー関連ニュースの中から、日本語読者にとって継続的な把握価値が高い記事を見極めます。

目的:
各記事について、以下を0〜10で評価する。
1. トピック自体の重要度
2. 記事単体の代表性
3. 過去2日同一トピック記事との差分
4. 同日同一トピック内で代表記事になり得るか

重要:
- これは最終採用フラグではない。
- K列には何も書かない。
- P列は入力情報であり、過去2日に収集した同一トピック記事のタイトルである。
- P列の内容を見て、過去記事と重複するか、続編として価値があるかを判定する。
- Businessプラン限定の記事は原則として記事選定AIの対象外である。
- 各記事は独立して評価する。
- ただし、同じバッチ内に同日同一トピックの記事がある場合は、同じ具体的事象に対して同じ sameDayTopicKey を返す。
- 入力された各 rowIndex について、必ず1件ずつ results に含める。

教師データからの補正:
- 経済・生活物価・燃料・貿易・農業・港湾物流・中央銀行関連は評価するが、定例会議・一般的な投資促進・成果不明の協議だけなら上げすぎない。
- 特に、為替、ドル供給、燃料価格、食用油、米・豆・塩などの生活必需品、輸出入、国境貿易、港湾、物流、農家収入、雇用・投資は高評価にする。
- 国営紙・軍政系メディアの記事でも、市場統計、制度変更、貿易、港湾、農業、投資、雇用、公共サービスに関する具体的な数字がある場合は採用候補にする。
- 公式発表系の記事は、生活への影響、制度変更、価格変動、拘束・制限・停止、国際的な政治意味が明確な場合に評価する。単なる会議開催・協力確認・投資誘致の一般論は低めにする。
- 地域の公共交通、罰金、営業停止、通信・排水・市場整備などは、住民生活や行政運用の変化が具体的なら社会・インフラ記事として拾う。
- 安全保障記事は、攻撃・死亡・負傷という語だけでは高評価にしない。主要戦線、主要都市、国境、戦略拠点、民間人多数被害、インフラ・物流・国際社会への波及がある場合に高評価にする。
- 文化・観光・スポーツは原則低評価。ただし、UNESCO、国際機関、全国規模、経済効果、観光収入、軍政プロパガンダ、外交的意味がある場合は例外的に評価する。

採用可能性が高い記事:
- 政治、統治、選挙、憲法、政権中枢、NUG、CRPH、NUCC、AFTA、SCEF
- 外交、国連、ASEAN、EU、ILO、制裁、周辺国との関係
- 経済、為替、燃料、物価、中央銀行、税制、輸出入、国境貿易
- 農業、食料、生活必需品、港湾、物流、雇用、投資、市場統計
- 国民生活に影響する制度変更、交通、電力、通信、公共サービス
- 人権、人道、政治犯、報道の自由、労働者、難民、ロヒンギャ
- 国境情勢、主要戦線、主要武装勢力、戦略拠点、主要インフラ
- 民間人被害が大きい、または国際社会への波及がある安全保障記事

採用可能性が低い記事:
- 地方の単発戦闘、単発攻撃、少数の死傷のみの記事
- 式典、祝電、表敬訪問、会議出席など儀礼的な公式発表
- 通常の文化、宗教、スポーツ、芸能、観光記事
- 通常の犯罪、事故、火災などで政策的・社会的波及が小さい記事
- 過去2日の同一トピック記事と、要点・結論・新情報がほぼ同じ記事

過去2日同一トピック判定の使い方:
1. P列に過去2日の同一トピック記事タイトルがある場合、現在の記事と比較する。
2. 同一トピックかつ記事の要点が同じなら、過去記事と重複するため低スコアにする。
3. 同じテーマでも、過去2日の記事から進展がある、数字が更新された、新しい反応がある、政策・被害・国際反応などの要点が異なる場合は、続編として評価する。
4. 続編として評価する場合は、過去2日の同一トピック記事と何が違うのかを continuationDiffJa に明記する。
5. P列が空の場合は、過去2日重複なしとして扱う。

同日同一トピック:
- 同日に同一トピックの記事が複数ある場合、同じ具体的事象なら必ず同じ sameDayTopicKey を返す。
- sameDayTopicKey は、媒体名やURLではなく、出来事の中核に基づく短いキーにする。
- 重要: 同じ出来事を異なる媒体・角度から報じた記事は、必ず同一のsameDayTopicKeyにする。
- 例えば、恩赦・囚人釈放を報じる記事が複数あれば、すべて amnesty-prisoner-release にする。
- 燃料価格・燃料不足を報じる複数記事は、すべて fuel-price-shortage にする。
- キーを細かく分けすぎない。以下は悪い例: junta-amnesty-march / peasants-day-amnesty / political-prisoner-release（同じ事象なのにキーが分散）
- 例: amnesty-prisoner-release / fuel-price-shortage / cbm-forex-policy / rakhine-aa-conflict / thai-border-refugees
- 同日同一トピック内では、代表記事になりやすい記事を高い representativeScore にする。
- 代表記事として強い記事とは、要約が具体的で、数字・当事者・政策内容・影響範囲が明確な記事である。
- 単なる転載、要点が薄い記事、儀礼的な記事、過去記事とほぼ同じ記事は representativeScore を下げる。

評価上の注意:
- 「死亡」「攻撃」「空爆」という語があるだけで高得点にしない。
- 国民生活、経済、制度、外交、国境、主要勢力、民間人多数被害への影響を重視する。
- 続編の場合は、過去記事との差分が明確なら重複として減点しすぎない。
- 直近で話題化しているテーマは採用候補に残すが、同じ話を毎日繰り返すだけの記事は落とす。
- 要点が同じ重複記事は、重要テーマであっても低スコアにする。
- 同じURLの記事は同一記事として扱われる可能性が高い。
- ニュース番組、動画配信、ニュースまとめ、ライブ配信など、複数ニュースの入口・番組告知に見える記事は、単一の独立記事としての代表性を低く評価する。
- sameDayTopicKey は「同じ事実・同じ政策・同じ事件・同じ発表」を報じる場合だけ共有する。ジャンル、地域、媒体、登場主体が似ているだけの記事を同じキーにしない。
- 判断に迷う場合は、広すぎるキーではなく、見出しの中核事象に基づくやや狭いキーを返す。

対象記事:
${articleBlocks}

出力はJSONのみ。コードブロック禁止。
必ず以下の形式のJSONオブジェクトだけを返す。
JSONオブジェクトを2個以上連続して出力しない。
説明文、Markdown、区切り線、再掲、追加コメントは一切出力しない。
先頭文字は {、末尾文字は } にする。
各 results 要素には、入力の rowIndex を必ず含める。

{
  "results": [
    {
      "rowIndex": 0,
      "mainCategory": "politics | diplomacy | economy | security | human_rights | border | infrastructure | society | culture_event | crime_accident | other",
      "importanceScore": 0,
      "economicLivelihoodScore": 0,
      "lifeImpactScore": 0,
      "internationalImpactScore": 0,
      "policyImpactScore": 0,
      "conflictImpactScore": 0,
      "strategicConflictScore": 0,
      "noveltyScore": 0,
      "representativeScore": 0,
      "specificityScore": 0,
      "pastTopicRelation": "no_past_topic | duplicate_same_point | continuation_update | related_but_different | unknown",
      "softNewsException": "none | international_recognition | national_scale | economic_impact | propaganda_signal | diplomatic_signal | military_regime_signal",
      "reasonTags": ["economic_policy"],
      "rejectTags": ["local_single_combat"],
      "continuationDiffJa": "過去2日記事との差分。続編でない場合は空文字",
      "sameDayTopicKey": "short-same-day-topic-key",
      "sameDayRepresentativeReasonJa": "同日同一トピック内で代表記事として強い/弱い理由",
      "rationaleJa": "80〜160字で、なぜそのスコアになり得るかを説明"
    }
  ]
}

各スコア項目は0〜10で評価する。
`.trim();
}

/************************************************************
 * スコア計算
 ************************************************************/

/**
 * ルールベースの補助スコア
 *
 * 教師データ反映:
 * - 経済・生活・燃料・為替・貿易・農業を強めに加点
 * - 単発戦闘・儀礼・通常ソフトニュースを減点
 * - ただし、国際性・経済性があるソフトニュースは減点しすぎない
 */
function _selectionRuleScore_(article) {
  const text = [
    article.media,
    article.headlineA,
    article.headlineFinal,
    article.headlineBody,
    article.summary,
    article.originalTitle,
  ].join("\n");

  let score = 0;

  // 経済・生活・燃料・貿易・農業。生活実感や市場変動を伴うものを重視する。
  score += _keywordScoreCapped_(
    text,
    [
      "中央銀行",
      "為替",
      "ドル",
      "バーツ",
      "燃料",
      "軽油",
      "ガソリン",
      "物価",
      "価格",
      "高騰",
      "食用油",
      "米",
      "豆",
      "塩",
      "輸出",
      "輸入",
      "国境貿易",
      "港湾",
      "港",
      "物流",
      "農業",
      "農家",
      "収穫",
      "市場",
      "銀行",
      "投資",
      "雇用",
      "税",
      "関税",
      "商業",
      "貿易",
      "経済",
    ],
    2,
    12,
  );

  // 外交・国際・人道
  score += _keywordScoreCapped_(
    text,
    [
      "ASEAN",
      "国連",
      "EU",
      "ILO",
      "制裁",
      "難民",
      "避難民",
      "ロヒンギャ",
      "タイ",
      "中国",
      "インド",
      "日本",
      "韓国",
      "米国",
      "アメリカ",
      "バングラデシュ",
      "マレーシア",
      "国際",
      "人道",
      "支援",
    ],
    2,
    10,
  );

  // 政治・制度・人権
  score += _keywordScoreCapped_(
    text,
    [
      "選挙",
      "憲法",
      "政党",
      "NLD",
      "NUG",
      "CRPH",
      "NUCC",
      "政治犯",
      "恩赦",
      "釈放",
      "徴兵",
      "報道の自由",
      "人権",
      "大統領",
      "副大統領",
      "軍評議会",
      "SAC",
    ],
    2,
    10,
  );

  // 戦略性のある安全保障・インフラ
  score += _keywordScoreCapped_(
    text,
    [
      "主要道路",
      "アジアハイウェイ",
      "国境",
      "戦略拠点",
      "基地",
      "空港",
      "港",
      "港湾",
      "通信",
      "電力",
      "民間人多数",
      "主要都市",
      "橋",
      "鉄道",
      "物流",
      "避難民",
    ],
    2,
    8,
  );

  // 単発戦闘・少人数被害は下げる。
  // ただし、国境・物流・主要都市・国際反応などがあれば下げすぎない。
  if (
    _hasAny_(text, [
      "単発",
      "村",
      "数人",
      "2人死亡",
      "3人死亡",
      "負傷",
      "殺害",
    ]) &&
    !_hasAny_(text, [
      "国境",
      "主要道路",
      "主要都市",
      "民間人多数",
      "国際",
      "物流",
      "燃料",
      "港",
      "空港",
      "避難民",
    ])
  ) {
    score -= 8;
  }

  // 儀礼・式典系
  // ただし、軍政トップ出席・宗教称号授与など軍政動向把握価値がある場合は減点を弱める
  if (
    _hasAny_(text, [
      "祝電",
      "表敬",
      "式典",
      "開会式",
      "閉会式",
      "研修",
      "セミナー",
      "記念式典",
      "親善試合",
      "寄付式",
    ])
  ) {
    if (
      _hasAny_(text, [
        "ミンアウンフライン",
        "総司令官",
        "副総司令官",
        "大統領",
        "副大統領",
        "宗教称号",
        "称号授与",
      ])
    ) {
      // 軍政トップ出席の行事は軍政動向把握価値があるため減点を小さくする
      score -= 3;
    } else {
      score -= 10;
    }
  }

  // 通常の文化・スポーツ・観光。
  // 国際性・経済性・宣伝性・軍政プロパガンダ性があれば減点しすぎない。
  if (
    _hasAny_(text, [
      "サッカー",
      "芸能",
      "パゴダ",
      "祭り",
      "コンテスト",
      "観光",
    ]) &&
    !_hasAny_(text, [
      "UNESCO",
      "ユネスコ",
      "無形文化遺産",
      "文化遺産",
      "国際",
      "全国",
      "観光収入",
      "経済効果",
      "軍政",
      "宣伝",
      "外交",
      "博物館",
      "宇宙",
      "大臣",
      "総司令官",
      "ミンアウンフライン",
    ])
  ) {
    score -= 8;
  }

  // 番組告知・ニュースまとめは、単一記事としての日次代表性が低いため強く抑える。
  if (_isNewsRoundupOrProgram_(text)) {
    score -= 18;
  }

  // 農業・復旧・地域インフラ・国際規格/市場は、固有キーワードではなく生活/制度影響の軸として拾う。
  if (_hasAgricultureProductionLivelihoodSignal_(text)) score += 5;
  if (_hasClimateOrStandardsMarketSignal_(text)) score += 4;
  if (_hasDisasterRecoveryPublicAidSignal_(text)) score += 4;
  if (_hasRegionalStrategicInfrastructureSignal_(text)) score += 4;
  if (_hasInternationalSecurityCooperationSignal_(text)) score += 3;

  return Math.max(-25, Math.min(30, score));
}

/**
 * トピック重要度スコア
 *
 * 記事単体の出来ではなく、
 * 「この話題自体が採用対象として重要か」を見る。
 */
function _calculateTopicImportanceScore_(obj, ruleScore, article) {
  let score = 0;

  score += _safeNumber_(obj.importanceScore) * 2.0;
  score += _safeNumber_(obj.economicLivelihoodScore) * 2.1;
  score += _safeNumber_(obj.lifeImpactScore) * 1.8;
  score += _safeNumber_(obj.policyImpactScore) * 1.5;
  score += _safeNumber_(obj.internationalImpactScore) * 1.3;

  // 安全保障は戦略性を重視し、単なる戦闘件数では上げすぎない
  score += _safeNumber_(obj.strategicConflictScore) * 1.2;
  score += _safeNumber_(obj.conflictImpactScore) * 0.6;

  score += ruleScore;

  // 文化・観光・スポーツでも例外理由があれば戻す
  const softException = String(obj.softNewsException || "none");
  if (softException !== "none") {
    score += 8;
  }

  // 軍政トップ出席の行事は、軍政の動向把握に価値がある
  const articleText = _selectionArticleText_(article);
  if (
    _hasAny_(articleText, ["ミンアウンフライン", "総司令官", "副総司令官"]) &&
    _hasAny_(articleText, [
      "式典",
      "授与",
      "寄付",
      "訓示",
      "演説",
      "視察",
      "出席",
    ])
  ) {
    score += 8;
  }

  return _clampRound_(score, 0, 100);
}

/**
 * 代表記事スコア
 *
 * 同日同一トピックの中で、
 * その記事が代表として選ばれやすいかを見る。
 */
function _calculateRepresentativeScore_(obj, article) {
  let score = 0;

  score += _safeNumber_(obj.representativeScore) * 4.0;
  score += _safeNumber_(obj.noveltyScore) * 2.5;
  score += _safeNumber_(obj.specificityScore) * 3.5;

  const relation = String(obj.pastTopicRelation || "unknown");
  const hasPastTopic = String(article.pastTopicTitles || "").trim() !== "";

  if (hasPastTopic && relation === "duplicate_same_point") {
    score -= 25;
  }

  if (hasPastTopic && relation === "continuation_update") {
    score += 8;
  }

  const rejectTags = Array.isArray(obj.rejectTags) ? obj.rejectTags : [];

  if (rejectTags.indexOf("ceremonial_official_news") !== -1) score -= 10;
  if (rejectTags.indexOf("routine_meeting") !== -1) score -= 8;
  if (rejectTags.indexOf("minor_casualty_only") !== -1) score -= 8;
  if (rejectTags.indexOf("local_single_combat") !== -1) score -= 8;

  return _clampRound_(score, 0, 100);
}

/**
 * 最終選定スコア
 *
 * トピック重要度と代表記事スコアを統合し、
 * 過去2日重複を反映する。
 */
function _calculateSelectionPossibilityScore_(
  obj,
  topicImportanceScore,
  representativeScore,
  article,
) {
  obj = obj || {};

  const topicScore = _safeNumber_(topicImportanceScore);
  const repScore = _safeNumber_(representativeScore);
  const relation = String(obj.pastTopicRelation || "unknown");
  const category = String(obj.mainCategory || "other");
  const softException = String(obj.softNewsException || "none");
  const rejectTags = Array.isArray(obj.rejectTags) ? obj.rejectTags : [];
  const hasPastTopic = _hasPastTopicTitles_(article);
  const text = _selectionArticleText_(article);

  let score = topicScore * 0.62 + repScore * 0.38;

  if (topicScore >= 70 && repScore >= 55) score += 4;
  if (topicScore >= 55 && repScore >= 70) score += 3;

  if (topicScore >= 75 && repScore < 35) score -= 8;
  if (topicScore < 35 && repScore < 50) score -= 8;
  if (topicScore < 25) score = Math.min(score, 42);

  if (hasPastTopic) {
    if (relation === "duplicate_same_point") {
      score -= 18;
      score = Math.min(score, 48);
    } else if (relation === "continuation_update") {
      score += 7;
    } else if (relation === "related_but_different") {
      score += 4;
    } else if (!_looksLikeTrendContinuation_(article, obj)) {
      score -= 6;
    }
  } else if (relation === "no_past_topic") {
    score += 2;
  }

  if (hasPastTopic && _looksLikeTrendContinuation_(article, obj)) {
    score += 3;
  }

  if (_safeNumber_(obj.economicLivelihoodScore) >= 8) score += 3;
  if (_safeNumber_(obj.lifeImpactScore) >= 8) score += 3;
  if (
    _safeNumber_(obj.policyImpactScore) >= 8 &&
    _safeNumber_(obj.internationalImpactScore) >= 6
  ) {
    score += 3;
  }

  if (category === "security" || category === "crime_accident") {
    const strategic = _safeNumber_(obj.strategicConflictScore);
    const conflict = _safeNumber_(obj.conflictImpactScore);

    if (
      strategic <= 4 &&
      conflict <= 5 &&
      !_hasAny_(text, [
        "国境",
        "主要道路",
        "主要都市",
        "戦略拠点",
        "民間人多数",
        "避難",
        "物流",
        "インフラ",
        "空港",
        "港",
        "電力",
        "通信",
        "国際",
      ])
    ) {
      score = Math.min(score - 6, 52);
    }
  }

  if (category === "culture_event" && softException === "none") {
    score = Math.min(score - 6, 45);
  } else if (softException !== "none") {
    score += 4;
  }

  if (rejectTags.indexOf("ceremonial_official_news") !== -1) score -= 10;
  if (rejectTags.indexOf("routine_meeting") !== -1) score -= 8;
  if (rejectTags.indexOf("sports_entertainment") !== -1) score -= 6;
  if (rejectTags.indexOf("religious_cultural_routine") !== -1) score -= 6;
  if (rejectTags.indexOf("ordinary_crime") !== -1) score -= 8;
  if (rejectTags.indexOf("minor_casualty_only") !== -1) score -= 8;
  if (rejectTags.indexOf("local_single_combat") !== -1) score -= 8;
  if (rejectTags.indexOf("duplicate_same_point") !== -1) {
    score -= 12;
    score = Math.min(score, 45);
  }

  if (_isNewsRoundupOrProgram_(text)) {
    score -= 18;
    score = Math.min(score, 38);
  }

  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text)) {
    score -= 8;
    score = Math.min(score, 70);
  }

  return _clampRound_(score, 0, 100);
}

/************************************************************
 * 教師データ補正・重複抑制
 ************************************************************/

/**
 * 教師データの傾向を、モデル出力後の最終スコアに反映する。
 * 目的は「高重要度順」ではなく、手動選定に近い編集ポートフォリオへ寄せること。
 */
function _applyTeacherCalibratedFinalScore_(score, obj, article) {
  let adjusted = Number(score || 0);
  const text = _selectionArticleText_(article);

  // ミャンマーへの直接性が弱い海外一般ニュースは、経済・外交でも大きく抑制する。
  // ただし、地域回廊・越境インフラ・周辺国の安全保障経済に明確な波及がある場合は例外にする。
  if (
    !_hasMyanmarRelevanceForSelection_(text) &&
    !_hasRegionalStrategicInfrastructureSignal_(text)
  ) {
    adjusted = Math.min(adjusted, 35);
  }

  if (_isNewsRoundupOrProgram_(text)) {
    adjusted = Math.min(adjusted - 18, 38);
  }

  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text)) {
    adjusted -= 10;
    adjusted = Math.min(adjusted, 72);
  }

  if (
    _hasAny_(text, [
      "タイ政府",
      "米中央軍",
      "ホルムズ",
      "ロシアとインド",
      "中国湖南省",
      "インドBJP",
      "ウクライナ",
      "世界各国",
    ]) &&
    !_hasAny_(text, [
      "ミャンマー人",
      "ミャンマーへの",
      "ミャンマー軍",
      "軍事政権",
      "ミンアウンフライン",
      "チャット",
      "ロヒンギャ",
    ])
  ) {
    adjusted = Math.min(adjusted, 35);
  }

  // P列に過去2日同一トピックがある場合は、重複抑制とトレンド評価を分ける。
  // 新しい局面がある場合は短期トレンドとして候補に残し、同じ要点の再掲だけを強く抑制する。
  if (_hasPastTopicTitles_(article)) {
    if (_looksLikeTrendContinuation_(article, obj)) {
      adjusted -= 3;
    } else {
      adjusted -= 22;
      adjusted = Math.min(adjusted, 50);
    }
  }

  // 定例の国営・軍政系の開発・入札・通常統計は、過剰採用を抑える。
  if (
    _hasAny_(text, [
      "入札公募",
      "資材調達入札",
      "協力企業を募集",
      "コンテナ船235隻",
      "GPS",
      "QRコード",
      "定期点検",
    ]) &&
    !_hasAny_(text, [
      "価格",
      "高騰",
      "不足",
      "停止",
      "足止め",
      "避難",
      "生活困窮",
      "賄賂",
      "不正",
    ])
  ) {
    adjusted -= 12;
  }

  adjusted -= _teacherOverselectionPenaltyScore_(article, obj);

  // 手動選定で拾われやすい「生活・国境・汚職・大規模犯罪・農業統計・外交経済」を補正。
  adjusted += _teacherRecallBoostScore_(article, obj);

  adjusted += _temporalTrendAdjustmentScore_(article, obj);

  return _clampRound_(adjusted, 0, 100);
}

/**
 * 同日同一トピック内での代表記事らしさを、媒体・具体性・教師傾向で微調整する。
 */
function _teacherRepresentativeAdjustmentScore_(article, obj) {
  const text = _selectionArticleText_(article);
  const media = String(article.media || "");
  let score = 0;

  if (
    _hasAny_(media, ["Myanmar Now", "DVB", "Mizzima", "Khit Thit", "Irrawaddy"])
  ) {
    score += 3;
  }

  if (
    _hasAny_(media, [
      "Global New Light",
      "国営紙",
      "Popular Myanmar",
      "国軍系メディア",
    ])
  ) {
    // 市場統計・農業・数字のある生活情報は残すが、儀礼・通常発表は代表性を下げる。
    if (
      !_hasAny_(text, [
        "価格",
        "チャット",
        "トン",
        "キロ",
        "％",
        "%",
        "貿易",
        "投資",
        "農業",
        "米",
        "塩",
        "港",
        "労働者",
      ])
    ) {
      score -= 4;
    }

    // 政府・中央銀行・外務省本人の発言や会談は、二次媒体より一次情報として代表にしやすい。
    if (
      _hasAny_(text, [
        "中央銀行総裁",
        "外務大臣",
        "大使",
        "ASEAN",
        "首脳会議",
      ]) &&
      _hasAny_(text, ["協議", "会談", "要求", "貿易", "金融", "解放"])
    ) {
      score += 6;
    }
  }

  if (
    _hasAny_(text, [
      "足止め",
      "1000人",
      "1万人",
      "3万人",
      "93キロ",
      "23億",
      "2万5000トン",
    ])
  ) {
    score += 5;
  }

  return score;
}

/**
 * 教師データ上、現在のプロンプト・スコア式が低く見積もりやすい記事群を救う。
 *
 * 設計方針:
 * - 特定記事のキーワード暗記（固有の数字・人名の組み合わせ）を避け、汎用カテゴリで補正する。
 * - 初見記事でも同類の記事が適切にスコアアップされるようにする。
 */
function _teacherRecallBoostScore_(article, obj) {
  const text = _selectionArticleText_(article);
  obj = obj || {};

  let score = 0;

  const importance = _safeNumber_(obj.importanceScore);
  const economy = _safeNumber_(obj.economicLivelihoodScore);
  const life = _safeNumber_(obj.lifeImpactScore);
  const international = _safeNumber_(obj.internationalImpactScore);
  const policy = _safeNumber_(obj.policyImpactScore);
  const conflict = _safeNumber_(obj.conflictImpactScore);
  const strategic = _safeNumber_(obj.strategicConflictScore);
  const novelty = _safeNumber_(obj.noveltyScore);
  const representative = _safeNumber_(obj.representativeScore);
  const specificity = _safeNumber_(obj.specificityScore);
  const category = String(obj.mainCategory || "other");

  // 教師データの固有トピックを暗記せず、編集上安定している「価値の軸」で補正する。
  // 軸: 具体性、生活影響、制度変更、国際波及、規模、説明可能な新規性。
  if (economy >= 7 && (specificity >= 6 || _hasQuantitativeEvidence_(text)))
    score += 6;
  if (life >= 7 && (policy >= 5 || _hasDirectImpactSignal_(text))) score += 7;
  if (policy >= 7 && (novelty >= 6 || _hasLifecycleChangeSignal_(text)))
    score += 7;
  if (international >= 7 && _hasMyanmarRelevanceForSelection_(text)) score += 6;

  // 治安は「戦闘語」ではなく、規模・住民被害・国境/物流/インフラ波及がある場合だけ救う。
  if (
    (category === "security" || category === "border") &&
    (strategic >= 7 || (conflict >= 7 && _hasScaleOrSpilloverSignal_(text)))
  ) {
    score += 6;
  }

  // 通常は低めの社会・文化・スポーツでも、国際認知、社会的論争、移民/難民、軍政宣伝、経済波及があれば例外化する。
  if (
    (category === "culture_event" || category === "society") &&
    _hasPublicMeaningExceptionSignal_(text, obj)
  ) {
    score += 6;
  }

  // 越境・海外ミャンマー人・人権/労働・公共サービス・汚職/詐欺摘発は、固有名ではなく影響類型で拾う。
  if (_hasCrossBorderHumanImpactSignal_(text)) score += 6;
  if (_hasOperationalPublicServiceSignal_(text)) score += 6;
  if (
    _hasInstitutionalAccountabilitySignal_(text) &&
    (specificity >= 5 || _hasQuantitativeEvidence_(text))
  )
    score += 5;

  // モデルが高い具体性・代表性を出しているのに、カテゴリ上の先入観で落ちそうな記事を少し救う。
  if (
    importance >= 6 &&
    novelty >= 7 &&
    representative >= 7 &&
    specificity >= 7
  )
    score += 5;

  // 低く見積もられやすいが手動選定で拾われやすい、生活・復旧・地域戦略・国際制度の軸。
  if (_hasTransportSafetyPublicSignal_(text)) score += 5;
  if (_hasAgricultureProductionLivelihoodSignal_(text)) score += 6;
  if (_hasClimateOrStandardsMarketSignal_(text)) score += 6;
  if (_hasDisasterRecoveryPublicAidSignal_(text)) score += 6;
  if (_hasRegionalStrategicInfrastructureSignal_(text)) score += 6;
  if (_hasInternationalSecurityCooperationSignal_(text)) score += 5;

  // P/O列は「今だけ動いている話題」の信号として使う。ただし、新局面が説明できる場合だけ加点する。
  if (
    _hasPastTopicTitles_(article) &&
    _looksLikeMeaningfulNewDevelopment_(article, obj)
  ) {
    score += Math.min(8, 3 + Number(article.pastTopicCount || 0) * 2);
  }

  // 固定語での過適合を避けるため、救済補正は強くしすぎない。
  return Math.min(30, score);
}

/**
 * 教師データ上の過採用傾向を抑える。
 * 特定タイトルの暗記ではなく、公式発表・重複しやすい経済外交・海外一般ニュースの過大評価を補正する。
 */
function _teacherOverselectionPenaltyScore_(article, obj) {
  const text = _selectionArticleText_(article);
  const media = String(article.media || "");
  obj = obj || {};
  let score = 0;

  const officialLike = _hasAny_(media, [
    "Global New Light",
    "国営紙",
    "Popular Myanmar",
    "国軍系メディア",
  ]);

  // ミャンマーへの直接性が弱い海外一般ニュースは、常に強く抑える。
  // ただし、越境回廊・地域インフラ・安全保障経済の文脈が明確なら過度に抑えない。
  if (
    !_hasMyanmarRelevanceForSelection_(text) &&
    !_hasRegionalStrategicInfrastructureSignal_(text)
  )
    score += 20;

  if (_isNewsRoundupOrProgram_(text)) score += 18;
  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text)) score += 10;

  // 公式発表・会合・式典は、制度変更、生活影響、数字、拘束/停止/解放などの結果がなければ抑える。
  if (
    officialLike &&
    _isRoutineOfficialAnnouncement_(text) &&
    !_hasConcreteConsequenceSignal_(text)
  ) {
    score += 12;
  }

  // 過去2日同一トピックがあり、新局面を説明できないものは重複として抑える。
  if (
    _hasPastTopicTitles_(article) &&
    !_looksLikeMeaningfulNewDevelopment_(article, obj)
  ) {
    score += Math.min(16, 8 + Number(article.pastTopicCount || 0) * 3);
  }

  // 数字の更新だけで、生活・制度・市場ショック・国際波及がないものは上げすぎない。
  if (_isRoutineNumericUpdateWithoutShock_(article, obj)) score += 8;

  // モデルの除外タグが重複・儀礼・通常会議を示す場合は補強する。
  const rejectTags = Array.isArray(obj.rejectTags) ? obj.rejectTags : [];
  if (rejectTags.indexOf("duplicate_same_point") !== -1) score += 8;
  if (rejectTags.indexOf("ceremonial_official_news") !== -1) score += 8;
  if (rejectTags.indexOf("routine_meeting") !== -1) score += 6;

  return Math.min(28, score);
}

/**
 * 固定ジャンルではなく、直近数日の報道密度と新規性で短期トレンドを補正する。
 * P/O列は「重複」だけでなく「同じ話題が動いている兆候」としても使う。
 */
function _temporalTrendAdjustmentScore_(article, obj) {
  const pastCount = Number(article.pastTopicCount || 0);
  const hasPast = _hasPastTopicTitles_(article);
  if (!hasPast && pastCount <= 0) return 0;

  let score = 0;

  // トレンドは「同じ話題がある」だけではなく、前回から何が変わったかで判定する。
  if (_looksLikeMeaningfulNewDevelopment_(article, obj)) {
    score += Math.min(12, 4 + pastCount * 2);
  } else {
    score -= Math.min(12, 5 + pastCount * 2);
  }

  // 報道密度が高く、かつ反応・拡大・実施段階などの変化がある場合だけ短期トレンドとして補正する。
  if (
    pastCount >= 2 &&
    _hasEscalationOrReactionSignal_(_selectionArticleText_(article))
  ) {
    score += 4;
  }

  // 数字・日付・金額だけの更新は、影響信号がなければトレンド扱いしない。
  if (_isRoutineNumericUpdateWithoutShock_(article, obj)) {
    score -= 5;
  }

  return _clampRound_(score, -12, 14);
}

/**
 * モデルのsameDayTopicKeyが揺れる/粗すぎるケースを、編集上の同一事象キーに寄せる。
 */
function _teacherSameDayTopicKey_(article, obj) {
  obj = obj || {};
  const modelKey = _normalizeSelectionTopicKey_(
    obj.sameDayTopicKey || obj.topicKey || "",
  );

  // 固有トピックの手書きマッピングは避ける。
  // モデルが返したキーを基本にし、空・粗すぎる・媒体名/日付っぽい場合だけ記事本文から汎用キーを作る。
  if (modelKey && !_looksLikeWeakSelectionTopicKey_(modelKey)) {
    return modelKey.slice(0, 120);
  }

  return _genericSelectionTopicKeyFromArticle_(article, obj);
}

function _looksLikeWeakSelectionTopicKey_(key) {
  const s = String(key || "");
  if (!s || s === "none" || s === "other" || s === "topic") return true;
  if (s.length < 8) return true;
  if (/^\d+$/.test(s)) return true;
  if (
    _hasAny_(s, ["unknown", "general-news", "misc", "article", "news-update"])
  )
    return true;
  return false;
}

function _genericSelectionTopicKeyFromArticle_(article, obj) {
  const category = String((obj && obj.mainCategory) || "other").toLowerCase();
  const source = String(
    article.headlineFinal ||
      article.headlineA ||
      article.headlineBody ||
      article.originalTitle ||
      article.summary ||
      article.url ||
      "topic",
  );

  const normalized = source
    .toLowerCase()
    .replace(/https?:\/\//g, "")
    .replace(/[0-9０-９]+/g, "n")
    .replace(/\s+/g, "-")
    .replace(/[|｜:：,，.。()（）\[\]「」『』、]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);

  return _normalizeSelectionTopicKey_(
    category + "-" + (normalized || "topic"),
  ).slice(0, 120);
}

function _teacherReasonTags_(article, obj) {
  const text = _selectionArticleText_(article);
  obj = obj || {};
  const tags = [];

  if (_hasQuantitativeEvidence_(text)) tags.push("quantitative_evidence");
  if (_hasDirectImpactSignal_(text)) tags.push("direct_life_impact");
  if (_hasLifecycleChangeSignal_(text))
    tags.push("policy_or_event_stage_change");
  if (_hasCrossBorderHumanImpactSignal_(text))
    tags.push("cross_border_human_impact");
  if (_hasOperationalPublicServiceSignal_(text))
    tags.push("public_service_or_local_operation");
  if (_hasInstitutionalAccountabilitySignal_(text))
    tags.push("accountability_or_large_crime");
  if (_hasPublicMeaningExceptionSignal_(text, obj))
    tags.push("soft_news_public_meaning_exception");
  if (_hasPastTopicTitles_(article)) tags.push("has_past_topic_input_p_col");
  if (_looksLikeMeaningfulNewDevelopment_(article, obj))
    tags.push("meaningful_update_vs_past_topic");

  return tags;
}

function _teacherRejectTags_(article, obj) {
  const text = _selectionArticleText_(article);
  obj = obj || {};
  const tags = [];

  if (!_hasMyanmarRelevanceForSelection_(text))
    tags.push("not_myanmar_relevant");
  if (
    _isRoutineOfficialAnnouncement_(text) &&
    !_hasConcreteConsequenceSignal_(text)
  )
    tags.push("routine_official_without_consequence");
  if (
    _hasPastTopicTitles_(article) &&
    !_looksLikeMeaningfulNewDevelopment_(article, obj)
  )
    tags.push("past_topic_likely_duplicate");
  if (_isRoutineNumericUpdateWithoutShock_(article, obj))
    tags.push("routine_numeric_update_without_shock");

  return tags;
}

function _hasQuantitativeEvidence_(text) {
  const s = String(text || "");
  if (/[0-9０-９]+/.test(s)) return true;
  return _hasAny_(s, [
    "％",
    "%",
    "万",
    "千",
    "億",
    "トン",
    "キロ",
    "人",
    "世帯",
    "社",
    "件",
    "チャット",
    "ドル",
  ]);
}

function _hasLifecycleChangeSignal_(text) {
  return _hasAny_(text, [
    "新たに",
    "追加",
    "更新",
    "発表",
    "決定",
    "承認",
    "開始",
    "実施",
    "施行",
    "停止",
    "再開",
    "解除",
    "制限",
    "禁止",
    "要求",
    "警告",
    "合意",
    "交渉",
    "訴追",
    "審理",
    "判決",
    "解放",
    "拘束",
    "逮捕",
    "摘発",
    "押収",
    "辞任",
    "任命",
    "成立",
    "改定",
    "変更",
  ]);
}

function _hasEscalationOrReactionSignal_(text) {
  return _hasAny_(text, [
    "拡大",
    "悪化",
    "急増",
    "急落",
    "急騰",
    "相次ぐ",
    "継続",
    "再び",
    "反発",
    "抗議",
    "批判",
    "懸念",
    "対応",
    "要請",
    "警告",
    "被害拡大",
    "不足",
    "停止",
    "足止め",
    "避難",
    "混乱",
  ]);
}

function _hasDirectImpactSignal_(text) {
  return _hasAny_(text, [
    "生活",
    "住民",
    "労働者",
    "農家",
    "通勤",
    "通学",
    "患者",
    "避難民",
    "難民",
    "利用者",
    "消費者",
    "価格",
    "不足",
    "停止",
    "制限",
    "罰金",
    "営業停止",
    "賃金",
    "雇用",
    "失業",
    "収入",
    "医療",
    "教育",
    "水",
    "電力",
    "通信",
    "交通",
    "物流",
    "市場",
    "食料",
    "燃料",
    "税",
    "手数料",
  ]);
}

function _hasScaleOrSpilloverSignal_(text) {
  return _hasAny_(text, [
    "多数",
    "数十",
    "数百",
    "数千",
    "万人",
    "大規模",
    "主要",
    "国境",
    "越境",
    "隣国",
    "国際",
    "物流",
    "インフラ",
    "道路",
    "橋",
    "港",
    "空港",
    "鉄道",
    "発電",
    "通信",
    "民間人",
    "避難",
    "難民",
    "都市",
  ]);
}

function _hasPublicMeaningExceptionSignal_(text, obj) {
  const softException = String((obj && obj.softNewsException) || "none");
  if (softException !== "none") return true;

  return _hasAny_(text, [
    "国際",
    "認定",
    "登録",
    "受賞",
    "世界",
    "アジア",
    "外交",
    "批判",
    "論争",
    "物議",
    "差別",
    "難民",
    "移民",
    "アイデンティティ",
    "プロパガンダ",
    "宣伝",
    "軍政",
    "資金",
    "観光収入",
    "経済効果",
  ]);
}

function _hasCrossBorderHumanImpactSignal_(text) {
  return (
    _hasAny_(text, [
      "国境",
      "越境",
      "海外",
      "隣国",
      "出国",
      "入国",
      "帰国",
      "送還",
      "再入国",
    ]) &&
    _hasAny_(text, [
      "ミャンマー人",
      "労働者",
      "移民",
      "難民",
      "避難民",
      "被害者",
      "学生",
      "漁師",
      "家族",
    ]) &&
    _hasAny_(text, [
      "拘束",
      "拒否",
      "支援",
      "雇用",
      "資格",
      "ビザ",
      "不法",
      "詐欺",
      "人身売買",
      "解放",
      "足止め",
    ])
  );
}

function _hasOperationalPublicServiceSignal_(text) {
  return (
    _hasAny_(text, [
      "公共",
      "交通",
      "バス",
      "道路",
      "橋",
      "水",
      "給水",
      "電力",
      "通信",
      "学校",
      "病院",
      "市場",
      "港",
      "物流",
      "農業用水",
    ]) &&
    _hasAny_(text, [
      "開始",
      "停止",
      "制限",
      "増便",
      "建設",
      "完成",
      "罰金",
      "営業停止",
      "不足",
      "利用者",
      "住民",
      "需要",
    ])
  );
}

function _hasInstitutionalAccountabilitySignal_(text) {
  return _hasAny_(text, [
    "汚職",
    "賄賂",
    "不正",
    "詐欺",
    "人身売買",
    "薬物",
    "密輸",
    "摘発",
    "押収",
    "逮捕",
    "起訴",
    "裁判",
    "調査",
  ]);
}

function _hasConcreteConsequenceSignal_(text) {
  return (
    _hasQuantitativeEvidence_(text) ||
    _hasDirectImpactSignal_(text) ||
    _hasLifecycleChangeSignal_(text) ||
    _hasScaleOrSpilloverSignal_(text)
  );
}

function _hasTransportSafetyPublicSignal_(text) {
  return (
    _hasAny_(text, [
      "バス",
      "列車",
      "鉄道",
      "フェリー",
      "船",
      "道路",
      "橋",
      "交通",
      "ターミナル",
      "空港",
    ]) &&
    _hasAny_(text, [
      "事故",
      "衝突",
      "死亡",
      "負傷",
      "けが",
      "ひかれ",
      "逮捕",
      "罰金",
      "取り締まり",
      "営業停止",
      "混乱",
      "停止",
    ])
  );
}

function _hasAgricultureProductionLivelihoodSignal_(text) {
  return (
    _hasAny_(text, [
      "農業",
      "農家",
      "栽培",
      "作付け",
      "収穫",
      "米",
      "豆",
      "穀物",
      "作物",
      "肥料",
      "水産",
      "畜産",
    ]) &&
    _hasAny_(text, [
      "価格",
      "収入",
      "輸出",
      "輸入",
      "市場",
      "雇用",
      "不足",
      "高騰",
      "急落",
      "支援",
      "計画",
      "ゾーン",
      "加工",
      "生産",
      "投資",
    ])
  );
}

function _hasClimateOrStandardsMarketSignal_(text) {
  return (
    _hasAny_(text, [
      "気候変動",
      "炭素",
      "カーボン",
      "排出",
      "持続可能",
      "認証",
      "基準",
      "規格",
      "標準",
      "ESG",
    ]) &&
    _hasAny_(text, [
      "農業",
      "稲作",
      "市場",
      "輸出",
      "投資",
      "企業",
      "収入",
      "国際",
      "価格",
    ])
  );
}

function _hasDisasterRecoveryPublicAidSignal_(text) {
  return (
    _hasAny_(text, [
      "被災",
      "復旧",
      "復興",
      "地震",
      "洪水",
      "サイクロン",
      "暴風雨",
      "災害",
      "住宅",
      "仮設",
      "プレハブ",
      "学校",
      "病院",
    ]) &&
    _hasAny_(text, [
      "支援",
      "寄贈",
      "配送",
      "建設",
      "再開",
      "避難",
      "住民",
      "生徒",
      "公共",
      "提供",
    ])
  );
}

function _hasRegionalStrategicInfrastructureSignal_(text) {
  return (
    _hasAny_(text, [
      "回廊",
      "トンネル",
      "鉄道",
      "港",
      "道路",
      "高速道路",
      "国境",
      "越境",
      "物流",
      "経済特区",
      "インフラ",
      "一帯一路",
      "連結性",
    ]) &&
    _hasAny_(text, [
      "中国",
      "タイ",
      "インド",
      "ASEAN",
      "地域戦略",
      "戦略",
      "貿易",
      "軍事",
      "安全保障",
      "投資",
    ])
  );
}

function _hasInternationalSecurityCooperationSignal_(text) {
  return (
    _hasAny_(text, [
      "国連",
      "平和維持",
      "合同訓練",
      "軍事協力",
      "防衛協力",
      "安全保障協力",
      "PKO",
    ]) &&
    _hasAny_(text, [
      "ミャンマー",
      "国軍",
      "軍",
      "インド",
      "中国",
      "タイ",
      "ASEAN",
      "国際",
    ])
  );
}

function _isNewsRoundupOrProgram_(text) {
  return (
    _hasAny_(text, [
      "ニュース番組",
      "ニュース・プライムタイム",
      "プライムタイム",
      "ライブ配信",
      "ニュースまとめ",
      "まとめニュース",
      "本日のニュース",
      "ニュース一覧",
    ]) && !_hasAny_(text, ["独自", "詳報", "調査", "インタビュー"])
  );
}

function _isLocalSecurityIncidentWithoutStrategicConsequence_(text) {
  return (
    _hasAny_(text, [
      "戦闘",
      "攻撃",
      "空爆",
      "砲撃",
      "徴兵",
      "避難民",
      "攻勢",
      "衝突",
      "殺害",
    ]) &&
    _hasAny_(text, ["村", "郡区", "町", "地域", "地区", "村落"]) &&
    !_hasAny_(text, [
      "航空燃料",
      "海軍基地",
      "空港",
      "港",
      "主要道路",
      "国境",
      "越境",
      "中国",
      "タイ",
      "インド",
      "ASEAN",
      "国連",
      "制裁",
      "物流",
      "電力",
      "通信",
      "石油",
      "ガス",
      "経済特区",
      "選挙",
      "制度変更",
      "数万人",
      "万人",
      "海上",
    ])
  );
}

function _isRoutineOfficialAnnouncement_(text) {
  return _hasAny_(text, [
    "会合",
    "協議",
    "会談",
    "委員会",
    "作業部会",
    "表敬",
    "祝電",
    "式典",
    "開会式",
    "閉会式",
    "研修",
    "セミナー",
    "出席",
    "視察",
    "意見交換",
    "連携強化",
    "協力確認",
    "推進を協議",
  ]);
}

function _looksLikeMeaningfulNewDevelopment_(article, obj) {
  const text = _selectionArticleText_(article);
  obj = obj || {};
  const relation = String(obj.pastTopicRelation || "unknown");

  if (relation === "continuation_update") return true;
  if (
    relation === "related_but_different" &&
    _hasConcreteConsequenceSignal_(text)
  )
    return true;

  // 過去トピックがある場合でも、変化・規模・影響のいずれかが説明できるときだけ続編扱いにする。
  return (
    _hasLifecycleChangeSignal_(text) &&
    (_hasQuantitativeEvidence_(text) ||
      _hasDirectImpactSignal_(text) ||
      _hasScaleOrSpilloverSignal_(text) ||
      _hasEscalationOrReactionSignal_(text))
  );
}

function _isRoutineNumericUpdateWithoutShock_(article, obj) {
  const text = _selectionArticleText_(article);
  if (!_hasQuantitativeEvidence_(text)) return false;

  const relation = String((obj && obj.pastTopicRelation) || "unknown");
  const repeated =
    _hasPastTopicTitles_(article) || relation === "duplicate_same_point";
  if (!repeated) return false;

  // 数字更新でも、明確な生活影響・制度変更・急変・停止/不足があれば抑制しない。
  if (_hasEscalationOrReactionSignal_(text)) return false;
  if (_hasDirectImpactSignal_(text) && _hasLifecycleChangeSignal_(text))
    return false;
  if (_hasScaleOrSpilloverSignal_(text) && _hasLifecycleChangeSignal_(text))
    return false;

  return _hasAny_(text, [
    "上昇",
    "下落",
    "更新",
    "基準",
    "参考価格",
    "市場安定",
    "推移",
    "前週",
    "今週",
  ]);
}

function _selectionArticleText_(article) {
  return [
    article.media,
    article.headlineA,
    article.headlineFinal,
    article.headlineBody,
    article.summary,
    article.originalTitle,
  ].join("\n");
}

function _hasPastTopicTitles_(article) {
  return String(article.pastTopicTitles || "").trim() !== "";
}

function _looksLikeContinuationUpdate_(text) {
  return _hasAny_(text, [
    "新たに",
    "追加",
    "更新",
    "上昇",
    "下落",
    "増加",
    "減少",
    "発表",
    "判明",
    "開始",
    "施行",
    "停止",
    "再開",
    "合意",
    "訴追",
    "審理",
    "拡大",
    "悪化",
    "急増",
    "相次ぐ",
    "継続",
    "再び",
    "対応",
    "反発",
    "抗議",
    "警告",
  ]);
}

function _looksLikeTrendContinuation_(article, obj) {
  const text = _selectionArticleText_(article);
  const relation = String((obj && obj.pastTopicRelation) || "unknown");

  if (
    relation === "continuation_update" ||
    relation === "related_but_different"
  ) {
    return true;
  }

  if (!_looksLikeContinuationUpdate_(text)) return false;

  return _hasAny_(text, [
    "数字",
    "人",
    "名",
    "％",
    "%",
    "チャット",
    "ドル",
    "価格",
    "発表",
    "要求",
    "協議",
    "開始",
    "停止",
    "再開",
    "死亡",
    "負傷",
    "避難",
    "拘束",
    "制裁",
    "選挙",
    "徴兵",
    "国境",
  ]);
}

function _hasMyanmarRelevanceForSelection_(text) {
  return _hasAny_(text, [
    "ミャンマー",
    "ビルマ",
    "ヤンゴン",
    "ネピドー",
    "マンダレー",
    "ラカイン",
    "カレン",
    "カチン",
    "チン州",
    "シャン",
    "モン州",
    "サガイン",
    "マグウェー",
    "バゴー",
    "エーヤワディ",
    "タニンダーリ",
    "タチレク",
    "メーソット",
    "ターク県",
    "国軍",
    "軍事政権",
    "軍評議会",
    "NUG",
    "PDF",
    "AA",
    "TNLA",
    "MNDAA",
    "UWSA",
    "ロヒンギャ",
    "チャット",
    "ミンアウンフライン",
  ]);
}

function _fallbackSelectionTopicKey_(article) {
  const base = String(
    article.headlineFinal ||
      article.headlineA ||
      article.headlineBody ||
      article.originalTitle ||
      article.url ||
      "topic",
  )
    .toLowerCase()
    .replace(/https?:\/\//g, "")
    .replace(/\s+/g, "-")
    .replace(/[|｜:：,，.。()（）\[\]「」『』]/g, "-")
    .replace(/-+/g, "-")
    .slice(0, 80);

  return _normalizeSelectionTopicKey_(base || "topic");
}

function _mergeSelectionTags_(a, b) {
  const out = [];
  [a || [], b || []].forEach(function (list) {
    list.forEach(function (tag) {
      const s = String(tag || "").trim();
      if (s && out.indexOf(s) === -1) out.push(s);
    });
  });
  return out;
}

/**
 * R列スコアからラベルを付ける
 */
function _selectionLabelFromScore_(score) {
  if (score >= 80) return "強い選定候補";
  if (score >= 60) return "選定候補";
  if (score >= 40) return "要確認";
  return "低可能性";
}

/**
 * R〜AB列へ書き込み
 */
function _writeSelectionScoreResultToRow_(sheet, rowIndex, result) {
  const values = [
    [
      result.score,
      result.label,
      result.category,
      result.reasonTags.join(","),
      result.rejectTags.join(","),
      result.continuationDiff,
      result.rationale,
      result.sameDayTopicKey,
      "OK",
      result.topicImportanceScore,
      result.representativeScore,
    ],
  ];

  sheet
    .getRange(
      rowIndex,
      SELECT_COL_R_SCORE,
      1,
      SELECT_COL_AB_REPRESENTATIVE - SELECT_COL_R_SCORE + 1,
    )
    .setValues(values);
}

/************************************************************
 * 同日同一トピック順位付け
 ************************************************************/

/**
 * 同日同一トピック内で代表記事順位を付ける。
 *
 * 方針:
 * - R列スコアは同一化しない
 * - AA列のトピック重要度はグループ内最大値にそろえる
 * - R列スコアを優先し、同点時にAB列の代表記事スコアでAC列に順位を付ける
 * - Geminiが生成するsameDayTopicKeyがバラバラでも、意味的に同一トピックを
 *   マージして正しくグループ化する
 */
function _rankSameDayTopicRepresentatives_(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const lastCol = Math.max(SELECT_COL_AF_RECOMMEND_FLAG, sheet.getLastColumn());
  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  const dateGroups = {};

  values.forEach(function (row, i) {
    const rowIndex = i + 2;
    const dateVal = row[0]; // A
    const topicKey = String(
      row[SELECT_COL_Y_SAME_DAY_TOPIC_KEY - 1] || "",
    ).trim();
    const status = String(row[SELECT_COL_Z_STATUS - 1] || "").trim();

    const score = Number(row[SELECT_COL_R_SCORE - 1] || 0);
    const topicImportance = Number(
      row[SELECT_COL_AA_TOPIC_IMPORTANCE - 1] || 0,
    );
    const representative = Number(row[SELECT_COL_AB_REPRESENTATIVE - 1] || 0);

    if (!score) return;
    if (!topicKey) return;
    if (
      !(
        status === "OK" ||
        status === "OK(SAME_DAY_RANKED)" ||
        status === "OK(DAILY_RANKED)"
      )
    )
      return;

    const dateKey = _selectionDateKey_(dateVal);
    if (!dateKey) return;

    const normalizedKey = _normalizeSelectionTopicKey_(topicKey);
    if (!normalizedKey) return;

    const mergedKey = _mergeRelatedTopicKeys_(normalizedKey);
    if (!dateGroups[dateKey]) dateGroups[dateKey] = [];

    dateGroups[dateKey].push({
      rowIndex: rowIndex,
      row: row,
      score: score,
      topicImportance: topicImportance,
      representative: representative,
      topicKey: mergedKey,
    });
  });

  Object.keys(dateGroups).forEach(function (dateKey) {
    const topicGroups = _clusterSameDayTopicItems_(dateGroups[dateKey]);

    topicGroups.forEach(function (group) {
      const groupTopicScore = Math.max.apply(
        null,
        group.map(function (x) {
          return x.topicImportance;
        }),
      );

      group.sort(function (a, b) {
        if (b.score !== a.score) return b.score - a.score;
        return b.representative - a.representative;
      });

      group.forEach(function (item, idx) {
        const rank = idx + 1;

        sheet
          .getRange(item.rowIndex, SELECT_COL_AA_TOPIC_IMPORTANCE)
          .setValue(groupTopicScore);

        sheet.getRange(item.rowIndex, SELECT_COL_AC_TOPIC_RANK).setValue(rank);

        sheet
          .getRange(item.rowIndex, SELECT_COL_Z_STATUS)
          .setValue("OK(SAME_DAY_RANKED)");
      });
    });
  });
}

function _clusterSameDayTopicItems_(items) {
  const sorted = (items || []).slice().sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return b.representative - a.representative;
  });

  const clusters = [];

  sorted.forEach(function (item) {
    let bestIndex = -1;
    let bestSimilarity = 0;

    clusters.forEach(function (cluster, idx) {
      const sim = _maxSimilarityToSameDayCluster_(item, cluster);
      if (sim > bestSimilarity) {
        bestSimilarity = sim;
        bestIndex = idx;
      }
    });

    if (bestIndex >= 0) {
      const anchor = clusters[bestIndex][0];
      const sameKey =
        item.topicKey && anchor.topicKey && item.topicKey === anchor.topicKey;

      // 同じモデルキーでも文字片がほぼ重ならない場合は誤結合として分離する。
      // 逆にキーが違っても、見出し・要約が十分近い場合は同一事象として結合する。
      if (
        (sameKey && bestSimilarity >= 0.1) ||
        (!sameKey && bestSimilarity >= 0.18)
      ) {
        clusters[bestIndex].push(item);
        return;
      }
    }

    clusters.push([item]);
  });

  return clusters;
}

function _maxSimilarityToSameDayCluster_(item, cluster) {
  let best = 0;
  const anchors = (cluster || []).slice(0, 3);
  anchors.forEach(function (anchor) {
    const sim = _sameDayArticleSimilarityFromRows_(item.row, anchor.row);
    if (sim > best) best = sim;
  });
  return best;
}

function _splitSameDayTopicGroupBySimilarity_(group) {
  if (!group || group.length <= 2) return [group || []];

  const sorted = group.slice().sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return b.representative - a.representative;
  });

  const clusters = [];

  sorted.forEach(function (item) {
    let bestIndex = -1;
    let bestSimilarity = 0;

    clusters.forEach(function (cluster, idx) {
      const anchor = cluster[0];
      const sim = _sameDayArticleSimilarityFromRows_(item.row, anchor.row);
      if (sim > bestSimilarity) {
        bestSimilarity = sim;
        bestIndex = idx;
      }
    });

    // モデルのsameDayTopicKeyは便利だが、バッチ内で別記事へ伝播して粗くなりやすい。
    // 見出し・要約の文字片が十分に重なる場合だけ同一事象としてまとめる。
    if (bestIndex >= 0 && bestSimilarity >= 0.12) {
      clusters[bestIndex].push(item);
    } else {
      clusters.push([item]);
    }
  });

  return clusters;
}

function _sameDayArticleSimilarityFromRows_(rowA, rowB) {
  const setA = _selectionCharGramSet_(_sameDayGroupingTextFromRow_(rowA));
  const setB = _selectionCharGramSet_(_sameDayGroupingTextFromRow_(rowB));
  const keysA = Object.keys(setA);
  const keysB = Object.keys(setB);
  if (!keysA.length || !keysB.length) return 0;

  let intersection = 0;
  keysA.forEach(function (k) {
    if (setB[k]) intersection += 1;
  });

  const union = keysA.length + keysB.length - intersection;
  const jaccard = union ? intersection / union : 0;
  const containment = intersection / Math.min(keysA.length, keysB.length);
  return Math.max(jaccard, containment * 0.45);
}

function _sameDayGroupingTextFromRow_(row) {
  return [
    row[4], // E
    row[5], // F
    row[6], // G
    String(row[8] || "").slice(0, 260), // I
    row[12], // M
  ].join("\n");
}

function _selectionCharGramSet_(text) {
  const normalized = String(text || "")
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, "")
    .replace(/[\s\n\r\t|｜:：,，.。()（）\[\]「」『』、・]/g, "")
    .replace(
      /ミャンマー|ビルマ|国軍|軍事政権|記事|報道|発表|関係者|について/g,
      "",
    )
    .slice(0, 420);

  const out = {};
  if (!normalized) return out;

  for (let i = 0; i < normalized.length - 1; i++) {
    out[normalized.slice(i, i + 2)] = true;
  }
  for (let j = 0; j < normalized.length - 2; j++) {
    out[normalized.slice(j, j + 3)] = true;
  }
  return out;
}

/**
 * 日次の採用候補順位とAI採用候補フラグを付ける。
 *
 * 方針:
 * - 同日同一トピック内順位が1位の記事だけを日次候補にする
 * - 1日の採用目安は、非Business記事数の20%程度
 * - 下限15件、上限25件
 */
function _assignDailySelectionRecommendations_(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const lastCol = Math.max(SELECT_COL_AF_RECOMMEND_FLAG, sheet.getLastColumn());
  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  // AD〜AFに書く出力
  const outputs = values.map(function () {
    return ["", "", ""];
  });

  const dateGroups = {};

  values.forEach(function (row, i) {
    const dateVal = row[0]; // A
    const media = String(row[2] || "").trim(); // C
    const score = Number(row[SELECT_COL_R_SCORE - 1] || 0);
    const representative = Number(row[SELECT_COL_AB_REPRESENTATIVE - 1] || 0);
    const topicRank = Number(row[SELECT_COL_AC_TOPIC_RANK - 1] || 0);
    const status = String(row[SELECT_COL_Z_STATUS - 1] || "").trim();

    if (!dateVal) return;
    if (!score) return;
    if (!media) return;
    if (media === "(Businessプラン限定)") return;
    if (
      !(
        status === "OK" ||
        status === "OK(SAME_DAY_RANKED)" ||
        status === "OK(DAILY_RANKED)"
      )
    )
      return;

    const dateKey = _selectionDateKey_(dateVal);
    if (!dateKey) return;

    if (!dateGroups[dateKey]) {
      dateGroups[dateKey] = {
        allRows: [],
        candidates: [],
      };
    }

    const item = {
      index: i,
      rowIndex: i + 2,
      row: row,
      score: score,
      rankingScore: _clampRound_(
        score + _teacherDailyRankingAdjustment_(row),
        0,
        100,
      ),
      representative: representative,
      topicRank: topicRank || 1,
    };

    dateGroups[dateKey].allRows.push(item);

    // 原則は同日同一トピック内の代表記事だけを候補化する。
    // ただし、別角度の一次情報・政治外交・生活影響が強い2位記事は軽い減点で残す。
    if (!topicRank || topicRank === 1) {
      dateGroups[dateKey].candidates.push(item);
    } else if (_isSecondarySameTopicSelectionCandidate_(row, item)) {
      item.rankingScore = Math.max(0, item.rankingScore - 8);
      dateGroups[dateKey].candidates.push(item);
    }
  });

  Object.keys(dateGroups).forEach(function (dateKey) {
    const group = dateGroups[dateKey];
    const quota = _dailySelectionQuota_(group.allRows.length);

    // まず同日トピック内2位以下は非代表扱いにする
    group.allRows.forEach(function (item) {
      if (item.topicRank > 1) {
        outputs[item.index] = ["", quota, "SAME_TOPIC_NON_REPRESENTATIVE"];
      }
    });

    group.candidates = _orderDailySelectionCandidatesWithPortfolio_(
      group.candidates,
      quota,
    );

    group.candidates.forEach(function (item, idx) {
      const dailyRank = idx + 1;
      let flag = "NOT_RECOMMENDED";

      if (
        dailyRank <= quota &&
        (item.score >= 50 ||
          _isEditorialLowScoreRescueCandidate_(item.row, item))
      ) {
        flag = "AI_RECOMMENDED";
      } else if (dailyRank <= quota && item.score >= 35) {
        flag = "REVIEW";
      } else if (item.score >= 65) {
        flag = "BACKUP_HIGH_SCORE";
      }

      outputs[item.index] = [dailyRank, quota, flag];
    });
  });

  sheet
    .getRange(2, SELECT_COL_AD_DAILY_RANK, outputs.length, 3)
    .setValues(outputs);

  // ステータスを更新
  values.forEach(function (row, i) {
    const status = String(row[SELECT_COL_Z_STATUS - 1] || "").trim();
    const score = Number(row[SELECT_COL_R_SCORE - 1] || 0);

    if (score && (status === "OK" || status === "OK(SAME_DAY_RANKED)")) {
      sheet.getRange(i + 2, SELECT_COL_Z_STATUS).setValue("OK(DAILY_RANKED)");
    }
  });
}

function _isSecondarySameTopicSelectionCandidate_(row, item) {
  if (!item || item.topicRank > 4) return false;
  if (Number(item.score || 0) < 50) return false;

  const text = [
    row[2], // C media
    row[4], // E
    row[5], // F
    row[6], // G
    row[8], // I
    row[12], // M
    row[20], // U reason tags
    row[22], // W continuation diff
  ].join("\n");

  if (_isNewsRoundupOrProgram_(text)) return false;

  if (
    _hasAny_(text, ["続編", "差分", "新たに", "別角度", "追加", "更新"]) &&
    Number(item.score || 0) >= 50
  ) {
    return true;
  }

  // 固有名・一時的トピックではなく、別角度として残す価値があるかを汎用シグナルで見る。
  return (
    _hasDirectImpactSignal_(text) ||
    _hasLifecycleChangeSignal_(text) ||
    _hasScaleOrSpilloverSignal_(text) ||
    _hasCrossBorderHumanImpactSignal_(text) ||
    _hasOperationalPublicServiceSignal_(text) ||
    _hasInstitutionalAccountabilitySignal_(text) ||
    _hasTransportSafetyPublicSignal_(text) ||
    _hasAgricultureProductionLivelihoodSignal_(text) ||
    _hasDisasterRecoveryPublicAidSignal_(text) ||
    _hasRegionalStrategicInfrastructureSignal_(text) ||
    _hasInternationalSecurityCooperationSignal_(text) ||
    _hasPublicMeaningExceptionSignal_(text, {})
  );
}

function _isEditorialLowScoreRescueCandidate_(row, item) {
  const score = Number((item && item.score) || 0);
  const text = _selectionRowTextForRanking_(row);
  if (!score) return false;
  if (_isNewsRoundupOrProgram_(text)) return false;
  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text) && score < 60)
    return false;

  if (score >= 40 && _hasConcreteConsequenceSignal_(text)) return true;
  if (
    score >= 35 &&
    (_hasTransportSafetyPublicSignal_(text) ||
      _hasAgricultureProductionLivelihoodSignal_(text) ||
      _hasClimateOrStandardsMarketSignal_(text) ||
      _hasDisasterRecoveryPublicAidSignal_(text) ||
      _hasCrossBorderHumanImpactSignal_(text) ||
      _hasOperationalPublicServiceSignal_(text) ||
      _hasInstitutionalAccountabilitySignal_(text) ||
      _hasInternationalSecurityCooperationSignal_(text))
  ) {
    return true;
  }

  if (score >= 25 && _hasRegionalStrategicInfrastructureSignal_(text))
    return true;
  if (
    score >= 30 &&
    _hasPublicMeaningExceptionSignal_(text, {}) &&
    (_hasMyanmarRelevanceForSelection_(text) ||
      _hasRegionalStrategicInfrastructureSignal_(text))
  ) {
    return true;
  }

  return false;
}

/**
 * 1日の採用目安件数
 *
 * 手動選定の傾向（全体の約25%）に合わせ、
 * 非Business記事数の25%を目安にしつつ、15〜30件に収める。
 */
function _selectionRowTextForRanking_(row) {
  return [
    row[2], // C media
    row[4], // E
    row[5], // F
    row[6], // G
    row[8], // I
    row[12], // M
    row[SELECT_COL_T_CATEGORY - 1], // T
    row[SELECT_COL_U_REASON_TAGS - 1], // U
    row[SELECT_COL_V_REJECT_TAGS - 1], // V
    row[SELECT_COL_W_CONTINUATION_DIFF - 1], // W
  ].join("\n");
}

function _teacherDailyRankingAdjustment_(row) {
  const text = _selectionRowTextForRanking_(row);
  const category = String(row[SELECT_COL_T_CATEGORY - 1] || "").trim();
  const reasonTags = String(row[SELECT_COL_U_REASON_TAGS - 1] || "");
  const rejectTags = String(row[SELECT_COL_V_REJECT_TAGS - 1] || "");

  let score = 0;

  // 日次ランキングでは、単なる高重要度だけでなく、編集上の採用価値を軽く補正する。
  if (_hasDirectImpactSignal_(text)) score += 5;
  if (_hasLifecycleChangeSignal_(text)) score += 4;
  if (_hasScaleOrSpilloverSignal_(text)) score += 4;
  if (_hasCrossBorderHumanImpactSignal_(text)) score += 5;
  if (_hasOperationalPublicServiceSignal_(text)) score += 5;
  if (_hasInstitutionalAccountabilitySignal_(text)) score += 4;
  if (_hasPublicMeaningExceptionSignal_(text, {})) score += 3;
  if (_hasTransportSafetyPublicSignal_(text)) score += 5;
  if (_hasAgricultureProductionLivelihoodSignal_(text)) score += 5;
  if (_hasClimateOrStandardsMarketSignal_(text)) score += 4;
  if (_hasDisasterRecoveryPublicAidSignal_(text)) score += 5;
  if (_hasRegionalStrategicInfrastructureSignal_(text)) score += 5;
  if (_hasInternationalSecurityCooperationSignal_(text)) score += 4;

  // モデル後段で付けた抽象タグもランキングに反映する。
  if (reasonTags.indexOf("meaningful_update_vs_past_topic") !== -1) score += 4;
  if (reasonTags.indexOf("direct_life_impact") !== -1) score += 4;
  if (reasonTags.indexOf("cross_border_human_impact") !== -1) score += 4;
  if (reasonTags.indexOf("public_service_or_local_operation") !== -1)
    score += 4;
  if (reasonTags.indexOf("soft_news_public_meaning_exception") !== -1)
    score += 3;

  // 重複・定例・ミャンマー直接性なしは日次順位でさらに落とす。
  if (rejectTags.indexOf("not_myanmar_relevant") !== -1) score -= 12;
  if (rejectTags.indexOf("past_topic_likely_duplicate") !== -1) score -= 8;
  if (rejectTags.indexOf("routine_official_without_consequence") !== -1)
    score -= 8;
  if (rejectTags.indexOf("routine_numeric_update_without_shock") !== -1)
    score -= 6;

  if (
    _isRoutineOfficialAnnouncement_(text) &&
    !_hasConcreteConsequenceSignal_(text)
  ) {
    score -= 6;
  }

  if (!_hasMyanmarRelevanceForSelection_(text)) {
    if (_hasRegionalStrategicInfrastructureSignal_(text)) {
      score -= 2;
    } else {
      score -= 12;
    }
  }

  if (_isNewsRoundupOrProgram_(text)) score -= 16;
  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text)) score -= 8;

  // 文化・社会系は通常低めだが、公共的意味がある場合だけ少し戻す。
  if (
    (category === "culture_event" || category === "society") &&
    _hasPublicMeaningExceptionSignal_(text, {})
  ) {
    score += 4;
  }

  return _clampRound_(score, -18, 18);
}

function _selectionPortfolioBucketFromRow_(row) {
  const text = _selectionRowTextForRanking_(row);
  const category = String(row[SELECT_COL_T_CATEGORY - 1] || "").trim();

  if (
    category === "politics" ||
    category === "diplomacy" ||
    category === "human_rights"
  ) {
    return "politics_diplomacy_rights";
  }

  if (_hasRegionalStrategicInfrastructureSignal_(text)) {
    return "regional_strategy_infrastructure";
  }

  if (
    category === "economy" ||
    _hasAny_(text, ["価格", "雇用", "貿易", "市場", "税", "輸出", "輸入"]) ||
    _hasAgricultureProductionLivelihoodSignal_(text) ||
    _hasClimateOrStandardsMarketSignal_(text)
  ) {
    return "economy_livelihood";
  }

  if (
    category === "security" ||
    category === "border" ||
    _hasScaleOrSpilloverSignal_(text)
  ) {
    return "security_border_humanitarian";
  }

  if (
    _hasOperationalPublicServiceSignal_(text) ||
    _hasTransportSafetyPublicSignal_(text) ||
    _hasDisasterRecoveryPublicAidSignal_(text) ||
    category === "infrastructure"
  ) {
    return "public_service_infrastructure";
  }

  if (
    _hasInstitutionalAccountabilitySignal_(text) ||
    category === "crime_accident"
  ) {
    return "accountability_crime";
  }

  if (
    category === "society" ||
    category === "culture_event" ||
    _hasPublicMeaningExceptionSignal_(text, {})
  ) {
    return "society_culture_exception";
  }

  return "other";
}

function _compareDailySelectionItems_(a, b) {
  if (b.rankingScore !== a.rankingScore) return b.rankingScore - a.rankingScore;
  if (b.score !== a.score) return b.score - a.score;
  return b.representative - a.representative;
}

function _orderDailySelectionCandidatesWithPortfolio_(items, quota) {
  const sorted = (items || []).slice().sort(_compareDailySelectionItems_);
  if (!sorted.length) return sorted;

  const headLimit = Math.min(Number(quota || 0), sorted.length);
  if (headLimit <= 0) return sorted;

  const selected = [];
  const remaining = sorted.slice();
  const bucketCounts = {};

  function markPicked(item) {
    const bucket = _selectionPortfolioBucketFromRow_(item.row);
    bucketCounts[bucket] = Number(bucketCounts[bucket] || 0) + 1;
    selected.push(item);
  }

  function takeBest(predicate) {
    let bestIndex = -1;
    let bestScore = -9999;
    for (let i = 0; i < remaining.length; i++) {
      const item = remaining[i];
      if (predicate && !predicate(item)) continue;
      const bucket = _selectionPortfolioBucketFromRow_(item.row);
      const count = Number(bucketCounts[bucket] || 0);
      const diversityPenalty = count * 5;
      const effectiveScore =
        Number(item.rankingScore || 0) -
        diversityPenalty +
        Number(item.representative || 0) * 0.01;
      if (effectiveScore > bestScore) {
        bestScore = effectiveScore;
        bestIndex = i;
      }
    }
    if (bestIndex < 0) return null;
    const picked = remaining.splice(bestIndex, 1)[0];
    markPicked(picked);
    return picked;
  }

  // まず上位の強い候補を残す。ここは従来ロジックを尊重する。
  const protectedHeadCount = Math.min(
    headLimit,
    Math.max(6, Math.round(headLimit * 0.5)),
  );
  while (selected.length < protectedHeadCount && remaining.length) {
    takeBest(null);
  }

  // その後、手動選定に近い「編集ポートフォリオ」を確保する。
  // 固定トピック名ではなく、価値の軸ごとの最低限の露出を作る。
  const dailyCutoffScore = _dailySelectionCutoffScore_(sorted, headLimit);

  const reserveTargets = _buildDynamicPortfolioReserveTargets_(
    sorted,
    headLimit,
    bucketCounts,
    dailyCutoffScore,
  );

  _portfolioReserveBucketsByPriority_(reserveTargets).forEach(
    function (bucket) {
      const target = Number(
        (reserveTargets[bucket] && reserveTargets[bucket].target) || 0,
      );

      while (
        selected.length < headLimit &&
        Number(bucketCounts[bucket] || 0) < target
      ) {
        const picked = takeBest(function (item) {
          if (_selectionPortfolioBucketFromRow_(item.row || []) !== bucket)
            return false;
          if (!_isPortfolioReserveCandidate_(item, bucket)) return false;
          return _passesDynamicPortfolioReserveCutoff_(
            item,
            bucket,
            dailyCutoffScore,
          );
        });

        if (!picked) break;
      }
    },
  );

  while (selected.length < headLimit && remaining.length) {
    takeBest(null);
  }

  remaining.sort(_compareDailySelectionItems_);
  return selected.concat(remaining);
}

function _isPortfolioReserveCandidate_(item, bucket) {
  const score = Number((item && item.score) || 0);
  const rankingScore = Number((item && item.rankingScore) || 0);
  const text = _selectionRowTextForRanking_(item.row || []);
  if (_isNewsRoundupOrProgram_(text)) return false;

  if (bucket === "regional_strategy_infrastructure") {
    return score >= 25 || rankingScore >= 35;
  }
  if (bucket === "society_culture_exception") {
    return score >= 30 || _isEditorialLowScoreRescueCandidate_(item.row, item);
  }
  if (bucket === "public_service_infrastructure") {
    return score >= 30 || _isEditorialLowScoreRescueCandidate_(item.row, item);
  }
  if (bucket === "accountability_crime") {
    return score >= 35 || _isEditorialLowScoreRescueCandidate_(item.row, item);
  }
  if (bucket === "economy_livelihood") {
    return score >= 35 || _isEditorialLowScoreRescueCandidate_(item.row, item);
  }
  if (bucket === "politics_diplomacy_rights") {
    return score >= 40 || _isEditorialLowScoreRescueCandidate_(item.row, item);
  }
  if (bucket === "security_border_humanitarian") {
    return (
      score >= 45 && !_isLocalSecurityIncidentWithoutStrategicConsequence_(text)
    );
  }
  return score >= 40;
}

function _dailySelectionCutoffScore_(sorted, headLimit) {
  if (!sorted || !sorted.length) return 0;

  const idx = Math.min(
    Math.max(Number(headLimit || 1) - 1, 0),
    sorted.length - 1,
  );

  return Number(sorted[idx].rankingScore || sorted[idx].score || 0);
}

function _buildDynamicPortfolioReserveTargets_(
  items,
  headLimit,
  currentBucketCounts,
  dailyCutoffScore,
) {
  const stats = {};

  (items || []).forEach(function (item) {
    const row = item.row || [];
    const bucket = _selectionPortfolioBucketFromRow_(row);
    const text = _selectionRowTextForRanking_(row);

    if (!bucket) return;
    if (_isNewsRoundupOrProgram_(text)) return;
    if (!_isPortfolioReserveCandidate_(item, bucket)) return;
    if (
      !_passesDynamicPortfolioReserveCutoff_(item, bucket, dailyCutoffScore)
    ) {
      return;
    }

    const strength = _dynamicPortfolioReserveStrength_(item, bucket);

    if (!stats[bucket]) {
      stats[bucket] = {
        bucket: bucket,
        candidates: 0,
        strongCandidates: 0,
        topScores: [],
        bestStrength: 0,
      };
    }

    stats[bucket].candidates += 1;
    stats[bucket].topScores.push(strength);
    stats[bucket].bestStrength = Math.max(stats[bucket].bestStrength, strength);

    if (strength >= 62) {
      stats[bucket].strongCandidates += 1;
    }
  });

  const targets = {};

  Object.keys(stats).forEach(function (bucket) {
    const st = stats[bucket];
    st.topScores.sort(function (a, b) {
      return b - a;
    });

    const currentCount = Number(
      (currentBucketCounts && currentBucketCounts[bucket]) || 0,
    );

    let target = 0;

    // 強い候補が1本あるなら、未採用カテゴリの取りこぼし防止として1枠だけ発動。
    if (st.bestStrength >= 60) {
      target = 1;
    }

    // そのカテゴリに強い候補が複数ある日だけ、2枠目を許可。
    if (
      st.candidates >= 3 &&
      st.strongCandidates >= 2 &&
      Number(st.topScores[1] || 0) >= 58
    ) {
      target = 2;
    }

    // 候補密度が非常に高く、かつ広い公共性を持つカテゴリだけ3枠目を許可。
    if (
      st.candidates >= 6 &&
      st.strongCandidates >= 3 &&
      Number(st.topScores[2] || 0) >= 58 &&
      _isBroadPortfolioBucket_(bucket)
    ) {
      target = 3;
    }

    // 固定比率ではなく、安全上限だけ置く。
    target = Math.min(
      target,
      _dynamicPortfolioReserveCap_(bucket, headLimit),
      st.candidates,
    );

    // 既に上位選定で十分入っているカテゴリは追加救済しない。
    if (target > currentCount) {
      targets[bucket] = {
        target: target,
        priority:
          st.bestStrength +
          st.strongCandidates * 4 +
          Math.min(st.candidates, 5),
      };
    }
  });

  return targets;
}

function _portfolioReserveBucketsByPriority_(reserveTargets) {
  return Object.keys(reserveTargets || {}).sort(function (a, b) {
    return (
      Number(reserveTargets[b].priority || 0) -
      Number(reserveTargets[a].priority || 0)
    );
  });
}

function _passesDynamicPortfolioReserveCutoff_(item, bucket, dailyCutoffScore) {
  const score = Number((item && item.score) || 0);
  const rankingScore = Number((item && item.rankingScore) || score || 0);
  const text = _selectionRowTextForRanking_((item && item.row) || []);

  if (_isNewsRoundupOrProgram_(text)) return false;

  // 採用ラインから遠すぎる記事は、カテゴリ枠だけでは救済しない。
  // ただし、地域戦略インフラや公共影響が明確な記事は少し広めに見る。
  let margin = 12;

  if (
    bucket === "regional_strategy_infrastructure" ||
    bucket === "public_service_infrastructure" ||
    _hasConcreteConsequenceSignal_(text)
  ) {
    margin = 18;
  }

  if (rankingScore >= Number(dailyCutoffScore || 0) - margin) {
    return true;
  }

  // 元スコアが十分高い記事は、ランキング補正で沈んでいても救済候補に残す。
  if (score >= 58) {
    return true;
  }

  // 低スコア救済は乱発しない。日次採用ラインから離れすぎていない場合だけ。
  if (
    _isEditorialLowScoreRescueCandidate_((item && item.row) || [], item) &&
    rankingScore >= Number(dailyCutoffScore || 0) - 22
  ) {
    return true;
  }

  return false;
}

function _dynamicPortfolioReserveStrength_(item, bucket) {
  const row = (item && item.row) || [];
  const text = _selectionRowTextForRanking_(row);

  let strength = Number(
    (item && item.rankingScore) || (item && item.score) || 0,
  );

  if (Number((item && item.topicRank) || 0) === 1) strength += 5;
  if (Number((item && item.representative) || 0) >= 80) strength += 3;

  if (_hasConcreteConsequenceSignal_(text)) strength += 6;
  if (_hasCrossBorderHumanImpactSignal_(text)) strength += 5;
  if (_hasOperationalPublicServiceSignal_(text)) strength += 5;
  if (_hasInstitutionalAccountabilitySignal_(text)) strength += 5;
  if (_hasPublicMeaningExceptionSignal_(text, {})) strength += 4;

  if (_hasRegionalStrategicInfrastructureSignal_(text)) strength += 6;
  if (_hasDisasterRecoveryPublicAidSignal_(text)) strength += 5;
  if (_hasAgricultureProductionLivelihoodSignal_(text)) strength += 4;
  if (_hasTransportSafetyPublicSignal_(text)) strength += 4;
  if (_hasInternationalSecurityCooperationSignal_(text)) strength += 4;

  if (!_hasMyanmarRelevanceForSelection_(text)) {
    if (_hasRegionalStrategicInfrastructureSignal_(text)) {
      strength -= 3;
    } else {
      strength -= 14;
    }
  }

  if (_isLocalSecurityIncidentWithoutStrategicConsequence_(text)) {
    strength -= 8;
  }

  if (_isNewsRoundupOrProgram_(text)) {
    strength -= 20;
  }

  return strength;
}

function _dynamicPortfolioReserveCap_(bucket, headLimit) {
  const q = Number(headLimit || 0);

  // これは「目標比率」ではなく「最大でもここまで」という安全上限。
  if (bucket === "regional_strategy_infrastructure") return 1;
  if (bucket === "society_culture_exception") return 1;

  if (bucket === "security_border_humanitarian") {
    return Math.max(1, Math.round(q * 0.16));
  }

  if (bucket === "economy_livelihood") {
    return Math.max(1, Math.round(q * 0.22));
  }

  if (bucket === "politics_diplomacy_rights") {
    return Math.max(1, Math.round(q * 0.2));
  }

  if (bucket === "public_service_infrastructure") {
    return Math.max(1, Math.round(q * 0.16));
  }

  if (bucket === "accountability_crime") {
    return Math.max(1, Math.round(q * 0.14));
  }

  return Math.max(1, Math.round(q * 0.12));
}

function _isBroadPortfolioBucket_(bucket) {
  return (
    bucket === "economy_livelihood" ||
    bucket === "politics_diplomacy_rights" ||
    bucket === "public_service_infrastructure" ||
    bucket === "security_border_humanitarian"
  );
}

function _dailySelectionQuota_(dayArticleCount) {
  return Math.min(30, Math.max(15, Math.round(dayArticleCount * 0.25)));
}

/************************************************************
 * Gemini APIキー取得・API呼び出し
 ************************************************************/

/**
 * selection.js 専用APIキー取得
 *
 * Script Properties:
 * - GEMINI_SELECTION_API_KEY
 */
function getSelectionGeminiApiKeyBundle_(sheetName, source, tag) {
  const props = PropertiesService.getScriptProperties();
  const apiKey = String(
    props.getProperty("GEMINI_SELECTION_API_KEY") || "",
  ).trim();

  if (!apiKey) {
    throw new Error(
      "Selection Gemini API key is not set. Set GEMINI_SELECTION_API_KEY in Script Properties.",
    );
  }

  return {
    apiKey: apiKey,
    propName: "GEMINI_SELECTION_API_KEY",
  };
}

/**
 * selection.js 専用Gemini API呼び出し
 */
function callSelectionGeminiWithKey_(apiKey, prompt, tag, propName) {
  if (!apiKey) {
    return "ERROR: Selection Gemini API key is empty";
  }

  // 1. primary Gemini
  const primary = _callSelectionGeminiModelWithKey_(
    apiKey,
    GEMINI_SELECTION_MODEL,
    prompt,
    tag + ":primaryGemini",
    propName,
  );

  if (primary.ok) {
    return primary.text;
  }

  Logger.log(
    "[selection-score] primary Gemini failed model=%s code=%s fallback=%s error=%s",
    GEMINI_SELECTION_MODEL,
    primary.code,
    _shouldSelectionFallbackToOtherModel_(primary),
    String(primary.error || "").slice(0, 300),
  );

  // 429 / 503 / high demand 系だけ、別モデルへ逃がす。
  if (!_shouldSelectionFallbackToOtherModel_(primary)) {
    return primary.error;
  }

  Utilities.sleep(SELECT_MODEL_FALLBACK_WAIT_MS);

  // 2. Gemini 2.5 Flash に1回だけフォールバック
  const gemini25 = _callSelectionGeminiModelWithKey_(
    apiKey,
    GEMINI_SELECTION_FALLBACK_MODEL,
    prompt,
    tag + ":fallbackGemini25Flash",
    propName,
  );

  if (gemini25.ok) {
    Logger.log(
      "[selection-score] fallback Gemini succeeded model=%s tag=%s",
      GEMINI_SELECTION_FALLBACK_MODEL,
      tag,
    );
    return gemini25.text;
  }

  Logger.log(
    "[selection-score] fallback Gemini failed model=%s code=%s error=%s",
    GEMINI_SELECTION_FALLBACK_MODEL,
    gemini25.code,
    String(gemini25.error || "").slice(0, 300),
  );

  // 3. Gemini 2.5 Flash も失敗したら OpenAI gpt-5-mini へ1回だけフォールバック
  const openai = _callSelectionOpenAiGpt5Mini_(prompt, tag + ":fallbackOpenAI");

  if (openai.ok) {
    Logger.log("[selection-score] OpenAI fallback succeeded tag=%s", tag);
    return openai.text;
  }

  return (
    "ERROR: Selection all model fallbacks failed. " +
    "primary=" +
    String(primary.error || "").slice(0, 180) +
    " / gemini25=" +
    String(gemini25.error || "").slice(0, 180) +
    " / openai=" +
    String(openai.error || "").slice(0, 180)
  );
}

/**
 * 指定したGeminiモデルへ1回だけ呼び出す。
 */
function _callSelectionGeminiModelWithKey_(
  apiKey,
  modelName,
  prompt,
  tag,
  propName,
) {
  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/" +
    encodeURIComponent(modelName) +
    ":generateContent";

  const payload = {
    contents: [
      {
        role: "user",
        parts: [
          {
            text: String(prompt || ""),
          },
        ],
      },
    ],
    generationConfig: {
      temperature: 0.2,
      topP: 0.9,
      maxOutputTokens: SELECT_MAX_OUTPUT_TOKENS,
      responseMimeType: "application/json",
    },
  };

  const options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "x-goog-api-key": apiKey,
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    _waitBeforeSelectionGeminiCall_();

    const res = UrlFetchApp.fetch(url, options);
    const code = res.getResponseCode();
    const body = res.getContentText();

    if (code < 200 || code >= 300) {
      return {
        ok: false,
        code: code,
        provider: "gemini",
        model: modelName,
        body: body,
        error:
          "ERROR: Selection Gemini HTTP " +
          code +
          " model=" +
          modelName +
          " prop=" +
          String(propName || "") +
          " body=" +
          body.slice(0, 500),
      };
    }

    const json = JSON.parse(body);

    const text =
      json &&
      json.candidates &&
      json.candidates[0] &&
      json.candidates[0].content &&
      json.candidates[0].content.parts &&
      json.candidates[0].content.parts[0] &&
      json.candidates[0].content.parts[0].text;

    if (!text) {
      return {
        ok: false,
        code: code,
        provider: "gemini",
        model: modelName,
        body: body,
        error:
          "ERROR: Selection Gemini empty response model=" +
          modelName +
          " body=" +
          body.slice(0, 500),
      };
    }

    return {
      ok: true,
      code: code,
      provider: "gemini",
      model: modelName,
      text: text,
    };
  } catch (e) {
    return {
      ok: false,
      code: 0,
      provider: "gemini",
      model: modelName,
      error:
        "ERROR: Selection Gemini fetch failed model=" +
        modelName +
        " " +
        String(e).slice(0, 500),
    };
  }
}

/**
 * Gemini側で429 / 503 / high demand系なら別モデル・別プロバイダへ逃がす。
 */
function _shouldSelectionFallbackToOtherModel_(result) {
  if (!result) return false;

  const code = Number(result.code || 0);
  if (code === 429 || code === 503) return true;

  const text = [result.error, result.body].join("\n");

  if (text.indexOf('"code": 429') !== -1) return true;
  if (text.indexOf('"code": 503') !== -1) return true;
  if (text.indexOf("high demand") !== -1) return true;
  if (text.indexOf("currently experiencing high demand") !== -1) return true;
  if (text.indexOf("rate limit") !== -1) return true;
  if (text.indexOf("RESOURCE_EXHAUSTED") !== -1) return true;
  if (text.indexOf("UNAVAILABLE") !== -1) return true;

  return false;
}

/**
 * Gemini APIの1分あたり呼び出し回数を抑制する。
 * Script Properties の GEMINI_SELECTION_TARGET_RPM があればそれを優先する。
 */
function _waitBeforeSelectionGeminiCall_() {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const props = PropertiesService.getScriptProperties();

    const rpmFromProp = Number(
      props.getProperty("GEMINI_SELECTION_TARGET_RPM") || "",
    );

    const targetRpm =
      !isNaN(rpmFromProp) && rpmFromProp > 0
        ? rpmFromProp
        : SELECT_GEMINI_TARGET_RPM;

    const minIntervalMs = Math.ceil(60000 / targetRpm);
    const now = Date.now();
    const last = Number(
      props.getProperty(SELECT_GEMINI_RATE_LAST_CALL_PROP) || 0,
    );

    const waitMs = Math.max(0, last + minIntervalMs - now);

    if (waitMs > 0) {
      Utilities.sleep(waitMs);
    }

    props.setProperty(SELECT_GEMINI_RATE_LAST_CALL_PROP, String(Date.now()));
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}

/**
 * Gemini fallbackも失敗した場合にOpenAI gpt-5-miniへ1回だけ投げる。
 */
function _callSelectionOpenAiGpt5Mini_(prompt, tag) {
  const props = PropertiesService.getScriptProperties();
  const apiKey = String(props.getProperty(OPENAI_API_KEY_PROP) || "").trim();

  if (!apiKey) {
    return {
      ok: false,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      error:
        "ERROR: OpenAI API key is not set. Set " +
        OPENAI_API_KEY_PROP +
        " in Script Properties.",
    };
  }

  const url = "https://api.openai.com/v1/responses";

  const payload = {
    model: GPT5_MINI_MODEL,
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text: String(prompt || ""),
          },
        ],
      },
    ],
    text: {
      format: {
        type: "json_object",
      },
    },
    reasoning: {
      effort: "minimal",
    },
    max_output_tokens: SELECT_MAX_OUTPUT_TOKENS,
    store: false,
  };

  const options = {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + apiKey,
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    const res = UrlFetchApp.fetch(url, options);
    const code = res.getResponseCode();
    const body = res.getContentText();

    if (code < 200 || code >= 300) {
      return {
        ok: false,
        code: code,
        provider: "openai",
        model: GPT5_MINI_MODEL,
        body: body,
        error:
          "ERROR: OpenAI HTTP " +
          code +
          " model=" +
          GPT5_MINI_MODEL +
          " body=" +
          body.slice(0, 500),
      };
    }

    const json = JSON.parse(body);
    const text = _extractSelectionOpenAiResponseText_(json);

    if (!text) {
      return {
        ok: false,
        code: code,
        provider: "openai",
        model: GPT5_MINI_MODEL,
        body: body,
        error:
          "ERROR: OpenAI empty response model=" +
          GPT5_MINI_MODEL +
          " body=" +
          body.slice(0, 500),
      };
    }

    return {
      ok: true,
      code: code,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      text: text,
    };
  } catch (e) {
    return {
      ok: false,
      code: 0,
      provider: "openai",
      model: GPT5_MINI_MODEL,
      error:
        "ERROR: OpenAI fetch failed model=" +
        GPT5_MINI_MODEL +
        " " +
        String(e).slice(0, 500),
    };
  }
}

/**
 * OpenAI Responses APIの返却JSONからテキストを取り出す。
 */
function _extractSelectionOpenAiResponseText_(json) {
  if (!json) return "";

  if (typeof json.output_text === "string" && json.output_text.trim()) {
    return json.output_text.trim();
  }

  const output = Array.isArray(json.output) ? json.output : [];

  for (let i = 0; i < output.length; i++) {
    const item = output[i];
    if (!item) continue;

    const content = Array.isArray(item.content) ? item.content : [];

    for (let j = 0; j < content.length; j++) {
      const part = content[j];
      if (!part) continue;

      if (typeof part.text === "string" && part.text.trim()) {
        return part.text.trim();
      }

      if (
        part.type === "output_text" &&
        typeof part.text === "string" &&
        part.text.trim()
      ) {
        return part.text.trim();
      }
    }
  }

  return "";
}

/**
 * selection.js 専用Gemini設定の確認用
 * APIキー本体はログに出さない
 */
function checkSelectionGeminiScriptProperties_() {
  const props = PropertiesService.getScriptProperties();

  const geminiApiKey = String(
    props.getProperty("GEMINI_SELECTION_API_KEY") || "",
  ).trim();

  const openAiApiKey = String(
    props.getProperty(OPENAI_API_KEY_PROP) || "",
  ).trim();

  const rpm = String(
    props.getProperty("GEMINI_SELECTION_TARGET_RPM") ||
      SELECT_GEMINI_TARGET_RPM,
  ).trim();

  Logger.log("Selection Gemini primary model: " + GEMINI_SELECTION_MODEL);
  Logger.log(
    "Selection Gemini fallback model: " + GEMINI_SELECTION_FALLBACK_MODEL,
  );
  Logger.log("Selection OpenAI fallback model: " + GPT5_MINI_MODEL);
  Logger.log("Selection target RPM: " + rpm);
  Logger.log("Selection Gemini API key: " + (geminiApiKey ? "set" : "not set"));
  Logger.log("OpenAI API key: " + (openAiApiKey ? "set" : "not set"));
}

/************************************************************
 * 共通ヘルパー
 ************************************************************/

/**
 * Gemini応答からJSONオブジェクトを取り出す
 */
function _parseSelectionJsonObjectFromModelResponse_(resp) {
  let text = String(resp || "").trim();

  text = text
    .replace(/^\uFEFF/, "")
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(text);
  } catch (e1) {}

  const jsonText = _extractFirstBalancedSelectionJsonObject_(text);

  if (!jsonText) {
    throw new Error("JSON object not found. head=" + text.slice(0, 160));
  }

  try {
    return JSON.parse(jsonText);
  } catch (e2) {
    throw new Error(
      String(e2) +
        " / jsonHead=" +
        jsonText.slice(0, 160) +
        " / jsonTail=" +
        jsonText.slice(-160),
    );
  }
}

function _extractFirstBalancedSelectionJsonObject_(text) {
  const s = String(text || "");
  const start = s.indexOf("{");
  if (start === -1) return "";

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = start; i < s.length; i++) {
    const ch = s.charAt(i);

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "{") {
      depth++;
    } else if (ch === "}") {
      depth--;
      if (depth === 0) {
        return s.slice(start, i + 1);
      }
      if (depth < 0) return "";
    }
  }

  return "";
}

/**
 * topicKey正規化
 */
function _normalizeSelectionTopicKey_(key) {
  return String(key || "")
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^\p{L}\p{N}-]/gu, "")
    .slice(0, 120);
}

/**
 * sameDayTopicKey を日次グルーピング用に軽く正規化する。
 *
 * 方針:
 * - 固定トピック辞書で広くマージしすぎない。
 * - モデルキーに含まれがちな一般語・媒体性の低い語だけを落とす。
 * - トピックの具体性を残し、初見記事でも過度に同一化しない。
 */
function _mergeRelatedTopicKeys_(normalizedKey) {
  const key = String(normalizedKey || "").trim();
  if (!key) return "";

  // 固定トピック辞書ではなく、モデルキーのノイズだけを落とす。
  // 目的は「広くまとめすぎること」を避けること。
  const stopTokens = [
    "myanmar",
    "burma",
    "junta",
    "sac",
    "news",
    "article",
    "report",
    "update",
    "latest",
    "today",
    "ミャンマー",
    "ビルマ",
    "軍政",
    "国軍",
    "記事",
    "報道",
    "発表",
    "関連",
    "最新",
  ];

  const tokens = key
    .split("-")
    .map(function (x) {
      return String(x || "").trim();
    })
    .filter(function (x) {
      if (!x) return false;
      if (/^[0-9０-９]+$/.test(x)) return false;
      if (/^n+$/.test(x)) return false;
      return stopTokens.indexOf(x) === -1;
    });

  if (!tokens.length) return key;

  // 具体性を残すため、短くしすぎない。
  return tokens.slice(0, 10).join("-").slice(0, 120);
}

/**
 * 日付キー化
 */
function _selectionDateKey_(dateVal) {
  if (!dateVal) return "";

  const tz = Session.getScriptTimeZone();

  if (dateVal instanceof Date) {
    return Utilities.formatDate(dateVal, tz, "yyyyMMdd");
  }

  const s = String(dateVal || "").trim();
  if (!s) return "";

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return Utilities.formatDate(d, tz, "yyyyMMdd");
  }

  return s.replace(/[^\d]/g, "").slice(0, 8);
}

/**
 * キーワードスコア。上限あり。
 */
function _keywordScoreCapped_(text, keywords, perHit, cap) {
  let s = 0;

  keywords.forEach(function (kw) {
    if (String(text || "").indexOf(kw) !== -1) {
      s += perHit;
    }
  });

  return Math.min(cap, s);
}

/**
 * いずれかのキーワードを含むか
 */
function _hasAny_(text, keywords) {
  const s = String(text || "");

  return keywords.some(function (kw) {
    return s.indexOf(kw) !== -1;
  });
}

/**
 * 数値化
 */
function _safeNumber_(v) {
  const n = Number(v);
  if (isNaN(n)) return 0;
  return n;
}

/**
 * 丸めて範囲内に収める
 */
function _clampRound_(v, min, max) {
  const n = Math.round(Number(v) || 0);
  return Math.max(min, Math.min(max, n));
}
