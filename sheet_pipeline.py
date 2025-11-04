# scripts/mna_sheet_pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, json, unicodedata, shutil
import threading
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Protocol, runtime_checkable, cast
from typing import List, Dict
import time
import os
import re

import logging, contextlib, time

def _setup_logger():
    level = (os.getenv("MNA_LOG_LEVEL") or "INFO").upper()
    try:
        level_val = getattr(logging, level)
    except Exception:
        level_val = logging.INFO
    logging.basicConfig(
        level=level_val,
        format="%(asctime)s %(levelname)s %(message)s",
    )
_setup_logger()

@contextlib.contextmanager
def _timeit(section: str, **fields):
    meta = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    logging.info(f"▶ {section}... {meta}".rstrip())
    t0 = time.time()
    try:
        yield
        dt = time.time() - t0
        logging.info(f"✔ {section} done ({dt:.2f}s)")
    except Exception:
        logging.exception(f"✖ {section} failed")
        raise

# ===== 既存の fetch_articles.py から再利用できるものを拝借（※プロンプトは本ファイルで管理） =====
try:
    from fetch_articles import (
        MMT,                      # UTC+6:30
        call_gemini_with_retries, # Gemini呼び出し（既存のリトライ/レート制御）
        client_summary,
        deduplicate_by_url,
    )
except Exception:
    MMT = timezone(timedelta(hours=6, minutes=30))
    call_gemini_with_retries = None
    client_summary = None
    def deduplicate_by_url(items):
        seen, out = set(), []
        for it in items:
            u = (it.get("url") or "").strip()
            if u and u not in seen:
                out.append(it); seen.add(u)
        return out

# ===== 本文キャッシュ（bundle/bodies.json） & 本文取得  =====
_BODIES_LOCK = threading.Lock()

def _bodies_cache_path(out_dir: str) -> str:
    return os.path.join(out_dir, "bodies.json")

def _load_bodies_cache(out_dir: str) -> dict[str, dict]:
    p = _bodies_cache_path(out_dir)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_bodies_cache(out_dir: str, cache: dict[str, dict]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(_bodies_cache_path(out_dir), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

import requests
from bs4 import BeautifulSoup
try:
    # fetch_articles.py の実装を再利用
    from fetch_articles import (
        get_body_with_refetch,
        fetch_once_irrawaddy,
        extract_body_irrawaddy,
        extract_body_generic_from_soup,
        extract_body_mail_pdf_scoped,  # ★追加：より堅牢な抽出器
        translate_fulltexts_for_business,
        build_combined_pdf_for_business,
        _jp_date,
    )
except Exception:
    get_body_with_refetch = None
    fetch_once_irrawaddy = None
    extract_body_irrawaddy = None
    extract_body_generic_from_soup = None
    translate_fulltexts_for_business = None
    build_combined_pdf_for_business = None
    _jp_date = None

def _simple_fetch(url: str) -> str:
    r = requests.get(url, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def _get_body_once(url: str, source: str, out_dir: str, title: str = "") -> str:
    """
    1) bundle/bodies.json を見てあれば返す
    2) なければ取得→保存→返す
    """
    cache = _load_bodies_cache(out_dir)
    cached = cache.get(url)
    if cached and (cached.get("body") or "").strip():
        return cached["body"]

    # 取得（Irrawaddy は専用、それ以外は generic）
    if get_body_with_refetch is None:
        # フォールバック（極力通らない想定）
        body = ""
        try:
            html = _simple_fetch(url)
            soup = BeautifulSoup(html, "html.parser")
            body = " ".join(s.get_text(" ", strip=True) for s in soup.select("article, .content, .entry-content, .post-content")[:1])[:20000]
        except Exception:
            body = ""
    else:
        try:
            if "irrawaddy" in url.lower() or (source or "").lower() == "irrawaddy":
                html_fetcher = fetch_once_irrawaddy
                extractor    = extract_body_irrawaddy
            else:
                html_fetcher = _simple_fetch
                # ★DVB などで落ちにくい “メール/PDF 用の強化抽出器” を優先
                extractor    = extract_body_mail_pdf_scoped if 'extract_body_mail_pdf_scoped' in globals() else extract_body_generic_from_soup
            body = get_body_with_refetch(url, html_fetcher, extractor, retries=2, wait_seconds=2, quiet=True) or ""
        except Exception:
            body = ""

    with _BODIES_LOCK:
        cache = _load_bodies_cache(out_dir)  # 競合対策で再読込
        cache[url] = {"source": source, "title": title, "body": body}
        _save_bodies_cache(out_dir, cache)
    return body

# ===== 翻訳プロンプト：共通ルール（fetch_articles.py と同一） =====
PROMPT_TERMINOLOGY_RULES = (
    "【翻訳時の用語統一ルール（必ず従うこと）】\n"
    "このルールは記事タイトルと本文の翻訳に必ず適用してください。\n"
    "クーデター指導者⇒総司令官\n"
    "テロリスト指導者ミン・アウン・フライン⇒ミン・アウン・フライン\n"
    "テロリストのミン・アウン・フライン⇒ミン・アウン・フライン\n"
    "テロリスト軍事指導者⇒総司令官\n"
    "テロリスト軍事政権⇒軍事政権\n"
    "テロリスト軍事評議会⇒軍事政権\n"
    "テロリスト軍⇒国軍\n"
    "軍事評議会⇒軍事政権\n"
    "軍事委員会⇒軍事政権\n"
    "徴用⇒徴兵\n"
    "軍事評議会軍⇒国軍\n"
    "アジア道路⇒アジアハイウェイ\n"
    "来客登録⇒宿泊登録\n"
    "来客登録者⇒宿泊登録者\n"
    "タウンシップ⇒郡区\n"
    "北オークカラパ⇒北オカラッパ\n"
    "北オカラパ⇒北オカラッパ\n"
    "サリンギ郡区⇒タンリン郡区\n"
    "ネーピードー⇒ネピドー\n"
    "ファシスト国軍⇒国軍\n"
    "クーデター軍⇒国軍\n"
    "ミャンマー国民⇒ミャンマー人\n"
    "タディンユット⇒ダディンジュ\n"
    "ティティンジュット⇒ダディンジュ\n"
)
PROMPT_SPECIAL_RULES = (
    "【翻訳時の特別ルール】\n"
    "このルールも記事タイトルと本文の翻訳に必ず適用してください。\n"
    "「ဖမ်းဆီး」の訳語は文脈によって使い分けること。\n"
    "- 犯罪容疑や法律違反に対する文脈の場合は「逮捕」とする。\n"
    "- 犯罪容疑や法律違反に基づかない文脈の場合は「拘束」とする。\n"
)
PROMPT_CURRENCY_RULES = (
    "【通貨換算ルール】\n"
    "このルールも記事タイトルと本文の翻訳に必ず適用してください。\n"
    "ミャンマー通貨「チャット（Kyat、ကျပ်）」が出てきた場合は、日本円に換算して併記してください。\n"
    "- 換算レートは 1チャット = 0.037円 を必ず使用すること。\n"
    "- 記事中にチャットが出た場合は必ず「◯チャット（約◯円）」の形式に翻訳してください。\n"
    "- 日本円の表記は小数点以下は四捨五入してください（例: 16,500円）。\n"
    "- 他のレートは使用禁止。\n"
    "- チャット以外の通貨（例：タイの「バーツ」や米ドルなど）には適用しない。換算は行わないこと。\n"
)
COMMON_TRANSLATION_RULES = (
    PROMPT_TERMINOLOGY_RULES + "\n" +
    PROMPT_SPECIAL_RULES + "\n" +
    PROMPT_CURRENCY_RULES + "\n"
)

TITLE_OUTPUT_RULES = (
    "出力は見出し文だけを1行で返してください。\n"
    "【翻訳】や【日本語見出し案】、## 翻訳 などのラベル・注釈タグ・見出しは出力しないでください。\n"
)

# ===== ▼ プロンプト管理（見出し翻訳 / 本文要約）================================
# 見出し翻訳（見出し3案）※共通ルールを含む
HEADLINE_PROMPT_1 = (
    f"{COMMON_TRANSLATION_RULES}"
    f"{TITLE_OUTPUT_RULES}"
    "あなたは報道見出しの専門翻訳者です。以下の英語/ビルマ語のニュース見出しタイトルを、"
    "自然で簡潔な日本語見出しに翻訳してください。固有名詞は一般的な日本語表記を優先し、"
    "意訳しすぎず要点を保ち、記号の乱用は避けます。\n"
)

def make_headline_prompt_2_from(variant1_ja: str) -> str:
    """
    案1（日本語見出し）をインプットにして、要件に沿った案2を生成する。
    """
    return (
        f"{COMMON_TRANSLATION_RULES}"
        f"{TITLE_OUTPUT_RULES}"
        "以下は先に作成した日本語見出し（案1）です。\n"
        f"【案1】{variant1_ja}\n\n"
        "この案1を素材に、次の要件で新しい別案（案2）を1行で出力してください。\n"
        "・直訳ではなく、ニュース見出しとして自然な日本語にする\n"
        "・30文字以内で要点を端的に\n"
        "・主語・動作を明確に\n"
        "・重複語を避ける\n"
        "・報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）\n"
        "・「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける\n"
    )

# 本文から要素抽出して新聞見出しを作る（日本語1行）
HEADLINE_PROMPT_3 = (
    f"{COMMON_TRANSLATION_RULES}"
    f"{TITLE_OUTPUT_RULES}"
    "あなたは新聞社の見出しデスクです。以下の本文（原文／機械翻訳含む可能性あり）を読み、"
    "記事の要点（誰／どこ／何が起きた／規模・数値／結果／時点）を抽出し、"
    "自然で簡潔な**日本語の報道見出し**を1行で作成してください。\n"
    "ルール：\n"
    "- 主語と動作を明確に（曖昧表現や冗長な修飾は削除）\n"
    "- 重要な固有名詞・数値は優先して残す\n"
    "- 「〜と述べた」「〜が行われた」等の婉曲は避ける\n"
    "- 事実関係が曖昧な断定は避ける（推定語を最小限に）\n"
)

# ===== クリーニング手順（必要に応じて適用） =====
STEP12_FILTERS = (
    "Step 1: 例外チェック（最優先）\n"
    "Q1. タイトルや本文が“写真キャプション/クレジットのみ”“媒体名のみ”“出典/翻訳注記のみ”“記者名/配信ラベルのみ”“Datelineのみ”など、"
    "本文に相当しない要素だけの場合は本文扱いから除外する。\n\n"
    "Step 2: 入力クレンジング（本文抽出）\n"
    "- 写真キャプション/クレジット行（例:『写真:』『ဓာတ်ပုံ』『Photo』『(写真』『(Photo』『（写真』等で始まる）を除外\n"
    "- 媒体名だけの行（例: BBC Burmese / DVB / Myanmar Now 等）を除外\n"
    "- 出典や翻訳注記（例:『Source: …』『Translated by …』等）を除外\n"
    "- 記者名や配信ラベルだけの行（例: By … / Reuters / AP / SCMP 等）を除外\n"
    "- 発行地＋日付（Dateline）のみの行（例: 'Yangon, Sept. 30' / 'ネピドー、2024年2月15日' 等）を除外\n"
    "  ※行頭や本文冒頭のこれらも必ず除去する。\n"
    "- 連続する空行は1つに圧縮し、本文段落のみ残す。\n"
)

# ===== 統合タスク（本文要約） =====
STEP3_TASK = (
    "Step 3: 翻訳と要約処理\n"
    "以下のルールに従って、本文を要約してください。\n\n"
    f"{COMMON_TRANSLATION_RULES}"
    "本文要約：\n"
    "- 以下の記事本文について重要なポイントをまとめ、500字以内で具体的に要約してください。\n"
    "- 自然な日本語に翻訳してください。\n"
    "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
    "- レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n\n"
    "本文要約の出力条件：\n"
    "- 1行目は`【要約】`とだけしてください。\n"
    "- 2行目以降が全て空行になってはいけません。\n"
    "- 見出しや箇条書きを適切に使って整理してください。\n"
    "- 見出しや箇条書きにはマークダウン記号（#, *, - など）を使わず、単純なテキストとして出力してください。\n"
    "- 見出しは `[ ]` で囲んでください。\n"
    "- 空行は作らないでください。\n"
    "- 特殊記号は使わないでください（全体をHTMLとして送信するわけではないため）。\n"
    "- 箇条書きは`・`を使ってください。\n"
    "- 「【要約】」は1回だけ書き、途中や本文の末尾には繰り返さないでください。\n"
    "- 思考用の手順（Step 1/2/3、Q1/Q2、→ など）は出力に含めないこと。\n"
    "- 本文要約の合計は最大500文字以内に収めてください。\n\n"
)

def _build_summary_prompt(item: dict, *, body_max: int) -> str:
    """Gemini用の要約プロンプトを生成（Irrawaddy特例なし、超要約も削除済み）"""
    header = "次の手順で記事を判定・処理してください。\n\n"
    pre = STEP12_FILTERS + "\n\n"
    body = (item.get("body") or "")[:max(body_max, 0)]
    input_block = (
        "入力データ：\n"
        "###\n[記事タイトル]\n###\n"
        f"{item.get('title') or ''}\n\n"
        "[記事本文]\n###\n"
        f"{body}\n"
        "###\n"
    )
    return header + pre + STEP3_TASK + "\n" + input_block

# ===== 収集器：export_all_articles_to_csv.py から日付別収集関数を利用 =====
collectors_loaded = False
try:
    from tmp.export_all_articles_to_csv import (
        collect_mizzima_all_for_date,
        collect_bbc_all_for_date,
        collect_irrawaddy_all_for_date,
        collect_khitthit_all_for_date,
        collect_myanmar_now_mm_all_for_date,
        collect_dvb_all_for_date,
    )
    collectors_loaded = True
except Exception as e:
    collectors_loaded = False
    print(f"[error] collectors import failed: {e}", file=sys.stderr)

# ===== レートリミッタ（export_all_articles_to_csv.py の実装を再利用） =====
try:
    # 実体としての実装（見つからなければ None）
    from tmp.export_all_articles_to_csv import RateLimiter as _RateLimiterImpl
except Exception:
    _RateLimiterImpl = None

@runtime_checkable
class _RateLimiterProto(Protocol):
    rpm: int
    min_interval: float
    jitter: float
    def wait(self) -> None: ...

# 実行時に main() で初期化して、各 Gemini 呼び出しの直前で wait() する
_LIMITER: Optional[_RateLimiterProto] = None

# ===== Google Sheets 認証（Service Account） ====
import gspread
from google.oauth2.service_account import Credentials
SHEET_ID = os.getenv("MNA_SHEET_ID")
SHEET_NAME = os.getenv("MNA_SHEET_NAME")

def _gc_client():
    """
    認証の優先順:
      1) GOOGLE_SERVICE_ACCOUNT_FILE（今回のワークフローで出力する一時ファイル）
      2) GOOGLE_APPLICATION_CREDENTIALS（GCP標準）
      3) GOOGLE_SERVICE_ACCOUNT_JSON（JSON文字列）
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    file = (os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or "").strip()
    if file:
        creds = Credentials.from_service_account_file(file, scopes=scopes)
        return gspread.authorize(creds)

    app_cred = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if app_cred:
        creds = Credentials.from_service_account_file(app_cred, scopes=scopes)
        return gspread.authorize(creds)

    info = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if info:
        try:
            creds = Credentials.from_service_account_info(json.loads(info), scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            raise SystemExit(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON (JSON parse failed): {e}") from e

    raise SystemExit(
        "Google SA credential not found. "
        "Set one of: GOOGLE_SERVICE_ACCOUNT_FILE / GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SERVICE_ACCOUNT_JSON"
    )
    
# =========================
# 媒体→APIキー環境変数マップ
# =========================
# ここに必要な媒体を追加してください（大文字小文字や全角半角は _norm() で吸収します）
SOURCE_KEY_ENV_MAP: dict[str, str] = {
    # 例： "BBC" や "BBC Burmese" → GEMINI_API_KEY_BBC
    "bbc": "GEMINI_API_KEY_BBC",
    "bbc burmese": "GEMINI_API_KEY_BBC",
    "mizzima": "GEMINI_API_KEY_MIZZIMA",
    "khit thit": "GEMINI_API_KEY_KHITTHIT",
    "myanmar now": "GEMINI_API_KEY_MYANMARNOW",
    "dvb": "GEMINI_API_KEY_DVB",
    "irrawaddy": "GEMINI_API_KEY_IRRAWADDY",
}

_SPACE_RE = re.compile(r"\s+")

def _norm(s: str) -> str:
    """全角→半角/前後空白/連続空白を正規化して小文字化"""
    s = (s or "").strip()
    try:
        import unicodedata
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    s = _SPACE_RE.sub(" ", s)
    return s.lower()

# ===== Gemini キー =====
def _gemini_key_for_source(source: str) -> str | None:
    """
    媒体名から適切なAPIキー（環境変数の値）を返す。
    ※ GEMINI_API_KEY（グローバル）のフォールバックは一切使わない。
    見つからない場合は None を返す。
    """
    src = _norm(source)
    env_name = SOURCE_KEY_ENV_MAP.get(src)
    if not env_name:
        return None
    key = (os.getenv(env_name) or "").strip()
    return key or None

def _clip_body_for_headline(body: str, max_chars: int = 1200) -> str:
    """見出し生成用に本文を安全に切り詰める。段落の途中切りも許容。"""
    b = (body or "").strip()
    if len(b) <= max_chars:
        return b
    return b[:max_chars].rstrip()

# 既存定義を差し替え
def _headline_variants_ja(title: str, source: str, url: str, body: str = "") -> List[str]:
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        t = unicodedata.normalize("NFC", title or "").strip()
        return [t, t, t]

    os.environ["GEMINI_API_KEY"] = key
    model = os.getenv("GEMINI_HEADLINE_MODEL", "gemini-2.5-flash")

    # ---- 案1：原題ベース（変更なし）----
    try:
        if _LIMITER: _LIMITER.wait()
        resp1 = call_gemini_with_retries(
            client_summary,
            f"{HEADLINE_PROMPT_1}\n\n原題: {title}\nsource:{source}\nurl:{url}",
            model=model,
        )
        v1 = unicodedata.normalize("NFC", (resp1.text or "").strip())
    except Exception:
        v1 = unicodedata.normalize("NFC", (title or "").strip())

    # ---- 案2：案1を素材に再生成（変更なし／前回方針のまま）----
    try:
        if _LIMITER: _LIMITER.wait()
        prompt2 = make_headline_prompt_2_from(v1)
        resp2 = call_gemini_with_retries(client_summary, prompt2, model=model)
        v2 = unicodedata.normalize("NFC", (resp2.text or "").strip())
    except Exception:
        v2 = v1

    # ---- 案3：本文から要素抽出して新聞見出し化（今回の修正）----
    try:
        if _LIMITER: _LIMITER.wait()
        body_for_prompt = _clip_body_for_headline(body, max_chars=1200)
        if not body_for_prompt:
            # 本文が無いときは案1でフォールバック
            v3 = v1
        else:
            prompt3 = (
                f"{HEADLINE_PROMPT_3}\n\n"
                "【本文】\n" + body_for_prompt + "\n\n"
                f"（参考）原題: {title}\nsource:{source}\nurl:{url}\n"
            )
            resp3 = call_gemini_with_retries(client_summary, prompt3, model=model)
            v3 = unicodedata.normalize("NFC", (resp3.text or "").strip())
    except Exception:
        v3 = v1

    return [v1, v2, v3]

def _summary_ja(source: str, title: str, body: str, url: str) -> str:
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        # グローバルキー無し前提：媒体別キーが無い場合は安全フォールバック＋警告
        try:
            import sys
            print(f"[warn] Gemini key not configured for source='{source}'. "
                  f"Skipped API call; returned trimmed body.", file=sys.stderr)
        except Exception:
            pass
        return unicodedata.normalize("NFC", (body or "").strip())[:400]
    os.environ["GEMINI_API_KEY"] = key
    payload = {"source": source, "title": title, "url": url, "body": body}
    prompt = _build_summary_prompt(payload, body_max=1800)
    try:
        if _LIMITER:
            _LIMITER.wait()
        resp = call_gemini_with_retries(
            client_summary, prompt, model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-2.5-flash")
        )
        return unicodedata.normalize("NFC", (resp.text or "").strip())
    except Exception:
        return unicodedata.normalize("NFC", (body or "").strip())[:400]

def _is_ayeyarwady(title_ja: str, summary_ja: str) -> bool:
    """
    fetch_articles.py と同一の Ayeyarwady 判定ロジック（is_ayeyarwady_hit）を利用する。
    取りこぼしやバージョン差異を避けるため、まずは関数を import。
    もし import に失敗した場合のみ、fetch_articles 側のキーワードにフォールバック。
    """
    from unicodedata import normalize

    t = normalize("NFC", (title_ja or "").strip())
    b = normalize("NFC", (summary_ja or "").strip())

    try:
        # fetch_articles.py の本家ロジックを直接利用
        from fetch_articles import is_ayeyarwady_hit
        return bool(is_ayeyarwady_hit(t, b))
    except Exception:
        # フォールバック：fetch_articles.py と同じキーワード集合で判定
        try:
            from fetch_articles import AYEYARWADY_KEYWORDS
            kws = AYEYARWADY_KEYWORDS
        except Exception:
            # どうしても import できない環境用の最小セット（fetch_articles.py と同値ではないが安全側）
            kws = ["ဧရာဝတီတိုင်း", "Ayeyarwady Region", "Ayeyarwady region", "ayeyarwady region"]

        hay = f"{t} {b}"
        return any(kw in hay for kw in kws)

def _collect_all_for(target_date_mmt: date) -> List[Dict]:
    if not collectors_loaded:
        raise SystemExit("収集関数の読み込み失敗。export_all_articles_to_csv.py を配置してください。")
    items: List[Dict] = []
    for fn, kwargs in [
        # (collect_irrawaddy_all_for_date, {}),
        # (collect_bbc_all_for_date, {}),
        # (collect_khitthit_all_for_date, {"max_pages": 5}),
        (collect_dvb_all_for_date, {}),
        (collect_mizzima_all_for_date, {"max_pages": 3}),
        # (collect_myanmar_now_mm_all_for_date, {"max_pages": 3}),
    ]:
        name = fn.__name__.replace("_all_for_date", "")
        try:
            with _timeit(f"collector:{name}", date=target_date_mmt.isoformat(), kwargs=kwargs or None):
                before = len(items)
                fetched = fn(target_date_mmt, **kwargs)
                items.extend(fetched)
                logging.info(f"[collect:{name}] fetched={len(fetched)} total={len(items)}")
        except Exception as e:
            logging.exception(f"[warn] collector failed: {fn.__name__}: {e}")
    dedup_before = len(items)
    items = deduplicate_by_url(items)
    if dedup_before != len(items):
        logging.info(f"[collect] dedup {dedup_before} -> {len(items)} (-{dedup_before - len(items)})")
    else:
        logging.info(f"[collect] total={len(items)} (no dup)")
    return items

# ===== Sheets I/O =====
def _ws():
    return _gc_client().open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def _read_all_rows():
    ws = _ws(); vals = ws.get_all_values()
    header = vals[0] if vals else []
    rows = vals[1:] if len(vals) > 1 else []
    return header, rows, ws

def _existing_urls_set() -> set:
    header, rows, _ = _read_all_rows()
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_K = name_to_idx.get("URL", 10)
    urls = set()
    for r in rows:
        try:
            u = (r[idx_K] or "").strip()
            if u: urls.add(u)
        except Exception: pass
    return urls

def _append_rows(rows_to_append: List[List[str]]):
    if not rows_to_append:
        return
    header, rows, ws = _read_all_rows()
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_A = name_to_idx.get("日付", 0)  # A列（見出し "日付"）
    filled = sum(1 for r in rows if (r[idx_A] or "").strip())  # A列のみで判定
    start_row = 2 + filled
    logging.info(f"[sheet] append start_row=A{start_row} rows={len(rows_to_append)}")
    with _timeit("sheet.append", rows=len(rows_to_append), start_row=start_row):
        ws.update(f"A{start_row}", rows_to_append, value_input_option="USER_ENTERED")

def _keep_only_rows_of_date(date_str: str) -> int:
    """A列が date_str(YYYY-MM-DD) の行だけ残す (= 今日以外は A:J クリア & K=FALSE)。戻り値=削除行数"""
    header, rows, ws = _read_all_rows()
    if not rows: return 0
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_A = name_to_idx.get("日付", 1)  # A列（見出し "日付"）
    kept = []
    for r in rows:
        try:
            if (r[idx_A] or "").strip() == date_str:
                kept.append(r[:10])  # A..J を保持
        except Exception:
            pass

    total = len(rows)

    # A:J は一旦全クリア
    ws.batch_clear(["A2:J"])

    # 今日の行だけ A2 から詰め直す（A..J）
    if kept:
        ws.update("A2", kept, value_input_option="USER_ENTERED")

    # K列（採用フラグなど）は全行 FALSE にリセット（A..J が空の行も含む）
    if total > 0:
        ws.update(f"K2:K{total+1}", [["FALSE"]]*total, value_input_option="USER_ENTERED")

    return len(rows) - len(kept)

# ===== コマンド：収集→追記（16/18/20/22） =====
def cmd_collect_to_sheet(args):
    now_mmt = datetime.now(MMT)
    target = now_mmt.date()
    
    logging.info(f"[collect] start target_date_mmt={target.isoformat()} clear_yesterday={getattr(args,'clear_yesterday',False)}")

    if getattr(args, "clear_yesterday", False):
        today = now_mmt.date().isoformat()
        removed = _keep_only_rows_of_date(today)
        with _timeit("clear-yesterday", date=today):
            removed = _keep_only_rows_of_date(today)
        logging.info(f"[clean] kept only {today} (removed {removed} rows)")

    items = _collect_all_for(target)
    if not items:
        logging.warning("[collect] no items to write")
        print("no items to write"); return

    existing = _existing_urls_set()
    logging.info(f"[sheet] existing_urls={len(existing)}")
    ts = datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S")
    rows_to_append = []
    bundle_dir = getattr(args, "bundle_dir", "bundle")
    for it in items:
        source = it.get("source") or ""
        title  = it.get("title") or ""
        url    = (it.get("url") or "").strip()
        if not url or url in existing:
            logging.debug(f"[skip] url empty or duplicated url={url!r}")
            continue
        # 本文はここで1回取得してキャッシュへ保存 → 以後の処理（要約/見出し/全文PDF）で共用
        body   = _get_body_once(url, source, out_dir=bundle_dir, title=title)

        with _timeit("headline-variants", source=source):
            f, g, h = _headline_variants_ja(title, source, url, body)
        with _timeit("summary", source=source):
            summ    = _summary_ja(source, title, body, url)
            # summ = ""
        # 異常時のみ “最後の【要約】抽出＋手順行除去” を適用（存在すれば）
        try:
            from fetch_articles import normalize_summary_text
            summ = normalize_summary_text(summ)
        except Exception:
            pass
        is_ay   = _is_ayeyarwady(f, summ)
        logging.info(f"[row] source={source} ayeyarwady={is_ay} url={url}")

        rows_to_append.append([
            target.isoformat(),          # A 日付(配信日)
            ts,                          # B timestamp
            source,                      # C メディア
            "TRUE" if is_ay else "FALSE",# D エーヤワディー
            f, g, h,                     # E/F/G 見出し訳３案
            "",                          # H 確定見出し（手動）
            summ,                        # I 本文要約
            url,                         # J URL
        ])
        existing.add(url)

    _append_rows(rows_to_append)
    print(f"appended {len(rows_to_append)} rows")

# ===== コマンド：シート→bundle再生成（02:30） =====
def cmd_build_bundle_from_sheet(args):
    # ① 読み込み
    with _timeit("build-bundle:read"):
        header, rows, _ = _read_all_rows()
    if not rows:
        logging.warning("[bundle] no rows")
        print("no rows"); return
    logging.info(f"[bundle] rows_total={len(rows)} (採用フラグ=TRUEのみ抽出)")

    # 列名に依存しないよう、列記号(A..K)を固定でインデックス化して参照する
    col = {chr(ord('A')+i): i for i in range(len(header))}  # A:0, B:1, ... K:10
    get = lambda r, key, default="": (r[col.get(key, -1)] if col.get(key, -1) >= 0 else default).strip()

    MEDIA_ORDER = [
        "Mizzima (Burmese)", "BBC Burmese", "Irrawaddy",
        "Khit Thit Media", "Myanmar Now", "DVB",
    ]
    order = {m: i for i, m in enumerate(MEDIA_ORDER)}

    # ② 採用フラグで選別（件数・媒体内訳をログ）
    from collections import defaultdict
    with _timeit("build-bundle:select"):
        selected = []
        media_counts = defaultdict(int)
        for r in rows:
            if get(r, "K").upper() != "TRUE":  # 採用フラグ(K)
                continue
            media = get(r, "C")  # メディア(C)
            selected.append((order.get(media, 999), r))
            media_counts[media] += 1
        selected.sort(key=lambda x: x[0])

    total_selected = len(selected)
    if total_selected == 0:
        logging.info("[bundle] no summaries selected (L=TRUE)")
        print("no summaries selected (L=TRUE)"); return

    # 媒体別内訳（INFO）
    breakdown = ", ".join(f"{k}:{v}" for k, v in sorted(media_counts.items(), key=lambda x: order.get(x[0], 999)))
    logging.info(f"[bundle] selected={total_selected} by_media=({breakdown})")

    # ③ summaries 構築
    with _timeit("build-bundle:construct", selected=total_selected):
        summaries = []
        for _, r in selected:
            delivery    = get(r, "A")                        # 日付(A)
            media       = get(r, "C")                        # メディア(C)
            title_final = get(r, "H")                        # 確定見出し日本語訳(H)
            body_sum    = get(r, "I")                        # 本文要約(I)
            url         = get(r, "J")                        # URL(J)
            is_ay       = (get(r, "D").upper() == "TRUE")    # エーヤワディー(D)
            summaries.append({
                "source": media,
                "url": url,
                "title": unicodedata.normalize("NFC", title_final),
                "summary": unicodedata.normalize("NFC", body_sum),
                "is_ayeyarwady": is_ay,
                "date_mmt": delivery,
            })

    out_dir = os.path.abspath(args.bundle_dir)

    # ④ 書き出し（既存ディレクトリの再作成も含めて計測）
    with _timeit("build-bundle:write", out_dir=out_dir, items=len(summaries)):
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        meta = {
            "date_mmt": summaries[0]["date_mmt"],
            "generated_from": "sheet",
            "generated_at_mmt": datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        with open(os.path.join(out_dir, "summaries.json"), "w", encoding="utf-8") as f:
            json.dump(summaries, f, ensure_ascii=False, indent=2)

    logging.info(f"[bundle] rebuilt dir={out_dir} items={len(summaries)} date_mmt={summaries[0]['date_mmt']}")
    print(f"bundle rebuilt: {out_dir} (items={len(summaries)})")

    # ★ここで全文翻訳PDFを生成（BUSINESS/TRIAL 添付用）
    try:
        _make_business_pdf_from_summaries(
            summaries=summaries,
            date_iso=summaries[0]["date_mmt"],
            out_dir=out_dir,
        )
    except Exception as e:
        logging.warning(f"[bundle] business PDF generation skipped: {e}")

    # キャッシュ済み本文を使って全文翻訳PDFを生成（BUSINESS/TRIAL 添付用）
    def _make_business_pdf_from_summaries(summaries: list[dict], date_iso: str, out_dir: str):
        if not (translate_fulltexts_for_business and build_combined_pdf_for_business):
            logging.warning("[bundle] PDF helpers unavailable; skip business PDF")
            return
        cache = _load_bodies_cache(out_dir)
        urls_in_order: list[str] = []
        url_to_source_title_body: dict[str, dict] = {}
        for s in summaries:
            url = (s.get("url") or "").strip()
            if not url:
                continue
            source = s.get("source") or ""
            title  = s.get("title") or ""
            urls_in_order.append(url)
            body = (cache.get(url) or {}).get("body", "")
            if not body:
                body = _get_body_once(url, source, out_dir=out_dir, title=title)
            url_to_source_title_body[url] = {"source": source, "title": title, "body": body}
        if not urls_in_order:
            logging.info("[bundle] no urls for PDF")
            return
        translated = translate_fulltexts_for_business(urls_in_order, url_to_source_title_body)
        if not translated:
            logging.warning("[bundle] translation returned empty; skip PDF")
            return
        pdf_bytes = build_combined_pdf_for_business(translated)
        # 添付ファイル名（日本語日付）
        try:
            y, m, d = map(int, (date_iso or "").split("-"))
            jp = _jp_date(date(y, m, d)) if _jp_date else date_iso
        except Exception:
            jp = date_iso
        attachment_name = f"ミャンマーニュース全文訳【{jp}】.pdf"
        with open(os.path.join(out_dir, "digest.pdf"), "wb") as f:
            f.write(pdf_bytes)
        with open(os.path.join(out_dir, "attachment_name.txt"), "w", encoding="utf-8") as f:
            f.write(attachment_name)
        logging.info(f"[bundle] wrote business PDF: {attachment_name} ({len(pdf_bytes)} bytes)")

    try:
        _make_business_pdf_from_summaries(summaries, meta["date_mmt"], out_dir)
    except Exception as e:
        logging.warning(f"[bundle] failed to build business pdf: {e}")

# ===== CLI =====
import argparse
def main():
    p = argparse.ArgumentParser(description="MNA sheet pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("collect-to-sheet", help="収集→要約→sheet追記（16/18/20/22）")
    p1.add_argument("--clear-yesterday", action="store_true", help="前日分だけA2:Jから除去（16:00用）")
    p1.add_argument("--bundle-dir", default="bundle", help="本文キャッシュ/成果物の保存先（既定: bundle）")
    # === Gemini free tier を想定したレート設定（CLI指定 > 環境変数 > 既定）===
    p1.add_argument("--rpm", type=int, default=int(os.getenv("GEMINI_REQS_PER_MIN", "9")))
    p1.add_argument("--min-interval", type=float, default=float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "2.0")))
    p1.add_argument("--jitter", type=float, default=float(os.getenv("GEMINI_JITTER_SEC", "0.3")))
    p1.set_defaults(func=cmd_collect_to_sheet)

    p2 = sub.add_parser("build-bundle", help="sheetからbundle生成（02:30）")
    p2.add_argument("--bundle-dir", default="bundle", help="bundle出力先")
    p2.set_defaults(func=cmd_build_bundle_from_sheet)

    args = p.parse_args()
    # レートリミッタ初期化（collect-to-sheet のときのみ意味がある）
    global _LIMITER
    if _RateLimiterImpl and args.cmd == "collect-to-sheet":
        _LIMITER = cast(_RateLimiterProto, _RateLimiterImpl(
            getattr(args, "rpm", int(os.getenv("GEMINI_REQS_PER_MIN", "9"))),
            getattr(args, "min_interval", float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "2.0"))),
            getattr(args, "jitter", float(os.getenv("GEMINI_JITTER_SEC", "0.3"))),
        ))
        msg = f"[rate] rpm={_LIMITER.rpm}, min_interval={_LIMITER.min_interval}s, jitter<= {_LIMITER.jitter}s"
        print(msg)
        logging.info(msg)
    else:
        _LIMITER = None
    args.func(args)

if __name__ == "__main__":
    main()
