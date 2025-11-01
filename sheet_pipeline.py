# scripts/mna_sheet_pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, json, unicodedata, shutil
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Protocol, runtime_checkable, cast
from typing import List, Dict
import time
import os
import re

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
    
    # ===== ▼ プロンプト管理（見出し翻訳 / 本文要約）================================
    # 見出し翻訳（見出し3案）※共通ルールを含む
    HEADLINE_PROMPT_1 = (
        f"{COMMON_TRANSLATION_RULES}"
        "以下のニュース見出しを、日本語で簡潔に自然な『新聞見出し』にしてください。\n"
        "固有名詞は一般的な日本語表記を優先し、句読点は最小限にしてください。\n"
    )
    HEADLINE_PROMPT_2 = (
        f"{COMMON_TRANSLATION_RULES}"
        "同じニュース見出しについて、語順や言い回しを変えた日本語見出しの別案を作成してください。\n"
        "要点は同じ、トーンは中立、簡潔にしてください。\n"
    )
    HEADLINE_PROMPT_3 = (
        f"{COMMON_TRANSLATION_RULES}"
        "同じ内容で三つ目の日本語見出し案を作成してください。\n"
        "冗長さを避け、読み手に要点がすぐ伝わる表現にしてください。\n"
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
        "以下のルールに従って、記事タイトルを自然な日本語に翻訳し、本文を要約してください。\n\n"
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

def _headline_variants_ja(title: str, source: str, url: str) -> List[str]:
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        t = unicodedata.normalize("NFC", title or "").strip()
        return [t, t, t]
    os.environ["GEMINI_API_KEY"] = key
    out = []
    for prompt in (HEADLINE_PROMPT_1, HEADLINE_PROMPT_2, HEADLINE_PROMPT_3):
        try:
            # free tier での瞬間上限を避けるため、呼び出し直前で待機
            if _LIMITER:
                _LIMITER.wait()
            resp = call_gemini_with_retries(
                client_summary,
                f"{prompt}\n\n原題: {title}\nsource:{source}\nurl:{url}",
                model=os.getenv("GEMINI_HEADLINE_MODEL", "gemini-2.5-flash"),
            )
            txt = (resp.text or "").strip()
        except Exception:
            txt = title
        out.append(unicodedata.normalize("NFC", txt))
    return out

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
    prompt = _build_summary_prompt(payload, skip_filters=False, body_max=1800)
    try:
        if _LIMITER:
            _LIMITER.wait()
        resp = call_gemini_with_retries(
            client_summary, prompt, model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-2.5-flash")
        )
        return unicodedata.normalize("NFC", (resp.text or "").strip())
    except Exception:
        return unicodedata.normalize("NFC", (body or "").strip())[:400]
    
# ===== タイトル＋本文要約を一括生成（必要に応じて利用） =====
def _parse_title_and_summaries(text: str) -> tuple[str | None, str | None]:
    """モデル出力から【タイトル】/【要約】を抽出して返す。"""
    t = (text or "").strip()
    title_ja = None
    summary = None
    for line in t.splitlines():
        line = line.strip()
        if line.startswith("【タイトル】") and title_ja is None:
            title_ja = line.replace("【タイトル】", "", 1).strip()
        elif line.startswith("【要約】") and summary is None:
            summary = ""
            found = True
            continue
        elif found:
            summary += line
    return title_ja, summary or None

def translate_title_and_summarize(source: str, title: str, body: str, url: str):
    """
    fetch_articles.py と同等ポリシーで、【タイトル】/【要約】を一括生成。
    返り値: dict(title_ja, summary_ja)
    """
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        return {
            "title_ja": (title or "").strip(),
            "summary_ja": (body or "").strip()[:400],
        }

    os.environ["GEMINI_API_KEY"] = key
    payload = {"source": source, "title": title, "url": url, "body": body}
    prompt = _build_summary_prompt(payload, skip_filters=False, body_max=1800)
    try:
        if _LIMITER:
            _LIMITER.wait()
        resp = call_gemini_with_retries(
            client_summary, prompt, model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-2.5-flash")
        )
        text = (resp.text or "").strip()
        t, s = _parse_title_and_summaries(text)
        return {
            "title_ja": t or (title or "").strip(),
            "summary_ja": s or (body or "").strip()[:400],
        }
    except Exception:
        return {
            "title_ja": (title or "").strip(),
            "summary_ja": (body or "").strip()[:400],
        }

def _is_ayeyarwady(title_ja: str, summary_ja: str) -> bool:
    try:
        from fetch_articles import is_ayeyarwady_article
        return bool(is_ayeyarwady_article(title_ja, summary_ja))
    except Exception:
        pass
    hay = (title_ja + " " + summary_ja).lower()
    for k in ["ayeyarwady", "irrawaddy", "pathein", "エーヤワディ", "イラワジ", "パテイン"]:
        if k.lower() in hay: return True
    return False

def _collect_all_for(target_date_mmt: date) -> List[Dict]:
    if not collectors_loaded:
        raise SystemExit("収集関数の読み込み失敗。export_all_articles_to_csv.py を配置してください。")
    items: List[Dict] = []
    for fn in (
        collect_mizzima_all_for_date,
        collect_bbc_all_for_date,
        collect_irrawaddy_all_for_date,
        collect_khitthit_all_for_date,
        collect_myanmar_now_mm_all_for_date,
        collect_dvb_all_for_date,
    ):
        try:
            items.extend(fn(target_date_mmt))
        except Exception as e:
            print(f"[warn] collector failed: {fn.__name__}: {e}")
    return deduplicate_by_url(items)

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
    if not rows_to_append: return
    header, rows, ws = _read_all_rows()
    start_row = 2 + len(rows)
    ws.update(f"A{start_row}", rows_to_append, value_input_option="RAW")

def _remove_rows_of_date(date_str: str) -> int:
    """A列が date_str(YYYY-MM-DD) の行のみ除去して A2:J を再構成。戻り値=削除行数"""
    header, rows, ws = _read_all_rows()
    if not rows: return 0
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_B = name_to_idx.get("日付", 1)
    kept = []
    for r in rows:
        try:
            if (r[idx_B] or "").strip() != date_str:
                kept.append(r[:10])
        except Exception:
            kept.append(r[:10])
    ws.batch_clear(["A2:J"])
    if kept:
        ws.update("A2", kept, value_input_option="RAW")
    return len(rows) - len(kept)

def _keep_only_rows_of_date(date_str: str) -> int:
    """A列が date_str(YYYY-MM-DD) の行だけ残す (= 今日以外を全削除)。戻り値=削除行数"""
    header, rows, ws = _read_all_rows()
    if not rows: return 0
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_B = name_to_idx.get("日付", 1)
    kept = []
    for r in rows:
        try:
            if (r[idx_B] or "").strip() == date_str:
                kept.append(r[:10])
        except Exception:
            # 形式不正な行は削除対象（= keptに入れない）
            pass
    ws.batch_clear(["A2:J"])
    if kept:
        ws.update("A2", kept, value_input_option="RAW")
    return len(rows) - len(kept)

# ===== コマンド：収集→追記（16/18/20/22） =====
def cmd_collect_to_sheet(args):
    now_mmt = datetime.now(MMT)
    target = now_mmt.date()

    if getattr(args, "clear_yesterday", False):
        today = now_mmt.date().isoformat()
        removed = _keep_only_rows_of_date(today)
        print(f"[clean] kept only {today} (removed {removed} rows)")

    items = _collect_all_for(target)
    if not items:
        print("no items to write"); return

    existing = _existing_urls_set()
    ts = datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S")
    rows_to_append = []
    for it in items:
        source = it.get("source") or ""
        title  = it.get("title") or ""
        url    = (it.get("url") or "").strip()
        if not url or url in existing: continue
        body   = it.get("body") or ""

        f, g, h = _headline_variants_ja(title, source, url)
        summ    = _summary_ja(source, title, body, url)
        is_ay   = _is_ayeyarwady(f, summ)

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
    header, rows, _ = _read_all_rows()
    if not rows: print("no rows"); return
    col = {n:i for i,n in enumerate(header)}
    get = lambda r, name, default="": (r[col.get(name, -1)] if col.get(name, -1) >= 0 else default).strip()

    MEDIA_ORDER = [
        "Mizzima (Burmese)", "BBC Burmese", "Irrawaddy",
        "Khit Thit Media", "Myanmar Now (mm)", "DVB",
    ]
    order = {m:i for i,m in enumerate(MEDIA_ORDER)}

    selected = []
    for r in rows:
        if (get(r, "採用フラグ").upper() != "TRUE"): continue
        media = get(r, "メディア")
        selected.append((order.get(media, 999), r))
    selected.sort(key=lambda x: x[0])

    summaries = []
    for _, r in selected:
        delivery   = get(r, "日付")
        media      = get(r, "メディア")
        title_final= get(r, "確定見出し日本語訳") or get(r, "見出し日本語訳①")
        body_sum   = get(r, "本文要約")
        url        = get(r, "URL")
        is_ay      = (get(r, "エーヤワディー").upper() == "TRUE")
        summaries.append({
            "source": media,
            "url": url,
            "title_ja": unicodedata.normalize("NFC", title_final),
            "summary_ja": unicodedata.normalize("NFC", body_sum),
            "is_ayeyarwady": is_ay,
            "date_mmt": delivery,
        })
    if not summaries:
        print("no summaries selected (L=TRUE)"); return

    out_dir = os.path.abspath(args.bundle_dir)
    if os.path.isdir(out_dir): shutil.rmtree(out_dir)
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
    print(f"bundle rebuilt: {out_dir} (items={len(summaries)})")

# ===== CLI =====
import argparse
def main():
    p = argparse.ArgumentParser(description="MNA sheet pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("collect-to-sheet", help="収集→要約→sheet追記（16/18/20/22）")
    p1.add_argument("--clear-yesterday", action="store_true", help="前日分だけA2:Jから除去（16:00用）")
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
        print(f"[rate] rpm={_LIMITER.rpm}, min_interval={_LIMITER.min_interval}s, jitter<= {_LIMITER.jitter}s")
    else:
        _LIMITER = None
    args.func(args)

if __name__ == "__main__":
    main()
