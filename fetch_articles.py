import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import parse as parse_date
import re

import os
import sys
from email.message import EmailMessage
from email.utils import formataddr
import unicodedata
from google import genai
from collections import defaultdict
import time
import json
import pprint as _pprint
import argparse
import pathlib
import random
from typing import List, Dict, Optional
from urllib.parse import urlparse  # 追加
from collections import deque
import base64
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from email.policy import SMTP
from email.header import Header
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from email.message import EmailMessage

try:
    import httpx
except Exception:
    httpx = None
try:
    import urllib3
except Exception:
    urllib3 = None

try:
    from google.api_core.exceptions import (
        ServiceUnavailable,
        ResourceExhausted,
        DeadlineExceeded,
        InternalServerError,
    )
except Exception:
    ServiceUnavailable = ResourceExhausted = DeadlineExceeded = InternalServerError = (
        Exception
    )

try:
    import zoneinfo  # 3.9+
except ImportError:
    from backports import zoneinfo  # 3.8系なら
    
# === Bundle I/O helpers for two-phase workflow ===
def _write_bundle(bundle_dir, date_mmt, summaries_non_ayeyar, pdf_bytes, attachment_name, subject="Daily Myanmar News Alert"):
    import json, pathlib
    b = pathlib.Path(bundle_dir)
    b.mkdir(parents=True, exist_ok=True)
    meta = {
        "subject": subject,
        "date_mmt": date_mmt.isoformat()
    }
    (b/"meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    (b/"summaries.json").write_text(json.dumps(summaries_non_ayeyar, ensure_ascii=False), encoding="utf-8")
    if pdf_bytes:
        (b/"digest.pdf").write_bytes(pdf_bytes)
    if attachment_name:
        (b/"attachment_name.txt").write_text(attachment_name, encoding="utf-8")
    print(f"[bundle] wrote to {b.resolve()}")

def _load_bundle(bundle_dir):
    import json, pathlib
    b = pathlib.Path(bundle_dir)
    meta = json.loads((b/"meta.json").read_text(encoding="utf-8"))
    summaries = json.loads((b/"summaries.json").read_text(encoding="utf-8"))
    pdf_path = b/"digest.pdf"
    pdf_bytes = pdf_path.read_bytes() if pdf_path.exists() else None
    attachment_name = (b/"attachment_name.txt").read_text(encoding="utf-8") if (b/"attachment_name.txt").exists() else None
    return meta, summaries, pdf_bytes, attachment_name

# --- helper: preserve newlines for Sheet Pipeline Send only ---
def _nl2br(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.replace("\n", "<br>")

# ========= Gemini リトライ調整用の定数 =========
GEMINI_MAX_RETRIES = 7          # 既定 5 → 7
GEMINI_BASE_DELAY = 10.0        # 既定 2.0 → 10.0
GEMINI_MAX_DELAY = 120.0        # 既定 30.0 → 120.0

# 翻訳のバッチサイズ（瞬間負荷を下げる）
TRANSLATION_BATCH_SIZE = 2      # 既定 3 → 2

# 乱数ジッター付き指数バックオフ
def _exp_backoff_sleep(attempt: int, base_delay: float, max_delay: float) -> float:
    """
    attempt: 0 origin (0,1,2,...)
    return: sleep seconds (指数バックオフ + 0〜1秒のジッター), max_delayでクリップ
    """
    import math, random
    # 2^attempt * base_delay を上限 max_delay でクリップ
    delay = min(max_delay, (2 ** attempt) * base_delay)
    # 0〜1秒の小さなジッターを加える（スパイク回避）
    return min(max_delay, delay + random.random())


# Gemini本番用
client_summary = genai.Client(api_key=os.getenv("GEMINI_API_SUMMARY_KEY"))
client_dedupe = genai.Client(api_key=os.getenv("GEMINI_API_DEDUPE_KEY"))
# === Gemini（全文翻訳用） ===
client_fulltext = genai.Client(api_key=os.getenv("GEMINI_API_FULLTEXT_KEY"))

def _is_retriable_exc(e: Exception) -> bool:
    msg = (str(e) or "").lower()
    name = e.__class__.__name__.lower()

    # Google系の明示的リトライ対象
    if isinstance(
        e,
        (ServiceUnavailable, ResourceExhausted, DeadlineExceeded, InternalServerError),
    ):
        return True

    # httpx/urllib3系（環境に無ければ無視）
    if httpx and isinstance(
        e,
        (
            getattr(httpx, "RemoteProtocolError", Exception),
            getattr(httpx, "ReadTimeout", Exception),
            getattr(httpx, "ConnectError", Exception),
        ),
    ):
        return True
    if urllib3 and isinstance(
        e,
        (
            urllib3.exceptions.ProtocolError,
            urllib3.exceptions.ReadTimeoutError,
            urllib3.exceptions.MaxRetryError,
        ),
    ):
        return True

    # 文字列での判定（実装差分吸収）
    hints = [
        "remoteprotocolerror",
        "servererror",
        "internal",  # "500 internal", "internal error" など
        "server disconnected",
        "unavailable",
        "500",
        "503",
        "502",
        "504",
        "gateway",
        "timeout",
        "temporar",
        "overload",
    ]
    if any(h in msg or h in name for h in hints):
        return True
    return False


# === Gemini 使用量ログ（入出力トークン） ======================================
def _usage_from_resp(resp):
    """
    google-genai のレスポンスから usage を取り出す（snake/camel双方に耐性）。
    戻り値: dict(prompt_token_count, candidates_token_count, total_token_count,
                cache_creation_input_token_count, cache_read_input_token_count)
    """
    usage = (
        getattr(resp, "usage_metadata", None)
        or getattr(resp, "usageMetadata", None)
        or {}
    )
    ud = {}
    if usage:
        get = usage.get if isinstance(usage, dict) else lambda k, d=None: getattr(usage, k, d)
        ud["prompt_token_count"] = get(
            "prompt_token_count", get("input_token_count", get("input_tokens", 0))
        )
        ud["candidates_token_count"] = get(
            "candidates_token_count",
            get("output_token_count", get("output_tokens", 0)),
        )
        ud["total_token_count"] = get(
            "total_token_count",
            get(
                "total_tokens",
                (ud.get("prompt_token_count", 0) or 0)
                + (ud.get("candidates_token_count", 0) or 0),
            ),
        )
        ud["cache_creation_input_token_count"] = get(
            "cache_creation_input_token_count", 0
        )
        ud["cache_read_input_token_count"] = get("cache_read_input_token_count", 0)
    return ud


def _log_gemini_usage(resp, *, tag: str = "gen", model: str = ""):
    """標準出力＋JSONLファイル(gemini_usage.log)へ入出力トークンを記録"""
    try:
        u = _usage_from_resp(resp) or {}
        rec = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "tag": tag,
            "model": model,
            **u,
        }
        print(
            "📊 TOKENS[{tag}] in={in_} out={out} total={tot} (cache create/read={cc}/{cr})".format(
                tag=tag,
                in_=rec.get("prompt_token_count", 0),
                out=rec.get("candidates_token_count", 0),
                tot=rec.get("total_token_count", 0),
                cc=rec.get("cache_creation_input_token_count", 0),
                cr=rec.get("cache_read_input_token_count", 0),
            )
        )
        try:
            with open("gemini_usage.log", "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            pass
    except Exception as e:
        print(f"⚠️ usage log failed: {e}")


# === Free tier monitor (10 Requests per Minute / 250 Requests per Day / 250k Tokens per Minute[input]) ======
# 追加ログ: 出力側の Tokens per Minute (output) も集計して表示する
class _FreeTierWatch:
    def __init__(self, rpm_limit=10, rpd_limit=250, tpm_limit=250_000):
        self.rpm_limit = int(os.getenv("GEMINI_FREE_RPM", rpm_limit))
        # “RPD” の略称は使わず、正式名称で扱う
        self.requests_per_day_limit = int(os.getenv("GEMINI_FREE_RPD", rpd_limit))
        # 無料枠のTPM判定は入力が基準
        self.tpm_limit = int(os.getenv("GEMINI_FREE_TPM", tpm_limit))

        self.req_times = deque()  # 直近60秒の成功リクエスト完了時刻
        self.tpm_in_points = deque()  # 直近60秒の (時刻, 入力トークン)
        self.tpm_out_points = deque()  # 直近60秒の (時刻, 出力トークン)
        self.day_key = None  # MMT 日付キー（UTC+6:30）
        self.requests_per_day_count = 0

        # “越えた瞬間だけ”通知するためのラッチ
        self._over_rpm = False
        self._over_tpm_in = False
        self._over_rpd = False

        # 毎回のレート窓スナップショット出力（標準出力のみ／既定ON）
        self._rate_window_log_enabled = str(
            os.getenv("GEMINI_RATE_WINDOW_LOG", "1")
        ).lower() not in ("0", "false", "off")

    def _mmt_today(self, now_utc):
        mmt = timezone(timedelta(hours=6, minutes=30))
        return now_utc.astimezone(mmt).date()

    def record(
        self,
        prompt_tokens: int,
        output_tokens: int = 0,
        *,
        tag: str = "gen",
        model: str = "",
    ):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)

        # 直近60秒窓（Requests per Minute / Tokens per Minute）
        self.req_times.append(now)
        self.tpm_in_points.append((now, int(prompt_tokens or 0)))
        self.tpm_out_points.append((now, int(output_tokens or 0)))
        cutoff = now - timedelta(seconds=60)
        while self.req_times and self.req_times[0] < cutoff:
            self.req_times.popleft()
        while self.tpm_in_points and self.tpm_in_points[0][0] < cutoff:
            self.tpm_in_points.popleft()
        while self.tpm_out_points and self.tpm_out_points[0][0] < cutoff:
            self.tpm_out_points.popleft()

        rpm = len(self.req_times)
        tpm_in = sum(tok for _, tok in self.tpm_in_points)
        tpm_out = sum(tok for _, tok in self.tpm_out_points)

        # Requests per Day — MMT日付でカウント
        today_mmt = self._mmt_today(now)
        if self.day_key != today_mmt:
            self.day_key = today_mmt
            self.requests_per_day_count = 0
            self._over_rpd = False  # 日またぎでリセット
        self.requests_per_day_count += 1  # この成功リクエストを計上

        # 超過判定（入力TPM/RPM/Requests per Day）
        over_rpm = rpm > self.rpm_limit
        over_tpm_in = tpm_in > self.tpm_limit
        over_rpd = self.requests_per_day_count > self.requests_per_day_limit

        def _emit_exceeded(kind_label: str, detail: str):
            # kind_label は正式名称で： "Requests per Minute" / "Tokens per Minute (input)" / "Requests per Day"
            print(
                f"🚩 FREE-TIER EXCEEDED [{kind_label}] {detail} | tag={tag} model={model}"
            )

        # 超過通知（正式名称）
        if over_rpm and not self._over_rpm:
            self._over_rpm = True
            _emit_exceeded(
                "Requests per Minute", f"{rpm}>{self.rpm_limit} within last 60s"
            )
        elif not over_rpm:
            self._over_rpm = False

        if over_tpm_in and not self._over_tpm_in:
            self._over_tpm_in = True
            _emit_exceeded(
                "Tokens per Minute (input)",
                f"input={tpm_in} > {self.tpm_limit} in last 60s",
            )
        elif not over_tpm_in:
            self._over_tpm_in = False

        if over_rpd and not self._over_rpd:
            self._over_rpd = True
            _emit_exceeded(
                "Requests per Day",
                f"{self.requests_per_day_count}>{self.requests_per_day_limit} (MMT day {today_mmt})",
            )

        # 毎回のレート窓スナップショット（人間可読、JSON出力なし）
        if self._rate_window_log_enabled:
            print(
                "ℹ️ WINDOW [rate] "
                f"Requests per Minute={rpm} | "
                f"Tokens per Minute (input)={tpm_in} | "
                f"Tokens per Minute (output)={tpm_out} | "
                f"Requests per Day={self.requests_per_day_count} "
                f"(MMT day {today_mmt}) | tag={tag} model={model}"
            )


# 有効/無効トグル（既定=有効）
_FREE_TIER_CHECK_ENABLED = str(os.getenv("GEMINI_FREE_TIER_CHECK", "1")).lower() not in (
    "0",
    "false",
    "off",
)
_FREE_TIER_MON = _FreeTierWatch() if _FREE_TIER_CHECK_ENABLED else None


def call_gemini_with_retries(
    client,
    prompt: str,
    model: str = "gemini-2.5-flash",
    max_retries: int = GEMINI_MAX_RETRIES,
    base_delay: float = GEMINI_BASE_DELAY,
    max_delay: float = GEMINI_MAX_DELAY,
    usage_tag: str = "generic",
):
    """
    Gemini 呼び出しの共通リトライラッパー。
    - 503/UNAVAILABLE/一時的ネットワークエラーは指数バックオフ+ジッターで再試行
    - 429/レート系は待機して再試行（Gemini Freeの瞬間上限に当たることが多い）
    - それ以外の恒久的エラーは即時raise
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            # 実際の呼び出し（既存コードの呼び方に合わせて調整）
            resp = client.models.generate_content(model=model, contents=prompt)
            # 使用量ログ
            try:
                _log_gemini_usage(resp, tag=(usage_tag or "gen"), model=model)
            except Exception:
                pass
            # Free tier 監視（MMT日次 / RPM / 入力TPM）
            try:
                if _FREE_TIER_MON:
                    u = _usage_from_resp(resp) or {}
                    _FREE_TIER_MON.record(
                        int(u.get("prompt_token_count") or 0),
                        output_tokens=int(u.get("candidates_token_count") or 0),
                        tag=(usage_tag or "gen"),
                        model=model,
                    )
            except Exception:
                pass
            return resp
        except Exception as e:
            msg = str(e)
            last_exc = e

            # 例外メッセージの簡易判定（SDK差異を吸収するため文字列ベース）
            is_503 = "503" in msg or "UNAVAILABLE" in msg or "overloaded" in msg
            is_429 = "429" in msg or "RESOURCE_EXHAUSTED" in msg or "rate" in msg.lower()
            
            # ✅ 追加: 網羅的な判定関数を併用（server disconnected / RemoteProtocolError 等も拾う）
            should_retry = _is_retriable_exc(e) or is_429

            # 再試行対象
            if should_retry:
                print(f"⚠️ Gemini retry {attempt+1}/{max_retries} after: {e}")
                sleep_sec = _exp_backoff_sleep(attempt, base_delay, max_delay)
                if is_429:
                    sleep_sec = min(GEMINI_MAX_DELAY, sleep_sec + 5.0)
                try:
                    import time
                    time.sleep(sleep_sec)
                except KeyboardInterrupt:
                    raise
                continue

            # 非リトライ系は即raise
            raise

    # すべて失敗
    raise last_exc if last_exc else RuntimeError("Gemini call failed with unknown error.")


# 要約用に送る本文の最大文字数（固定）
# Irrawaddy英語記事が3500文字くらいある
BODY_MAX_CHARS = 3500

# ミャンマー標準時 (UTC+6:30)
MMT = timezone(timedelta(hours=6, minutes=30))

# --- Unified body cache to reuse first-pass full bodies across stages ---
BODY_CACHE = {}
def _cache_body(url: str, body: str):
    try:
        if url and body:
            BODY_CACHE[url] = body
    except Exception:
        pass

def _get_cached_body(url: str) -> str:
    try:
        return BODY_CACHE.get(url) or ""
    except Exception:
        return ""
# -----------------------------------------------------------------------


# 今日の日付
# ニュースの速報性重視で今日分のニュース配信の方針
def get_today_date_mmt():
    s = (os.getenv("DATE_MMT") or "").strip()
    if s:
        try:
            # 手動実行の入力（YYYY-MM-DD）が来ていればそれを採用
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            print(f"⚠️ DATE_MMT の形式が不正です: {s}（YYYY-MM-DD で指定してください）→ 自動日付にフォールバック")

    # 本番用、今日の日付
    now_mmt = datetime.now(MMT)
    return now_mmt.date()


# 共通キーワードリスト（全メディア共通で使用する）
NEWS_KEYWORDS = [
    # ミャンマー（国名・現行名称）
    "မြန်မာ",
    "မြန်မာ့",
    "Myanmar",
    "myanmar",
    # ビルマ（旧国名・通称）
    "ဗမာ",
    "Burma",
    "burma",
    # アウンサンスーチー（Aung San Suu Kyi）
    "အောင်ဆန်းစုကြည်",
    "Aung San Suu Kyi",
    "aung san suu kyi",
    # ミンアウンフライン（Min Aung Hlaing）
    "မင်းအောင်လှိုင်",
    "Min Aung Hlaing",
    "min aung hlaing",
    # チャット（Kyat）
    "Kyat",
    "kyat",
    # 徴兵制（Conscription / Military Draft）, 徴兵, 兵役
    "စစ်တပ်ဝင်ခေါ်ရေး",
    "စစ်မှုထမ်း",
    "အတင်းတပ်ဝင်ခေါ်ခြင်း",
    "တပ်ဝင်ခေါ် ",
    "Conscription",
    "conscription",
    "Military Draft",
    "Military draft",
    "military draft",
    "Military Service",
    "Military service",
    "military service",
    # ロヒンギャ お願いされてない
    # "ရိုဟင်ဂျာ",
    # "Rohingya",
    # "rohingya",
    # 国境貿易・交易
    "နယ်စပ်ကုန်သွယ်ရေး",
    # ヤンゴン管区
    # "ရန်ကုန်တိုင်း",
    # ヤンゴン
    "ရန်ကုန်",
    "Yangon Region",
    "Yangon region",
    "yangon region",
]

# Unicode正規化（NFC）を適用
NEWS_KEYWORDS = [unicodedata.normalize("NFC", kw) for kw in NEWS_KEYWORDS]

# --- Ayeyarwady (エーヤワディ) 系だけを抜き出すサブセット ---
AYEYARWADY_KEYWORDS = [
    "ဧရာဝတီတိုင်း",
    "Ayeyarwady Region",
    "Ayeyarwady region",
    "ayeyarwady region",
]
AYEYARWADY_KEYWORDS = [unicodedata.normalize("NFC", kw) for kw in AYEYARWADY_KEYWORDS]

def is_ayeyarwady_hit(title: str, body: str) -> bool:
    """タイトル/本文にエーヤワディ系キーワードが含まれるか"""
    return any(kw in title or kw in body for kw in AYEYARWADY_KEYWORDS)

# 「チャット」語のバリエーションを通貨語として拾う
CURRENCY_WORD = r"(?:မြန်မာ(?:့)?(?:နိုင်ငံ)?\s*)?(?:ငွေ\s*)?ကျပ်(?:ငွေ)?"

_DIGITS = r"[0-9၀-၉][0-9၀-၉,\.]*"
_SCALE = r"(?:သောင်း|သိန်း|သန်း)"
_TRAIL = r"(?:\s*(?:ကျော်|လောက်|ခန့်))?"

# 1) 数字→通貨（num/scale はここで1回だけ定義）
_KYAT_NUM_FIRST = re.compile(
    rf"""
    (?P<num>{_DIGITS})\s*(?P<scale>{_SCALE})?\s*(?:{CURRENCY_WORD})
    {_TRAIL}
    """,
    re.VERBOSE,
)

# 2) 通貨→数字（同じグループ名をここでも1回だけ定義）
_KYAT_CCY_FIRST = re.compile(
    rf"""
    (?:{CURRENCY_WORD})\s*(?P<scale>{_SCALE})?\s*(?P<num>{_DIGITS})
    {_TRAIL}
    """,
    re.VERBOSE,
)


class _OrPattern:
    """複数の compiled regex をまとめ、.search で最初に当たった Match を返す薄いラッパ"""

    def __init__(self, *compiled):
        self._compiled = compiled
        self.pattern = " | ".join(p.pattern for p in compiled)  # 参考用
        self.flags = compiled[0].flags if compiled else 0

    def search(self, string, pos=0):
        for p in self._compiled:
            m = p.search(string, pos)
            if m:
                return m
        return None


KYAT_PATTERN = _OrPattern(_KYAT_NUM_FIRST, _KYAT_CCY_FIRST)


def any_keyword_hit(title: str, body: str) -> bool:
    # 通常のキーワード一致
    if any(kw in title or kw in body for kw in NEWS_KEYWORDS):
        return True
    # 通貨「ကျပ်」だけは正規表現で判定
    if KYAT_PATTERN.search(title) or KYAT_PATTERN.search(body):
        return True
    return False



def clean_html_content(html: str) -> str:
    html = html.replace("\xa0", " ").replace("&nbsp;", " ")
    # 制御文字（カテゴリC）を除外、可視Unicodeはそのまま
    return "".join(c for c in html if unicodedata.category(c)[0] != "C")


# 本文が取得できるまで「requestsでリトライする」
def fetch_with_retry(url, retries=3, wait_seconds=2):
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200 and res.text.strip():
                return res
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
        time.sleep(wait_seconds)
    raise Exception(f"Failed to fetch {url} after {retries} attempts.")


# 本文が空なら「一定秒数待って再取得」
def extract_paragraphs_with_wait(soup_article, retries=2, wait_seconds=2):
    for attempt in range(retries + 1):
        paragraphs = soup_article.select("div.entry-content p")
        if not paragraphs:
            paragraphs = soup_article.select("div.node-content p")
        if not paragraphs:
            paragraphs = soup_article.select("article p")
        if not paragraphs:
            paragraphs = soup_article.find_all("p")
        if not paragraphs:
            paragraphs = soup_article.find_all("div.td-post-content p")

        if paragraphs:
            return paragraphs

        print(f"Paragraphs not found, waiting {wait_seconds}s and retrying...")
        time.sleep(wait_seconds)
    return []


# === 汎用の <p> 抽出器（サイト共通） ===
def extract_body_generic_from_soup(soup):
    for sel in ["div.entry-content p", "div.node-content p", "article p", "div.td-post-content p"]:
        ps = soup.select(sel)
        if ps:
            break
    else:
        ps = soup.find_all("p")
    txts = [p.get_text(strip=True) for p in ps if p.get_text(strip=True)]
    return "\n".join(txts).strip()


# === メール要約と同じ感覚で本文スコープを決める：PDF専用で使用 ===
def extract_body_mail_pdf_scoped(url: str, soup) -> str:
    import re as _re
    import unicodedata as _uni
    U = (url or "").lower()

    # ドメイン固有の本文コンテナ候補（優先度順）
    DOMAIN_SCOPES = []
    if ("yktnews.com" in U) or ("khitthit" in U) or ("mizzima" in U):
        DOMAIN_SCOPES = ["div.td-post-content", "div.entry-content", "article .td-post-content"]
    if ("dvb" in U):
        DOMAIN_SCOPES = DOMAIN_SCOPES or ["div.node-content", "div.entry-content", "article"]
    if ("myanmar-now" in U) or ("myanmarnow" in U):
        DOMAIN_SCOPES = DOMAIN_SCOPES or ["div.article-content", "article .content-body", "article"]

    # 汎用候補
    GENERIC_SCOPES = [
        "article", "main", "div[itemprop=articleBody]",
        "div.entry-content", "div.node-content", "div.td-post-content",
    ]

    # 非本文ブロックは事前に除去
    EXCLUDE_SELS = [
        "#comments", ".comments", "section.comments",
        "form", "form.comment-form",
        "aside", "nav", "header", "footer",
        ".tags", ".tagcloud", ".post-tags", ".td-post-source-tags",
        ".related", ".td_block_related_posts", ".jeg_related_post", ".jnews_related_post_container",
        ".share", ".social", ".post-share", ".post-meta",
    ]

    # ルート決定
    root = None
    for sel in DOMAIN_SCOPES:
        n = soup.select_one(sel)
        if n: root = n; break
    if root is None:
        for sel in GENERIC_SCOPES:
            n = soup.select_one(sel)
            if n: root = n; break
    if root is None:
        root = soup

    for sel in EXCLUDE_SELS:
        for n in root.select(sel):
            n.decompose()

    # #タグ雲アンカー（a要素で #始まり）を削除
    for a in root.find_all("a"):
        t = (a.get_text(strip=True) or "")
        if t.startswith("#"):
            a.decompose()

    # <p> から本文を構築（タグ雲行／WPコメント定型は除外）
    ps = root.select("p") or root.find_all("p")
    lines = []
    for p in ps:
        t = p.get_text(strip=True)
        if not t: continue
        if "Save my name, email, and website" in t: continue
        if _re.match(r'^(?:[#＃][^\s#]+(?:\s+|$)){3,}$', t): continue
        t = _re.sub(r"\s+", " ", t).strip()
        if t:
            lines.append(_uni.normalize("NFC", t))
    return "\n".join(lines).strip()


def _ensure_meta_dates(url_to_meta: dict, date_mmt_iso: str):
    """
    url_to_meta: { url: {"source":..., "date": str|None, ...}, ... }
    date_mmt_iso: "YYYY-MM-DD"（ダイジェスト対象日/MMT）
    """
    from bs4 import BeautifulSoup
    from datetime import datetime, timezone, timedelta
    MMT = timezone(timedelta(hours=6, minutes=30))

    missing = [u for u, v in url_to_meta.items() if not v.get("date")]
    if not missing:
        return

    fixed = 0
    for u in missing:
        try:
            resp = fetch_once_requests(u, timeout=10)
            soup = BeautifulSoup(resp, "html.parser")
            meta = soup.find("meta", attrs={"property": "article:published_time"})
            iso = meta.get("content").strip() if meta and meta.get("content") else None
            if iso:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                url_to_meta[u]["date"] = dt.astimezone(MMT).date().isoformat()
                fixed += 1
                continue
        except Exception:
            pass
        # 取れなければダイジェスト日で埋める
        url_to_meta[u]["date"] = date_mmt_iso

    print(f"[info] meta date backfilled: {fixed} fetched, {len(missing)-fixed} filled with digest date")

# === requests を使うシンプルな fetch_once（1回） ===
def fetch_once_requests(url, timeout=15):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    # 文字化け回避のため bytes を返す（デコードは BeautifulSoup に任せる）
    return r.content


# === 再フェッチ付き・本文取得ユーティリティ ===
def get_body_with_refetch(
    url, fetcher, extractor, retries=3, wait_seconds=2, quiet=False
):
    """
    fetcher(url) -> html(bytes or str)
    extractor(soup) -> body(str)
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            html = fetcher(url)
            # bytes/str どちらでも BeautifulSoup に渡せる
            soup = BeautifulSoup(html, "html.parser")

            # 誤って latin-1 系で解釈された場合は UTF-8 で再解釈して保険をかける
            enc = (getattr(soup, "original_encoding", None) or "").lower()
            if enc in ("iso-8859-1", "latin-1", "windows-1252"):
                soup = BeautifulSoup(html, "html.parser", from_encoding="utf-8")

            body = extractor(soup)
            if body:
                return unicodedata.normalize("NFC", body)

            if not quiet:
                print(f"[refetch] body empty, retrying {attempt+1}/{retries} → {url}")
        except Exception as e:
            last_err = e
            if not quiet:
                print(f"[refetch] EXC {attempt+1}/{retries}: {e} → {url}")
        time.sleep(wait_seconds)

    if not quiet and last_err:
        print(f"[refetch] give up after {retries+1} tries → {url}")
    return ""


# === Irrawaddy専用 ===
# 本文が取得できるまで「requestsでリトライする」
def fetch_with_retry_irrawaddy(url, retries=3, wait_seconds=2, session=None):
    """
    Irrawaddy 専用フェッチャ（単発トライ版）。
    - curl_cffi で1回 → 失敗なら cloudscraper で1回 → 失敗なら requests で1回。
    - 追加のリトライや /amp への再試行は実施しない。
    """
    import os
    import random
    import time
    import urllib.parse

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )
    HEADERS = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.irrawaddy.com/",
        "Connection": "keep-alive",
    }

    def _amp_url(u: str) -> str:
        # https://.../path/ なら https://.../path/amp
        # https://.../path  なら https://.../path/amp
        if not u.endswith("/"):
            u = u + "/"
        return urllib.parse.urljoin(u, "amp")

    # --- Try 1: curl_cffi (Chrome 指紋) 単発 ---
    try:
        from curl_cffi import requests as cfr  # type: ignore[import-not-found]
        proxies = {
            # Irrawaddy専用のプロキシ指定があれば最優先で使う（他サイトには影響しない）
            "http":  os.getenv("IRRAWADDY_HTTP_PROXY")  or os.getenv("HTTP_PROXY")  or os.getenv("http_proxy"),
            "https": os.getenv("IRRAWADDY_HTTPS_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy"),
        }
        r = cfr.get(
            url,
            headers=HEADERS,
            impersonate="chrome124",
            timeout=30,
            allow_redirects=True,
            proxies={k: v for k, v in proxies.items() if v},
        )
        if r.status_code == 200 and (r.text or "").strip():
            return r
    except Exception as e:
        print(f"[fetch-cffi] EXC: {e} → {url}")

    # --- Try 2: cloudscraper 単発 ---
    try:
        import cloudscraper
        import requests as rq
        sess = session or rq.Session()
        scraper = cloudscraper.create_scraper(
            sess=sess,
            browser={"browser": "chrome", "platform": "windows", "mobile": False},
            delay=7,
        )
        r = scraper.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code == 200 and getattr(r, "text", "").strip():
            return r
    except Exception as e:
        print(f"[fetch-cs] EXC: {e} → {url}")

    # --- Try 3: requests 単発（/news/ のときのみ /amp を1回だけ試す） ---
    try:
        import requests
        sess = session or requests.Session()
        r2 = sess.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        print(
            f"[fetch-rq] final: HTTP {r2.status_code} len={len(getattr(r2,'text',''))} → {url}"
        )
        if r2.status_code == 200 and getattr(r2, "text", "").strip():
            return r2
        # 403/503 かつ /news/ の記事URLに限り、/amp を“1回だけ”試す
        if r2.status_code in (403, 503) and "/news/" in url and "/category/" not in url:
            amp = _amp_url(url)
            r3 = sess.get(amp, headers=HEADERS, timeout=20, allow_redirects=True)
            print(
                f"[fetch-rq] amp: HTTP {r3.status_code} len={len(getattr(r3,'text',''))} → {amp}"
            )
            if r3.status_code == 200 and getattr(r3, "text", "").strip():
                return r3
        try:
            svr = r2.headers.get("server") or r2.headers.get("Server")
            ray = r2.headers.get("cf-ray")
            sucuri = r2.headers.get("x-sucuri-id") or r2.headers.get("x-sucuri-block")
            print(f"[fetch-rq] headers: server={svr} cf-ray={ray} sucuri={sucuri}")
        except Exception:
            pass
    except Exception as e:
        print(f"[fetch-rq] EXC final: {e} → {url}")

    raise Exception(f"Failed to fetch {url} after {retries} attempts.")


# === DVB専用 ===
def fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=None):
    """
    DVB (https://burmese.dvb.no) 向けの多段フェッチャ。
    1) curl_cffi(Chrome指紋) → 2) cloudscraper → 3) requests の順。
    403/429/503 は指数バックオフ。/post/* では /amp / ?output=amp も試す。
    """
    import os
    import time
    import random

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )
    BASE = "https://burmese.dvb.no"
    HEADERS = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,my;q=0.8,ja;q=0.7",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"{BASE}/",
        "Connection": "keep-alive",
    }

    def _amp_candidates(u: str):
        u = u.strip()
        q = "&" if "?" in u else "?"
        return [u.rstrip("/") + "/amp", u + f"{q}output=amp"]

    # --- Try 1: curl_cffi ---
    try:
        from curl_cffi import requests as cfr  # type: ignore

        proxies = {
            "http": os.getenv("HTTP_PROXY") or os.getenv("http_proxy"),
            "https": os.getenv("HTTPS_PROXY") or os.getenv("https_proxy"),
        }
        for attempt in range(retries):
            r = cfr.get(
                url,
                headers=HEADERS,
                impersonate="chrome124",
                timeout=30,
                allow_redirects=True,
                proxies={k: v for k, v in proxies.items() if v},
            )
            if r.status_code == 200 and (r.text or "").strip():
                return r
            # 記事URLはAMP系も試す
            if r.status_code in (403, 503) and "/post/" in url:
                for amp in _amp_candidates(url):
                    r2 = cfr.get(
                        amp,
                        headers=HEADERS,
                        impersonate="chrome124",
                        timeout=30,
                        allow_redirects=True,
                        proxies={k: v for k, v in proxies.items() if v},
                    )
                    if r2.status_code == 200 and (r2.text or "").strip():
                        return r2
            if r.status_code in (403, 429, 503):
                time.sleep(wait_seconds * (2**attempt) + random.uniform(0, 0.8))
                continue
            break
    except Exception as e:
        print(f"[dvb-cffi] EXC: {e} → {url}")

    # --- Try 2: cloudscraper ---
    try:
        import cloudscraper
        import requests as rq

        sess = session or rq.Session()
        scraper = cloudscraper.create_scraper(
            sess=sess,
            browser={"browser": "chrome", "platform": "windows", "mobile": False},
            delay=7,
        )
        for attempt in range(retries):
            try:
                r = scraper.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
                if r.status_code == 200 and getattr(r, "text", "").strip():
                    return r
                if r.status_code in (403, 503) and "/post/" in url:
                    for amp in _amp_candidates(url):
                        r2 = scraper.get(
                            amp, headers=HEADERS, timeout=30, allow_redirects=True
                        )
                        if r2.status_code == 200 and getattr(r2, "text", "").strip():
                            return r2
                if r.status_code in (403, 429, 503):
                    time.sleep(wait_seconds * (2**attempt) + random.uniform(0, 0.8))
                    continue
                break
            except Exception as e:
                print(f"[dvb-cs] {attempt+1}/{retries} EXC: {e} → {url}")
                time.sleep(wait_seconds * (2**attempt) + random.uniform(0, 0.8))
    except Exception as e:
        print(f"[dvb-cs] INIT EXC: {e} → {url}")

    # --- Try 3: requests ---
    try:
        import requests

        sess = session or requests.Session()
        r2 = sess.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r2.status_code == 200 and getattr(r2, "text", "").strip():
            return r2
        if r2.status_code in (403, 503) and "/post/" in url:
            for amp in _amp_candidates(url):
                r3 = sess.get(amp, headers=HEADERS, timeout=30, allow_redirects=True)
                if r3.status_code == 200 and getattr(r3, "text", "").strip():
                    return r3
    except Exception as e:
        print(f"[dvb-rq] EXC final: {e} → {url}")

    raise Exception(f"Failed to fetch DVB {url} after {retries} attempts.")


def _norm_text(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def _norm_id(u):
    """ID/URL照合用の軽量正規化：末尾スラッシュを落とす"""
    if isinstance(u, str):
        return u.rstrip("/")
    return u


def _parse_category_date_text(text: str):
    # 例: 'August 9, 2025'
    text = re.sub(r"\s+", " ", text.strip())
    return datetime.strptime(text, "%B %d, %Y").date()


def _article_date_from_meta_mmt(soup):
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if not meta or not meta.get("content"):
        return None
    iso = meta["content"].replace("Z", "+00:00")  # 末尾Z対策
    dt = datetime.fromisoformat(iso)
    return dt.astimezone(MMT).date()


def _extract_title(soup):
    # 1) 明示の見出し
    h = soup.select_one("h1.jeg_post_title") or soup.select_one("h1.entry-title") or soup.find("h1")
    if h and h.get_text(strip=True):
        return _norm_text(h.get_text(strip=True))
    # 2) og:title
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return _norm_text(og.get("content", "").strip())
    # 3) <title>
    t = soup.find("title")
    if t and t.get_text(strip=True):
        return _norm_text(t.get_text(strip=True))
    return None


def _title_from_slug(u: str) -> str:
    try:
        from urllib.parse import urlparse, unquote
        path = urlparse(u).path or ""
        seg = path.rstrip("/").split("/")[-1]
        seg = seg.replace(".html", "")
        seg = unquote(seg)
        seg = seg.replace("-", " ")
        # 先頭大文字化（英文タイトル用の簡易整形）
        return _norm_text(seg.title())
    except Exception:
        return ""


def _oembed_title_irrawaddy(u: str) -> str:
    try:
        api = (
            "https://www.irrawaddy.com/wp-json/oembed/1.0/embed?url="
            + requests.utils.requote_uri(u)
        )
        r = requests.get(
            api,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/128.0.0.0 Safari/537.36"
                )
            },
            timeout=10,
        )
        if r.status_code == 200 and r.text.strip():
            try:
                data = r.json()
            except Exception:
                # JSONとして読めない場合は諦める
                return ""
            t = (data.get("title") or "").strip()
            return _norm_text(t)
    except Exception:
        pass
    return ""


def _is_excluded_by_ancestor(node) -> bool:
    excluded = {
        "jnews_inline_related_post",
        "jeg_postblock_21",
        "widget",
        "widget_jnews_popular",
        "jeg_postblock_5",
        "jnews_related_post_container",
        "widget widget_jnews_popular",
        "jeg_footer_primary clearfix",
    }
    for anc in node.parents:
        classes = anc.get("class", [])
        if any(c in excluded for c in classes):
            return True
    return False


# 本文抽出
def extract_body_irrawaddy(soup):
    # <div class="content-inner "> 配下の <p>のみ（除外ブロック配下は除外）
    paragraphs = []
    content_inners = soup.select("div.content-inner")
    if not content_inners:
        content_inners = [
            div
            for div in soup.find_all("div")
            if "content-inner" in (div.get("class") or [])
        ]
    for root in content_inners:
        for p in root.find_all("p"):
            if _is_excluded_by_ancestor(p):
                continue
            txt = p.get_text(strip=True)
            if txt:
                paragraphs.append(_norm_text(txt))
    return "\n".join(paragraphs).strip()


#  Irrawaddy 用 fetch_once（既存の fetch_with_retry_irrawaddy を1回ラップ）
def fetch_once_irrawaddy(url, session=None):
    r = fetch_with_retry_irrawaddy(url, retries=1, wait_seconds=0, session=session)
    # cloudscraper のレスポンスも bytes を返す（デコードは BeautifulSoup に任せる）
    return r.content


# === ここまで ===


# ===== キーワード未ヒット時の共通ロガー（簡素版） =====
LOG_NO_KEYWORD_MISSES = True


def log_no_keyword_hit(source: str, url: str, title: str, body: str, stage: str):
    """
    キーワード未ヒットの記事を標準出力に出す（stage・本文抜粋は出力しない）。
    """
    if not LOG_NO_KEYWORD_MISSES:
        return
    try:
        title = unicodedata.normalize("NFC", title or "")
    except Exception:
        pass

    print("\n----- NO KEYWORD HIT -----")
    print(f"[source] {source}")
    print(f"[url]    {url}")
    print(f"[title]  {title}")
    print("----- END NO KEYWORD HIT -----\n")


# Mizzimaカテゴリーページ巡回で取得
def get_mizzima_articles_from_category(
    date_obj, base_url, source_name, category_path, max_pages=3
):
    # ==== ローカル定数 Mizzima除外対象キーワード（タイトル用）====
    EXCLUDE_TITLE_KEYWORDS = [
        # 春の革命日誌
        "နွေဦးတော်လှန်ရေး နေ့စဉ်မှတ်စု",
        # 写真ニュース
        "ဓာတ်ပုံသတင်း",
    ]

    article_urls = []

    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            url = f"{base_url}{category_path}"
        else:
            url = f"{base_url}{category_path}/page/{page_num}/"

        try:
            res = requests.get(url, timeout=10)
            if res.status_code != 200:
                continue

            soup = BeautifulSoup(res.content, "html.parser")
            links = [
                a["href"]
                for a in soup.select("main.site-main article a.post-thumbnail[href]")
            ]
            article_urls.extend(links)

        except Exception as e:
            print(f"Error crawling category page {url}: {e}")
            continue

    filtered_articles = []
    for url in article_urls:
        try:
            res_article = fetch_with_retry(url)
            soup_article = BeautifulSoup(res_article.content, "html.parser")

            meta_tag = soup_article.find("meta", property="article:published_time")
            if not meta_tag or not meta_tag.has_attr("content"):
                continue

            date_str = meta_tag["content"]
            article_datetime_utc = datetime.fromisoformat(date_str)
            article_datetime_mmt = article_datetime_utc.astimezone(MMT)
            article_date = article_datetime_mmt.date()

            if article_date != date_obj:
                continue

            title_tag = soup_article.find("meta", attrs={"property": "og:title"})
            if not title_tag or not title_tag.has_attr("content"):
                continue
            title = title_tag["content"].strip()

            # === 除外キーワード判定（タイトルをNFC正規化してから） ===
            title_nfc = unicodedata.normalize("NFC", title)
            if any(kw in title_nfc for kw in EXCLUDE_TITLE_KEYWORDS):
                print(f"SKIP: excluded keyword in title → {url} | TITLE: {title_nfc}")
                continue

            content_div = soup_article.find("div", class_="entry-content")
            if not content_div:
                continue

            paragraphs = []
            for p in content_div.find_all("p"):
                if p.find_previous("h2", string=re.compile("Related Posts", re.I)):
                    break
                paragraphs.append(p)

            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            body_text = unicodedata.normalize("NFC", body_text)

            if not body_text.strip():
                continue

            # キーワード判定は正規化済みタイトルで行う
            if not any_keyword_hit(title, body_text):
                log_no_keyword_hit(
                    source_name, url, title, body_text, "mizzima:category"
                )
                continue

            filtered_articles.append(
                {
                    "source": source_name,
                    "url": url,
                    "title": title,
                    "date": article_date.isoformat(),
                    "body": body_text,
                }
            )

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return filtered_articles


# BCCはRSSあるのでそれ使う
def get_bbc_burmese_articles_for(target_date_mmt):
    
    # ==== ローカル定数 ====
    NOISE_PATTERNS = [
        r"BBC\s*News\s*မြန်မာ",  # 固定署名（Burmese表記）
        r"BBC\s*Burmese",  # 英語表記
    ]

    # BBC Burmese の「タイトル除外」用キーワード（将来追加しやすいよう配列で保持）
    EXCLUDE_TITLE_KEYWORDS = [
        "နိုင်ငံတဝန်းသတင်းများ အနှစ်ချုပ်",
    ]
    # 合成差異を避けるため NFC に正規化しておく
    EXCLUDE_TITLE_KEYWORDS = [
        unicodedata.normalize("NFC", kw) for kw in EXCLUDE_TITLE_KEYWORDS
    ]

    def _remove_noise_phrases(text: str) -> str:
        """BBC署名などのノイズフレーズを除去"""
        if not text:
            return text
        for pat in NOISE_PATTERNS:
            text = re.sub(pat, "", text, flags=re.IGNORECASE)
        return text.strip()

    # MEMO: ログ用
    # あるテキスト中でキーワードがどこにヒットしたかを返す（周辺文脈つき）
    # def _find_hits(text: str, keywords):
    #     hits = []
    #     for kw in keywords:
    #         start = 0
    #         while True:
    #             i = text.find(kw, start)
    #             if i == -1:
    #                 break
    #             s = max(0, i - 30)
    #             e = min(len(text), i + len(kw) + 30)
    #             ctx = text[s:e].replace("\n", " ")
    #             hits.append({"kw": kw, "pos": i, "ctx": ctx})
    #             start = i + len(kw)
    #     return hits

    rss_url = "https://feeds.bbci.co.uk/burmese/rss.xml"
    session = requests.Session()

    try:
        res = session.get(rss_url, timeout=10)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ RSS取得エラー: {e}")
        return []

    soup = BeautifulSoup(res.content, "xml")
    articles = []

    for item in soup.find_all("item"):
        pub_date_tag = item.find("pubDate")
        if not pub_date_tag:
            continue

        # RSSはUTC → MMTへ変換し、対象日だけ通す
        try:
            pub_date = parse_date(pub_date_tag.text)
            pub_date_mmt = pub_date.astimezone(MMT).date()
        except Exception as e:
            print(f"❌ pubDate parse error: {e}")
            continue

        if pub_date_mmt != target_date_mmt:
            continue

        title = (
            (item.find("title") or {}).get_text(strip=True)
            if item.find("title")
            else ""
        )
        link = (
            (item.find("link") or {}).get_text(strip=True) if item.find("link") else ""
        )
        if not link:
            continue
        
        # === タイトル除外（RSS から取得したタイトルで先に判定して早期スキップ） ===
        rss_title_nfc = unicodedata.normalize("NFC", title or "")
        if any(kw in rss_title_nfc for kw in EXCLUDE_TITLE_KEYWORDS):
            print(f"SKIP: excluded title keyword (BBC) → {link} | TITLE: {rss_title_nfc}")
            continue

        try:
            article_res = session.get(link, timeout=10)
            article_res.raise_for_status()
            article_soup = BeautifulSoup(article_res.content, "html.parser")

            # ===== ここで除外セクションをまとめて削除 =====
            # 記事署名やメタ情報
            for node in article_soup.select(
                'section[role="region"][aria-labelledby="article-byline"]'
            ):
                node.decompose()
            # 「おすすめ／最も読まれた」ブロック
            for node in article_soup.select(
                'section[data-e2e="recommendations-heading"][role="region"]'
            ):
                node.decompose()
            # ついでにヘッダー/ナビ/フッター等のノイズも落としておく（任意）
            for node in article_soup.select(
                'header[role="banner"], nav[role="navigation"], footer[role="contentinfo"], aside'
            ):
                node.decompose()
            # ============================================

            # 本文は main 内の <p> に限定
            main = article_soup.select_one('main[role="main"]') or article_soup
            paragraphs = [p.get_text(strip=True) for p in main.find_all("p")]
            # 空行やノイズを削る
            paragraphs = [t for t in paragraphs if t]
            body_text = "\n".join(paragraphs)

            # ミャンマー文字の合成差異を避けるため NFC 正規化
            title_nfc = unicodedata.normalize("NFC", title)
            title_nfc = _remove_noise_phrases(title_nfc)
            body_text_nfc = unicodedata.normalize("NFC", body_text)
            body_text_nfc = _remove_noise_phrases(body_text_nfc)

            # キーワード判定
            if not any_keyword_hit(title_nfc, body_text_nfc):
                log_no_keyword_hit(
                    "BBC Burmese", link, title_nfc, body_text_nfc, "bbc:article"
                )
                continue

            # MEMO: ログ用、=== デバッグ: 判定前にタイトル/本文の要約を出す ===
            # print("----- DEBUG CANDIDATE -----")
            # print("URL:", link)
            # print("TITLE:", repr(title_nfc))
            # print("BODY_HEAD:", repr(body_text_nfc[:500]))
            # print("BODY_LEN:", len(body_text_nfc))

            # # キーワード判定（ヒット詳細も取る）
            # title_hits = _find_hits(title_nfc, NEWS_KEYWORDS)
            # body_hits  = _find_hits(body_text_nfc, NEWS_KEYWORDS)
            # total_hits = title_hits + body_hits

            # if not total_hits:
            #     print("SKIP: no keyword hits.")
            #     continue

            # # === デバッグ: どのキーワードがどこで当たったか ===
            # print("HITS:", len(total_hits))
            # if title_hits:
            #     print(" - in TITLE:")
            #     for h in title_hits[:10]:
            #         print(f"   kw={repr(h['kw'])} ctx=…{h['ctx']}…")
            # if body_hits:
            #     print(" - in BODY:")
            #     for h in body_hits[:10]:  # 長くなるので最大10件
            #         print(f"   kw={repr(h['kw'])} ctx=…{h['ctx']}…")

            print(f"✅ 抽出記事: {title_nfc} ({link})")
            _cache_body(link, body_text_nfc)
            articles.append(
                {
                    "title": title_nfc,
                    "url": link,
                    "date": pub_date_mmt.isoformat(),
                    "source": "BBC Burmese",
                    "body": body_text_nfc,
                }
            )

        except Exception as e:
            print(f"❌ 記事取得/解析エラー: {e}")
            continue

    return articles


# khit_thit_mediaカテゴリーページ巡回で取得
def get_khit_thit_media_articles_from_category(date_obj, max_pages=3):
    # 追加カテゴリを含む巡回対象
    CATEGORY_URLS = [
        "https://yktnews.com/category/news/",
        "https://yktnews.com/category/politics/",
        "https://yktnews.com/category/editor-choice/",
        "https://yktnews.com/category/interview/",
        "https://yktnews.com/category/china-watch/",
    ]

    HASHTAG_TOKEN_RE = re.compile(
        r"(?:(?<=\s)|^)\#[^\s#]+"
    )  # 空白or行頭から始まる #トークンを除去（多言語対応）

    def _remove_hashtag_links(soup):
        """
        <a>や<strong><a>…</a></strong>のような入れ子を含め、
        テキストが '#' で始まるアンカーを本文から除去する。
        """
        # a要素の中で可視テキストが '#' で始まるものをまるごと削除
        for a in soup.select("a"):
            txt = a.get_text(strip=True)
            if txt.startswith("#"):
                a.decompose()

    collected_urls = set()
    for base_url in CATEGORY_URLS:
        for page in range(1, max_pages + 1):
            url = f"{base_url}page/{page}/" if page > 1 else base_url
            print(f"Fetching {url}")
            try:
                res = fetch_with_retry(url)
            except Exception as e:
                print(f"[khitthit] stop pagination (missing/unreachable): {url} -> {e}")
                break

            soup = BeautifulSoup(res.content, "html.parser")
            entry_links = soup.select("p.entry-title.td-module-title a[href]")
            if not entry_links:
                print(f"[khitthit] stop pagination (no entries): {url}")
                break

            for a in entry_links:
                href = a.get("href")
                if not href:
                    continue
                if href in collected_urls:  # ← 既出URLは明示スキップ
                    continue
                collected_urls.add(href)

    filtered_articles = []
    for url in collected_urls:
        try:
            res_article = fetch_with_retry(url)
            soup_article = BeautifulSoup(res_article.content, "html.parser")

            # 日付取得
            meta_tag = soup_article.find("meta", property="article:published_time")
            if not meta_tag or not meta_tag.has_attr("content"):
                continue
            date_str = meta_tag["content"]
            article_datetime_utc = datetime.fromisoformat(date_str)
            article_datetime_mmt = article_datetime_utc.astimezone(MMT)
            article_date = article_datetime_mmt.date()
            if article_date != date_obj:
                continue  # 対象日でなければスキップ

            # タイトル取得
            title_tag = soup_article.find("h1")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            # 本文取得  ← この直前に “ハッシュタグ除去” を差し込む
            _remove_hashtag_links(soup_article)  # ① HTML段階で #アンカーを除去
            paragraphs = extract_paragraphs_with_wait(soup_article)
            # ② テキスト化後も保険で #トークンを除去
            body_text = "\n".join(
                HASHTAG_TOKEN_RE.sub("", p.get_text(strip=True)).strip()
                for p in paragraphs
                if p.get_text(strip=True)  # 空パラはそもそも捨てる
            )
            body_text = unicodedata.normalize("NFC", body_text)
            if not body_text.strip():
                continue  # 本文が空ならスキップ

            if not any_keyword_hit(title, body_text):
                log_no_keyword_hit(
                    "Khit Thit Media", url, title, body_text, "khitthit:category"
                )
                continue  # キーワード無しは除外
            _cache_body(url, body_text)

            filtered_articles.append(
                {
                    "url": url,
                    "title": title,
                    "date": date_obj.isoformat(),
                    "source": "Khit Thit Media",  # deduplicate_by_urlのログで使われる
                    "body": body_text,
                }
            )
        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    before = len(filtered_articles)
    filtered_articles = deduplicate_by_url(filtered_articles)
    print(f"[khitthit] dedup: {before} -> {len(filtered_articles)}")  # 最小ログ

    return filtered_articles


# irrawaddy
def get_irrawaddy_articles_for(date_obj, debug=True):
    """
    指定の Irrawaddy カテゴリURL群（相対パス）を1回ずつ巡回し、
    MMTの指定日(既定: 今日)にヒットする記事のみ返す。
    さらにホーム https://www.irrawaddy.com/ の
    data-id="kuDRpuo" カラム内からも同様に候補収集する。

    - /category/news/asia, /category/news/world は除外（先頭一致・大小無視）
    - 一覧では「時計アイコン付きの日付リンク」から当日候補を抽出
    - 記事側では <meta property="article:published_time"> を MMT に変換して再確認
    - 本文は <div class="content-inner "> 配下の <p> から抽出（特定ブロック配下は除外）
    返り値: [{url, title, date}]
    依存: MMT, get_today_date_mmt, fetch_with_retry, any_keyword_hit
    """

    session = requests.Session()

    # ==== 巡回対象（相対パス、重複ありでもOK：内部でユニーク化） ====
    CATEGORY_PATHS_RAW = [
        "/category/news/",
        "/category/politics",
        "/category/news/war-against-the-junta",
        "/category/news/conflicts-in-numbers",
        "/category/news/junta-crony",
        "/category/news/ethnic-issues",
        "/category/business",
        "/category/business/economy",
        "/category/Features",
        "/category/Opinion",
        "/category/Opinion/editorial",
        "/category/Opinion/commentary",
        "/category/Opinion/guest-column",
        "/category/Opinion/analysis",
        "/category/in-person",
        "/category/in-person/interview",
        "/category/in-person/profile",
        "/category/Specials",
        "/category/specials/women",
        "/category/from-the-archive",
        "/category/Specials/myanmar-china-watch",
        # "/category/Video" # 除外依頼有
        # "/category/culture/books" #除外依頼有
        # "/category/Cartoons" # 除外依頼有
        # "/category/election-2020", # 2021年で更新止まってる
        # "/category/Opinion/letters", # 2014年で更新止まってる
        # "/category/Dateline", # 2020年で更新止まってる
        # "/category/specials/places-in-history", # 2020年で更新止まってる
        # "/category/specials/on-this-day", # 2023年で更新止まってる
        # "/category/Specials/myanmar-covid-19", # 2022年で更新止まってる
        # "/category/Lifestyle", # 2020年で更新止まってる
        # "/category/Travel", # 2020年で更新止まってる
        # "/category/Lifestyle/Food", # 2020年で更新止まってる
        # "/category/Lifestyle/fashion-design", # 2019年で更新止まってる
        # "/category/photo", # 2016年で更新止まってる
        # "/category/photo-essay", # 2021年で更新止まってる
    ]
    BASE = "https://www.irrawaddy.com"
    EXCLUDE_PREFIXES = [
        "/category/news/asia",  # 除外依頼有
        "/category/news/world",  # 除外依頼有
        "/video",  # "/category/Video"は除外対象だがこのパターンもある
        "/cartoons",  # "/category/Cartoons"は除外対象だがこのパターンもある
    ]  # 先頭一致・大小無視

    def _is_excluded_url(href: str) -> bool:
        try:
            p = urlparse(href or "").path.lower()
        except Exception:
            p = (href or "").lower()
        return any(p.startswith(x) for x in EXCLUDE_PREFIXES)

    # ==== 正規化・ユニーク化・除外 ====
    def _norm(p: str) -> str:
        return re.sub(r"/{2,}", "/", p.strip())

    paths, seen = [], set()
    for p in CATEGORY_PATHS_RAW:
        q = _norm(p)
        if any(q.lower().startswith(x) for x in EXCLUDE_PREFIXES):
            continue
        if q not in seen:
            seen.add(q)
            paths.append(q)

    # 2) 簡易ロガー（消す時はこの1行と dbg(...) を消すだけ）
    dbg = (lambda *a, **k: print(*a, **k)) if debug else (lambda *a, **k: None)

    results = []
    seen_urls = set()
    candidate_urls = []
    fallback_titles: Dict[str, str] = {}

    # ==== 1) 各カテゴリURLを1回ずつ巡回 → 当日候補抽出 ====
    for rel_path in paths:
        url = f"{BASE}{rel_path}"
        # print(f"Fetching {url}")
        try:
            res = fetch_with_retry_irrawaddy(url, session=session)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(res.content, "html.parser")
        wrapper = soup.select_one("div.jeg_content")  # テーマによっては無いこともある

        # ✅ union 方式：wrapper 内→見つからなければページ全体の順で探索
        scopes = ([wrapper] if wrapper else []) + [soup]

        for scope in scopes:
            # ヒーロー枠＋通常リスト＋汎用メタを一発で拾う
            links = scope.select(
                ".jnews_category_hero_container .jeg_meta_date a[href], "
                "div.jeg_postblock_content .jeg_meta_date a[href], "
                ".jeg_post_meta .jeg_meta_date a[href]"
            )
            # 時計アイコン付きだけに限定（ノイズ回避）
            links = [a for a in links if a.find("i", class_="fa fa-clock-o")]

            # （任意）デバッグ表示
            # dbg(f"[cat] union-links={len(links)} @ {url}")
            for a in links[:2]:
                _txt = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
                # dbg("   →", _txt, "|", a.get("href"))

            found = 0
            for a in links:
                href = a.get("href") or ""
                raw = a.get_text(" ", strip=True)
                try:
                    shown_date = _parse_category_date_text(raw)
                except Exception:
                    # 必要最小限のデバッグだけ
                    # dbg("[cat] date-parse-fail:", re.sub(r"\s+", " ", raw)[:120])
                    continue

                # ▼ ここで /video などを除外
                if _is_excluded_url(href):
                    continue

                if shown_date == date_obj and href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
                    found += 1

            # wrapper 内で“当日”が見つかったら soup まで広げず終了。
            # wrapper が無い場合（scopes が [soup] だけの時）も1周で抜ける。
            if found > 0:
                # dbg(f"[cat] STOP (added {found} candidates) @ {url}")
                break

    # ==== 1.5) ホーム（kuDRpuoカラム）巡回 → 当日候補抽出（新規） ====
    try:
        home_url = f"{BASE}/"
        res_home = fetch_with_retry_irrawaddy(home_url, session=session)
        soup_home = BeautifulSoup(res_home.content, "html.parser")

        # data-id でスコープ特定（class でも拾えるように冗長化）
        home_scope = soup_home.select_one(
            'div.elementor-element-kuDRpuo[data-id="kuDRpuo"], '
            "div.elementor-element-kuDRpuo, "
            '[data-id="kuDRpuo"]'
        )

        if home_scope:
            links = home_scope.select(".jeg_meta_date a[href]")
            links = [a for a in links if a.find("i", class_="fa fa-clock-o")]
            for a in links:
                href = a.get("href") or ""
                raw = a.get_text(" ", strip=True)
                try:
                    shown_date = _parse_category_date_text(raw)
                except Exception:
                    continue

                # ▼ ここでも除外
                if _is_excluded_url(href):
                    continue

                if shown_date == date_obj and href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
    except Exception as e:
        print(f"Error scanning homepage column kuDRpuo: {e}")

    # ログ、候補URL収集が終わった直後（カテゴリ＋ホーム統合のあと）
    dbg(f"[irrawaddy] candidates={len(candidate_urls)} (unique)")

    # ==== 1.9) フィード/外部RSSフォールバック（GitHub Actions等で403が続く場合） ====
    def _mmt_date(dt: datetime) -> date:
        try:
            return dt.astimezone(MMT).date()
        except Exception:
            # naive の場合はUTC→MMT換算とみなす
            return (dt.replace(tzinfo=timezone.utc)).astimezone(MMT).date()

    def _fetch_text(url: str, timeout: int = 20) -> str:
        try:
            r = requests.get(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/128.0.0.0 Safari/537.36"
                    )
                },
                timeout=timeout,
            )
            if r.status_code == 200 and (r.text or "").strip():
                return r.text
        except Exception:
            pass
        return ""

    def _fetch_text_via_jina(url: str, timeout: int = 25) -> str:
        try:
            alt = f"https://r.jina.ai/http://{url.lstrip('/')}"
            r = requests.get(
                alt,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/128.0.0.0 Safari/537.36"
                    )
                },
                timeout=timeout,
            )
            if r.status_code == 200 and (r.text or "").strip():
                return r.text
        except Exception:
            pass
        return ""

    def _rss_items_from_google_news() -> List[Dict[str, str]]:
        # Google News RSS（Irrawaddy限定）
        gnews = (
            "https://news.google.com/rss/search?"  
            "q=site:irrawaddy.com+when:1d&hl=en-US&gl=US&ceid=US:en"
        )
        xml = _fetch_text(gnews)
        if not xml:
            return []
        try:
            root = ET.fromstring(xml)
        except Exception:
            return []
        items = []
        href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
        for it in root.findall(".//item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            desc = (it.findtext("description") or "").strip()
            # description 内の最初の Irrawaddy 直リンクを優先
            direct = None
            try:
                m = href_re.search(desc)
                if m:
                    cand = m.group(1)
                    if "irrawaddy.com" in cand:
                        direct = cand
            except Exception:
                pass
            items.append({
                "title": title,
                "link": direct or link,
                "pubDate": pub,
            })
        return items

    def _parse_rfc822_date(s: str) -> Optional[datetime]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            return parse_date(s)
        except Exception:
            return None

    def _fallback_candidates_via_feeds() -> List[Dict[str, str]]:
        # 1) WordPress JSON（多くの場合WAF対象）
        wp_url = "https://www.irrawaddy.com/wp-json/wp/v2/posts?per_page=50&_fields=link,date,title"
        wp_json = _fetch_text(wp_url) or _fetch_text_via_jina(wp_url)
        cands: List[Dict[str, str]] = []
        if wp_json:
            try:
                arr = json.loads(wp_json)
                for o in arr:
                    link = (o.get("link") or "").strip()
                    ds = o.get("date") or ""
                    # タイトル（WPは title.rendered のことが多い）
                    t = o.get("title")
                    if isinstance(t, dict):
                        tt = (t.get("rendered") or "").strip()
                    else:
                        tt = (t or "").strip()
                    dt = _parse_rfc822_date(ds) or (
                        parse_date(ds) if ds else None
                    )
                    if link and dt and _mmt_date(dt) == date_obj and not _is_excluded_url(link):
                        cands.append({"url": link, "title": tt, "date": date_obj.isoformat()})
            except Exception:
                pass

        # 2) サイトRSS（403の可能性あり → 失敗時はスキップ）
        if not cands:
            feed_url = "https://www.irrawaddy.com/feed"
            feed_xml = _fetch_text(feed_url) or _fetch_text_via_jina(feed_url)
            if feed_xml:
                try:
                    root = ET.fromstring(feed_xml)
                    for it in root.findall(".//item"):
                        title = (it.findtext("title") or "").strip()
                        link = (it.findtext("link") or "").strip()
                        pub = (it.findtext("pubDate") or "").strip()
                        dt = _parse_rfc822_date(pub)
                        if link and dt and _mmt_date(dt) == date_obj and not _is_excluded_url(link):
                            cands.append({
                                "url": link,
                                "title": title,
                                "date": date_obj.isoformat(),
                            })
                except Exception:
                    pass

        # 3) Google News RSS（最終フォールバック）
        if not cands:
            # Google News RSS も通常取得→ダメなら Jina 経由で試す
            items = _rss_items_from_google_news()
            if not items:
                gnews = (
                    "https://news.google.com/rss/search?q=site:irrawaddy.com+when:1d&hl=en-US&gl=US&ceid=US:en"
                )
                xml = _fetch_text_via_jina(gnews)
                if xml:
                    try:
                        root = ET.fromstring(xml)
                        href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
                        items = []
                        for it in root.findall(".//item"):
                            title = (it.findtext("title") or "").strip()
                            link = (it.findtext("link") or "").strip()
                            pub = (it.findtext("pubDate") or "").strip()
                            desc = (it.findtext("description") or "").strip()
                            direct = None
                            m = href_re.search(desc)
                            if m and "irrawaddy.com" in m.group(1):
                                direct = m.group(1)
                            items.append({
                                "title": title,
                                "link": direct or link,
                                "pubDate": pub,
                            })
                    except Exception:
                        items = []

            def _resolve_gnews(u: str) -> str:
                try:
                    if "irrawaddy.com" in u or not u:
                        return u
                    if "news.google.com" not in u:
                        return u
                    UA = (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/128.0.0.0 Safari/537.36"
                    )
                    r = requests.get(
                        u,
                        headers={"User-Agent": UA},
                        timeout=10,
                        allow_redirects=False,
                    )
                    loc = r.headers.get("location") or r.headers.get("Location")
                    if loc and "irrawaddy.com" in loc:
                        return loc
                except Exception:
                    pass
                return u

            for it in items:
                link = _resolve_gnews(it.get("link") or "")
                title = it.get("title") or ""
                pub = it.get("pubDate") or ""
                dt = _parse_rfc822_date(pub)
                if not link or _is_excluded_url(link):
                    continue
                if dt and _mmt_date(dt) == date_obj:
                    cands.append({
                        "url": link,
                        "title": title,
                        "date": date_obj.isoformat(),
                    })

        # ユニーク化
        seen = set()
        uniq = []
        for o in cands:
            u = o["url"].strip()
            if u and u not in seen:
                seen.add(u)
                uniq.append(o)
        return uniq

    if len(candidate_urls) == 0:
        dbg("[irrawaddy] fallback to RSS/Google News due to 0 candidates")
        feed_cands = _fallback_candidates_via_feeds()
        dbg(f"[irrawaddy] feed candidates={len(feed_cands)}")
        # 既存パスと同じ形式に合わせる
        for o in feed_cands:
            u = o.get("url") or ""
            if u and u not in seen_urls:
                candidate_urls.append(u)
                seen_urls.add(u)
                if o.get("title"):
                    fallback_titles[u] = o.get("title") or ""

    # ==== 2) 候補記事で厳密確認（meta日付/本文/キーワード） ====
    for url in candidate_urls:
        if _is_excluded_url(url):  # ベルト＆サスペンダー
            continue
        try:
            title = ""
            body = ""
            # ① 直接取得（1回だけ）
            try:
                html_once = _fetch_text(url, timeout=20)
                if html_once:
                    soup_article = BeautifulSoup(html_once, "html.parser")
                    # Irrawaddy ドメインのときだけ、厳密に meta 日付を照合
                    try:
                        host = urlparse(url).netloc.lower()
                    except Exception:
                        host = ""
                    if "irrawaddy.com" in host:
                        if _article_date_from_meta_mmt(soup_article) != date_obj:
                            continue
                    title = _extract_title(soup_article) or ""
                    body = extract_body_irrawaddy(soup_article) or ""
            except Exception:
                pass

            # ② 直接取得できない場合、r.jina.ai 経由の本文テキストにフォールバック
            if not body:
                def _jina_fetch(u: str) -> str:
                    # r.jina.ai は Readability 抽出したプレーンテキストを返す
                    alt = f"https://r.jina.ai/http://{u.lstrip('/') }"
                    t = _fetch_text(alt, timeout=25)
                    if not t and "/news/" in u:
                        # AMP を試す
                        amp = u if u.endswith("/amp") else urljoin(u.rstrip("/") + "/", "amp")
                        alt2 = f"https://r.jina.ai/http://{amp.lstrip('/') }"
                        t = _fetch_text(alt2, timeout=25)
                    return t

                body_txt = _jina_fetch(url)
                if body_txt:
                    body = body_txt
                    if not title:
                        # フィードで拾ったタイトルを最終手段として流用
                        title = fallback_titles.get(url, "")

            # ③ それでもタイトルが空なら、oEmbed またはスラッグから補完
            if not title:
                if "irrawaddy.com" in (url or ""):
                    t2 = _oembed_title_irrawaddy(url)
                    if t2:
                        title = t2
                if not title:
                    title = _title_from_slug(url)

            # キーワードフィルタ（他媒体と同様）追加
            # 念のためタイトル/本文をNFC正規化してから判定
            title_nfc = unicodedata.normalize("NFC", title)
            body_nfc = unicodedata.normalize("NFC", body)
            if not any_keyword_hit(title_nfc, body_nfc):
                # ログを出してスキップ（本文抜粋は出さない共通ロガー）
                log_no_keyword_hit("Irrawaddy", url, title_nfc, body_nfc, "irrawaddy:article")
                _cache_body(url, body_nfc)
                continue

            results.append(
                {
                    "url": url,
                    "title": title_nfc,
                    "date": date_obj.isoformat(),
                    "body": body_nfc,
                    "source": "Irrawaddy",  # 重複削除関数を使うため追加
                }
            )
        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    # ==== 3) 最終重複排除（URLでユニーク化・先勝ち） ====
    before_dedup = len(results)
    results = deduplicate_by_url(results)

    # ログ、重複削除件数
    dbg(f"[irrawaddy] dedup: {before_dedup} -> {len(results)}")

    # ログ、最終的なresultの中身
    dbg(f"[irrawaddy] kept={len(results)}")

    def _one(s: str, n: int = 60) -> str:
        s = re.sub(r"\s+", " ", (s or "")).strip()
        return s[:n]

    for r in results[:3]:
        dbg(f"  - {_one(r.get('title'))} | {r.get('url')}")
    if len(results) > 3:
        dbg(f"  ... (+{len(results)-3} more)")

    return results


# DVB
def get_dvb_articles_for(date_obj: date, debug: bool = True) -> List[Dict]:
    """
    - /category/... の一覧（1ページ目＋?page=2）から、指定日と一致するカードだけ候補化。
    - 記事ページでは <title> / .full_content p を抽出。
    - タイトル・本文をNFC正規化して any_keyword_hit でフィルタ。
    - 返り値: [{url, title, date, body, source}]
    ※ DVB専用 fetch_with_retry_dvb を使用。
    以下3カテゴリ以外の記事は、すべて/category/8/newsに含まれている。
    - /category/1799/international-news
    - /category/1793/sports-news
    - /category/6/features
    当該3カテゴリは除外したいグループになるので/category/8/newsのみを取得対象とする。
    """
    BASE = "https://burmese.dvb.no"
    CATEGORY_PATHS = [
        "/category/8/news",
        # "/category/17/news_politics-new",
        # "/category/16/news_economics-new",
        # "/category/15/news_health-news-news",
        # "/category/18/news_social-news",
        # "/category/1787/news_education-news",
        # "/category/10/news_environment-weather",
        # "/category/1789/news_labour-news",
        # "/category/1788/news_farmers-news",
        # "/category/1797/news_criminals-news",
        # "/category/9/news_media-news",
        # "/category/6/features",
        # "/category/13/interview",
        # "/category/1799/international-news",
        # "/category/1793/sports-news",
    ]

    def _norm_path(p: str) -> str:
        return re.sub(r"/{2,}", "/", (p or "").strip())

    def _parse_dvb_date(text: str) -> Optional[date]:
        if not text:
            return None
        s = re.sub(r"\s+", " ", text.strip())
        try:
            return datetime.strptime(s, "%B %d, %Y").date()
        except ValueError:
            return None

    def _extract_title_dvb(soup: BeautifulSoup) -> str:
        t = (soup.title.string or "").strip() if soup.title else ""
        if t:
            return t
        h = soup.select_one(".text-2xl, h1, .post-title")
        return (h.get_text(" ", strip=True) if h else "").strip()

    def _extract_body_dvb(soup: BeautifulSoup) -> str:
        host = soup.select_one(".full_content")
        if not host:
            return ""
        parts = []
        for p in host.select("p"):
            txt = p.get_text(" ", strip=True)
            txt = re.sub(r"\s+", " ", txt)
            if txt:
                parts.append(txt)
        return "\n".join(parts).strip()

    log = (lambda *a, **k: print(*a, **k)) if debug else (lambda *a, **k: None)
    results: List[Dict] = []
    candidate_urls: List[str] = []
    seen_urls = set()

    # 共有セッション（cookies/指紋を一覧→記事で引き継ぐ）
    try:
        sess = requests.Session()
    except Exception:
        sess = None

    # ---- 1) カテゴリ一覧巡回（各カテゴリにつき page=1,2）
    for rel in CATEGORY_PATHS:
        rel = _norm_path(rel)
        for page_no in (1, 2):
            url = f"{BASE}{rel}" if page_no == 1 else f"{BASE}{rel}?page=2"
            try:
                res = fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=sess)
            except Exception as e:
                log(f"[warn] fetch fail {url}: {e}")
                continue

            if getattr(res, "status_code", 200) != 200:
                log(f"[skip] non-200 ({res.status_code}) {url}")
                continue

            soup = BeautifulSoup(
                getattr(res, "content", None) or res.text, "html.parser"
            )

            # 一覧ブロック（特徴で特定。無ければフォールバックでページ全体）
            blocks = soup.select(
                "div.md\\:grid.grid-cols-3.gap-4.mt-5, div.grid.grid-cols-3.gap-4.mt-5"
            ) or [soup]

            found = 0
            for scope in blocks:
                anchors = scope.select('a[href^="/post/"]')
                for a in anchors:
                    href = a.get("href") or ""
                    # 第一候補：カード内の date ブロック
                    date_div = a.select_one(
                        "div.flex.gap-1.text-xs.mt-2.text-gray-500 div"
                    )
                    date_text = (
                        date_div.get_text(" ", strip=True) if date_div else ""
                    ).strip()
                    # フォールバック：英語月名パターン
                    if not date_text:
                        full = a.get_text(" ", strip=True)
                        m = re.search(
                            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}",
                            full,
                        )
                        date_text = m.group(0) if m else ""
                    d = _parse_dvb_date(date_text)
                    if d and d == date_obj:
                        uabs = href if href.startswith("http") else f"{BASE}{href}"
                        if uabs not in seen_urls:
                            candidate_urls.append(uabs)
                            seen_urls.add(uabs)
                            found += 1
            log(f"[list] {url} -> candidates+{found}")

    log(f"[dvb] candidates total = {len(candidate_urls)} (unique)")

    # ---- 2) 候補記事ページで抽出（any_keyword_hit で絞り込み）
    for url in candidate_urls:
        try:
            res = fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=sess)
            if getattr(res, "status_code", 200) != 200:
                log(f"[skip] non-200 article {res.status_code} {url}")
                continue
            soup = BeautifulSoup(
                getattr(res, "content", None) or res.text, "html.parser"
            )

            title = _extract_title_dvb(soup)
            body = _extract_body_dvb(soup)
            if not title or not body:
                log(f"[skip] empty title/body {url}")
                continue

            title_nfc = unicodedata.normalize("NFC", title)
            body_nfc = unicodedata.normalize("NFC", body)
            if not any_keyword_hit(title_nfc, body_nfc):
                _cache_body(url, body_nfc)
                log_no_keyword_hit("DVB", url, title_nfc, body_nfc, "dvb:article")
                continue

            results.append(
                {
                    "url": url,
                    "title": title_nfc,
                    "date": date_obj.isoformat(),
                    "body": body_nfc,
                    "source": "dvb",
                }
            )
        except Exception as e:
            log(f"[warn] article fail {url}: {e}")
            continue

    # ---- 3) 重複排除
    before = len(results)
    results = deduplicate_by_url(results)
    log(f"[dvb] dedup: {before} -> {len(results)}")

    # ---- 4) デバッグ表示（先頭数件）
    def _one(s: str, n: int = 60) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())[:n]

    for r in results[:3]:
        log(f"  - {_one(r.get('title'))} | {r.get('url')}")
    if len(results) > 3:
        log(f"  ... (+{len(results)-3} more)")

    return results

# Myanmar Now 
def get_myanmar_now_articles_mm(date_obj, max_pages=3):
    """
    Myanmar Now の各カテゴリから対象日の記事を取得して返す。
    - カテゴリ一覧を最大 max_pages ページ巡回
    - 一覧では <span class="date meta-item tie-icon">Month D, YYYY</span> を見て今日だけ抽出
    - 個別記事では <meta property="article:published_time" content="..."> をUTC→MMT変換して最終確認
    - タイトル末尾の " - Myanmar Now" を除去
    - 本文は div.entry-content.entry.clearfix 内の <p> だけ（画像等は含めない）
    返り値: list[dict] {url, title, date(ISO str, MMT), body, source="Myanmar Now"}
    """

    BASE_CATEGORIES = [
        "https://myanmar-now.org/mm/news/category/news/",                 # ニュース
        "https://myanmar-now.org/mm/news/category/news/3/",               # 政治
        "https://myanmar-now.org/mm/news/category/news/17/",              # 経済
        "https://myanmar-now.org/mm/news/category/news/social-issue/",    # 社会
        "https://myanmar-now.org/mm/news/category/news/19/",              # 教育
        "https://myanmar-now.org/mm/news/category/news/international-news/",  # 国際ニュース
        "https://myanmar-now.org/mm/news/category/multimedia/16/",        # 健康
        "https://myanmar-now.org/mm/news/category/in-depth/",             # 特集記事
        "https://myanmar-now.org/mm/news/category/in-depth/analysis/",    # 分析
        "https://myanmar-now.org/mm/news/category/in-depth/investigation/", # 調査報道
        "https://myanmar-now.org/mm/news/category/in-depth/profile/",     # プロフィール
        "https://myanmar-now.org/mm/news/category/in-depth/society/",     # 社会分野
        "https://myanmar-now.org/mm/news/category/opinion/",              # 論説
        "https://myanmar-now.org/mm/news/category/opinion/commentary/",   # 論評
        "https://myanmar-now.org/mm/news/category/opinion/29/",           # 編集長の論説
        "https://myanmar-now.org/mm/news/category/opinion/interview/",    # インタビュー
        # "https://myanmar-now.org/mm/news/category/opinion/essay/",        # エッセイ
        # "https://myanmar-now.org/mm/news/category/opinion/26/",           # 風刺
        # "https://myanmar-now.org/mm/news/category/multimedia/video/",     # 動画ニュース
        # "https://myanmar-now.org/mm/news/category/multimedia/13/",        # フォトエッセイ
    ]

    # "September 8, 2025" のような英語表記
    today_label = f"{date_obj.strftime('%B')} {date_obj.day}, {date_obj.year}"

    def _strip_source_suffix(title: str) -> str:
        if not title:
            return title
        return re.sub(r"\s*-\s*Myanmar Now\s*$", "", title).strip()

    def _collect_article_urls_from_category(cat_url: str) -> set[str]:
        urls = set()
        for page in range(1, max_pages + 1):
            url = f"{cat_url}page/{page}/" if page > 1 else cat_url
            try:
                res = fetch_with_retry(url)
            except Exception:
                break
            soup = BeautifulSoup(res.content, "html.parser")

            for span in soup.select("span.date.meta-item.tie-icon"):
                if (span.get_text(strip=True) or "") != today_label:
                    continue
                a = span.find_parent("a", href=True)
                if not a:
                    parent_a = span
                    while parent_a and parent_a.name != "a":
                        parent_a = parent_a.parent
                    if parent_a and parent_a.name == "a" and parent_a.get("href"):
                        a = parent_a
                if a and a.get("href"):
                    href = a["href"]
                    if "/mm/news/" in href:
                        urls.add(href)
        return urls

    collected = set()
    for base in BASE_CATEGORIES:
        collected |= _collect_article_urls_from_category(base)

    results = []
    for url in collected:
        try:
            # --- helpers ---
            def _fetch_text(u: str, timeout: int = 20) -> str:
                try:
                    r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
                    if r.status_code == 200 and (r.text or "").strip():
                        return r.text
                except Exception:
                    pass
                return ""

            def _fetch_text_via_jina(u: str, timeout: int = 25) -> str:
                try:
                    alt = f"https://r.jina.ai/http://{u.lstrip('/')}"
                    r = requests.get(alt, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
                    if r.status_code == 200 and (r.text or "").strip():
                        return r.text
                except Exception:
                    pass
                return ""

            def _oembed_title(u: str) -> str:
                try:
                    api = (
                        "https://myanmar-now.org/wp-json/oembed/1.0/embed?url="
                        + requests.utils.requote_uri(u)
                    )
                    r = requests.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                    if r.status_code == 200 and (r.text or "").strip():
                        data = r.json()
                        return unicodedata.normalize("NFC", (data.get("title") or "").strip())
                except Exception:
                    pass
                return ""

            def _title_from_slug(u: str) -> str:
                try:
                    from urllib.parse import urlparse, unquote
                    seg = urlparse(u).path.rstrip("/").split("/")[-1]
                    seg = unquote(seg).replace("-", " ")
                    return unicodedata.normalize("NFC", seg)
                except Exception:
                    return ""

            # --- fetch once ---
            soup = None
            dt_mmt = None
            try:
                res = fetch_with_retry(url)
                soup = BeautifulSoup(res.content, "html.parser")
                meta = soup.find("meta", attrs={"property": "article:published_time"})
                if meta and meta.get("content"):
                    try:
                        dt_utc = datetime.fromisoformat(meta["content"])
                    except Exception:
                        dt_utc = parse_date(meta["content"]).astimezone(timezone.utc)
                    dt_mmt = dt_utc.astimezone(MMT)
            except Exception:
                soup = None
            meta_ok = dt_mmt is not None and dt_mmt.date() == date_obj

            # --- title ---
            title = ""
            if soup is not None:
                title_raw = (soup.title.get_text(strip=True) if soup.title else "").strip()
                title = _strip_source_suffix(unicodedata.normalize("NFC", title_raw))
                if not title:
                    h1 = soup.find("h1")
                    if h1:
                        title = _strip_source_suffix(
                            unicodedata.normalize("NFC", h1.get_text(strip=True))
                        )
            if not title:
                title = _oembed_title(url) or _title_from_slug(url)
                title = _strip_source_suffix(title)
            if not title:
                continue

            # --- body ---
            body = ""
            if soup is not None:
                content_root = soup.select_one("div.entry-content.entry.clearfix") or soup
                parts = []
                for p in content_root.find_all("p"):
                    txt = p.get_text(strip=True)
                    if txt:
                        parts.append(txt)
                body = unicodedata.normalize("NFC", "\n".join(parts).strip())
                if not body:
                    paragraphs = extract_paragraphs_with_wait(soup)
                    body = unicodedata.normalize(
                        "NFC",
                        "\n".join(
                            p.get_text(strip=True)
                            for p in paragraphs
                            if getattr(p, "get_text", None)
                        ),
                    ).strip()
            if not body:
                body = _fetch_text_via_jina(url) or _fetch_text(url)
            if not body:
                continue

            # 4) キーワード判定
            _cache_body(url, body)
            if not any_keyword_hit(title, body):
                log_no_keyword_hit("Myanmar Now", url, title, body, "mnw:article")
                continue

            results.append({
                "url": url,
                "title": title,
                "date": (dt_mmt.isoformat() if meta_ok else date_obj.isoformat()),
                "body": body,
                "source": "Myanmar Now",
            })
        except Exception as e:
            print(f"[warn] Myanmar Now article fetch failed: {url} ({e})")
            continue

    before = len(results)
    results = deduplicate_by_url(results)
    print(f"[myanmar-now-mm] dedup: {before} -> {len(results)}")
    return results

# 同じURLの重複削除
def deduplicate_by_url(articles):
    seen_urls = set()
    unique_articles = []
    for art in articles:
        if art["url"] in seen_urls:
            print(
                f"🛑 URL Duplicate Removed: {art['source']} | {art['title']} | {art['url']}"
            )
            continue
        seen_urls.add(art["url"])
        unique_articles.append(art)
    return unique_articles


# 翻訳対象キュー
translation_queue = []


def process_and_enqueue_articles(
    articles,
    source_name,
    seen_urls=None,
    bypass_keyword=False,
    trust_existing_body=False,
):
    if seen_urls is None:
        seen_urls = set()

    queued_items = []
    for art in articles:
        if art["url"] in seen_urls:
            continue
        seen_urls.add(art["url"])

        try:
            # ① まずは記事オブジェクトに本文が来ていたらそれを使う
            body_text = (art.get("body") or "").strip() if trust_existing_body else ""

            # ② 無ければフェッチ（内部で再フェッチ付きユーティリティを使用）
            if not body_text:
                if source_name == "Irrawaddy" or "irrawaddy.com" in art["url"]:
                    body_text = get_body_with_refetch(
                        art["url"],
                        fetcher=lambda u: fetch_once_irrawaddy(
                            u, session=requests.Session()
                        ),
                        extractor=extract_body_irrawaddy,  # 既存の抽出器を使用
                        retries=3,
                        wait_seconds=2,
                        quiet=False,
                    )
                else:
                    body_text = get_body_with_refetch(
                        art["url"],
                        fetcher=fetch_once_requests,
                        extractor=extract_body_generic_from_soup,
                        retries=2,
                        wait_seconds=1,
                        quiet=True,
                    )

            # ③ 正規化
            title_nfc = unicodedata.normalize("NFC", art["title"])
            body_nfc = unicodedata.normalize("NFC", body_text)
            
            # エーヤワディ系/全体/非エーヤワディのヒット判定（各1回のみ）
            is_ayeyar = is_ayeyarwady_hit(title_nfc, body_nfc)
            # 非エーヤワディのヒット（NEWS_KEYWORDS のみ）
            hit_non_aye = any_keyword_hit(title_nfc, body_nfc)
            # 全体ヒット = 非エーヤワディ or エーヤワディ
            hit_full = hit_non_aye or is_ayeyar

            # ④ キーワード判定（Irrawaddyなど必要に応じてバイパス）
            if not bypass_keyword:
                if not hit_full:
                    log_no_keyword_hit(
                        source_name,
                        art["url"],
                        title_nfc,
                        body_nfc,
                        "enqueue:after-fetch",
                    )
                    continue

            # ⑤ キュー投入
            queued_items.append(
                {
                    "source": source_name,
                    "url": art["url"],
                    "title": art["title"],  # 翻訳前タイトル
                    "body": body_text,  # 翻訳前本文
                    "is_ayeyar": is_ayeyar,  # エーヤワディ系ヒット判定
                    "hit_full": hit_full,  # 全体キーワード判定
                    "hit_non_ayeyar": hit_non_aye,  # 非エーヤワディ判定
                }
            )

        except Exception as e:
            print(f"Error processing {art['url']}: {e}")
            continue

    translation_queue.extend(queued_items)


# MEMO: ログ用、デバック用関数
# def process_translation_batches(batch_size=10, wait_seconds=60):
#     summarized_results = []

#     # テスト用に translation_queue の中身をそのまま summarized_results に詰める
#     for item in translation_queue:
#         summarized_results.append({
#             "source": item["source"],
#             "url": item["url"],
#             "title": item["title"],      # 翻訳前タイトル
#             "summary": item["body"][:2000]  # 要約の代わりに本文冒頭
#         })

#     # デバッグ出力（summarized_results の中身を省略せず確認）
#     print("===== DEBUG: summarized_results =====")
#     pprint.pprint(summarized_results, width=120, compact=False)
#     print("===== END DEBUG =====")

#     # ここで処理終了
#     return summarized_results


# 重複記事削除処理セット
def _strip_tags(text: str) -> str:
    # 要約に含めた <br> などを素テキスト化（最低限）
    text = text.replace("<br>", "\n")
    return re.sub(r"<[^>]+>", "", text)

def _has_procedure_lines(s: str) -> bool:
    # 思考用の行（Step/Q/→）が混ざっているかをざっくり検知
    if not s:
        return False
    for ln in s.splitlines():
        ln = ln.strip()
        if re.match(r'^(Step\s*\d+|Q\d+\.|→)', ln):
            return True
    return False

def _count_summary_markers(s: str) -> int:
    if not s:
        return 0
    return len(re.findall(r'^【\s*要約\s*】\s*$', s, flags=re.M))

def extract_final_summary(generated: str) -> str:
    """
    “最後の【要約】ブロック”だけ抜き出し、Step/Q/→ を除去。
    見出し「【要約】」は含めず本文のみ返す。
    """
    text = (generated or "").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    idxs = [i for i, ln in enumerate(lines) if re.match(r'^【\s*要約\s*】\s*$', ln)]
    start = idxs[-1] + 1 if idxs else 0
    block = lines[start:]

    # 終了マーカー（必要に応じて）
    stop_markers = [r'^本文を読む', r'^【\s*タイトル\s*】']
    for i, ln in enumerate(block):
        if any(re.match(p, ln) for p in stop_markers):
            block = block[:i]
            break

    block = [ln for ln in block if not re.match(r'^(Step\s*\d+|Q\d+\.|→)', ln)]
    return "\n".join(block).strip()

def build_summary_lines(output_text: str, original_lines: list[str]) -> list[str]:
    """
    既存の summary 行（original_lines）を“基本そのまま”返す。
    ただし以下のときのみ、最後の【要約】抽出に切り替える：
      - 「【要約】」が 2 回以上
      - Step/Q/→ の手順系が混入
    """
    multi = _count_summary_markers(output_text) >= 2
    polluted = _has_procedure_lines(output_text)

    if not multi and not polluted:
        # いつも通りの挙動（既存処理）を維持
        return original_lines

    # 異常時だけ補正
    summary_only = extract_final_summary(output_text)
    fixed = [ln.strip() for ln in summary_only.splitlines() if ln.strip()]

    # 先頭に見出しが無ければ付与（従来の体裁を維持）
    if not fixed or not re.match(r'^【\s*要約\s*】\s*$', fixed[0]):
        fixed.insert(0, "【要約】")
    return fixed

def _safe_json_loads_maybe_extract(text: str):
    """
    生成AIが前後に余計な文を付けた場合でもJSON部分だけ抽出して読む保険。
    """
    try:
        return json.loads(text)
    except Exception:
        # 最後の { ... } を素朴に抽出
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if m:
            return json.loads(m.group(0))
        raise
    
# 公開ヘルパ
def normalize_summary_text(output_text: str) -> str:
    """
    既存出力（output_text）を“基本そのまま”返すが、
    ・「【要約】」が2回以上
    ・Step/Q/→ の手順系が混入
    のときのみ、最後の【要約】ブロックに補正して返す。
    先頭行に「【要約】」が無ければ付与する。
    """
    original_lines = [(output_text or "").strip()]  # 既定は丸ごと1行扱い
    try:
        lines = build_summary_lines(output_text, original_lines)
        return "\n".join(lines).strip()
    except Exception:
        # 何かあっても壊さない
        return (output_text or "").strip()

# 重複判定のログ出力
def log_dedupe_report(
    data: dict,
    id_map: dict,
    id_to_meta: dict,
    article_ids_in_order: list[str],
    *,
    printer=print,
    header="🧩 DEDUPE REPORT",
):
    """
    LLM応答データ(data)と、ID→記事メタ情報のマップを受け取り、
    重複判定レポートを整形して出力する。

    - data: {"kept":[...], "removed":[...], "clusters":[...]}
    - id_map: {id -> 元オブジェクト}
    - id_to_meta: {id -> {"title": str, "source": str}}
    - article_ids_in_order: 入力順序のIDリスト（元配列の順を保つために使用）
    - printer: 出力関数（print や logger.info など）
    """
    kept_list = data.get("kept") or []
    removed_list = data.get("removed") or []
    clusters = data.get("clusters") or []

    kept_ids = [x.get("id") for x in kept_list if x.get("id") in id_map]
    kept_set = set(kept_ids)

    printer(f"\n===== {header} =====")

    # 1) Kept 概要
    printer(f"Kept: {len(kept_ids)} item(s)")
    for k in kept_list:
        kid = k.get("id")
        meta = id_to_meta.get(kid, {})
        why = (k.get("why") or "").strip()
        if kid in id_map:
            why_part = (
                f"  | why: {why}" if why else ""
            )  # ← バックスラッシュを式に入れない
            printer(
                f"  ✓ [{kid}] {meta.get('title','(no title)')}  | src={meta.get('source','')}"
                f"{why_part}"
            )
        else:
            printer(f"  ✓ [{kid}] (unknown id)")

    # 2) Removed 詳細（どれの重複として落ちたか）
    printer(f"\nRemoved (LLM-reported): {len(removed_list)} item(s)")
    for r in removed_list:
        rid = r.get("id")
        dup = r.get("duplicate_of")
        why = (r.get("why") or "").strip()
        rmeta = id_to_meta.get(rid, {"title": "(unknown)", "source": ""})
        kmeta = id_to_meta.get(dup, {"title": "(unknown)", "source": ""})
        unknown_flags = []
        if rid not in id_map:
            unknown_flags.append("RID_NOT_IN_INPUT")
        if dup and dup not in id_map:
            unknown_flags.append("KEPT_NOT_IN_INPUT")
        uf = f"  [{', '.join(unknown_flags)}]" if unknown_flags else ""
        reason_line = f"\n      reason: {why}" if why else ""  # ← 先に作る
        printer(
            f"  - [{rid}] {rmeta['title']}  | src={rmeta['source']}\n"
            f"      → duplicate of [{dup}] {kmeta['title']}  | src={kmeta['source']}{uf}"
            f"{reason_line}"
        )

    # 3) 実差分（入力 - kept）
    derived_removed_ids = [aid for aid in article_ids_in_order if aid not in kept_set]
    printer(f"\nRemoved (derived by kept-set): {len(derived_removed_ids)} item(s)")
    for rid in derived_removed_ids:
        rmeta = id_to_meta.get(rid, {"title": "(unknown)", "source": ""})
        rrec = next((x for x in removed_list if x.get("id") == rid), None)
        if rrec:
            dup = rrec.get("duplicate_of")
            why = (rrec.get("why") or "").strip()
            kmeta = id_to_meta.get(dup, {"title": "(unknown)", "source": ""})
            reason_line = f"\n      reason: {why}" if why else ""  # ← 先に作る
            printer(
                f"  - [{rid}] {rmeta['title']}  | src={rmeta['source']}\n"
                f"      → duplicate of [{dup}] {kmeta['title']}  | src={kmeta['source']}"
                f"{reason_line}"
            )
        else:
            printer(
                f"  - [{rid}] {rmeta['title']}  | src={rmeta['source']} (※ LLMのremovedに未記載)"
            )

    # 4) 参照整合性チェック
    unknown_kept = [
        kid for kid in [x.get("id") for x in kept_list] if kid not in id_map
    ]
    unknown_removed = [r.get("id") for r in removed_list if r.get("id") not in id_map]
    if unknown_kept:
        printer(f"\n⚠️ Keptに未知のIDが含まれています: {unknown_kept}")
    if unknown_removed:
        printer(f"⚠️ Removedに未知のIDが含まれています: {unknown_removed}")

    # 5) クラスタ概要（任意）
    if clusters:
        printer("\nCluster summary:")
        cluster_kept_map = {
            k.get("cluster_id"): k.get("id") for k in kept_list if k.get("cluster_id")
        }
        for c in clusters:
            cid = c.get("cluster_id")
            members = c.get("member_ids") or []
            event_key = c.get("event_key") or ""
            kept_id_for_cluster = cluster_kept_map.get(cid)
            printer(
                f"  • cluster={cid}  members={len(members)}  kept={kept_id_for_cluster}  event='{event_key}'"
            )

    printer("===== END DEDUPE REPORT =====\n")


def dedupe_articles_with_llm(
    client,
    summarized_results,
    debug=True,
    *,
    logger=None,
    ultra_max_chars=300,
    summary_fallback_chars=600,
):
    """
    summarized_results (list[dict]) を受け取り、重複クラスターごとに1本だけ残した配列を返す。
    Irrawaddy（source == "Irrawaddy" または URL に "irrawaddy.com" を含む）は
    LLM での重複判定をスキップして常に keep する。
    依存: call_gemini_with_retries, _safe_json_loads_maybe_extract, _strip_tags, log_dedupe_report
    """

    if not summarized_results:
        return summarized_results

    # 出力関数
    if debug:
        printer = logger.info if logger else print
    else:

        def _noop(*args, **kwargs):
            return None

        printer = _noop

    # ===== LLM入力用（Irrawaddy を除外）を構築 =====
    irrawaddy_ids = set()
    articles_for_llm = []
    id_map_llm = {}
    id_to_meta_llm = {}
    ids_in_order_llm = []
    all_ids_in_order = []  # 返却時の順序維持用

    for idx, it in enumerate(summarized_results):
        _id_raw = it.get("url") or f"idx-{idx}"
        _id = _norm_id(_id_raw)  # ★ 入力側（自分側）のIDを正規化
        all_ids_in_order.append(_id)

        # Irrawaddy 判定（ご指定どおり）
        is_irrawaddy = (it.get("source") == "Irrawaddy") or (
            "irrawaddy.com" in (it.get("url") or "")
        )
        if is_irrawaddy:
            irrawaddy_ids.add(_id)
            continue  # LLM には送らない

        # 非 Irrawaddy → LLM 入力へ
        body_ultra = (it.get("ultra") or "").strip()
        body_fallback = _strip_tags(it.get("summary", ""))[:summary_fallback_chars]
        body = body_ultra[:ultra_max_chars] if body_ultra else body_fallback

        ids_in_order_llm.append(_id)
        id_map_llm[_id] = it
        id_to_meta_llm[_id] = {"title": it.get("title"), "source": it.get("source")}
        articles_for_llm.append(
            {
                "id": _id,
                "source": it.get("source"),
                "title": it.get("title"),
                "body": body,
            }
        )

    # すべて Irrawaddy だった場合はそのまま返す
    if not articles_for_llm:
        if debug and irrawaddy_ids:
            printer(
                f"⏭️ 全 {len(irrawaddy_ids)} 件が Irrawaddy。LLM 重複判定はスキップします。"
            )
        return summarized_results

    # ===== デバッグ出力（LLM に送る分のみ） =====
    if debug:
        if irrawaddy_ids:
            printer(f"⏭️ Irrawaddy {len(irrawaddy_ids)} 件は常に keep（LLM スキップ）。")
        printer("===== DEBUG 2: articles SENT TO LLM =====")
        printer(_pprint.pformat(articles_for_llm, width=120, compact=False))
        printer("===== END DEBUG 2 =====\n")

    # ===== プロンプト（非 Irrawaddy のみ） =====
    prompt = (
        "あなたはニュースの重複判定フィルタです。\n"
        "以後の判定は各記事の「title」と「body（これは超要約または短縮要約）」のみを使用し、元本文には戻って再参照しません。\n"
        "目的：同一主旨（トピック + 角度 + 発信主体）を報じる記事を束ね、各クラスターから1本だけ残します。出力は必ずJSONのみ。\n\n"
        "【定義】\n"
        "・トピック一致：who / what / where / when のうち少なくとも3要素が一致（言い換え・言語差は同一扱い。日付は±14日を同一扱い可）。\n"
        "・記事の種類（type）：以下の正規化カテゴリのいずれか1つに内部で分類して用いる（出力には含めない）。\n"
        "  速報/単報, 政策発表要点, 公式発表/声明, インタビュー, 解説/背景, 物声明, 組織声明, 公示,\n"
        "  データ/統計, まとめ/ダイジェスト, ライブ/時系列更新,\n"
        "  写真/映像特集, 社説/論説/寄稿, プロフィール\n"
        "  近い同義語は内部で正規化：『press release/announcement→公式発表/声明』『explainer/analysis→解説/背景』\n"
        "  『roundup/digest→まとめ/ダイジェスト』『live updates→ライブ/時系列更新』\n"
        "  判別不能な場合は type=不明 とし、種類一致には数えない。\n"
        "・発信主体（provenance）：以下のいずれか1つを内部で推定して用いる。\n"
        "  ① 本人指示/首長の直言（例：ミン・アウン・フラインが「指示/命令/表明」）\n"
        "  ② 公式機関の発表（官報/会見/文書/広報）\n"
        "  ③ 匿名の軍筋/関係者/消息筋/内部筋（「軍筋によれば」「関係者によると」等）\n"
        "  ④ 現地運用・治安部隊/委員会の実務通達\n\n"
        "【判定方針】\n"
        "1) 同一主旨 = 『トピック一致』かつ『種類一致（typeが一致、かつ不明以外）』かつ『発信主体（provenance）一致』の全てを満たす場合に限る。\n"
        "   ※ まとめ/ダイジェスト/複数案件列挙の要約と、単一案件の速報・解説は重複にしない（別クラスター）。\n"
        "   ※ 同一テーマ（例：選挙運動規制）でも『内容規制』と『運用・手続（許認可/場所/時間/警備/管理）』は別角度として必ず別クラスターにする。\n"
        "   例：〈軍への批判的選挙運動を禁じる（内容規制）〉と〈軍の管理下・事前許可でのみ選挙活動可（運用・手続）〉は別クラスター。\n"
        "   例：〈MAH本人が“批判禁止”を指示（本人指示）〉と〈ネピドー軍筋が“許可制・管理下”と伝聞（軍筋）〉は、角度も発信主体も異なるため別クラスター。\n"
        "2) クラスター化：記事は最も一致度が高いクラスターにのみ所属。不確実なら別クラスターにする。\n"
        "3) 残す基準：a)固有情報量（地名/人数/金額/組織名/新規事実） b)具体性/明瞭さ c)タイトル情報量。\n"
        "   同点なら 本文長（bodyの文字数）→ source昇順 → id昇順 の順で決定。\n"
        "4) 入力外の事実は加えない。統合記事は作らない。\n\n"
        "【出力の制約】\n"
        "・JSONのみを返す。余計なテキストやキーは禁止。\n"
        "・kept/removed/clusters の id は必ず入力 articles の id に含まれていること。\n"
        "・clusters[].member_ids は入力 id を重複なくすべて含むこと。クラスター数と kept件数は同数。\n"
        "・removed[].duplicate_of は同一クラスター内の kept id を指すこと。\n"
        "・why は16〜24字程度、event_key は25字以内に収めること。\n\n"
        "入力:\n"
        f'{{\\n  "articles": {json.dumps(articles_for_llm, ensure_ascii=False)}\\n}}\\n\\n'
        "出力フォーマット（JSONのみ）:\n"
        "{\n"
        '  "kept": [ {"id":"<残す記事ID>", "cluster_id":"<ID>", "why":"16-24字"} ],\n'
        '  "removed": [ {"id":"<除外記事ID>", "duplicate_of":"<残した記事ID>", "why":"16-24字"} ],\n'
        '  "clusters": [ {"cluster_id":"<ID>", "member_ids":["<id1>","<id2>","..."], "event_key":"25字以内"} ]\n'
        "}\n"
    )

    try:
        resp = call_gemini_with_retries(
            client,
            prompt,
            model="gemini-2.5-flash",
            max_retries=GEMINI_MAX_RETRIES,
            base_delay=GEMINI_BASE_DELAY,
            max_delay=GEMINI_MAX_DELAY,
            usage_tag="dedupe",
        )
        data = _safe_json_loads_maybe_extract(resp.text)

        # ★ LLM応答内のIDをすべて正規化しておく
        for k in ("kept", "removed"):
            arr = data.get(k) or []
            for rec in arr:
                if "id" in rec:
                    rec["id"] = _norm_id(rec["id"])
                if "duplicate_of" in rec and rec["duplicate_of"]:
                    rec["duplicate_of"] = _norm_id(rec["duplicate_of"])

        for c in data.get("clusters", []) or []:
            if "cluster_id" in c:
                c["cluster_id"] = _norm_id(c["cluster_id"])
            if "member_ids" in c and isinstance(c["member_ids"], list):
                c["member_ids"] = [_norm_id(x) for x in c["member_ids"]]

        kept_ids_others = [
            x.get("id") for x in data.get("kept", []) if x.get("id") in id_map_llm
        ]

        # レポート（LLM に送った分のみ）
        if debug:
            log_dedupe_report(
                data=data,
                id_map=id_map_llm,
                id_to_meta=id_to_meta_llm,
                article_ids_in_order=ids_in_order_llm,
                printer=printer,
                header="🧩 DEDUPE REPORT (non-Irrawaddy only)",
            )

        # kept が出たときのみフィルタ適用し、Irrawaddy を合流
        if kept_ids_others:
            kept_union = set(kept_ids_others) | irrawaddy_ids
            filtered = [
                obj
                for obj, _id in zip(summarized_results, all_ids_in_order)
                if _id in kept_union
            ]
            return filtered

        # うまく判定できなかったら原本を返す（Irrawaddy も当然残る）
        return summarized_results

    except Exception as e:
        print(f"🛑 Dedupe failed, returning original list: {e}")
        return summarized_results


# ===== 要約・翻訳プロンプトパーツ =====
STEP12_FILTERS = (
    "Step 1: 例外チェック（最優先）\n"
    "Q1. 記事タイトルまたは本文が `Myawaddy`, `မြဝတီ`, `Muse`, `မူဆယ်`, `国境貿易`, `国境交易`に関する内容ですか？\n"
    "→ Yes の場合、この後の判定は行わず Step 3 に進んでください。\n"
    "→ No の場合は Step 2 へ進んでください。\n\n"
    "Step 2: 除外条件チェック（地域例外あり／主題＝記事の角度で判定）\n"
    "Q2. 本記事の主題が、特定の地域（郡区・タウンシップ・市・村）における\n"
    "    局地的治安イベントの「発生そのもの」を速報・記録・報道する記事ですか？\n"
    "    （戦闘・交戦、攻撃〔襲撃/爆破/狙撃/IED/ドローン〕、衝突、爆撃/砲撃/空爆、\n"
    "      強盗/略奪、抗議活動、投降、解放、殺人事件の発生報道・被害集計）\n"
    "    補足：軍事政権に抗議するデモ・集会・行進などの開催や参加人数・場所を報じる記事も上記「抗議活動」に含まれ、除外対象とする。\n"
    "\n"
    "除外しない明確な例（= No とする）：\n"
    "- 人物/組織の発言・反論・声明・会見・プレスリリース・告発・否定が主題のもの\n"
    "  （事件の具体例や地名・人数が含まれていても主題が上記で示した「発言」なら No）\n"
    "- 事件の発生そのものではなく、事件が引用として使われているだけの記事\n"
    "\n"
    "主題判定の手がかり：\n"
    "- タイトル・冒頭優先（タイトル先頭60字＋本文冒頭300字を重み付け）\n"
    "- 発言主題を示す合図語（日本語/英語/ビルマ語）：\n"
    "  「声明」「発表」「反論」「否定」「会見」「談話」「と述べた」「と語った」「と主張」、\n"
    '  "statement","press conference","spokesperson","said","denied","accused",\n'
    "  「ပြောဆို」「ထုတ်ပြန်」「တုံ့ပြန်」「ဆိုသည်」「ပြောကြား」「ပြောရေးဆိုခွင့်ရှိသူ」「သတင်းစာရှင်းလင်းပွဲ」\n"
    "→ 上記に該当すれば No として Step 3 へ進む。\n"
    "\n"
    "→ Yes の場合でも、記事の主たる発生場所が次の地域に該当するなら除外せず Step 3 へ進んでください：\n"
    "   ・ヤンゴン管区 / Yangon Region / ရန်ကုန်တိုင်း\n"
    "   ・エーヤワディ管区 / Ayeyarwady Region / ဧရာဝတီတိုင်း\n"
    "→ 上記以外の地域であれば処理を終了し、Step 3 には進まないでください。回答は exit の1語のみ（記号・装飾・コードブロックなし、小文字）で返してください。\n"
    "→ No の場合は Step 3 へ進んでください。\n"
)


# ===== 翻訳プロンプト：共通ルール =====
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

STEP3_TASK = (
    "Step 3: 翻訳と要約処理\n"
    "以下のルールに従って、記事タイトルを自然な日本語に翻訳し、本文を要約してください。\n\n"
    f"{COMMON_TRANSLATION_RULES}"
    "タイトル：\n"
    "あなたは報道見出しの専門翻訳者です。以下の英語/ビルマ語の見出しタイトルを、"
    "自然で簡潔な日本語見出しに翻訳してください。固有名詞は一般的な日本語表記を優先し、"
    "意訳しすぎず要点を保ち、記号の乱用は避けます。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
    "タイトルの出力条件：\n"
    "- 出力は必ず1行で「【タイトル】<半角スペース1つ><訳したタイトル>」の形式にする。\n"
    "- 「【タイトル】」の直後に改行しない。\n"
    "- 「【タイトル】<半角スペース1つ><訳したタイトル>」以外の文言は回答に含めない。\n\n"
    "本文要約：\n"
    "- 以下の記事本文について重要なポイントをまとめ、最大500字で具体的に要約する（500字を超えない）。\n"
    "- 自然な日本語に翻訳する。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
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
    "- 本文要約の合計は最大500文字以内に収める。超えそうな場合は重要情報を優先して削る（日時・主体・行為・規模・結果を優先）。\n\n"
    "本文超要約：\n"
    "- 以下の記事本文について重要なポイント・ユニークなキーワードをまとめ、200字以内で要約してください。\n"
    "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
    "- 例：『誰が』『何を』『どこで』『いつ』『規模（人数/金額等）』を含める。\n\n"
    "本文超要約の出力条件：\n"
    "- 1行目は`【超要約】`とだけしてください。\n"
    "- 2行目以降全て空行になってはいけません。\n"
    "- 本文超要約の合計は最大200文字以内に収めてください。\n\n"
)

SKIP_NOTE_IRRAWADDY = "【重要】本記事は Irrawaddy の記事です。Step 1 と Step 2 は実施せず、直ちに Step 3 のみを実施してください。\n\n"

# ===== 用語集（A:Myanmar / B:English / C:本文訳 / D:見出し訳） =====
_TERM_CACHE: list[dict] | None = None
TERM_SHEET_ID = os.getenv("MNA_SHEET_ID")
TERM_SHEET_NAME = os.getenv("MNA_TERM_SHEET_NAME") or "regions"

def _gc_client_terms():
    # 既存の gspread 認証関数があればそれを再利用。無ければこのモジュール内の実装でOK。
    from google.oauth2.service_account import Credentials
    import gspread, json
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
        creds = Credentials.from_service_account_info(json.loads(info), scopes=scopes)
        return gspread.authorize(creds)
    raise SystemExit("Google SA credential not found for regions.")

def _load_term_glossary_gsheet() -> list[dict]:
    global _TERM_CACHE
    if _TERM_CACHE is not None:
        return _TERM_CACHE
    try:
        ws = _gc_client_terms().open_by_key(TERM_SHEET_ID).worksheet(TERM_SHEET_NAME)
        vals = ws.get_all_values() or []
        rows = []
        for r in (vals[1:] if len(vals) > 1 else []):
            mm = (r[0] if len(r) > 0 else "").strip()
            en = (r[1] if len(r) > 1 else "").strip()
            bj = (r[2] if len(r) > 2 else "").strip()
            tj = (r[3] if len(r) > 3 else "").strip()
            if not (mm or en):
                continue
            rows.append({"mm": mm, "en": en, "body_ja": bj, "title_ja": tj})
        _TERM_CACHE = rows
    except Exception:
        _TERM_CACHE = []
    return _TERM_CACHE

def _build_term_rules_prompt(title_src: str, body_src: str) -> str:
    ts, bs = (title_src or ""), (body_src or "")
    rules_t, rules_b = [], []
    for row in _load_term_glossary_gsheet():
        mm, en, bj, tj = row["mm"], row["en"], row["body_ja"], row["title_ja"]
        hit_t = (mm and mm in ts) or (en and en.lower() in ts.lower())
        hit_b = (mm and mm in bs) or (en and en.lower() in bs.lower())
        if hit_t and tj: rules_t.append(f"- {mm or en} ⇒ {tj}")
        if hit_b and bj: rules_b.append(f"- {mm or en} ⇒ {bj}")
    if not (rules_t or rules_b): return ""
    out = ["【用語固定ルール（この記事で該当した語のみ・厳守）】"]
    if rules_t:
        out.append("▼見出しに出た場合は次を必ず採用："); out.extend(rules_t)
    if rules_b:
        out.append("▼本文に出た場合は次を必ず採用："); out.extend(rules_b)
    return "\n".join(out) + "\n"

def _apply_term_glossary_to_output(text: str, *, src: str, prefer: str) -> str:
    import re
    t = (text or ""); s = (src or "")
    for row in _load_term_glossary_gsheet():
        mm, en = row["mm"], row["en"]
        ja = row["title_ja"] if prefer == "title_ja" else row["body_ja"]
        if not ja: continue
        if not ((mm and mm in s) or (en and en.lower() in s.lower())): continue
        if en:
            t = re.sub(rf"(?<![A-Za-z]){re.escape(en)}(?![A-Za-z])", ja, t)
        if mm:
            t = t.replace(mm, ja)
    return t

# =========================
# Region/State Glossary (Google Sheets)
# =========================
def _load_region_glossary_gsheet(sheet_key: str | None, worksheet_name: str = "regions"):
    """
    Google シート A:D を列位置で読む（列名は見ない）
      A: Myanmar / B: English / C: 本文訳 / D: 見出し訳
    戻り値: {"mm","en","ja","ja_body","ja_headline"} の配列（ja は後方互換のフォールバック）
    """
    if not sheet_key:
        return []
    try:
        import os, json
        import gspread
        from google.oauth2.service_account import Credentials
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            print("[region-glossary] skip: GOOGLE_CREDENTIALS_JSON not set")
            return []
        creds_info = json.loads(creds_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_key).worksheet(worksheet_name)
        values = ws.get("A:D") or []
        if len(values) <= 1:
            print("[region-glossary] no data rows in A:D")
            return []
        rows_data = values[1:]  # 1行目はヘッダ相当としてスキップ
        out = []
        for r in rows_data:
            mm = (r[0] if len(r) > 0 else "").strip()
            en = (r[1] if len(r) > 1 else "").strip()
            ja_body = (r[2] if len(r) > 2 else "").strip()
            ja_head = (r[3] if len(r) > 3 else "").strip()
            if not (mm or en or ja_body or ja_head):
                continue
            ja = ja_head or ja_body  # 既存呼び出しの後方互換
            out.append({"mm": mm, "en": en, "ja": ja, "ja_body": ja_body, "ja_headline": ja_head})
        print(f"[region-glossary] loaded {len(out)} entries from A:D of '{worksheet_name}'")
        return out
    except Exception as e:
        print(f"[region-glossary] failed to load gsheet: {e}")
        return []
    
# ===== Region Glossary helpers (タイトル=D列 / 本文=C列) =====
_REGIONS_CACHE: list[dict] | None = None

def _load_regions_cached() -> list[dict]:
    global _REGIONS_CACHE
    if _REGIONS_CACHE is None:
        _REGIONS_CACHE = _load_region_glossary_gsheet(
            os.getenv("MNA_SHEET_ID"),
            os.getenv("MNA_REGION_SHEET_NAME") or "regions",
        )
    return _REGIONS_CACHE or []

def _select_region_entries_for_text(text: str, entries: list[dict]) -> list[dict]:
    """本文/タイトル文字列に mm/en のどちらかが“出現した行だけ”を返す（NFCで近似一致）。"""
    import re, unicodedata
    if not (text and entries):
        return []
    t = unicodedata.normalize("NFC", text)
    mm_block = r"\u1000-\u109F"  # Myanmar
    picked, seen = [], set()
    for e in entries:
        mm = unicodedata.normalize("NFC", (e.get("mm") or ""))
        en = (e.get("en") or "")
        hit = False
        if mm:
            pat_mm = re.compile(rf"(?<![{mm_block}]){re.escape(mm)}(?![{mm_block}])")
            if pat_mm.search(t):
                hit = True
        if (not hit) and en:
            pat_en = re.compile(rf"\b{re.escape(en)}\b", re.IGNORECASE)
            if pat_en.search(t):
                hit = True
        if hit:
            key = (mm, en)
            if key not in seen:
                seen.add(key); picked.append(e)
    return picked

def _build_region_glossary_prompt_for(entries: list[dict], *, use_headline_ja: bool) -> str:
    """選ばれた entries から、翻訳プロンプトに埋め込む“用語固定”ルールを作る。"""
    if not entries:
        return ""
    lines = []
    for e in entries:
        mm = e.get("mm") or ""
        en = e.get("en") or ""
        ja = (e.get("ja_headline") or e.get("ja")) if use_headline_ja else (e.get("ja_body") or e.get("ja"))
        if not ja:
            continue
        if mm and en:
            lines.append(f"- 「{mm}」または「{en}」が出たら、必ず「{ja}」と訳す。")
        elif mm:
            lines.append(f"- 「{mm}」が出たら、必ず「{ja}」と訳す。")
        elif en:
            lines.append(f"- 「{en}」が出たら、必ず「{ja}」と訳す。")
    return ("【用語固定（必須）】\n" + "\n".join(lines) + "\n") if lines else ""

def build_prompt(item: dict, *, skip_filters: bool, body_max: int) -> str:
    header = "次の手順で記事を判定・処理してください。\n\n"
    pre = SKIP_NOTE_IRRAWADDY if skip_filters else STEP12_FILTERS + "\n\n"
    input_block = (
        "入力データ：\n"
        "###\n[記事タイトル]\n###\n"
        f"{item['title']}\n\n"
        "[記事本文]\n###\n"
        f"{item['body'][:body_max]}\n"
        "###\n"
    )
    # --- ここから追加：タイトル=D列 / 本文=C列 の“用語固定（必須）”を、出現語だけ注入 ---
    title_src = (item.get("title") or "")
    body_src  = (item.get("body") or "")[:body_max]
    rg_title = _build_region_glossary_prompt_for(
        _select_region_entries_for_text(title_src, _load_regions_cached()),
        use_headline_ja=True,   # タイトルは D 列（見出し訳）
    )
    rg_body = _build_region_glossary_prompt_for(
        _select_region_entries_for_text(body_src, _load_regions_cached()),
        use_headline_ja=False,  # 本文は C 列（本文訳）
    )
    # terms シートのヒット語だけを固定する既存ルール（見出し=title_ja / 本文=body_ja）
    term_rules = _build_term_rules_prompt(title_src, body_src)

    # ルール → 入力、の順でプロンプトを構成
    return header + pre + STEP3_TASK + rg_title + rg_body + term_rules + "\n" + input_block


# 超要約を先に抜く処理
def _normalize_heading_text(s: str) -> str:
    """見出し検出のための軽量正規化（括弧の異体字や不可視文字を吸収）"""
    trans = {
        ord("［"): "【",
        ord("〔"): "【",
        ord("〖"): "【",  # 左
        ord("］"): "】",
        ord("〕"): "】",
        ord("〗"): "】",  # 右
    }
    s = s.translate(trans)
    # 全角スペース→半角、NBSP/ZWSP/FEFF/ZWJ/ZWNJ を除去
    s = s.replace("\u3000", " ").replace("\xa0", " ")
    s = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", s)
    # 「要」「約」の間の変則スペースも吸収
    s = re.sub(r"(要)\s+(約)", r"\1\2", s)
    return unicodedata.normalize("NFC", s)


def _cut_ultra_block(lines):
    """
    「超要約」ブロック（見出し行〜次見出し直前まで）を切り出して削除。
    括弧の異体字（［］/〖〗/〔〕）や不可視文字、全角スペースに耐性あり。
    """
    # 正規化した影を作る（検出はこっち、削除は元linesで）
    norm = [_normalize_heading_text(ln) for ln in lines]

    HEAD_RE = re.compile(
        r"^【[\s\u3000\u200b\ufeff]*超[\s\u3000\u200b\ufeff]*要[\s\u3000\u200b\ufeff]*約[\s\u3000\u200b\ufeff]*】"
    )
    NEXT_HDR_RE = re.compile(r"^【.*?】")  # 他の見出し（要約/タイトル等）

    for i, ln_norm in enumerate(norm):
        if not HEAD_RE.match(ln_norm):
            continue

        # 見出し行の“同一行本文”（正規化後でOK）
        inline = HEAD_RE.sub("", ln_norm).strip()
        start = i + 1

        # 次の見出し直前まで
        end = start
        while end < len(norm) and not NEXT_HDR_RE.match(norm[end]):
            end += 1

        parts = []
        if inline:
            parts.append(inline)
        parts.extend(lines[start:end])  # 本文は元の行を使う

        new_lines = lines[:i] + lines[end:]
        return " ".join(parts).strip(), new_lines

    return "", lines


# 本処理関数
def process_translation_batches(batch_size=TRANSLATION_BATCH_SIZE, wait_seconds=60):
    # MEMO: TEST用、Geminiを呼ばず、URLリストだけ返す
    # summarized_results = []
    # for item in translation_queue:
    #     summarized_results.append({
    #         "source": item["source"],
    #         "url": item["url"],
    #         "title": item['title'],
    #         "summary": item['body'][:BODY_MAX_CHARS]
    #     })

    summarized_results = []
    for i in range(0, len(translation_queue), batch_size):
        batch = translation_queue[i : i + batch_size]
        print(f"⚙️ Processing batch {i // batch_size + 1}...")

        for item in batch:
            try:
                # デバッグ: 入力データを確認
                print("----- DEBUG: Prompt Input -----")
                print(f"TITLE: {item['title']}")
                print(f"BODY[:{BODY_MAX_CHARS}]: {item['body'][:BODY_MAX_CHARS]}")

                # プロンプト実行、Irrawaddy は Step1/2 をスキップ
                is_irrawaddy = (item.get("source") == "Irrawaddy") or (
                    "irrawaddy.com" in (item.get("url") or "")
                )
                prompt = build_prompt(
                    item, skip_filters=is_irrawaddy, body_max=BODY_MAX_CHARS
                )

                resp = call_gemini_with_retries(
                    client_summary, prompt, model="gemini-2.5-flash"
                )
                output_text = resp.text.strip()

                print("----- DEBUG: Model Output -----")
                print(output_text)

                # --- exit を広めに判定（バッククォートや句読点混入対策）---
                EXIT_ONLY_RE = re.compile(
                    r"^\s*(?:`{0,3})?\s*exit\s*(?:`{0,3})?\.?\s*$", re.IGNORECASE
                )
                if EXIT_ONLY_RE.match(output_text):
                    continue

                # --- 行整形（NFC + 空行除去）---
                lines = [
                    unicodedata.normalize("NFC", ln).strip()
                    for ln in output_text.splitlines()
                    if ln.strip()
                ]

                # --- 超要約を先に抜く（本文からも消す）---
                ultra_text, lines = _cut_ultra_block(lines)

                # --- タイトル抽出（要件に合わせて厳格化）---
                # ルール:
                #  A) 「【タイトル】訳題」= 同一行
                #  B) 1行目が「【タイトル】」のみ → 次の行を訳題として採用
                #  C) 上記以外のラベル揺れ（タイトル:, Title: など）は無視（救済しない）
                title_text = ""
                title_idx = next(
                    (
                        i
                        for i, ln in enumerate(lines)
                        if re.match(r"^【\s*タイトル\s*】", ln)
                    ),
                    None,
                )
                if title_idx is not None:
                    # マーカー行を解析
                    m = re.match(r"^【\s*タイトル\s*】\s*(.*)$", lines[title_idx])
                    inline = (m.group(1) or "").strip()
                    # マーカー行は消す
                    lines.pop(title_idx)

                    if inline:
                        # A) 同一行（【タイトル】◯◯）
                        # 先頭にコロンが紛れる事故だけ軽く除去（ラベル救済ではない）
                        title_text = inline.lstrip(":：").strip()
                    else:
                        # B) 次の行をタイトルとして採用（存在すれば）
                        if title_idx < len(lines):
                            title_text = lines[title_idx].strip()
                            lines.pop(title_idx)

                # 最終フォールバック（空を許さない）
                translated_title = (
                    title_text or item.get("title") or "（翻訳失敗）"
                ).strip()

                # --- 要約ラベルを先頭に強制 ---
                if not lines or not re.match(r"^【\s*要約\s*】\s*$", lines[0]):
                    lines.insert(0, "【要約】")

                lines_summary = build_summary_lines(output_text, lines)
                summary_text = "\n".join(lines_summary).strip()
                summary_html  = summary_text.replace("\n", "<br>")

                norm_url = _norm_id(item.get("url") or "")

                summarized_results.append(
                    {
                        "source": item["source"],
                        "url": norm_url,  # ★ 正規化済み
                        "title": translated_title,
                        "summary": summary_html,
                        "ultra": ultra_text,
                        "is_ayeyar": item.get("is_ayeyar", False),  # エーヤワディ系ヒット判定
                        "hit_full": item.get("hit_full", False),  # 全体キーワード判定
                        "hit_non_ayeyar": item.get("hit_non_ayeyar", False),  # 非エーヤワディ判定
                        "date": item.get("date"), 
                    }
                )

            except Exception as e:
                print(
                    "🛑 Error during translation:", e.__class__.__name__, "|", repr(e)
                )
                continue

            # バッチ内で微スリープしてバーストを抑える
            time.sleep(0.6)

        if i + batch_size < len(translation_queue):
            print(f"🕒 Waiting {wait_seconds} seconds before next batch...")
            time.sleep(wait_seconds)

    # 重複判定→片方残し（最終アウトプットの形式は変えない）
    deduped = dedupe_articles_with_llm(client_dedupe, summarized_results, debug=True)

    # 念のため：返却フォーマットを固定（余計なキーが混ざっていたら落とす）
    normalized = [
        {
            "source": x.get("source"),
            "url": x.get("url"),
            "title": x.get("title"),
            "summary": x.get("summary"),
            "is_ayeyar": x.get("is_ayeyar", False),  # エーヤワディ系ヒット判定
            "hit_full": x.get("hit_full", False),  # 全体キーワード判定
            "hit_non_ayeyar": x.get("hit_non_ayeyar", False),  # 非エーヤワディ判定
            "date": x.get("date"), 
        }
        for x in deduped
    ]
    return normalized


# ===== 全文翻訳（Business向けPDF用） =====
# 方針：
#  - 2件まとめ翻訳（JSON配列）
#  - 本文クレンジング＋6000文字上限（入力TPM削減）
#  - 事前レートチェック（RPM/TPM-in）で超過前に待機
#  - 要約バッチと同じ制御（バッチ内0.6s／バッチ間60s）
#  - 「この関数専用の処理」はなるべく関数内に閉じ込める（既存の共通処理は再利用）

# 既存の COMMON_TRANSLATION_RULES / call_gemini_with_retries / _safe_json_loads_maybe_extract /
# client_fulltext / _FREE_TIER_MON / TRANSLATION_BATCH_SIZE を使用
def translate_fulltexts_for_business(urls_in_order, url_to_source_title_body):
    """
    Business向けPDFで使う全文翻訳。順序は urls_in_order に従って返します。
    既存のラッパー（call_gemini_with_retries）とモニタ（_FREE_TIER_MON）をそのまま利用しつつ、
    この関数専用のクレンジング／事前チェック／2件まとめ翻訳 を関数内に閉じ込めています。
    戻り値: [{"url","title_ja","body_ja"}, ...]
    """
    # --- ローカル定数（環境変数は増やさず定数化） ---
    # Business 向け全文翻訳では本文を途中で切らずにほぼ全量翻訳したい。
    # Gemini Flash の安全コンテキスト上限を考慮し、上限を 100,000 文字に引き上げ。
    FULLTEXT_MAX_CHARS = 100_000
    BATCH = TRANSLATION_BATCH_SIZE  # 既定=2（= 2件まとめ）
    WAIT  = 60                      # 要約と同じ 1 分待機

    # --- ローカル import（この関数だけが使うもの） ---
    import re, json, time, unicodedata
    from datetime import datetime, timezone

    # --- English & Burmese leading labels / outlet-only lines ---
    EXCLUDE_LINE_PATTERNS = [
        # Captions / media labels (English)
        r"^(?:Photo|Image|Video|Graphic|Map|Caption)[:：\)]",
        r"^\((?:Photo|Image|Video|Graphic|Map)\)\s*$",
        # Credits / sources (English)
        r"^(?:Credit|Credits|Source|Sources|Via|Courtesy of)[:：]",
        # Editorial / translation notes (English)
        r"^(?:With reporting by|Reported by|Reporting by|Edited by|Compiled by|Translation|Translator|Translated by).*$",

        # Captions / labels (Burmese)
        r"^(?:ဓာတ်ပုံ|ရုပ်ပုံ)[:：]",            # photo / image
        # Sources / credits (Burmese)
        r"^(?:ရင်းမြစ်)[:：]",                 # source
        # Editorial / translation notes (Burmese)
        r"^(?:ဘာသာပြန်|ဘာသာပြန်သူ|တည်းဖြတ်)[:：]",

        # Outlet-only line (English outlet names)
        r"^(?:BBC Burmese|DVB|Myanmar Now|The Irrawaddy|Khit Thit Media|Mizzima|RFA Burmese|VOA Burmese|Eleven Media|Frontier Myanmar|Reuters|AP|Associated Press|AFP|SCMP)\s*$",
        # 3個以上のハッシュタグ行（タグ雲）
        r'^(?:[#＃][^\s#]+(?:\s+|$)){3,}$',
        # WPコメント欄の定型文・コメント誘導
        r'^(?:Save my name, email, and website.*)$',
        r'^(?:Leave a comment|Post a Comment|Your email address will not be published).*$',
        # 単独 "PDF" 行
        r'^(?:PDF)\s*$',
        # 記号だけの行
        r'^[\s\-/–—•·|\\\(\)\[\]{}“”"\'«»。、．…／]+$',
        # "##..." で始まるタグ群の行
        r'^(?:##.*)$',
    ]
    EXCLUDE_RE_LIST = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_LINE_PATTERNS]

    def compact_body(body: str) -> str:
        if not body:
            return ""
        norm_lines = []
        for ln in body.splitlines():
            ln = unicodedata.normalize("NFC", ln)
            ln = re.sub(r"\s+", " ", ln).strip()
            if not ln:
                continue
            if any(p.search(ln) for p in EXCLUDE_RE_LIST):
                continue
            norm_lines.append(ln)
        s = "\n\n".join(norm_lines).strip()
        return re.sub(r"\n{3,}", "\n\n", s)

    def trim_by_chars(s: str, max_chars: int) -> str:
        s = s or ""
        return s if len(s) <= max_chars else (s[:max_chars].rstrip() + "\n\n[…本文が長いためここまでを翻訳]")

    def rough_token_estimate(s: str) -> int:
        """英語 or ビルマ語（ミャンマー文字）前提の**ざっくり**見積り。
        - English 優勢: 4 chars ≒ 1 token
        - Myanmar 優勢: 2 chars ≒ 1 token
        - 混在/その他: 3 chars ≒ 1 token（安全側）
        ※ 目的は超過前待機の“目安”なので、少し多め（安全側）に見積もります。
        """
        s = s or ""
        total = len(s)
        if total == 0:
            return 0
        # Unicode ブロックでミャンマー文字を概算カウント
        my = len(re.findall(r"[\u1000-\u109F\uAA60-\uAA7F\uA9E0-\uA9FF]", s))
        la = len(re.findall(r"[A-Za-z]", s))  # 英字の概算
        my_ratio = my / total
        la_ratio = la / total
        if my_ratio >= 0.6:
            div = 2.0
        elif la_ratio >= 0.6:
            div = 4.0
        else:
            div = 3.0
        return max(0, int(total / div))

    def precheck_sleep(predicted_prompt_tokens: int, tag: str = "fulltext-batch"):
        """_FREE_TIER_MON の窓データで RPM/TPM(in) を事前チェックして、危なければ待機。"""
        try:
            mon = _FREE_TIER_MON  # 既存のグローバル（監視オブジェクト）
        except NameError:
            mon = None
        if not mon:
            return
        try:
            # RPM guard
            rpm_now = len(getattr(mon, "req_times", []))
            rpm_lim = getattr(mon, "rpm_limit", 10)
            if rpm_now + 1 > rpm_lim:
                oldest = mon.req_times[0] if mon.req_times else None
                if oldest:
                    wait = max(0.0, 60.0 - (datetime.utcnow().replace(tzinfo=timezone.utc) - oldest).total_seconds())
                    if wait > 0:
                        print(f"🕒 free-tier guard (RPM) sleeping {wait:.1f}s | tag={tag}")
                        time.sleep(wait)
            # TPM(in) guard（緩め）
            tpm_in_now = sum(tok for _, tok in getattr(mon, "tpm_in_points", []))
            tpm_lim = getattr(mon, "tpm_limit", 250000)
            if tpm_in_now + int(predicted_prompt_tokens) > tpm_lim:
                print("🕒 free-tier guard (TPM-in) sleeping 5s | tag=", tag)
                time.sleep(5)
        except Exception:
            pass
        
    def _safe_json_loads_extract(text: str):
        """
        LLMが前後に余計な文やコードフェンスを付けても
        JSON（配列/オブジェクト）を確実に取り出して読み込む。
        """
        import json, re

        t = (text or "").strip()

        # 1) まずは素で
        try:
            return json.loads(t)
        except Exception:
            pass

        # 2) ```json … ``` や ``` … ``` を除去
        t2 = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", t, flags=re.I | re.M).strip()
        if t2 != t:
            try:
                return json.loads(t2)
            except Exception:
                pass

        # 3) 先頭からバランス括弧で [ … ] を優先抽出（配列）
        def _scan_balanced(s: str, opener: str, closer: str):
            depth = 0
            in_str = False
            esc = False
            start = None
            for i, ch in enumerate(s):
                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                    continue
                if ch == '"':
                    in_str = True
                    continue
                if ch == opener:
                    if depth == 0:
                        start = i
                    depth += 1
                elif ch == closer and depth:
                    depth -= 1
                    if depth == 0 and start is not None:
                        yield s[start : i + 1]

        for block in list(_scan_balanced(t2, "[", "]")) + list(_scan_balanced(t2, "{", "}")):
            try:
                return json.loads(block)
            except Exception:
                continue

        # 4) 最後の保険：最初に見つかった {…} または [… ] を丸ごと
        m = re.search(r"(\[.*\]|\{.*\})", t2, flags=re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                pass

        raise ValueError("Could not extract JSON from LLM response")
    
    # === 日本語検知（CJK+かな/カナ）と未翻訳判定 ===
    def _contains_cjk(s: str) -> bool:
        return bool(s and re.search(r"[\u3040-\u30FF\u4E00-\u9FFF\u3400-\u4DBF]", s))

    def _needs_retry_untranslated(body: str) -> bool:
        """
        “日本語が全く含まれない”なら未翻訳と見なす（原文がビルマ語でも英語でも対象）。
        """
        t = (body or "").strip()
        return bool(t) and (not _contains_cjk(t))

    # === 全文翻訳プロンプトの共通ビルダー（既存prompt_partsの共通化） ===
    def _build_fulltext_prompt(input_array: list[dict]) -> str:
        # 文字列の隣接連結と + の混在での解析エラーを避けるため、配列で組み立て
        prompt_parts = (
            "次のニュース記事の【本文だけ】を**自然な日本語**に完全翻訳してください。\n"
            "・固有名詞は一般的な日本語表記に\n"
            "・ビルマ語/英語が混在していてもOK\n"
            "・タイトル（見出し）は訳さない／出力しない\n"
            "・本文は改行と段落を活かして読みやすく\n"
            "・半角の()括弧はすべて全角の（ ）に統一すること\n\n"
            f"{COMMON_TRANSLATION_RULES}"
            f"{_build_region_glossary_prompt_for(_select_region_entries_for_text(' '.join([x.get('body','') for x in input_array]), _load_regions_cached()), use_headline_ja=False)}"
            "【本文以外は必ず除外（この関数専用）】\n"
            "以下は原文に含まれていても翻訳・出力しないこと（含めたら減点）。\n"
            "- 写真キャプション／クレジット（先頭が「写真:」「ဓာတ်ပုံ」「Photo」「(写真」「(Photo」「（写真」などの行）\n"
            "- 媒体名だけの行（例: South China Morning Post / BBC Burmese / DVB / Myanmar Now などの媒体名のみ）\n"
            "- 出典や翻訳注記（例:「このニュースは…を翻訳したものです。」「Translated by …」「Source: …」「(China’s Ministry of Public Security)」等）\n"
            "- 記者名や配信ラベルだけの行（例: By … / Reuters / AP / SCMP などの単独行）\n"
            "- 発行地＋日付（Dateline）のみの行（例: "
            "'Yangon, Sept. 30' / 'Nay Pyi Taw, 30 September' / "
            "'ရန်ကုန်၊ စက်တင်ဘာ ၃၀' / 'နေပြည်တော်၊ ဖေဖော်ဝါရီ ၁၅' / "
            "'ヤンゴン、9月30日' / 'ネピドー、2024年2月15日' など）。\n"
            "  ※行頭や本文冒頭に置かれている場合も必ず除去すること。\n\n"
            "【入力クレンジング手順】\n"
            "1) 行単位で走査し、上記の非本文要素に一致する行をすべて削除する。\n"
            "2) 連続する空行は1つに圧縮し、本文段落のみ残す。\n"
            "3) 残った本文のみを翻訳対象とする（キャプション・媒体名・注記・Datelineは訳さない）。\n\n"
            "【出力仕様】出力はJSONのみ：\n"
            '[{"url":str, "body_ja":str}, …]\n'
            "input = ",
            json.dumps(input_array, ensure_ascii=False),
        )
        return "".join(prompt_parts)
    
    # === 未翻訳検知時の“単発リトライ”（同じプロンプトを単一要素配列で再利用） ===
    def _single_fulltext_retry(url: str, raw_body: str, max_chars: int = 6000) -> str:
        body_trim = trim_by_chars(raw_body or "", max_chars)
        prompt = _build_fulltext_prompt([{"url": url, "body": body_trim}])

        precheck_sleep(rough_token_estimate(prompt), tag="fulltext-retry")
        try:
            resp = call_gemini_with_retries(
                client_fulltext,
                prompt,
                model="gemini-2.5-flash",
                usage_tag="fulltext-retry",
            )
            text = getattr(resp, "text", None) or ""
            arr  = _safe_json_loads_extract(text)
            if isinstance(arr, dict):
                arr = [arr]
            if isinstance(arr, list):
                for x in arr:
                    if isinstance(x, dict) and x.get("url") == url:
                        return (x.get("body_ja") or "").strip()
        except Exception as e:
            print("[warn] fulltext single retry failed:", e)
        return ""

    # --- 1) 前処理（クレンジング＋6000字上限） ---
    prepared = []
    for u in urls_in_order:
        meta = (url_to_source_title_body.get(u) or {})
        title_src = (meta.get("title") or "").strip()
        body_src  = (meta.get("body")  or "").strip()
        # Prefer cached full body from first pass
        _cached = _get_cached_body(u)
        if _cached:
            body_src = _cached.strip()
        if not body_src:
            continue

        # PDFの全文要約では 100,000 字までは翻訳対象に含める。
        body_compact = trim_by_chars(compact_body(body_src), FULLTEXT_MAX_CHARS)
        prepared.append({"url": u, "title": title_src, "body": body_compact})

    # --- 2) まとめ翻訳（JSON配列で返答） ---
    results = []
    for i in range(0, len(prepared), BATCH):
        batch = prepared[i:i+BATCH]
        input_array = [{"url": b["url"], "body": b["body"]} for b in batch]

        # 文字列の隣接連結と + の混在での解析エラーを避けるため、配列で組み立て        
        prompt = _build_fulltext_prompt(input_array)

        precheck_sleep(rough_token_estimate(prompt), tag="fulltext-batch")

        try:
            resp = call_gemini_with_retries(
                client_fulltext,
                prompt,
                model="gemini-2.5-flash",
                usage_tag="fulltext",
            )
            text = getattr(resp, "text", None) or ""
            arr = _safe_json_loads_extract(text)
            if isinstance(arr, dict):
                arr = [arr]
            url_to_res = {}
            for x in (arr or []):
                if isinstance(x, dict) and x.get("url"):
                    url_to_res[str(x["url"])] = x
            for b in batch:
                x = url_to_res.get(b["url"]) or {}
                body_src = b["body"]
                body_ja = (x.get("body_ja") or body_src).strip()
                # 《最終置換》terms グロッサリを本文用（body_ja）で適用
                body_ja = _apply_term_glossary_to_output(body_ja, src=body_src, prefer="body_ja")
                results.append({"url": b["url"], "body_ja": body_ja})
        except Exception as e:
            print("🛑 fulltext batch failed:", e)
            for b in batch:
                # 失敗フォールバック時も用語表の最終置換を適用
                bj = _apply_term_glossary_to_output(b["body"], src=b["body"], prefer="body_ja")
                results.append({"url": b["url"], "body_ja": bj})

        # === このバッチで積んだ結果のうち「日本語が全く無い」ものだけ再翻訳 ===
        start_idx = len(results) - len(batch)
        end_idx   = len(results)
        for j in range(start_idx, end_idx):
            item = results[j]
            url  = item["url"]
            body = item.get("body_ja") or ""
            if _needs_retry_untranslated(body):
                print(f"[warn] fulltext seems untranslated (no Japanese detected): {url}")
                # 元の生本文（整形前）を取り出す
                raw_body = (url_to_source_title_body.get(url, {}) or {}).get("body") or body
                fixed = _single_fulltext_retry(url, raw_body, max_chars=FULLTEXT_MAX_CHARS)
                # 最終採用条件：日本語が含まれていればOK
                if fixed and _contains_cjk(fixed):
                    # 再取得結果にも terms を本文用で最終置換（地域名はプロンプトで強制済み）
                    results[j]["body_ja"] = _apply_term_glossary_to_output(
                        fixed, src=raw_body, prefer="body_ja"
                    )
                    print(f"[ok] repaired untranslated fulltext via single retry: {url}")
                # 呼びすぎ回避の小休止（要約と同じ運用）
                time.sleep(0.6)

        time.sleep(0.6)  # バッチ内マイクロスリープ（要約と合わせる）
        if i + BATCH < len(prepared):
            print(f"🕒 Waiting {WAIT} seconds before next fulltext batch …")
            time.sleep(WAIT)  # バッチ間 1 分待機（要約と合わせる）

    # --- 3) 入力順で並べ直し ---
    url_to_item = {x["url"]: x for x in results}
    return [url_to_item[u] for u in urls_in_order if u in url_to_item]


# 日本語日付を作る（0埋めなし）
def _jp_date(d: date) -> str:
    return f"{d.year}年{d.month}月{d.day}日"


def build_combined_pdf_for_business(translated_items, out_path=None):
    """
    Business向け：全文翻訳PDF（本文のみ、背景#f9f9f9）を1本に結合。
    translated_items: 各要素に "title_ja", "body_ja", "source" を含める想定。
    ※ "summary_plain" はあっても無視します（描画しません）。
    """
    # ===== 依存を関数内に閉じ込める =====
    import os, re, unicodedata
    from fpdf import FPDF

    # ===== レイアウト定数 =====
    LEFT_RIGHT_MARGIN = 16.0
    TOP_MARGIN        = 16.0
    BOTTOM_MARGIN     = 16.0

    TITLE_SIZE        = 15
    META_SIZE         = 11   # メディア＋日付の1行
    BODY_SIZE         = 11
    URL_SIZE          = 10   # URLの文字サイズ
    LINE_H_TITLE      = 6.5
    LINE_H_META       = 5.5
    LINE_H_BODY       = 5.5

    BODY_BG_RGB       = (249, 249, 249)  # 本文背景 #f9f9f9

    TITLE_BODY_GAP = 5.0  # タイトル→本文の余白（mm）
    TITLE_META_GAP_H = LINE_H_BODY # ← 追加：タイトル→メタ行の余白（本文1行ぶん）
    
    # ===== 正規化ユーティリティ（不自然改行の抑止） =====
    _ZW_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
    _SOFT_BREAK_RE = re.compile(r"(?<!\n)\n(?!\n)")
    
    # --- PDF中だけで使う「英字 + 半角スペース + CJK」境界のノーブレーク化 ---
    NBSP = "\u00A0"  # non-breaking space
    _CJK_RANGE = r"\u3040-\u30FF\u4E00-\u9FFF"  # ひらがな/カタカナ/漢字

    def _protect_latin_cjk_boundaries(s: str) -> str:
        """
        英数字の直後に半角スペースがあり、その次がCJK（和文）のときだけ、
        そのスペースを NBSP に置換して、PDF内の行頭分割を抑制する（PDF専用）。
        例: 'Ooredoo Myanmar社' → 'Ooredoo\\u00A0Myanmar社'
        """
        import re
        if not s:
            return s
        return re.sub(rf"([A-Za-z0-9]) ([{_CJK_RANGE}])", rf"\1{NBSP}\2", s)

    def _normalize_text_for_pdf(text: str) -> str:
        """
        段落の空行は残しつつ、段落内の“ソフト改行”を結合する。
        - 空行(=改行のみ)で段落を区切り
        - 段落内部は行末が句点類で終わらない限り、改行を削除して連結
        """
        import re
        if not text:
            return ""

        # 改行正規化
        t = text.replace("\r\n", "\n").replace("\r", "\n")

        # ゼロ幅文字の除去（既存の _ZW_RE を使う）
        t = _ZW_RE.sub("", t)

        # 単発改行だけを文脈に応じて結合（段落 \n\n は対象外）
        s = t  # 置換の基準スナップショット
        BULLETS = "・●○■□◆◇▶▷•*-–—"

        def _soft_join(m):
            i = m.start()                     # 改行の位置
            left  = s[i-1] if i > 0 else ""   # 左隣の1文字
            right = s[i+1] if i+1 < len(s) else ""  # 右隣の1文字

            # 箇条書きが次行先頭なら改行は保持
            if right and right in BULLETS:
                return "\n"

            # 英数→英数はスペースで結合（"(YA)\nmember" → "(YA) member"）
            if re.match(r"[A-Za-z0-9\)\]]", left) and re.match(r"[A-Za-z0-9\(\[]", right):
                return " "

            # それ以外（CJK含む）は無空白で結合
            return ""

        t = _SOFT_BREAK_RE.sub(_soft_join, s)
        
        # ===== 半角括弧を全角に統一し、括弧のまわりの“折り返し可能な空白”を除去（URLは除外） =====
        _URL_RE = re.compile(r"https?://\S+")

        def _convert_paren_outside_urls(text: str) -> str:
            def _conv(chunk: str) -> str:
                # 1) 半角→全角
                chunk = chunk.replace("(", "（").replace(")", "）")
                # 2) 「語 + 空白 + （」を「語 + （」へ（空白を削除）
                chunk = re.sub(r"(?<=\S) （", "（", chunk)
                # 3) 「） + 空白 + 語」を「） + 語」へ（空白を削除）
                chunk = re.sub(r"） (?=\S)", "）", chunk)
                return chunk

            out = []
            last = 0
            for m in _URL_RE.finditer(text):
                out.append(_conv(text[last:m.start()]))  # URLの手前は変換
                out.append(m.group(0))                   # URL本体はそのまま
                last = m.end()
            out.append(_conv(text[last:]))               # 末尾
            return "".join(out)

        t = _convert_paren_outside_urls(t)
        
        # 行頭禁則（開きカッコの直前のスペースを NBSP に変換して改行禁止）
        t = re.sub(r"(?<=\S) (?=[\(\（\[\【])", "\u00A0", t)  # \u00A0 = NBSP
        
        # --- ① 英数字同士の単発改行を NBSP でブリッジ（段落改行は温存）---
        # 例: "Finance\nUncovered" / "No\n Vote" / "Paka\nPha"
        t = re.sub(r"(?<=[A-Za-z0-9])\n\s*(?=[A-Za-z0-9])", "\u00A0", t)

        # --- ② スペースで挟まれたハイフンを改行禁止に ---
        # 例: "Zero - Column - 5 -)" → "Zero - Column - 5 -)"
        t = t.replace(" - ", "\u00A0-\u00A0")

        # --- ③ 数字の直後に来る '‐-)' を改行不可ハイフンに置換 ---
        # 例: "5-)" の '-' を U+2011 (NON-BREAKING HYPHEN) に
        t = re.sub(r"(?<=\d)-(?=\))", "\u2011", t)
        
        # 英語の大文字始まり 2〜3語は語間を NBSP にして分断を防止（URLは対象外）
        _PROPER_RE = re.compile(
            r'\b([A-Z][A-Za-z0-9&\.\-]{1,20})\s+([A-Z][A-Za-z0-9&\.\-]{1,20})(?:\s+([A-Z][A-Za-z0-9&\.\-]{1,20}))?\b'
        )
        _place = {}
        def _stash(m):
            k = f"__URL{len(_place)}__"
            _place[k] = m.group(0)
            return k
        _work = _URL_RE.sub(_stash, t)  # URLは退避して対象外に
        _work = _PROPER_RE.sub(lambda m: "\u00A0".join([w for w in m.group(0).split(' ') if w]), _work)
        for k, v in _place.items():
            _work = _work.replace(k, v)
        t = _work

        # 段落で分割（連続する空行を1つの区切りとみなす）
        paras = re.split(r"\n\s*\n", t.strip(), flags=re.MULTILINE)
        cleaned_paras = []

        # 文末とみなす文字（これで終わっていれば改行を維持）、カッコ/角カッコは“文末記号”ではないため除外
        SENT_END = r"[。．\.！？!?…」』]"

        for p in paras:
            # 行単位に分割（空行はこの段階では存在しない前提）
            lines = [ln.strip() for ln in p.split("\n") if ln.strip() != ""]
            if not lines:
                continue

            buf = lines[0]
            for ln in lines[1:]:
                # 直前が文末記号で終わるなら改行維持（=新しい文として連結）
                if re.search(SENT_END + r"$", buf):
                    buf = buf + "\n" + ln
                else:
                    # それ以外は“ソフト改行”とみなし、改行を削除して結合
                    # （日本語なのでスペースは挟まない）
                    buf = buf + ln
            cleaned_paras.append(buf)

        # 段落間は空行1つ（= \n\n）で接続
        return "\n\n".join(cleaned_paras)

    # ===== 正規化ユーティリティ（不自然改行の抑止） =====
    _ZW_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
    _SOFT_BREAK_RE = re.compile(r"(?<!\n)\n(?!\n)")

    def _normalize_text_for_pdf(text: str) -> str:
        """
        段落の空行は残しつつ、段落内の“ソフト改行”を結合する。
        - 空行(=改行のみ)で段落を区切り
        - 段落内部は行末が句点類で終わらない限り、改行を削除して連結
        """
        import re
        if not text:
            return ""

        # 改行正規化
        t = text.replace("\r\n", "\n").replace("\r", "\n")
        
        # ゼロ幅文字の除去（既存の _ZW_RE を使う）
        t = _ZW_RE.sub("", t)
        
        # 単発改行だけを文脈に応じて結合（段落 \n\n は対象外）
        s = t  # 置換の基準スナップショット
        BULLETS = "・●○■□◆◇▶▷•*-–—"

        def _soft_join(m):
            i = m.start()                     # 改行の位置
            left  = s[i-1] if i > 0 else ""   # 左隣の1文字
            right = s[i+1] if i+1 < len(s) else ""  # 右隣の1文字

            # 箇条書きが次行先頭なら改行は保持
            if right and right in BULLETS:
                return "\n"

            # 英数→英数はスペースで結合（"(YA)\nmember" → "(YA) member"）
            if re.match(r"[A-Za-z0-9\)\]]", left) and re.match(r"[A-Za-z0-9\(\[]", right):
                return " "

            # それ以外（CJK含む）は無空白で結合
            return ""

        t = _SOFT_BREAK_RE.sub(_soft_join, s)

        # 段落で分割（連続する空行を1つの区切りとみなす）
        paras = re.split(r"\n\s*\n", t.strip(), flags=re.MULTILINE)
        cleaned_paras = []

        # 文末とみなす文字（これで終わっていれば改行を維持）、カッコ/角カッコは“文末記号”ではないため除外
        SENT_END = r"[。．\.！？!?…」』]"

        for p in paras:
            # 行単位に分割（空行はこの段階では存在しない前提）
            lines = [ln.strip() for ln in p.split("\n") if ln.strip() != ""]
            if not lines:
                continue

            buf = lines[0]
            for ln in lines[1:]:
                # 直前が文末記号で終わるなら改行維持（=新しい文として連結）
                if re.search(SENT_END + r"$", buf):
                    buf = buf + "\n" + ln
                else:
                    # それ以外は“ソフト改行”とみなし、改行を削除して結合
                    # （日本語なのでスペースは挟まない）
                    buf = buf + ln
            cleaned_paras.append(buf)

        # 段落間は空行1つ（= \n\n）で接続
        return "\n\n".join(cleaned_paras)

    # ===== PDFユーティリティ =====
    def _epw(pdf):  # effective page width
        return pdf.w - pdf.l_margin - pdf.r_margin

    def _register_jp_fonts(pdf):
        font_regular = os.environ.get("PDF_FONT_PATH")
        font_bold    = os.environ.get("PDF_FONT_BOLD_PATH") or font_regular
        if not font_regular:
            raise RuntimeError("PDF_FONT_PATH が未設定です（JPフォント .ttf/.otf のパスを指定）")
        try: pdf.add_font("JP",  "", font_regular, uni=True)
        except Exception: pass
        try: pdf.add_font("JP-B","", font_bold,    uni=True)
        except Exception: pass

    def _format_meta_date(date_str: str) -> str:
        import re
        s = (date_str or "").strip()
        # "YYYY-MM-DDThh:mm:ss" → "YYYY-MM-DD" に切り落とし
        if "T" in s:
            s = s.split("T", 1)[0]
        m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if not m:
            return s  # 既に和式/別形式ならそのまま
        y, mo, d = map(int, m.groups())
        return f"{y}年{mo}月{d}日"

    def _write_title_with_source(pdf, title, media, date_str=""):
        # 実効幅
        epw = getattr(pdf, "epw", pdf.w - pdf.l_margin - pdf.r_margin)

        # 1) タイトル（左揃え・太字）
        pdf.set_x(pdf.l_margin)
        title = (title or "").strip()
        if title:
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("JP-B", size=TITLE_SIZE)
            pdf.multi_cell(w=epw, h=LINE_H_TITLE, txt=title, align="L", border=0)

        # 2) タイトル→メタ行の“本文1行ぶん”の空白
        pdf.ln(TITLE_META_GAP_H)

        # 3) メタ行（メディア＋日付）… 例）"Khit Thit Media　2025年9月30日"
        media = (media or "").strip()
        date_txt = _format_meta_date(date_str)
        meta_line = f"{media}　{date_txt}".strip("　 ")

        if meta_line:
            pdf.set_x(pdf.l_margin)
            pdf.set_font("JP", size=META_SIZE)
            pdf.multi_cell(w=epw, h=LINE_H_META, txt=meta_line, align="L", border=0)

        # 4) メタ行→本文の余白（既存の設定を尊重）
        pdf.ln(TITLE_BODY_GAP)

    def _write_body_with_bg(pdf, body):
        """本文を #f9f9f9 背景で出力（複数ページにまたがってOK）。"""
        
        pdf.set_x(pdf.l_margin)  # 本文前にも左マージンに揃えて開始
        
        txt = _normalize_text_for_pdf(body)
        if not txt:
            return
        
        # ★ 追加：英字→和文の境界だけノーブレーク化（PDF出力専用）
        txt = _protect_latin_cjk_boundaries(txt)
        
        pdf.set_font("JP", size=BODY_SIZE)
        pdf.set_fill_color(*BODY_BG_RGB)
        # 1行ごとに塗られるため、段落全体として薄グレーになります
        pdf.multi_cell(w=_epw(pdf), h=LINE_H_BODY, txt=txt, align="L", border=0, fill=True)
        pdf.ln(2.0)
        
    def _write_url_footer(pdf, url):
        url = (url or "").strip()
        if not url:
            return
        pdf.ln(1.0)
        pdf.set_text_color(0, 0, 200)
        pdf.set_font("JP", size=URL_SIZE)
        # クリック可能なリンクで書き出す
        pdf.write(h=LINE_H_BODY, txt=url, link=url)
        pdf.set_text_color(0, 0, 0)

    # ===== PDF 本体 =====
    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=BOTTOM_MARGIN)
    pdf.set_margins(LEFT_RIGHT_MARGIN, TOP_MARGIN, LEFT_RIGHT_MARGIN)
    _register_jp_fonts(pdf)

    # 各記事は必ず「新しいページの先頭から」開始。※要約は描画しない
    for item in translated_items or []:
        pdf.add_page()
        pdf.set_xy(pdf.l_margin, pdf.t_margin)

        _write_title_with_source(
            pdf,
            title=item.get("title_ja", "") or "",
            media=item.get("source", "") or "",
            date_str=item.get("date", "") or "",    # ← 日付を渡す
        )
        _write_body_with_bg(pdf, item.get("body_ja", "") or "")
        _write_url_footer(pdf, item.get("url", "") or "")

    # ===== 出力（bytearray対策込み） =====
    out = pdf.output(dest="S")
    if isinstance(out, (bytes, bytearray)):
        pdf_bytes = bytes(out)
    else:
        pdf_bytes = out.encode("latin1")

    if out_path:
        with open(out_path, "wb") as f:
            f.write(pdf_bytes)

    return pdf_bytes


def send_email_digest(
    summaries,
    *,
    recipients_env=None,
    include_read_link: bool = True,
    trial_footer_url: Optional[str] = None,
    attachment_bytes: Optional[bytes] = None,
    attachment_name: Optional[str] = None,
    delivery_date_mmt: Optional[date] = None,
    attach_pdf: bool = True,
    preserve_newlines: bool = False,
):
    def _build_gmail_service():
        cid = os.getenv("GMAIL_CLIENT_ID")
        csec = os.getenv("GMAIL_CLIENT_SECRET")
        rtok = os.getenv("GMAIL_REFRESH_TOKEN")
        if not (cid and csec and rtok):
            raise RuntimeError(
                "Gmail API credentials (CLIENT_ID/SECRET/REFRESH_TOKEN) are missing."
            )
        creds = Credentials(
            token=None,
            refresh_token=rtok,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cid,
            client_secret=csec,
        )
        return build("gmail", "v1", credentials=creds, cache_discovery=False)
    
    # ===== メール件名 =====
    SUBJECT_BRAND = "MNA"

    def _date_str_yyyymmdd(actual_delivery_dt: datetime | None = None, tz_name: str = "Asia/Yangon") -> str:
        """実際の配信日のローカル日付を yyyy/m/d で返す（先頭ゼロなし）"""
        tz = zoneinfo.ZoneInfo(tz_name)
        dt = (actual_delivery_dt or datetime.now(tz)).astimezone(tz)
        return f"{dt.year}/{dt.month}/{dt.day}"

    def _sanitize_one_line(s: str) -> str:
        """改行・過剰空白を除去し、メール件名で崩れないように整形"""
        if not s:
            return ""
        s = " ".join(str(s).split())          # 改行や連続空白を単一空白に
        s = s.replace("】】", "】")            # 二重クローズ誤爆の保険
        return s.strip()

    def build_mail_subject(
        headlines: list[str] | None,
        is_ayeyarwady_only: bool,
        actual_delivery_dt: datetime | None = None,
        brand: str = SUBJECT_BRAND,
        tz_name: str = "Asia/Yangon",
    ) -> str:
        """
        要件:
        1) エーヤワディのみのメール
            → 【MNA yyyy/m/d】エーヤワディ関連記事
        2) 上記以外
            → 【MNA yyyy/m/d】ヘッドライン上位2本を「見出し / 見出し 他」
            （1本しか無ければ「見出し 他」、0本なら「ヘッドライン」）
        """
        ds = _date_str_yyyymmdd(actual_delivery_dt, tz_name)
        prefix = f"【{brand} {ds}】"

        if is_ayeyarwady_only:
            return f"{prefix}エーヤワディ関連記事"

        titles = [ _sanitize_one_line(t) for t in (headlines or []) if _sanitize_one_line(t) ]
        if len(titles) >= 2:
            return f"{prefix}{titles[0]} / {titles[1]}　他"
        elif len(titles) == 1:
            return f"{prefix}{titles[0]}　他"
        else:
            return f"{prefix}ヘッドライン"

    sender_email = os.getenv("EMAIL_SENDER")
    env_name = recipients_env
    # --- 1) カンマ区切りの宛先リストを読み込み ---
    raw_recipients = os.getenv(env_name, "")
    recipient_emails = []
    for addr in raw_recipients.split(","):
        addr = addr.strip()
        # --- 2) 空文字・不正形式を除外 ---
        if not addr or "@" not in addr:
            continue
        recipient_emails.append(addr)
    # === ガード: 宛先が有効でなければスキップ ===
    if not recipient_emails:
        print(f"⚠️ No valid recipient emails found in {env_name}. Skipping email send.")
        return

    digest_date = get_today_date_mmt()
    date_str = digest_date.strftime("%Y年%-m月%-d日") + "分"

    headlines = [f"✓ {item['title']}" for item in summaries]
    headline_html = (
        "<div style='margin-bottom:20px'>"
        f"<strong>本日のヘッドライン</strong><br>"
        + "<br>".join(headlines)
        + "</div><hr>"
    )

    html_content = "<html><body style='font-family: Arial, sans-serif; background-color: #ffffff; color: #333333;'>"
    html_content += headline_html

    for item in summaries:
        media = item["source"]
        title_jp = item["title"]; url = item["url"]
        summary_raw = item["summary"]
        summary_html = _nl2br(summary_raw) if preserve_newlines else summary_raw
        heading_html = (
            "<h2 style='margin-bottom:5px'>"
            f"{title_jp}　"
            "<span style='font-size:0.83rem;font-weight:600'>"
            f"{media} "
            "</span>"
            "</h2>"
        )
        html_content += (
            "<div style='margin-bottom:20px'>"
            f"{heading_html}"
            "<div style='background-color:#f9f9f9;padding:10px;border-radius:8px'>"
            f"{summary_html}"
            "</div>"
        )
        # J列（URL）が空なら「本文を読む」を非表示
        if include_read_link and (url or "").strip():
            html_content += (
                f"<p><a href='{url}' style='color:#1a0dab' target='_blank'>本文を読む</a></p>"
            )
        html_content += "</div><hr style='border-top: 1px solid #cccccc;'>"

    if trial_footer_url:
        # ===== TRIAL footer (HTML/CSS, no images) =====
        BASE_FONT = "Arial, Helvetica, 'Hiragino Kaku Gothic ProN', 'Noto Sans JP', Meiryo, sans-serif"
        TEXT = "#111111"
        MUTED = "#666666"
        BORDER = "#E5E7EB"
        BG_ALL = "#FFFFFF"
        CARD_BG = "#f2f2f2"
        ACCENT = "#0057E1"

        html_content += (
            # 外側（白背景）
            f"<div style='background:{BG_ALL};padding:24px 0;margin:24px -8px 0 -8px;'>"
            # 内側カード：改行しないため max-width を拡張（例：700px）
            f"<div style='max-width:700px;margin:0 auto;background:{CARD_BG};border-radius:12px;"
            f"padding:20px 24px;box-sizing:border-box;'>"

                # 見出し（font-size は span に移して !important）
                f"<div style='text-align:center;margin:0 0 20px 0'>"
                f"<p style='margin:0 0 12px 0;font-family:{BASE_FONT};color:{TEXT};'>"
                f"  <span style='font-size:22px !important;font-weight:700 !important;-webkit-font-smoothing:antialiased;'>ご優待のご案内</span>"
                f"</p>"
                # 本文：改行させないなら white-space:nowrap; を付与（必要なければ次行の nowrap を削除）
                f"<p style='margin:0;font-size:15px;letter-spacing:0.2px;line-height:1.7;"
                f"font-family:{BASE_FONT};color:{TEXT};white-space:nowrap;'>"
                "トライアル期間中に有料プランへお申込みいただいた方、"
                "<span style='font-weight:700'>全員にAmazonギフト券を進呈致します。</span></p>"
                f"</div>"

                # 比較表（元のまま）
                f"<div style='text-align:center;margin:24px 0;'>"
                f"<table role='presentation' cellpadding='0' cellspacing='0' border='0' align='center'>"
                f"<tr>"
                    # タイトル行（上下線）
                    f"<td colspan='3' style='padding:10px;border-top:2px solid #9CA3AF;"
                    f"border-bottom:2px solid #9CA3AF;text-align:center;background:{CARD_BG};font-weight:400'>"
                    "特別ご優待（Amazonギフト券）</td>"
                f"</tr>"
                f"<tr>"
                    f"<th style='padding:10px;text-align:left;font-weight:400;background:{CARD_BG}'></th>"
                    f"<th style='padding:10px;text-align:center;font-weight:400;background:{CARD_BG}'>Liteプラン</th>"
                    f"<th style='padding:10px;text-align:center;font-weight:400;background:{CARD_BG}'>Businessプラン</th>"
                f"</tr>"
                f"<tr>"
                    f"<td style='padding:12px 10px;white-space:nowrap;background:{CARD_BG};vertical-align:top'>"
                    "トライアル開始後<br>15日以内のお申込</td>"
                    f"<td style='padding:12px 10px;text-align:center;background:{CARD_BG};vertical-align:top'>3,000円分</td>"
                    f"<td style='padding:12px 10px;text-align:center;background:{CARD_BG};vertical-align:top'>6,000円分</td>"
                f"</tr>"
                f"<tr>"
                    # 最終行の下線
                    f"<td style='padding:12px 10px;white-space:nowrap;background:{CARD_BG};vertical-align:top;border-bottom:2px solid #9CA3AF'>16〜30日目のお申込</td>"
                    f"<td style='padding:12px 10px;text-align:center;background:{CARD_BG};vertical-align:top;border-bottom:2px solid #9CA3AF'>2,000円分</td>"
                    f"<td style='padding:12px 10px;text-align:center;background:{CARD_BG};vertical-align:top;border-bottom:2px solid #9CA3AF'>5,000円分</td>"
                f"</tr>"
                f"</table></div>"

                # === ボタン（コンパクト版・冗長指定で堅牢化） ===
                f"<div style='text-align:center;margin:24px 0 12px 0;'>"
                f"<table role='presentation' border='0' cellspacing='0' cellpadding='0' align='center' style='margin:0 auto;'>"
                f"  <tr>"
                f"    <td align='center' bgcolor='{ACCENT}' "
                f"        style='border-radius:8px;background:{ACCENT};"
                f"               padding:14px 20px;min-width:240px;mso-padding-alt:14px 20px;"
                f"               font-size:18px !important;font-weight:800 !important;color:#FFFFFF !important;'>"

                # === Outlook用 ===
                f"      <!--[if mso]>"
                f"      <v:roundrect xmlns:v='urn:schemas-microsoft-com:vml' href='{trial_footer_url}' "
                f"        style='height:44px;v-text-anchor:middle;width:260px;' arcsize='12%' stroke='f' fillcolor='{ACCENT}'>"
                f"        <w:anchorlock/>"
                f"        <center style='color:#FFFFFF;font-family:{BASE_FONT};font-size:18px;font-weight:800;'>有料プランを始める</center>"
                f"      </v:roundrect>"
                f"      <![endif]-->"

                # === 非Outlook（Gmail等） ===
                f"      <!--[if !mso]><!-- -->"
                f"      <a href='{trial_footer_url}' target='_blank' "
                f"         style='display:block !important;text-decoration:none !important;"
                f"                color:#FFFFFF !important;font-size:18px !important;font-weight:800 !important;'>"
                f"        <span style='display:block !important;color:#FFFFFF !important;"
                f"                     font-family:{BASE_FONT} !important;"
                f"                     font-size:18px !important;font-weight:800 !important;"
                f"                     line-height:1.4em !important;text-decoration:none !important;'>"
                f"          有料プランを始める"
                f"        </span>"
                f"      </a>"
                f"      <!--<![endif]-->"
                f"    </td>"
                f"  </tr>"
                f"</table>"
                f"</div>"

                # 備考（変更なし）
                f"<div align='center' style='text-align:center;margin-top:6px;'>"
                f"  <p style='margin:0;max-width:700px;font-family:{BASE_FONT};font-size:14px;line-height:1.8;color:{MUTED};'>"
                "    ※ 無料トライアルと<span style='text-decoration:underline'>同一メールアドレス</span>でのお申込みに限ります。<br>"
                "    ※ トライアル期間終了後のお申込みは対象外となります。"
                f"  </p>"
                f"</div>"

            f"</div>"
            f"</div>"
        )
    html_content += "</body></html>"
    html_content = clean_html_content(html_content)

    # ===== 件名（要件準拠） =====
    # 1) エーヤワディのみ → 【MNA yyyy/m/d】エーヤワディ関連記事
    # 2) それ以外 → 【MNA yyyy/m/d】見出し1 / 見出し2 他（見出し数に応じて調整）
    titles_for_subject = [it.get("title", "") for it in summaries if (it.get("title") or "").strip()]
    is_ayeyar_only_batch = bool(summaries) and all(bool(s.get("is_ayeyar")) for s in summaries)
    # 件名の日付は __main__ 側で決定した date_mmt を使用
    dt_for_subject = None
    if delivery_date_mmt is not None:
        try:
            tz = zoneinfo.ZoneInfo("Asia/Yangon")
            dt_for_subject = datetime(delivery_date_mmt.year, delivery_date_mmt.month, delivery_date_mmt.day, tzinfo=tz)
        except Exception:
            dt_for_subject = None
    subject = build_mail_subject(titles_for_subject, is_ayeyar_only_batch, actual_delivery_dt=dt_for_subject)
    from_display_name = "Myanmar News Alert"

    subject = re.sub(r"[\r\n]+", " ", subject).strip()
    
    # ---- 個別送信（プライバシー重視）＋簡易バックオフ ----
    from googleapiclient.errors import HttpError
    import json, random, time, base64

    def _parse_reason_from_http_error(err: HttpError) -> str:
        try:
            data = json.loads(err.content.decode("utf-8"))
            if "error" in data:
                if "errors" in data["error"] and data["error"]["errors"]:
                    return data["error"]["errors"][0].get("reason", "")
                if "status" in data["error"]:
                    return data["error"]["status"]
        except Exception:
            pass
        return ""

    def _send_gmail_with_retry(service, raw_b64: str, to_addr: str, *, retry_max: int = 5, backoff_base: float = 1.0, backoff_max: float = 60.0):
        attempt = 0
        while True:
            try:
                body = {"raw": raw_b64}
                return service.users().messages().send(userId="me", body=body).execute()
            except HttpError as e:
                status = getattr(e, "status_code", None) or getattr(e.resp, "status", None)
                reason = _parse_reason_from_http_error(e)
                if reason in {"dailyLimitExceeded", "quotaExceeded"}:
                    print(f"❌ Gmail API: 日次/総量上限に達しました (status={status}, reason={reason}). 中断します。")
                    raise
                if status in (403, 429) or reason in {"rateLimitExceeded", "userRateLimitExceeded"}:
                    if attempt >= retry_max:
                        print(f"❌ Gmail API: レート制限により送信断念 (to={to_addr}).")
                        return None
                    delay = min(backoff_max, backoff_base * (2 ** attempt))
                    delay *= (1.0 + random.uniform(0.0, 0.3))
                    print(f"⏳ レート制限 (status={status}, reason={reason}) → {delay:.1f}s 待機して再試行 ({attempt+1}/{retry_max})")
                    time.sleep(delay)
                    attempt += 1
                    continue
                print(f"⚠️ Gmail API: 恒久エラーのためスキップ (to={to_addr}, status={status}, reason={reason})")
                return None
            except Exception as e:
                print(f"⚠️ 予期せぬエラーのためスキップ (to={to_addr}): {e}")
                return None

    service = _build_gmail_service()

    sent_count = 0
    for rcpt in recipient_emails:
        # 各受信者ごとに新しいメッセージを作成
        msg = EmailMessage(policy=SMTP)
        msg["Subject"] = subject
        msg["From"] = formataddr((str(Header(from_display_name, "utf-8")), sender_email))
        msg["To"] = rcpt
        msg.set_content("HTMLメールを開ける環境でご確認ください。", charset="utf-8")
        msg.add_alternative(html_content, subtype="html", charset="utf-8")

        # PDF 添付はフラグで制御（デフォルトは従来通り True）
        if attach_pdf and attachment_bytes and attachment_name:
            msg.add_attachment(
                attachment_bytes,
                maintype="application",
                subtype="pdf",
                filename=attachment_name,
            )

        raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        sent = _send_gmail_with_retry(service, raw_b64, rcpt)
        if sent and isinstance(sent, dict):
            sent_count += 1
            print("✅ Gmail API 送信完了 messageId:", sent.get("id"), "to:", rcpt)

        # 軽いスロットリング（バースト抑制）
        time.sleep(0.5)

    print(f"📬 個別送信 完了: {sent_count}/{len(recipient_emails)} 件")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily Myanmar News Alert runner (mono / two-phase)")
    parser.add_argument("--phase", choices=["mono", "collect", "send"], default="mono",
                        help="mono=従来どおり（収集+送信）。collect=収集のみ（CSV_EMAIL_RECIPIENTSだけ即時送信可）。send=束ねデータから送信。")
    parser.add_argument("--bundle-dir", default="bundle", help="二段構成での保存/読込ディレクトリ")
    parser.add_argument("--recipients-env", default=None, help="--phase send 時に送る宛先の環境変数名（例: LITE_EMAIL_RECIPIENTS）")
    parser.add_argument(
        "--sheet-mail-preserve-newlines",
        action="store_true",
        help="（Sheet Pipeline — Send専用）本文要約の改行をメールHTMLで保持"
    )
    args, unknown = parser.parse_known_args()

    # 今日の日付をミャンマー時間で取得
    date_mmt = get_today_date_mmt()
    
    # ------------------------------
    # SEND 専用ショートサーキット
    # 収集済み bundle を読み込み、対象宛先にメール送信
    # ------------------------------
    if args.phase == "send":
        # bundle 読み込み
        try:
            meta, summaries_loaded, pdf_loaded, attachment_name_loaded = _load_bundle(args.bundle_dir)
        except FileNotFoundError as e:
            raise SystemExit(f"[send] bundle not found under: {os.path.abspath(args.bundle_dir)} : {e}")

        # エーヤワディ専用サマリ（オプション）
        try:
            import json, pathlib
            b = pathlib.Path(args.bundle_dir)
            summaries_ayeyar_loaded = json.loads((b / "summaries_ayeyar.json").read_text(encoding="utf-8"))
        except Exception:
            summaries_ayeyar_loaded = []

        # 実際の配信日（MMT）で統一
        delivery_date_mmt = get_today_date_mmt()

        def _send_one(env_name: str):
            """単一宛先へ送信（INTERNAL はエーヤワディ専用サマリ、それ以外は bundle の summaries を使用）"""
            if not (os.getenv(env_name, "").strip()):
                return  # 宛先未設定はスキップ

            is_trial = (env_name == "TRIAL_EMAIL_RECIPIENTS")
            is_lite  = (env_name == "LITE_EMAIL_RECIPIENTS")
            is_internal = (env_name == "INTERNAL_EMAIL_RECIPIENTS")

            # 添付の扱い：Liteはなし／他は bundle のPDF（従来仕様踏襲）
            attachment_bytes = None if is_lite else (pdf_loaded if pdf_loaded else None)
            # PDF名は「配信日MMT」に差し替え（添付がある場合のみ）
            if attachment_bytes:
                try:
                    jp = _jp_date(delivery_date_mmt)
                    attachment_name = f"ミャンマーニュース全文訳【{jp}】.pdf"
                except Exception:
                    attachment_name = attachment_name_loaded if attachment_name_loaded else None
            else:
                attachment_name = None

            # 本文データの選択
            summaries_for_mail = summaries_ayeyar_loaded if is_internal else summaries_loaded
            if not summaries_for_mail:
                print(f"[send] {env_name}: 対象記事なしのためスキップ")
                return

            send_email_digest(
                summaries_for_mail,
                recipients_env=env_name,
                include_read_link=(not is_lite),
                trial_footer_url=(os.getenv("PAID_PLAN_URL", "").strip() or None) if is_trial else None,
                attachment_bytes=attachment_bytes,
                attachment_name=attachment_name,
                delivery_date_mmt=delivery_date_mmt,
                attach_pdf=(not is_internal),   # ← INTERNAL（エーヤワディ専用）は PDF 無し
                preserve_newlines=args.sheet_mail_preserve_newlines,
            )

        if args.recipients_env:
            # 従来どおり単体送信（INTERNAL もサポート）
            _send_one(args.recipients_env)
        else:
            # まとめ送信（存在する宛先のみ）
            for env_name in (
                "LITE_EMAIL_RECIPIENTS",
                "BUSINESS_EMAIL_RECIPIENTS",
                "TRIAL_EMAIL_RECIPIENTS",
                "INTERNAL_EMAIL_RECIPIENTS",  # ← エーヤワディ専用
            ):
                _send_one(env_name)

        raise SystemExit(0)
    
    seen_urls = set()

    print("=== Mizzima (Burmese) ===")
    articles_mizzima = get_mizzima_articles_from_category(
        date_mmt,
        "https://bur.mizzima.com",
        "Mizzima (Burmese)",
        "/category/%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8/%e1%80%99%e1%80%bc%e1%80%94%e1%80%ba%e1%80%99%e1%80%ac%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8",
        max_pages=3,
    )
    process_and_enqueue_articles(
        articles_mizzima, 
        "Mizzima (Burmese)", 
        seen_urls, 
        trust_existing_body=True
    )

    print("=== BBC Burmese ===")
    articles_bbc = get_bbc_burmese_articles_for(date_mmt)
    process_and_enqueue_articles(
        articles_bbc, 
        "BBC Burmese", 
        seen_urls, 
        trust_existing_body=True
    )

    print("=== Irrawaddy ===")
    articles_irrawaddy = get_irrawaddy_articles_for(date_mmt)
    # MEMO: ログ用、デバックでログ確認
    # print("RESULTS:", json.dumps(articles_irrawaddy, ensure_ascii=False, indent=2))
    process_and_enqueue_articles(
        articles_irrawaddy,
        "Irrawaddy",
        seen_urls,
        bypass_keyword=True,  # ← Irrawaddyはキーワードで落とさない
        trust_existing_body=True,  # ← さっき入れた body をそのまま使う（再フェッチしない）
    )
    
    # ログ出力（件数＋先頭数件を表示）
    try:
        print(f"[irrawaddy] collected: {len(articles_irrawaddy)} items for {date_mmt}")
        for a in articles_irrawaddy[:10]:
            title = (a.get("title") or "").replace("\n", " ").strip()
            url = a.get("url", "")
            d = a.get("date", "")
            print(f"  - {d} | {title[:80]} | {url}")
        if len(articles_irrawaddy) > 10:
            print(f"  ... (+{len(articles_irrawaddy)-10} more)")
    except Exception:
        pass

    print("=== Khit Thit Media ===")
    articles_khit = get_khit_thit_media_articles_from_category(date_mmt, max_pages=1)
    # articles_khit = get_khit_thit_media_articles_from_category(date_mmt, max_pages=3)
    process_and_enqueue_articles(
        articles_khit, 
        "Khit Thit Media", 
        seen_urls
    )

    print("=== DVB ===")
    articles_dvb = get_dvb_articles_for(date_mmt, debug=True)
    process_and_enqueue_articles(
        articles_dvb, 
        "DVB", 
        seen_urls, 
        trust_existing_body=True
    )
    
    print("=== Myanmar Now ===")
    articles_mn = get_myanmar_now_articles_mm(date_mmt, max_pages=3)
    process_and_enqueue_articles(
        articles_mn,
        "Myanmar Now",
        seen_urls,
        bypass_keyword=False,
        trust_existing_body=True,
    )

    # URLベースの重複排除を先に行う
    print(f"⚙️ Removing URL duplicates from {len(translation_queue)} articles...")
    translation_queue = deduplicate_by_url(translation_queue)

    # バッチ翻訳実行 (5件ごとに1分待機)
    all_summaries = process_translation_batches(batch_size=TRANSLATION_BATCH_SIZE, wait_seconds=60)

    # 2) エーヤワディ以外のキーワードヒット（エーヤワディに該当しないものだけ）
    summaries_non_ayeyar = [
        s for s in all_summaries if s.get("hit_non_ayeyar") and not s.get("is_ayeyar")
    ]
    
    # ===== Business向け：全文翻訳 → 1ファイルPDF化 → 添付送信 =====
    # 1) Business に送る記事URLの順序（summaries_non_ayeyarに合わせる）
    business_urls = [s["url"] for s in summaries_non_ayeyar]

    # 2) URL→原題・原文本文のマップを組み立て（translation_queue を利用）
    #    ※ translation_queue の各要素: {"url","title","body",...}
    url_to_source_title_body = {}
    for q in translation_queue:
        u = _norm_id(q.get("url") or "")
        if u and u not in url_to_source_title_body:
            url_to_source_title_body[u] = {"title": q.get("title",""), "body": q.get("body","")}

    # 3) 全文翻訳（Gemini）
    fulltexts = translate_fulltexts_for_business(business_urls, url_to_source_title_body)
    
    # 4) URL→メール用タイトル（= 要約で使った最終タイトル）のマップ
    url_to_mail_title = {}
    for s in summaries_non_ayeyar:
        url_to_mail_title[_norm_id(s["url"])] = s["title"]

    # 5) PDFに使うタイトルをメールと完全一致に上書き
    for it in fulltexts:
        u = _norm_id(it.get("url",""))
        if u in url_to_mail_title:
            it["title_ja"] = url_to_mail_title[u]

    # 5.5) PDF用メタ（source / summary_plain / date）を結合して translated_items を作る
    import re

    def html_to_plain(s: str) -> str:
        s = s or ""
        s = re.sub(r"(?i)<br\s*/?>", "\n", s)
        s = re.sub(r"<[^>]+>", "", s)
        return s.strip()

    # url -> {source, summary_plain, date}
    url_to_meta = {}
    for s in summaries_non_ayeyar:
        u = _norm_id(s["url"])
        url_to_meta[u] = {
            "source": s.get("source", "") or "",
            "summary_plain": html_to_plain(s.get("summary", "") or ""),
            "date": (s.get("date") or ""),   # ← 追加（"YYYY-MM-DD"想定。時間付きでもOK）
            "url": u,                        # ← 追加（PDF末尾リンク用の保険）
        }

    # （任意）デバッグ：date欠落を検知
    _missing = [k for k,v in url_to_meta.items() if not v.get("date")]
    if _missing:
        print(f"[warn] missing date for {len(_missing)} item(s), example: {_missing[:3]}")
        
    _ensure_meta_dates(url_to_meta, date_mmt.isoformat())

    # fulltexts（translate_fulltexts_for_business の戻り）へメタをマージ
    translated_items = []
    for ft in fulltexts:
        u = _norm_id(ft.get("url",""))
        meta = url_to_meta.get(u, {})
        translated_items.append({
            "title_ja":      ft.get("title_ja", "") or "",
            "body_ja":       ft.get("body_ja", "") or "",
            "source":        meta.get("source", "") or "",
            "date":          meta.get("date", "") or "",   # ← 追加
            "url":           u,                            # ← 追加（PDF末尾に追記）
        })

    # 6) PDF作成（1記事=複数ページ可） — 添付ファイル名を日本語に
    pdf_bytes = None
    digest_d = get_today_date_mmt()
    jp_date = _jp_date(digest_d)  # 例: 2025年9月29日
    attachment_name = f"ミャンマーニュース全文訳【{jp_date}】.pdf"
    try:
        # ★ ここを fulltexts → translated_items に変更
        pdf_bytes = build_combined_pdf_for_business(translated_items)
        print(f"✅ PDF built in-memory: {attachment_name} ({len(pdf_bytes)} bytes)")
    except Exception as e:
        print("🛑 PDF build failed:", e)
        
    # === SEND PHASES ===
    if args.phase == "mono":
        # 従来どおり：収集→送信をまとめて一度に実行（テスト向け）
        send_email_digest(
            summaries_non_ayeyar,
            recipients_env="BUSINESS_EMAIL_RECIPIENTS",
            include_read_link=True,
            attachment_bytes=pdf_bytes if pdf_bytes else None,
            attachment_name=attachment_name if pdf_bytes else None,
            delivery_date_mmt=date_mmt,
        )
        send_email_digest(
            summaries_non_ayeyar,
            recipients_env="TRIAL_EMAIL_RECIPIENTS",
            include_read_link=True,
            trial_footer_url=(os.getenv("PAID_PLAN_URL", "").strip() or None),
            attachment_bytes=pdf_bytes if pdf_bytes else None,
            attachment_name=attachment_name if pdf_bytes else None,
            delivery_date_mmt=date_mmt,
        )
    elif args.phase == "collect":
        # ニュース収集＋bundle保存＋CSV_EMAIL_RECIPIENTS だけ即時送信
        if os.getenv("CSV_EMAIL_RECIPIENTS", "").strip():
            send_email_digest(
            summaries_non_ayeyar,
            recipients_env="CSV_EMAIL_RECIPIENTS",
            include_read_link=True,
            attachment_bytes=pdf_bytes if pdf_bytes else None,
            attachment_name=attachment_name if pdf_bytes else None,
            delivery_date_mmt=date_mmt,
            )
        # 後段送信用に束ねデータを保存
        _write_bundle(args.bundle_dir, date_mmt, summaries_non_ayeyar, pdf_bytes, attachment_name)
        print("[collect] bundle prepared.")
        
        from pathlib import Path
        import json
        b = Path(args.bundle_dir)
        summaries_ayeyar_only = [s for s in all_summaries if s.get("is_ayeyar")]
        (b / "summaries_ayeyar.json").write_text(
            json.dumps(summaries_ayeyar_only, ensure_ascii=False),
            encoding="utf-8"
        )