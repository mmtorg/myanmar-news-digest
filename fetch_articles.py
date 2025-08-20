import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import parse as parse_date
import re

# Chat GPT
# from openai import OpenAI, OpenAIError
import smtplib
import os
import sys
from email.message import EmailMessage
from email.policy import SMTPUTF8
from email.utils import formataddr
import unicodedata
from google import genai
from collections import defaultdict
import time
import json
import pprint as _pprint
import random
from typing import List, Dict, Optional

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
    )
except Exception:
    ServiceUnavailable = ResourceExhausted = DeadlineExceeded = Exception


# Gemini本番用
client_summary = genai.Client(api_key=os.getenv("GEMINI_API_SUMMARY_KEY"))
client_dedupe = genai.Client(api_key=os.getenv("GEMINI_API_DEDUPE_KEY"))


# Chat GPT
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _is_retriable_exc(e: Exception) -> bool:
    msg = (str(e) or "").lower()
    name = e.__class__.__name__.lower()

    # Google系の明示的リトライ対象
    if isinstance(e, (ServiceUnavailable, ResourceExhausted, DeadlineExceeded)):
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
        "server disconnected",
        "unavailable",
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


def call_gemini_with_retries(
    client,
    prompt,
    model="gemini-2.5-flash",
    max_retries=5,
    base_delay=2.0,
    max_delay=30.0,
):
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            return client.models.generate_content(model=model, contents=prompt)
        except Exception as e:
            if not _is_retriable_exc(e) or attempt == max_retries:
                raise
            print(
                f"⚠️ Gemini retry {attempt}/{max_retries} after: {e.__class__.__name__} | {e}"
            )
            # ジッター付き指数バックオフ
            time.sleep(min(max_delay, delay) + random.random() * 0.5)
            delay *= 2


# 要約用に送る本文の最大文字数（固定）
# Irrawaddy英語記事が3500文字くらいある
BODY_MAX_CHARS = 3500

# ミャンマー標準時 (UTC+6:30)
MMT = timezone(timedelta(hours=6, minutes=30))


# 今日の日付
# ニュースの速報性重視で今日分のニュース配信の方針
def get_today_date_mmt():
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
    # ロヒンギャ
    "ရိုဟင်ဂျာ",
    "Rohingya",
    "rohingya",
    # 国境貿易・交易
    "နယ်စပ်ကုန်သွယ်ရေး",
    # ヤンゴン管区
    "ရန်ကုန်တိုင်း",
    "Yangon Region",
    "Yangon region",
    "yangon region",
    # エーヤワディ管区
    "ဧရာဝတီတိုင်း",
    "Ayeyarwady Region",
    "Ayeyarwady region",
    "ayeyarwady region",
]

# Unicode正規化（NFC）を適用
NEWS_KEYWORDS = [unicodedata.normalize("NFC", kw) for kw in NEWS_KEYWORDS]

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

        if paragraphs:
            return paragraphs

        print(f"Paragraphs not found, waiting {wait_seconds}s and retrying...")
        time.sleep(wait_seconds)
    return []


# === 汎用の <p> 抽出器（サイト共通） ===
def extract_body_generic_from_soup(soup):
    for sel in ["div.entry-content p", "div.node-content p", "article p"]:
        ps = soup.select(sel)
        if ps:
            break
    else:
        ps = soup.find_all("p")
    txts = [p.get_text(strip=True) for p in ps if p.get_text(strip=True)]
    return "\n".join(txts).strip()


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
    まず curl_cffi(Chrome指紋) を使い、ダメなら cloudscraper、最後に requests。
    403/429/503 は指数バックオフ。記事URLは /amp も試す。
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

    # --- Try 1: curl_cffi (Chrome 指紋) ---
    try:
        from curl_cffi import requests as cfr  # type: ignore[import-not-found]

        proxies = {
            "http": os.getenv("HTTP_PROXY") or os.getenv("http_proxy"),
            "https": os.getenv("HTTPS_PROXY") or os.getenv("https_proxy"),
        }
        for attempt in range(retries):
            r = cfr.get(
                url,
                headers=HEADERS,
                impersonate="chrome124",  # ★ http2= は渡さない
                timeout=30,
                allow_redirects=True,
                proxies={k: v for k, v in proxies.items() if v},
            )
            if r.status_code == 200 and (r.text or "").strip():
                return r

            # 記事URLで 403/503 のときは /amp も試す
            if r.status_code in (403, 503) and "/news/" in url:
                amp = _amp_url(url)
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
        print(f"[fetch-cffi] EXC: {e} → {url}")

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

                # 記事URLのときは /amp も
                if r.status_code in (403, 503) and "/news/" in url:
                    amp = _amp_url(url)
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
                print(f"[fetch-cs] {attempt+1}/{retries} EXC: {e} → {url}")
                time.sleep(wait_seconds * (2**attempt) + random.uniform(0, 0.8))
    except Exception as e:
        print(f"[fetch-cs] INIT EXC: {e} → {url}")

    # --- Try 3: requests ---
    try:
        import requests

        sess = session or requests.Session()
        r2 = sess.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        print(
            f"[fetch-rq] final: HTTP {r2.status_code} len={len(getattr(r2,'text',''))} → {url}"
        )
        if r2.status_code == 200 and getattr(r2, "text", "").strip():
            return r2
        if r2.status_code in (403, 503) and "/news/" in url:
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
    t = soup.find("title")
    return _norm_text(t.get_text(strip=True)) if t else None


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

    # ==== ローカル関数 ====
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
    ]  # 先頭一致・大小無視

    # ==== 正規化・ユニーク化・除外 ====
    def norm(p: str) -> str:
        return re.sub(r"/{2,}", "/", p.strip())

    paths, seen = [], set()
    for p in CATEGORY_PATHS_RAW:
        q = norm(p)
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

                if shown_date == date_obj and href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
    except Exception as e:
        print(f"Error scanning homepage column kuDRpuo: {e}")

    # ログ、候補URL収集が終わった直後（カテゴリ＋ホーム統合のあと）
    dbg(f"[irrawaddy] candidates={len(candidate_urls)} (unique)")

    # ==== 2) 候補記事で厳密確認（meta日付/本文/キーワード） ====
    for url in candidate_urls:
        try:
            res_article = fetch_with_retry_irrawaddy(url, session=session)
            soup_article = BeautifulSoup(res_article.content, "html.parser")

            if _article_date_from_meta_mmt(soup_article) != date_obj:
                continue

            title = _extract_title(soup_article)
            if not title:
                continue

            body = extract_body_irrawaddy(soup_article)
            if not body:
                continue

            # irrawaddyはどの記事もほしいとのことなのでキーワード検索は外す
            # 大半ミャンマー記事でキーワード含んでなくても取得対象のこともあった、無駄記事の取得が目立つようであれば追加検討
            # if not any_keyword_hit(title, body):
            #     continue

            results.append(
                {
                    "url": url,
                    "title": title,
                    "date": date_obj.isoformat(),
                    "body": body,
                    "source": "irrawaddy",  # 重複削除関数を使うため追加
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

            # ④ キーワード判定（Irrawaddyなど必要に応じてバイパス）
            if not bypass_keyword:
                if not any_keyword_hit(title_nfc, body_nfc):
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
        resp = call_gemini_with_retries(client, prompt, model="gemini-2.5-flash")
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

STEP3_TASK = (
    "Step 3: 翻訳と要約処理\n"
    "以下のルールに従って、記事タイトルを自然な日本語に翻訳し、本文を要約してください。\n\n"
    "タイトル：\n"
    "- 記事タイトルを自然な日本語に翻訳してください。\n"
    "タイトルの出力条件：\n"
    "- 出力は必ず1行で「【タイトル】<半角スペース1つ><訳したタイトル>」の形式にする。\n"
    "- 「【タイトル】」の直後に改行しない。\n"
    "- 「【タイトル】<半角スペース1つ><訳したタイトル>」以外の文言は回答に含めない。\n\n"
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
    "本文超要約：\n"
    "- 以下の記事本文について重要なポイント・ユニークなキーワードをまとめ、200字以内で要約してください。\n"
    "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
    "- 例：『誰が』『何を』『どこで』『いつ』『規模（人数/金額等）』を含める。\n\n"
    "本文超要約の出力条件：\n"
    "- 1行目は`【超要約】`とだけしてください。\n"
    "- 2行目以が降全て空行になってはいけません。\n"
    "- 本文超要約の合計は最大200文字以内に収めてください。\n\n"
)

SKIP_NOTE_IRRAWADDY = "【重要】本記事は Irrawaddy の記事です。Step 1 と Step 2 は実施せず、直ちに Step 3 のみを実施してください。\n\n"


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
    return header + pre + STEP3_TASK + "\n" + input_block


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
def process_translation_batches(batch_size=5, wait_seconds=60):
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

                summary_text = "\n".join(lines).strip()
                summary_html = summary_text.replace("\n", "<br>")

                norm_url = _norm_id(item.get("url") or "")

                summarized_results.append(
                    {
                        "source": item["source"],
                        "url": norm_url,  # ★ 正規化済み
                        "title": translated_title,
                        "summary": summary_html,
                        "ultra": ultra_text,
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
        }
        for x in deduped
    ]
    return normalized


def send_email_digest(summaries):
    sender_email = os.getenv("EMAIL_SENDER")
    sender_pass = os.getenv("GMAIL_APP_PASSWORD")
    recipient_emails = os.getenv("EMAIL_RECIPIENTS", "").split(",")

    digest_date = get_today_date_mmt()
    date_str = digest_date.strftime("%Y年%-m月%-d日") + "分"

    # メディアごとにまとめる
    media_grouped = defaultdict(list)
    for item in summaries:
        media_grouped[item["source"]].append(item)

    subject = "ミャンマー関連ニュース【" + date_str + "】"

    # ✅ ヘッドライン部分を先に構築
    headlines = []
    for item in summaries:
        headlines.append(f"✓ {item['title']}")  # ← 半角スペース追加

    headline_html = (
        "<div style='margin-bottom:20px'>"
        f"------- ヘッドライン ({len(summaries)}本) -------<br>"
        + "<br>".join(headlines)  # ← 各タイトルを改行で表示
        + "</div><hr>"
    )

    # ✅ メール本文全体のHTML
    html_content = """
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #ffffff; color: #333333;">
    """

    # 先頭にヘッドライン挿入
    html_content += headline_html

    # 記事ごとの本文
    for media, articles in media_grouped.items():
        for item in articles:
            title_jp = item["title"]
            url = item["url"]
            summary_html = item["summary"]

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
                f"<p><a href='{url}' style='color:#1a0dab' target='_blank'>本文を読む</a></p>"
                "</div><hr style='border-top: 1px solid #cccccc;'>"
            )

    html_content += "</body></html>"
    html_content = clean_html_content(html_content)

    from_display_name = "Myanmar News Digest"

    msg = EmailMessage(policy=SMTPUTF8)
    msg["Subject"] = subject
    msg["From"] = formataddr((from_display_name, sender_email))
    msg["To"] = ", ".join(recipient_emails)
    msg.set_content("HTMLメールを開ける環境でご確認ください。", charset="utf-8")
    msg.add_alternative(html_content, subtype="html", charset="utf-8")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_pass)
            server.send_message(msg)
            print("✅ メール送信完了")
    except Exception as e:
        print(f"❌ メール送信エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    date_mmt = get_today_date_mmt()
    seen_urls = set()

    # articles = get_frontier_articles_for(date_mmt)
    # for art in articles:
    #     print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    # === Mizzima (Burmese) ===
    print("=== Mizzima (Burmese) ===")
    articles_mizzima = get_mizzima_articles_from_category(
        date_mmt,
        "https://bur.mizzima.com",
        "Mizzima (Burmese)",
        "/category/%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8/%e1%80%99%e1%80%bc%e1%80%94%e1%80%ba%e1%80%99%e1%80%ac%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8",
        max_pages=3,
    )
    process_and_enqueue_articles(
        articles_mizzima, "Mizzima (Burmese)", seen_urls, trust_existing_body=True
    )

    print("=== BBC Burmese ===")
    articles_bbc = get_bbc_burmese_articles_for(date_mmt)
    process_and_enqueue_articles(
        articles_bbc, "BBC Burmese", seen_urls, trust_existing_body=True
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

    print("=== Khit Thit Media ===")
    articles_khit = get_khit_thit_media_articles_from_category(date_mmt, max_pages=3)
    process_and_enqueue_articles(articles_khit, "Khit Thit Media", seen_urls)

    print("=== DVB ===")
    articles_dvb = get_dvb_articles_for(date_mmt, debug=True)
    process_and_enqueue_articles(
        articles_dvb, "DVB", seen_urls, trust_existing_body=True
    )

    # URLベースの重複排除を先に行う
    print(f"⚙️ Removing URL duplicates from {len(translation_queue)} articles...")
    translation_queue = deduplicate_by_url(translation_queue)

    # バッチ翻訳実行 (5件ごとに1分待機)
    all_summaries = process_translation_batches(batch_size=5, wait_seconds=60)

    send_email_digest(all_summaries)
