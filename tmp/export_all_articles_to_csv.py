
# -*- coding: utf-8 -*-
"""
export_all_articles_to_csv.py

新規処理:
・各メディアの「キーワード絞り込み前」の記事一覧を取得
・MMTで 2025-08-23(土) 以降
・タイトルを gemini-2.5-flash で日本語翻訳（バッチ翻訳対応）
・CSV (UTF-8 BOM) に A:メディア名 / B:日本語タイトル / C:発行日(MMT) / D:URL
・無料枠対策: レートリミット (RPM/最小インターバル/ジッター) + バッチ翻訳

使い方(例):
  python export_all_articles_to_csv.py --start 2025-08-23 --out articles.csv \
    --batch-size 20 --rpm 20 --min-interval 1.5 --jitter 0.3
必要: 同ディレクトリに fetch_articles.py (添付ファイル) を配置し import 可能であること。
Gemini API の認証は fetch_articles.py の実装に従います。

[プロンプト]
- 添付したファイルのコードを参考に考えてもらいたいです。添付したコードの修正ではなく新しい処理として作ってください。ただし、添付したコードで使える処理はそのまま使ってOKです。
- 添付のコードでキーワード検索で絞り込むを行う前の時点での全てのメディアの記事の情報を取得したいです。その結果はCSVにして出力したいです。
- CATEGORY_URLS、CATEGORY_PATHS_RAW、EXCLUDE_PREFIXESなどのURLの指定条件は従ってください
- 以下、CSVのアウトプットのイメージです
    - A列：メディア名
    - B列：日本語に翻訳したタイトル
    - C列：記事が発行された日付
    - D列：記事のURL
- タイムゾーンはミャンマー時間です
- ミャンマー時間で8月23日土曜日以降の記事が取得対象です
- 記事のタイトルを日本語に翻訳するには、添付したコードと同様に、gemini-2.5-flashを使ってください。プロンプトは添付したコードのタイトルを日本語に翻訳するプロンプトを使ってOKです
- CSVに吐き出す日本語は文字化けしないように注意してください
"""

from __future__ import annotations
import argparse
import csv
import sys
import time
import unicodedata
from datetime import datetime, date, timedelta
from typing import Dict, List, Iterable
import os
import random
from collections import deque
import re
import json
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
from curl_cffi.requests import Session as CurlSession

# --- 添付コード（fetch_articles.py）から利用する関数/定数 ---
from fetch_articles import (
    MMT,
    build_prompt,
    call_gemini_with_retries,
    client_summary,
    deduplicate_by_url,
    fetch_with_retry_irrawaddy,
    extract_body_irrawaddy,
    _parse_category_date_text,
    _article_date_from_meta_mmt,
    _extract_title,
    fetch_with_retry,
    fetch_with_retry_dvb,
    extract_paragraphs_with_wait,
)

# ----------------------------------------------------------------

def daterange_mmt(start: date, end: date) -> Iterable[date]:
    """MMT日付で start..end（両端含む）を返す"""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

# ===== BBC Burmese (RSS) =====
def collect_bbc_all_for_date(target_date_mmt: date) -> List[Dict]:
    """RSSをUTC→MMT換算し、対象日一致のみ返す（キーワード絞り込みなし）"""
    rss_url = "https://feeds.bbci.co.uk/burmese/rss.xml"
    session = _make_pooled_session()
    try:
        res = session.get(rss_url, timeout=10)
        res.raise_for_status()
    except Exception as e:
        print(f"[bbc] RSS取得失敗: {e}")
        return []

    soup = BeautifulSoup(res.content, "xml")
    out: List[Dict] = []
    for item in soup.find_all("item"):
        pub_date_tag = item.find("pubDate")
        link_tag = item.find("link")
        title_tag = item.find("title")
        if not (pub_date_tag and link_tag and title_tag):
            continue
        try:
            pub_dt = parse_date(pub_date_tag.text).astimezone(MMT)
            if pub_dt.date() != target_date_mmt:
                continue
            title = unicodedata.normalize("NFC", (title_tag.text or "").strip())
            url = (link_tag.text or "").strip()
            if not (title and url):
                continue
            out.append(
                {
                    "source": "BBC Burmese",
                    "title": title,
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": "",
                }
            )
        except Exception:
            continue
    return out

from requests.adapters import HTTPAdapter
def _make_pooled_session() -> requests.Session:
    """
    出力・ロジック不変のまま、接続プール/Keep-Alive/圧縮転送を有効化した Session を返す。
    """
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)  # リトライは既存ロジックに委ねる
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/128.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s

# ===== Khit Thit Media =====
def collect_khitthit_all_for_date(target_date_mmt: date, max_pages: int = 15) -> List[Dict]:
    """
    fetch_articles.py と同じロジック:
      ・カテゴリ一覧/記事取得ともに fetch_with_retry のみを使用
      ・キーワード絞り込みは実施しない
      ・ページ数は 15 まで拡大
    """
    CATEGORY_URLS = [
        "https://yktnews.com/category/news/",
        "https://yktnews.com/category/politics/",
        "https://yktnews.com/category/editor-choice/",
        "https://yktnews.com/category/interview/",
        "https://yktnews.com/category/china-watch/",
    ]

    def _remove_hashtag_links(soup):
        for a in soup.select("a"):
            txt = a.get_text(strip=True)
            if txt.startswith("#"):
                a.decompose()

    HASHTAG_TOKEN_RE = re.compile(r"(?:(?<=\s)|^)\#[^\s#]+")

    collected_urls = set()
    for base_url in CATEGORY_URLS:
        for page in range(1, max_pages + 1):
            url = f"{base_url}page/{page}/" if page > 1 else base_url
            try:
                res = fetch_with_retry(url)
            except Exception as e:
                print(f"[khitthit] stop pagination: {url} -> {e}")
                break

            soup = BeautifulSoup(res.content, "html.parser")
            entry_links = soup.select("p.entry-title.td-module-title a[href]")
            if not entry_links:
                print(f"[khitthit] no entries: {url}")
                break
            for a in entry_links:
                href = a.get("href")
                if href and href not in collected_urls:
                    collected_urls.add(href)

    results: List[Dict] = []
    for url in collected_urls:
        try:
            res = fetch_with_retry(url)
            soup = BeautifulSoup(res.content, "html.parser")

            # 発行日時 → MMT
            meta_tag = soup.find("meta", property="article:published_time")
            if not meta_tag or not meta_tag.has_attr("content"):
                continue
            dt = datetime.fromisoformat(meta_tag["content"]).astimezone(MMT)
            if dt.date() != target_date_mmt:
                continue

            # タイトル
            h1 = soup.find("h1") or soup.find("title")
            title = (h1.get_text(strip=True) if h1 else "").strip()
            if not title:
                continue

            # 本文（#除去）
            _remove_hashtag_links(soup)
            paragraphs = extract_paragraphs_with_wait(soup)
            body_text = "\n".join(
                HASHTAG_TOKEN_RE.sub("", p.get_text(strip=True)).strip()
                for p in paragraphs
                if p.get_text(strip=True)
            ).strip()

            results.append(
                {
                    "source": "Khit Thit Media",
                    "title": unicodedata.normalize("NFC", title),
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": unicodedata.normalize("NFC", body_text),
                }
            )
        except Exception as e:
            print(f"[khitthit] article fail {url}: {e}")
            continue

    return results

# ===== DVB =====
def collect_dvb_all_for_date(target_date_mmt: date) -> List[Dict]:
    """
    fetch_articles.py と同じ方針で、DVB 専用フェッチャ fetch_with_retry_dvb を使用。
    一覧（/category/8/news と ?page=2）→ 記事ページの順で取得。
    """
    BASE = "https://burmese.dvb.no"
    CATEGORY_PATHS = ["/category/8/news"]

    try:
        sess = _make_pooled_session()
    except Exception:
        sess = None

    candidate_urls: List[str] = []
    seen_urls = set()

    for path in CATEGORY_PATHS:
        for page_no in (1, 2):
            url = f"{BASE}{path}" if page_no == 1 else f"{BASE}{path}?page=2"
            try:
                res = fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=sess)
            except Exception as e:
                print(f"[dvb] list fetch fail {url}: {e}")
                continue

            soup = BeautifulSoup(getattr(res, "content", None) or res.text, "html.parser")

            blocks = soup.select(
                "div.md\\:grid.grid-cols-3.gap-4.mt-5, div.grid.grid-cols-3.gap-4.mt-5"
            ) or [soup]

            for scope in blocks:
                anchors = scope.select('a[href^="/post/"]')
                for a in anchors:
                    href = a.get("href") or ""
                    date_div = a.select_one("div.flex.gap-1.text-xs.mt-2.text-gray-500 div")
                    date_text = (date_div.get_text(" ", strip=True) if date_div else "").strip()
                    if not date_text:
                        full = a.get_text(" ", strip=True)
                        m = re.search(
                            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}",
                            full,
                        )
                        date_text = m.group(0) if m else ""
                    d = None
                    try:
                        if date_text:
                            d = _parse_category_date_text(date_text)
                    except Exception:
                        d = None
                    if d and d == target_date_mmt:
                        uabs = href if href.startswith("http") else f"{BASE}{href}"
                        if uabs not in seen_urls:
                            candidate_urls.append(uabs)
                            seen_urls.add(uabs)

    results: List[Dict] = []
    for url in candidate_urls:
        try:
            res = fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=sess)
            soup = BeautifulSoup(getattr(res, "content", None) or res.text, "html.parser")

            # タイトル抽出を強化: og:title → h1/.post-title → <title> の順
            title_tag = soup.find("meta", attrs={"property": "og:title"})
            if title_tag and title_tag.has_attr("content"):
                title = (title_tag["content"] or "").strip()
            else:
                t = soup.select_one(".text-2xl, h1, .post-title") or soup.find("title")
                title = (t.get_text(strip=True) if t else "").strip()
            host = soup.select_one(".full_content")
            body = ""
            if host:
                parts = []
                for p in host.select("p"):
                    txt = re.sub(r"\s+", " ", p.get_text(" ", strip=True))
                    if txt:
                        parts.append(txt)
                body = "\n".join(parts).strip()

            if not title or not body:
                continue

            results.append(
                {
                    "url": url,
                    "title": unicodedata.normalize("NFC", title),
                    "date": target_date_mmt.isoformat(),
                    "body": unicodedata.normalize("NFC", body),
                    "source": "DVB",
                }
            )
        except Exception as e:
            print(f"[dvb] article fail {url}: {e}")
            continue

    if results:
        before = len(results)
        results = deduplicate_by_url(results)
        if before != len(results):
            print(f"[dvb] dedup: {before} -> {len(results)}")

    return results

# ===== Mizzima =====
def collect_mizzima_all_for_date(target_date_mmt: date, max_pages: int = 15) -> List[Dict]:
    base_url = "https://bur.mizzima.com"
    category_path = (
        "/category/%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8/"
        "%e1%80%99%e1%80%bc%e1%80%94%e1%80%ba%e1%80%99%e1%80%ac"
        "%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8"
    )

    EXCLUDE_TITLE_KEYWORDS = [
        "နွေဦးတော်လှန်ရေး နေ့စဉ်မှတ်စု",
        "ဓာတ်ပုံသတင်း",
    ]

    article_urls: List[str] = []
    session = _make_pooled_session()
    for page_num in range(1, max_pages + 1):
        url = f"{base_url}{category_path}" if page_num == 1 else f"{base_url}{category_path}/page/{page_num}/"
        try:
            res = session.get(url, timeout=10)
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.content, "html.parser")
            links = [a["href"] for a in soup.select("main.site-main article a.post-thumbnail[href]")]
            article_urls.extend(links)
        except Exception as e:
            print(f"[mizzima] list fail {url}: {e}")
            continue

    results: List[Dict] = []
    for url in article_urls:
        try:
            res = session.get(url, timeout=10)
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.content, "html.parser")

            meta_tag = soup.find("meta", property="article:published_time")
            if not meta_tag or not meta_tag.has_attr("content"):
                continue
            dt = datetime.fromisoformat(meta_tag["content"]).astimezone(MMT)
            if dt.date() != target_date_mmt:
                continue

            title_tag = soup.find("meta", attrs={"property": "og:title"})
            title = (title_tag["content"].strip() if title_tag and title_tag.has_attr("content") else "")
            if not title:
                continue
            title_nfc = unicodedata.normalize("NFC", title)
            if any(kw in title_nfc for kw in EXCLUDE_TITLE_KEYWORDS):
                continue

            content_div = soup.find("div", class_="entry-content")
            if not content_div:
                continue
            paras = []
            for p in content_div.find_all("p"):
                if p.find_previous("h2", string=re.compile("Related Posts", re.I)):
                    break
                paras.append(p)
            body_text = "\n".join(p.get_text(strip=True) for p in paras).strip()
            if not body_text:
                continue

            results.append(
                {
                    "source": "Mizzima (Burmese)",
                    "title": title_nfc,
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": unicodedata.normalize("NFC", body_text),
                }
            )
        except Exception as e:
            print(f"[mizzima] article fail {url}: {e}")
            continue

    return results

# ===== Myanmar Now (mm) =====
def collect_myanmar_now_mm_all_for_date(target_date_mmt: date, max_pages: int = 3) -> List[Dict]:
    """
    Myanmar Now (mm) の各カテゴリから対象日の記事を取得（キーワード絞り込みなし）。
    返り値: list[dict] {source, title, url, date(ISO str, MMT), body}
    """
    BASE_CATEGORIES = [
        "https://myanmar-now.org/mm/news/category/news/",
        "https://myanmar-now.org/mm/news/category/news/3/",
        "https://myanmar-now.org/mm/news/category/news/17/",
        "https://myanmar-now.org/mm/news/category/news/social-issue/",
        "https://myanmar-now.org/mm/news/category/news/19/",
        "https://myanmar-now.org/mm/news/category/news/international-news/",
        "https://myanmar-now.org/mm/news/category/multimedia/16/",
        "https://myanmar-now.org/mm/news/category/in-depth/",
        "https://myanmar-now.org/mm/news/category/in-depth/analysis/",
        "https://myanmar-now.org/mm/news/category/in-depth/investigation/",
        "https://myanmar-now.org/mm/news/category/in-depth/profile/",
        "https://myanmar-now.org/mm/news/category/in-depth/society/",
        "https://myanmar-now.org/mm/news/category/opinion/",
        "https://myanmar-now.org/mm/news/category/opinion/commentary/",
        "https://myanmar-now.org/mm/news/category/opinion/29/",
        "https://myanmar-now.org/mm/news/category/opinion/interview/",
    ]
    
    sess = _make_pooled_session()

    def _strip_source_suffix(title: str) -> str:
        if not title:
            return title
        return re.sub(r"\s*-\s*Myanmar Now\s*$", "", title).strip()

    today_label = f"{target_date_mmt.strftime('%B')} {target_date_mmt.day}, {target_date_mmt.year}"

    def _collect_article_urls_from_category(cat_url: str) -> set[str]:
        urls: set[str] = set()
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
                if a and a.get("href") and "/mm/news/" in a["href"]:
                    urls.add(a["href"])
        return urls

    collected: set[str] = set()
    for base in BASE_CATEGORIES:
        collected |= _collect_article_urls_from_category(base)

    items: List[Dict] = []

    def _fetch_text(url: str, timeout: int = 20) -> str:
        try:
            r = sess.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if r.status_code == 200 and (r.text or "").strip():
                return r.text
        except Exception:
            pass
        return ""

    def _fetch_text_via_jina(url: str, timeout: int = 25) -> str:
        try:
            alt = f"https://r.jina.ai/http://{url.lstrip('/')}"
            r = sess.get(alt, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
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
            r = sess.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
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
    for url in collected:
        try:
            title = ""
            body = ""
            meta_date_ok = False
            soup = None
            try:
                res = fetch_with_retry(url)
                soup = BeautifulSoup(res.content, "html.parser")
            except Exception:
                soup = None

            if soup is not None:
                meta = soup.find("meta", attrs={"property": "article:published_time"})
                if meta and meta.get("content"):
                    try:
                        dt_utc = datetime.fromisoformat(meta["content"])  # aware if offset
                    except Exception:
                        dt_utc = parse_date(meta["content"])  # may be aware
                    dt_mmt = dt_utc.astimezone(MMT)
                    if dt_mmt.date() == target_date_mmt:
                        meta_date_ok = True

            # タイトル
            if soup is not None:
                title_raw = (soup.title.get_text(strip=True) if soup.title else "").strip()
                title = _strip_source_suffix(unicodedata.normalize("NFC", title_raw))
                if not title:
                    h1 = soup.find("h1")
                    if h1:
                        title = _strip_source_suffix(unicodedata.normalize("NFC", h1.get_text(strip=True)))
            if not title:
                title = _oembed_title(url) or _title_from_slug(url)
                title = _strip_source_suffix(title)
            if not title:
                continue

            # 本文
            if soup is not None:
                body_parts = []
                content_root = soup.select_one("div.entry-content.entry.clearfix") or soup
                for p in content_root.find_all("p"):
                    txt = p.get_text(strip=True)
                    if txt:
                        body_parts.append(txt)
                body = unicodedata.normalize("NFC", "\n".join(body_parts).strip())
                if not body:
                    paragraphs = extract_paragraphs_with_wait(soup)
                    body = unicodedata.normalize("NFC", "\n".join(
                        p.get_text(strip=True) for p in paragraphs if getattr(p, "get_text", None)
                    )).strip()
            if not body:
                body = _fetch_text_via_jina(url)
                if not body:
                    body = _fetch_text(url)
            if not body:
                continue

            items.append({
                "source": "Myanmar Now",
                "title": title,
                "url": url,
                # 直接HTMLでmeta日付確認できた場合はその日付、
                # そうでない場合もカテゴリ抽出が当日なので target_date_mmt を採用
                "date": (dt_mmt.isoformat() if meta_date_ok else target_date_mmt.isoformat()),
                "body": body,
            })
        except Exception as e:
            print(f"[warn] Myanmar Now article fetch failed: {url} ({e})")
            continue

    return items

# ===== Irrawaddy (no keyword filter) =====
def collect_irrawaddy_all_for_date(target_date_mmt: date, debug: bool = False) -> List[Dict]:
    """
    Irrawaddy のカテゴリ一覧＋ホームの当日候補から、記事ページを精査して
    指定MMT日付の記事だけを収集（キーワード絞り込みは行わない）。

    返り値の仕様は本モジュール内の他媒体と同様：
      [{"source","title","url","date","body"}]
    """
    from urllib.parse import urlparse

    BASE = "https://www.irrawaddy.com"
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
    EXCLUDE_PREFIXES = [
        "/category/news/asia",  # 除外依頼有
        "/category/news/world",  # 除外依頼有
        "/video",  # "/category/Video"は除外対象だがこのパターンもある
        "/cartoons",  # "/category/Cartoons"は除外対象だがこのパターンもある
    ]
    
    sess = _make_pooled_session()

    def _is_excluded_url(href: str) -> bool:
        try:
            p = urlparse(href or "").path.lower()
        except Exception:
            p = (href or "").lower()
        return any(p.startswith(x) for x in EXCLUDE_PREFIXES)

    def _norm_path(p: str) -> str:
        p = (p or "").strip()
        p = re.sub(r"/{2,}", "/", p)
        return p

    # 正規化＋除外＋ユニーク
    paths, seen = [], set()
    for p in CATEGORY_PATHS_RAW:
        q = _norm_path(p)
        if any(q.lower().startswith(x) for x in EXCLUDE_PREFIXES):
            continue
        if q not in seen:
            seen.add(q)
            paths.append(q)

    # 共有セッション（Cookie/指紋引き継ぎ用）
    try:
        session = requests.Session()
    except Exception:
        session = None

    results: List[Dict] = []
    seen_urls = set()
    candidate_urls: List[str] = []
    origins: Dict[str, str] = {}
    # RSS/検索から得た補助情報（タイトル/日付）をURLキーで保持
    feed_hints: Dict[str, Dict[str, str]] = {}
    if debug:
        print(f"[irrawaddy] target_date={target_date_mmt}")

    # 1) 各カテゴリを1回ずつ巡回し、当日候補URLを収集
    for rel in paths:
        url = f"{BASE}{rel}"
        try:
            res = fetch_with_retry_irrawaddy(url, session=session)
        except Exception as e:
            print(f"[irrawaddy] list fetch fail {url}: {e}")
            continue
        if debug:
            sc = getattr(res, "status_code", "?")
            body = getattr(res, "content", None)
            blen = len(body) if body is not None else len(getattr(res, "text", ""))
            print(f"[irrawaddy][list] fetched: {url} status={sc} bytes={blen}")
        soup = BeautifulSoup(getattr(res, "content", None) or getattr(res, "text", ""), "html.parser")
        if debug:
            title_txt = (soup.title.get_text(strip=True) if soup.title else "")
            c_hero = len(soup.select('.jnews_category_hero_container'))
            c_postmeta = len(soup.select('.jeg_post_meta'))
            c_date_links = len(soup.select('.jeg_meta_date a[href]'))
            c_titles = len(soup.select('.jeg_post_title a[href]'))
            c_rel_next = len([ln for ln in soup.select('link[rel="next"]') if ln.get('href')])
            print(f"[irrawaddy][list] title='{title_txt[:60]}' hero={c_hero} post_meta={c_postmeta} date_links={c_date_links} titles={c_titles} rel_next={c_rel_next}")
        wrapper = soup.select_one("div.jeg_content")
        if debug:
            print(f"[irrawaddy][list] wrapper_found={'yes' if wrapper else 'no'}")
        scopes = ([wrapper] if wrapper else []) + [soup]

        cat_added = 0
        for scope in scopes:
            links = scope.select(
                ".jnews_category_hero_container .jeg_meta_date a[href], "
                "div.jeg_postblock_content .jeg_meta_date a[href], "
                ".jeg_post_meta .jeg_meta_date a[href]"
            )
            # 時計アイコン <i class="fa fa-clock-o"> が無い要素も許容する
            if debug:
                print(f"[irrawaddy][list] candidates in-page: raw={len(links)}")

            found = 0
            for a in links:
                href = (a.get("href") or "").strip()
                raw = a.get_text(" ", strip=True)
                try:
                    shown_date = _parse_category_date_text(raw)
                except Exception:
                    continue
                if _is_excluded_url(href):
                    continue
                if shown_date == target_date_mmt and href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
                    origins[href] = origins.get(href, "cat")
                    found += 1
                    cat_added += 1
            if found > 0:
                break
        if debug:
            print(f"[irrawaddy][list] added_from_category={found} total_candidates={len(candidate_urls)}")

        # カテゴリRSSフォールバックは無効化（不要のため削除）

    # 1.5) ホーム特定カラム（data-id=kuDRpuo）でも当日候補を収集
    try:
        res_home = fetch_with_retry_irrawaddy(f"{BASE}/", session=session)
        if debug:
            sc = getattr(res_home, "status_code", "?")
            body = getattr(res_home, "content", None)
            blen = len(body) if body is not None else len(getattr(res_home, "text", ""))
            print(f"[irrawaddy][home] fetched: / status={sc} bytes={blen}")
        soup_home = BeautifulSoup(getattr(res_home, "content", None) or getattr(res_home, "text", ""), "html.parser")
        home_scope = soup_home.select_one(
            'div.elementor-element-kuDRpuo[data-id="kuDRpuo"], '
            "div.elementor-element-kuDRpuo, "
            '[data-id="kuDRpuo"]'
        )
        if debug:
            print(f"[irrawaddy][home] home_scope_found={'yes' if home_scope else 'no'}")
        if home_scope:
            links = home_scope.select(".jeg_meta_date a[href]")
            # 時計アイコン <i class="fa fa-clock-o"> が無い要素も許容する
            if debug:
                print(f"[irrawaddy][home] raw={len(links)}")
            for a in links:
                href = (a.get("href") or "").strip()
                raw = a.get_text(" ", strip=True)
                try:
                    shown_date = _parse_category_date_text(raw)
                except Exception:
                    continue
                if _is_excluded_url(href):
                    continue
                if shown_date == target_date_mmt and href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
                    origins[href] = origins.get(href, "home")
    except Exception as e:
        print(f"[irrawaddy] home scan fail: {e}")

    if debug:
        print(f"[irrawaddy] candidates(unique)={len(candidate_urls)}")
        for u in candidate_urls[:5]:
            print(f"  - {u}")

    # 1.9) 候補が空なら RSS / Google News からフォールバック
    def _fetch_text(url: str, timeout: int = 20) -> str:
        try:
            r = sess.get(
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
            r = sess.get(alt, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if r.status_code == 200 and (r.text or "").strip():
                return r.text
        except Exception:
            pass
        return ""

    def _mmt_date(dt_utc: datetime) -> date:
        return dt_utc.astimezone(MMT).date()

    def _rss_items_from_google_news() -> List[Dict[str, str]]:
        # 当日限定（MMT判定は後段）
        gnews = (
            "https://news.google.com/rss/search?"
            "q=site:irrawaddy.com+when:1d&hl=en-US&gl=US&ceid=US:en"
        )
        xml = _fetch_text(gnews)
        if not xml:
            xml = _fetch_text_via_jina(gnews)
            if not xml:
                return []
        try:
            from xml.etree import ElementTree as ET
            root = ET.fromstring(xml)
        except Exception:
            return []
        import re as _re
        href_re = _re.compile(r'href=["\']([^"\']+)["\']', _re.I)
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
            items.append({"title": title, "link": direct or link, "pubDate": pub})
        return items

    if len(candidate_urls) == 0:
        # 1) WP JSON
        wp_url = (
            "https://www.irrawaddy.com/wp-json/wp/v2/posts?per_page=50&_fields=link,date,title"
        )
        wp_json = _fetch_text(wp_url) or _fetch_text_via_jina(wp_url)
        if wp_json:
            try:
                arr = json.loads(wp_json)
                for o in arr:
                    link = (o.get("link") or "").strip()
                    ds = o.get("date") or ""
                    t = o.get("title")
                    if isinstance(t, dict):
                        tt = (t.get("rendered") or "").strip()
                    else:
                        tt = (t or "").strip()
                    try:
                        dt = parse_date(ds)
                    except Exception:
                        dt = None
                    if link and dt and _mmt_date(dt) == target_date_mmt and not _is_excluded_url(link):
                        if link not in seen_urls:
                            candidate_urls.append(link)
                            seen_urls.add(link)
                            origins[link] = "feed"
                        feed_hints[link] = {"title": tt, "date": target_date_mmt.isoformat()}
            except Exception:
                pass

        # 2) RSS
        if len(candidate_urls) == 0:
            feed_url = "https://www.irrawaddy.com/feed"
            feed_xml = _fetch_text(feed_url) or _fetch_text_via_jina(feed_url)
            if feed_xml:
                try:
                    from xml.etree import ElementTree as ET
                    root = ET.fromstring(feed_xml)
                    for it in root.findall(".//item"):
                        title = (it.findtext("title") or "").strip()
                        link = (it.findtext("link") or "").strip()
                        pub = (it.findtext("pubDate") or "").strip()
                        try:
                            dt = parse_date(pub)
                        except Exception:
                            dt = None
                        if link and dt and _mmt_date(dt) == target_date_mmt and not _is_excluded_url(link):
                            if link not in seen_urls:
                                candidate_urls.append(link)
                                seen_urls.add(link)
                                origins[link] = "feed"
                            feed_hints[link] = {"title": title, "date": target_date_mmt.isoformat()}
                except Exception:
                    pass

        # 3) Google News（当日のみ）
        if len(candidate_urls) == 0:
            for it in _rss_items_from_google_news():
                title = it.get("title") or ""
                link = (it.get("link") or "").strip()
                pub = it.get("pubDate") or ""
                try:
                    dt = parse_date(pub)
                except Exception:
                    dt = None
                if link and dt and _mmt_date(dt) == target_date_mmt and not _is_excluded_url(link):
                    if link not in seen_urls:
                        candidate_urls.append(link)
                        seen_urls.add(link)
                        origins[link] = "feed"
                    feed_hints[link] = {"title": title, "date": target_date_mmt.isoformat()}
        if debug:
            print(f"[irrawaddy] fallback candidates={len(candidate_urls)}")

    # 2) 各候補記事の meta 日付を MMT で厳密確認し、タイトル/本文を抽出
    for url in candidate_urls:
        if _is_excluded_url(url):
            continue
        try:
            # 2.1) 直接HTML（1回）→ 失敗時は空のまま
            title = ""
            body = ""
            try:
                res = fetch_with_retry_irrawaddy(url, session=session)
                soup = BeautifulSoup(getattr(res, "content", None) or res.text, "html.parser")
                meta_date = _article_date_from_meta_mmt(soup)
            except Exception:
                soup = None
                meta_date = None

            if debug:
                print(f"[irrawaddy][article] url={url} meta_date={meta_date}")
            if meta_date != target_date_mmt:
                # フィード補助があればフォールバック採用
                hint = feed_hints.get(url)
                if hint and (hint.get("date") == target_date_mmt.isoformat()):
                    title_fb = (hint.get("title") or "").strip()
                    if title_fb:
                        if debug:
                            print("  -> fallback: use feed title/date (meta_date mismatch)")
                        results.append(
                            {
                                "source": "Irrawaddy",
                                "title": unicodedata.normalize("NFC", title_fb),
                                "url": url,
                                "date": target_date_mmt.isoformat(),
                                "body": "",
                            }
                        )
                        continue
                # カテゴリ/ホーム由来の場合は、一覧の当日判定を信頼して採用
                if origins.get(url) in ("cat", "home"):
                    title_fb = (feed_hints.get(url, {}).get("title") or "").strip()
                    if not title_fb:
                        # 最低限のタイトル補完（oEmbed/slug）
                        # 再利用の簡易関数をここでも使用
                        def _title_from_slug_local(u: str) -> str:
                            try:
                                from urllib.parse import urlparse, unquote
                                seg = urlparse(u).path.rstrip("/").split("/")[-1]
                                seg = unquote(seg).replace(".html", "").replace("-", " ")
                                return unicodedata.normalize("NFC", seg.title())
                            except Exception:
                                return ""
                        title_fb = _title_from_slug_local(url)
                    results.append(
                        {
                            "source": "Irrawaddy",
                            "title": unicodedata.normalize("NFC", title_fb),
                            "url": url,
                            "date": target_date_mmt.isoformat(),
                            "body": "",
                        }
                    )
                    continue
                if debug:
                    print("  -> skip: date mismatch")
                continue

            if soup is not None:
                title = _extract_title(soup) or ""
                body = extract_body_irrawaddy(soup) or ""
            title = unicodedata.normalize("NFC", title).strip()
            body = unicodedata.normalize("NFC", body).strip()

            # 2.2) タイトルが空ならフィード→ oEmbed → スラッグ
            def _title_from_slug(u: str) -> str:
                try:
                    from urllib.parse import urlparse, unquote
                    seg = urlparse(u).path.rstrip("/").split("/")[-1]
                    seg = unquote(seg).replace(".html", "").replace("-", " ")
                    return unicodedata.normalize("NFC", seg.title())
                except Exception:
                    return ""

            def _oembed_title(u: str) -> str:
                try:
                    api = (
                        "https://www.irrawaddy.com/wp-json/oembed/1.0/embed?url="
                        + requests.utils.requote_uri(u)
                    )
                    r = sess.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                    if r.status_code == 200 and (r.text or "").strip():
                        data = r.json()
                        t = (data.get("title") or "").strip()
                        return unicodedata.normalize("NFC", t)
                except Exception:
                    pass
                return ""

            if not title:
                # フィード補助があればフォールバック採用
                hint = feed_hints.get(url)
                title_fb = (hint or {}).get("title") or ""
                if title_fb:
                    if debug:
                        print("  -> fallback: use feed title (empty title)")
                    results.append(
                        {
                            "source": "Irrawaddy",
                            "title": unicodedata.normalize("NFC", title_fb),
                            "url": url,
                            "date": target_date_mmt.isoformat(),
                            "body": body,  # 取れていればそのまま、無ければ空
                        }
                    )
                    continue
                # oEmbed / slug
                title = _oembed_title(url) or _title_from_slug(url)
                title = (title or "").strip()
                if not title:
                    if debug:
                        print("  -> skip: empty title")
                    continue

            # 2.3) 本文が空なら r.jina.ai の本文テキストにフォールバック
            if not body:
                alt = f"https://r.jina.ai/http://{url.lstrip('/')}"
                txt = _fetch_text(alt, timeout=25)
                if (not txt) and "/news/" in url:
                    from urllib.parse import urljoin as _urljoin
                    amp = url if url.endswith("/amp") else _urljoin(url.rstrip("/") + "/", "amp")
                    alt2 = f"https://r.jina.ai/http://{amp.lstrip('/')}"
                    txt = _fetch_text(alt2, timeout=25)
                if txt:
                    body = unicodedata.normalize("NFC", txt).strip()
            if not body and debug:
                print("  -> note: empty body")

            results.append(
                {
                    "source": "Irrawaddy",
                    "title": title,
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": body,
                }
            )
        except Exception as e:
            print(f"[irrawaddy] article fail {url}: {e}")
            continue

    return results

# ===== Global New Light Of Myanmar (GNLM) =====
def collect_gnlm_all_for_date(target_date_mmt: date, max_pages: int = 3) -> List[Dict]:
    """
    Global New Light of Myanmar の National / Business / Local News から
    対象MMT日付の記事を取得（キーワード絞り込みなし）。
    """

    BASE_CATEGORIES = [
        "https://www.gnlm.com.mm/category/national/",
        "https://www.gnlm.com.mm/category/business/",
        "https://www.gnlm.com.mm/category/local-news/",
    ]

    def _title_from_slug(u: str) -> str:
        try:
            from urllib.parse import urlparse, unquote
            seg = urlparse(u).path.rstrip("/").split("/")[-1]
            seg = unquote(seg).replace("-", " ")
            return unicodedata.normalize("NFC", seg)
        except Exception:
            return ""

    # ★ ここを curl_cffi に変更（最重要）
    sess = CurlSession(impersonate="chrome")

    collected_urls: set[str] = set()

    # ---- 一覧ページ ----
    for base in BASE_CATEGORIES:
        for page in range(1, max_pages + 1):
            list_url = base if page == 1 else f"{base}page/{page}/"
            try:
                res = sess.get(list_url, timeout=20)
                if res.status_code != 200 or not res.text.strip():
                    raise Exception(f"status={res.status_code}")
            except Exception as e:
                print(f"[gnlm] list fetch failed: {e} url={list_url}")
                break

            soup = BeautifulSoup(res.text, "html.parser")
            articles = soup.select("article.archives-page")
            if not articles:
                break

            stop_paging = False

            for art in articles:
                date_span = art.select_one("div.post-date span")
                if not date_span:
                    continue

                try:
                    d = datetime.strptime(
                        date_span.get_text(strip=True),
                        "%B %d, %Y"
                    ).date()
                except:
                    continue

                if d > target_date_mmt:
                    continue
                elif d < target_date_mmt:
                    stop_paging = True
                    break

                a = art.select_one("h4.post-title a[href]")
                if a and a.get("href"):
                    collected_urls.add(a["href"])

            if stop_paging:
                break

    # ---- 個別記事 ----
    out = []

    for url in sorted(collected_urls):
        try:
            res = sess.get(url, timeout=20)
            if res.status_code != 200 or not res.text.strip():
                raise Exception(f"status={res.status_code}")
        except Exception as e:
            print(f"[gnlm] article fetch failed: {e} url={url}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")

        art_date = _article_date_from_meta_mmt(soup) or target_date_mmt

        title = _extract_title(soup)
        if not title:
            h1 = soup.select_one("header#article-title h1.entry-title")
            if h1:
                title = h1.get_text(strip=True)
        if not title:
            title = _title_from_slug(url)
        if not title:
            continue

        body_parts = []
        content = soup.select_one("div.entry-content")
        if content:
            lead = content.find("h3")
            if lead:
                body_parts.append(lead.get_text(" ", strip=True))
            for p in content.select("> p"):
                t = p.get_text(" ", strip=True)
                if t:
                    body_parts.append(t)

        body = "\n".join(body_parts)

        out.append({
            "source": "Global New Light of Myanmar",
            "title": unicodedata.normalize("NFC", title),
            "url": url,
            "date": art_date.isoformat(),
            "body": body,
        })

    return out

# ===== 単体翻訳（既存プロンプト流用） =====
def translate_title_only(item: Dict, *, model: str = "gemini-2.5-flash") -> str:
    """
    build_prompt(..., skip_filters=True) を使い、タイトルのみ日本語化。
    生成結果から「【タイトル】 …」を抽出。失敗時は原題を返す。
    """
    payload = {
        "source": item.get("source") or "",
        "url": item.get("url") or "",
        "title": item.get("title") or "",
        "body": item.get("body") or "",
    }
    try:
        prompt = build_prompt(payload, skip_filters=True, body_max=0)
        resp = call_gemini_with_retries(client_summary, prompt, model=model)
        text = (resp.text or "").strip()
        lines = [unicodedata.normalize("NFC", ln).strip() for ln in text.splitlines() if ln.strip()]
        idx = next((i for i, ln in enumerate(lines) if re.match(r"^【\s*タイトル\s*】", ln)), None)
        if idx is not None:
            m = re.match(r"^【\s*タイトル\s*】\s*(.*)$", lines[idx])
            inline = (m.group(1) or "").strip() if m else ""
            lines.pop(idx)
            if inline:
                return inline.lstrip(":：").strip() or payload["title"]
            if idx < len(lines):
                return lines[idx].strip() or payload["title"]
        return payload["title"]
    except Exception as e:
        print(f"[translate] fail for {payload.get('url')}: {e}")
        return payload["title"]

# ===== バッチ翻訳 =====
def translate_titles_in_batch(items: List[Dict], *, model: str = "gemini-2.5-flash") -> List[str]:
    """
    items: dict の配列（source/title/url程度）。同数の日本語訳タイトル配列を返す。
    失敗時は空リストを返し、呼び出し側でフォールバック。
    """
    numbered = []
    for i, it in enumerate(items, 1):
        src = (it.get("source") or "").replace("\n", " ").strip()
        ttl = (it.get("title") or "").replace("\n", " ").strip()
        url = (it.get("url") or "").strip()
        numbered.append(f'{i}. [source:{src}] [url:{url}] title="{ttl}"')

    sys_prompt = (
        "あなたは報道見出しの専門翻訳者です。以下の複数の英語/ビルマ語の見出しタイトルを、"
        "自然で簡潔な日本語見出しに翻訳してください。固有名詞は一般的な日本語表記を優先し、"
        "意訳しすぎず要点を保ち、記号の乱用は避けます。"
        "出力は厳密な JSON のみで、説明文やコードフェンスは一切出力しないでください。"
        'フォーマットは {"results":[{"i":1,"ja":"..."},...]} です。'
    )
    user_prompt = "翻訳対象:\n" + "\n".join(numbered) + "\n\n" + \
        '出力は次の JSON のみ: {"results":[{"i":1,"ja":"..."},{"i":2,"ja":"..."}]}\n' \
        "注意: i は入力番号、ja は日本語訳タイトル（見出しとして自然な文言）。"

    try:
        resp = call_gemini_with_retries(
            client_summary,
            f"{sys_prompt}\n\n{user_prompt}",
            model=model,
        )
        text = (resp.text or "").strip()

        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            raise ValueError("no JSON braces")
        blob = text[start : end + 1]

        data = json.loads(blob)
        results = data.get("results", [])
        mapping = {int(r.get("i")): (r.get("ja") or "").strip() for r in results if "i" in r and "ja" in r}
        out = []
        for i in range(1, len(items) + 1):
            out.append(mapping.get(i, ""))  # 欠けは空文字
        return out
    except Exception as e:
        print(f"[batch-translate] fail ({len(items)} items): {e}")
        return []

# ===== レートリミッタ =====
class RateLimiter:
    """リクエスト/分 と 最小インターバル を同時に満たすための単純なスライディングウィンドウ"""
    def __init__(self, rpm: int, min_interval: float, jitter: float = 0.0):
        self.rpm = max(1, int(rpm))
        self.min_interval = max(0.0, float(min_interval))
        self.jitter = max(0.0, float(jitter))
        self._win = deque()
        self._last = 0.0

    def wait(self):
        now = time.time()
        if self._last:
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
        window = 60.0
        now = time.time()
        while self._win and now - self._win[0] >= window:
            self._win.popleft()
        if len(self._win) >= self.rpm:
            sleep_for = window - (now - self._win[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)
        if self.jitter > 0:
            time.sleep(random.uniform(0.0, self.jitter))
        self._last = time.time()
        self._win.append(self._last)

# ===== メイン =====
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="2025-08-23", help="MMT基準の開始日 (YYYY-MM-DD)")
    parser.add_argument("--out", type=str, default="articles_since_2025-08-23_MMT.csv", help="出力CSVパス")

    # レート制御（CLI > 環境変数 > 既定）
    parser.add_argument("--rpm", type=int, default=int(os.getenv("GEMINI_REQS_PER_MIN", "30")),
                        help="Requests per minute limit (default: env GEMINI_REQS_PER_MIN or 30)")
    parser.add_argument("--min-interval", type=float, default=float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "0.5")),
                        help="Minimum seconds between requests (default: env GEMINI_MIN_INTERVAL_SEC or 0.5)")
    parser.add_argument("--jitter", type=float, default=float(os.getenv("GEMINI_JITTER_SEC", "0.0")),
                        help="Random jitter [0..jitter] seconds per request (default: env GEMINI_JITTER_SEC or 0.0)")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("GEMINI_BATCH_SIZE", "20")),
                        help="Titles per request for batch translation (default: env GEMINI_BATCH_SIZE or 20)")

    args = parser.parse_args(argv)

    # MMT 今日
    today_mmt = datetime.now(MMT).date()
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    except ValueError:
        print("ERROR: --start は YYYY-MM-DD 形式で指定してください。")
        return 2
    if start_date > today_mmt:
        print("ERROR: --start は今日(MMT)以前を指定してください。")
        return 2

    all_rows: List[Dict] = []

    for d in daterange_mmt(start_date, today_mmt):
        print(f"=== {d.isoformat()} (MMT) ===")
        # Irrawaddy（キーワード絞り込み前の収集版: ローカル関数）
        try:
            irw = collect_irrawaddy_all_for_date(d, debug=False)
        except Exception as e:
            print(f"[irrawaddy] fail: {e}")
            irw = []
        all_rows.extend(irw)
        # BBC / Khit Thit / DVB / Mizzima
        all_rows.extend(collect_bbc_all_for_date(d))
        all_rows.extend(collect_khitthit_all_for_date(d, max_pages=15))
        all_rows.extend(collect_dvb_all_for_date(d))  # DVB 内部を 1〜15 ページ対応済み
        all_rows.extend(collect_mizzima_all_for_date(d, max_pages=15))

    # 重複除去の前後をログ
    print(f"Dedup by URL: before={len(all_rows)}")
    all_rows = deduplicate_by_url(all_rows)
    print(f"Dedup by URL: after={len(all_rows)}")

    # レート制御ログ
    print(f"Rate limit: rpm={args.rpm}, min_interval={args.min_interval}s, jitter<= {args.jitter}s")
    print(f"Batch translation size: {args.batch_size}")

    # バッチ翻訳
    limiter = RateLimiter(args.rpm, args.min_interval, args.jitter)
    pending_idx = [i for i, it in enumerate(all_rows) if not it.get("title_ja")]
    bs = max(1, int(args.batch_size))
    for s in range(0, len(pending_idx), bs):
        idxs = pending_idx[s : s + bs]
        batch_items = [all_rows[i] for i in idxs]
        limiter.wait()
        ja_list = translate_titles_in_batch(batch_items)
        if len(ja_list) != len(batch_items) or any(j == "" for j in ja_list):
            print(f"[batch-translate] fallback single: {len(batch_items)} items")
            for k, item in zip(idxs, batch_items):
                limiter.wait()
                all_rows[k]["title_ja"] = translate_title_only(item)
        else:
            for k, ja in zip(idxs, ja_list):
                all_rows[k]["title_ja"] = ja

    # CSV 出力（UTF-8 BOM）
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["メディア名", "日本語タイトル", "発行日(MMT)", "URL"])
        for a in all_rows:
            dd = a.get("date") or ""
            if "T" in dd:
                try:
                    dt = datetime.fromisoformat(dd)
                    dd = dt.astimezone(MMT).date().isoformat()
                except Exception:
                    pass
            writer.writerow([a.get("source") or "", a.get("title_ja") or "", dd, a.get("url") or ""])
    print(f"✅ CSV written: {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
