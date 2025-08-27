
# -*- coding: utf-8 -*-
"""
export_all_articles_to_csv.py

新規処理: 添付の fetch_articles.py を利用しつつ、
・各メディアの「キーワード絞り込み前」の記事一覧を取得
・ミャンマー時間で 2025-08-23(土) 以降の記事に限定
・タイトルを gemini-2.5-flash（同一プロンプト規約）で日本語に翻訳
・CSV (UTF-8 BOM付き) に A:メディア名 / B:日本語タイトル / C:発行日(YYYY-MM-DD, MMT) / D:URL を出力
・Gemini 無料枠対策: リクエスト間隔のレートリミット（RPM/最小インターバル/ジッター）を導入

使い方(例):
  python export_all_articles_to_csv.py --start 2025-08-23 --out articles_since_2025-08-23.csv \
    --rpm 20 --min-interval 1.5 --jitter 0.3

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

# 追加インポート（レート制御）
import os
import random
from collections import deque

# --- 添付コードからインポート（そのまま利用） ---
from fetch_articles import (
    MMT,
    build_prompt,
    call_gemini_with_retries,
    client_summary,
    deduplicate_by_url,
)

# ---- Utilities ----
import re
import json
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date


def daterange_mmt(start: date, end: date) -> Iterable[date]:
    """MMT日付で start から end まで（両端含む）の date を返す"""
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


# ===== Irrawaddy =====
# 既存の get_irrawaddy_articles_for はキーワードフィルタを行わない設計なので流用
from fetch_articles import get_irrawaddy_articles_for  # noqa: E402


# ===== BBC Burmese (RSS) =====
def collect_bbc_all_for_date(target_date_mmt: date) -> List[Dict]:
    """
    BBC Burmese は RSS を使い、UTC→MMT 変換後に対象日一致だけに絞る。
    キーワード絞り込みは一切行わない。
    戻り値: [{source, title, url, date}]
    """
    rss_url = "https://feeds.bbci.co.uk/burmese/rss.xml"
    session = requests.Session()
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
            title = unicodedata.normalize("NFC", title_tag.text or "").strip()
            url = (link_tag.text or "").strip()
            if not (title and url):
                continue
            out.append(
                {
                    "source": "BBC Burmese",
                    "title": title,
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": "",  # 翻訳はタイトルのみで行うため空でOK
                }
            )
        except Exception:
            continue
    return out


# ===== Khit Thit Media =====
def collect_khitthit_all_for_date(target_date_mmt: date, max_pages: int = 3) -> List[Dict]:
    """
    get_khit_thit_media_articles_from_category の「前段」を再実装。
    カテゴリURL群のリストは fetch_articles.py の定義に合わせる。
    ・本文取得とハッシュタグ除去は踏襲
    ・キーワード絞り込みは実施しない
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

    from fetch_articles import fetch_with_retry, extract_paragraphs_with_wait  # reuse

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

            # 日付 (meta property="article:published_time") を MMT にして一致判定
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
    get_dvb_articles_for の前段を再実装。
    ・/category/8/news の 1ページ目 + ?page=2 を走査
    ・記事側で MMT 日付一致だけ確認
    ・キーワード絞り込みは実施しない
    """
    BASE = "https://burmese.dvb.no"
    CATEGORY_PATHS = ["/category/8/news"]

    from fetch_articles import fetch_with_retry  # reuse

    collected_urls = set()

    for path in CATEGORY_PATHS:
        for page in (None, 2):
            url = f"{BASE}{path}" if page is None else f"{BASE}{path}?page={page}"
            try:
                res = fetch_with_retry(url)
            except Exception as e:
                print(f"[dvb] list fetch fail {url}: {e}")
                continue

            soup = BeautifulSoup(res.content, "html.parser")

            def _norm_url(href: str) -> str:
                if href.startswith("http"):
                    return href
                return BASE + href

            if path == "/category/8/news":
                cards = soup.select("div.listing_content.item.item_length-1 a[href]")
                if not cards:
                    cards = soup.select("div.listing_content.item.item_length-2 a[href]")
                if not cards:
                    cards = soup.select("div.listing_content.item a[href]")
                for a in cards:
                    href = a.get("href")
                    if href:
                        collected_urls.add(_norm_url(href))

    results: List[Dict] = []
    for url in collected_urls:
        try:
            res = fetch_with_retry(url)
            soup = BeautifulSoup(res.content, "html.parser")

            # 発行日時 (UTC) → MMT
            meta = soup.find("meta", property="article:published_time")
            if not meta or not meta.has_attr("content"):
                continue
            dt = datetime.fromisoformat(meta["content"]).astimezone(MMT)
            if dt.date() != target_date_mmt:
                continue

            # タイトルと本文
            t = soup.find("h1") or soup.find("title")
            title = (t.get_text(strip=True) if t else "").strip()
            body_ps = soup.select(".full_content p")
            body = "\n".join(p.get_text(strip=True) for p in body_ps).strip()

            if not title:
                continue

            results.append(
                {
                    "source": "DVB",
                    "title": unicodedata.normalize("NFC", title),
                    "url": url,
                    "date": target_date_mmt.isoformat(),
                    "body": unicodedata.normalize("NFC", body),
                }
            )
        except Exception as e:
            print(f"[dvb] article fail {url}: {e}")
            continue

    return results


# ===== Mizzima =====
def collect_mizzima_all_for_date(target_date_mmt: date, max_pages: int = 3) -> List[Dict]:
    """
    get_mizzima_articles_from_category の前段を再実装。
    ・base_url と category_path は fetch_articles.py の __main__ と同じ指定を採用
    ・本文取得まで行うが、キーワード絞り込みは行わない
    """
    base_url = "https://bur.mizzima.com"
    category_path = (
        "/category/%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8/"
        "%e1%80%99%e1%80%bc%e1%80%94%e1%80%ba%e1%80%99%e1%80%ac"
        "%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8"
    )

    EXCLUDE_TITLE_KEYWORDS = [
        "နွေဦးတော်လှန်ရေး နေ့စဉ်မှတ်စု",  # 春の革命日誌
        "ဓာတ်ပုံသတင်း",  # 写真ニュース
    ]

    article_urls: List[str] = []
    session = requests.Session()
    for page_num in range(1, max_pages + 1):
        url = (
            f"{base_url}{category_path}"
            if page_num == 1
            else f"{base_url}{category_path}/page/{page_num}/"
        )
        try:
            res = session.get(url, timeout=10)
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.content, "html.parser")
            links = [
                a["href"]
                for a in soup.select("main.site-main article a.post-thumbnail[href]")
            ]
            article_urls.extend(links)
        except Exception as e:
            print(f"[mizzima] list fail {url}: {e}")
            continue

    from fetch_articles import fetch_with_retry  # reuse

    results: List[Dict] = []
    for url in article_urls:
        try:
            res = fetch_with_retry(url)
            soup = BeautifulSoup(res.content, "html.parser")

            # 発行日時 (UTC) → MMT
            meta_tag = soup.find("meta", property="article:published_time")
            if not meta_tag or not meta_tag.has_attr("content"):
                continue
            dt = datetime.fromisoformat(meta_tag["content"]).astimezone(MMT)
            if dt.date() != target_date_mmt:
                continue

            # タイトル
            title_tag = soup.find("meta", attrs={"property": "og:title"})
            title = (title_tag["content"].strip() if title_tag and title_tag.has_attr("content") else "")
            if not title:
                continue
            title_nfc = unicodedata.normalize("NFC", title)

            # タイトルの除外キーワード
            if any(kw in title_nfc for kw in EXCLUDE_TITLE_KEYWORDS):
                continue

            # 本文
            content_div = soup.find("div", class_="entry-content")
            if not content_div:
                continue
            paras = []
            for p in content_div.find_all("p"):
                # Related Posts より上だけ
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


# ===== タイトル翻訳（プロンプトは添付コードの build_prompt を流用、Step1/2はスキップ） =====
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
        # 余分行を除去して「【タイトル】」行を厳格抽出
        lines = [unicodedata.normalize("NFC", ln).strip() for ln in text.splitlines() if ln.strip()]
        # A) 同一行 / B) 次行採用 のルール
        idx = next((i for i, ln in enumerate(lines) if re.match(r"^【\s*タイトル\s*】", ln)), None)
        if idx is not None:
            m = re.match(r"^【\s*タイトル\s*】\s*(.*)$", lines[idx])
            inline = (m.group(1) or "").strip() if m else ""
            lines.pop(idx)
            if inline:
                return inline.lstrip(":：").strip() or payload["title"]
            if idx < len(lines):
                return lines[idx].strip() or payload["title"]
        # フォールバック
        return payload["title"]
    except Exception as e:
        print(f"[translate] fail for {payload.get('url')}: {e}")
        return payload["title"]


# ===== レートリミッタ =====
class RateLimiter:
    """リクエスト/分 と 最小インターバル を同時に満たすための単純なスライディングウィンドウ"""
    def __init__(self, rpm: int, min_interval: float, jitter: float = 0.0):
        self.rpm = max(1, int(rpm))
        self.min_interval = max(0.0, float(min_interval))
        self.jitter = max(0.0, float(jitter))
        self._win = deque()   # 直近60秒の呼び出しtimestamp
        self._last = 0.0

    def wait(self):
        now = time.time()

        # 連続呼び出しの最小インターバル
        if self._last:
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)

        # 1分あたりの上限
        window = 60.0
        now = time.time()
        while self._win and now - self._win[0] >= window:
            self._win.popleft()
        if len(self._win) >= self.rpm:
            sleep_for = window - (now - self._win[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)

        # ランダムジッター（バースト回避）
        if self.jitter > 0:
            time.sleep(random.uniform(0.0, self.jitter))

        # 記録
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

    all_rows: List[Dict] = []  # 後で URL で重複排除

    for d in daterange_mmt(start_date, today_mmt):
        print(f"=== {d.isoformat()} (MMT) ===")

        # Irrawaddy（既存関数をそのまま利用）
        try:
            irw = get_irrawaddy_articles_for(d, debug=False)
        except Exception as e:
            print(f"[irrawaddy] fail: {e}")
            irw = []
        all_rows.extend(irw)

        # BBC
        all_rows.extend(collect_bbc_all_for_date(d))

        # Khit Thit Media
        all_rows.extend(collect_khitthit_all_for_date(d, max_pages=3))

        # DVB
        all_rows.extend(collect_dvb_all_for_date(d))

        # Mizzima
        all_rows.extend(collect_mizzima_all_for_date(d, max_pages=3))

    # URL で重複排除
    print(f"Dedup by URL: before={len(all_rows)}")
    all_rows = deduplicate_by_url(all_rows)
    print(f"Dedup by URL: after={len(all_rows)}")

    # レート制御の設定ログ
    print(f"Rate limit: rpm={args.rpm}, min_interval={args.min_interval}s, jitter<= {args.jitter}s")

    # タイトル翻訳（1件ずつ）
    limiter = RateLimiter(args.rpm, args.min_interval, args.jitter)
    for item in all_rows:
        limiter.wait()
        jp = translate_title_only(item)
        item["title_ja"] = jp

    # CSV 出力（UTF-8 BOM付きで文字化けを防止）
    # A: メディア名 / B: 日本語タイトル / C: 発行日 / D: URL
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["メディア名", "日本語タイトル", "発行日(MMT)", "URL"])
        for a in all_rows:
            dd = a.get("date") or ""
            if "T" in dd:
                try:
                    # ISO文字列→MMT→date
                    dt = datetime.fromisoformat(dd)
                    dd = dt.astimezone(MMT).date().isoformat()
                except Exception:
                    pass
            writer.writerow([a.get("source") or "", a.get("title_ja") or "", dd, a.get("url") or ""])

    print(f"✅ CSV written: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
