import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date, timezone
from dateutil.parser import parse as parse_date
import re
# Chat GPT
# from openai import OpenAI, OpenAIError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import sys
from email import policy  # ← 追加
from email.header import Header  # ← 追加必要
from email.message import EmailMessage
from email.policy import SMTPUTF8
from email.utils import formataddr
import unicodedata
from google import genai
from google.api_core.exceptions import GoogleAPICallError
from collections import defaultdict
import time
import json
import pprint

# 記事重複排除ロジック(BERT埋め込み版)のライブラリインポート
from sentence_transformers import SentenceTransformer, util

# Gemini本番用
# client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# GeminiTEST用
client = genai.Client(api_key=os.getenv("GEMINI_TEST_API_KEY"))

# Chat GPT
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ミャンマー標準時 (UTC+6:30)
MMT = timezone(timedelta(hours=6, minutes=30))

# 今日の日付
# ニュースの速報性重視で今日分のニュース配信の方針
def get_today_date_mmt():
    # 本番用、今日の日付
    # now_mmt = datetime.now(MMT)
    # テスト用、昨日の日付にする
    now_mmt = datetime.now(MMT) - timedelta(days=1)
    return now_mmt.date()

# 共通キーワードリスト（全メディア共通で使用する）
NEWS_KEYWORDS = [
    # ミャンマー（国名・現行名称）
    "မြန်မာ", "မြန်မာ့", "Myanmar", "myanmar",
    
    # ビルマ（旧国名・通称）
    "ဗမာ", "Burma", "burma",
    
    # アウンサンスーチー（Aung San Suu Kyi）
    "အောင်ဆန်းစုကြည်", "Aung San Suu Kyi", "aung san suu kyi",
    
    # ミンアウンフライン（Min Aung Hlaing）
    "မင်းအောင်လှိုင်", "Min Aung Hlaing", "min aung hlaing",
    
    # チャット（Kyat）
    "Kyat", "kyat",
    
    # 徴兵制（Conscription / Military Draft）, 徴兵, 兵役
    "Conscription", "conscription", "Military Draft", "military draft", "military service", "military service", "စစ်တပ်ဝင်ခေါ်ရေး", "စစ်မှုထမ်း", "အတင်းတပ်ဝင်ခေါ်ခြင်း", "တပ်ဝင်ခေါ် "
]

# Unicode正規化（NFC）を適用
NEWS_KEYWORDS = [unicodedata.normalize('NFC', kw) for kw in NEWS_KEYWORDS]

# チャットは数字に続くもののみ（通貨判定）
KYAT_PATTERN = re.compile(
    r'(?<=[0-9၀-၉])[\s,\.]*(?:သောင်း|သိန်း|သန်း)?\s*ကျပ်'
)

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
    return ''.join(c for c in html if unicodedata.category(c)[0] != 'C')

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


# Mizzimaカテゴリーページ巡回で取得
def get_mizzima_articles_from_category(date_obj, base_url, source_name, category_path, max_pages=3):
    # ==== ローカル定数 Mizzima除外対象キーワード（タイトル用）====
    EXCLUDE_TITLE_KEYWORDS = [
        # 春の革命日誌
        "နွေဦးတော်လှန်ရေး နေ့စဉ်မှတ်စု",
        # 写真ニュース
        "ဓာတ်ပုံသတင်း"
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
            links = [a['href'] for a in soup.select("main.site-main article a.post-thumbnail[href]")]
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
            title_nfc = unicodedata.normalize('NFC', title)
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
            body_text = unicodedata.normalize('NFC', body_text)

            if not body_text.strip():
                continue

            # キーワード判定は正規化済みタイトルで行う
            if not any_keyword_hit(title, body_text):
                continue

            filtered_articles.append({
                "source": source_name,
                "url": url,
                "title": title,
                "date": article_date.isoformat()
            })

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return filtered_articles

# BCCはRSSあるのでそれ使う
def get_bbc_burmese_articles_for(target_date_mmt):
    # ==== ローカル定数 ====
    NOISE_PATTERNS = [
        r"BBC\s*News\s*မြန်မာ",  # 固定署名（Burmese表記）
        r"BBC\s*Burmese"        # 英語表記
    ]

    # ==== ローカル関数 ====
    def _remove_noise_phrases(text: str) -> str:
        """BBC署名などのノイズフレーズを除去"""
        if not text:
            return text
        for pat in NOISE_PATTERNS:
            text = re.sub(pat, "", text, flags=re.IGNORECASE)
        return text.strip()

    # あるテキスト中でキーワードがどこにヒットしたかを返す（周辺文脈つき）
    def _find_hits(text: str, keywords):
        hits = []
        for kw in keywords:
            start = 0
            while True:
                i = text.find(kw, start)
                if i == -1:
                    break
                s = max(0, i-30); e = min(len(text), i+len(kw)+30)
                ctx = text[s:e].replace("\n", " ")
                hits.append({"kw": kw, "pos": i, "ctx": ctx})
                start = i + len(kw)
        return hits

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

        title = (item.find("title") or {}).get_text(strip=True) if item.find("title") else ""
        link = (item.find("link") or {}).get_text(strip=True) if item.find("link") else ""
        if not link:
            continue

        try:
            article_res = session.get(link, timeout=10)
            article_res.raise_for_status()
            article_soup = BeautifulSoup(article_res.content, "html.parser")

            # ===== ここで除外セクションをまとめて削除 =====
            # 記事署名やメタ情報
            for node in article_soup.select('section[role="region"][aria-labelledby="article-byline"]'):
                node.decompose()
            # 「おすすめ／最も読まれた」ブロック
            for node in article_soup.select('section[data-e2e="recommendations-heading"][role="region"]'):
                node.decompose()
            # ついでにヘッダー/ナビ/フッター等のノイズも落としておく（任意）
            for node in article_soup.select('header[role="banner"], nav[role="navigation"], footer[role="contentinfo"], aside'):
                node.decompose()
            # ============================================

            # 本文は main 内の <p> に限定
            main = article_soup.select_one('main[role="main"]') or article_soup
            paragraphs = [p.get_text(strip=True) for p in main.find_all('p')]
            # 空行やノイズを削る
            paragraphs = [t for t in paragraphs if t]
            body_text = "\n".join(paragraphs)

            # ミャンマー文字の合成差異を避けるため NFC 正規化
            title_nfc = unicodedata.normalize('NFC', title)
            title_nfc = _remove_noise_phrases(title_nfc)
            body_text_nfc = unicodedata.normalize('NFC', body_text)
            body_text_nfc = _remove_noise_phrases(body_text_nfc)

            # キーワード判定
            if not any_keyword_hit(title_nfc, body_text_nfc):
                print(f"SKIP: no keyword hits → {link} | TITLE: {title_nfc}")
                continue

            # # === デバッグ: 判定前にタイトル/本文の要約を出す ===
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
            articles.append({
                "title": title_nfc,
                "url": link,
                "date": pub_date_mmt.isoformat(),
            })

        except Exception as e:
            print(f"❌ 記事取得/解析エラー: {e}")
            continue

    return articles

# khit_thit_ediaカテゴリーページ巡回で取得
def get_khit_thit_edia_articles_from_category(date_obj, max_pages=3):
    base_url="https://yktnews.com/category/news/"
    article_urls = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}page/{page}/" if page > 1 else base_url
        print(f"Fetching {url}")
        res = fetch_with_retry(url)
        soup = BeautifulSoup(res.content, "html.parser")

        # 記事リンク抽出
        entry_links = soup.select('p.entry-title.td-module-title a[href]')
        page_article_urls = [a['href'] for a in entry_links if a.has_attr('href')]
        article_urls.extend(page_article_urls)

    filtered_articles = []
    for url in article_urls:
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

            # 本文取得 (khit_thit_edia用パターン)
            paragraphs = soup_article.select("div.tdb-block-inner p")
            if not paragraphs:
                paragraphs = soup_article.select("div.tdb_single_content p")
            if not paragraphs:
                paragraphs = soup_article.select("article p")
            if not paragraphs:
                paragraphs = soup_article.find_all("p")
            
            paragraphs = extract_paragraphs_with_wait(soup_article)
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            body_text = unicodedata.normalize('NFC', body_text)

            if not body_text.strip():
                continue  # 本文が空ならスキップ

            if not any_keyword_hit(title, body_text):
                continue  # キーワード無しは除外

            filtered_articles.append({
                "url": url,
                "title": title,
                "date": date_obj.isoformat()
            })

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return filtered_articles

# irrawaddy
def get_irrawaddy_articles_for(date_obj, debug=True):
    """
    指定の Irrawaddy カテゴリURL群（相対パス）を1回ずつ巡回し、
    MMTの指定日(既定: 今日)かつ any_keyword_hit にヒットする記事のみ返す。

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
        # "/category/election-2020", # 2021年で更新止まってる
        "/category/Features",
        "/category/Opinion",
        "/category/Opinion/editorial",
        "/category/Opinion/commentary",
        "/category/Opinion/guest-column",
        "/category/Opinion/analysis",
        # "/category/Opinion/letters", # 2014年で更新止まってる
        "/category/in-person",
        "/category/in-person/interview",
        "/category/in-person/profile",
        # "/category/Dateline", # 2020年で更新止まってる
        "/category/Specials",
        "/category/specials/women",
        # "/category/specials/places-in-history", # 2020年で更新止まってる
        # "/category/specials/on-this-day", # 2023年で更新止まってる
        "/category/from-the-archive",
        # "/category/Specials/myanmar-covid-19", # 2022年で更新止まってる
        "/category/Specials/myanmar-china-watch",
        # "/category/Lifestyle", # 2020年で更新止まってる
        # "/category/Travel", # 2020年で更新止まってる
        # "/category/Lifestyle/Food", # 2020年で更新止まってる
        # "/category/Lifestyle/fashion-design", # 2019年で更新止まってる
        # "/category/photo", # 2016年で更新止まってる
        # "/category/photo-essay", # 2021年で更新止まってる
    ]
    BASE = "https://www.irrawaddy.com"
    EXCLUDE_PREFIXES = ["/category/news/asia", "/category/news/world"]  # 先頭一致・大小無視

    # ==== 正規化・ユニーク化・除外 ====
    norm = lambda p: re.sub(r"/{2,}", "/", p.strip())
    paths, seen = [], set()
    for p in CATEGORY_PATHS_RAW:
        q = norm(p)
        if any(q.lower().startswith(x) for x in EXCLUDE_PREFIXES):
            continue
        if q not in seen:
            seen.add(q)
            paths.append(q)

    # ==== ローカル関数 ====
    def _norm_text(text: str) -> str:
        return unicodedata.normalize('NFC', text)

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
            "widget", "widget_jnews_popular",
            "jeg_postblock_5",
            "jnews_related_post_container",
            "jeg_footer_primary",
        }
        for anc in node.parents:
            classes = anc.get("class", [])
            if any(c in excluded for c in classes):
                return True
        return False

    def _extract_body_irrawaddy(soup):
        # <div class="content-inner "> 配下の <p>のみ（除外ブロック配下は除外）
        paragraphs = []
        content_inners = soup.select("div.content-inner")
        if not content_inners:
            content_inners = [div for div in soup.find_all("div")
                            if "content-inner" in (div.get("class") or [])]
        for root in content_inners:
            for p in root.find_all("p"):
                if _is_excluded_by_ancestor(p):
                    continue
                txt = p.get_text(strip=True)
                if txt:
                    paragraphs.append(_norm_text(txt))
        return "\n".join(paragraphs).strip()
    
    def _fetch_with_retry_irrawaddy(url, retries=3, wait_seconds=2, session=None):
        """
        Irrawaddy専用フェッチャ：最初から cloudscraper で取得し、403/429/503 は指数バックオフで再試行。
        最後の手段として requests にフォールバック（ほぼ到達しない想定）。
        """
        import random
        try:
            import cloudscraper
        except ImportError:
            raise RuntimeError("cloudscraper が必要です。pip install cloudscraper を実行してください。")

        sess = session or requests.Session()

        UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36")
        HEADERS = {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.irrawaddy.com/",
            "Connection": "keep-alive",
        }

        # cloudscraper を最初に使う（既存 Session をラップしてクッキー共有）
        scraper = cloudscraper.create_scraper(
            sess=sess,
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )

        for attempt in range(retries):
            try:
                r = scraper.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
                print(f"[fetch-cs] {attempt+1}/{retries}: HTTP {r.status_code} len={len(getattr(r,'text',''))} → {url}")
                if r.status_code == 200 and getattr(r, "text", "").strip():
                    return r
                if r.status_code in (403, 429, 503):
                    time.sleep(wait_seconds * (2 ** attempt) + random.uniform(0, 0.8))
                    continue
                break
            except Exception as e:
                print(f"[fetch-cs] {attempt+1}/{retries} EXC: {e} → {url}")
                time.sleep(wait_seconds * (2 ** attempt) + random.uniform(0, 0.8))

        # 非常用フォールバック（ほぼ不要）。成功すれば返す。
        try:
            r2 = sess.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            print(f"[fetch-rq] final: HTTP {r2.status_code} len={len(r2.text)} → {url}")
            if r2.status_code == 200 and r2.text.strip():
                return r2
        except Exception as e:
            print(f"[fetch-rq] EXC final: {e} → {url}")

        raise Exception(f"Failed to fetch {url} after {retries} attempts.")

    # 2) 簡易ロガー（消す時はこの1行と dbg(...) を消すだけ）
    dbg = (lambda *a, **k: print(*a, **k)) if debug else (lambda *a, **k: None)

    results = []
    seen_urls = set()
    candidate_urls = []

    # ✅ これを追加（または入れ直す）
    _shown_parsefail = 0
    _shown_mismatch  = 0

    # ==== 1) カテゴリ巡回 ====
    for rel_path in paths:
        url = f"{BASE}{rel_path}"
        print(f"Fetching {url}")
        try:
            res = _fetch_with_retry_irrawaddy(url, session=session)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(res.content, "html.parser")
        wrapper = soup.select_one("div.jnews_category_content_wrapper")
        scope = wrapper if wrapper else soup

        links = scope.select("div.jeg_postblock_content .jeg_meta_date a[href]")
        if not links:
            links = scope.select(".jeg_post_meta .jeg_meta_date a[href]")
        if not links:
            links = [a for a in scope.select("div.jeg_postblock_content a[href]")
                    if a.find("i", class_="fa fa-clock-o")]

        # デバッグ：何件拾えたか＆先頭2件の中身
        dbg(f"[cat] date-links={len(links)} @ {url}")
        for a in links[:2]:
            _txt = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
            dbg("   →", _txt, "|", a.get("href"))

        if not links:
            dbg(f"[cat] no date links @ {url}")
            continue

        for a in links:
            href = a.get("href")
            raw = a.get_text(" ", strip=True)
            try:
                shown_date = _parse_category_date_text(raw)
            except Exception:
                if _shown_parsefail < 3:
                    dbg("[cat] date-parse-fail:", re.sub(r"\s+", " ", raw)[:120])
                    _shown_parsefail += 1
                continue

            if shown_date == date_obj:
                if href and href not in seen_urls:
                    candidate_urls.append(href)
                    seen_urls.add(href)
            else:
                if _shown_mismatch < 3:
                    dbg("[cat] date-mismatch:", shown_date, "target:", date_obj, "→", href)
                    _shown_mismatch += 1

    dbg(f"[cat] candidates={len(candidate_urls)}")

    # ==== 2) 記事確認 ====
    for url in candidate_urls:
        try:
            res_article = _fetch_with_retry_irrawaddy(url, session=session)
        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

        soup_article = BeautifulSoup(res_article.content, "html.parser")

        meta_date = _article_date_from_meta_mmt(soup_article)
        if meta_date is None:
            dbg("[art] meta-missing:", url)
            continue
        if meta_date != date_obj:
            dbg("[art] meta-mismatch:", meta_date, "target:", date_obj, "→", url)
            continue

        title = _extract_title(soup_article)
        if not title:
            dbg("[art] title-missing:", url)
            continue

        body = _extract_body_irrawaddy(soup_article)
        if not body:
            dbg("[art] body-empty:", url)
            continue

        if not any_keyword_hit(title, body):
            dbg("[art] keyword-not-hit:", url)
            continue

        results.append({
            "url": url,
            "title": title,
            "date": date_obj.isoformat(),
        })

    dbg(f"[final] kept={len(results)}")

    # results = []
    # seen_urls = set()
    # candidate_urls = []

    # # ==== 1) 各カテゴリURLを1回ずつ巡回 → 当日候補抽出 ====
    # for rel_path in paths:
    #     url = f"{BASE}{rel_path}"
    #     print(f"Fetching {url}")
    #     try:
    #         res = _fetch_with_retry_irrawaddy(url, session=session)
    #     except Exception as e:
    #         print(f"Error fetching {url}: {e}")
    #         continue

    #     soup = BeautifulSoup(res.content, "html.parser")
    #     wrapper = soup.select_one("div.jnews_category_content_wrapper")
    #     scope = wrapper if wrapper else soup

    #     links = scope.select("div.jeg_postblock_content .jeg_meta_date a[href]")
    #     if not links:
    #         # フォールバック：時計アイコンを含む a
    #         links = [a for a in scope.select("div.jeg_postblock_content a[href]")
    #                 if a.find("i", class_="fa fa-clock-o")]

    #     for a in links:
    #         if not a.find("i", class_="fa fa-clock-o"):
    #             continue
    #         href = a.get("href")
    #         if not href or href in seen_urls:
    #             continue
    #         try:
    #             shown_date = _parse_category_date_text(a.get_text(" ", strip=True))
    #         except Exception:
    #             continue
    #         if shown_date == date_obj:
    #             candidate_urls.append(href)
    #             seen_urls.add(href)

    # # ==== 2) 候補記事で厳密確認（meta日付/本文/キーワード） ====
    # for url in candidate_urls:
    #     try:
    #         res_article = _fetch_with_retry_irrawaddy(url, session=session)
    #         soup_article = BeautifulSoup(res_article.content, "html.parser")

    #         if _article_date_from_meta_mmt(soup_article) != date_obj:
    #             continue

    #         title = _extract_title(soup_article)
    #         if not title:
    #             continue

    #         body = _extract_body_irrawaddy(soup_article)
    #         if not body:
    #             continue

    #         if not any_keyword_hit(title, body):
    #             continue

    #         results.append({
    #             "url": url,
    #             "title": title,
    #             "date": date_obj.isoformat(),
    #         })
    #     except Exception as e:
    #         print(f"Error processing {url}: {e}")
    #         continue

    return results

# 同じURLの重複削除
def deduplicate_by_url(articles):
    seen_urls = set()
    unique_articles = []
    for art in articles:
        if art['url'] in seen_urls:
            print(f"🛑 URL Duplicate Removed: {art['source']} | {art['title']} | {art['url']}")
            continue
        seen_urls.add(art['url'])
        unique_articles.append(art)
    return unique_articles

# 翻訳対象キュー
translation_queue = []

def process_and_enqueue_articles(articles, source_name, seen_urls=None):
    if seen_urls is None:
        seen_urls = set()

    queued_items = []
    for art in articles:
        if art['url'] in seen_urls:
            continue
        seen_urls.add(art['url'])

        try:
            res = requests.get(art['url'], timeout=10)
            soup = BeautifulSoup(res.content, "html.parser")
            # 本文pタグ取得 (リトライ付き)
            paragraphs = extract_paragraphs_with_wait(soup, retries=2, wait_seconds=2)
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)

            title_nfc = unicodedata.normalize('NFC', art['title'])
            body_nfc  = unicodedata.normalize('NFC', body_text)

            # ★ここでNEWS_KEYWORDSフィルターをかける
            if not any(keyword in title_nfc or keyword in body_nfc for keyword in NEWS_KEYWORDS):
                continue  # キーワード含まれてなければスキップ

            queued_items.append({
                "source": source_name,
                "url": art["url"],
                "title": art["title"],  # 翻訳前タイトル
                "body": body_text,      # 翻訳前本文
            })
        except Exception as e:
            print(f"Error processing {art['url']}: {e}")
            continue

    translation_queue.extend(queued_items)

# デバック用関数
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
        m = re.search(r'\{.*\}', text, flags=re.DOTALL)
        if m:
            return json.loads(m.group(0))
        raise

def dedupe_articles_with_llm(client, summarized_results):
    """
    summarized_results (list[dict]) を受け取り、重複クラスターごとに1本だけ残した配列を返す。
    返却形式は元と同じ（source, url, title, summary のみ）。
    """
    if not summarized_results:
        return summarized_results

    # ===== ① summarized_results のまま表示 =====
    print("===== DEBUG 1: summarized_results BEFORE DEDUPE =====")
    pprint.pprint(summarized_results, width=120, compact=False)
    print("===== END DEBUG 1 =====\n")

    # LLM入力用に articles を構築（id はURL優先、なければ連番）
    articles = []
    id_map = {}
    for idx, it in enumerate(summarized_results):
        _id = it.get("url") or f"idx-{idx}"
        # 内部用の原本（返却時にそのまま使う）
        id_map[_id] = it

        # 本文相当として summary を渡す（タイトルと本文の両方を比較させる）
        articles.append({
            "id": _id,
            "source": it.get("source"),
            "title": it.get("title"),
            "body": _strip_tags(it.get("summary", "")),
        })

    # ===== LLMに渡すarticlesも確認 =====
    print("===== DEBUG 2: articles SENT TO LLM =====")
    pprint.pprint(articles, width=120, compact=False)
    print("===== END DEBUG 2 =====\n")

    prompt = (
        "あなたはニュースの重複判定フィルタです。\n"
        "目的：タイトルと本文を比較し、「同一の出来事」を報じる記事を重複として束ね、各クラスターから1本だけ残します。\n"
        "出力は必ずJSONのみ。\n\n"
        "判定方針:\n"
        "1) 同一出来事＝「誰」「何を」「どこ/対象」「いつ」の少なくとも3要素が一致し、コア事実が同じ（言い換え・言語差は同一扱い。日付は±14日まで同一扱い）。\n"
        "2) クラスター化：最も一致度が高いクラスターにのみ所属。\n"
        "3) 残す基準：a)固有情報量が多い b)具体性/明瞭さ c)本文が長い d)同点ならsourceの文字列昇順。\n"
        "4) 統合記事は作らない。入力外の事実は加えない。\n\n"
        "入力:\n"
        "{\n  \"articles\": " + json.dumps(articles, ensure_ascii=False) + "\n}\n\n"
        "出力フォーマット（JSONのみ）:\n"
        "{\n"
        "  \"kept\": [\n"
        "    {\"id\": \"<残す記事ID>\", \"cluster_id\": \"<ID>\", \"why\": \"<1-2文>\"}\n"
        "  ],\n"
        "  \"removed\": [\n"
        "    {\"id\": \"<除外記事ID>\", \"duplicate_of\": \"<残した記事ID>\", \"why\": \"<1-2文>\"}\n"
        "  ],\n"
        "  \"clusters\": [\n"
        "    {\"cluster_id\": \"<ID>\", \"member_ids\": [\"<id1>\", \"<id2>\", \"...\"], \"event_key\": \"<出来事の短文>\"}\n"
        "  ]\n"
        "}\n"
    )

    try:
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        data = _safe_json_loads_maybe_extract(resp.text)
        kept_ids = [x.get("id") for x in data.get("kept", []) if x.get("id") in id_map]

        # 元の順序を保ったままフィルタ
        kept_set = set(kept_ids)
        if kept_set:
            filtered = [obj for obj in summarized_results if (obj.get("url") or f"idx-{summarized_results.index(obj)}") in kept_set]
            return filtered

        # うまく判定できなかったら原本を返す
        return summarized_results
    except Exception as e:
        print(f"🛑 Dedupe failed, returning original list: {e}")
        return summarized_results

# 本処理関数
def process_translation_batches(batch_size=10, wait_seconds=60):

    # ⚠️ TEST: Geminiを呼ばず、URLリストだけ返す
    # summarized_results = []
    # for item in translation_queue:
    #     summarized_results.append({
    #         "source": item["source"],
    #         "url": item["url"],
    #         "title": item['title'],
    #         "summary": item['body'][:2000]
    #     })

    summarized_results = []
    for i in range(0, len(translation_queue), batch_size):
        batch = translation_queue[i:i + batch_size]
        print(f"⚙️ Processing batch {i // batch_size + 1}...")

        for item in batch:
            prompt = (
                "次の手順で記事を判定・処理してください。\n\n"
                "Step 1: 例外チェック（最優先）\n"
                "Q1. 記事タイトルまたは本文が `Myawaddy`, `မြဝတီ`, `Muse`, `မူဆယ်`に関する内容ですか？\n"
                "→ Yes の場合、この後の判定は行わず Step 3 に進んでください。\n"
                "→ No の場合は Step 2 へ進んでください。\n\n"
                "Step 2: 除外条件チェック\n"
                "Q2. 特定の地域（郡区、タウンシップ、市、村）で発生した局地的な戦闘、紛争、攻撃、衝突、爆撃、強盗、抗議活動に関する記事ですか？（地域全体の被害報告・統計も含む）\n"
                "→ Yes の場合は処理を終了してください、Step 3 には進まないでください、`exit`だけレスポンスを返してください。\n"
                "→ No の場合は Step 3 へ進んでください。\n\n"
                "Step 3: 翻訳と要約処理\n"
                "以下のルールに従って、記事タイトルを自然な日本語に翻訳し、本文を要約してください。\n\n"
                "タイトル：\n"
                "- 記事タイトルを自然な日本語に翻訳してください。\n"
                "- レスポンスでは必ず「【タイトル】 ◯◯」の形式で返してください。\n"
                "- それ以外の文言は不要です。\n\n"
                "本文要約：\n"
                "- 以下の記事本文について重要なポイントをまとめ、具体的に要約してください。\n"
                "- 自然な日本語に翻訳してください。\n"
                "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
                "- レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n\n"
                "出力条件：\n"
                "- 1行目は`【要約】`とだけしてください。\n"
                "- 見出しや箇条書きを適切に使って整理してください。\n"
                "- 見出しや箇条書きにはマークダウン記号（#, *, - など）を使わず、単純なテキストとして出力してください。\n"
                "- 見出しは `[ ]` で囲んでください。\n"
                "- 空行は作らないでください。\n"
                "- 特殊記号は使わないでください（全体をHTMLとして送信するわけではないため）。\n"
                "- 箇条書きは`・`を使ってください。\n"
                "- 要約の文字数は最大500文字としてください。\n\n"
                "入力データ：\n"
                "###\n"
                "[記事タイトル]\n"
                "###\n"
                f"{item['title']}\n\n"
                "[記事本文]\n"
                "###\n"
                f"{item['body'][:2000]}\n"
                "###\n"
            )

            try:
                # デバッグ: 入力データを確認
                print("----- DEBUG: Prompt Input -----")
                print(f"TITLE: {item['title']}")
                print(f"BODY[:2000]: {item['body'][:2000]}")

                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
                output_text = resp.text.strip()

                # デバッグ: モデル出力を確認
                print("----- DEBUG: Model Output -----")
                print(output_text)

                # exitが返ってきたらスキップ
                if output_text.strip().lower() == "exit":
                    continue

                # タイトル行と要約の抽出
                lines = output_text.splitlines()
                title_line = next((line for line in lines if line.startswith("【タイトル】")), None)
                summary_lines = [line for line in lines if line and not line.startswith("【タイトル】")]

                if title_line:
                    translated_title = title_line.replace("【タイトル】", "").strip()
                else:
                    translated_title = "（翻訳失敗）"

                summary_text = "\n".join(summary_lines).strip()

                # 出力条件に沿ってHTMLに変換（改行→<br>）
                summary_html = summary_text.replace("\n", "<br>")

                summarized_results.append({
                    "source": item["source"],
                    "url": item["url"],
                    "title": translated_title,
                    "summary": summary_html,
                })

            except Exception as e:
                print(f"🛑 Error during translation: {e}")
                continue

        if i + batch_size < len(translation_queue):
            print(f"🕒 Waiting {wait_seconds} seconds before next batch...")
            time.sleep(wait_seconds)

    # 重複判定→片方残し（最終アウトプットの形式は変えない）
    deduped = dedupe_articles_with_llm(client, summarized_results)

    # 念のため：返却フォーマットを固定（余計なキーが混ざっていたら落とす）
    normalized = [
        {
            "source": x.get("source"),
            "url": x.get("url"),
            "title": x.get("title"),
            "summary": x.get("summary"),
        } for x in deduped
    ]
    return normalized

def send_email_digest(summaries):
    sender_email = os.getenv("EMAIL_SENDER")
    sender_pass = os.getenv("GMAIL_APP_PASSWORD")
    # メール送信先本番用
    # recipient_emails = os.getenv("EMAIL_RECIPIENTS", "").split(",")
    # メール送信先テスト用
    recipient_emails = ["yasu.23721740311@gmail.com"]


    # ✅ 今日の日付を取得してフォーマット
    digest_date = get_today_date_mmt()
    date_str = digest_date.strftime("%Y年%-m月%-d日") + "分"

    # メディアごとにまとめる
    media_grouped = defaultdict(list)
    for item in summaries:
        media_grouped[item["source"]].append(item)

    # メールタイトル
    subject = "ミャンマー関連ニュース【" + date_str + "】"

    # メール本文のHTML生成
    html_content = """
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #ffffff; color: #333333;">
    """

    # メディアでグループ化は使うが、見出しは各記事の中に入れる
    for media, articles in media_grouped.items():
        for item in articles:
            
            title_jp = item["title"]          # 「タイトル: 」の接頭辞は外す
            url = item["url"]
            summary_html = item["summary"]    # 既に <br> 整形済み

            # 参考HTML準拠：見出し(h2)の右側にメディア名。
            heading_html = (
                "<h2 style='margin-bottom:5px'>"
                f"{title_jp}　"
                "<span style='font-size:0.83rem;font-weight:600'>"  # ← h5相当
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
                "</div>"
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

    # 記事取得＆キューに貯める
    # Mizzima (English)外す
    # print("=== Mizzima (English) ===")
    # articles_eng = get_mizzima_articles_from_category(
    #     date_mmt,
    #     "https://eng.mizzima.com",
    #     "Mizzima (English)",
    #     "/category/news/myanmar_news",
    #     max_pages=3
    # )
    # process_and_enqueue_articles(articles_eng, "Mizzima (English)", seen_urls)
    
    # === Mizzima (Burmese) ===
    print("=== Mizzima (Burmese) ===")
    articles_bur = get_mizzima_articles_from_category(
        date_mmt,
        "https://bur.mizzima.com",
        "Mizzima (Burmese)",
        "/category/%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8/%e1%80%99%e1%80%bc%e1%80%94%e1%80%ba%e1%80%99%e1%80%ac%e1%80%9e%e1%80%90%e1%80%84%e1%80%ba%e1%80%b8",
        max_pages=3
    )
    process_and_enqueue_articles(articles_bur, "Mizzima (Burmese)", seen_urls)

    # print("=== Voice of Myanmar ===")
    # articles4 = get_vom_articles_for(date_mmt)
    # for art in articles4:
    #     print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    # print("=== Ludu Wayoo ===")
    # articles5 = get_ludu_articles_for(date_mmt)
    # for art in articles5:
    #     print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== BBC Burmese ===")
    articles6 = get_bbc_burmese_articles_for(date_mmt)
    process_and_enqueue_articles(articles6, "BBC Burmese", seen_urls)

    print("=== Khit Thit Media ===")
    articles7 = get_khit_thit_edia_articles_from_category(date_mmt, max_pages=3)
    process_and_enqueue_articles(articles7, "Khit Thit Media", seen_urls)

    print("=== Irrawaddy ===")
    articles8 = get_irrawaddy_articles_for(date_mmt)

    # デバックでログ確認
    print("RESULTS:", json.dumps(articles8, ensure_ascii=False, indent=2))
    sys.exit(1)

    process_and_enqueue_articles(articles8, "Irrawaddy", seen_urls)

    # URLベースの重複排除を先に行う
    print(f"⚙️ Removing URL duplicates from {len(translation_queue)} articles...")
    translation_queue = deduplicate_by_url(translation_queue)

    # バッチ翻訳実行 (10件ごとに1分待機)
    all_summaries = process_translation_batches(batch_size=10, wait_seconds=60)

    send_email_digest(all_summaries)
