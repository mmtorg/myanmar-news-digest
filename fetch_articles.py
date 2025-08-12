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

# ===== Mizzima除外対象キーワード（タイトル用） =====
EXCLUDE_TITLE_KEYWORDS = [
    # 春の革命日誌
    "နွေဦးတော်လှန်ရေး နေ့စဉ်မှတ်စု",
    # 写真ニュース
    "ဓာတ်ပုံသတင်း"
]

# Mizzimaカテゴリーページ巡回で取得
def get_mizzima_articles_from_category(date_obj, base_url, source_name, category_path, max_pages=3):
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
NOISE_PATTERNS = [
    r"BBC\s*News\s*မြန်မာ",  # 固定署名（Burmese表記）
    r"BBC\s*Burmese"        # 英語表記
]

def remove_noise_phrases(text: str) -> str:
    """BBC署名などのノイズフレーズを除去"""
    if not text:
        return text
    for pat in NOISE_PATTERNS:
        text = re.sub(pat, "", text, flags=re.IGNORECASE)
    return text.strip()

# あるテキスト中でキーワードがどこにヒットしたかを返す（周辺文脈つき）
def find_hits(text: str, keywords):
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

def get_bbc_burmese_articles_for(target_date_mmt):
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
            title_nfc = remove_noise_phrases(title_nfc)
            body_text_nfc = unicodedata.normalize('NFC', body_text)
            body_text_nfc = remove_noise_phrases(body_text_nfc)

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
            # title_hits = find_hits(title_nfc, NEWS_KEYWORDS)
            # body_hits  = find_hits(body_text_nfc, NEWS_KEYWORDS)
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

# BERT埋め込みで類似記事判定
def deduplicate_articles(articles, similarity_threshold=0.92):
    if not articles:
        return []

    # 重複した場合の記事優先度
    media_priority = {
        "BBC Burmese": 1,
        # "Mizzima (English)": 2,
        "Mizzima (Burmese)": 2,
        "Khit Thit Media": 3
    }

    model = SentenceTransformer('cl-tohoku/bert-base-japanese-v2')
    texts = [art['title'] + " " + art['body'][:300] for art in articles]  # 本文は先頭300文字だけ
    embeddings = model.encode(texts, convert_to_tensor=True)

    cosine_scores = util.pytorch_cos_sim(embeddings, embeddings).cpu().numpy()

    visited = set()
    unique_articles = []

    # まずタイトル完全一致グルーピング
    title_seen = {}
    for idx, art in enumerate(articles):
        if art['title'] in title_seen:
            continue  # すでに同じタイトルの記事が登録されていればスキップ
        title_seen[art['title']] = idx
        unique_articles.append(art)
        visited.add(idx)

    # 次にBERTベースの類似判定
    for i in range(len(articles)):
        if i in visited:
            continue

        group = [i]
        for j in range(i + 1, len(articles)):
            if cosine_scores[i][j] > similarity_threshold:
                group.append(j)
                visited.add(j)

        group_sorted = sorted(group, key=lambda idx: media_priority.get(articles[idx]['source'], 99))
        unique_articles.append(articles[group_sorted[0]])
        visited.add(i)

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

            # ★ここでNEWS_KEYWORDSフィルターをかける
            if not any(keyword in art['title'] or keyword in body_text for keyword in NEWS_KEYWORDS):
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
#     print("🔧 Debug mode: Gemini API is NOT called.")
#     for i in range(0, len(translation_queue), batch_size):
#         batch = translation_queue[i:i + batch_size]
#         print(f"⚙️ Processing batch {i // batch_size + 1}...")

#         for idx, item in enumerate(batch, 1):
#             title = item.get("title", "")
#             body = item.get("body", "") or ""
#             body_head = body[:2000]

#             print("—" * 40)
#             print(f"[{idx}] URL: {item.get('url','')}")
#             print(f"TITLE: {repr(title)}")
#             print(f"BODY[:2000]: {repr(body_head)}")

#             # テスト用に最小限の結果を返す（翻訳・要約はダミー）
#             summarized_results.append({
#                 "source": item.get("source", ""),
#                 "url": item.get("url", ""),
#                 "title": title,                 # 翻訳なし（そのまま）
#                 "summary": body_head.replace("\n", "<br>")  # 先頭だけ
#             })

#         if i + batch_size < len(translation_queue):
#             print(f"🕒 Waiting {wait_seconds} seconds before next batch...")
#             time.sleep(wait_seconds)

#     return summarized_results

# 本処理関数
def process_translation_batches(batch_size=10, wait_seconds=60):

    # ⚠️ TEST: Geminiを呼ばず、URLリストだけ返す
    # summarized_results = []
    # for item in translation_queue:
    #     summarized_results.append({
    #         "source": item["source"],
    #         "url": item["url"],
    #         "title": "（タイトルはテスト省略）",
    #         "summary": "（要約テスト省略）"
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

    return summarized_results

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

    # URLベースの重複排除を先に行う
    print(f"⚙️ Removing URL duplicates from {len(translation_queue)} articles...")
    translation_queue = deduplicate_by_url(translation_queue)

    # ✅ 全記事取得後 → BERT類似度で重複排除
    print(f"⚙️ Deduplicating {len(translation_queue)} articles...")
    deduplicated_articles = deduplicate_articles(translation_queue)

    # translation_queue を重複排除後のリストに置き換え
    translation_queue.clear()
    translation_queue.extend(deduplicated_articles)

    # バッチ翻訳実行 (10件ごとに1分待機)
    all_summaries = process_translation_batches(batch_size=10, wait_seconds=60)

    send_email_digest(all_summaries)
