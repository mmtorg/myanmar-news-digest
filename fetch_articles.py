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

# Gemini
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Chat GPT
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ミャンマー標準時 (UTC+6:30)
MMT = timezone(timedelta(hours=6, minutes=30))

# 今日の日付
# ニュースの速報性重視で今日分のニュース配信の方針
def get_today_date_mmt():
    # now_mmt = datetime.now(MMT)
    # return now_mmt.date()
    now_mmt = date(2025, 8, 6)  # ← テスト用：2025年8月6日に上書き
    return now_mmt

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
    "Kyat", "kyat", "ကျပ်",
    
    # 徴兵制（Conscription / Military Draft）
    "Conscription", "conscription", "Military Draft", "military draft", "စစ်တပ်ဝင်ခေါ်ရေး",
    
    # 選挙（Election）
    "Election", "election", "ရွေးကောက်ပွဲ"
]
# Unicode正規化（NFC）を適用
NEWS_KEYWORDS = [unicodedata.normalize('NFC', kw) for kw in NEWS_KEYWORDS]

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

            if not any(keyword in title or keyword in body_text for keyword in NEWS_KEYWORDS):
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
    rss_url = "https://feeds.bbci.co.uk/burmese/rss.xml"
    res = requests.get(rss_url, timeout=10)
    soup = BeautifulSoup(res.content, "xml")

    articles = []
    for item in soup.find_all("item"):
        pub_date_tag = item.find("pubDate")
        if not pub_date_tag:
            continue

        try:
            pub_date = parse_date(pub_date_tag.text)  # RSSはUTC基準
            pub_date_mmt = pub_date.astimezone(MMT).date()  # ← MMTに変換して日付抽出
        except Exception as e:
            print(f"❌ pubDate parse error: {e}")
            continue

        if pub_date_mmt != target_date_mmt:
            continue  # 今日(MMT基準)の日付と一致しない記事はスキップ

        title = item.find("title").text.strip()
        link = item.find("link").text.strip()

        try:
            article_res = requests.get(link, timeout=10)
            article_soup = BeautifulSoup(article_res.content, "html.parser")
            # 本文pタグをリトライ付きで取得
            paragraphs = extract_paragraphs_with_wait(article_soup, retries=2, wait_seconds=2)
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            # ここでNFC正規化を追加
            body_text = unicodedata.normalize('NFC', body_text)

            if not any(keyword in title or keyword in body_text for keyword in NEWS_KEYWORDS):
                continue  # キーワードが含まれていなければ除外

            print(f"✅ 抽出記事: {title} ({link})")  # ログ出力で抽出記事確認
            articles.append({
                "title": title,
                "url": link,
                "date": pub_date_mmt.isoformat()
            })

        except Exception as e:
            print(f"❌ 記事取得エラー: {e}")
            continue

    return articles

# yktnewsカテゴリーページ巡回で取得
def get_yktnews_articles_from_category(date_obj, max_pages=3):
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

            # 本文取得 (YKTNews用パターン)
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

            if not any(keyword in title or keyword in body_text for keyword in NEWS_KEYWORDS):
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
def deduplicate_articles(articles, similarity_threshold=0.80): # 類似度閾値、高いほど厳しい、チューニング
    if not articles:
        return []

    # 重複した場合の記事優先度
    media_priority = {
        "BBC Burmese": 1,
        "Mizzima (English)": 2,
        "Mizzima (Burmese)": 3,
        "YKT News": 4
    }

    model = SentenceTransformer('distiluse-base-multilingual-cased-v2')
    texts = [art['title'] + " " + art['body'][:2000] for art in articles]  # 本文は先頭2000文字を見に行く、チューニング
    embeddings = model.encode(texts, convert_to_tensor=True)

    cosine_scores = util.pytorch_cos_sim(embeddings, embeddings).cpu().numpy()

    visited = set()
    unique_articles = []

    # デバッグ出力: 類似スコア確認 ← ここから追加
    for i in range(len(articles)):
        for j in range(i + 1, len(articles)):
            score = cosine_scores[i][j]
            if score > 0.60:
                print(f"🔍 類似度: {score:.4f}")
                print(f" - {articles[i]['title']} ({articles[i]['source']})")
                print(f" - {articles[j]['title']} ({articles[j]['source']})")
                print(f" - URLs:\n   {articles[i]['url']}\n   {articles[j]['url']}")
                print("----------")
    # ← ここまで追加

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

def process_translation_batches(batch_size=10, wait_seconds=60):

    # ⚠️ TEST: Geminiを呼ばず、URLリストだけ返す
    summarized_results = []
    for item in translation_queue:
        summarized_results.append({
            "source": item["source"],
            "url": item["url"],
            "title": "（タイトルはテスト省略）",
            "summary": "（要約テスト省略）"
        })

    # summarized_results = []
    # for i in range(0, len(translation_queue), batch_size):
    #     batch = translation_queue[i:i + batch_size]
    #     print(f"⚙️ Processing batch {i // batch_size + 1}...")

    #     for item in batch:
    #         prompt = (
    #             "以下は記事のタイトルです。自然な日本語に翻訳し「【タイトル】 ◯◯」とレスポンスでは返してください。それ以外の文言は不要です。\n"
    #             "###\n"
    #             f"{item['title']}\n"
    #             "###\n\n"
    #             "以下の記事の本文について重要なポイントをまとめ具体的に要約してください。自然な日本語に訳してください。\n"
    #             "個別記事の本文の要約のみとしてください。メディアの説明やページ全体の解説は不要です。\n"
    #             "レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n"
    #             "以下、出力の条件です。\n"
    #             "- 1行目は「【要約】」とだけしてください。"
    #             "- 見出しや箇条書きを適切に使って見やすく整理してください。\n"
    #             "- 見出しや箇条書きにはマークダウン記号（#, *, - など）は使わず、単純なテキストとして出力してください。\n"
    #             "- 見出しは `[  ]` で囲んでください。\n"
    #             "- テキストが入っていない改行は作らないでください。\n"
    #             "- 全体をHTMLで送るわけではないので、特殊記号は使わないでください。\n"
    #             "- 箇条書きは「・」を使ってください。\n"
    #             "- 要約の文字数は最大500文字を超えてはいけません。\n"
    #             "###\n"
    #             f"{item['body'][:2000]}\n"
    #             "###"
    #         )

    #         try:
    #             resp = client.models.generate_content(
    #                 model="gemini-2.5-flash",
    #                 contents=prompt
    #             )
    #             output_text = resp.text.strip()

    #             # パース
    #             lines = output_text.splitlines()
    #             title_line = next((line for line in lines if line.startswith("【タイトル】")), None)
    #             summary_lines = [line for line in lines if line and not line.startswith("【タイトル】")]

    #             if title_line:
    #                 translated_title = title_line.replace("【タイトル】", "").strip()
    #             else:
    #                 translated_title = "（翻訳失敗）"

    #             summary_text = "\n".join(summary_lines).strip()
    #             summary_html = summary_text.replace("\n", "<br>")

    #             summarized_results.append({
    #                 "source": item["source"],
    #                 "url": item["url"],
    #                 "title": translated_title,
    #                 "summary": summary_html,
    #             })

    #         except Exception as e:
    #             print(f"🛑 Error during translation: {e}")
    #             continue

    #     if i + batch_size < len(translation_queue):
    #         print(f"🕒 Waiting {wait_seconds} seconds before next batch...")
    #         time.sleep(wait_seconds)

    return summarized_results

def send_email_digest(summaries):
    sender_email = os.getenv("EMAIL_SENDER")
    sender_pass = os.getenv("GMAIL_APP_PASSWORD")
    recipient_emails = os.getenv("EMAIL_RECIPIENTS", "").split(",")

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
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #ffffff; color: #333333;">
    """

    for media, articles in media_grouped.items():
        html_content += f"<h2 style='color: #2a2a2a; margin-top: 30px;'>{media} からのニュース</h2>"

        # ⚠️ TEST: Geminiを呼ばず、URLリストだけ返す
        for item in articles:
            url = item["url"]
            html_content += (
                f"<div style='margin-bottom: 10px;'>"
                f"<p><a href='{url}' style='color: #1a0dab;'>本文を読む</a></p>"
                f"</div>"
            )

        # for item in articles:
        #     title_jp = "タイトル: " + item["title"]
        #     url = item["url"]

        #     summary_html = item["summary"]  # すでにHTML整形済みをそのまま使う
        #     html_content += (
        #         f"<div style='margin-bottom: 20px;'>"
        #         f"<h4 style='margin-bottom: 5px;'>{title_jp}</h4>"
        #         f"<p><a href='{url}' style='color: #1a0dab;'>本文を読む</a></p>"
        #         f"<div style='background-color: #f9f9f9; padding: 10px; border-radius: 8px;'>"
        #         f"{summary_html}"
        #         f"</div></div><hr style='border-top: 1px solid #cccccc;'>"
        #     )

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
    # date_mmt = get_today_date_mmt()
    date_mmt = date(2025, 8, 6)  # ← テスト用：2025年8月6日に上書き
    seen_urls = set()
    
    # articles = get_frontier_articles_for(date_mmt)
    # for art in articles:
    #     print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    # 記事取得＆キューに貯める
    print("=== Mizzima (English) ===")
    articles_eng = get_mizzima_articles_from_category(
        date_mmt,
        "https://eng.mizzima.com",
        "Mizzima (English)",
        "/category/news/myanmar_news",
        max_pages=3
    )
    process_and_enqueue_articles(articles_eng, "Mizzima (English)", seen_urls)
    
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

    print("=== YKT News ===")
    articles7 = get_yktnews_articles_from_category(date_mmt, max_pages=3)
    process_and_enqueue_articles(articles7, "YKT News", seen_urls)

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
