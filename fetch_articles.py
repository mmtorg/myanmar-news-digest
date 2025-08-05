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
# 記事重複排除ロジック(BERT埋め込み版)のライブラリインポート
from sentence_transformers import SentenceTransformer, util

# Gemini
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Chat GPT
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ミャンマー標準時 (UTC+6:30)
MMT = timezone(timedelta(hours=6, minutes=30))

# 昨日の日付
# ミャンマー時間で正午でもBBCの当日のニュースは配信されないので、昨日の日付のニュースを取得することとしてる
def get_yesterday_date_mmt():
    now_mmt = datetime.now(MMT)
    yesterday_mmt = now_mmt - timedelta(days=1)
    return yesterday_mmt.date()

# 今日の日付
# def get_today_date_mmt():
#     now_mmt = datetime.now(MMT)
#     return now_mmt.date()

# 共通キーワードリスト（全メディア共通で使用する）
NEWS_KEYWORDS = ["မြန်မာ", "မြန်မာ့", "ဗမာ", "အောင်ဆန်းစုကြည်", "မင်းအောင်လှိုင်", "Myanmar", "Burma"]
NEWS_KEYWORDS = [unicodedata.normalize('NFC', kw) for kw in NEWS_KEYWORDS]

def clean_html_content(html: str) -> str:
    html = html.replace("\xa0", " ").replace("&nbsp;", " ")
    # 制御文字（カテゴリC）を除外、可視Unicodeはそのまま
    return ''.join(c for c in html if unicodedata.category(c)[0] != 'C')

def clean_text(text: str) -> str:
    import unicodedata
    if not text:
        return ""
    return ''.join(
        c if (unicodedata.category(c)[0] != 'C' and c != '\xa0') else ' '
        for c in text
    )

def get_frontier_articles_for(date_obj):
    base_url = "https://www.frontiermyanmar.net"
    list_url = base_url + "/en/news"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("div.teaser a")
    article_urls = [base_url + a["href"] for a in links if a.get("href", "").startswith("/")]

    filtered_articles = []
    for url in article_urls:
        try:
            res_article = requests.get(url, timeout=10)
            soup_article = BeautifulSoup(res_article.content, "html.parser")
            time_tag = soup_article.find("time")
            if not time_tag:
                continue
            date_str = time_tag.get("datetime", "")
            if not date_str:
                continue
            article_date = datetime.fromisoformat(date_str).date()
            if article_date == date_obj:
                title = soup_article.find("h1").get_text(strip=True)
                filtered_articles.append({
                    "url": url,
                    "title": title,
                    "date": article_date.isoformat()
                })
        except Exception:
            continue

    return filtered_articles

def get_mizzima_articles_for(date_obj, base_url, source_name):
    list_url = base_url  # トップページ
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.find_all("a", href=True)

    # URLに /YYYY/MM/DD/ が含まれるもののみ
    date_pattern = re.compile(r"/\d{4}/\d{2}/\d{2}/")
    article_urls = [a["href"] for a in links if date_pattern.search(a["href"])]

    target_date_str = date_obj.strftime("%Y/%m/%d")  # 例: "2025/08/02"

    filtered_articles = []
    for url in article_urls:
        if target_date_str not in url:
            continue  # URLに昨日の日付が無ければスキップ

        try:
            res_article = requests.get(url, timeout=10)
            soup_article = BeautifulSoup(res_article.content, "html.parser")

            # タイトル取得
            title_tag = soup_article.find("h1")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            # ★ 指定div内のpタグのみ取得
            content_divs = soup_article.select("div.mag-post-single, div.entry-content")
            paragraphs = []
            for div in content_divs:
                paragraphs += div.find_all("p")
                
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            body_text = unicodedata.normalize('NFC', body_text)

            if not body_text.strip():
                continue  # 本文が空ならスキップ

            # タイトルor本文にキーワードがあれば対象とする
            if not any(keyword in title or keyword in body_text for keyword in NEWS_KEYWORDS):
                continue

            filtered_articles.append({
                "source": source_name,
                "url": url,
                "title": title,
                "date": date_obj.isoformat()
            })

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return filtered_articles

def get_vom_articles_for(date_obj):
    base_url = "https://voiceofmyanmarnews.com"
    list_url = base_url + "/?cat=1"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("h2.entry-title a")
    article_urls = [a["href"] for a in links if a.get("href", "").startswith("https://")]

    filtered_articles = []
    for url in article_urls:
        try:
            res_article = requests.get(url, timeout=10)
            soup_article = BeautifulSoup(res_article.content, "html.parser")
            date_div = soup_article.select_one("time.entry-date")
            if not date_div:
                continue
            date_text = date_div.get_text(strip=True)
            # 例: "July 25, 2025" をパース
            try:
                article_date = datetime.strptime(date_text, "%B %d, %Y").date()
            except ValueError:
                continue
            if article_date == date_obj:
                title = soup_article.find("h1").get_text(strip=True)
                filtered_articles.append({
                    "url": url,
                    "title": title,
                    "date": article_date.isoformat()
                })
        except Exception:
            continue

    return filtered_articles

def get_ludu_articles_for(date_obj):
    base_url = "https://ludunwayoo.com"
    list_url = base_url + "/en/news"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("h2.entry-title a")
    article_urls = [a["href"] for a in links if a.get("href", "").startswith("http")]

    filtered_articles = []
    for url in article_urls:
        try:
            res_article = requests.get(url, timeout=10)
            soup_article = BeautifulSoup(res_article.content, "html.parser")
            time_tag = soup_article.find("time")
            if not time_tag:
                continue
            date_str = time_tag.get("datetime", "")
            if not date_str:
                continue
            article_date = datetime.fromisoformat(date_str).date()
            if article_date == date_obj:
                title = soup_article.find("h1").get_text(strip=True)
                filtered_articles.append({
                    "url": url,
                    "title": title,
                    "date": article_date.isoformat()
                })
        except Exception:
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
            continue  # 昨日(MMT基準)の日付と一致しない記事はスキップ

        title = item.find("title").text.strip()
        link = item.find("link").text.strip()

        try:
            article_res = requests.get(link, timeout=10)
            article_soup = BeautifulSoup(article_res.content, "html.parser")
            paragraphs = article_soup.find_all("p")
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

# def get_bbc_burmese_articles_for(date_obj):
#     base_url = "https://www.bbc.com"
#     list_url = base_url + "/burmese"
#     res = requests.get(list_url, timeout=10)
#     soup = BeautifulSoup(res.content, "html.parser")
#     links = soup.select("a[href^='/burmese/']")
#     article_urls = [
#         base_url + a["href"]
#         for a in links
#         if any(part in a["href"] for part in ["articles", "media"])
#     ]

#     seen = set()
#     filtered_articles = []
#     for url in article_urls:
#         if url in seen:
#             continue
#         seen.add(url)
#         try:
#             res_article = requests.get(url, timeout=10)
#             soup_article = BeautifulSoup(res_article.content, "html.parser")
#             time_tag = soup_article.find("time")
#             if not time_tag:
#                 continue
#             date_str = time_tag.get("datetime", "")
#             if not date_str:
#                 continue
#             article_date = datetime.fromisoformat(date_str).date()
#             if article_date == date_obj:
#                 title = soup_article.find("h1").get_text(strip=True)
#                 filtered_articles.append({
#                     "url": url,
#                     "title": title,
#                     "date": article_date.isoformat()
#                 })
#         except Exception:
#             continue

#     return filtered_articles

def get_yktnews_articles_for(date_obj):
    base_url = "https://yktnews.com"
    list_url = base_url + "/category/news/"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.find_all("a", href=True)

    # URLに /YYYY/MM/ が含まれるもののみ
    date_pattern = re.compile(r"/\d{4}/\d{2}/")
    article_urls = [a["href"] for a in links if date_pattern.search(a["href"])]

    print(article_urls)

    target_date_str = date_obj.strftime("%Y-%m-%d")  # 例: "2025-08-02"
    target_month_str = date_obj.strftime("%Y/%m")  # 例: "2025/08"

    filtered_articles = []
    for url in article_urls:
        if target_month_str not in url:
            continue  # URLに対象月が無ければスキップ

        try:
            res_article = requests.get(url, timeout=10)
            
            soup_article = BeautifulSoup(res_article.content, "html.parser")

            # 日付チェック
            time_tag = soup_article.select_one("div.tdb-block-inner time.entry-date")
            if not time_tag or not time_tag.has_attr("datetime"):
                continue

            date_str = time_tag["datetime"]
            article_date = datetime.fromisoformat(date_str).astimezone(MMT).date()
            if article_date != date_obj:
                continue  # 昨日の日付でなければスキップ

            # タイトル取得
            title_tag = soup_article.find("h1")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            # 本文取得 (フォールバック方式)
            paragraphs = soup_article.select("div.tdb-block-inner p")
            if not paragraphs:
                paragraphs = soup_article.select("div.tdb_single_content p")
            if not paragraphs:
                paragraphs = soup_article.select("article p")
            if not paragraphs:
                paragraphs = soup_article.find_all("p")  # 最終手段：全Pタグ
            
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            body_text = unicodedata.normalize('NFC', body_text)

            if not body_text.strip():
                continue  # 本文が空ならスキップ

            # タイトルor本文にキーワードがあれば対象とする
            if not any(keyword in title or keyword in body_text for keyword in NEWS_KEYWORDS):
                continue

            filtered_articles.append({
                "url": url,
                "title": title,
                "date": date_obj.isoformat()
            })

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return filtered_articles


# Chat GPT使う場合
# def translate_and_summarize(text: str) -> str:
#     if not text or not text.strip():
#         print("⚠️ 入力テキストが空です。")
#         return "（翻訳・要約に失敗しました）"

#     prompt = (
#         "以下の記事の内容について重要なポイントをまとめ、具体的に要約してください。" 
#         "文字数は800文字までとします。自然な日本語に訳してください。\n\n"
#         f"{text[:2000]}"  # 入力長を適切に制限（APIの入力トークン制限を超えないように）
#     )

#     try:
#         response = client.chat.completions.create(
#             model="gpt-3.5-turbo",
#             messages=[{"role": "user", "content": prompt}]
#         )
#         return response.choices[0].message.content.strip()

#     except OpenAIError as api_err:
#         # OpenAI全体の例外を網羅
#         print(f"🛑 OpenAI API エラー: {api_err}")
#         return "（翻訳・要約に失敗しました）"
#     except Exception as e:
#         # その他の予期しない例外
#         print(f"予期せぬエラー: {e}")
#         return "（翻訳・要約に失敗しました）"

# BERT埋め込みで類似記事判定
def deduplicate_articles(articles, similarity_threshold=0.92):
    if not articles:
        return []

    # 重複した場合の記事優先度
    media_priority = {
        "BBC Burmese": 1,
        "Mizzima (English)": 2,
        "Mizzima (Burmese)": 3,
        "YKT News": 4
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
            # デバック
            print(f"🛑 Duplicate Title Found: '{art['title']}'\n - Kept: {articles[title_seen[art['title']]]['source']} | {articles[title_seen[art['title']]]['url']}\n - Removed: {art['source']} | {art['url']}")
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
                # デバック
                print(f"🛑 BERT Duplicate Found:\n - Kept Candidate: {articles[i]['source']} | {articles[i]['title']} | {articles[i]['url']}\n - Removed Candidate: {articles[j]['source']} | {articles[j]['title']} | {articles[j]['url']}\n (Similarity: {cosine_scores[i][j]:.4f})")
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
            paragraphs = soup.find_all("p")
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
    summarized_results = []

    for i in range(0, len(translation_queue), batch_size):
        batch = translation_queue[i:i + batch_size]
        print(f"⚙️ Processing batch {i // batch_size + 1}...")

        for item in batch:
            prompt = (
                "以下は記事のタイトルです。自然な日本語に翻訳し「【タイトル】 ◯◯」とレスポンスでは返してください。それ以外の文言は不要です。\n"
                "###\n"
                f"{item['title']}\n"
                "###\n\n"
                "以下の記事の本文について重要なポイントをまとめ具体的に要約してください。自然な日本語に訳してください。\n"
                "個別記事の本文の要約のみとしてください。メディアの説明やページ全体の解説は不要です。\n"
                "レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n"
                "以下、出力の条件です。\n"
                "- 1行目は「【要約】」とだけしてください。"
                "- 見出しや箇条書きを適切に使って見やすく整理してください。\n"
                "- 見出しや箇条書きにはマークダウン記号（#, *, - など）は使わず、単純なテキストとして出力してください。\n"
                "- 見出しは `[  ]` で囲んでください。\n"
                "- テキストが入っていない改行は作らないでください。\n"
                "- 全体をHTMLで送るわけではないので、特殊記号は使わないでください。\n"
                "- 箇条書きは「・」を使ってください。\n"
                "- 要約の文字数は最大500文字を超えてはいけません。\n"
                "###\n"
                f"{item['body'][:2000]}\n"
                "###"
            )

            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
                output_text = resp.text.strip()

                # パース
                lines = output_text.splitlines()
                title_line = next((line for line in lines if line.startswith("【タイトル】")), None)
                summary_lines = [line for line in lines if line and not line.startswith("【タイトル】")]

                if title_line:
                    translated_title = title_line.replace("【タイトル】", "").strip()
                else:
                    translated_title = "（翻訳失敗）"

                summary_text = "\n".join(summary_lines).strip()
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
    recipient_emails = os.getenv("EMAIL_RECIPIENTS", "").split(",")

    # ✅ 今日の日付を取得してフォーマット
    digest_date = get_yesterday_date_mmt()
    # digest_date = get_today_date_mmt()
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
        html_content += f"<h3 style='color: #2a2a2a; margin-top: 30px;'>{media} からのニュース</h3>"

        for item in articles:
            title_jp = "タイトル: " + item["title"]
            url = item["url"]

            summary_html = item["summary"]  # すでにHTML整形済みをそのまま使う
            html_content += (
                f"<div style='margin-bottom: 20px;'>"
                f"<h4 style='margin-bottom: 5px;'>{title_jp}</h4>"
                f"<p><a href='{url}' style='color: #1a0dab;'>本文を読む</a></p>"
                f"<div style='background-color: #f9f9f9; padding: 10px; border-radius: 8px;'>"
                f"{summary_html}"
                f"</div></div><hr style='border-top: 1px solid #cccccc;'>"
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
    date_mmt = get_yesterday_date_mmt()
    # date_mmt = get_today_date_mmt()
    seen_urls = set()
    
    # articles = get_frontier_articles_for(date_mmt)
    # for art in articles:
    #     print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    # 記事取得＆キューに貯める
    print("=== Mizzima (English) ===")
    articles_eng = get_mizzima_articles_for(date_mmt, "https://eng.mizzima.com", "Mizzima (English)")
    process_and_enqueue_articles(articles_eng, "Mizzima (English)", seen_urls)
    
    print("=== Mizzima (Burmese) ===")
    articles_bur = get_mizzima_articles_for(date_mmt, "https://bur.mizzima.com", "Mizzima (Burmese)")
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
    articles7 = get_yktnews_articles_for(date_mmt)
    process_and_enqueue_articles(articles7, "YKT News", seen_urls)

    # ✅ 全記事取得後 → BERT類似度で重複排除
    print(f"⚙️ Deduplicating {len(translation_queue)} articles...")
    deduplicated_articles = deduplicate_articles(translation_queue)

    # translation_queue を重複排除後のリストに置き換え
    translation_queue.clear()
    translation_queue.extend(deduplicated_articles)

    # バッチ翻訳実行 (10件ごとに1分待機)
    all_summaries = process_translation_batches(batch_size=10, wait_seconds=60)

    send_email_digest(all_summaries)
