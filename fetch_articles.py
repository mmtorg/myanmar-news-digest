import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import openai
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

openai.api_key = os.getenv("OPENAI_API_KEY")

def get_yesterday_date_mmt():
    mm_yesterday = datetime.utcnow() + timedelta(hours=6.5) - timedelta(days=1)
    return mm_yesterday.date()

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

def get_mizzima_articles_for(date_obj):
    base_url = "https://www.mizzima.com"
    list_url = base_url + "/news/domestic"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("div.views-row a")
    article_urls = [base_url + a["href"] for a in links if a.get("href", "").startswith("/")]

    filtered_articles = []
    for url in article_urls:
        try:
            res_article = requests.get(url, timeout=10)
            soup_article = BeautifulSoup(res_article.content, "html.parser")
            meta_tag = soup_article.find("meta", {"property": "article:published_time"})
            if not meta_tag:
                continue
            date_str = meta_tag.get("content", "")
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

def get_bbc_burmese_articles_for(date_obj):
    base_url = "https://www.bbc.com"
    list_url = base_url + "/burmese"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("a[href^='/burmese/articles/']")
    article_urls = [base_url + a["href"] for a in links]

    seen = set()
    filtered_articles = []
    for url in article_urls:
        if url in seen:
            continue
        seen.add(url)
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

def get_yktnews_articles_for(date_obj):
    base_url = "https://yktnews.com"
    list_url = base_url + "/category/news/"
    res = requests.get(list_url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("h3.entry-title a")
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

def translate_and_summarize(text):
    prompt = (
        "以下の記事の内容について重要なポイントをまとめ、具体的に解説してください。"
        "文字数は800文字までとします。文章は自然な日本語に訳してください。\n\n"
        f"{text}"
    )

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # 必要に応じて 4 に変更可能
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1024
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"OpenAI API エラー: {e}")
        return "（翻訳・要約に失敗しました）"

def process_and_summarize_articles(articles, source_name):
    results = []
    for art in articles:
        try:
            res = requests.get(art['url'], timeout=10)
            soup = BeautifulSoup(res.content, "html.parser")
            paragraphs = soup.find_all("p")
            text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            summary = translate_and_summarize(text)
            results.append({
                "source": source_name,
                "url": art["url"],
                "title": art["title"],
                "summary": summary
            })
        except Exception as e:
            continue
    return results

def send_email_digest(summaries, subject="Daily Myanmar News Digest"):
    sender_email = os.getenv("EMAIL_SENDER")
    sender_pass = os.getenv("GMAIL_APP_PASSWORD")
    recipient_emails = os.getenv("EMAIL_RECIPIENTS", "").split(",")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_emails)

    html_content = "<html><body>"
    html_content += "<h2>🇲🇲 ミャンマー関連ニュース（日本語要約）</h2>"

    for item in summaries:
        html_content += f"<h3>{item['source']}: {item['title']}</h3>"
        html_content += f"<p><a href='{item['url']}'>{item['url']}</a></p>"
        html_content += f"<p>{item['summary']}</p><hr>"

    html_content += "</body></html>"

    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_pass)
            server.sendmail(sender_email, recipient_emails, msg.as_string())
            print("✅ メール送信完了")
    except Exception as e:
        print(f"❌ メール送信エラー: {e}")

if __name__ == "__main__":
    yesterday = get_yesterday_date_mmt()
    articles = get_frontier_articles_for(yesterday)
    for art in articles:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== Mizzima ===")
    articles3 = get_mizzima_articles_for(yesterday)
    for art in articles3:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== Voice of Myanmar ===")
    articles4 = get_vom_articles_for(yesterday)
    for art in articles4:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== Ludu Wayoo ===")
    articles5 = get_ludu_articles_for(yesterday)
    for art in articles5:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== BBC Burmese ===")
    articles6 = get_bbc_burmese_articles_for(yesterday)
    for art in articles6:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    print("=== YKT News ===")
    articles7 = get_yktnews_articles_for(yesterday)
    for art in articles7:
        print(f"{art['date']} - {art['title']}\n{art['url']}\n")

    all_summaries = []
    all_summaries += process_and_summarize_articles(get_frontier_articles_for(yesterday), "Frontier Myanmar")
    all_summaries += process_and_summarize_articles(get_mizzima_articles_for(yesterday), "Mizzima")
    all_summaries += process_and_summarize_articles(get_vom_articles_for(yesterday), "Voice of Myanmar")
    all_summaries += process_and_summarize_articles(get_ludu_articles_for(yesterday), "Ludu Wayoo")
    all_summaries += process_and_summarize_articles(get_bbc_burmese_articles_for(yesterday), "BBC Burmese")
    all_summaries += process_and_summarize_articles(get_yktnews_articles_for(yesterday), "YKT News")

    send_email_digest(all_summaries)


