import requests
from bs4 import BeautifulSoup
import openai

openai.api_key = "YOUR_API_KEY"

def translate_and_summarize(text):
    prompt = f"""以下の内容について重要なポイントをまとめ具体的に要約してください。要約した文章は自然な日本語に訳してください。：
{text}"""
    res = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return res["choices"][0]["message"]["content"]

def get_frontier_urls():
    url = "https://www.frontiermyanmar.net/en/news"
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("div.teaser a")
    return ["https://www.frontiermyanmar.net" + a["href"] for a in links if a["href"].startswith("/")]

def main():
    urls = get_frontier_urls()[:3]
    for url in urls:
        res = requests.get(url)
        soup = BeautifulSoup(res.content, "html.parser")
        text = " ".join([p.get_text() for p in soup.find_all("p")])
        summary = translate_and_summarize(text)
        print(f"{url}\n{summary}\n{'-'*40}")

if __name__ == "__main__":
    main()
