# -*- coding: utf-8 -*-
"""
export_dvb_only_to_csv.py (fixed)

DVBのみを対象に、MMTで 2025-08-23(土) 以降〜今日(MMT)まで、
日付ごとにその日に発行された記事を収集し、CSV(UTF-8 BOM)に
A:メディア名 / B:日本語タイトル / C:発行日(MMT) / D:URL を出力。

- 一覧ページ: 1〜15ページを巡回（カードの英語日付で一次フィルタ）
- 一覧ページ取得は各ページ最大3回トライし、失敗したページはスキップ（処理は継続）
- 記事ページ取得は fetch_with_retry_dvb を使用
- タイトルの日本語化は gemini-2.5-flash-lite（バッチ翻訳→失敗時は単体翻訳）
- 無料枠配慮のレートリミット（rpm/min_interval/jitter + batch-size）
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

# 既存の共通関数/定数を流用
from fetch_articles import (
    MMT,
    build_prompt,
    call_gemini_with_retries,
    client_summary,
    fetch_with_retry_dvb,
)

# ========== ユーティリティ ==========
def daterange_mmt(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

class RateLimiter:
    """Requests/min と最小インターバルを両方満たす簡易レートリミッタ"""
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

# ========== 翻訳 ==========
def translate_title_only(item: Dict, *, model: str = "gemini-2.5-flash-lite") -> str:
    """タイトルのみ日本語化（既存プロンプトを流用）"""
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
        # 「【タイトル】…」の行を抽出
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
    except Exception:
        return payload["title"]

def translate_titles_in_batch(items: List[Dict], *, model: str = "gemini-2.5-flash-lite") -> List[str]:
    """厳密JSONで返させるバッチ翻訳（失敗時は空リスト→呼び出し側で単体翻訳にフォールバック）"""
    numbered = []
    for i, it in enumerate(items, 1):
        src = (it.get("source") or "").replace("\n", " ").strip()
        ttl = (it.get("title") or "").replace("\n", " ").strip()
        url = (it.get("url") or "").strip()
        numbered.append(f'{i}. [source:{src}] [url:{url}] title="{ttl}"')

    sys_prompt = (
        "あなたは報道見出しの専門翻訳者です。以下の英語/ビルマ語の見出しを自然な日本語見出しに翻訳してください。"
        "固有名詞は一般的な日本語表記を優先し、簡潔で要点を保ち、記号の乱用は避けます。"
        "出力は厳密なJSONのみ。説明文・コードフェンス禁止。"
        'フォーマットは {"results":[{"i":1,"ja":"..."},...]} です。'
    )
    user_prompt = "翻訳対象:\n" + "\n".join(numbered) + "\n\n" + \
        '出力は次のJSONのみ: {"results":[{"i":1,"ja":"..."},{"i":2,"ja":"..."}]}'

    try:
        resp = call_gemini_with_retries(client_summary, f"{sys_prompt}\n\n{user_prompt}", model=model)
        text = (resp.text or "").strip()
        start = text.find("{"); end = text.rfind("}")
        if start == -1 or end == -1:
            raise ValueError("no JSON braces")
        data = json.loads(text[start:end+1])
        mapping = {int(r.get("i")): (r.get("ja") or "").strip()
                   for r in data.get("results", []) if "i" in r and "ja" in r}
        return [mapping.get(i, "") for i in range(1, len(items) + 1)]
    except Exception:
        return []

# ======== DVB 一覧＆記事（“取得できている”コードの方針へ差し替え） ========
_MONTHS = ("January","February","March","April","May","June","July",
           "August","September","October","November","December")
_DATE_RE = re.compile(
    rf"(?:{'|'.join(_MONTHS)})\s+\d{{1,2}},\s*\d{{4}}"
)

def _parse_dvb_date(text: str):
    """例: 'August 29, 2025' → date"""
    if not text:
        return None
    s = re.sub(r"\s+", " ", text.strip())
    try:
        return datetime.strptime(s, "%B %d, %Y").date()
    except ValueError:
        return None

def _abs(BASE: str, href: str) -> str:
    return href if href.startswith("http") else f"{BASE}{href}"

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
        txt = re.sub(r"\s+", " ", p.get_text(" ", strip=True))
        if txt:
            parts.append(txt)
    return "\n".join(parts).strip()

def collect_dvb_for_date(target_date_mmt: date, max_pages: int = 15) -> List[Dict]:
    """
    一覧（/category/8/news の Tailwind グリッド）から /post/ のみを収集。
    カード上の英語日付をパースして対象日一致のURLだけ候補化→記事ページ抽出。
    一覧各ページは最大3回トライして失敗はスキップ。
    """
    BASE = "https://burmese.dvb.no"
    CATEGORY_PATHS = ["/category/8/news"]

    try:
        sess = requests.Session()
    except Exception:
        sess = None

    candidate_urls: List[str] = []
    seen = set()

    # 一覧ページ: 1〜max_pages / 各ページ3回トライ
    for path in CATEGORY_PATHS:
        for page in range(1, max_pages + 1):
            url = f"{BASE}{path}" if page == 1 else f"{BASE}{path}?page={page}"
            ok = False
            for attempt in range(3):
                try:
                    res = fetch_with_retry_dvb(url, retries=2, wait_seconds=1, session=sess)
                    soup = BeautifulSoup(getattr(res, "content", None) or res.text, "html.parser")

                    # グリッド（無ければページ全体）をスコープに
                    scopes = soup.select("div.md\\:grid.grid-cols-3.gap-4.mt-5, div.grid.grid-cols-3.gap-4.mt-5") or [soup]

                    found = 0
                    for scope in scopes:
                        for a in scope.select('a[href^="/post/"]'):
                            href = a.get("href") or ""
                            # カードに埋め込まれている日付テキストを探す
                            date_div = a.select_one("div.flex.gap-1.text-xs.mt-2.text-gray-500 div")
                            raw = (date_div.get_text(" ", strip=True) if date_div else "").strip()
                            if not raw:
                                # フォールバック：アンカー全体から英語日付を拾う
                                full = a.get_text(" ", strip=True)
                                m = _DATE_RE.search(full)
                                raw = m.group(0) if m else ""
                            d = _parse_dvb_date(raw)
                            if d and d == target_date_mmt and href:
                                u = _abs(BASE, href)
                                if u not in seen:
                                    candidate_urls.append(u)
                                    seen.add(u)
                                    found += 1
                    ok = True
                    break
                except Exception as e:
                    print(f"[dvb] list fetch fail {url} ({attempt+1}/3): {e}")
                    time.sleep(1.0)
            if not ok:
                print(f"[dvb] skip list page: {url}")
                continue

    # 記事ページ抽出
    results: List[Dict] = []
    for url in candidate_urls:
        try:
            res = fetch_with_retry_dvb(url, retries=4, wait_seconds=2, session=sess)
            soup = BeautifulSoup(getattr(res, "content", None) or res.text, "html.parser")

            # 念のため meta の日付でも再確認（無ければスルー）
            meta = soup.find("meta", attrs={"property": "article:published_time"})
            if meta and meta.get("content"):
                try:
                    dt = datetime.fromisoformat(meta["content"]).astimezone(MMT)
                    if dt.date() != target_date_mmt:
                        continue
                except Exception:
                    pass

            title = _extract_title_dvb(soup)
            if not title:
                continue
            body = _extract_body_dvb(soup)

            results.append({
                "source": "DVB",
                "title": unicodedata.normalize("NFC", title),
                "url": url,
                "date": target_date_mmt.isoformat(),
                "body": unicodedata.normalize("NFC", body),
            })
        except Exception as e:
            print(f"[dvb] article fail {url}: {e}")
            continue

    return results

# ========== メイン ==========
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="2025-08-23", help="MMT基準の開始日 (YYYY-MM-DD)")
    parser.add_argument("--out", type=str, default="dvb_articles_since_2025-08-23_MMT.csv", help="出力CSVパス")
    # 無料枠を意識した保守的デフォルト（必要に応じて上げてください）
    parser.add_argument("--rpm", type=int, default=int(os.getenv("GEMINI_REQS_PER_MIN", "12")),
                        help="Requests per minute (default: env or 12)")
    parser.add_argument("--min-interval", type=float, default=float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "1.5")),
                        help="Minimum seconds between requests (default: env or 1.5)")
    parser.add_argument("--jitter", type=float, default=float(os.getenv("GEMINI_JITTER_SEC", "0.3")),
                        help="Random jitter [0..jitter] seconds per request (default: env or 0.3)")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("GEMINI_BATCH_SIZE", "25")),
                        help="Titles per request for batch translation (default: env or 25)")

    args = parser.parse_args(argv)

    # 今日(MMT)
    today_mmt = datetime.now(MMT).date()
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    except ValueError:
        print("ERROR: --start は YYYY-MM-DD 形式で指定してください。")
        return 2
    if start_date > today_mmt:
        print("ERROR: --start は今日(MMT)以前を指定してください。")
        return 2

    print(f"Collect DVB articles from {start_date.isoformat()} to {today_mmt.isoformat()} (MMT)")

    rows: List[Dict] = []
    for d in daterange_mmt(start_date, today_mmt):
        print(f"=== {d.isoformat()} (MMT) ===")
        rows.extend(collect_dvb_for_date(d, max_pages=15))

    # 翻訳（まずバッチ、欠けは単体フォールバック）
    limiter = RateLimiter(args.rpm, args.min_interval, args.jitter)
    pending_idx = [i for i, it in enumerate(rows) if not it.get("title_ja")]
    bs = max(1, int(args.batch_size))
    for s in range(0, len(pending_idx), bs):
        idxs = pending_idx[s:s+bs]
        batch_items = [rows[i] for i in idxs]
        limiter.wait()
        ja_list = translate_titles_in_batch(batch_items, model="gemini-2.5-flash-lite")
        if len(ja_list) != len(batch_items) or any(j == "" for j in ja_list):
            print(f"[batch-translate] fallback single: {len(batch_items)} items")
            for k, item in zip(idxs, batch_items):
                limiter.wait()
                rows[k]["title_ja"] = translate_title_only(item, model="gemini-2.5-flash-lite")
        else:
            for k, ja in zip(idxs, ja_list):
                rows[k]["title_ja"] = ja

    # CSV出力（UTF-8 BOM）
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["メディア名", "日本語タイトル", "発行日(MMT)", "URL"])
        for a in rows:
            dd = a.get("date") or ""
            # 念のためISO日付に丸め
            if "T" in dd:
                try:
                    dt = datetime.fromisoformat(dd)
                    dd = dt.astimezone(MMT).date().isoformat()
                except Exception:
                    pass
            w.writerow([a.get("source") or "DVB", a.get("title_ja") or "", dd, a.get("url") or ""])
    print(f"✅ CSV written: {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
