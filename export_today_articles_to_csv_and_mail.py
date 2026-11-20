# -*- coding: utf-8 -*-
"""
export_today_articles_to_csv_and_mail.py

目的:
- 各メディアの「キーワード絞り込み前」の“本日(MMT)”の記事を収集
- タイトルを gemini-3.1-flash-lite で日本語に一括(バッチ)翻訳
- CSV (UTF-8 BOM) を 1列目:発行日(MMT) / 2列目:メディア名 / 3列目:日本語タイトル で出力
# 以前の URL 列は削除（将来復活のため該当処理はコメントアウト）
- CSV を Gmail API で指定アドレスへ送付（fetch_articles.py と同方式）
- 送信後は CSV を削除してストレージ抑制
- 無料運用: バッチ翻訳 + レートリミッタ (デフォルト9RPM, 100件/リクエスト)

使い方(ローカル実行例):
  GEMINI_API_SUMMARY_KEY=... \
  GMAIL_CLIENT_ID=... GMAIL_CLIENT_SECRET=... GMAIL_REFRESH_TOKEN=... \
  EMAIL_SENDER=you@example.com CSV_EMAIL_RECIPIENTS=to@example.com \
  python export_today_articles_to_csv_and_mail.py \
    --out today_MMT.csv --batch-size 100 --rpm 9 --min-interval 2.0 --jitter 0.3

GitHub Actions からは下の workflow を参照。
"""

from __future__ import annotations
import argparse
import csv
import os
import sys
import time
import json
import base64
import unicodedata
from email.message import EmailMessage
from email.utils import formataddr
from email.policy import SMTP
from email.header import Header
from datetime import datetime
from typing import List, Dict
from datetime import date

# ===== 既存コードから再利用 =====
# 1) 収集・Gemini呼び出し・定数 (MMT/Irrawaddy/各種フェッチ) など
from fetch_articles import (
    MMT,
    call_gemini_with_retries,
    client_summary,
    deduplicate_by_url,
)
from tmp.export_all_articles_to_csv import (
    collect_bbc_all_for_date,
    collect_khitthit_all_for_date,
    collect_mizzima_all_for_date,
    collect_dvb_all_for_date,
    collect_irrawaddy_all_for_date,
    collect_myanmar_now_mm_all_for_date,
    collect_gnlm_all_for_date,
    collect_popular_all_for_date,
    collect_frontier_all_for_date,
    collect_jetro_biznews_mm_all_for_date,
    translate_titles_in_batch,
    translate_title_only,
    RateLimiter,
)

# ===== Gmail APIは fetch_articles.py と同じやり方で使う =====
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re

# --- force UTF-8 stdout/stderr for GitHub Actions ---
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass
def _build_gmail_service():
    cid = os.getenv("GMAIL_CLIENT_ID")
    csec = os.getenv("GMAIL_CLIENT_SECRET")
    rtok = os.getenv("GMAIL_REFRESH_TOKEN")
    if not (cid and csec and rtok):
        raise RuntimeError(
            "Gmail API credentials (CLIENT_ID/SECRET/REFRESH_TOKEN) are missing."
        )
    creds = Credentials(
        token=None,
        refresh_token=rtok,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def send_csv_via_gmail(csv_path: str, *, subject: str, body_text: str) -> None:
    """
    fetch_articles.py と同じ Gmail API 方式で CSV を添付して送信。
    宛先は CSV_EMAIL_RECIPIENTS のみ（カンマ/セミコロン/空白区切り対応）。
    """
    sender_email = os.getenv("EMAIL_SENDER")
    recipients_env = os.getenv("CSV_EMAIL_RECIPIENTS", "")
    # カンマ/セミコロン/空白で分割し、空要素を除去
    recipients = [r.strip() for r in re.split(r"[,\s;]+", recipients_env) if r.strip()]
    if not sender_email:
        raise RuntimeError("EMAIL_SENDER is not set.")
    if not recipients:
        raise RuntimeError("CSV_EMAIL_RECIPIENTS is not set or empty.")

    # 本文はプレーンテキストのみ（HTML不要）
    from_display_name = "Myanmar News CSV"
    msg = EmailMessage(policy=SMTP)
    msg["Subject"] = subject.strip().replace("\n", " ")
    msg["From"] = formataddr((str(Header(from_display_name, "utf-8")), sender_email))
    msg["To"] = ", ".join(recipients)
    msg.set_content(body_text, charset="utf-8")

    # 添付（UTF-8 BOM のCSV）
    with open(csv_path, "rb") as f:
        data = f.read()
    msg.add_attachment(
        data,
        maintype="text",
        subtype="csv",
        filename=os.path.basename(csv_path),
        disposition="attachment",
    )

    try:
        service = _build_gmail_service()
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        body = {"raw": raw}
        sent = service.users().messages().send(userId="me", body=body).execute()
        print("✅ Gmail 送信完了 messageId:", sent.get("id"))
    except HttpError as e:
        try:
            content = e.content.decode("utf-8", "replace") if hasattr(e, "content") else ""
        except Exception:
            content = ""
        print("❌ Gmail API HttpError")
        print(f"   status: {getattr(e, 'status_code', getattr(getattr(e, 'resp', None), 'status', 'unknown'))}")
        print(f"   details: {content}")
        raise


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s or "")


def _jp_date(d: date) -> str:
    """YYYY年M月D日の日本語表記（MMTの今日を想定）"""
    return f"{d.year}年{d.month}月{d.day}日"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="myanmar_news.csv", help="出力CSVパス")
    # free tier を前提に厳しめの既定
    parser.add_argument("--rpm", type=int, default=int(os.getenv("GEMINI_REQS_PER_MIN", "9")))
    parser.add_argument("--min-interval", type=float, default=float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "2.0")))
    parser.add_argument("--jitter", type=float, default=float(os.getenv("GEMINI_JITTER_SEC", "0.3")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("GEMINI_BATCH_SIZE", "100")))
    args = parser.parse_args(argv)

    # 本日（MMT）
    today_mmt = datetime.now(MMT).date()
    print(f"=== Collecting articles for {today_mmt.isoformat()} (MMT) ===")

    # 収集（“キーワード絞り込み前”）
    all_rows = []
    try:
        # Irrawaddy（キーワード絞り込み前、当日MMTのみ）
        irw = collect_irrawaddy_all_for_date(today_mmt, debug=True)
    except Exception as e:
        print(f"[irrawaddy] fail: {e}")
        irw = []
        
    # # Irrawaddyログ出力（件数＋先頭数件を表示）
    # try:
    #     print(f"[irrawaddy] collected: {len(irw)} items for {today_mmt}")
    #     for a in irw[:10]:
    #         title = (a.get("title") or "").replace("\n", " ").strip()
    #         url = a.get("url", "")
    #         d = a.get("date", "")
    #         print(f"  - {d} | {title[:80]} | {url}")
    #     if len(irw) > 10:
    #         print(f"  ... (+{len(irw)-10} more)")
    # except Exception:
    #     pass
    
    all_rows.extend(irw)

    # 他メディア
    all_rows.extend(collect_bbc_all_for_date(today_mmt))
    all_rows.extend(collect_khitthit_all_for_date(today_mmt, max_pages=5))
    # DVB（フィルタなし、1〜15ページ対応）
    all_rows.extend(collect_dvb_all_for_date(today_mmt))
    all_rows.extend(collect_mizzima_all_for_date(today_mmt, max_pages=3))
    # Myanmar Now (mm) — 今日分（フィルタなし）
    all_rows.extend(collect_myanmar_now_mm_all_for_date(today_mmt, max_pages=3))
    # GNLM — 今日分（フィルタなし）
    all_rows.extend(collect_gnlm_all_for_date(today_mmt, max_pages=3))
    # Popular Myanmar — 今日分（フィルタなし）
    all_rows.extend(collect_popular_all_for_date(today_mmt))
    # Frontier Myanmar
    all_rows.extend(collect_frontier_all_for_date(today_mmt))
    # JETRO
    all_rows.extend(collect_jetro_biznews_mm_all_for_date(today_mmt, max_pages=1))

    # URL重複は既存関数で除去
    all_rows = deduplicate_by_url(all_rows)
    print(f"Total unique articles: {len(all_rows)}")

    # タイトル日本語化（まずバッチ、欠損は単品フォールバック）
    limiter = RateLimiter(args.rpm, args.min_interval, args.jitter)
    pending_idx = [i for i, it in enumerate(all_rows) if not it.get("title_ja")]
    bs = max(1, int(args.batch_size))
    for s in range(0, len(pending_idx), bs):
        idxs = pending_idx[s : s + bs]
        batch_items = [all_rows[i] for i in idxs]
        limiter.wait()
        ja_list = translate_titles_in_batch(batch_items)
        if len(ja_list) != len(batch_items) or any(j.strip() == "" for j in ja_list):
            print(f"[batch-translate] fallback single: {len(batch_items)} items")
            for k, item in zip(idxs, batch_items):
                limiter.wait()
                all_rows[k]["title_ja"] = translate_title_only(item)
        else:
            for k, ja in zip(idxs, ja_list):
                all_rows[k]["title_ja"] = ja

    # CSV 出力（UTF-8 BOM, 列順: 発行日 / メディア名 / 日本語タイトル）
    csv_path = args.out
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        # 列名は「発行日 / メディア名 / 日本語タイトル / URL」
        w.writerow(["発行日", "メディア名", "日本語タイトル", "URL"])
        for a in all_rows:
            # 発行日フィールドの整形（ISO, MMT）
            dd = a.get("date") or ""
            if "T" in dd:
                try:
                    dt = datetime.fromisoformat(dd)
                    dd = dt.astimezone(MMT).date().isoformat()
                except Exception:
                    pass
            # 出力順: 発行日 / メディア名 / 日本語タイトル
            w.writerow([
                dd,
                _nfc(a.get("source")),
                _nfc(a.get("title_ja") or a.get("title")),
                a.get("url", ""),
            ])
    print(f"✅ CSV written: {csv_path}")

    # メール送信（成功時のみ削除）
    _djp = _jp_date(today_mmt)
    # 件名・本文はご指定のフォーマット（例: ミャンマー記事CSV【2025年8月28日分】）
    subject = f"ミャンマー記事CSV【{_djp}分】"
    body = f"ミャンマー記事CSV【{_djp}分】を送ります。"
    try:
        send_csv_via_gmail(csv_path, subject=subject, body_text=body)
        try:
            os.remove(csv_path)
            print(f"🧹 CSV deleted: {csv_path}")
        except Exception as e:
            print(f"⚠️ CSV delete failed (kept on runner): {e}")
    except Exception:
        print("❌ メール送信に失敗したため CSV は残しておきます。")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
