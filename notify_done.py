#!/usr/bin/env python3
import argparse, base64, json, os, sys, urllib.request, urllib.parse
from email.message import EmailMessage
from datetime import datetime, timezone
from email.header import Header
from email.utils import formataddr

TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"

def fetch_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    data = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode("utf-8")
    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "access_token" not in payload:
        raise RuntimeError(f"failed to get access_token: {payload}")
    return payload["access_token"]

def send_gmail(access_token: str, raw_mime: bytes) -> dict:
    body = json.dumps({"raw": base64.urlsafe_b64encode(raw_mime).decode("ascii")}).encode("utf-8")
    req = urllib.request.Request(GMAIL_SEND_URL, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def build_message(sender: str, to_list: list[str], subject: str, body: str) -> bytes:
    msg = EmailMessage()
    from_display_name = "Myanmar News Alert"
    # 表示名付きで From を設定（fetch_articles.py と同じ形式）
    msg["From"] = formataddr((str(Header(from_display_name, "utf-8")), sender))
    msg["To"] = ", ".join(to_list)
    msg["Subject"] = subject
    msg.set_content(body)
    return msg.as_bytes()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--to", required=True, help="comma-separated recipients")
    ap.add_argument("--subject", required=True)
    ap.add_argument("--body", required=True)
    ap.add_argument("--sheet-url", default="", help="Google Sheet URL to include")
    args = ap.parse_args()

    client_id = os.environ.get("GMAIL_CLIENT_ID")
    client_secret = os.environ.get("GMAIL_CLIENT_SECRET")
    refresh_token = os.environ.get("GMAIL_REFRESH_TOKEN")
    sender = os.environ.get("EMAIL_SENDER", "")  # オプション

    if not all([client_id, client_secret, refresh_token]):
        print("GMAIL_* secrets not set", file=sys.stderr)
        sys.exit(2)

    # 本文に Sheet URL を追記
    body_lines = [args.body]
    if args.sheet_url:
        body_lines.append(f"\nSpreadsheet: {args.sheet_url}")
    body_lines.append(f"\nSent at (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    full_body = "\n".join(body_lines)

    recipients = [x.strip() for x in args.to.split(",") if x.strip()]
    if not recipients:
        print("no recipients", file=sys.stderr)
        sys.exit(2)

    # sender が未指定でも OK（Gmail 側の認証ユーザーが使われる）
    if not sender:
        sender = "me"

    # アクセストークン取得 → 送信
    token = fetch_access_token(client_id, client_secret, refresh_token)
    raw = build_message(sender, recipients, args.subject, full_body)
    result = send_gmail(token, raw)
    # 最小限の結果表示
    print("gmail_send_ok:", bool(result.get("id")))
    print("gmail_message_id:", result.get("id"))

if __name__ == "__main__":
    main()
