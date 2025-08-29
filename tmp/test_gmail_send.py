# -*- coding: utf-8 -*-
# test_gmail_send.py
import os, sys, base64, re
from email.message import EmailMessage
from email.utils import formataddr
from email.policy import SMTP
from email.header import Header
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def _build_gmail_service():
    cid  = os.getenv("GMAIL_CLIENT_ID")
    csec = os.getenv("GMAIL_CLIENT_SECRET")
    rtok = os.getenv("GMAIL_REFRESH_TOKEN")
    if not (cid and csec and rtok):
        raise RuntimeError("GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN が未設定です。")
    # fetch_articles.py と同様、Refresh Token 方式（scopesはRefresh Token側に紐づく）
    creds = Credentials(
        token=None,
        refresh_token=rtok,
        token_uri="https://www.googleapis.com/oauth2/v4/token",
        client_id=cid,
        client_secret=csec,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

    sender = os.getenv("EMAIL_SENDER")
    # 受信者は CSV_EMAIL_RECIPIENTS のみ
    recips_env = os.getenv("CSV_EMAIL_RECIPIENTS", "")
    recips = [r.strip() for r in re.split(r"[,\s;]+", recips_env) if r.strip()]
    # ★ ログに中身を出す（Secrets の場合は GitHub により自動マスクされる点に注意）
    print(f"CSV_EMAIL_RECIPIENTS (raw): {recips_env}")
    print(f"CSV_EMAIL_RECIPIENTS (parsed list): {recips}")
    if not sender:
        raise RuntimeError("EMAIL_SENDER が未設定です。")
    if not recips:
        raise RuntimeError("CSV_EMAIL_RECIPIENTS が未設定/空です。")

    attach = sys.argv[1] if len(sys.argv) > 1 else "mail_test.csv"
    if not os.path.exists(attach):
        with open(attach, "w", encoding="utf-8-sig") as f:
            f.write("col1,col2\nhello,world\n")

    msg = EmailMessage(policy=SMTP)
    msg["Subject"] = "Gmail API テスト送信"
    msg["From"] = formataddr((str(Header("Myanmar News CSV", "utf-8")), sender))
    msg["To"] = ", ".join(recips)
    msg.set_content("これはテストメールです（CSV添付あり）。", charset="utf-8")
    with open(attach, "rb") as f:
        data = f.read()
    msg.add_attachment(
        data, maintype="text", subtype="csv",
        filename=os.path.basename(attach), disposition="attachment"
    )

    print(f"📤 sending from {sender} to {recips} attach={os.path.abspath(attach)}")
    try:
        svc = _build_gmail_service()
        try:
            prof = svc.users().getProfile(userId="me").execute()
            print(f"👤 Gmail account (me): {prof.get('emailAddress')}")
        except Exception as e:
            print(f"(profile check skipped: {e})")

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        sent = svc.users().messages().send(userId="me", body={"raw": raw}).execute()
        print(f"✅ sent messageId={sent.get('id')}")
        return 0

    except HttpError as e:
        status = getattr(e, "status_code", getattr(getattr(e, "resp", None), "status", "unknown"))
        try:
            details = e.content.decode("utf-8", "replace")
        except Exception:
            details = ""
        print("❌ HttpError while sending")
        print("   status:", status)
        print("   details:", details)
        return 1
    except Exception as e:
        import traceback
        print("❌ Unexpected error:", e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
