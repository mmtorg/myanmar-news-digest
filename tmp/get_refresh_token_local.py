# -*- coding: utf-8 -*-
# python get_refresh_token_local.py実行してGMAIL_REFRESH_TOKEN再発行可能

import os, json
from google_auth_oauthlib.flow import InstalledAppFlow

# 1) 下の client_config をあなたの Client ID/Secret に置き換え
client_config = {
  "installed": {
    "client_id": "<YOUR_CLIENT_ID>",
    "project_id": "<YOUR_PROJECT_ID>",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "client_secret": "<YOUR_CLIENT_SECRET>",
    "redirect_uris": ["http://localhost"]
  }
}

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def main():
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline", include_granted_scopes="true")
    print("\n=== TOKENS ===")
    print("access_token:", creds.token)
    print("refresh_token:", creds.refresh_token) 
    print("token_uri:", creds.token_uri)
    print("client_id:", creds.client_id)
    print("client_secret:", creds.client_secret)
    print("scopes:", creds.scopes)

if __name__ == "__main__":
    main()
