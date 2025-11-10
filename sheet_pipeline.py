# scripts/mna_sheet_pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, json, unicodedata, shutil
import threading
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Protocol, runtime_checkable, cast
from typing import List, Dict
import time
import os
import re

import logging, contextlib, time

# --- ensure we can import helpers living next to this file ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

def _setup_logger():
    level = (os.getenv("MNA_LOG_LEVEL") or "INFO").upper()
    try:
        level_val = getattr(logging, level)
    except Exception:
        level_val = logging.INFO
    logging.basicConfig(
        level=level_val,
        format="%(asctime)s %(levelname)s %(message)s",
    )
_setup_logger()

@contextlib.contextmanager
def _timeit(section: str, **fields):
    meta = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    logging.info(f"▶ {section}... {meta}".rstrip())
    t0 = time.time()
    try:
        yield
        dt = time.time() - t0
        logging.info(f"✔ {section} done ({dt:.2f}s)")
    except Exception:
        logging.exception(f"✖ {section} failed")
        raise
    
def _jp_date(s: str) -> str:
    if not s:
        return ""
    # 代表的な2形式を許容
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except Exception:
            pass
    # だめならそのまま返す（念のため）
    return s

def _coerce_date(d) -> date | None:
    """YYYY-MM-DD / YYYY/MM/DD / datetime/ date を date に正規化。失敗時は None。"""
    if isinstance(d, date):
        return d
    if isinstance(d, datetime):
        return d.date()
    s = (d or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

# ===== 既存の fetch_articles.py から再利用できるものを拝借（※プロンプトは本ファイルで管理） =====
try:
    from fetch_articles import (
        MMT,                      # UTC+6:30
        call_gemini_with_retries, # Gemini呼び出し（既存のリトライ/レート制御）
        client_summary,
        deduplicate_by_url,
    )
except Exception:
    MMT = timezone(timedelta(hours=6, minutes=30))
    call_gemini_with_retries = None
    client_summary = None
    def deduplicate_by_url(items):
        seen, out = set(), []
        for it in items:
            u = (it.get("url") or "").strip()
            if u and u not in seen:
                out.append(it); seen.add(u)
        return out
    
# === fetch_articles.py の文字数制御に揃える（import できなければフォールバック） ===
try:
    from fetch_articles import trim_by_chars as _trim_by_chars_impl
except Exception:
    _trim_by_chars_impl = None

# 明示的な上限（fetch_articles.py の FULLTEXT_MAX_CHARS=100_000 に合わせる）
FULLTEXT_MAX_CHARS = int(os.getenv("FULLTEXT_MAX_CHARS", "100000"))

def trim_by_chars(s: str, max_chars: int) -> str:
    """
    fetch_articles.py の trim_by_chars 相当。
    import に成功していればそちらを使い、失敗時のみ簡易版で代替。
    """
    if _trim_by_chars_impl:
        return _trim_by_chars_impl(s, max_chars)
    s = (s or "")
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    suffix = "…（本文が長いためここまでを翻訳）"
    return s[: max(0, max_chars - len(suffix))] + suffix

try:
    # Irrawaddy 専用の取得/抽出を使い回す
    from fetch_articles import (
        fetch_with_retry_irrawaddy,
        extract_body_irrawaddy,
    )
except Exception:
    fetch_with_retry_irrawaddy = None
    extract_body_irrawaddy = None
    
# --- 用語集（州・管区訳）: タイトル=D / 本文=C を fetch_articles の実装で再利用 ---
try:
    from fetch_articles import (
        _load_region_glossary_gsheet as _fa_load_regions,
        _select_region_entries_for_text as _fa_sel_regions,
        _build_region_glossary_prompt_for as _fa_build_rg_for,
        _apply_region_glossary_to_text as _fa_apply_region_glossary_to_text,
    )
    _REGION_CACHE: list[dict] | None = None
    def _regions():
        global _REGION_CACHE
        if _REGION_CACHE is None:
            _REGION_CACHE = _fa_load_regions(
                os.getenv("MNA_SHEET_ID"),
                os.getenv("MNA_REGION_SHEET_NAME") or "regions",
            )
        return _REGION_CACHE or []
    def _region_rules_for_title(title: str) -> str:
        return _fa_build_rg_for(_fa_sel_regions(title or "", _regions()), use_headline_ja=True)
    def _region_rules_for_body(body: str) -> str:
        return _fa_build_rg_for(_fa_sel_regions(body or "", _regions()), use_headline_ja=False)
    def _apply_region_glossary_to_text(s: str) -> str:
        return _fa_apply_region_glossary_to_text(s)
except Exception:
    def _region_rules_for_title(_: str) -> str: return ""
    def _region_rules_for_body(_: str) -> str: return ""
    def _apply_region_glossary_to_text(s: str) -> str: return s

# ===== 本文キャッシュ（bundle/bodies.json） & 本文取得  =====
_BODIES_LOCK = threading.Lock()

def _bodies_cache_path(out_dir: str) -> str:
    return os.path.join(out_dir, "bodies.json")

def _load_bodies_cache(out_dir: str) -> dict[str, dict]:
    p = _bodies_cache_path(out_dir)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_bodies_cache(out_dir: str, cache: dict[str, dict]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(_bodies_cache_path(out_dir), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

import requests
from bs4 import BeautifulSoup
try:
    # fetch_articles.py の実装を再利用
    from fetch_articles import (
    get_body_with_refetch,
    fetch_once_irrawaddy,
    extract_body_irrawaddy,
    extract_body_generic_from_soup,
    extract_body_mail_pdf_scoped,
    translate_fulltexts_for_business,
    build_combined_pdf_for_business,
    _jp_date,
    )
    from fetch_articles import fetch_with_retry_dvb  # ★DVB専用フェッチャ（既存を呼ぶだけ）
except Exception:
    get_body_with_refetch = None
    fetch_once_irrawaddy = None
    extract_body_irrawaddy = None
    extract_body_generic_from_soup = None
    translate_fulltexts_for_business = None
    build_combined_pdf_for_business = None
    _jp_date = None

def _simple_fetch(url: str) -> str:
    r = requests.get(url, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def _fetch_once_dvb(url: str, session: Optional[requests.Session] = None) -> bytes:
    """
    DVB 用：fetch_articles.py の fetch_with_retry_dvb を “1回だけ”呼ぶラッパ。
    """
    try:
        r = fetch_with_retry_dvb(url, retries=1, wait_seconds=0, session=session)
        if hasattr(r, "content") and r.content:
            return r.content
        t = getattr(r, "text", "") or ""
        return t.encode("utf-8", "ignore")
    except Exception:
        try:
            rr = requests.get(url, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
            rr.raise_for_status()
            return rr.content
        except Exception:
            return b""

def _extract_body_dvb_first_then_scoped(url: str, soup: "BeautifulSoup") -> str:
    """
    DVB は本文が .full_content に入っていることが多い。
    まず .full_content p を試し、ダメなら extract_body_mail_pdf_scoped にフォールバック。
    """
    host = soup.select_one(".full_content")
    if host:
        parts = []
        for p in host.select("p"):
            txt = p.get_text(" ", strip=True)
            txt = re.sub(r"\s+", " ", txt)
            if txt:
                parts.append(txt)
        body = "\n".join(parts).strip()
        if body:
            return body
    return extract_body_mail_pdf_scoped(url, soup)

def _get_body_once(url: str, source: str, out_dir: str, title: str = "") -> str:
    """
    1) bundle/bodies.json を見てあれば返す
    2) なければ取得→保存→返す
    - Irrawaddy は本文抽出失敗時に r.jina.ai → AMP (/amp, ?output=amp) の順でフォールバック
    - Google News (news.google.com/rss/articles/...) は最終到達URLを解決してから試行
    """
    from urllib.parse import urlparse
    import requests
    from bs4 import BeautifulSoup

    def _resolve_news_google_redirect(u: str, timeout: int = 20) -> str:
        """news.google.com/rss/articles/... を publisher の最終URLへ解決（成功時のみ差し替え）"""
        try:
            r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, allow_redirects=True)
            final_url = getattr(r, "url", "") or u
            return final_url
        except Exception:
            return u

    def _fetch_text_via_jina(u: str, timeout: int = 25) -> str:
        """r.jina.ai 経由でプレーンテキストを取得（本文がHTMLで取れなかった時の救済）"""
        try:
            alt = f"https://r.jina.ai/http://{u.lstrip('/')}"
            r = requests.get(alt, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if r.status_code == 200:
                txt = (r.text or "").strip()
                if txt:
                    return txt
        except Exception:
            pass
        return ""

    # --- 1) キャッシュ命中なら即返す ---
    cache = _load_bodies_cache(out_dir)
    cached = cache.get(url)
    if cached and (cached.get("body") or "").strip():
        return cached["body"]

    # --- 事前正規化：Google News の場合は最終到達URLを一度解決 ---
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if "news.google.com" in host:
        url = _resolve_news_google_redirect(url)

    # --- 2) 本文取得（Irrawaddy / DVB は専用フェッチャ。その他は共通フェッチャ＆抽出器）---
    body = ""
    if get_body_with_refetch is None:
        # フォールバック（極力通らない想定）: 既存の共通フェッチャ＆抽出器で最低限動かす
        try:
            html = fetch_once_requests(url)
            soup = BeautifulSoup(html, "html.parser")
            raw = extract_body_generic_from_soup(soup) or ""
            body = trim_by_chars(raw, FULLTEXT_MAX_CHARS)
        except Exception:
            body = ""
    else:
        try:
            src_l = (source or "").lower()
            url_l = (url or "").lower()

            if "irrawaddy" in url_l or "irrawaddy" in src_l:
                # ★ Irrawaddy: 専用フェッチャ + 専用抽出器
                def _irw_fetch(u: str) -> str:
                    try:
                        res = fetch_with_retry_irrawaddy(u)  # fetch_articles.py 側の関数
                        return getattr(res, "text", "") or getattr(res, "content", b"").decode("utf-8", "ignore")
                    except Exception:
                        return ""
                html_fetcher = _irw_fetch
                extractor    = extract_body_irrawaddy

            elif "dvb" in url_l or "burmese.dvb.no" in url_l or "dvb" in src_l:
                # ★ DVB: 専用フェッチャ + DVB向け抽出（失敗時は強化抽出器にフォールバック）
                html_fetcher = lambda u: _fetch_once_dvb(u, session=requests.Session())
                extractor    = lambda soup, _u=url: _extract_body_dvb_first_then_scoped(_u, soup)

            else:
                # ★ その他: fetch_articles.py 側の“実在する”共通フェッチャ＆抽出器を使う
                #   - フェッチャ: fetch_once_requests
                #   - 抽出器: extract_body_mail_pdf_scoped があればそれ、無ければ extract_body_generic_from_soup
                from fetch_articles import fetch_once_requests, extract_body_generic_from_soup
                html_fetcher = fetch_once_requests
                if globals().get("extract_body_mail_pdf_scoped") is not None:
                    def _scoped(soup, _u=url):
                        return extract_body_mail_pdf_scoped(_u, soup)
                    extractor = _scoped
                else:
                    extractor = extract_body_generic_from_soup

            body = get_body_with_refetch(
                url,
                html_fetcher,
                extractor,
                retries=2,
                wait_seconds=2,
                quiet=True
            ) or ""
        except Exception:
            body = ""

    # --- 3) Irrawaddy に限り：本文が空なら Jina → AMP で救済（CSVパイプライン相当のBを移植）---
    if not (body or "").strip():
        src_l = (source or "").lower()
        url_l = (url or "").lower()
        if "irrawaddy" in url_l or "irrawaddy" in src_l:
            # 3-1) まず元URLで r.jina.ai
            txt = _fetch_text_via_jina(url, timeout=25)
            # 3-2) /news/ を含む場合は AMP も試す（/amp, ?output=amp）
            if not txt:
                try:
                    p = urlparse(url)
                    if "/news/" in p.path:
                        amp1 = url.rstrip("/") + "/amp"
                        q = "&" if "?" in url else "?"
                        amp2 = url + f"{q}output=amp"
                        for amp in (amp1, amp2):
                            txt = _fetch_text_via_jina(amp, timeout=25)
                            if txt:
                                break
                except Exception:
                    pass
            body = (txt or "").strip()

    # --- 4) 空本文はキャッシュしない（将来の再取得の余地を残す）---
    if body.strip():
        with _BODIES_LOCK:
            cache = _load_bodies_cache(out_dir)  # 競合対策で再読込
            cache[url] = {"source": source, "title": title, "body": body}
            _save_bodies_cache(out_dir, cache)

    return body

# ===== 翻訳プロンプト：共通ルール（fetch_articles.py と同一） =====
PROMPT_TERMINOLOGY_RULES = (
    "【翻訳時の用語統一ルール（必ず従うこと）】\n"
    "このルールは記事タイトルと本文の翻訳に必ず適用してください。\n"
    "クーデター指導者⇒総司令官\n"
    "テロリスト指導者ミン・アウン・フライン⇒ミン・アウン・フライン\n"
    "テロリストのミン・アウン・フライン⇒ミン・アウン・フライン\n"
    "テロリスト軍事指導者⇒総司令官\n"
    "テロリスト軍事政権⇒軍事政権\n"
    "テロリスト軍事評議会⇒軍事政権\n"
    "テロリスト軍⇒国軍\n"
    "軍事評議会⇒軍事政権\n"
    "軍事委員会⇒軍事政権\n"
    "徴用⇒徴兵\n"
    "軍事評議会軍⇒国軍\n"
    "アジア道路⇒アジアハイウェイ\n"
    "来客登録⇒宿泊登録\n"
    "来客登録者⇒宿泊登録者\n"
    "タウンシップ⇒郡区\n"
    "北オークカラパ⇒北オカラッパ\n"
    "北オカラパ⇒北オカラッパ\n"
    "サリンギ郡区⇒タンリン郡区\n"
    "ネーピードー⇒ネピドー\n"
    "ファシスト国軍⇒国軍\n"
    "クーデター軍⇒国軍\n"
    "ミャンマー国民⇒ミャンマー人\n"
    "タディンユット⇒ダディンジュ\n"
    "ティティンジュット⇒ダディンジュ\n"
)
PROMPT_SPECIAL_RULES = (
    "【翻訳時の特別ルール】\n"
    "このルールも記事タイトルと本文の翻訳に必ず適用してください。\n"
    "「ဖမ်းဆီး」の訳語は文脈によって使い分けること。\n"
    "- 犯罪容疑や法律違反に対する文脈の場合は「逮捕」とする。\n"
    "- 犯罪容疑や法律違反に基づかない文脈の場合は「拘束」とする。\n"
)
PROMPT_CURRENCY_RULES = (
    "【通貨換算ルール】\n"
    "このルールも記事タイトルと本文の翻訳に必ず適用してください。\n"
    "ミャンマー通貨「チャット（Kyat、ကျပ်）」が出てきた場合は、日本円に換算して併記してください。\n"
    "- 換算レートは 1チャット = 0.037円 を必ず使用すること。\n"
    "- 記事中にチャットが出た場合は必ず「◯チャット（約◯円）」の形式に翻訳してください。\n"
    "- 日本円の表記は小数点以下は四捨五入してください（例: 16,500円）。\n"
    "- 他のレートは使用禁止。\n"
    "- チャット以外の通貨（例：タイの「バーツ」や米ドルなど）には適用しない。換算は行わないこと。\n"
)
COMMON_TRANSLATION_RULES = (
    PROMPT_TERMINOLOGY_RULES + "\n" +
    PROMPT_SPECIAL_RULES + "\n" +
    PROMPT_CURRENCY_RULES + "\n"
)

TITLE_OUTPUT_RULES = (
    "出力は見出し文だけを1行で返してください。\n"
    "【翻訳】や【日本語見出し案】、## 翻訳 などのラベル・注釈タグ・見出しは出力しないでください。\n"
    "文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
)

# ===== ▼ プロンプト管理（見出し翻訳 / 本文要約）================================
# 見出し翻訳（見出し3案）※共通ルールを含む
HEADLINE_PROMPT_1 = (
    f"{COMMON_TRANSLATION_RULES}"
    f"{TITLE_OUTPUT_RULES}"
    "あなたは報道見出しの専門翻訳者です。以下の英語/ビルマ語のニュース見出しタイトルを、"
    "自然で簡潔な日本語見出しに翻訳してください。固有名詞は一般的な日本語表記を優先し、"
    "意訳しすぎず要点を保ち、記号の乱用は避けます。\n"
)

def make_headline_prompt_2_from(variant1_ja: str) -> str:
    """
    案1（日本語見出し）をインプットにして、要件に沿った案2を生成する。
    """
    return (
        f"{COMMON_TRANSLATION_RULES}"
        f"{TITLE_OUTPUT_RULES}"
        "以下は先に作成した日本語見出し（案1）です。\n"
        f"【案1】{variant1_ja}\n\n"
        "この案1を素材に、次の要件で新しい別案（案2）を1行で出力してください。\n"
        "・直訳ではなく、ニュース見出しとして自然な日本語にする\n"
        "・30文字以内で要点を端的に\n"
        "・主語・動作を明確に\n"
        "・重複語を避ける\n"
        "・報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）\n"
        "・「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける\n"
    )

# 本文から要素抽出して新聞見出しを作る（日本語1行）
HEADLINE_PROMPT_3 = (
    f"{COMMON_TRANSLATION_RULES}"
    f"{TITLE_OUTPUT_RULES}"
    "あなたは新聞社の見出しデスクです。以下の本文（原文／機械翻訳含む可能性あり）を読み、"
    "記事の要点（誰／どこ／何が起きた／規模・数値／結果／時点）を抽出し、"
    "自然で簡潔な**日本語の報道見出し**を1行で作成してください。\n"
    "ルール：\n"
    "- 主語と動作を明確に（曖昧表現や冗長な修飾は削除）\n"
    "- 重要な固有名詞・数値は優先して残す\n"
    "- 「〜と述べた」「〜が行われた」等の婉曲は避ける\n"
    "- 事実関係が曖昧な断定は避ける（推定語を最小限に）\n"
)

# ===== クリーニング手順（必要に応じて適用） =====
STEP12_FILTERS = (
    "Step 1: 例外チェック（最優先）\n"
    "Q1. タイトルや本文が“写真キャプション/クレジットのみ”“媒体名のみ”“出典/翻訳注記のみ”“記者名/配信ラベルのみ”“Datelineのみ”など、"
    "本文に相当しない要素だけの場合は本文扱いから除外する。\n\n"
    "Step 2: 入力クレンジング（本文抽出）\n"
    "- 写真キャプション/クレジット行（例:『写真:』『ဓာတ်ပုံ』『Photo』『(写真』『(Photo』『（写真』等で始まる）を除外\n"
    "- 媒体名だけの行（例: BBC Burmese / DVB / Myanmar Now 等）を除外\n"
    "- 出典や翻訳注記（例:『Source: …』『Translated by …』等）を除外\n"
    "- 記者名や配信ラベルだけの行（例: By … / Reuters / AP / SCMP 等）を除外\n"
    "- 発行地＋日付（Dateline）のみの行（例: 'Yangon, Sept. 30' / 'ネピドー、2024年2月15日' 等）を除外\n"
    "  ※行頭や本文冒頭のこれらも必ず除去する。\n"
    "- 連続する空行は1つに圧縮し、本文段落のみ残す。\n"
)

# ===== 統合タスク（本文要約） =====
STEP3_TASK = (
    "Step 3: 翻訳と要約処理\n"
    "以下のルールに従って、本文を要約してください。\n\n"
    f"{COMMON_TRANSLATION_RULES}"
    "本文要約：\n"
    "- 以下の記事本文について重要なポイントをまとめ、最大500字で具体的に要約する（500字を超えない）。\n"
    "- 自然な日本語に翻訳する。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
    "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
    "- レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n\n"
    "本文要約の出力条件：\n"
    "- 1行目は`【要約】`とだけしてください。\n"
    "- 2行目以降が全て空行になってはいけません。\n"
    "- 見出しや箇条書きを適切に使って整理してください。\n"
    "- 見出しや箇条書きにはマークダウン記号（#, *, - など）を使わず、単純なテキストとして出力してください。\n"
    "- 見出しは `[ ]` で囲んでください。\n"
    "- 空行は作らないでください。\n"
    "- 特殊記号は使わないでください（全体をHTMLとして送信するわけではないため）。\n"
    "- 箇条書きは`・`を使ってください。\n"
    "- 「【要約】」は1回だけ書き、途中や本文の末尾には繰り返さないでください。\n"
    "- 思考用の手順（Step 1/2/3、Q1/Q2、→ など）は出力に含めないこと。\n"
    "- 本文要約の合計は最大500文字以内に収める。超えそうな場合は重要情報を優先して削る（日時・主体・行為・規模・結果を優先）。\n\n"
)

def _build_summary_prompt(item: dict, *, body_max: int) -> str:
    """Gemini用の要約プロンプトを生成（Irrawaddy特例なし、超要約も削除済み）"""
    header = "次の手順で記事を判定・処理してください。\n\n"
    pre = STEP12_FILTERS + "\n\n"
    body = (item.get("body") or "")[:max(body_max, 0)]
    input_block = (
        "入力データ：\n"
        "###\n[記事タイトル]\n###\n"
        f"{item.get('title') or ''}\n\n"
        "[記事本文]\n###\n"
        f"{body}\n"
        "###\n"
    )
    rg_title = _region_rules_for_title(item.get("title") or "")
    rg_body  = _region_rules_for_body(body)
    term_rules = _build_term_rules_prompt(item.get("title") or "", body)
    return header + pre + STEP3_TASK + (rg_title + rg_body) + term_rules + "\n" + input_block

# ===== 用語集（A:Myanmar / B:English / C:本文訳 / D:見出し訳） =====
_TERM_CACHE: list[dict] | None = None
TERM_SHEET_ID = os.getenv("MNA_TERM_SHEET_ID")
TERM_SHEET_NAME = os.getenv("MNA_TERM_SHEET_NAME") or "regions"

def _load_term_glossary_gsheet() -> list[dict]:
    """用語集を {mm,en,body_ja,title_ja} の配列で返す。失敗時は空配列。"""
    global _TERM_CACHE
    if _TERM_CACHE is not None:
        return _TERM_CACHE
    try:
        gc = _gc_client()
        ws = gc.open_by_key(TERM_SHEET_ID).worksheet(TERM_SHEET_NAME)
        vals = ws.get_all_values() or []
        rows = []
        for r in (vals[1:] if len(vals) > 1 else []):
            mm = (r[0] if len(r) > 0 else "").strip()
            en = (r[1] if len(r) > 1 else "").strip()
            bj = (r[2] if len(r) > 2 else "").strip()  # 本文訳（C）
            tj = (r[3] if len(r) > 3 else "").strip()  # 見出し訳（D）
            if not (mm or en):
                continue
            rows.append({"mm": mm, "en": en, "body_ja": bj, "title_ja": tj})
        _TERM_CACHE = rows
    except Exception:
        _TERM_CACHE = []
    return _TERM_CACHE

def _build_term_rules_prompt(title_src: str, body_src: str) -> str:
    """この記事で **実際にヒットした語だけ** を箇条書きで指示文にする。"""
    ts, bs = (title_src or ""), (body_src or "")
    if not (ts or bs):
        return ""
    rules_t, rules_b = [], []
    for row in _load_term_glossary_gsheet():
        mm, en, bj, tj = row["mm"], row["en"], row["body_ja"], row["title_ja"]
        hit_t = (mm and mm in ts) or (en and en.lower() in ts.lower())
        hit_b = (mm and mm in bs) or (en and en.lower() in bs.lower())
        if hit_t and tj:
            rules_t.append(f"- {mm or en} ⇒ {tj}")
        if hit_b and bj:
            rules_b.append(f"- {mm or en} ⇒ {bj}")
    if not (rules_t or rules_b):
        return ""
    out = ["【用語固定ルール（この記事で該当した語のみ・厳守）】"]
    if rules_t:
        out.append("▼見出しに出た場合は次を必ず採用：")
        out.extend(rules_t)
    if rules_b:
        out.append("▼本文に出た場合は次を必ず採用：")
        out.extend(rules_b)
    return "\n".join(out) + "\n"

_WORD_RE = re.compile(r"\b", re.UNICODE)
def _apply_term_glossary_to_output(text: str, *, src: str, prefer: str) -> str:
    """
    仕上げ用の軽い置換。
    - src（原文）に A/B が出ている語だけ対象
    - 出力内に英語/ビルマ語が残っていたら C/D の日本語で上書き
    prefer: 'title_ja' または 'body_ja'
    """
    t = (text or "")
    if not t:
        return t
    s = (src or "")
    for row in _load_term_glossary_gsheet():
        mm, en = row["mm"], row["en"]
        ja = row["title_ja"] if prefer == "title_ja" else row["body_ja"]
        if not ja:
            continue
        if not ((mm and mm in s) or (en and en.lower() in s.lower())):
            continue
        # 出力内に en/mm が残っていれば日本語で置換
        if en:
            t = re.sub(rf"(?<![A-Za-z]){re.escape(en)}(?![A-Za-z])", ja, t)
        if mm:
            t = t.replace(mm, ja)
    return t

# ===== 収集器：export_all_articles_to_csv.py から日付別収集関数を利用 =====
collectors_loaded = False
try:
    from tmp.export_all_articles_to_csv import (
        collect_mizzima_all_for_date,
        collect_bbc_all_for_date,
        collect_irrawaddy_all_for_date,
        collect_khitthit_all_for_date,
        collect_myanmar_now_mm_all_for_date,
        collect_dvb_all_for_date,
    )
    collectors_loaded = True
except Exception as e:
    collectors_loaded = False
    print(f"[error] collectors import failed: {e}", file=sys.stderr)

# ===== レートリミッタ（export_all_articles_to_csv.py の実装を再利用） =====
try:
    # 実体としての実装（見つからなければ None）
    from tmp.export_all_articles_to_csv import RateLimiter as _RateLimiterImpl
except Exception:
    _RateLimiterImpl = None

@runtime_checkable
class _RateLimiterProto(Protocol):
    rpm: int
    min_interval: float
    jitter: float
    def wait(self) -> None: ...

# 実行時に main() で初期化して、各 Gemini 呼び出しの直前で wait() する
_LIMITER: Optional[_RateLimiterProto] = None

# ===== Google Sheets 認証（Service Account） ====
import gspread
from google.oauth2.service_account import Credentials
SHEET_ID = os.getenv("MNA_SHEET_ID")
SHEET_NAME = os.getenv("MNA_SHEET_NAME")

def _gc_client():
    """
    認証の優先順:
      1) GOOGLE_SERVICE_ACCOUNT_FILE（今回のワークフローで出力する一時ファイル）
      2) GOOGLE_APPLICATION_CREDENTIALS（GCP標準）
      3) GOOGLE_SERVICE_ACCOUNT_JSON（JSON文字列）
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    file = (os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or "").strip()
    if file:
        creds = Credentials.from_service_account_file(file, scopes=scopes)
        return gspread.authorize(creds)

    app_cred = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if app_cred:
        creds = Credentials.from_service_account_file(app_cred, scopes=scopes)
        return gspread.authorize(creds)

    info = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if info:
        try:
            creds = Credentials.from_service_account_info(json.loads(info), scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            raise SystemExit(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON (JSON parse failed): {e}") from e

    raise SystemExit(
        "Google SA credential not found. "
        "Set one of: GOOGLE_SERVICE_ACCOUNT_FILE / GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SERVICE_ACCOUNT_JSON"
    )
    
# =========================
# 媒体→APIキー環境変数マップ
# =========================
# ここに必要な媒体を追加してください（大文字小文字や全角半角は _norm() で吸収します）
SOURCE_KEY_ENV_MAP: dict[str, str] = {
    # 例： "BBC" や "BBC Burmese" → GEMINI_API_KEY_BBC
    "bbc": "GEMINI_API_KEY_BBC",
    "bbc burmese": "GEMINI_API_KEY_BBC",
    "mizzima": "GEMINI_API_KEY_MIZZIMA",
    "mizzima burmese": "GEMINI_API_KEY_MIZZIMA",
    "mizzima (burmese)": "GEMINI_API_KEY_MIZZIMA",
    "khit thit": "GEMINI_API_KEY_KHITTHIT",
    "khit thit media": "GEMINI_API_KEY_KHITTHIT",
    "myanmar now": "GEMINI_API_KEY_MYANMARNOW",
    "dvb": "GEMINI_API_KEY_DVB",
    "irrawaddy": "GEMINI_API_KEY_IRRAWADDY",
}

_SPACE_RE = re.compile(r"\s+")

def _norm(s: str) -> str:
    """全角→半角/前後空白/連続空白を正規化して小文字化"""
    s = (s or "").strip()
    try:
        import unicodedata
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    s = _SPACE_RE.sub(" ", s)
    return s.lower()

# ===== Gemini キー =====
def _gemini_key_for_source(source: str) -> str | None:
    """
    媒体名から適切なAPIキー（環境変数の値）を返す。
    ※ GEMINI_API_KEY（グローバル）のフォールバックは一切使わない。
    見つからない場合は None を返す。
    """
    src = _norm(source)
    env_name = SOURCE_KEY_ENV_MAP.get(src)
    if not env_name:
        return None
    key = (os.getenv(env_name) or "").strip()
    return key or None

def _clip_body_for_headline(body: str, max_chars: int = 1200) -> str:
    """見出し生成用に本文を安全に切り詰める。段落の途中切りも許容。"""
    b = (body or "").strip()
    if len(b) <= max_chars:
        return b
    return b[:max_chars].rstrip()

# 既存定義を差し替え
def _headline_variants_ja(title: str, source: str, url: str, body: str = "") -> List[str]:
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        t = unicodedata.normalize("NFC", title or "").strip()
        return [t, t, t]

    os.environ["GEMINI_API_KEY"] = key
    model = os.getenv("GEMINI_HEADLINE_MODEL", "gemini-2.5-flash")

    # タイトルに出現した語 → D列（見出し訳）を採用
    # 本文に出現した語 → C列（本文訳）を採用
    rg_title = _region_rules_for_title(title)
    rg_body  = _region_rules_for_body(body)
    glossary = rg_title + rg_body + _build_term_rules_prompt(title, body)

    # ---- 案1：原題ベース ----
    try:
        if _LIMITER: _LIMITER.wait()
        resp1 = call_gemini_with_retries(
            client_summary,
            f"{HEADLINE_PROMPT_1}{glossary}\n\n原題: {title}\nsource:{source}\nurl:{url}",
            model=model,
        )
        v1 = unicodedata.normalize("NFC", (resp1.text or "").strip())
    except Exception:
        v1 = unicodedata.normalize("NFC", (title or "").strip())

    # ---- 案2：案1を素材に再生成（変更なし／前回方針のまま）----
    try:
        if _LIMITER: _LIMITER.wait()
        prompt2 = (glossary or "") + make_headline_prompt_2_from(v1)
        resp2 = call_gemini_with_retries(client_summary, prompt2, model=model)
        v2 = unicodedata.normalize("NFC", (resp2.text or "").strip())
    except Exception:
        v2 = v1

    # ---- 案3：本文から要素抽出して新聞見出し化（今回の修正）----
    try:
        if _LIMITER: _LIMITER.wait()
        body_for_prompt = _clip_body_for_headline(body, max_chars=1200)
        if not body_for_prompt:
            # 本文が無いときは案1でフォールバック
            v3 = v1
        else:
            prompt3 = (
                f"{HEADLINE_PROMPT_3}\n\n"
                + (glossary or "")
                + "【本文】\n" + body_for_prompt + "\n\n"
                f"（参考）原題: {title}\nsource:{source}\nurl:{url}\n"
            )
            resp3 = call_gemini_with_retries(client_summary, prompt3, model=model)
            v3 = unicodedata.normalize("NFC", (resp3.text or "").strip())
    except Exception:
        v3 = v1

    # 生成後の最終統一（州・管区名 → 既存）＋（用語集 → 新規）
    v1 = _apply_region_glossary_to_text(v1); v1 = _apply_term_glossary_to_output(v1, src=title, prefer="title_ja")
    v2 = _apply_region_glossary_to_text(v2); v2 = _apply_term_glossary_to_output(v2, src=title, prefer="title_ja")
    v3 = _apply_region_glossary_to_text(v3); v3 = _apply_term_glossary_to_output(v3, src=title, prefer="title_ja")
    return [v1, v2, v3]

def _summary_ja(source: str, title: str, body: str, url: str) -> str:
    key = _gemini_key_for_source(source)
    if not (call_gemini_with_retries and client_summary and key):
        # グローバルキー無し前提：媒体別キーが無い場合は安全フォールバック＋警告
        try:
            import sys
            print(f"[warn] Gemini key not configured for source='{source}'. "
                  f"Skipped API call; returned trimmed body.", file=sys.stderr)
        except Exception:
            pass
        return unicodedata.normalize("NFC", (body or "").strip())[:400]
    os.environ["GEMINI_API_KEY"] = key
    # fetch_articles.py に定義があればそれを採用、無ければ 1800 に固定
    try:
        from fetch_articles import BODY_MAX_CHARS as _FA_BODY_MAX
        body_max = int(_FA_BODY_MAX)
    except Exception:
        body_max = 1800
    payload = {"source": source, "title": title, "url": url, "body": body}
    prompt = _build_summary_prompt(payload, body_max=body_max)
    try:
        if _LIMITER:
            _LIMITER.wait()
        resp = call_gemini_with_retries(
            client_summary, prompt, model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-2.5-flash")
        )
        text = unicodedata.normalize("NFC", (resp.text or "").strip())
        text = _apply_region_glossary_to_text(text)
        return _apply_term_glossary_to_output(text, src=body, prefer="body_ja")
    except Exception:
        text = unicodedata.normalize("NFC", (body or "").strip())[:400]
        text = _apply_region_glossary_to_text(text)
        return _apply_term_glossary_to_output(text, src=body, prefer="body_ja")

def _is_ayeyarwady(title_ja: str, summary_ja: str) -> bool:
    """
    fetch_articles.py と同一の Ayeyarwady 判定ロジック（is_ayeyarwady_hit）を利用する。
    取りこぼしやバージョン差異を避けるため、まずは関数を import。
    もし import に失敗した場合のみ、fetch_articles 側のキーワードにフォールバック。
    """
    from unicodedata import normalize

    t = normalize("NFC", (title_ja or "").strip())
    b = normalize("NFC", (summary_ja or "").strip())

    try:
        # fetch_articles.py の本家ロジックを直接利用
        from fetch_articles import is_ayeyarwady_hit
        return bool(is_ayeyarwady_hit(t, b))
    except Exception:
        # フォールバック：fetch_articles.py と同じキーワード集合で判定
        try:
            from fetch_articles import AYEYARWADY_KEYWORDS
            kws = AYEYARWADY_KEYWORDS
        except Exception:
            # どうしても import できない環境用の最小セット（fetch_articles.py と同値ではないが安全側）
            kws = ["ဧရာဝတီတိုင်း", "Ayeyarwady Region", "Ayeyarwady region", "ayeyarwady region"]

        hay = f"{t} {b}"
        return any(kw in hay for kw in kws)

def _collect_all_for(target_date_mmt: date) -> List[Dict]:
    if not collectors_loaded:
        raise SystemExit("収集関数の読み込み失敗。export_all_articles_to_csv.py を配置してください。")
    items: List[Dict] = []
    for fn, kwargs in [
        (collect_mizzima_all_for_date, {"max_pages": 3}),
        (collect_bbc_all_for_date, {}),
        (collect_irrawaddy_all_for_date, {}),
        (collect_khitthit_all_for_date, {"max_pages": 5}),
        (collect_dvb_all_for_date, {}),
        (collect_myanmar_now_mm_all_for_date, {"max_pages": 3}),
    ]:
        name = fn.__name__.replace("_all_for_date", "")
        try:
            with _timeit(f"collector:{name}", date=target_date_mmt.isoformat(), kwargs=kwargs or None):
                before = len(items)
                fetched = fn(target_date_mmt, **kwargs)
                items.extend(fetched)
                logging.info(f"[collect:{name}] fetched={len(fetched)} total={len(items)}")
        except Exception as e:
            logging.exception(f"[warn] collector failed: {fn.__name__}: {e}")
    dedup_before = len(items)
    items = deduplicate_by_url(items)
    if dedup_before != len(items):
        logging.info(f"[collect] dedup {dedup_before} -> {len(items)} (-{dedup_before - len(items)})")
    else:
        logging.info(f"[collect] total={len(items)} (no dup)")
    return items

# ===== Sheets I/O =====
def _ws():
    return _gc_client().open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def _read_all_rows():
    ws = _ws(); vals = ws.get_all_values()
    header = vals[0] if vals else []
    rows = vals[1:] if len(vals) > 1 else []
    return header, rows, ws

def _existing_urls_set() -> set:
    header, rows, _ = _read_all_rows()
    name_to_idx = {n:i for i,n in enumerate(header)}
    idx_K = name_to_idx.get("URL", 10)
    urls = set()
    for r in rows:
        try:
            u = (r[idx_K] or "").strip()
            if u: urls.add(u)
        except Exception: pass
    return urls

def _append_rows(rows_to_append: List[List[str]]):
    if not rows_to_append:
        return
    header, rows, ws = _read_all_rows()

    # A列を固定的に参照（列名に依存しない）
    idx_A = 0

    filled = sum(1 for r in rows if (r[idx_A] or "").strip())  # A列のみで判定
    start_row = 2 + filled
    logging.info(f"[sheet] append start_row=A{start_row} rows={len(rows_to_append)}")

    with _timeit("sheet.append", rows=len(rows_to_append), start_row=start_row):
        ws.update(f"A{start_row}", rows_to_append, value_input_option="USER_ENTERED")

def _keep_only_rows_of_date(date_str: str) -> int:
    """A列が date_str(YYYY-MM-DD) の行だけ残す (= 今日以外は A:J と K をクリア)。戻り値=削除行数"""
    header, rows, ws = _read_all_rows()
    if not rows:
        return 0

    # A列を固定的に参照（列名に依存しない）
    idx_A = 0

    kept = []
    for r in rows:
        try:
            if (r[idx_A] or "").strip() == date_str:
                kept.append(r[:10])  # A..J を保持
        except Exception:
            pass

    total = len(rows)

    # A:J は一旦全クリア
    ws.batch_clear(["A2:J"])

    # 今日の行だけ A2 から詰め直す（A..J）
    if kept:
        ws.update("A2", kept, value_input_option="USER_ENTERED")

    # K列（採用フラグ）は全行 ブランク にリセット（A..J が空の行も含む）
    if total > 0:
        ws.update(f"K2:K{total+1}", [[""]]*total, value_input_option="USER_ENTERED")

    return len(rows) - len(kept)

# ===== コマンド：収集→追記（16/18/20/22） =====
def cmd_collect_to_sheet(args):
    now_mmt = datetime.now(MMT)
    target = now_mmt.date()
    
    logging.info(f"[collect] start target_date_mmt={target.isoformat()} clear_yesterday={getattr(args,'clear_yesterday',False)}")

    if getattr(args, "clear_yesterday", False):
        today = now_mmt.date().isoformat()
        removed = _keep_only_rows_of_date(today)
        with _timeit("clear-yesterday", date=today):
            removed = _keep_only_rows_of_date(today)
        logging.info(f"[clean] kept only {today} (removed {removed} rows)")

    items = _collect_all_for(target)
    if not items:
        logging.warning("[collect] no items to write")
        print("no items to write"); return

    existing = _existing_urls_set()
    logging.info(f"[sheet] existing_urls={len(existing)}")
    ts = datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S")
    rows_to_append = []
    bundle_dir = getattr(args, "bundle_dir", "bundle")
    for it in items:
        source = it.get("source") or ""
        title  = it.get("title") or ""
        url    = (it.get("url") or "").strip()
        if not url or url in existing:
            logging.debug(f"[skip] url empty or duplicated url={url!r}")
            continue
        # collector が本文を持っていればそれを最優先（再取得しない）
        body = (it.get("body") or "").strip()
        if body:
            with _BODIES_LOCK:
                cache = _load_bodies_cache(bundle_dir)
                cache[url] = {"source": source, "title": title, "body": body}
                _save_bodies_cache(bundle_dir, cache)
        else:
            # なければ堅牢抽出器で1回だけ取得→キャッシュ
            body = _get_body_once(url, source, out_dir=bundle_dir, title=title)

        with _timeit("headline-variants", source=source):
            f, g, h = _headline_variants_ja(title, source, url, body)
        with _timeit("summary", source=source):
            summ    = _summary_ja(source, title, body, url)
            # summ = ""
        # 異常時のみ “最後の【要約】抽出＋手順行除去” を適用（存在すれば）
        try:
            from fetch_articles import normalize_summary_text
            summ = normalize_summary_text(summ)
        except Exception:
            pass
        is_ay   = _is_ayeyarwady(f, summ)
        logging.info(f"[row] source={source} body_len={len(body)} ayeyarwady={is_ay} url={url}")

        rows_to_append.append([
            target.isoformat(),          # A 日付(配信日)
            ts,                          # B timestamp
            source,                      # C メディア
            "TRUE" if is_ay else "FALSE",# D エーヤワディー
            f, g, h,                     # E/F/G 見出し訳３案
            "",                          # H 確定見出し（手動）
            summ,                        # I 本文要約
            url,                         # J URL
        ])
        existing.add(url)

    _append_rows(rows_to_append)
    print(f"appended {len(rows_to_append)} rows")

# ===== コマンド：シート→bundle再生成（02:30） =====
def cmd_build_bundle_from_sheet(args):
    # ① 読み込み
    with _timeit("build-bundle:read"):
        header, rows, _ = _read_all_rows()
    if not rows:
        logging.warning("[bundle] no rows")
        print("no rows"); return
    logging.info(f"[bundle] rows_total={len(rows)} (採用フラグ=K='a' のみ抽出)")

    # 列名に依存しないよう、列記号(A..K)を固定でインデックス化して参照する
    col = {chr(ord('A')+i): i for i in range(len(header))}  # A:0, B:1, ... K:10
    get = lambda r, key, default="": (r[col.get(key, -1)] if col.get(key, -1) >= 0 else default).strip()

    MEDIA_ORDER = [
        "Mizzima (Burmese)", "BBC Burmese", "Irrawaddy",
        "Khit Thit Media", "DVB", "Myanmar Now",
    ]
    order = {m: i for i, m in enumerate(MEDIA_ORDER)}

    # ② 採用フラグで選別（件数・媒体内訳をログ）
    from collections import defaultdict
    with _timeit("build-bundle:select"):
        selected = []
        media_counts = defaultdict(int)
        for r in rows:
            if get(r, "K") != "a":             # 採用フラグ(K) 小文字 'a' のみ採用
                continue
            media = get(r, "C")  # メディア(C)
            selected.append((order.get(media, 999), r))
            media_counts[media] += 1
        selected.sort(key=lambda x: x[0])

    total_selected = len(selected)
    if total_selected == 0:
        logging.info("[bundle] no summaries selected (K='a')")
        print("no summaries selected (K='a')"); return

    # 媒体別内訳（INFO）
    breakdown = ", ".join(f"{k}:{v}" for k, v in sorted(media_counts.items(), key=lambda x: order.get(x[0], 999)))
    logging.info(f"[bundle] selected={total_selected} by_media=({breakdown})")

    # ③ summaries 構築
    with _timeit("build-bundle:construct", selected=total_selected):
        summaries = []
        for _, r in selected:
            delivery    = get(r, "A")                        # 日付(A)
            media       = get(r, "C")                        # メディア(C)
            title_final = get(r, "H")                        # 確定見出し日本語訳(H)
            body_sum    = get(r, "I")                        # 本文要約(I)
            url         = get(r, "J")                        # URL(J)
            is_ay       = (get(r, "D").upper() == "TRUE")    # エーヤワディー(D)
            summaries.append({
                "source": media,
                "url": url,
                "title": unicodedata.normalize("NFC", title_final),
                "summary": unicodedata.normalize("NFC", body_sum),
                "is_ayeyarwady": is_ay,
                "date_mmt": delivery,
            })

    out_dir = os.path.abspath(args.bundle_dir)

    # ④ 書き出し（既存ディレクトリの再作成も含めて計測）
    with _timeit("build-bundle:write", out_dir=out_dir, items=len(summaries)):
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        meta = {
            "date_mmt": summaries[0]["date_mmt"],
            "generated_from": "sheet",
            "generated_at_mmt": datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        with open(os.path.join(out_dir, "summaries.json"), "w", encoding="utf-8") as f:
            json.dump(summaries, f, ensure_ascii=False, indent=2)

    logging.info(f"[bundle] rebuilt dir={out_dir} items={len(summaries)} date_mmt={summaries[0]['date_mmt']}")
    print(f"bundle rebuilt: {out_dir} (items={len(summaries)})")

    # キャッシュ済み本文を使って全文翻訳PDFを生成（BUSINESS/TRIAL 添付用）
    def _make_business_pdf_from_summaries(summaries: list[dict], date_iso: str, out_dir: str):
        if not (translate_fulltexts_for_business and build_combined_pdf_for_business):
            logging.warning("[bundle] PDF helpers unavailable; skip business PDF")
            return
        cache = _load_bodies_cache(out_dir)
        urls_in_order: list[str] = []
        url_to_source_title_body: dict[str, dict] = {}
        for s in summaries:
            url = (s.get("url") or "").strip()
            if not url:
                continue
            source = s.get("source") or ""
            title  = s.get("title") or ""
            urls_in_order.append(url)
            body = (cache.get(url) or {}).get("body", "")
            if not body:
                body = _get_body_once(url, source, out_dir=out_dir, title=title)
            url_to_source_title_body[url] = {"source": source, "title": title, "body": body}
        if not urls_in_order:
            logging.info("[bundle] no urls for PDF")
            return
        translated = translate_fulltexts_for_business(urls_in_order, url_to_source_title_body)
        if not translated:
            logging.warning("[bundle] translation returned empty; skip PDF")
            return

        # ---- PDFビルダーが期待するメタ（title_ja / source / date / url）を付与する ----
        # sheet_pipeline の summaries には、すでに確定見出しやメディア、配信日が入っている
        #   - s["title"]      : 確定見出し（日本語）
        #   - s["source"]     : メディア名
        #   - s["date_mmt"]   : 日付（YYYY-MM-DD）
        #   - s["url"]        : 記事URL
        def _norm(u: str) -> str:
            return (u or "").rstrip("/")
        url_to_meta = {}
        for s in summaries:
            u = _norm(s.get("url", ""))
            if not u:
                continue
            # PDFビルダーは “文字列” を想定。シートは既に YYYY-MM-DD なのでそのまま使う
            date_raw = s.get("date_mmt", "") or (date_iso or "")
            # 念のため正規化（YYYY-MM-DD 以外が来たら date に直してから isoformat）
            d_obj = _coerce_date(date_raw)
            date_str = (d_obj.isoformat() if d_obj else (date_iso or ""))
            url_to_meta[u] = {
                "title_ja": s.get("title", "") or "",
                "source":   s.get("source", "") or "",
                "date":     date_str,   # ← 文字列で保持
                "url":      u,
            }

        translated_items = []
        for it in translated:
            u = _norm(it.get("url", ""))
            meta = url_to_meta.get(u, {})
            # ★ Business全文PDF用：title_ja / body_ja も最終置換
            title_ja = _apply_region_glossary_to_text(meta.get("title_ja", ""))
            body_ja  = _apply_region_glossary_to_text(it.get("body_ja", "") or "")
            translated_items.append({
                "url":      u,
                "title_ja": title_ja,
                "body_ja":  body_ja,
                "source":   meta.get("source", ""),
                "date":     meta.get("date", ""),
            })

        pdf_bytes = build_combined_pdf_for_business(translated_items)

        # 添付ファイル名（日本語日付）
        try:
            y, m, d = map(int, (date_iso or "").split("-"))
            jp = _jp_date(date(y, m, d)) if _jp_date else date_iso
        except Exception:
            jp = date_iso
        attachment_name = f"ミャンマーニュース全文訳【{jp}】.pdf"
        with open(os.path.join(out_dir, "digest.pdf"), "wb") as f:
            f.write(pdf_bytes)
        with open(os.path.join(out_dir, "attachment_name.txt"), "w", encoding="utf-8") as f:
            f.write(attachment_name)
        logging.info(f"[bundle] wrote business PDF: {attachment_name} ({len(pdf_bytes)} bytes)")

    try:
        _make_business_pdf_from_summaries(summaries, meta["date_mmt"], out_dir)
    except Exception as e:
        logging.warning(f"[bundle] failed to build business pdf: {e}")

# ===== CLI =====
import argparse
def main():
    p = argparse.ArgumentParser(description="MNA sheet pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("collect-to-sheet", help="収集→要約→sheet追記（16/18/20/22）")
    p1.add_argument("--clear-yesterday", action="store_true", help="前日分だけA2:Jから除去（16:00用）")
    p1.add_argument("--bundle-dir", default="bundle", help="本文キャッシュ/成果物の保存先（既定: bundle）")
    # === Gemini free tier を想定したレート設定（CLI指定 > 環境変数 > 既定）===
    p1.add_argument("--rpm", type=int, default=int(os.getenv("GEMINI_REQS_PER_MIN", "9")))
    p1.add_argument("--min-interval", type=float, default=float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "2.0")))
    p1.add_argument("--jitter", type=float, default=float(os.getenv("GEMINI_JITTER_SEC", "0.3")))
    p1.set_defaults(func=cmd_collect_to_sheet)

    p2 = sub.add_parser("build-bundle", help="sheetからbundle生成（02:30）")
    p2.add_argument("--bundle-dir", default="bundle", help="bundle出力先")
    p2.set_defaults(func=cmd_build_bundle_from_sheet)

    args = p.parse_args()
    # レートリミッタ初期化（collect-to-sheet のときのみ意味がある）
    global _LIMITER
    if _RateLimiterImpl and args.cmd == "collect-to-sheet":
        _LIMITER = cast(_RateLimiterProto, _RateLimiterImpl(
            getattr(args, "rpm", int(os.getenv("GEMINI_REQS_PER_MIN", "9"))),
            getattr(args, "min_interval", float(os.getenv("GEMINI_MIN_INTERVAL_SEC", "2.0"))),
            getattr(args, "jitter", float(os.getenv("GEMINI_JITTER_SEC", "0.3"))),
        ))
        msg = f"[rate] rpm={_LIMITER.rpm}, min_interval={_LIMITER.min_interval}s, jitter<= {_LIMITER.jitter}s"
        print(msg)
        logging.info(msg)
    else:
        _LIMITER = None
    args.func(args)

if __name__ == "__main__":
    main()
