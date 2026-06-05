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

# ===== collect rules (no env var switching) =====
# Irrawaddy は負荷が重いため、定時収集では 21:30 / 22:30 / 23:20 MMT のみ対象にする。
_IRRAWADDY_ALLOWED_CRONS = {
    "0 15 * * *",   # 21:30 MMT
    "0 16 * * *",   # 22:30 MMT
    "50 16 * * *",  # 23:20 MMT
}

# GNLM（Global New Light Of Myanmar / 国営紙）は、定時収集では
# 12:30 / 16:30 / 19:30 / 22:30 MMT の4枠だけ対象にする。
_GNLM_ALLOWED_CRONS = {
    "0 6 * * *",    # 12:30 MMT
    "0 10 * * *",   # 16:30 MMT
    "0 13 * * *",   # 19:30 MMT
    "0 16 * * *",   # 22:30 MMT
}

def _should_collect_irrawaddy(schedule_cron: str | None) -> bool:
    """Return True only for Irrawaddy-enabled collection slots."""
    if not schedule_cron:
        return False
    return schedule_cron.strip() in _IRRAWADDY_ALLOWED_CRONS

def _should_collect_gnlm(schedule_cron: str | None) -> bool:
    """Return True only for GNLM-enabled collection slots."""
    if not schedule_cron:
        return False
    return schedule_cron.strip() in _GNLM_ALLOWED_CRONS

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
        call_llm_with_fallback,   # Gemini→OpenAI フォールバック
        client_summary,
        deduplicate_by_url,
    )
except Exception:
    MMT = timezone(timedelta(hours=6, minutes=30))
    call_llm_with_fallback = None
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

def _load_bodies_cache(out_dir: str) -> list[dict]:
    """
    bodies.json を「item_id ベースの配列」として読み込む。
    旧形式（{url: {...}} の辞書）も後方互換で配列へ変換して受け付ける。
    """
    p = _bodies_cache_path(out_dir)
    if not os.path.exists(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []

    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]

    # 旧形式: {url: {...}} を、順序を保った配列へ変換
    if isinstance(raw, dict):
        items: list[dict] = []
        for url, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            item = {"url": url}
            item.update(payload)
            items.append(item)
        return items

    return []

def _save_bodies_cache(out_dir: str, cache: list[dict]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(_bodies_cache_path(out_dir), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def _find_body_cache_entry(cache: list[dict], *, url: str = "", item_id: str = "") -> dict | None:
    """
    item_id を優先して検索し、無ければ URL で後ろから検索する。
    URL 検索を後ろから行うことで、同一URLの複数行がある場合は最後の行を優先できる。
    """
    key_item = (item_id or "").strip()
    key_url = (url or "").strip().rstrip("/")

    if key_item:
        for entry in cache:
            if not isinstance(entry, dict):
                continue
            if (entry.get("item_id") or "").strip() == key_item:
                return entry

    if key_url:
        for entry in reversed(cache):
            if not isinstance(entry, dict):
                continue
            if (entry.get("url") or "").strip().rstrip("/") == key_url:
                return entry

    return None

def _upsert_body_cache_entry(
    cache: list[dict],
    *,
    url: str = "",
    item_id: str = "",
    source: str = "",
    title: str = "",
    body: str = "",
    body_ja: str = "",
) -> list[dict]:
    """
    配列形式の bodies キャッシュへ追加/更新する。
    - item_id があれば item_id 優先
    - item_id が無ければ URL 単位で更新
    - ヒットしなければ末尾へ追加（配列順を維持）
    """
    idx = None
    key_item = (item_id or "").strip()
    key_url = (url or "").strip().rstrip("/")

    if key_item:
        for i, entry in enumerate(cache):
            if not isinstance(entry, dict):
                continue
            if (entry.get("item_id") or "").strip() == key_item:
                idx = i
                break
    elif key_url:
        for i, entry in enumerate(cache):
            if not isinstance(entry, dict):
                continue
            if (entry.get("url") or "").strip().rstrip("/") == key_url:
                idx = i
                break

    if idx is not None:
        existing = dict(cache[idx])
        entry = dict(existing)
    else:
        entry = {}
        cache.append(entry)
        idx = len(cache) - 1

    if key_item:
        entry["item_id"] = key_item
    if key_url:
        entry["url"] = key_url
    if source:
        entry["source"] = source
    if title:
        entry["title"] = title
    if body:
        entry["body"] = body
    if body_ja:
        entry["body_ja"] = body_ja

    cache[idx] = entry
    return cache

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
        _is_irrawaddy_excluded_url,
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

def _resolve_news_google_redirect_global(u: str, timeout: int = 20) -> str:
    """
    news.google.com/rss/articles/... を publisher の実URLへ解決する。
    Irrawaddy は export_all_articles_to_csv.py の専用 resolver を優先再利用し、
    それが使えない場合も同等の段階的解決（token decode / redirect / canonical）を試す。
    失敗時は空文字を返す。
    """
    if not u:
        return ""

    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        host = ""

    def _is_allowed_irrawaddy_url(candidate: str) -> bool:
        try:
            parsed = urlparse(candidate or "")
            cand_host = (parsed.netloc or "").lower()
        except Exception:
            return False
        if not cand_host.endswith("irrawaddy.com"):
            return False
        if _is_irrawaddy_excluded_url(candidate):
            return False
        return True

    if "news.google.com" not in host:
        return u if _is_allowed_irrawaddy_url(u) else ""

    if _resolve_google_news_link_irrawaddy_shared is not None:
        try:
            resolved = (_resolve_google_news_link_irrawaddy_shared(u, timeout=timeout) or "").strip()
            if resolved:
                return resolved
        except Exception:
            pass

    # fallback 1) /rss/articles/<token> の token から URL を直接復元
    try:
        import base64
        parts = urlparse(u).path.split("/")
        if "articles" in parts:
            token = parts[parts.index("articles") + 1]
            if token:
                token += "=" * (-len(token) % 4)
                raw = base64.urlsafe_b64decode(token)
                m = re.search(rb"https?://[^\s\"'<>\x00]+", raw)
                if m:
                    decoded = m.group(0).decode("utf-8", errors="ignore").strip()
                    if _is_allowed_irrawaddy_url(decoded):
                        return decoded
                    return ""
    except Exception:
        pass

    # fallback 2) 通常のリダイレクト解決
    try:
        r = requests.get(
            u,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
            allow_redirects=True,
        )
        final_url = (getattr(r, "url", "") or "").strip()
        if _is_allowed_irrawaddy_url(final_url):
            return final_url
    except Exception:
        pass

    # fallback 3) HTML から canonical / og:url を拾う
    try:
        r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        html = (r.text or "").strip() if getattr(r, 'status_code', 0) == 200 else ""
        if html:
            m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.I)
            if m:
                cand = m.group(1).strip()
                if _is_allowed_irrawaddy_url(cand):
                    return cand
            m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
            if m:
                cand = m.group(1).strip()
                if _is_allowed_irrawaddy_url(cand):
                    return cand
    except Exception:
        pass

    return ""

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

def _get_body_once(url: str, source: str, out_dir: str, title: str = "", summary: str = "") -> str:
    """
    1) bundle/bodies.json を見てあれば返す
    2) なければ取得→保存→返す
    - Irrawaddy は本文抽出失敗時に r.jina.ai → AMP (/amp, ?output=amp) の順でフォールバック
    - Google News (news.google.com/rss/articles/...) は最終到達URLを解決してから試行
    """
    from urllib.parse import urlparse
    import requests
    from bs4 import BeautifulSoup

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
    cached = _find_body_cache_entry(cache, url=url)
    if cached and isinstance(cached, dict) and (cached.get("body") or "").strip():
        return cached["body"]

    # --- 事前正規化：Google News の場合は最終到達URLを一度解決 ---
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if "news.google.com" in host:
        resolved = _resolve_news_google_redirect_global(url)
        if not resolved:
            logging.warning(f"[body] unresolved google-news url={url}")
            return ""
        url = resolved

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
                        b = fetch_once_irrawaddy(u)  # ← ここが direct→必要ならBD になる
                        if isinstance(b, (bytes, bytearray)):
                            return b.decode("utf-8", "ignore")
                        return (b or "")
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
            if body:
                logging.info(f"[body] irrawaddy rescued via jina/amp url={url} body_len={len(body)}")

    if not (body or "").strip():
        logging.warning(f"[body] empty source={source} url={url}")

    # --- 4) 空本文はキャッシュしない（将来の再取得の余地を残す）---
    if body.strip():
        with _BODIES_LOCK:
            cache = _load_bodies_cache(out_dir)  # 競合対策で再読込
            existing = _find_body_cache_entry(cache, url=url) or {}
            cache = _upsert_body_cache_entry(
                cache,
                url=url,
                source=source,
                title=title,
                body=body,
                body_ja=(existing.get("body_ja") or ""),
            )
            _save_bodies_cache(out_dir, cache)

    return body

# ===== 翻訳プロンプト：共通ルール =====
COMMON_TRANSLATION_RULES = """
【翻訳時の用語統一ルール（必ず従うこと）】
このルールは記事タイトルと本文の翻訳に必ず適用してください。
軍事評議会・軍事委員会 → 親軍政権
徴用 → 徴兵
軍事評議会軍 → 国軍
軍政軍 → 国軍
親軍政権部隊 → 国軍
親軍政権側部隊 → 国軍
ミンアウンフライン率いる国軍 → 国軍
親軍政権トップ・ミンアウンフライン率いる国軍 → 国軍
アジア道路 → アジアハイウェイ
来客登録 → 宿泊登録 / 来客登録者 → 宿泊登録者
タウンシップ → 郡区
北オークカラパ・北オカラパ → 北オカラッパ
サリンギ郡区 → タンリン郡区
ネーピードー → ネピドー
ミャンマー国民 → ミャンマー人
タディンユット → ダディンジュ

【国軍表記の強制ルール】
「စစ်တပ်」「စစ်ကောင်စီတပ်」「စစ်အာဏာရှင်တပ်」「အကြမ်းဖက် စစ်တပ်」「junta troops」「regime forces」「military column」が国軍・国軍部隊を指す文脈では、本文要約・見出しとも「国軍」と表記する。
「親軍政権軍」「軍事政権軍」「軍政軍」「親軍政権部隊」「軍事政権部隊」「親軍政権側部隊」「軍事政権側部隊」「親軍政権トップ・ミンアウンフライン率いる国軍」「軍事政権トップ・ミンアウンフライン率いる国軍」「ミンアウンフライン率いる国軍」は使用禁止。必ず「国軍」に置き換える。
原文に Min Aung Hlaing / မင်းအောင်လှိုင် が明示されていない場合、国軍の行動主体に「ミンアウンフライン」を補ってはならない。

【ミャンマー情勢の用語置き換えルール】（反政権側の運動・組織を指す文脈のみ）
革命 → 抵抗 / 革命勢力 → 抵抗勢力 / 革命軍 → 抵抗勢力

【その他の用語統一】
サイドカー → サイカー / SpaceX → スペースX / KK Park → KKパーク / Starlink → スターリンク

【翻訳時の特別ルール】
「ဖမ်းဆီး」の訳語は文脈によって使い分けること。
- 犯罪容疑や法律違反に対する文脈 → 「逮捕」
- それ以外の文脈 → 「拘束」

【政党名の訳語ルール（USDP）】
「ကြံ့ဖွတ်ပါတီ」「ပြည်ထောင်စုကြံ့ခိုင်ရေးနှင့် ဖွံ့ဖြိုးရေးပါတီ」「USDP」が原文に出た場合：
使用してよい訳語：「国軍系政党」「国軍系USDP党」「国軍系連邦団結発展党（USDP）」
- 見出し：「国軍系USDP党」を優先
- 本文初出：「国軍系連邦団結発展党（USDP）」を優先
- 2回目以降：「USDP」
- 一般文脈：「国軍系政党」
禁止：上記3種類以外の省略形（例：「国軍系USDP」「USDP政党」など）

【武装組織名の訳語ルール（BGF）】
「BGF」「Karen Border Guard Force」「カレン国境警備隊」が原文に出た場合：
使用してよい訳語：「国軍系勢力」「国軍系勢力BGF」「国軍傘下のカレン国境警備隊」「国軍傘下のカレン国境警備隊（BGF）」
- 見出し：「国軍傘下BGF」を優先
- 本文初出：「国軍傘下のカレン国境警備隊（BGF）」を優先
- 2回目以降：「BGF」
- 一般文脈：「国軍系勢力」
禁止：上記以外の省略形（例：「国軍傘下BGF」「BGF部隊」「国軍系BGF」など）

【武装組織名の訳語ルール（DKBA）】
「DKBA」「D.K.B.A」「ဒီမိုကရေစီ အကျိုးပြု ကရင်တပ်မတော်」「ဒီမိုကရက်တစ်ကရင်အကျိုးပြုတပ်မတော်」が原文に出た場合：
使用してよい訳語：「親国軍勢力DKBA」「DKBA」
- 見出し：「親国軍勢力DKBA」を優先
- 本文初出：「親国軍勢力DKBA」を優先
- 2回目以降：文脈上明らかな場合のみ「DKBA」
- 一般文脈：「親国軍勢力DKBA」
禁止：「国軍傘下DKBA」「国軍傘下の民主カレン仏教徒軍（DKBA）」「国軍系DKBA」「DKBA部隊」「国軍傘下DKBA軍」など

【武装組織名の訳語ルール（ピューソーティー）】
「ピューソーティー」「ピュー・ソー・ティー」（同一語）が出た場合：
- 見出し：「国軍民兵」
- 本文初出：「国軍民兵ピューソーティー」
- 本文2回目以降：「ピューソーティー」

【中立的記述ルール（必ず守ること）】
- 記者の地の文には政治的に偏った語・価値判断語・レッテル語を使用せず、中立語に置き換える。
  ・残虐な攻撃 → 「攻撃」
  ・不当な拘束 → 「拘束が行われた」
  ・違法な政権 → 「違法と批判されている」（誰かの評価と分かる表現に言い換える）
  ・不正な／偽りの／偽装選挙 → 「選挙」
- 【国軍・親軍政権に付くレッテル】（ファシスト国軍・テロリスト軍・クーデター軍・テロリスト軍事政権・テロリスト軍事評議会・クーデター軍事評議会 等）
  → 「国軍」「ミャンマー国軍」「親軍政権」のいずれかに統一。
- 反政権側組織への国営系メディアのレッテル語も削除する。
  テロ組織NUG → 「NUG」 / 違法武装組織PDF → 「PDF」 / 分離主義テロ組織○○ → 「○○武装組織」

【引用・スローガン・発言部分の扱い】
以下は原文を改変せず保持する（引用符内は絶対に書き換えない）：
1) デモ参加者・団体・市民のスローガン / 2) 当事者の発言・声明 / 3) いずれかの側の主張・批判・要求
要約ではこれらを「〜と述べた／〜と主張した／〜と訴えた」等の形式で紹介し、断定しない。
判定：引用符（" " / 「 」 / 『 』）内・明確な発言 → 残す、それ以外 → 中立化。

【選挙に関する特例】
中立化ルールは「記者の地の文」にのみ適用。スローガン・団体声明・個人の主張（例：「偽りの選挙は不要」「不当な選挙だ」）は原文のニュアンスを保持する。

【時制表現の禁止ルール（要約用）】
「本日」「昨日」「明日」は使用禁止。必ず原文の具体的な日付を明記すること。

【日付の扱いルール】
- 原文に年が書かれていない日付に年を補わないこと（「12月4日」のように月日のみで表記）。
- 「ယနေ့ (မေ ၉)」「May 9」「9 May」のように月日だけが書かれている場合は、「5月9日」のように月日のみで訳す。記事公開年・現在年・取得年・URL・メタデータから年を推測して補わない。
- 「原文の別箇所に同じ年がある」ことは年付与の根拠にならない。
  例）「March 2026」と「28 December」が混在しても、要約で「2026年12月28日」としてはならない。
- 年を出力してよい条件：原文の同じ日付表現に年が結び付いている場合のみ。ただし、当年は要約内では年を省き、「5月9日」「3月」のように月日または月のみで表記する。
- 当年は、この記事を処理している時点の年を指す。特定の年に固定しない。
- 例：処理年が2026年なら「2026年5月9日」→「5月9日」、処理年が2027年なら「2027年5月9日」→「5月9日」とする。
- 当年以外の年は、原文の同じ日付表現に年が結び付いている場合に限り年付きで表記してよい。
- 英語・ビルマ語原文でも同様。

【通貨換算ルール】
ミャンマー通貨「チャット（Kyat、ကျပ်）」が出た場合は日本円に換算して併記する。
- 換算レート：1チャット = 0.0360円（他レート禁止）
- 形式：「◯チャット（約◯円）」、日本円は小数点以下四捨五入、兆・億・万に機械的に分解。
- チャット以外（バーツ・米ドル等）には換算しない。
- 見出しでは換算しない（タイトル・見出しではチャット建てのみを優先）。

【金額分解ルール】
日本円は桁の繰り上げ・概算・丸めをせず、機械的に兆・億・万に分解すること。
- 21,060,000,000円 → 210億6000万円（「2兆1060億円」は誤り・桁がずれる）
- 5,432,100,000円 → 54億3210万円（「543億2100万円」は誤り）
- 1,234,567,890,000円 → 1兆2345億6789万円

【ミャンマー語の数詞・単位ルール】
数字と単位は前後どちらに置かれてもよく、スペースの有無も問わない（「၅၀ သိန်း」「သိန်း 50」「50သိန်း」等すべて同じ）。アラビア数字も同様に解釈する。

■ 「သိန်း」= 10万チャット
「N သိန်း」「သိန်း N」→ N × 10万チャット
- 「သိန်း 50」「50 သိန်း」「50သိန်း」→ 500万チャット
- 「သိန်း 3000」「3000 သိန်း」「3000သိန်း」→ 3億チャット
※誤り：「30 သိန်း」を「30万チャット」とするのは禁止（正しくは300万チャット）
円換算が必要な場合：まず正しいチャット建てを計算し、その後「約◯円」を併記すること。

■ 「သန်း」= 100万チャット
「N သန်း」「သန်း N」→ N × 100万チャット
- 「1 သန်း」「သန်း 1」→ 100万チャット
- 「50 သန်း」「သန်း 50」→ 5000万チャット
※誤り：「50 သန်း」を「50万チャット」とするのは禁止（正しくは5000万チャット）
円換算が必要な場合：まず正しいチャット建てを計算し、その後「約◯円」を併記すること。

■ 「ဘီလီယံ」= 10億チャット（N ≥ 1000 の場合は「兆」が立つ）
「N ဘီလီယံ」「ဘီလီယံ N」→ N × 10億チャット
- 「ဘီလီယံ 5」「5 ဘီလီယံ」→ 50億チャット
- 「ဘီလီယံ 540」「540 ဘီလီယံ」→ 5,400億チャット
- 「ဘီလီယံ 1000」→ 1兆0億チャット / 「ဘီလီယံ 5329」→ 5兆3,290億チャット
- 「1068 ဒသမ 66 ဘီလီယံ」→ 1兆686億6000万チャット
※誤り：「N ဘီလီယံ」を「N億チャット」へ短縮するのは禁止（N × 10億チャットを保持する）
円換算が必要な場合：まず正しいチャット建てを計算し、その後「約◯円」を併記すること。

■ 語尾（လောက်：〜くらい / ကျော်：〜超 / ခန့်：およそ）が付く場合
上記ルールで数値と単位を解釈し、語尾のニュアンスを日本語に反映する。
例：「သိန်း 50 ကျော်」→ 500万チャット超 / 「သန်း 50 လောက်」→ 5000万チャットくらい

■ ミャンマー語の概数金額表現
「ရာချီ」= 数百単位、「ထောင်ချီ」= 数千単位、「သောင်းချီ」= 数万単位。
これらが「သိန်း」「သန်း」「ဘီလီယံ」などの金額単位と結び付く場合、必ず前の単位を保持して解釈する。
- 「သိန်းရာချီ」→ 数百 × 10万チャット = 数千万チャット規模
- 「သိန်းထောင်ချီ」→ 数千 × 10万チャット = 数億チャット規模
- 「သန်းရာချီ」→ 数百 × 100万チャット = 数億チャット規模
- 「သန်းထောင်ချီ」→ 数千 × 100万チャット = 数十億チャット規模
※「ရာချီ」だけを見て「数百チャット」と訳すのは禁止。
※概数表現は正確な金額が不明なため、「数千万チャット規模」「数億チャット規模」のように表記し、日本円換算はしない。
"""

# プロンプト先頭に1回だけ載せる共通ルールブロック（見出し/本文要約/全文訳などに適用）
COMMON_RULES_HEADER = "【共通ルール（最優先）】\n" + COMMON_TRANSLATION_RULES + "\n\n"

# ============================================================
# メディア別表記・DKBA表記・国軍表記の強制補正
# - prompt.js と同じ方針を Python 側にも適用する。
# - LLM への指示だけでなく、生成後の文字列も機械的に補正する。
# ============================================================

_OFFICIAL_STYLE_SOURCE_NAMES = {
    "popular myanmar",
    "popular myanmar (国軍系メディア)",
    "news eleven",
    "news eleven burmese",
    "newseleven",
    "global new light of myanmar",
    "global new light",
    "global new light of myanmar (国営紙)",
    "gnlm",
}


def _normalize_source_name_for_style(source: str) -> str:
    s = str(source or "").strip()
    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _is_official_style_source(source: str) -> bool:
    return _normalize_source_name_for_style(source) in _OFFICIAL_STYLE_SOURCE_NAMES


def _build_source_specific_translation_rules(source: str) -> str:
    source_label = str(source or "").strip() or "不明"
    if _is_official_style_source(source):
        return (
            "【メディア別表記ルール（最優先）】\n"
            f"対象メディア: {source_label}\n"
            "このメディアが Popular Myanmar、News Eleven、Global New Light Of Myanmar のいずれかに該当する場合、以下を必ず守る。\n"
            "- 本文要約（summary）・全文翻訳では、Min Aung Hlaing / ミンアウンフライン / ミン・アウン・フライン / ミン・アウン・ライン / ミンアウンライン / မင်းအောင်လှိုင် は、初出のみ「ミンアウンフライン大統領」と表記し、同一要約・同一記事内の2回目以降は「大統領」と表記する。\n"
            "- 人物名の誤推定禁止：「တပ်မတော်ကာကွယ်ရေးဦးစီးချုပ်」「国軍総司令官」「Commander-in-Chief」などの役職名だけを根拠に、人物をミンアウンフラインと推定してはならない。原文に人物名が併記されている場合は、その人物名を必ず優先する。\n"
            "- 「ရဲဝင်းဦး」「ဗိုလ်ချုပ်ကြီး ရဲဝင်းဦး」「Ye Win Oo」「General Ye Win Oo」が原文に出た場合は、本文要約・全文翻訳・見出しとも必ず「国軍総司令官イェ・ウィン・ウー」と表記する。\n"
            "- Min Aung Hlaing / မင်းအောင်လှိုင် の表記ルールは、原文に Min Aung Hlaing 系の名前が明示されている場合に限って適用する。\n"
            "- 見出し・タイトルでは、同人物は必ず「大統領」と表記する。「ミンアウンフライン大統領」「親軍政権トップ・ミンアウンフライン」「軍事政権トップ・ミンアウンフライン」「国軍トップ・ミンアウンフライン」「ミンアウンフライン総司令官」は使わない。\n"
            "- 見出しで「総司令官、」「上級大将、」「国軍トップ、」のように肩書きだけで主語を省略しない。該当人物が Min Aung Hlaing の場合は必ず「大統領、...」で始める。\n"
            "- 「軍事政権」「親軍政権」という表現は使用禁止。文脈上、組織・政権主体を示す必要がある場合は「政府」と表記する。\n"
            "- DKBA は全メディア共通で、見出しと本文初出は「親国軍勢力DKBA」、本文2回目以降は文脈上明らかな場合「DKBA」と表記し、「国軍傘下DKBA」は使わない。\n"
        )

    return (
        "【メディア別表記ルール（最優先）】\n"
        f"対象メディア: {source_label}\n"
        "このメディアは Popular Myanmar、News Eleven、Global New Light Of Myanmar 以外として扱う。\n"
        "- 本文要約（summary）・全文翻訳では、Min Aung Hlaing / ミンアウンフライン / ミン・アウン・フライン / ミン・アウン・ライン / ミンアウンライン / မင်းအောင်လှိုင် は、初出のみ「親軍政権トップ・ミンアウンフライン」と表記し、同一要約・同一記事内の2回目以降は「ミンアウンフライン」と表記する。\n"
        "- 人物名の誤推定禁止：「တပ်မတော်ကာကွယ်ရေးဦးစီးချုပ်」「国軍総司令官」「Commander-in-Chief」などの役職名だけを根拠に、人物をミンアウンフラインと推定してはならない。原文に人物名が併記されている場合は、その人物名を必ず優先する。\n"
        "- 「ရဲဝင်းဦး」「ဗိုလ်ချုပ်ကြီး ရဲဝင်းဦး」「Ye Win Oo」「General Ye Win Oo」が原文に出た場合は、本文要約・全文翻訳・見出しとも必ず「国軍総司令官イェ・ウィン・ウー」と表記する。\n"
        "- Min Aung Hlaing / မင်းအောင်လှိုင် の表記ルールは、原文に Min Aung Hlaing 系の名前が明示されている場合に限って適用する。\n"
        "- 見出し・タイトルでは、同人物は必ず「ミンアウンフライン」と表記する。「親軍政権トップ・ミンアウンフライン」「軍事政権トップ・ミンアウンフライン」「ミンアウンフライン大統領」「ミンアウンフライン総司令官」「国軍トップ・ミンアウンフライン」は使わない。\n"
        "- 見出しで「総司令官、」「国軍トップ、」「国軍指導者、」のように肩書きだけで主語を省略しない。該当人物が Min Aung Hlaing の場合は必ず「ミンアウンフライン、...」で始める。\n"
        "- DKBA は全メディア共通で、見出しと本文初出は「親国軍勢力DKBA」、本文2回目以降は文脈上明らかな場合「DKBA」と表記し、「国軍傘下DKBA」は使わない。\n"
    )


def _normalize_dkba_terms(text: str, *, short_after_first: bool = False) -> str:
    if text is None:
        return text
    s = str(text)
    if s.startswith("ERROR:"):
        return s
    placeholder = "__TERM_PLACEHOLDER_D__"

    s = re.sub(r"親国軍勢力\s*DKBA", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"国軍傘下の民主カレン仏教徒軍[（(]\s*D\.?\s*K\.?\s*B\.?\s*A\.?\s*[）)]", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"民主カレン仏教徒軍[（(]\s*D\.?\s*K\.?\s*B\.?\s*A\.?\s*[）)]", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"国軍傘下(?:の)?\s*D\.?\s*K\.?\s*B\.?\s*A\.?軍?", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"国軍系(?:勢力)?\s*D\.?\s*K\.?\s*B\.?\s*A\.?", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"D\.?\s*K\.?\s*B\.?\s*A\.?,?", placeholder, s, flags=re.IGNORECASE)
    s = re.sub(r"ဒီမိုကရေစီ\s*အကျိုးပြု\s*ကရင်တပ်မတော်", placeholder, s)
    s = re.sub(r"ဒီမိုကရက်တစ်ကရင်အကျိုးပြုတပ်မတော်", placeholder, s)
    s = re.sub(r"ဒီမိုကရေစီ\s*ကရင်\s*တပ်မတော်", placeholder, s)

    count = 0
    def repl(_m):
        nonlocal count
        count += 1
        if short_after_first and count >= 2:
            return "DKBA"
        return "親国軍勢力DKBA"

    return re.sub(re.escape(placeholder), repl, s)


def _normalize_tatmadaw_terms(text: str) -> str:
    if text is None:
        return text
    s = str(text)
    if s.startswith("ERROR:"):
        return s

    replacements = [
        (r"親軍政権軍", "国軍"),
        (r"軍事政権軍", "国軍"),
        (r"軍政軍", "国軍"),
        (r"親軍政権部隊", "国軍"),
        (r"軍事政権部隊", "国軍"),
        (r"親軍政権側部隊", "国軍"),
        (r"軍事政権側部隊", "国軍"),
        (r"親軍政権傘下(?:の)?部隊", "国軍"),
        (r"軍事政権傘下(?:の)?部隊", "国軍"),
        (r"親軍政権傘下(?:の)?軍", "国軍"),
        (r"軍事政権傘下(?:の)?軍", "国軍"),
        (r"(?:親軍政権|軍事政権)トップ・ミンアウンフライン(?:が率いる|の率いる|率いる)(?:国軍|軍|部隊|軍部隊)", "国軍"),
        (r"ミンアウンフライン(?:が率いる|の率いる|率いる)(?:国軍|軍|部隊|軍部隊)", "国軍"),
        (r"大統領(?:が率いる|の率いる|率いる)(?:国軍|軍|部隊|軍部隊)", "国軍"),
        (r"国軍の部隊", "国軍"),
        (r"国軍部隊", "国軍"),
    ]
    for pat, repl in replacements:
        s = re.sub(pat, repl, s)
    return s


def _normalize_military_regime_for_official_source(text: str) -> str:
    if text is None:
        return text
    return (
        str(text)
        .replace("軍事政権下", "政府の下")
        .replace("親軍政権下", "政府の下")
        .replace("軍事政権側", "政府側")
        .replace("親軍政権側", "政府側")
        .replace("軍事政権当局", "政府当局")
        .replace("親軍政権当局", "政府当局")
        .replace("軍事政権トップ", "政府トップ")
        .replace("親軍政権トップ", "政府トップ")
        .replace("軍事政権指導者", "政府指導者")
        .replace("親軍政権指導者", "政府指導者")
        .replace("軍事政権", "政府")
        .replace("親軍政権", "政府")
    )


def _normalize_military_regime_for_non_official_source(text: str) -> str:
    if text is None:
        return text
    return str(text).replace("軍事政権", "親軍政権")


def _replace_placeholder_with_short_after_first(text: str, placeholder: str, full_text: str, short_text: str, shorten_after_first: bool) -> str:
    count = 0
    def repl(_m):
        nonlocal count
        count += 1
        if shorten_after_first and count >= 2:
            return short_text
        return full_text
    return re.sub(re.escape(placeholder), repl, str(text or ""))


def _normalize_min_aung_hlaing_term(text: str, official_style: bool, context: str = "body") -> str:
    if text is None:
        return text
    s = str(text)
    if s.startswith("ERROR:"):
        return s

    placeholder = "__TERM_PLACEHOLDER_M__"
    context = str(context or "body").lower()
    is_headline = context in {"headline", "title"}

    if official_style:
        full_target = "大統領" if is_headline else "ミンアウンフライン大統領"
        short_target = "大統領"
    else:
        full_target = "ミンアウンフライン" if is_headline else "親軍政権トップ・ミンアウンフライン"
        short_target = "ミンアウンフライン"
    shorten_after_first = not is_headline

    mah_name = r"(?:ミン[・･]?アウン[・･]?(?:フライン|ライン)|Min\s+Aung\s+Hlaing|မင်းအောင်လှိုင်)"
    mah_title = r"(?:氏|大統領|国家大統領|暫定大統領|国軍大統領|上級大将|大将|総司令官|国軍司令官|国軍総司令官|元国軍司令官|元国軍総司令官|国家行政評議会議長|SAC議長)?"
    leader_prefix = r"(?:(?:ミャンマー)?(?:親軍政権|軍事政権|国軍|軍事委員会|SAC)(?:の)?(?:トップ|指導者|最高指導者|リーダー)(?:である|としての|の|・)?\s*)"
    title_before = r"(?:(?:元)?国軍(?:総)?司令官|国軍総司令官|総司令官|上級大将|大将|暫定大統領|国家大統領|大統領)\s*"

    s = re.sub(r"(?:親軍政権|軍事政権)トップ・ミンアウンフライン(?:大統領)?", placeholder, s)
    s = re.sub(r"ミンアウンフライン大統領", placeholder, s)
    s = re.sub(leader_prefix + mah_name + mah_title, placeholder, s, flags=re.IGNORECASE)
    s = re.sub(title_before + mah_name + mah_title, placeholder, s, flags=re.IGNORECASE)
    s = re.sub(mah_name + mah_title, placeholder, s, flags=re.IGNORECASE)

    return _replace_placeholder_with_short_after_first(s, placeholder, full_target, short_target, shorten_after_first)


def normalize_output_terminology_by_source(text: str, source: str, context: str = "body") -> str:
    if text is None:
        return text
    s = str(text)
    if s.startswith("ERROR:"):
        return s

    context = str(context or "body").lower()
    is_headline = context in {"headline", "title"}

    s = _normalize_dkba_terms(s, short_after_first=not is_headline)
    official_style = _is_official_style_source(source)
    s = _normalize_min_aung_hlaing_term(s, official_style, context=context)
    s = _normalize_tatmadaw_terms(s)

    if official_style:
        s = _normalize_military_regime_for_official_source(s)
    else:
        s = _normalize_military_regime_for_non_official_source(s)

    return s


def strip_current_year_from_summary_dates(text: str) -> str:
    """要約内の当年表記を、JS側ルールに合わせて月日／月のみへ寄せる。"""
    if text is None:
        return text
    s = str(text)
    if s.startswith("ERROR:"):
        return s
    try:
        year = str(datetime.now(MMT).year)
    except Exception:
        year = str(datetime.now(MMT).year)

    def _to_int_text(v: str) -> str:
        table = str.maketrans("０１２３４５６７８９", "0123456789")
        try:
            return str(int(str(v).translate(table)))
        except Exception:
            return str(v).translate(table)

    s = re.sub(
        rf"{re.escape(year)}年([0-9０-９]{{1,2}})月([0-9０-９]{{1,2}})日",
        lambda m: f"{_to_int_text(m.group(1))}月{_to_int_text(m.group(2))}日",
        s,
    )
    s = re.sub(
        rf"{re.escape(year)}年([0-9０-９]{{1,2}})月",
        lambda m: f"{_to_int_text(m.group(1))}月",
        s,
    )
    return s


TITLE_OUTPUT_RULES = (
    "出力は見出し文だけを1行で返してください。\n"
    "【翻訳】や【日本語見出し案】、## 翻訳 などのラベル・注釈タグ・見出しは出力しないでください。\n"
    "文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
)

# ===== ▼ プロンプト管理（見出し翻訳 / 本文要約）================================
# 見出し翻訳（見出し3案）※共通ルールを含む
HEADLINE_PROMPT_1 = (
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
    "本文要約：\n"
    "- 以下の記事本文について重要なポイントをまとめ、最大500字で具体的に要約する（500字を超えない）。\n"
    "- 自然な日本語に翻訳する。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。\n"
    "- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。\n"
    "- レスポンスでは要約のみを返してください、それ以外の文言は不要です。\n\n"
    "本文要約の出力条件：\n"
    "- 1行目は`【要約】`とだけ書いてください。\n"
    "- 2行目以降が全て空行になってはいけません。\n"
    "- 見出しや箇条書きを適切に使って整理してください。\n"
    "- 見出しや箇条書きにはマークダウン記号（#, *, - など）を使わず、単純なテキストだけで書いてください。\n"
    "- 見出しは `[見出し名]` の形式で出力してください。\n"
    "\n"
    "【空行ルール（必ず守ること）】\n"
    "- `【要約】` の直後には空行を入れずに本文または見出しを続けてください。\n"
    "- 見出し `[見出し名]` の直後には空行を入れず、次の行から本文を続けてください。\n"
    "- 以下の2つのケースに限り、段落間に「1行だけ」空行を入れてください：\n"
    "  1) 見出しブロック（`[見出し]`＋本文）と次の見出しブロックの間（例：`[A]` → 本文A → 空行 → `[B]`）。\n"
    "  2) 本文が箇条書きではなく文章段落として複数ある場合の段落同士の間（例： 段落1 → 空行 → 段落2）。\n"
    "- 箇条書き（・）同士の間には空行を入れないでください（行を詰めて連続させる）。\n"
    "- 空行を2行以上連続させないこと（必ず1行だけ）。\n"
    "- 上記以外では空行を作らないでください。\n"
    "\n"
    "【その他のルール】\n"
    "- 箇条書きは`・`を使ってください。\n"
    "- 特殊記号は使わないでください（全体をHTMLとして送信するわけではないため）。\n"
    "- 「【要約】」は冒頭の1回のみ使用してください。\n"
    "- 思考手順（Step1/2、Q1/Q2、→ など）は出力に含めないでください。\n"
    "- 要約全体は最大500文字以内。重要情報（日時／主体／行為／規模／結果）を優先してください。\n"
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
    source_rules = _build_source_specific_translation_rules(item.get("source") or "")
    term_rules = _build_term_rules_prompt(item.get("title") or "", body)
    return header + COMMON_RULES_HEADER + source_rules + "\n" + pre + STEP3_TASK + (rg_title + rg_body) + term_rules + "\n" + input_block

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
        collect_gnlm_all_for_date,
        collect_popular_all_for_date,
        collect_frontier_all_for_date,
        collect_jetro_biznews_mm_all_for_date,
        collect_news_eleven_all_for_date,
    )
    try:
        from tmp.export_all_articles_to_csv import (
            _resolve_google_news_link_irrawaddy as _resolve_google_news_link_irrawaddy_shared,
        )
    except Exception:
        _resolve_google_news_link_irrawaddy_shared = None
    collectors_loaded = True
except Exception as e:
    collectors_loaded = False
    _resolve_google_news_link_irrawaddy_shared = None
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
    "global new light of myanmar": "GEMINI_API_KEY_GNLM",
    "global new light": "GEMINI_API_KEY_GNLM",
    "gnlm": "GEMINI_API_KEY_GNLM",
    "global new light of myanmar (国営紙)": "GEMINI_API_KEY_GNLM",
    "popular myanmar": "GEMINI_API_KEY_POPULARMYANMAR",
    "popular myanmar (国軍系メディア)": "GEMINI_API_KEY_POPULARMYANMAR",
    "frontier myanmar": "GEMINI_API_KEY_FRONTIERMYANMAR",
    "frontier": "GEMINI_API_KEY_FRONTIERMYANMAR",
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

from fetch_articles import genai 

# --- 媒体ごとの Gemini client を返すヘルパー ---
_CLIENT_CACHE: dict[str, genai.Client] = {}

def _client_for_source(source: str) -> genai.Client | None:
    """
    source（媒体名）に紐づいた API キーから Gemini client を返す。
    - キーが定義されていなければ None
    - 1つのキーにつき client を使い回す（_CLIENT_CACHE）
    """
    key = _gemini_key_for_source(source)
    if not key:
        logging.warning(f"[gemini] No API key found for source='{source}'. Using fallback (client_summary).")
        return None

    if key in _CLIENT_CACHE:
        logging.info(f"[gemini] Reusing client for source='{source}' key_prefix={key[:8]}...")
        return _CLIENT_CACHE[key]

    client = genai.Client(api_key=key)
    client._key_prefix = key[:8]  
    _CLIENT_CACHE[key] = client
    logging.info(
        f"[gemini] Created NEW client for source='{source}' "
        f"env_key={SOURCE_KEY_ENV_MAP.get(_norm(source))} key_prefix={key[:8]} len={len(key)}"
    )
    return client

def _clip_body_for_headline(body: str, max_chars: int = 1200) -> str:
    """見出し生成用に本文を安全に切り詰める。段落の途中切りも許容。"""
    b = (body or "").strip()
    if len(b) <= max_chars:
        return b
    return b[:max_chars].rstrip()

def _headline_variants_ja(title: str, source: str, url: str, body: str = "") -> List[str]:
    # ★ 媒体ごとの client を取得
    client = _client_for_source(source)

    # client が用意できない場合は安全フォールバック
    if not (call_llm_with_fallback and client):
        t = unicodedata.normalize("NFC", title or "").strip()
        return [t, t, t]

    model = os.getenv("GEMINI_HEADLINE_MODEL", "gemini-2.5-flash")

    # タイトルに出現した語 → D列（見出し訳）を採用
    # 本文に出現した語 → C列（本文訳）を採用
    rg_title = _region_rules_for_title(title)
    rg_body  = _region_rules_for_body(body)
    glossary = rg_title + rg_body + _build_term_rules_prompt(title, body)
    source_rules = _build_source_specific_translation_rules(source)

    # ---- 案1：原題ベース ----
    try:
        if _LIMITER:
            _LIMITER.wait()
        resp1 = call_llm_with_fallback(
            client,
            f"{COMMON_RULES_HEADER}{source_rules}\n{HEADLINE_PROMPT_1}{glossary}\n\n原題: {title}\nsource:{source}\nurl:{url}",
            model=model,
        )
        v1 = unicodedata.normalize("NFC", (resp1.text or "").strip())
    except Exception:
        v1 = unicodedata.normalize("NFC", (title or "").strip())

    # ---- 案2：案1を素材に再生成 ----
    try:
        if _LIMITER:
            _LIMITER.wait()
        prompt2 = COMMON_RULES_HEADER + source_rules + "\n" + (glossary or "") + make_headline_prompt_2_from(v1)
        resp2 = call_llm_with_fallback(client, prompt2, model=model)
        v2 = unicodedata.normalize("NFC", (resp2.text or "").strip())
    except Exception:
        v2 = v1

    # ---- 案3：本文から要素抽出して新聞見出し化 ----
    try:
        if _LIMITER:
            _LIMITER.wait()
        body_for_prompt = _clip_body_for_headline(body, max_chars=1200)
        if not body_for_prompt:
            # 本文が無いときは案1でフォールバック
            v3 = v1
        else:
            prompt3 = (
                COMMON_RULES_HEADER
                + source_rules + "\n"
                + f"{HEADLINE_PROMPT_3}\n\n"
                + (glossary or "")
                + "【本文】\n" + body_for_prompt + "\n\n"
                f"（参考）原題: {title}\nsource:{source}\nurl:{url}\n"
            )
            resp3 = call_llm_with_fallback(client, prompt3, model=model)
            v3 = unicodedata.normalize("NFC", (resp3.text or "").strip())
    except Exception:
        v3 = v1

    # 生成後の最終統一（州・管区名 → 既存）＋（用語集 → 新規）
    v1 = _apply_region_glossary_to_text(v1); v1 = _apply_term_glossary_to_output(v1, src=title, prefer="title_ja"); v1 = normalize_output_terminology_by_source(v1, source, context="headline")
    v2 = _apply_region_glossary_to_text(v2); v2 = _apply_term_glossary_to_output(v2, src=title, prefer="title_ja"); v2 = normalize_output_terminology_by_source(v2, source, context="headline")
    v3 = _apply_region_glossary_to_text(v3); v3 = _apply_term_glossary_to_output(v3, src=title, prefer="title_ja"); v3 = normalize_output_terminology_by_source(v3, source, context="headline")
    return [v1, v2, v3]

def _summary_ja(source: str, title: str, body: str, url: str) -> str:
    # ★ 媒体ごとの client を取得
    client = _client_for_source(source)

    if not (call_llm_with_fallback and client):
        # グローバルキー無し前提：媒体別キーが無い場合は安全フォールバック＋警告
        try:
            import sys
            print(
                f"[warn] Gemini key/client not configured for source='{source}'. "
                f"Skipped API call; returned trimmed body.",
                file=sys.stderr,
            )
        except Exception:
            pass
        text = unicodedata.normalize("NFC", (body or "").strip())[:400]
        text = _apply_region_glossary_to_text(text)
        text = _apply_term_glossary_to_output(text, src=body, prefer="body_ja")
        text = normalize_output_terminology_by_source(text, source, context="body")
        text = strip_current_year_from_summary_dates(text)
        return text

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
        resp = call_llm_with_fallback(
            client,
            prompt,
            model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-2.5-flash"),
        )
        text = unicodedata.normalize("NFC", (resp.text or "").strip())
        text = _apply_region_glossary_to_text(text)
        text = _apply_term_glossary_to_output(text, src=body, prefer="body_ja")
        text = normalize_output_terminology_by_source(text, source, context="body")
        text = strip_current_year_from_summary_dates(text)
        return text
    except Exception:
        text = unicodedata.normalize("NFC", (body or "").strip())[:400]
        text = _apply_region_glossary_to_text(text)
        text = _apply_term_glossary_to_output(text, src=body, prefer="body_ja")
        text = normalize_output_terminology_by_source(text, source, context="body")
        text = strip_current_year_from_summary_dates(text)
        return text


def _is_ayeyarwady(title_raw: str, body_raw: str) -> bool:
    """
    記事「原文」のタイトル/本文に対して Ayeyarwady 判定を行う。

    fetch_articles.py の is_ayeyarwady_hit と同一ロジックを優先的に利用し、
    import に失敗した場合のみ、同じキーワード集合でフォールバック判定する。
    """
    from unicodedata import normalize

    # fetch_articles 側と同様に NFC 正規化してから判定
    t = normalize("NFC", (title_raw or "").strip())
    b = normalize("NFC", (body_raw  or "").strip())

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

def _item_dedupe_key(it: Dict) -> str:
    """
    同一URLの複数記事を区別するため、collector が持つ _row_key を優先する。
    なければ従来どおり URL を使う。
    """
    return (
        (it.get("_row_key") or "").strip()
        or (it.get("url") or "").strip()
    )

def _deduplicate_items(items: List[Dict]) -> List[Dict]:
    """
    _row_key 優先で順序保持 dedupe。
    BBC の /burmese/articles/... のような同一URL複数記事を落とさない。
    """
    seen = set()
    out: List[Dict] = []
    for it in items:
        key = _item_dedupe_key(it)
        if key and key not in seen:
            out.append(it); seen.add(key)
    return out

def _collect_all_for(
    target_date_mmt: date,
    schedule_cron: str | None = None,
    only_source: str | None = None,
) -> List[Dict]:
    if not collectors_loaded:
        raise SystemExit("収集関数の読み込み失敗。export_all_articles_to_csv.py を配置してください。")
    items: List[Dict] = []
    plan: List[tuple] = [
        ("Mizzima (Burmese)", collect_mizzima_all_for_date, {"max_pages": 3}),
        ("BBC Burmese", collect_bbc_all_for_date, {}),
        ("Khit Thit Media", collect_khitthit_all_for_date, {"max_pages": 5}),
        ("DVB", collect_dvb_all_for_date, {}),
        ("Myanmar Now", collect_myanmar_now_mm_all_for_date, {"max_pages": 3}),
    ]

    if _should_collect_gnlm(schedule_cron):
        plan.append(("GNLM", collect_gnlm_all_for_date, {"max_pages": 3}))
        logging.info(f"[rules] GNLM enabled (schedule_cron={schedule_cron})")
    else:
        logging.info(f"[rules] GNLM disabled (schedule_cron={schedule_cron})")

    plan.extend([
        ("Popular Myanmar", collect_popular_all_for_date, {}),
        ("Frontier Myanmar", collect_frontier_all_for_date, {}),
        ("JETRO", collect_jetro_biznews_mm_all_for_date, {}),
        ("News Eleven", collect_news_eleven_all_for_date, {}),
    ])

    if _should_collect_irrawaddy(schedule_cron):
        plan.insert(0, ("Irrawaddy", collect_irrawaddy_all_for_date, {}))
        logging.info(f"[rules] Irrawaddy enabled (schedule_cron={schedule_cron})")
    else:
        logging.info(f"[rules] Irrawaddy disabled (schedule_cron={schedule_cron})")

    if only_source:
        wanted = unicodedata.normalize("NFC", only_source).strip().casefold()
        before_count = len(plan)
        plan = [
            (label, fn, kwargs)
            for label, fn, kwargs in plan
            if unicodedata.normalize("NFC", label).strip().casefold() == wanted
        ]
        logging.info(
            f"[collect] only_source={only_source!r} matched_collectors={len(plan)}/{before_count}"
        )
        if not plan:
            logging.warning(f"[collect] no collector matched only_source={only_source!r}")

    for label, fn, kwargs in plan:
        name = label
        try:
            with _timeit(f"collector:{name}", date=target_date_mmt.isoformat(), kwargs=kwargs or None):
                before = len(items)
                fetched = fn(target_date_mmt, **kwargs)
                items.extend(fetched)
                logging.info(f"[collect:{name}] fetched={len(fetched)} total={len(items)}")
        except Exception as e:
            logging.exception(f"[warn] collector failed: {fn.__name__}: {e}")
    dedup_before = len(items)
    items = _deduplicate_items(items)
    if dedup_before != len(items):
        logging.info(f"[collect] dedup {dedup_before} -> {len(items)} (-{dedup_before - len(items)})")
    else:
        logging.info(f"[collect] total={len(items)} (no dup)")
    return items

# ===== Sheets I/O =====
# --- Google Sheets のセル上限対策 ---
# 1セルあたりの最大文字数は 50,000 文字なので、少し余裕を見てクリップする
SHEET_CELL_MAX_CHARS = 50000
SHEET_BODY_MAX_CHARS = 48000  # 本文用の安全マージン

def _clip_for_sheet_cell(s: str, max_chars: int = SHEET_BODY_MAX_CHARS) -> str:
    """
    Google Sheets の1セル上限 50,000文字を超えないように
    安全に切り詰めるヘルパー。
    """
    if not s:
        return ""
    s = str(s)
    if len(s) <= max_chars:
        return s

    # 末尾に「セル上限のため省略」の印を付けておく
    suffix = "\n\n[… セル内文字数上限のため省略 …]"
    keep = max_chars - len(suffix)
    if keep <= 0:
        # 念のための保険（ほぼ来ないはず）
        return s[:max_chars]
    return s[:keep].rstrip() + suffix

def _ws():
    return _gc_client().open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def _read_all_rows():
    ws = _ws(); vals = ws.get_all_values()
    header = vals[0] if vals else []
    rows = vals[1:] if len(vals) > 1 else []
    return header, rows, ws

def _existing_row_keys_set() -> set:
    header, rows, _ = _read_all_rows()
    name_to_idx = {n: i for i, n in enumerate(header)}
    idx_J = name_to_idx.get("URL", 9)   # J列
    idx_Q = 16                          # Q列: 内部 row key 用
    keys = set()
    for r in rows:
        try:
            row_key = ""
            if len(r) > idx_Q:
                row_key = (r[idx_Q] or "").strip()
            url = ""
            if len(r) > idx_J:
                url = (r[idx_J] or "").strip()
            key = row_key or url
            if key:
                keys.add(key)
        except Exception:
            pass
    return keys

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

    # A2 以降の全ての列をクリア
    ws.batch_clear(["2:1000"])   # クリアしたい最大行数は適宜調整

    # 今日の行だけ A2 から詰め直す（A..J）
    if kept:
        ws.update("A2", kept, value_input_option="USER_ENTERED")

    # K列（採用フラグ）は全行 ブランク にリセット（A..J が空の行も含む）
    if total > 0:
        ws.update(f"K2:K{total+1}", [[""]]*total, value_input_option="USER_ENTERED")

    return len(rows) - len(kept)

# ===== Gemini を呼ばずに E=タイトル原文, F=本文原文 を書き出す版 =====
def cmd_collect_to_sheet(args):
    now_mmt = datetime.now(MMT)
    target = now_mmt.date()
    if getattr(args, "target_offset_days", 0):
        target = target + timedelta(days=int(args.target_offset_days))
    
    logging.info(
        f"[collect] start target_date_mmt={target.isoformat()} "
        f"clear_yesterday={getattr(args,'clear_yesterday',False)} "
        f"target_offset_days={getattr(args,'target_offset_days',0)} "
        f"only_source={getattr(args,'only_source',None)!r}"
    )

    # 前日クリアオプション
    if getattr(args, "clear_yesterday", False):
        today = now_mmt.date().isoformat()
        with _timeit("clear-yesterday", date=today):
            removed = _keep_only_rows_of_date(today)
        logging.info(f"[clean] kept only {today} (removed {removed} rows)")

    # ① 各メディアから記事収集（export_all_articles_to_csv と同じ collectors）
    items = _collect_all_for(
        target,
        getattr(args, "schedule_cron", None),
        getattr(args, "only_source", None),
    )
    if not items:
        logging.warning("[collect] no items to write")
        print("no items to write")
        return

    existing = _existing_row_keys_set()
    logging.info(f"[sheet] existing_urls={len(existing)}")

    ts = datetime.now(MMT).strftime("%Y-%m-%d %H:%M:%S")
    rows_to_append: List[List[str]] = []
    bundle_dir = getattr(args, "bundle_dir", "bundle")

    for it in items:
        source = it.get("source") or ""
        title  = it.get("title") or ""
        url    = (it.get("url") or "").strip()
        row_key = _item_dedupe_key(it)
        
        # Google News URL は可能なら実記事URLへ正規化してから、
        # 重複判定 / 本文取得 / シート出力のすべてに使う
        normalized_url = url
        try:
            from urllib.parse import urlparse
            host = urlparse(url).netloc.lower()
        except Exception:
            host = ""

        if "news.google.com" in host:
            try:
                resolved = (_resolve_news_google_redirect_global(url) or "").strip()
                normalized_url = resolved or url
                if resolved and resolved != url:
                    logging.info(f"[url] normalized source={source} from={url} to={normalized_url}")
                elif not resolved:
                    logging.warning(f"[url] normalize failed; keep original source={source} url={url}")
            except Exception as e:
                normalized_url = url
                logging.warning(f"[url] normalize error; keep original source={source} url={url} err={e}")

        if not normalized_url:
            logging.warning(f"[skip] normalized_url empty source={source} raw_url={url!r}")
            continue

        if _is_irrawaddy_excluded_url(normalized_url):
            logging.info(f"[skip] excluded irrawaddy url={normalized_url}")
            continue

        effective_row_key = (row_key or normalized_url).strip()

        if effective_row_key in existing:
            logging.debug(f"[skip] duplicated row_key={effective_row_key!r}")
            continue

        # collector が本文を持っていればそれを最優先（再取得しない）
        body = (it.get("body") or "").strip()
        summary = (it.get("summary") or "").strip()
        if body:
            with _BODIES_LOCK:
                cache = _load_bodies_cache(bundle_dir)
                cache = _upsert_body_cache_entry(
                    cache,
                    url=normalized_url,
                    source=source,
                    title=title,
                    body=body,
                )
                _save_bodies_cache(bundle_dir, cache)
        else:
            # なければ堅牢抽出器で1回だけ取得→キャッシュ
            body = _get_body_once(normalized_url, source, out_dir=bundle_dir, title=title, summary=summary)

        # ★ ここから：Gemini は使わず、エーヤワディ判定だけを行う
        is_ay = _is_ayeyarwady(title, body)
        logging.info(
            f"[row] source={source} body_len={len(body)} "
            f"ayeyarwady={is_ay} url={normalized_url}"
        )

        # ★ E/F/G はブランク, Q に内部 row key, M にタイトル原文, N に本文原文
        rows_to_append.append([
            target.isoformat(),                  # A 日付(配信日)
            ts,                                  # B timestamp
            source,                              # C メディア
            "TRUE" if is_ay else "FALSE",        # D エーヤワディー
            "", "", "",                          # E/F/G 見出し訳３案（今回は空）
            "",                                  # H 確定見出し（手動）
            "",                                  # I 本文要約（空）
            normalized_url,                      # J URL
            "",                                  # K 採用フラグ（初期値空）
            "",                                  # L
            title,                               # M 記事タイトル原文
            _clip_for_sheet_cell(body),          # N 記事本文原文
            "",                                  # O
            "",                                  # P
            effective_row_key,                   # Q 内部 row key（重複判定用）
            "",                                  # R
            "",                                  # S
        ])
        existing.add(effective_row_key)

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

    # ② 採用フラグで選別（※並べ替えしない。シートの出現順を維持）
    from collections import defaultdict
    with _timeit("build-bundle:select"):
        selected_rows = []
        media_counts = defaultdict(int)
        for r in rows:
            if get(r, "K") != "a":
                continue
            selected_rows.append(r)
            media_counts[get(r, "C")] += 1

    total_selected = len(selected_rows)
    if total_selected == 0:
        logging.info("[bundle] no summaries selected (K='a')")
        print("no summaries selected (K='a')"); return

    # ③ summaries 構築 + bodies.json 構築
    with _timeit("build-bundle:construct", selected=len(selected_rows)):
        summaries = []
        bodies: list[dict] = []       # bodies.json 用。item_id ベースの配列で行順を維持
        pdf_items: list[dict] = []    # PDF生成用。item_id 単位で同一URL重複を保持

        for seq, r in enumerate(selected_rows, start=1):
            delivery    = get(r, "A") # 日付(A)
            media       = get(r, "C") # メディア(C)
            title_final = get(r, "H") # 確定見出し日本語訳(H)
            body_sum    = get(r, "I") # 本文要約(I)
            url         = get(r, "J") # URL(J)
            is_ay       = (get(r, "D").upper() == "TRUE") # エーヤワディー(D)
            item_id     = f"sheet-{seq:04d}"

            summaries.append({
                "item_id": item_id,
                "source": media,
                "url": url,
                "title": unicodedata.normalize("NFC", title_final),
                "summary": unicodedata.normalize("NFC", body_sum),
                "is_ayeyarwady": is_ay,
                "is_ayeyar": is_ay,
                "date_mmt": delivery,
            })

            # PDF用の原文データは URL ではなく item_id 単位で保持する
            body_raw   = get(r, "N")            # N列: 本文原文
            title_body = get(r, "H") or get(r, "M")
            pdf_items.append({
                "item_id": item_id,
                "source": media,
                "url": url,
                "title": unicodedata.normalize("NFC", title_body),
                "body": body_raw,
                "date_mmt": delivery,
            })

            # bodies.json も item_id 単位の配列として保持し、同一URLでも潰さない
            if url and body_raw:
                bodies.append({
                    "item_id": item_id,
                    "source": media,
                    "url": url,
                    "title": unicodedata.normalize("NFC", title_body),
                    "body": body_raw,
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

        # Ayeyarwady 専用サマリ（INTERNAL 配信で fetch_articles.py が参照）
        # ※ K列の採用フラグには依存しない（D=TRUE なら K 不問、かつシート出現順を維持）
        summaries_ayeyar = []
        for r in rows:  # rows 全体をシート出現順で走査
            if (get(r, "D").upper() != "TRUE"):
                continue
            delivery    = get(r, "A")  # 日付(A)
            media       = get(r, "C")  # メディア(C)
            
            # INTERNAL（エーヤワディ専用）向けタイトル
            # H列（確定見出し）が空なら、E列（見出し訳案1）をフォールバックとして採用
            title_final = get(r, "H")  # 確定見出し日本語訳(H)
            if not title_final:
                title_final = get(r, "E")  # フォールバック: 見出し訳案1(E)
            
            body_sum    = get(r, "I")  # 本文要約(I)
            url         = get(r, "J")  # URL(J)
            summaries_ayeyar.append({
                "source": media,
                "url": url,
                "title": unicodedata.normalize("NFC", title_final),
                "summary": unicodedata.normalize("NFC", body_sum),
                "is_ayeyarwady": True,
                "is_ayeyar": True,   # 件名ロジック互換
                "date_mmt": delivery,
            })

        with open(os.path.join(out_dir, "summaries_ayeyar.json"), "w", encoding="utf-8") as f:
            json.dump(summaries_ayeyar, f, ensure_ascii=False, indent=2)

    logging.info(f"[bundle] rebuilt dir={out_dir} items={len(summaries)} date_mmt={summaries[0]['date_mmt']}")
    print(f"bundle rebuilt: {out_dir} (items={len(summaries)})")

    # キャッシュ済み本文を使って全文翻訳PDFを生成（BUSINESS/TRIAL 添付用）
    def _make_business_pdf_from_items(pdf_items: list[dict], date_iso: str, out_dir: str):
        if not (translate_fulltexts_for_business and build_combined_pdf_for_business):
            logging.warning("[bundle] PDF helpers unavailable; skip business PDF")
            return

        items_for_translation: list[dict] = []
        for it in pdf_items:
            item_id = (it.get("item_id") or "").strip()
            url = (it.get("url") or "").strip()
            source = it.get("source") or ""
            title = it.get("title") or ""
            body = (it.get("body") or "").strip()

            if not body and url:
                body = _get_body_once(url, source, out_dir=out_dir, title=title)

            if not body:
                logging.warning(f"[bundle] empty body for PDF item_id={item_id} url={url}")
                continue

            items_for_translation.append({
                "item_id": item_id,
                "url": url,
                "source": source,
                "title": title,
                "body": body,
                "date_mmt": it.get("date_mmt") or date_iso,
            })

        if not items_for_translation:
            logging.info("[bundle] no items for PDF")
            return

        translated = translate_fulltexts_for_business(items_for_translation)
        if not translated:
            logging.warning("[bundle] translation returned empty; skip PDF")
            return

        # item_id 単位で PDF メタを引き当てる（同一URL重複を保持）
        item_to_meta = {}
        for it in items_for_translation:
            item_id = (it.get("item_id") or "").strip()
            if not item_id:
                continue
            date_raw = it.get("date_mmt", "") or (date_iso or "")
            d_obj = _coerce_date(date_raw)
            date_str = (d_obj.isoformat() if d_obj else (date_iso or ""))
            item_to_meta[item_id] = {
                "title_ja": it.get("title", "") or "",
                "source": it.get("source", "") or "",
                "date": date_str,
                "url": (it.get("url") or "").rstrip("/"),
            }

        translated_items = []
        for it in translated:
            item_id = (it.get("item_id") or "").strip()
            meta = item_to_meta.get(item_id, {})
            if not meta:
                continue
            source = meta.get("source", "") or ""
            title_ja = _apply_region_glossary_to_text(meta.get("title_ja", ""))
            title_ja = normalize_output_terminology_by_source(title_ja, source, context="headline")
            body_ja  = _apply_region_glossary_to_text(it.get("body_ja", "") or "")
            body_ja  = normalize_output_terminology_by_source(body_ja, source, context="body")
            translated_items.append({
                "item_id": item_id,
                "url": meta.get("url", ""),
                "title_ja": title_ja,
                "body_ja": body_ja,
                "source": meta.get("source", ""),
                "date": meta.get("date", ""),
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

    # ⑤ bodies.json の書き出し（item_id ベースの配列、シート行順のまま）
    if bodies:
        _save_bodies_cache(out_dir, bodies)

    try:
        _make_business_pdf_from_items(pdf_items, meta["date_mmt"], out_dir)
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
    p1.add_argument("--schedule-cron", default=None, help="(GitHub Actions) github.event.schedule の cron 文字列。Irrawaddy の実行枠判定に使用")
    p1.add_argument("--target-offset-days", type=int, default=0, help="収集対象日をMMT基準で相対シフトする。-1で昨日")
    p1.add_argument("--only-source", default=None, help="指定した媒体のみ収集する（例: 'Khit Thit Media'）")
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
