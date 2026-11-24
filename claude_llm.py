"""
Claude API（Anthropic）呼び出しの共通処理（Python 側）。

- 環境変数 LLM_PROVIDER が "claude"（既定）のとき、fetch_articles.py の
  call_gemini_with_retries / call_llm_with_fallback と
  selection_ml/run_selection_ml.py の Gemini 再スコアが Claude に切り替わる。
  "gemini" にすると従来の Gemini 経路（OpenAI フォールバック含む）に戻る。
- Claude 経路では OpenAI へのフォールバックは行わない（OpenAI 実装は残してある）。

環境変数:
- ANTHROPIC_API_KEY    : Claude API キー（必須）
- LLM_PROVIDER         : "claude" | "gemini"（既定 "claude"）
- CLAUDE_API_MODEL_MAIN  : 読者向けの日本語生成（要約・見出し・全文翻訳）。既定 claude-sonnet-5-5
- CLAUDE_API_MODEL_LIGHT : 判定・分類・タイトル翻訳。既定 claude-haiku-5-5
- CLAUDE_API_EFFORT      : 既定 low（思考トークン＝出力課金を抑える）
"""

from __future__ import annotations

import logging
import os
from types import SimpleNamespace
from typing import Any, Optional

try:
    import anthropic
except Exception:  # SDK 未インストールでも import 自体は通す（gemini 経路用）
    anthropic = None

LLM_PROVIDER = (os.getenv("LLM_PROVIDER") or "claude").strip().lower()

CLAUDE_MODEL_MAIN = (os.getenv("CLAUDE_API_MODEL_MAIN") or "claude-sonnet-5-5").strip()
CLAUDE_MODEL_LIGHT = (os.getenv("CLAUDE_API_MODEL_LIGHT") or "claude-haiku-5-5").strip()
CLAUDE_EFFORT = (os.getenv("CLAUDE_API_EFFORT") or "low").strip()

# 1時間TTL（同じ固定プロンプトが数分〜数十分おきに使われるため）
CLAUDE_CACHE_TTL = "1h"

# Sonnet 5.5 の安全分類器による拒否時に、Anthropic 側で別モデルに再実行させる（Claude内のみ）
CLAUDE_SERVER_FALLBACK_BETA = "server-side-fallback-2026-07-01"

# usage_tag → 軽量モデル（Haiku）を使う用途。これ以外は MAIN（Sonnet）。
LIGHT_USAGE_TAGS = {
    "dedupe",
    "title-translate",
    "selection-rerank",
}

# 1M トークンあたり USD（cache_write は 1時間TTL = 入力単価×2 として概算）
_PRICES = {
    "claude-sonnet-5-5": {"input": 2.0, "cache_read": 0.10, "cache_write": 4.0, "output": 10.0},
    "claude-haiku-5-5": {"input": 0.10, "cache_read": 0.01, "cache_write": 0.20, "output": 0.50},
}

_CLIENT = None


def is_claude_primary() -> bool:
    return LLM_PROVIDER == "claude"


def model_for_usage_tag(usage_tag: str) -> str:
    tag = (usage_tag or "").strip()
    if tag in LIGHT_USAGE_TAGS:
        return CLAUDE_MODEL_LIGHT
    return CLAUDE_MODEL_MAIN


def _get_client():
    global _CLIENT
    if _CLIENT is None:
        if anthropic is None:
            raise RuntimeError("anthropic SDK is not installed (pip install anthropic)")
        if not (os.getenv("ANTHROPIC_API_KEY") or "").strip():
            raise RuntimeError("ANTHROPIC_API_KEY is not set")
        # SDK が 408/409/429/5xx/529 と通信エラーを指数バックオフで自動リトライする
        _CLIENT = anthropic.Anthropic(max_retries=4, timeout=600.0)
    return _CLIENT


def _estimate_usd(model: str, usage: Any) -> Optional[float]:
    price = None
    for prefix, p in _PRICES.items():
        if (model or "").startswith(prefix):
            price = p
            break
    if not price or usage is None:
        return None
    return (
        (getattr(usage, "input_tokens", 0) or 0) * price["input"]
        + (getattr(usage, "cache_read_input_tokens", 0) or 0) * price["cache_read"]
        + (getattr(usage, "cache_creation_input_tokens", 0) or 0) * price["cache_write"]
        + (getattr(usage, "output_tokens", 0) or 0) * price["output"]
    ) / 1_000_000


def call_claude(
    prompt: str,
    *,
    usage_tag: str = "generic",
    model: Optional[str] = None,
    system: Optional[str] = None,
    cache_system: bool = True,
    json_schema: Optional[dict] = None,
    effort: Optional[str] = None,
    max_tokens: int = 32000,
) -> SimpleNamespace:
    """
    Claude を1回呼び出し（リトライは SDK 任せ）、Gemini の resp と同じく .text を持つオブジェクトを返す。

    - system を渡すと system ブロックにしてキャッシュ（固定ルール向け）
    - json_schema を渡すと Structured Outputs で JSON を強制
    - 拒否（refusal）/ max_tokens 到達 / 空応答は例外にする（呼び出し側の既存 except で扱う）
    """
    client = _get_client()
    model = model or model_for_usage_tag(usage_tag)
    effort = effort or CLAUDE_EFFORT

    output_config: dict[str, Any] = {"effort": effort}
    if json_schema:
        output_config["format"] = {"type": "json_schema", "schema": json_schema}

    params: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "output_config": output_config,
    }
    if system:
        block: dict[str, Any] = {"type": "text", "text": system}
        if cache_system:
            block["cache_control"] = {"type": "ephemeral", "ttl": CLAUDE_CACHE_TTL}
        params["system"] = [block]

    logging.info(f"[claude-call] model={model} effort={effort} usage_tag={usage_tag} prompt_chars={len(prompt or '')}")

    # 全文翻訳など出力が長い呼び出しで HTTP タイムアウトにならないよう、常にストリーミングで受ける
    if model.startswith("claude-sonnet-5-5"):
        # Haiku 5.5 には server-side fallback が無いので Sonnet 系だけ
        with client.beta.messages.stream(
            betas=[CLAUDE_SERVER_FALLBACK_BETA],
            fallbacks="default",
            **params,
        ) as stream:
            resp = stream.get_final_message()
    else:
        with client.messages.stream(**params) as stream:
            resp = stream.get_final_message()

    usage = getattr(resp, "usage", None)
    usd = _estimate_usd(getattr(resp, "model", model), usage)
    print(
        "[claude-usage][{tag}] model={m} in={i} cache_write={cw} cache_read={cr} out={o} stop={s} usd={u}".format(
            tag=usage_tag,
            m=getattr(resp, "model", model),
            i=getattr(usage, "input_tokens", 0),
            cw=getattr(usage, "cache_creation_input_tokens", 0),
            cr=getattr(usage, "cache_read_input_tokens", 0),
            o=getattr(usage, "output_tokens", 0),
            s=resp.stop_reason,
            u="n/a" if usd is None else f"{usd:.6f}",
        )
    )

    if resp.stop_reason == "refusal":
        details = getattr(resp, "stop_details", None)
        category = getattr(details, "category", None) if details else None
        raise RuntimeError(f"Claude refusal (category={category}) usage_tag={usage_tag}")
    if resp.stop_reason == "max_tokens":
        raise RuntimeError(f"Claude max_tokens reached (max_tokens={max_tokens}) usage_tag={usage_tag}")

    # thinking / fallback ブロックが混ざるので type で text だけを拾う
    text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
    if not text:
        raise RuntimeError(f"Claude returned empty text (stop={resp.stop_reason}) usage_tag={usage_tag}")

    return SimpleNamespace(text=text, usage=usage, model=getattr(resp, "model", model))
