#!/usr/bin/env python3
"""Train from monthly archive spreadsheets and score current sheet rows.

Pure ML classification version.

Applied changes:
- The model is binary classification again, not ranking.
  It predicts whether each row is likely to be manually selected.
- Manual order is kept as training weight only. Within each date/sheet group,
  K=a rows are read in their sheet order, and higher-priority manual rows get
  a larger positive sample weight. Labels are still binary: selected=1,
  non-selected=0.
- Hand-written topic rules remain removed:
  no priority-topic feature extraction, no direct-Myanmar-relevance feature,
  no previous-day duplicate/continuation adjustment, no recency weighting.
- Media-name features are also removed. Column C may still be read for records,
  but media names are not used as model features.
- Optional Gemini reranking is added as a second-stage evaluator.
  It returns a 0-100 Gemini score using abstract editorial criteria, then
  final_score is set to the higher of ML score and Gemini score.
"""

from __future__ import annotations

import http.client
import json
import os
import random
import re
import socket
import ssl
import sys
import time
import unicodedata
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any, Iterable

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

ARCHIVE_FILE_PREFIX = "prod_"
ARCHIVE_SHEET_NAME = "prod"
LOCAL_ADOPTED_ARCHIVE_SHEET_BY_TARGET = {"prod": "archive_prod", "dev": "archive_dev"}
MODEL_VERSION = "selection-ml-v17-max-ml-gemini-score"
OUTPUT_HEADERS = [
    "MLスコア",
    "ML判定補足",
    "MLモデルバージョン",
    "MLスコア上昇要因",
    "MLスコア低下要因",
    "ML寄与詳細JSON",
    "Geminiスコア",
    "Gemini判定",
    "Gemini理由",
    "最終スコア",
    "採用スコア種別",
    "Gemini詳細JSON",
]
OUTPUT_START_COLUMN = "R"
OUTPUT_END_COLUMN = "AC"
MAX_REASON_FEATURES = 8
MIN_COLUMNS = 32

SHEETS_WRITE_CHUNK_ROWS_DEFAULT = 25
SHEETS_WRITE_MAX_PAYLOAD_BYTES_DEFAULT = 1_800_000
SHEETS_WRITE_MAX_RETRIES_DEFAULT = 6
SHEETS_WRITE_RETRY_BASE_SECONDS_DEFAULT = 2.0
SHEETS_WRITE_RETRY_MAX_SECONDS_DEFAULT = 32.0

# Gemini reranking is enabled by default in code.
# GEMINI_API_KEY must be supplied from GitHub Actions Secrets or another environment variable.
ENABLE_GEMINI_RERANK = True
DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
DEFAULT_GEMINI_FALLBACK_MODEL = "gemini-2.5-flash"
GEMINI_MODEL_FALLBACK_WAIT_SECONDS_DEFAULT = 2.0
GEMINI_DEFAULT_MIN_ML_SCORE = 0
GEMINI_DEFAULT_MAX_ARTICLES = 150
GEMINI_DEFAULT_BATCH_SIZE = 20
ARCHIVE_ADOPTED_CONTEXT_MAX_ITEMS = 6
ARCHIVE_ADOPTED_CONTEXT_MAX_CHARS = 2600
SAME_DAY_CONTEXT_MAX_ITEMS = 6
SAME_DAY_CONTEXT_MAX_CHARS = 2600

COL_DATE = 0       # A
COL_MEDIA = 2      # C
COL_E = 4          # E
COL_F = 5          # F
COL_G = 6          # G
COL_I = 8          # I
COL_URL = 9        # J
COL_ADOPTED = 10   # K
COL_SAME_TOPIC_FLAG = 14  # O
COL_SAME_TOPIC_NOTE = 15  # P
COL_Q_KEY = 16     # Q


@dataclass(frozen=True)
class ArticleRecord:
    row_index: int
    date_key: str
    group_key: str
    media: str
    headline_a: str
    headline_final: str
    headline_body: str
    summary: str
    url: str
    same_topic_flag: str
    same_topic_note: str
    duplicate_key: str
    label: int | None = None
    manual_rank: int | None = None

    @property
    def full_text(self) -> str:
        return "\n".join(
            [self.headline_a, self.headline_final, self.headline_body, self.summary]
        ).strip()


class ConstantClassificationModel:
    """Fallback used when classification training data is not usable."""

    def __init__(self, probability: float, info: dict[str, Any]):
        self.probability = probability
        self.info = info

    def probabilities(self, rows: list[ArticleRecord]) -> list[float]:
        return [self.probability] * len(rows)

    def prediction_details(
        self,
        rows: list[ArticleRecord],
        top_n: int = MAX_REASON_FEATURES,
    ) -> list[dict[str, Any]]:
        score = normalize_probability_scores([self.probability])[0]
        return [
            {
                "score": score,
                "probability": float(self.probability),
                "decision_value": None,
                "intercept": None,
                "positive_reasons": [],
                "negative_reasons": [],
                "note": "学習データが2クラス分類に不足したため、全記事に一定確率を出力しました。",
            }
            for _ in rows
        ]


class SklearnClassificationModel:
    def __init__(self, feature_pipeline: Any, classifier: Any, info: dict[str, Any]):
        self.feature_pipeline = feature_pipeline
        self.classifier = classifier
        self.info = info
        self.feature_names = feature_names_from_pipeline(feature_pipeline)

    def probabilities(self, rows: list[ArticleRecord]) -> list[float]:
        features = self.feature_pipeline.transform(rows)
        if hasattr(self.classifier, "predict_proba"):
            return [float(v) for v in self.classifier.predict_proba(features)[:, 1]]
        # Defensive fallback for estimators without predict_proba.
        return [float(v) for v in self.classifier.predict(features)]

    def prediction_details(
        self,
        rows: list[ArticleRecord],
        top_n: int = MAX_REASON_FEATURES,
    ) -> list[dict[str, Any]]:
        """Return per-row score explanations based on linear-model contributions.

        LogisticRegression is a linear model. For each article, the contribution
        of a feature is roughly: transformed_feature_value * learned_coefficient.
        Positive values raise the selected-class log-odds; negative values lower it.
        """
        features = self.feature_pipeline.transform(rows)
        probabilities = self.probabilities(rows)
        scores = normalize_probability_scores(probabilities)
        coef = self.classifier.coef_[0]
        intercept = float(self.classifier.intercept_[0]) if hasattr(self.classifier, "intercept_") else 0.0

        if hasattr(self.classifier, "decision_function"):
            decision_values = [float(v) for v in self.classifier.decision_function(features)]
        else:
            decision_values = [None] * len(rows)

        details: list[dict[str, Any]] = []
        for idx, (score, probability) in enumerate(zip(scores, probabilities, strict=True)):
            row_vector = features.getrow(idx)
            contributions_matrix = row_vector.multiply(coef).tocoo()
            contributions: list[tuple[int, float]] = [
                (int(col), float(value))
                for col, value in zip(
                    contributions_matrix.col,
                    contributions_matrix.data,
                    strict=True,
                )
                if abs(float(value)) > 1e-12
            ]

            positive_items = sorted(
                [(col, value) for col, value in contributions if value > 0],
                key=lambda item: item[1],
                reverse=True,
            )[:top_n]
            negative_items = sorted(
                [(col, value) for col, value in contributions if value < 0],
                key=lambda item: item[1],
            )[:top_n]

            details.append(
                {
                    "score": score,
                    "probability": float(probability),
                    "decision_value": decision_values[idx],
                    "intercept": intercept,
                    "positive_reasons": [
                        build_feature_contribution_dict(col, value, self.feature_names)
                        for col, value in positive_items
                    ],
                    "negative_reasons": [
                        build_feature_contribution_dict(col, value, self.feature_names)
                        for col, value in negative_items
                    ],
                    "note": "係数×TF-IDF/メタ特徴量の寄与。正の値はスコア上昇、負の値はスコア低下。",
                }
            )

        return details


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def parse_target_sheet(value: str) -> str:
    sheet_name = value.strip()
    if sheet_name not in {"prod", "dev"}:
        raise RuntimeError("TARGET_SHEET must be prod or dev")
    return sheet_name


def is_archive_spreadsheet(file: dict[str, str]) -> bool:
    return file.get("name", "").startswith(ARCHIVE_FILE_PREFIX)


def get_services() -> tuple[Any, Any]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    info = json.loads(required_env("GOOGLE_SERVICE_ACCOUNT_JSON"))
    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )
    drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    return drive, sheets


def list_archive_spreadsheets(drive: Any, folder_id: str) -> list[dict[str, str]]:
    query = (
        f"'{folder_id}' in parents "
        "and mimeType = 'application/vnd.google-apps.spreadsheet' "
        "and trashed = false"
    )
    files: list[dict[str, str]] = []
    page_token = None

    while True:
        response = (
            drive.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, modifiedTime)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                orderBy="name",
                pageSize=1000,
            )
            .execute()
        )
        files.extend(
            file for file in response.get("files", []) if is_archive_spreadsheet(file)
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            return files


def read_sheet_values(sheets: Any, spreadsheet_id: str, range_name: str) -> list[list[Any]]:
    response = (
        sheets.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    return response.get("values", [])


def padded_row(row: Iterable[Any]) -> list[Any]:
    values = list(row)
    return values + [""] * max(0, MIN_COLUMNS - len(values))


def cell_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


CONTROL_CHAR_RE = re.compile(r"[\u0000-\u001f\u007f-\u009f]")
WHITESPACE_RE = re.compile(r"\s+")
JAPANESE_NORMALIZATION_REPLACEMENTS = {
    "ミンアンフライイン": "ミンアウンフライン",
    "ミンアウン・フライン": "ミンアウンフライン",
    "アウン・サン・スー・チー": "アウンサンスーチー",
    "アウン・サン・スーチー": "アウンサンスーチー",
    "キヤット": "チャット",
    "Kyat": "チャット",
    "kyat": "チャット",
    "Ｋ": "K",
}


def normalize_japanese_input_text(value: Any) -> str:
    """Normalize Japanese text before TF-IDF feature extraction."""
    s = cell_text(value)
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = CONTROL_CHAR_RE.sub(" ", s)
    s = WHITESPACE_RE.sub(" ", s).strip()
    for src, dst in JAPANESE_NORMALIZATION_REPLACEMENTS.items():
        s = s.replace(src, dst)
    return s


def normalize_key_text(value: str) -> str:
    s = normalize_japanese_input_text(value).lower()
    return re.sub(r"[\s\u3000\|｜:：,，.。()（）\[\]「」『』、\"'’‘“”!?！？/\\-]+", "", s)


def sheets_serial_to_date(value: float) -> date | None:
    # Google Sheets serial date: 1899-12-30 origin.
    try:
        if value < 10_000 or value > 80_000:
            return None
        return date(1899, 12, 30) + timedelta(days=int(value))
    except Exception:
        return None


def date_key(value: Any) -> str:
    if value is None or value == "":
        return ""

    if isinstance(value, (int, float)):
        parsed = sheets_serial_to_date(float(value))
        return parsed.strftime("%Y%m%d") if parsed else ""

    s = str(value).strip()
    if not s:
        return ""

    m = re.search(r"(20\d{2})[-/\.年](\d{1,2})[-/\.月](\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return date(y, mo, d).strftime("%Y%m%d")
        except ValueError:
            return ""

    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        candidate = digits[:8]
        try:
            datetime.strptime(candidate, "%Y%m%d")
            return candidate
        except ValueError:
            return ""

    return ""


def make_article_record(
    raw_row: Iterable[Any],
    row_index: int,
    label: int | None,
    group_key: str = "",
    manual_rank: int | None = None,
) -> ArticleRecord:
    values = padded_row(raw_row)
    parsed_date_key = date_key(values[COL_DATE])
    return ArticleRecord(
        row_index=row_index,
        date_key=parsed_date_key,
        group_key=group_key or parsed_date_key or "unknown",
        media=cell_text(values[COL_MEDIA]),
        headline_a=normalize_japanese_input_text(values[COL_E]),
        headline_final=normalize_japanese_input_text(values[COL_F]),
        headline_body=normalize_japanese_input_text(values[COL_G]),
        summary=normalize_japanese_input_text(values[COL_I]),
        url=cell_text(values[COL_URL]),
        same_topic_flag=cell_text(values[COL_SAME_TOPIC_FLAG]),
        same_topic_note=normalize_japanese_input_text(values[COL_SAME_TOPIC_NOTE]),
        duplicate_key=cell_text(values[COL_Q_KEY]),
        label=label,
        manual_rank=manual_rank,
    )


def is_usable_article(record: ArticleRecord) -> bool:
    return bool(record.full_text)


def record_identity_key(record: ArticleRecord) -> str:
    """Key used to connect candidate rows to manually selected duplicate rows."""
    if record.url:
        return "url:" + record.url
    if record.duplicate_key:
        return "q:" + record.duplicate_key
    title = normalize_key_text(
        " ".join([record.headline_a, record.headline_final, record.headline_body])
    )
    if title:
        return "title:" + title[:180]
    return ""


def build_classification_training_records_from_sheet(
    values: list[list[Any]],
    source_group_prefix: str,
) -> list[ArticleRecord]:
    """Build one deduped training set from an archive sheet.

    K=a rows are interpreted as selected articles. If an archive sheet contains
    duplicate manual rows appended below the candidate list, this function maps
    their order back to the original candidate rows by dedupes repeated candidates within the same date/source group.

    The model itself remains binary classification. Manual order is stored in
    manual_rank and later used as sample_weight for positive examples.
    """
    raw_records: list[ArticleRecord] = []
    for row_index, raw_row in enumerate(values[1:], start=2):
        base_record = make_article_record(
            raw_row,
            row_index,
            None,
            group_key=source_group_prefix,
        )
        group_key = base_record.date_key or source_group_prefix
        record = replace(base_record, group_key=group_key)
        if is_usable_article(record):
            raw_records.append(record)

    manual_keys_by_group: dict[str, list[str]] = {}
    seen_manual_by_group: dict[str, set[str]] = {}
    for record in raw_records:
        values_row = padded_row(values[record.row_index - 1])
        if cell_text(values_row[COL_ADOPTED]).lower() != "a":
            continue
        key = record_identity_key(record)
        if not key:
            continue
        group_seen = seen_manual_by_group.setdefault(record.group_key, set())
        if key in group_seen:
            continue
        group_seen.add(key)
        manual_keys_by_group.setdefault(record.group_key, []).append(key)

    manual_order_by_group_key: dict[tuple[str, str], int] = {}
    for group_key, keys in manual_keys_by_group.items():
        for order_index, key in enumerate(keys, start=1):
            manual_order_by_group_key[(group_key, key)] = order_index

    deduped: list[ArticleRecord] = []
    seen_candidates_by_group: dict[str, set[str]] = {}
    for record in raw_records:
        key = record_identity_key(record)
        if not key:
            # Keep records without an identity key as unique negatives.
            key = f"row:{record.row_index}"
        group_seen = seen_candidates_by_group.setdefault(record.group_key, set())
        if key in group_seen:
            continue
        group_seen.add(key)

        manual_rank = manual_order_by_group_key.get((record.group_key, key))
        label = 1 if manual_rank is not None else 0
        deduped.append(replace(record, label=label, manual_rank=manual_rank))

    return deduped


def load_archive_records(
    drive: Any,
    sheets: Any,
    archive_folder_id: str,
) -> tuple[list[ArticleRecord], int]:
    from googleapiclient.errors import HttpError

    archive_files = list_archive_spreadsheets(drive, archive_folder_id)
    rows: list[ArticleRecord] = []

    for archive_file in archive_files:
        try:
            values = read_sheet_values(
                sheets,
                archive_file["id"],
                f"{ARCHIVE_SHEET_NAME}!A:AF",
            )
        except HttpError as exc:
            print(
                f"[selection-ml-classifier] skip {archive_file['name']} / "
                f"{ARCHIVE_SHEET_NAME}: {exc}",
                file=sys.stderr,
            )
            continue

        rows.extend(
            build_classification_training_records_from_sheet(
                values,
                source_group_prefix=archive_file.get("name", archive_file["id"]),
            )
        )

    return rows, len(archive_files)


def load_current_rows(
    sheets: Any,
    spreadsheet_id: str,
    sheet_name: str,
) -> tuple[list[ArticleRecord], int]:
    values = read_sheet_values(sheets, spreadsheet_id, f"{sheet_name}!A:AF")
    sheet_row_count = max(0, len(values) - 1)
    rows: list[ArticleRecord] = []

    for row_index, raw_row in enumerate(values[1:], start=2):
        record = make_article_record(raw_row, row_index, None, group_key="current")
        if is_usable_article(record):
            rows.append(record)

    return rows, sheet_row_count


def build_local_adopted_records_from_sheet(
    values: list[list[Any]],
    source_sheet_name: str,
) -> list[ArticleRecord]:
    """Read K=a rows from archive_prod/archive_dev in the current spreadsheet.

    These records are not used as additional ML training data. They are passed
    to Gemini as adopted_archive_context so Gemini can judge whether a current
    article is a duplicate of an already selected item or a meaningful follow-up.
    """
    rows: list[ArticleRecord] = []
    for row_index, raw_row in enumerate(values[1:], start=2):
        values_row = padded_row(raw_row)
        if cell_text(values_row[COL_ADOPTED]).lower() != "a":
            continue
        record = make_article_record(
            raw_row,
            row_index,
            label=1,
            group_key=source_sheet_name,
        )
        if is_usable_article(record):
            rows.append(
                replace(
                    record,
                    group_key=record.date_key or source_sheet_name,
                    label=1,
                )
            )
    return rows


def local_adopted_archive_sheet_name(target_sheet: str) -> str:
    """Return the local archive sheet paired with the execution target sheet."""
    try:
        return LOCAL_ADOPTED_ARCHIVE_SHEET_BY_TARGET[target_sheet]
    except KeyError as exc:
        raise RuntimeError(
            f"Unsupported TARGET_SHEET for local archive lookup: {target_sheet}"
        ) from exc


def load_local_adopted_archive_records(
    sheets: Any,
    spreadsheet_id: str,
    target_sheet: str,
) -> list[ArticleRecord]:
    """Load K=a rows from the paired archive sheet in the same spreadsheet.

    Pairing rule:
    - TARGET_SHEET=prod -> archive_prod
    - TARGET_SHEET=dev  -> archive_dev

    These rows are not used as additional ML training data. They are passed to
    Gemini as adopted_archive_context so Gemini can judge duplicate vs follow-up.
    """
    from googleapiclient.errors import HttpError

    sheet_name = local_adopted_archive_sheet_name(target_sheet)
    try:
        values = read_sheet_values(sheets, spreadsheet_id, f"{sheet_name}!A:AF")
    except HttpError as exc:
        print(
            f"[selection-ml-classifier] local_adopted_archive_skip "
            f"target_sheet={target_sheet} sheet={sheet_name} error={exc}",
            file=sys.stderr,
        )
        return []

    rows = build_local_adopted_records_from_sheet(values, sheet_name)
    print(
        f"[selection-ml-classifier] local_adopted_archive_rows={len(rows)} "
        f"target_sheet={target_sheet} sheet={sheet_name}"
    )
    return rows


# ---------------------------------------------------------------------------
# scikit-learn feature extraction: text ML only + generic metadata
# ---------------------------------------------------------------------------


def extract_headline_a(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_a for row in rows]


def extract_headline_final(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_final for row in rows]


def extract_headline_body(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_body for row in rows]


def extract_summary(rows: list[ArticleRecord]) -> list[str]:
    return [row.summary for row in rows]


def generic_feature_dicts(rows: list[ArticleRecord]) -> list[dict[str, float | str]]:
    """Generic non-topic metadata only. No media-name or hand-written topic flags."""
    features: list[dict[str, float | str]] = []
    for row in rows:
        item: dict[str, float | str] = {
            "has_url": float(bool(row.url)),
            "has_duplicate_key": float(bool(row.duplicate_key)),
            "headline_a_len_bin": min(len(row.headline_a) // 20, 10),
            "headline_final_len_bin": min(len(row.headline_final) // 20, 10),
            "headline_body_len_bin": min(len(row.headline_body) // 20, 10),
            "summary_len_bin": min(len(row.summary) // 80, 10),
        }
        features.append(item)
    return features


def build_feature_pipeline() -> Any:
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.pipeline import FeatureUnion, Pipeline
    from sklearn.preprocessing import FunctionTransformer

    char_kwargs = dict(
        analyzer="char",
        ngram_range=(2, 6),
        min_df=2,
        sublinear_tf=True,
    )

    return FeatureUnion(
        [
            (
                "headline_a",
                Pipeline(
                    [
                        ("select", FunctionTransformer(extract_headline_a, validate=False)),
                        ("tfidf", TfidfVectorizer(max_features=20_000, **char_kwargs)),
                    ]
                ),
            ),
            (
                "headline_final",
                Pipeline(
                    [
                        ("select", FunctionTransformer(extract_headline_final, validate=False)),
                        ("tfidf", TfidfVectorizer(max_features=20_000, **char_kwargs)),
                    ]
                ),
            ),
            (
                "headline_body",
                Pipeline(
                    [
                        ("select", FunctionTransformer(extract_headline_body, validate=False)),
                        ("tfidf", TfidfVectorizer(max_features=20_000, **char_kwargs)),
                    ]
                ),
            ),
            (
                "summary",
                Pipeline(
                    [
                        ("select", FunctionTransformer(extract_summary, validate=False)),
                        ("tfidf", TfidfVectorizer(max_features=40_000, **char_kwargs)),
                    ]
                ),
            ),
            (
                "generic_metadata",
                Pipeline(
                    [
                        ("select", FunctionTransformer(generic_feature_dicts, validate=False)),
                        ("dict", DictVectorizer()),
                    ]
                ),
            ),
        ],
        transformer_weights={
            "headline_a": 1.2,
            "headline_final": 1.3,
            "headline_body": 1.2,
            "summary": 1.0,
            "generic_metadata": 0.6,
        },
    )




def feature_names_from_pipeline(feature_pipeline: Any) -> list[str]:
    """Collect feature names in the same order as FeatureUnion output columns."""
    names: list[str] = []
    transformer_weights = getattr(feature_pipeline, "transformer_weights", None) or {}

    for union_name, transformer in getattr(feature_pipeline, "transformer_list", []):
        if transformer == "drop" or transformer is None:
            continue

        raw_names: list[str]
        try:
            if hasattr(transformer, "steps") and transformer.steps:
                last_step = transformer.steps[-1][1]
                if hasattr(last_step, "get_feature_names_out"):
                    raw_names = [str(v) for v in last_step.get_feature_names_out()]
                elif hasattr(last_step, "get_feature_names"):
                    raw_names = [str(v) for v in last_step.get_feature_names()]
                else:
                    raw_names = []
            elif hasattr(transformer, "get_feature_names_out"):
                raw_names = [str(v) for v in transformer.get_feature_names_out()]
            elif hasattr(transformer, "get_feature_names"):
                raw_names = [str(v) for v in transformer.get_feature_names()]
            else:
                raw_names = []
        except Exception:
            raw_names = []

        weight = float(transformer_weights.get(union_name, 1.0))
        for raw_name in raw_names:
            names.append(f"{union_name}:{raw_name}:weight={weight:g}")

    return names


def split_feature_name(feature_name: str) -> tuple[str, str, float | None]:
    if not feature_name:
        return "unknown", "unknown", None

    weight: float | None = None
    if ":weight=" in feature_name:
        feature_name, weight_text = feature_name.rsplit(":weight=", 1)
        try:
            weight = float(weight_text)
        except ValueError:
            weight = None

    if ":" not in feature_name:
        return "unknown", feature_name, weight

    source, raw = feature_name.split(":", 1)
    return source, raw, weight


def source_label(source: str) -> str:
    labels = {
        "headline_a": "E列",
        "headline_final": "F列",
        "headline_body": "G列",
        "summary": "I列",
        "generic_metadata": "メタ情報",
    }
    return labels.get(source, source or "不明")


def display_feature_name(feature_name: str) -> str:
    source, raw, _weight = split_feature_name(feature_name)
    raw = raw.replace("\n", "\\n").replace("\r", "\\r").strip()
    raw = WHITESPACE_RE.sub(" ", raw)
    if len(raw) > 45:
        raw = raw[:45] + "…"
    return f"{source_label(source)}:{raw}"


def build_feature_contribution_dict(
    column_index: int,
    contribution: float,
    feature_names: list[str],
) -> dict[str, Any]:
    feature_name = (
        feature_names[column_index]
        if 0 <= column_index < len(feature_names)
        else f"feature_{column_index}"
    )
    source, raw, branch_weight = split_feature_name(feature_name)
    return {
        "feature": display_feature_name(feature_name),
        "source": source_label(source),
        "raw_feature": raw,
        "branch_weight": branch_weight,
        "contribution": round(float(contribution), 6),
    }


def format_contribution_list(items: list[dict[str, Any]]) -> str:
    if not items:
        return "該当なし"
    return " / ".join(
        f"{item['feature']}({float(item['contribution']):+.3f})"
        for item in items
    )[:3000]


def build_detail_json(detail: dict[str, Any]) -> str:
    payload = {
        "score": detail.get("score"),
        "probability": round(float(detail.get("probability", 0.0)), 6),
        "decision_value": (
            round(float(detail["decision_value"]), 6)
            if detail.get("decision_value") is not None
            else None
        ),
        "intercept": (
            round(float(detail["intercept"]), 6)
            if detail.get("intercept") is not None
            else None
        ),
        "positive_reasons": detail.get("positive_reasons", []),
        "negative_reasons": detail.get("negative_reasons", []),
        "note": detail.get("note", ""),
    }
    # Google Sheets cell limit is 50,000 chars. Keep enough margin.
    return json.dumps(payload, ensure_ascii=False)[:45000]



def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}


def env_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def env_float(name: str, default: float, min_value: float | None = None, max_value: float | None = None) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = float(raw_value)
    except ValueError:
        return default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def truncate_text(value: str, limit: int) -> str:
    value = WHITESPACE_RE.sub(" ", cell_text(value)).strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "…"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(round(float(value)))
    except Exception:
        return default


def clamp_score(value: Any, default: int = 0) -> int:
    return max(0, min(100, safe_int(value, default)))


def should_send_all_rows_to_gemini(rows: list[ArticleRecord], max_articles: int) -> bool:
    """Default to broad Gemini review when the daily row count fits the cap.

    This avoids hard-coding current-news keywords for low-score rescue. Gemini is
    still a second-stage reviewer because the final score uses the higher score
    between ML and Gemini.
    """
    if not env_bool("GEMINI_RERANK_ALL_CURRENT_ROWS_WHEN_POSSIBLE", True):
        return False
    return len(rows) <= max_articles


def select_gemini_rerank_candidates(
    rows: list[ArticleRecord],
    prediction_details: list[dict[str, Any]],
) -> list[tuple[ArticleRecord, dict[str, Any]]]:
    min_ml_score = env_int("GEMINI_RERANK_MIN_ML_SCORE", GEMINI_DEFAULT_MIN_ML_SCORE, 0, 100)
    max_articles = env_int("GEMINI_RERANK_MAX_ARTICLES", GEMINI_DEFAULT_MAX_ARTICLES, 1, 500)

    if should_send_all_rows_to_gemini(rows, max_articles):
        return list(zip(rows, prediction_details, strict=True))[:max_articles]

    candidates: list[tuple[ArticleRecord, dict[str, Any], int]] = []
    for row, detail in zip(rows, prediction_details, strict=True):
        ml_score = clamp_score(detail.get("score"), 0)
        same_topic_signal = bool(cell_text(row.same_topic_flag) or cell_text(row.same_topic_note))
        duplicate_signal = bool(cell_text(row.duplicate_key))

        if ml_score < min_ml_score and not same_topic_signal and not duplicate_signal:
            continue

        # Generic priority only. Do not hard-code topic keywords here.
        if ml_score >= 80:
            priority = 5
        elif same_topic_signal:
            priority = 4
        elif duplicate_signal:
            priority = 3
        elif ml_score >= 40:
            priority = 2
        else:
            priority = 1
        candidates.append((row, detail, priority))

    candidates.sort(
        key=lambda item: (
            -item[2],
            -clamp_score(item[1].get("score"), 0),
            item[0].row_index,
        )
    )
    return [(row, detail) for row, detail, _priority in candidates[:max_articles]]

def compact_ml_reasons(detail: dict[str, Any], key: str) -> list[str]:
    values: list[str] = []
    for item in detail.get(key, [])[:5]:
        feature = cell_text(item.get("feature"))
        contribution = item.get("contribution")
        if feature:
            values.append(f"{feature}({contribution:+.3f})" if isinstance(contribution, (int, float)) else feature)
    return values


def article_short_title(row: ArticleRecord) -> str:
    return row.headline_final or row.headline_a or row.headline_body or row.summary[:80]


def archive_candidate_text(row: ArticleRecord) -> str:
    return "\n".join(
        [row.headline_a, row.headline_final, row.headline_body, row.summary, row.duplicate_key]
    )


def similarity_tokens(value: str) -> set[str]:
    s = normalize_key_text(value)
    if not s:
        return set()
    # Character n-grams work reasonably for Japanese without maintaining a keyword list.
    tokens: set[str] = set()
    for n in (3, 4, 5):
        for i in range(0, max(0, len(s) - n + 1)):
            token = s[i : i + n]
            if token:
                tokens.add(token)
    return tokens


def generic_similarity_score(a: str, b: str) -> float:
    tokens_a = similarity_tokens(a)
    tokens_b = similarity_tokens(b)
    if not tokens_a or not tokens_b:
        return 0.0
    overlap = len(tokens_a & tokens_b)
    denom = max(1, min(len(tokens_a), len(tokens_b)))
    return overlap / denom


def build_archive_adopted_context_map(
    current_rows: list[ArticleRecord],
    archive_rows: list[ArticleRecord],
) -> dict[int, list[dict[str, Any]]]:
    """Find already-adopted archive articles that may be duplicates/continuations.

    The matching is intentionally generic: URL, Q duplicate key, normalized title,
    and text similarity. It does not copy selection.js's topic keyword rules.
    """
    adopted = [row for row in archive_rows if int(row.label or 0) == 1]
    out: dict[int, list[dict[str, Any]]] = {}
    if not adopted:
        return out

    for current in current_rows:
        current_title_key = normalize_key_text(" ".join([current.headline_a, current.headline_final, current.headline_body]))
        current_text = archive_candidate_text(current)
        scored: list[tuple[float, ArticleRecord, str]] = []
        for past in adopted:
            reasons: list[str] = []
            score = 0.0
            if current.url and past.url and current.url == past.url:
                score += 100.0
                reasons.append("url_exact")
            if current.duplicate_key and past.duplicate_key and current.duplicate_key == past.duplicate_key:
                score += 80.0
                reasons.append("duplicate_key_exact")

            past_title_key = normalize_key_text(" ".join([past.headline_a, past.headline_final, past.headline_body]))
            if current_title_key and past_title_key and current_title_key == past_title_key:
                score += 60.0
                reasons.append("title_exact")

            sim = generic_similarity_score(current_text, archive_candidate_text(past))
            if sim >= 0.18:
                score += sim * 40.0
                reasons.append(f"text_similarity={sim:.2f}")

            if score > 0:
                scored.append((score, past, ",".join(reasons)))

        scored.sort(key=lambda item: (-item[0], item[1].row_index))
        items: list[dict[str, Any]] = []
        total_chars = 0
        for score, past, reason in scored[:ARCHIVE_ADOPTED_CONTEXT_MAX_ITEMS]:
            item = {
                "archive_row_index": past.row_index,
                "date_key": past.date_key,
                "media": past.media,
                "headline": truncate_text(article_short_title(past), 180),
                "summary": truncate_text(past.summary, 320),
                "duplicate_key_q": truncate_text(past.duplicate_key, 160),
                "match_reason": reason,
                "match_score": round(score, 2),
                "url": truncate_text(past.url, 220),
            }
            serialized = json.dumps(item, ensure_ascii=False)
            if items and total_chars + len(serialized) > ARCHIVE_ADOPTED_CONTEXT_MAX_CHARS:
                break
            items.append(item)
            total_chars += len(serialized)
        if items:
            out[current.row_index] = items
    return out


def build_same_day_context_map(
    current_rows: list[ArticleRecord],
) -> dict[int, list[dict[str, Any]]]:
    """Find same-day current-sheet articles that may describe the same event.

    Gemini receives these candidates and decides whether the 4W elements
    (when / who / where / what) are actually the same. Python only performs a
    broad, generic pre-filter using Q key, title equality, URL, and text
    similarity to keep the prompt compact.
    """
    out: dict[int, list[dict[str, Any]]] = {}
    if len(current_rows) < 2:
        return out

    for current in current_rows:
        current_date_key = current.date_key or "current"
        current_title_key = normalize_key_text(
            " ".join([current.headline_a, current.headline_final, current.headline_body])
        )
        current_text = archive_candidate_text(current)
        scored: list[tuple[float, ArticleRecord, str]] = []

        for other in current_rows:
            if other.row_index == current.row_index:
                continue
            other_date_key = other.date_key or "current"
            if current_date_key != other_date_key:
                continue

            reasons: list[str] = []
            score = 0.0
            if current.url and other.url and current.url == other.url:
                score += 100.0
                reasons.append("url_exact")
            if current.duplicate_key and other.duplicate_key and current.duplicate_key == other.duplicate_key:
                score += 80.0
                reasons.append("duplicate_key_exact")

            other_title_key = normalize_key_text(
                " ".join([other.headline_a, other.headline_final, other.headline_body])
            )
            if current_title_key and other_title_key and current_title_key == other_title_key:
                score += 60.0
                reasons.append("title_exact")

            sim = generic_similarity_score(current_text, archive_candidate_text(other))
            if sim >= 0.18:
                score += sim * 40.0
                reasons.append(f"text_similarity={sim:.2f}")

            if score > 0:
                scored.append((score, other, ",".join(reasons)))

        scored.sort(key=lambda item: (-item[0], item[1].row_index))
        items: list[dict[str, Any]] = []
        total_chars = 0
        for score, other, reason in scored[:SAME_DAY_CONTEXT_MAX_ITEMS]:
            item = {
                "row_index": other.row_index,
                "date_key": other.date_key,
                "media": other.media,
                "headline": truncate_text(article_short_title(other), 180),
                "summary": truncate_text(other.summary, 320),
                "duplicate_key_q": truncate_text(other.duplicate_key, 160),
                "match_reason": reason,
                "match_score": round(score, 2),
                "url": truncate_text(other.url, 220),
            }
            serialized = json.dumps(item, ensure_ascii=False)
            if items and total_chars + len(serialized) > SAME_DAY_CONTEXT_MAX_CHARS:
                break
            items.append(item)
            total_chars += len(serialized)
        if items:
            out[current.row_index] = items
    return out


def build_gemini_article_payload(
    row: ArticleRecord,
    detail: dict[str, Any],
    archive_context: list[dict[str, Any]] | None = None,
    same_day_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "row_index": row.row_index,
        "ml_score": clamp_score(detail.get("score"), 0),
        "media_reference_only": row.media,
        "date_key": row.date_key,
        "headline_e": truncate_text(row.headline_a, 140),
        "headline_f": truncate_text(row.headline_final, 140),
        "headline_g": truncate_text(row.headline_body, 140),
        "summary": truncate_text(row.summary, 850),
        "same_topic_flag_o": row.same_topic_flag,
        "same_topic_note_p": truncate_text(row.same_topic_note, 260),
        "duplicate_key_q": truncate_text(row.duplicate_key, 180),
        "adopted_archive_context": archive_context or [],
        "same_day_context": same_day_context or [],
        "url": truncate_text(row.url, 240),
        "ml_positive_reasons": compact_ml_reasons(detail, "positive_reasons"),
        "ml_negative_reasons": compact_ml_reasons(detail, "negative_reasons"),
    }


def gemini_system_prompt() -> str:
    return """
あなたはミャンマー関連ニュースの日本語ダイジェスト編集者です。
各記事について、MLでは拾いにくい意味判断を行い、0〜100のGeminiスコアを返してください。

重要な前提:
- 媒体名は参考情報に過ぎません。媒体名に Myanmar / ミャンマー が含まれていても、それだけで直接関連・高評価にしません。
- 判定に使う本文情報は、原則として E/F/G/I列相当の headline_e / headline_f / headline_g / summary です。URLや媒体名だけを根拠にしません。
- O/P列の過去2日同一トピック情報は主判定に使いません。重複・続編・別観点の判定は adopted_archive_context と Q列 duplicate_key_q を優先します。

最重要の採点方針:
1. ミャンマー直接関連・無関係記事の定義
- E/F/G/I列相当の入力から、ミャンマーへの直接関係が確認できる記事を高く評価します。
- ミャンマーに関係ない記事と判定するのは、E/F/G/I列相当の入力に、ミャンマー関連キーワードが一切含まれない場合に限定します。
- ミャンマー関連キーワードには、ミャンマー、ビルマ、ヤンゴン、ネピドー、マンダレー、ミンアウンフライン、国軍、CBM、チャットなど、ミャンマー固有の地名・人物・機関・制度・通貨・経済行政用語を含めます。
- 特に「ヤンゴン」を含む記事は、ミャンマー国内地名のヒットとして扱い、ミャンマー関連の選定候補として残します。ヤンゴンのヒットはトピック内容よりも優先し、無関係記事・低優先記事として扱わないでください。
- 上記のようなミャンマー関連キーワードが一切なく、直接関係も確認できない海外一般ニュースは、話題性・国名・政府機関・法改正・選挙・芸能・スポーツなどがあっても低くします。
- ミャンマー関連キーワードが一切なく、直接関係も確認できない場合のみ、0〜20を目安にします。

2. 国名・外交の扱い
- 国の重要度は、トピックの重要度よりも優先します。既に選定基準として含まれている国名が E/F/G/I列相当の入力に含まれている場合は、その国名ヒット自体を選定候補化の根拠として扱い、トピック単体の軽重だけで低くしないでください。
- 中国・米国・日本・ロシアは最上位国として扱います。中国・ラオス・タイ・バングラデシュ・インド・ブルネイ・カンボジア・インドネシア・マレーシア・フィリピン・シンガポール・東ティモール・ベトナムは次点、韓国は補助的に扱います。
- 国名は、政府・当局・企業・団体・国民・国籍・人物・商品・制度・場所など、その国名自体が含まれていればヒットと判定して構いません。例: 中国政府、中国企業、中国人、中国籍、中国製、タイ国境、日本人、米国企業。
- 貿易、投資、安全保障、制裁、援助、国境、物流、企業活動、労働、制度変更など、ミャンマーへの具体的影響がある場合は高く評価します。
- 外交会談、訪問、表敬、声明、式典、挨拶など外交儀礼的な側面が強い記事でも、それだけを理由にスコアを下げる必要はありません。重要国との関係や今後の影響が読み取れる場合は、選定候補として評価します。
- ただし、ミャンマー関連キーワードや直接関係が確認できない国際一般ニュースは、重要国が登場しても高評価にしません。

3. 紛争・空爆・戦闘・攻撃
- 空爆、戦闘、攻撃に関する記事は、住民・市民・子ども・避難民など民間人の死亡者が1人以上存在する場合に選定対象候補とします。
- 住民の死亡者が存在せず、避難、けが人、住居破壊、施設被害などにとどまる場合は、原則として選定対象から外します。
- 国軍兵士、治安部隊、抵抗勢力、民族武装勢力の死亡者のみが存在し、住民の死亡者が存在しない場合も、原則として選定対象から外します。
- 住民死亡者が確認できる場合は、死亡者数、地域、攻撃主体、被害の広がり、行政・軍事・国境・物流上の意味を踏まえて評価します。
- 補給路、主要都市、国境、港湾、空港、経済回廊、支配地域、選挙、停戦、行政支配など、住民死亡に加えて戦略的意味がある場合はより高く評価します。
- 住民死亡の有無が不明な場合は、過大評価せず、risk_flagsに「住民死亡の有無不明」と明記してください。

4. 国内地域
- ヤンゴン、エーヤワディー、バゴーなどの地域名は、ミャンマー関連性の判断と、同じ事象の代表記事選びの補助として使います。
- 「ヤンゴン」を含む記事は、ミャンマー国内地名のヒットとして選定候補に残します。ヤンゴンのヒットはトピック内容よりも優先し、低優先・無関係扱いにはしないでください。

5. 優先トピック
以下のトピックを扱う場合は高評価します。ただし、既に選定基準として含まれている国名、またはミャンマー国内の「ヤンゴン」がヒットしている記事は、トピック重要度よりも国名・地名ヒットを優先して判断します。
- 公的機関による政策・制度・規制・法改正・許認可・行政手続き・税制・関税・輸出入・出入国・労働・企業活動に関わる発表、通達、告示、承認、決定、開始、停止、廃止。
- 物価、燃料価格、為替、外貨規制、価格統制、外貨使用制限。
- 中央銀行・CBMによる外貨売却、外貨配分、外貨供給、輸入決済、食用油・燃料・医薬品・生活必需品輸入向けの外貨配分。
- 税・関税・貿易規制、投資認可、事業許可、企業登録、輸出入実務、通関、国境物流、港湾・コンテナ船・海上物流。
- 行政システム、オンライン申請、手続き変更、出入国、旅券、ビザ、海外就労者関連。
- 雇用、労働政策、労働組合、賃金、労使関係。
- 政府・省庁・当局・公的機関が関わる開発計画、インフラ、電力需要・電力供給計画。
- ビジネス環境、中小企業、企業支援、融資、商工業者、企業活動に影響する制度や政策。
- ミンアウンフライン、国軍総司令官、ミャンマー政府・省庁などによる、読者に影響する政策・制度・税制・燃料・物価・環境・防災・経済・労働・電力・インフラ・貿易などの提案、発表、声明、指示、承認、決定。
- 法案提出、議会提出、上程、審議入り、可決、成立、法改正、罰則強化。
- 通信規制、監視、インターネット制限、情報統制。
- 食品・医薬品・品質基準、衛生基準、認証、検査、流通規制。
- 制裁、人権、国際機関・人権団体の声明、報告、要請、勧告。人権団体の声明ベースであり、実務的な制裁決定ではない場合でも、それだけを理由にスコアを下げる必要はありません。

除外・抑制:
- ミャンマー関連キーワードが一切なく、直接関係も確認できない海外一般ニュースは低くします。
- チャット/ドル等の金額表示だけで、物価・為替・外貨トピックにしません。
- 開発・インフラ・電力・港湾などの単語だけでは高評価にせず、公的発表・実務影響・数量的更新があるかを見ます。
- 外交儀礼的な会談・表敬訪問・式典・声明であること、人権団体の声明ベースであることは、それ単体では減点理由にしません。

同日内の同一事象候補の扱い:
- same_day_context は、同じスプレッドシート内の同日記事から、見出し/要約類似度で抽出された候補です。
- same_day_context に候補がある場合は、現在記事と候補記事の「いつ・誰が・どこで・何を」を必ず比較してください。
- 「いつ・誰が・どこで・何を」がほぼ同じなら same_day_event_relation を same_event_duplicate にし、同じ same_day_event_key を付けてください。Python側で同一キーの記事のGeminiスコアを、グループ内の最高Geminiスコアへ揃えます。
- 同じ大テーマでも、日付、主体、場所、行為、結果、数字、制度、被害、反応、実務影響のいずれかが明確に異なる場合は、same_event_duplicate ではなく continuation_update / different_angle / related_but_different を選んでください。
- 国名・地域名・一般トピックが似ているだけでは同一事象にしないでください。4W一致を必須条件にしてください。

archive採用済み記事候補の扱い:
- adopted_archive_context は、同じスプレッドシート内で実行対象シートに対応するarchiveシート（prod実行時はarchive_prod、dev実行時はarchive_dev）のK列=a行、および月次アーカイブ内のK列=a行から抽出された過去採用済み記事候補です。
- 現在記事と過去採用済み記事の「いつ・誰が・どこで・何を」がほぼ同じで、新しい進展・数字・反応・被害情報・制度変更・観点がなければ、過去採用済み記事との重複として低くします。
- 同じトピックでも、数字更新、被害拡大、新制度、関係者反応、実務影響の拡大など明確な続報なら高く評価します。
- 同じ大きなテーマでも、焦点・当事者・地域・政策面・経済面・市民被害面など観点が異なる場合は、単純重複ではなく別観点として評価します。

Geminiスコアの目安:
- 85〜100: 手動採用上位に入り得る非常に強い候補。
- 70〜84: 採用候補として強い。
- 55〜69: 候補にはなるが、他記事との比較が必要。
- 35〜54: 参考・バックアップ程度。
- 0〜34: 原則低優先。ミャンマー関連キーワードがない、住民死亡のない空爆・戦闘・攻撃、既出に近い等。ただし、既に選定基準として含まれている国名、またはヤンゴンがヒットしている新規記事は、トピック内容だけを理由に0〜34へ落とさないでください。

必ずJSONのみを返してください。説明文やMarkdownは不要です。
""".strip()

def build_gemini_rerank_prompt(payloads: list[dict[str, Any]]) -> str:
    return json.dumps(
        {
            "task": "semantic_gemini_score_for_hybrid_article_selection",
            "output_schema": {
                "articles": [
                    {
                        "row_index": "int: input row_index",
                        "gemini_score": "int 0..100: editorial score judged by Gemini",
                        "decision": "強い採用候補 | 採用候補 | 比較候補 | 低優先",
                        "rank_group": "A | B | C | D",
                        "direct_myanmar_relevance": "高 | 中 | 低",
                        "country_weight_tier": "none | top_china_us_japan_russia | neighbor_country | korea_country",
                        "domestic_region_tier": "none | yangon | ayeyarwady | bago | other_myanmar_region",
                        "conflict_damage_target": "none | civilian_death | civilian_injury_or_damage_only | military_or_resistance_death_only | mixed | unclear",
                        "event_when": "string: extracted when/date/time of the event, empty if unclear",
                        "event_who": "string: main actor/person/organization affected or acting, empty if unclear",
                        "event_where": "string: main place/location, empty if unclear",
                        "event_what": "string: main action/decision/incident, empty if unclear",
                        "same_day_event_relation": "no_same_day_context | same_event_duplicate | continuation_update | different_angle | related_but_different | unrelated | unknown",
                        "same_day_event_key": "string: stable key only when same_day_event_relation is same_event_duplicate; represent when|who|where|what",
                        "same_day_diff_ja": "string: difference from same-day context article, empty if same event or no context",
                        "priority_topic_tags": [
                            "official_policy_regulation_announcement",
                            "prices_fuel_forex",
                            "central_bank_forex_sale_allocation",
                            "tax_tariff_trade_regulation",
                            "investment_business_permit",
                            "import_export_border_logistics",
                            "port_container_shipping_logistics",
                            "administrative_system_procedure",
                            "migration_passport_visa",
                            "labor_policy_relations",
                            "development_infrastructure",
                            "power_demand_project_plan",
                            "business_sme",
                            "government_personnel_statement",
                            "myanmar_leadership_policy_statement",
                            "law_revision",
                            "telecom_surveillance_information_control",
                            "food_medicine_quality_standard",
                            "human_rights_sanctions_statement",
                        ],
                        "adopted_archive_relation": "no_archive_adopted | duplicate_same_content | continuation_update | different_angle | related_but_different | unrelated | unknown",
                        "adopted_archive_diff_ja": "string: difference from adopted archive article, empty if none",
                        "novelty_vs_archive_or_same_topic": "新規 | 重要続報 | 別観点 | 既出に近い | 不明",
                        "topic_key": "short stable topic key based on the event, not media or URL",
                        "editorial_axes": [
                            "direct_myanmar_relevance",
                            "js_priority_topic",
                            "public_policy_or_economic_impact",
                            "conflict_humanitarian_or_strategic_impact",
                            "international_relation_with_practical_impact",
                            "archive_duplicate_or_continuation_check",
                            "same_day_same_event_check",
                        ],
                        "reasons": ["string: 1-4 short Japanese reasons"],
                        "risk_flags": ["string: 0-4 short Japanese caution flags"],
                    }
                ]
            },
            "selection_criteria_from_js": {
                "use_input_columns": "Evaluate headline_e, headline_f, headline_g, and summary. Do not rely on media name or URL alone.",
                "direct_myanmar_relevance": "Classify an article as unrelated to Myanmar only when headline_e/headline_f/headline_g/summary contain no Myanmar-related keywords such as Myanmar, Burma, Yangon, Naypyidaw, Mandalay, Min Aung Hlaing, Tatmadaw, CBM, kyat, or other Myanmar-specific names/institutions/terms.",
                "country_weight": "Country hits have priority over topic importance. China/US/Japan/Russia are top-tier countries; neighboring or ASEAN-related countries are second tier; South Korea is supplementary. If any listed country name appears in headline_e/headline_f/headline_g/summary in any form or context, treat it as a country hit and keep the article as a selection candidate when Myanmar relevance is present or a Myanmar domestic location such as Yangon appears.",
                "diplomacy_treatment": "Do not down-rank an article merely because it has a diplomatic courtesy, visit, meeting, ceremony, or statement aspect. If an important country is involved and Myanmar relevance is present, keep it as a selection candidate even when practical impact is not fully decided yet.",
                "conflict": "For airstrikes, fighting, and attacks, keep as a selection candidate only when at least one resident/civilian death is present. Exclude cases with evacuation, injuries, house damage, or military/resistance deaths only when no resident/civilian death is present.",
                "domestic_region": "Use domestic regions for Myanmar relevance and representative/tie-break signals. Articles containing Yangon must remain selection candidates. A Yangon hit has priority over topic importance and must not be down-ranked to low priority merely because the topic is a general incident or not a policy/economic topic.",
                "priority_topics": [
                    "official policy/regulation/law/procedure announcement by Myanmar public bodies",
                    "prices, fuel, forex, foreign-currency controls",
                    "central bank foreign-currency sale/allocation for imports and essential goods",
                    "tax, tariff, trade regulation, investment/business permits",
                    "import/export, border logistics, port/container/shipping logistics",
                    "administrative systems, migration/passport/visa, overseas workers",
                    "labor policy and labor relations",
                    "government-backed development/infrastructure and power supply plans",
                    "business/SME support with practical impact",
                    "Myanmar leadership or ministries making policy statements with public/economic impact",
                    "law revision or bill submission",
                    "telecom, surveillance, information control",
                    "food, medicine, quality and hygiene standards",
                    "sanctions, human rights statements, reports, requests, or recommendations by international organizations or human rights groups; do not down-rank only because the item is statement-based rather than a finalized sanctions decision",
                ],
                "same_day": "Use same_day_context to compare when/who/where/what. If all 4W elements are substantially the same, return same_day_event_relation=same_event_duplicate and the same same_day_event_key for the duplicate rows. These duplicate rows will later share the highest Gemini score in that same-day event group.",
                "archive": "Use adopted_archive_context from the paired local archive sheet K=a rows (TARGET_SHEET=prod uses archive_prod; TARGET_SHEET=dev uses archive_dev) and monthly archive K=a rows to classify duplicate_same_content, continuation_update, different_angle, related_but_different, unrelated, or unknown.",
                "avoid_overfitting": "Do not copy exact JS keyword dictionaries or fixed score formulas. Apply the criteria semantically.",
            },
            "scoring_guide": {
                "85_to_100": "手動採用上位に入り得る非常に強い候補",
                "70_to_84": "採用候補として強い",
                "55_to_69": "候補にはなるが他記事との比較が必要",
                "35_to_54": "参考・バックアップ程度",
                "0_to_34": "原則低優先。直接関連薄い、既出、実務影響薄い等",
                "non_myanmar_cap": "ミャンマー関連キーワードが一切なく、直接関係も確認できない場合のみ原則0〜20",
                "diplomacy_country_treatment": "外交儀礼的な会談・表敬・声明であること自体は減点理由にしない。重要国かつミャンマー関連なら選定候補として残す",
            },
            "important_instruction": "MLスコアに引きずられすぎず、JSの指定5基準を抽象化した意味判断としてGeminiスコアを付けてください。同日内の4W一致判定と過去採用済み記事との差分判定は必ず行ってください。ただし最終スコアはPython側でMLスコアとGeminiスコアの高い方を採用します。Geminiだけで採否を決めないでください。",
            "articles": payloads,
        },
        ensure_ascii=False,
    )


GEMINI_RERANK_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "articles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "row_index": {"type": "integer"},
                    "gemini_score": {"type": "integer"},
                    "decision": {"type": "string"},
                    "rank_group": {"type": "string"},
                    "direct_myanmar_relevance": {"type": "string"},
                    "country_weight_tier": {"type": "string"},
                    "domestic_region_tier": {"type": "string"},
                    "conflict_damage_target": {"type": "string"},
                    "event_when": {"type": "string"},
                    "event_who": {"type": "string"},
                    "event_where": {"type": "string"},
                    "event_what": {"type": "string"},
                    "same_day_event_relation": {"type": "string"},
                    "same_day_event_key": {"type": "string"},
                    "same_day_diff_ja": {"type": "string"},
                    "same_day_score_adjustment_ja": {"type": "string"},
                    "same_day_score_source_rows": {"type": "array", "items": {"type": "integer"}},
                    "priority_topic_tags": {"type": "array", "items": {"type": "string"}},
                    "adopted_archive_relation": {"type": "string"},
                    "adopted_archive_diff_ja": {"type": "string"},
                    "novelty_vs_archive_or_same_topic": {"type": "string"},
                    "topic_key": {"type": "string"},
                    "editorial_axes": {"type": "array", "items": {"type": "string"}},
                    "reasons": {"type": "array", "items": {"type": "string"}},
                    "risk_flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "row_index",
                    "gemini_score",
                    "decision",
                    "rank_group",
                    "direct_myanmar_relevance",
                    "same_day_event_relation",
                    "adopted_archive_relation",
                    "reasons",
                    "risk_flags",
                ],
            },
        }
    },
    "required": ["articles"],
}

def parse_gemini_json_response(text_value: str) -> dict[str, Any]:
    text_value = cell_text(text_value)
    if text_value.startswith("```"):
        text_value = re.sub(r"^```(?:json)?\s*", "", text_value)
        text_value = re.sub(r"\s*```$", "", text_value)
    parsed = json.loads(text_value)
    if isinstance(parsed, list):
        return {"articles": parsed}
    if isinstance(parsed, dict):
        return parsed
    return {"articles": []}



def gemini_exception_text(exc: Exception) -> str:
    """Return a compact text blob for Gemini transient-error detection."""
    parts: list[str] = [str(exc)]

    for attr_name in ("code", "status_code", "status", "message"):
        value = getattr(exc, attr_name, None)
        if value not in (None, ""):
            parts.append(str(value))

    for response_attr in ("response", "http_response"):
        response = getattr(exc, response_attr, None)
        if response is None:
            continue
        for attr_name in ("status_code", "status", "text", "content"):
            value = getattr(response, attr_name, None)
            if callable(value):
                try:
                    value = value()
                except Exception:
                    value = None
            if value not in (None, ""):
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="replace")
                parts.append(str(value))

    return "\n".join(parts)


def gemini_error_code(exc: Exception) -> int | None:
    """Extract HTTP-like status code from google-genai exceptions when possible."""
    for attr_name in ("code", "status_code"):
        value = getattr(exc, attr_name, None)
        try:
            if value is not None:
                return int(value)
        except Exception:
            pass

    for response_attr in ("response", "http_response"):
        response = getattr(exc, response_attr, None)
        if response is None:
            continue
        for attr_name in ("status_code", "status"):
            value = getattr(response, attr_name, None)
            try:
                if value is not None:
                    return int(value)
            except Exception:
                pass

    text_value = gemini_exception_text(exc)
    for pattern in (
        r'"code"\s*:\s*(\d{3})',
        r"\bHTTP\s+(\d{3})\b",
        r"\bstatus_code\s*[=:]\s*(\d{3})\b",
    ):
        match = re.search(pattern, text_value, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))

    return None


def should_fallback_to_gemini_model(exc: Exception) -> bool:
    """Mirror selection.js: fallback only for 429/503/high-demand style errors."""
    code = gemini_error_code(exc)
    if code in {429, 503}:
        return True

    text_value = gemini_exception_text(exc).lower()
    fallback_signals = [
        '"code": 429',
        '"code": 503',
        "high demand",
        "currently experiencing high demand",
        "rate limit",
        "resource_exhausted",
        "unavailable",
    ]
    return any(signal in text_value for signal in fallback_signals)


def generate_gemini_content_once(
    client: Any,
    types: Any,
    model_name: str,
    contents: list[str],
) -> Any:
    """Call one Gemini model once, keeping schema support as a best-effort option."""
    try:
        return client.models.generate_content(
            model=model_name,
            contents=contents,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
                response_schema=GEMINI_RERANK_RESPONSE_SCHEMA,
            ),
        )
    except TypeError:
        # Older google-genai versions may not accept response_schema.
        return client.models.generate_content(
            model=model_name,
            contents=contents,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )


def generate_gemini_content_with_model_fallback(
    client: Any,
    types: Any,
    primary_model_name: str,
    fallback_model_name: str,
    contents: list[str],
    fallback_wait_seconds: float,
) -> tuple[Any, str]:
    """Try the primary Gemini model, then fallback model for transient capacity errors."""
    try:
        return generate_gemini_content_once(client, types, primary_model_name, contents), primary_model_name
    except Exception as primary_exc:
        should_fallback = should_fallback_to_gemini_model(primary_exc)
        print(
            f"[selection-ml-classifier] gemini_primary_failed "
            f"model={primary_model_name} code={gemini_error_code(primary_exc)} "
            f"fallback={should_fallback} error={str(primary_exc)[:300]}",
            file=sys.stderr,
        )
        if not should_fallback or fallback_model_name == primary_model_name:
            raise

        if fallback_wait_seconds > 0:
            time.sleep(fallback_wait_seconds)

        try:
            response = generate_gemini_content_once(client, types, fallback_model_name, contents)
            print(
                f"[selection-ml-classifier] gemini_fallback_succeeded "
                f"model={fallback_model_name}",
                file=sys.stderr,
            )
            return response, fallback_model_name
        except Exception as fallback_exc:
            raise RuntimeError(
                "Gemini primary and fallback models failed: "
                f"primary_model={primary_model_name} primary_error={str(primary_exc)[:300]} / "
                f"fallback_model={fallback_model_name} fallback_error={str(fallback_exc)[:300]}"
            ) from fallback_exc

def normalize_gemini_result(item: dict[str, Any]) -> dict[str, Any]:
    reasons = item.get("reasons", [])
    risk_flags = item.get("risk_flags", [])
    axes = item.get("editorial_axes", [])
    priority_tags = item.get("priority_topic_tags", [])
    same_day_source_rows = item.get("same_day_score_source_rows", [])

    if not isinstance(reasons, list):
        reasons = [cell_text(reasons)] if reasons else []
    if not isinstance(risk_flags, list):
        risk_flags = [cell_text(risk_flags)] if risk_flags else []
    if not isinstance(axes, list):
        axes = [cell_text(axes)] if axes else []
    if not isinstance(priority_tags, list):
        priority_tags = [cell_text(priority_tags)] if priority_tags else []
    if not isinstance(same_day_source_rows, list):
        same_day_source_rows = []

    normalized_same_day_source_rows: list[int] = []
    for value in same_day_source_rows:
        row_index = safe_int(value, -1)
        if row_index > 0:
            normalized_same_day_source_rows.append(row_index)

    return {
        "gemini_score": clamp_score(item.get("gemini_score"), 0),
        "decision": truncate_text(cell_text(item.get("decision")), 40),
        "rank_group": truncate_text(cell_text(item.get("rank_group")), 10),
        "direct_myanmar_relevance": truncate_text(cell_text(item.get("direct_myanmar_relevance")), 20),
        "country_weight_tier": truncate_text(cell_text(item.get("country_weight_tier")), 40),
        "domestic_region_tier": truncate_text(cell_text(item.get("domestic_region_tier")), 40),
        "conflict_damage_target": truncate_text(cell_text(item.get("conflict_damage_target")), 40),
        "event_when": truncate_text(cell_text(item.get("event_when")), 80),
        "event_who": truncate_text(cell_text(item.get("event_who")), 120),
        "event_where": truncate_text(cell_text(item.get("event_where")), 120),
        "event_what": truncate_text(cell_text(item.get("event_what")), 160),
        "same_day_event_relation": truncate_text(cell_text(item.get("same_day_event_relation")), 40),
        "same_day_event_key": truncate_text(cell_text(item.get("same_day_event_key")), 180),
        "same_day_diff_ja": truncate_text(cell_text(item.get("same_day_diff_ja")), 220),
        "same_day_score_adjustment_ja": truncate_text(cell_text(item.get("same_day_score_adjustment_ja")), 220),
        "same_day_score_source_rows": normalized_same_day_source_rows[:12],
        "priority_topic_tags": [truncate_text(str(tag), 80) for tag in priority_tags if cell_text(tag)][:8],
        "adopted_archive_relation": truncate_text(cell_text(item.get("adopted_archive_relation")), 40),
        "adopted_archive_diff_ja": truncate_text(cell_text(item.get("adopted_archive_diff_ja")), 220),
        "novelty_vs_archive_or_same_topic": truncate_text(cell_text(item.get("novelty_vs_archive_or_same_topic")), 40),
        "topic_key": truncate_text(cell_text(item.get("topic_key")), 120),
        "editorial_axes": [truncate_text(str(axis), 80) for axis in axes if cell_text(axis)][:6],
        "reasons": [truncate_text(str(reason), 90) for reason in reasons if cell_text(reason)][:4],
        "risk_flags": [truncate_text(str(flag), 90) for flag in risk_flags if cell_text(flag)][:4],
    }

def same_day_score_share_group_key(
    row: ArticleRecord,
    result: dict[str, Any],
) -> str:
    relation = cell_text(result.get("same_day_event_relation"))
    if relation != "same_event_duplicate":
        return ""

    raw_key = cell_text(result.get("same_day_event_key"))
    if not raw_key:
        raw_key = "|".join(
            [
                cell_text(result.get("event_when")),
                cell_text(result.get("event_who")),
                cell_text(result.get("event_where")),
                cell_text(result.get("event_what")),
            ]
        )

    normalized_key = normalize_key_text(raw_key)
    if len(normalized_key) < 8:
        return ""
    return f"{row.date_key or 'current'}:{normalized_key}"


def apply_same_day_score_sharing(
    rows: list[ArticleRecord],
    results: dict[int, dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Set same-day duplicate-event Gemini scores to the group maximum.

    Gemini decides the semantic 4W match and returns same_day_event_relation / key.
    This deterministic post-process then enforces the user's rule: if multiple
    same-day articles describe the same when/who/where/what event, their Gemini
    score becomes the highest score in that same-event group.
    """
    if not results:
        return results

    row_by_index = {row.row_index: row for row in rows}
    groups: dict[str, list[int]] = {}
    for row_index, result in results.items():
        row = row_by_index.get(row_index)
        if row is None:
            continue
        group_key = same_day_score_share_group_key(row, result)
        if not group_key:
            continue
        groups.setdefault(group_key, []).append(row_index)

    adjusted_count = 0
    for group_rows in groups.values():
        unique_rows = sorted(set(group_rows))
        if len(unique_rows) < 2:
            continue
        max_score = max(clamp_score(results[row_index].get("gemini_score"), 0) for row_index in unique_rows)
        for row_index in unique_rows:
            result = results[row_index]
            original_score = clamp_score(result.get("gemini_score"), 0)
            result["same_day_score_source_rows"] = unique_rows[:12]
            if original_score < max_score:
                result["gemini_score"] = max_score
                result["same_day_score_adjustment_ja"] = (
                    f"同日同一事象グループ{len(unique_rows)}件の最高Geminiスコア"
                    f"{max_score}点に揃えました。元スコア: {original_score}点。"
                )[:220]
                adjusted_count += 1
            elif not result.get("same_day_score_adjustment_ja"):
                result["same_day_score_adjustment_ja"] = (
                    f"同日同一事象グループ{len(unique_rows)}件の最高Geminiスコアとして使用。"
                )[:220]

    if adjusted_count:
        print(
            f"[selection-ml-classifier] same_day_score_sharing adjusted_rows={adjusted_count} "
            f"groups={sum(1 for rows_ in groups.values() if len(set(rows_)) >= 2)}"
        )
    return results


def run_gemini_rerank(
    rows: list[ArticleRecord],
    prediction_details: list[dict[str, Any]],
    archive_records: list[ArticleRecord] | None = None,
) -> dict[int, dict[str, Any]]:
    if not ENABLE_GEMINI_RERANK:
        # This branch is kept only as a code-level emergency switch.
        print("[selection-ml-classifier] gemini_rerank=disabled_by_code")
        return {}

    # Gemini reranking is intentionally always-on for this workflow.
    # Store the API key in GitHub Actions Secrets as GEMINI_API_KEY and expose it
    # to the job environment. Missing key should fail loudly instead of silently
    # producing ML-only output.
    api_key = required_env("GEMINI_API_KEY")

    candidates = select_gemini_rerank_candidates(rows, prediction_details)
    if not candidates:
        print("[selection-ml-classifier] gemini_rerank=skipped reason=no_candidates")
        return {}

    model_name = os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    fallback_model_name = (
        os.environ.get("GEMINI_FALLBACK_MODEL", DEFAULT_GEMINI_FALLBACK_MODEL).strip()
        or DEFAULT_GEMINI_FALLBACK_MODEL
    )
    batch_size = env_int("GEMINI_RERANK_BATCH_SIZE", GEMINI_DEFAULT_BATCH_SIZE, 1, 50)
    retry_count = env_int("GEMINI_RERANK_RETRIES", 2, 0, 5)
    sleep_seconds = env_float("GEMINI_RERANK_RETRY_SLEEP_SECONDS", 3.0, 0.0, 60.0)
    fallback_wait_seconds = env_float(
        "GEMINI_MODEL_FALLBACK_WAIT_SECONDS",
        GEMINI_MODEL_FALLBACK_WAIT_SECONDS_DEFAULT,
        0.0,
        60.0,
    )

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)
    results: dict[int, dict[str, Any]] = {}
    model_usage_counts: dict[str, int] = {}
    archive_context_map = build_archive_adopted_context_map(rows, archive_records or [])
    same_day_context_map = build_same_day_context_map(rows)

    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        payloads = [
            build_gemini_article_payload(
                row,
                detail,
                archive_context_map.get(row.row_index, []),
                same_day_context_map.get(row.row_index, []),
            )
            for row, detail in batch
        ]
        prompt = build_gemini_rerank_prompt(payloads)
        contents = [gemini_system_prompt(), prompt]

        last_error: Exception | None = None
        for attempt in range(retry_count + 1):
            try:
                response, used_model_name = generate_gemini_content_with_model_fallback(
                    client,
                    types,
                    model_name,
                    fallback_model_name,
                    contents,
                    fallback_wait_seconds,
                )
                model_usage_counts[used_model_name] = model_usage_counts.get(used_model_name, 0) + len(batch)
                parsed = parse_gemini_json_response(response.text)
                for item in parsed.get("articles", []):
                    if not isinstance(item, dict):
                        continue
                    row_index = safe_int(item.get("row_index"), -1)
                    if row_index > 0:
                        results[row_index] = normalize_gemini_result(item)
                break
            except Exception as exc:
                last_error = exc
                if attempt < retry_count:
                    time.sleep(sleep_seconds)
                else:
                    print(
                        f"[selection-ml-classifier] gemini_rerank_batch_failed "
                        f"start={start} size={len(batch)} error={exc}",
                        file=sys.stderr,
                    )
        if last_error is None:
            pass

    results = apply_same_day_score_sharing(rows, results)
    print(
        f"[selection-ml-classifier] gemini_rerank=done model={model_name} "
        f"fallback_model={fallback_model_name} candidates={len(candidates)} "
        f"results={len(results)} same_day_context_rows={len(same_day_context_map)} "
        f"archive_context_rows={len(archive_context_map)} "
        f"model_usage={json.dumps(model_usage_counts, ensure_ascii=False)}"
    )
    return results


def calculate_final_score(ml_score: int, gemini_score: int | None) -> int:
    """Use the higher score between ML and Gemini for AA列（最終スコア）."""
    ml_score = clamp_score(ml_score, 0)
    if gemini_score is None:
        return ml_score
    return max(ml_score, clamp_score(gemini_score, 0))


def selected_score_source(ml_score: int, gemini_score: int | None) -> str:
    """Return which score source was adopted for AB列."""
    ml_score = clamp_score(ml_score, 0)
    if gemini_score is None:
        return "ML採用（Geminiスコアなし）"

    gemini_score = clamp_score(gemini_score, 0)
    if gemini_score > ml_score:
        return "Gemini採用"
    if ml_score > gemini_score:
        return "ML採用"
    return "同点（ML/Gemini）"

def format_gemini_reason(result: dict[str, Any] | None) -> str:
    if not result:
        return ""
    reasons = result.get("reasons", []) or []
    risks = result.get("risk_flags", []) or []
    axes = result.get("editorial_axes", []) or []
    priority_tags = result.get("priority_topic_tags", []) or []
    parts = []
    if reasons:
        parts.append("理由: " + " / ".join(reasons))
    if risks:
        parts.append("注意: " + " / ".join(risks))
    if axes:
        parts.append("軸: " + " / ".join(axes))
    if priority_tags:
        parts.append("JS優先トピック: " + " / ".join(priority_tags))
    if result.get("direct_myanmar_relevance"):
        parts.append(f"直接関連: {result['direct_myanmar_relevance']}")
    if result.get("country_weight_tier"):
        parts.append(f"国名階層: {result['country_weight_tier']}")
    if result.get("conflict_damage_target"):
        parts.append(f"紛争被害対象: {result['conflict_damage_target']}")
    event_parts = [
        cell_text(result.get("event_when")),
        cell_text(result.get("event_who")),
        cell_text(result.get("event_where")),
        cell_text(result.get("event_what")),
    ]
    if any(event_parts):
        parts.append("4W: " + " / ".join(part or "-" for part in event_parts))
    if result.get("same_day_event_relation"):
        parts.append(f"同日内判定: {result['same_day_event_relation']}")
    if result.get("same_day_diff_ja"):
        parts.append(f"同日差分: {result['same_day_diff_ja']}")
    if result.get("same_day_score_adjustment_ja"):
        parts.append(f"同日スコア調整: {result['same_day_score_adjustment_ja']}")
    if result.get("adopted_archive_relation"):
        parts.append(f"過去採用との差分判定: {result['adopted_archive_relation']}")
    if result.get("adopted_archive_diff_ja"):
        parts.append(f"差分: {result['adopted_archive_diff_ja']}")
    novelty = result.get("novelty_vs_archive_or_same_topic") or result.get("novelty_vs_same_topic")
    if novelty:
        parts.append(f"新規性: {novelty}")
    if result.get("topic_key"):
        parts.append(f"トピックキー: {result['topic_key']}")
    return " / ".join(parts)[:3000]


def build_final_reason(ml_score: int, gemini_result: dict[str, Any] | None, final_score: int) -> str:
    gemini_score = (
        clamp_score(gemini_result.get("gemini_score"), 0)
        if gemini_result is not None
        else None
    )
    return selected_score_source(ml_score, gemini_score)

def build_gemini_detail_json(result: dict[str, Any] | None) -> str:
    if not result:
        return ""
    return json.dumps(result, ensure_ascii=False)[:45000]

def selected_count_by_group(rows: list[ArticleRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        if int(row.label or 0) == 1:
            counts[row.group_key or row.date_key or "unknown"] = (
                counts.get(row.group_key or row.date_key or "unknown", 0) + 1
            )
    return counts


def manual_order_sample_weights(rows: list[ArticleRecord]) -> list[float]:
    """Return sample weights while keeping the task as binary classification.

    Negative rows keep weight 1.0. For selected rows, earlier manual rows get
    higher weight. If a group has 24 selected rows, the first selected row gets
    about 3.0 and the last selected row gets about 1.08 before class balancing.
    """
    selected_counts = selected_count_by_group(rows)
    weights: list[float] = []
    for row in rows:
        if int(row.label or 0) != 1:
            weights.append(1.0)
            continue

        group_key = row.group_key or row.date_key or "unknown"
        selected_count = max(1, selected_counts.get(group_key, 1))
        rank = row.manual_rank if row.manual_rank is not None else selected_count
        rank = max(1, min(rank, selected_count))
        priority_ratio = (selected_count - rank + 1) / selected_count
        weights.append(1.0 + 2.0 * priority_ratio)

    return weights


def train_model(rows: list[ArticleRecord]) -> ConstantClassificationModel | SklearnClassificationModel:
    if not rows:
        raise RuntimeError("No usable training rows were found in the archive folder")

    training_rows = [row for row in rows if row.label is not None]
    labels = [1 if int(row.label or 0) == 1 else 0 for row in training_rows]
    positive_count = sum(labels)
    negative_count = len(labels) - positive_count
    sample_weights = manual_order_sample_weights(training_rows)

    positive_weights = [
        weight for row, weight in zip(training_rows, sample_weights, strict=True)
        if int(row.label or 0) == 1
    ]
    info = {
        "all_training_rows": len(rows),
        "classification_training_rows": len(training_rows),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "manual_order_weighting": True,
        "positive_weight_min": round(min(positive_weights), 3) if positive_weights else 0.0,
        "positive_weight_max": round(max(positive_weights), 3) if positive_weights else 0.0,
    }

    if len(training_rows) < 2 or positive_count == 0 or negative_count == 0:
        probability = positive_count / len(training_rows) if training_rows else 0.0
        print(
            "[selection-ml-classifier] no usable two-class training data; "
            f"using constant probability score={probability:.4f}",
            file=sys.stderr,
        )
        return ConstantClassificationModel(probability, info)

    from sklearn.linear_model import LogisticRegression

    feature_pipeline = build_feature_pipeline()
    try:
        features = feature_pipeline.fit_transform(training_rows)
    except ValueError as exc:
        probability = positive_count / len(training_rows)
        print(
            "[selection-ml-classifier] feature training could not build a vocabulary; "
            f"using constant probability score={probability:.4f}: {exc}",
            file=sys.stderr,
        )
        return ConstantClassificationModel(probability, info)

    classifier = LogisticRegression(
        solver="liblinear",
        class_weight="balanced",
        max_iter=1000,
        random_state=42,
    )
    classifier.fit(features, labels, sample_weight=sample_weights)
    return SklearnClassificationModel(feature_pipeline, classifier, info)


def normalize_probability_scores(probabilities: list[float]) -> list[int]:
    """Convert classifier probabilities to 0-100 scores."""
    scores: list[int] = []
    for probability in probabilities:
        if probability != probability:  # NaN guard
            probability = 0.0
        scores.append(max(0, min(100, round(float(probability) * 100))))
    return scores


def score_reason(score: int, model_info: dict[str, Any]) -> str:
    if score >= 80:
        band = "採用可能性が高い候補"
    elif score >= 60:
        band = "採用可能性がやや高い候補"
    elif score >= 40:
        band = "中位候補"
    else:
        band = "採用可能性が低めの候補"

    return (
        f"{band}（純ML分類版: selected=1 / non-selected=0 / "
        f"手動順位は採用行の学習重みとして使用 / LogisticRegression / "
        f"重要トピック特徴量なし / 媒体特徴量なし / 前日続編・重複補正なし / "
        f"学習 {model_info.get('classification_training_rows', 0)}件 / "
        f"採用 {model_info.get('positive_count', 0)}件 / "
        f"非採用 {model_info.get('negative_count', 0)}件）"
    )[:260]


def build_output_values(
    rows: list[ArticleRecord],
    row_count: int,
    prediction_details: list[dict[str, Any]],
    model_info: dict[str, Any],
    gemini_results: dict[int, dict[str, Any]] | None = None,
) -> list[list[Any]]:
    gemini_results = gemini_results or {}
    by_row_index: dict[int, list[Any]] = {}
    for row, detail in zip(rows, prediction_details, strict=True):
        ml_score = int(detail.get("score", 0))
        gemini_result = gemini_results.get(row.row_index)
        gemini_score = (
            clamp_score(gemini_result.get("gemini_score"), 0)
            if gemini_result is not None
            else None
        )
        final_score = calculate_final_score(ml_score, gemini_score)
        by_row_index[row.row_index] = [
            ml_score,
            score_reason(ml_score, model_info),
            MODEL_VERSION,
            format_contribution_list(detail.get("positive_reasons", [])),
            format_contribution_list(detail.get("negative_reasons", [])),
            build_detail_json(detail),
            gemini_score if gemini_score is not None else "",
            cell_text(gemini_result.get("decision")) if gemini_result else "",
            format_gemini_reason(gemini_result),
            final_score,
            build_final_reason(ml_score, gemini_result, final_score),
            build_gemini_detail_json(gemini_result),
        ]

    blank = [""] * len(OUTPUT_HEADERS)
    return [by_row_index.get(row_index, blank) for row_index in range(2, row_count + 2)]


def approximate_json_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def split_sheet_write_chunks(values: list[list[Any]]) -> list[tuple[int, list[list[Any]]]]:
    """Split output rows to keep each Sheets write request small and retryable."""
    if not values:
        return []

    max_rows = env_int("SHEETS_WRITE_CHUNK_ROWS", SHEETS_WRITE_CHUNK_ROWS_DEFAULT, 1, 500)
    max_payload_bytes = env_int(
        "SHEETS_WRITE_MAX_PAYLOAD_BYTES",
        SHEETS_WRITE_MAX_PAYLOAD_BYTES_DEFAULT,
        100_000,
        5_000_000,
    )

    chunks: list[tuple[int, list[list[Any]]]] = []
    current_start_row = 2
    current_rows: list[list[Any]] = []
    current_bytes = 0

    for sheet_row_index, row_values in enumerate(values, start=2):
        row_bytes = approximate_json_bytes(row_values)
        should_flush = bool(current_rows) and (
            len(current_rows) >= max_rows
            or current_bytes + row_bytes > max_payload_bytes
        )
        if should_flush:
            chunks.append((current_start_row, current_rows))
            current_start_row = sheet_row_index
            current_rows = []
            current_bytes = 0

        current_rows.append(row_values)
        current_bytes += row_bytes

    if current_rows:
        chunks.append((current_start_row, current_rows))

    return chunks


def is_retryable_sheets_exception(exc: Exception) -> bool:
    from googleapiclient.errors import HttpError

    if isinstance(exc, HttpError):
        status = int(getattr(exc.resp, "status", 0) or 0)
        return status in {408, 429, 500, 502, 503, 504}

    return isinstance(
        exc,
        (
            ssl.SSLError,
            socket.timeout,
            TimeoutError,
            ConnectionError,
            ConnectionResetError,
            BrokenPipeError,
            http.client.RemoteDisconnected,
            http.client.IncompleteRead,
            http.client.CannotSendRequest,
            http.client.ResponseNotReady,
        ),
    )


def execute_sheets_write_with_retry(request_factory: Any, description: str) -> Any:
    max_retries = env_int("SHEETS_WRITE_MAX_RETRIES", SHEETS_WRITE_MAX_RETRIES_DEFAULT, 0, 10)
    base_sleep = env_float("SHEETS_WRITE_RETRY_BASE_SECONDS", SHEETS_WRITE_RETRY_BASE_SECONDS_DEFAULT, 0.0, 60.0)
    max_sleep = env_float("SHEETS_WRITE_RETRY_MAX_SECONDS", SHEETS_WRITE_RETRY_MAX_SECONDS_DEFAULT, 1.0, 120.0)

    for attempt in range(max_retries + 1):
        try:
            # num_retries handles retryable HTTP responses inside google-api-python-client.
            # The outer loop also catches transport-level failures such as SSLEOFError.
            return request_factory().execute(num_retries=2)
        except Exception as exc:
            retryable = is_retryable_sheets_exception(exc)
            if not retryable or attempt >= max_retries:
                print(
                    f"[selection-ml-classifier] sheets_write_failed "
                    f"target={description} attempt={attempt + 1}/{max_retries + 1} "
                    f"retryable={retryable} error={exc}",
                    file=sys.stderr,
                )
                raise

            wait_seconds = min(max_sleep, base_sleep * (2 ** attempt)) + random.random()
            print(
                f"[selection-ml-classifier] sheets_write_retry "
                f"target={description} attempt={attempt + 1}/{max_retries + 1} "
                f"sleep={wait_seconds:.1f}s error={exc}",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"Sheets write retry loop unexpectedly ended: {description}")


def update_sheet_range_with_retry(
    sheets: Any,
    spreadsheet_id: str,
    range_name: str,
    values: list[list[Any]],
) -> None:
    execute_sheets_write_with_retry(
        lambda: sheets.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": values},
        ),
        range_name,
    )


def write_predictions(
    sheets: Any,
    spreadsheet_id: str,
    sheet_name: str,
    values: list[list[Any]],
) -> None:
    update_sheet_range_with_retry(
        sheets,
        spreadsheet_id,
        f"{sheet_name}!{OUTPUT_START_COLUMN}1:{OUTPUT_END_COLUMN}1",
        [OUTPUT_HEADERS],
    )

    chunks = split_sheet_write_chunks(values)
    total_payload_bytes = approximate_json_bytes(values) if values else 0
    print(
        f"[selection-ml-classifier] sheets_write rows={len(values)} "
        f"chunks={len(chunks)} approx_payload_bytes={total_payload_bytes}"
    )

    for start_row, chunk_values in chunks:
        end_row = start_row + len(chunk_values) - 1
        range_name = f"{sheet_name}!{OUTPUT_START_COLUMN}{start_row}:{OUTPUT_END_COLUMN}{end_row}"
        update_sheet_range_with_retry(
            sheets,
            spreadsheet_id,
            range_name,
            chunk_values,
        )


def run() -> None:
    mode = os.environ.get("MODE", "predict").strip()
    if mode != "predict":
        raise RuntimeError(f"Unsupported MODE: {mode}")

    spreadsheet_id = required_env("SPREADSHEET_ID")
    archive_folder_id = required_env("ARCHIVE_FOLDER_ID")
    target_sheet = parse_target_sheet(required_env("TARGET_SHEET"))
    drive, sheets = get_services()

    archive_records, archive_file_count = load_archive_records(
        drive,
        sheets,
        archive_folder_id,
    )
    local_adopted_archive_records = load_local_adopted_archive_records(
        sheets,
        spreadsheet_id,
        target_sheet,
    )
    gemini_archive_context_records = archive_records + local_adopted_archive_records

    print(
        f"[selection-ml-classifier] archive_files={archive_file_count} "
        f"archive_rows={len(archive_records)} "
        f"positives={sum(1 for row in archive_records if int(row.label or 0) > 0)} "
        f"local_adopted_archive_rows={len(local_adopted_archive_records)} "
        "rules=off priority_topics=off media_feature=off prev_day=off recency_weight=off gemini=max_score "
        "model=logistic_regression_classifier manual_order_weight=on"
    )

    model = train_model(archive_records)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    prediction_details = model.prediction_details(current_rows)
    gemini_results = run_gemini_rerank(
        current_rows,
        prediction_details,
        gemini_archive_context_records,
    )
    output_values = build_output_values(
        current_rows,
        row_count,
        prediction_details,
        model.info,
        gemini_results,
    )
    write_predictions(sheets, spreadsheet_id, target_sheet, output_values)
    print(
        f"[selection-ml-classifier] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)} "
        f"classification_training_rows={model.info.get('classification_training_rows', 0)} "
        f"positives={model.info.get('positive_count', 0)} "
        f"gemini_results={len(gemini_results)} output_columns={OUTPUT_START_COLUMN}:{OUTPUT_END_COLUMN}"
    )


if __name__ == "__main__":
    run()
