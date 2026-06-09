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
"""

from __future__ import annotations

import json
import os
import re
import sys
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
MODEL_VERSION = "selection-ml-v6-ja-pure-classifier-manual-order-weight-explain"
OUTPUT_HEADERS = [
    "MLスコア",
    "ML判定補足",
    "MLモデルバージョン",
    "MLスコア上昇要因",
    "MLスコア低下要因",
    "ML寄与詳細JSON",
]
OUTPUT_START_COLUMN = "R"
OUTPUT_END_COLUMN = "W"
MAX_REASON_FEATURES = 8
MIN_COLUMNS = 32

COL_DATE = 0       # A
COL_MEDIA = 2      # C
COL_E = 4          # E
COL_F = 5          # F
COL_G = 6          # G
COL_I = 8          # I
COL_URL = 9        # J
COL_ADOPTED = 10   # K
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
    their order back to the original candidate rows by URL/Q/title and dedupes
    repeated candidates within the same date/source group.

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
    """Generic non-topic metadata only. No hand-written topic flags."""
    features: list[dict[str, float | str]] = []
    for row in rows:
        item: dict[str, float | str] = {
            "has_url": float(bool(row.url)),
            "has_duplicate_key": float(bool(row.duplicate_key)),
            "headline_a_len_bin": min(len(row.headline_a) // 20, 10),
            "headline_final_len_bin": min(len(row.headline_final) // 20, 10),
            "headline_body_len_bin": min(len(row.headline_body) // 20, 10),
            "summary_len_bin": min(len(row.summary) // 80, 10),
            f"media={row.media[:80]}": 1.0,
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
        f"重要トピック特徴量なし / 前日続編・重複補正なし / "
        f"学習 {model_info.get('classification_training_rows', 0)}件 / "
        f"採用 {model_info.get('positive_count', 0)}件 / "
        f"非採用 {model_info.get('negative_count', 0)}件）"
    )[:260]


def build_output_values(
    rows: list[ArticleRecord],
    row_count: int,
    prediction_details: list[dict[str, Any]],
    model_info: dict[str, Any],
) -> list[list[Any]]:
    by_row_index: dict[int, list[Any]] = {}
    for row, detail in zip(rows, prediction_details, strict=True):
        score = int(detail.get("score", 0))
        by_row_index[row.row_index] = [
            score,
            score_reason(score, model_info),
            MODEL_VERSION,
            format_contribution_list(detail.get("positive_reasons", [])),
            format_contribution_list(detail.get("negative_reasons", [])),
            build_detail_json(detail),
        ]

    blank = [""] * len(OUTPUT_HEADERS)
    return [by_row_index.get(row_index, blank) for row_index in range(2, row_count + 2)]


def write_predictions(
    sheets: Any,
    spreadsheet_id: str,
    sheet_name: str,
    values: list[list[Any]],
) -> None:
    data = [
        {
            "range": f"{sheet_name}!{OUTPUT_START_COLUMN}1:{OUTPUT_END_COLUMN}1",
            "values": [OUTPUT_HEADERS],
        }
    ]
    if values:
        data.append(
            {
                "range": f"{sheet_name}!{OUTPUT_START_COLUMN}2:{OUTPUT_END_COLUMN}{len(values) + 1}",
                "values": values,
            }
        )

    (
        sheets.spreadsheets()
        .values()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "RAW",
                "data": data,
            },
        )
        .execute()
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

    print(
        f"[selection-ml-classifier] archive_files={archive_file_count} "
        f"archive_rows={len(archive_records)} "
        f"positives={sum(1 for row in archive_records if int(row.label or 0) > 0)} "
        "rules=off priority_topics=off prev_day=off recency_weight=off "
        "model=logistic_regression_classifier manual_order_weight=on"
    )

    model = train_model(archive_records)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    prediction_details = model.prediction_details(current_rows)
    output_values = build_output_values(
        current_rows,
        row_count,
        prediction_details,
        model.info,
    )
    write_predictions(sheets, spreadsheet_id, target_sheet, output_values)
    print(
        f"[selection-ml-classifier] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)} "
        f"classification_training_rows={model.info.get('classification_training_rows', 0)} "
        f"positives={model.info.get('positive_count', 0)}"
    )


if __name__ == "__main__":
    run()
