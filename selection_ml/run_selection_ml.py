#!/usr/bin/env python3
"""Train from monthly archive spreadsheets and score current sheet rows.

Pure ML comparison version.

This version intentionally removes the hand-written selection rules used in the
structured-flags version:
- no priority-topic feature extraction
- no direct-Myanmar-relevance rule feature
- no previous-day duplicate / continuation / different-angle detection
- no post-model previous-day probability adjustment
- no recency sample weighting

The model is trained only from historical labels using Japanese text in E/F/G/I,
plus lightweight generic metadata that does not encode topic-specific rules.
"""

from __future__ import annotations

import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

ARCHIVE_FILE_PREFIX = "prod_"
ARCHIVE_SHEET_NAME = "prod"
MODEL_VERSION = "selection-ml-v4-ja-pure-text-ml"
OUTPUT_HEADERS = ["ML採用確率スコア", "ML判定補足", "MLモデルバージョン"]
OUTPUT_START_COLUMN = "R"
OUTPUT_END_COLUMN = "T"
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
    media: str
    headline_a: str
    headline_final: str
    headline_body: str
    summary: str
    url: str
    duplicate_key: str
    label: int | None = None
    sample_weight: float = 1.0

    @property
    def full_text(self) -> str:
        return "\n".join(
            [self.headline_a, self.headline_final, self.headline_body, self.summary]
        ).strip()


class ConstantProbabilityModel:
    """Fallback used when archive labels contain only one class."""

    def __init__(self, probability: float):
        self.probability = probability

    def positive_probabilities(self, rows: list[ArticleRecord]) -> list[float]:
        return [self.probability] * len(rows)


class SklearnProbabilityModel:
    def __init__(self, pipeline: Any):
        self.pipeline = pipeline

    def positive_probabilities(self, rows: list[ArticleRecord]) -> list[float]:
        probabilities = self.pipeline.predict_proba(rows)
        positive_index = list(self.pipeline.classes_).index(1)
        return [float(row[positive_index]) for row in probabilities]


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


def make_article_record(raw_row: Iterable[Any], row_index: int, label: int | None) -> ArticleRecord:
    values = padded_row(raw_row)
    return ArticleRecord(
        row_index=row_index,
        date_key=date_key(values[COL_DATE]),
        media=cell_text(values[COL_MEDIA]),
        headline_a=normalize_japanese_input_text(values[COL_E]),
        headline_final=normalize_japanese_input_text(values[COL_F]),
        headline_body=normalize_japanese_input_text(values[COL_G]),
        summary=normalize_japanese_input_text(values[COL_I]),
        url=cell_text(values[COL_URL]),
        duplicate_key=cell_text(values[COL_Q_KEY]),
        label=label,
    )


def is_usable_article(record: ArticleRecord) -> bool:
    return bool(record.full_text)


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
                f"[selection-ml-pure] skip {archive_file['name']} / "
                f"{ARCHIVE_SHEET_NAME}: {exc}",
                file=sys.stderr,
            )
            continue

        for row_index, raw_row in enumerate(values[1:], start=2):
            values_row = padded_row(raw_row)
            label = int(cell_text(values_row[COL_ADOPTED]).lower() == "a")
            record = make_article_record(values_row, row_index, label)
            if is_usable_article(record):
                rows.append(record)

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
        record = make_article_record(raw_row, row_index, None)
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


def train_model(rows: list[ArticleRecord]) -> ConstantProbabilityModel | SklearnProbabilityModel:
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import FeatureUnion, Pipeline
    from sklearn.preprocessing import FunctionTransformer

    if not rows:
        raise RuntimeError("No usable training rows were found in the archive folder")

    labels = [int(row.label or 0) for row in rows]
    positive_rate = sum(labels) / len(labels)
    if len(set(labels)) == 1:
        print(
            "[selection-ml-pure] archive labels contain one class; "
            f"using constant probability {positive_rate:.4f}"
        )
        return ConstantProbabilityModel(positive_rate)

    char_kwargs = dict(
        analyzer="char",
        ngram_range=(2, 6),
        min_df=2,
        sublinear_tf=True,
    )

    features = FeatureUnion(
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

    pipeline = Pipeline(
        [
            ("features", features),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=1000,
                    random_state=42,
                    C=1.0,
                ),
            ),
        ]
    )

    try:
        pipeline.fit(rows, labels)
    except ValueError as exc:
        print(
            "[selection-ml-pure] model training could not build a vocabulary; "
            f"using archive positive rate {positive_rate:.4f}: {exc}",
            file=sys.stderr,
        )
        return ConstantProbabilityModel(positive_rate)
    return SklearnProbabilityModel(pipeline)


def score_reason(score: int, positive_count: int, training_count: int) -> str:
    if score >= 70:
        band = "過去の手動採用記事に強く類似"
    elif score >= 40:
        band = "過去の採用傾向に一部類似"
    else:
        band = "過去の手動採用傾向との類似度は低め"

    return (
        f"{band}（純ML版: 重要トピック特徴量なし / 前日続編・重複補正なし / "
        f"直近重みなし / 教師データ {training_count}件 / 採用 {positive_count}件）"
    )[:260]


def build_output_values(
    rows: list[ArticleRecord],
    row_count: int,
    probabilities: list[float],
    positive_count: int,
    training_count: int,
) -> list[list[Any]]:
    by_row_index: dict[int, list[Any]] = {}
    for row, probability in zip(rows, probabilities, strict=True):
        score = max(0, min(100, round(probability * 100)))
        by_row_index[row.row_index] = [
            score,
            score_reason(score, positive_count, training_count),
            MODEL_VERSION,
        ]

    return [by_row_index.get(row_index, ["", "", ""]) for row_index in range(2, row_count + 2)]


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

    training_rows = archive_records
    positive_count = sum(int(row.label or 0) for row in training_rows)

    print(
        f"[selection-ml-pure] archive_files={archive_file_count} "
        f"training_rows={len(training_rows)} positives={positive_count} "
        "rules=off priority_topics=off prev_day=off recency_weight=off"
    )

    model = train_model(training_rows)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    probabilities = model.positive_probabilities(current_rows)
    output_values = build_output_values(
        current_rows,
        row_count,
        probabilities,
        positive_count,
        len(training_rows),
    )
    write_predictions(sheets, spreadsheet_id, target_sheet, output_values)
    print(
        f"[selection-ml-pure] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)}"
    )


if __name__ == "__main__":
    run()
