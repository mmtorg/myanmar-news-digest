#!/usr/bin/env python3
"""Train from monthly archive spreadsheets and score current sheet rows.

Pure ML ranking version.

Applied changes:
- Manual order is used as graded relevance. Within each date/sheet group,
  K=a rows are read in their sheet order and converted to relevance scores.
  Example: if 24 rows were manually selected, the first selected row gets 24,
  the second gets 23, ..., the last gets 1, non-selected rows get 0.
- The model is changed from binary classification to ranking.
  It uses LightGBM's LGBMRanker with LambdaRank.
- Hand-written topic rules remain removed:
  no priority-topic feature extraction, no direct-Myanmar-relevance feature,
  no previous-day duplicate/continuation adjustment, no recency weighting.

Required extra dependency:
    lightgbm>=4.0.0
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
MODEL_VERSION = "selection-ml-v5-ja-pure-lightgbm-ranker-manual-order"
OUTPUT_HEADERS = ["MLランキングスコア", "ML判定補足", "MLモデルバージョン"]
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


class ConstantRankingModel:
    """Fallback used when ranking training data is not usable."""

    def __init__(self, score: float, info: dict[str, Any]):
        self.score = score
        self.info = info

    def raw_scores(self, rows: list[ArticleRecord]) -> list[float]:
        return [self.score] * len(rows)


class LightGbmRankingModel:
    def __init__(self, feature_pipeline: Any, ranker: Any, info: dict[str, Any]):
        self.feature_pipeline = feature_pipeline
        self.ranker = ranker
        self.info = info

    def raw_scores(self, rows: list[ArticleRecord]) -> list[float]:
        features = self.feature_pipeline.transform(rows)
        return [float(v) for v in self.ranker.predict(features)]


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


def build_rank_training_records_from_sheet(
    values: list[list[Any]],
    source_group_prefix: str,
) -> list[ArticleRecord]:
    """Build one deduped training set from an archive sheet.

    K=a rows are interpreted as manual order.  If an archive sheet contains
    duplicate manual rows appended below the candidate list, this function maps
    their order back to the original candidate rows by URL/Q/title and dedupes
    repeated candidates within the same date/source group.
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

    relevance_by_group_key: dict[tuple[str, str], tuple[int, int]] = {}
    for group_key, keys in manual_keys_by_group.items():
        selected_count = len(keys)
        for order_index, key in enumerate(keys, start=1):
            relevance = selected_count - order_index + 1
            relevance_by_group_key[(group_key, key)] = (relevance, order_index)

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

        relevance, manual_rank = relevance_by_group_key.get((record.group_key, key), (0, None))
        deduped.append(replace(record, label=relevance, manual_rank=manual_rank))

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
                f"[selection-ml-ranker] skip {archive_file['name']} / "
                f"{ARCHIVE_SHEET_NAME}: {exc}",
                file=sys.stderr,
            )
            continue

        rows.extend(
            build_rank_training_records_from_sheet(
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


def prepare_rank_training_groups(rows: list[ArticleRecord]) -> tuple[list[ArticleRecord], list[int], list[int]]:
    grouped: dict[str, list[ArticleRecord]] = {}
    for row in rows:
        grouped.setdefault(row.group_key or row.date_key or "unknown", []).append(row)

    training_rows: list[ArticleRecord] = []
    labels: list[int] = []
    group_sizes: list[int] = []

    for group_key in sorted(grouped):
        group_rows = grouped[group_key]
        group_labels = [int(row.label or 0) for row in group_rows]
        # Ranking groups with no positive relevance do not teach order, so skip them.
        if len(group_rows) < 2 or max(group_labels, default=0) <= 0 or len(set(group_labels)) < 2:
            continue
        training_rows.extend(group_rows)
        labels.extend(group_labels)
        group_sizes.append(len(group_rows))

    return training_rows, labels, group_sizes


def train_model(rows: list[ArticleRecord]) -> ConstantRankingModel | LightGbmRankingModel:
    if not rows:
        raise RuntimeError("No usable training rows were found in the archive folder")

    training_rows, labels, group_sizes = prepare_rank_training_groups(rows)
    positive_count = sum(1 for row in rows if int(row.label or 0) > 0)
    max_relevance = max((int(row.label or 0) for row in rows), default=0)

    info = {
        "all_training_rows": len(rows),
        "rank_training_rows": len(training_rows),
        "rank_groups": len(group_sizes),
        "positive_count": positive_count,
        "max_relevance": max_relevance,
    }

    if not training_rows or not group_sizes or max(labels, default=0) <= 0:
        print(
            "[selection-ml-ranker] no usable ranking groups; using constant rank score",
            file=sys.stderr,
        )
        return ConstantRankingModel(0.0, info)

    try:
        from lightgbm import LGBMRanker
    except ImportError as exc:
        raise RuntimeError(
            "LightGBM is required for ranking mode. Add `lightgbm>=4.0.0` "
            "to requirements.txt or your GitHub Actions install step."
        ) from exc

    feature_pipeline = build_feature_pipeline()
    try:
        features = feature_pipeline.fit_transform(training_rows)
    except ValueError as exc:
        print(
            "[selection-ml-ranker] feature training could not build a vocabulary; "
            f"using constant rank score: {exc}",
            file=sys.stderr,
        )
        return ConstantRankingModel(0.0, info)

    ranker = LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        boosting_type="gbdt",
        n_estimators=250,
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=8,
        subsample=0.9,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    ranker.fit(features, labels, group=group_sizes)
    return LightGbmRankingModel(feature_pipeline, ranker, info)


def normalize_ranking_scores(raw_scores: list[float]) -> list[int]:
    """Convert arbitrary ranker outputs to a 0-100 within-sheet ranking score."""
    if not raw_scores:
        return []
    n = len(raw_scores)
    if n == 1:
        return [100]

    # Dense ranking by raw score.  Top row becomes 100, bottom row becomes 0.
    order = sorted(range(n), key=lambda i: (-raw_scores[i], i))
    scores = [0] * n
    index = 0
    while index < n:
        end = index
        while end + 1 < n and raw_scores[order[end + 1]] == raw_scores[order[index]]:
            end += 1
        avg_rank = (index + 1 + end + 1) / 2.0
        normalized = round(100 * (n - avg_rank) / max(1, n - 1))
        for pos in range(index, end + 1):
            scores[order[pos]] = max(0, min(100, normalized))
        index = end + 1
    return scores


def score_reason(score: int, model_info: dict[str, Any]) -> str:
    if score >= 80:
        band = "ランキング上位候補"
    elif score >= 60:
        band = "上位寄り候補"
    elif score >= 40:
        band = "中位候補"
    else:
        band = "下位寄り候補"

    return (
        f"{band}（純MLランキング版: 手動順位を教師ラベル化 / LightGBM Ranker / "
        f"重要トピック特徴量なし / 前日続編・重複補正なし / "
        f"rank学習 {model_info.get('rank_training_rows', 0)}件 / "
        f"日付グループ {model_info.get('rank_groups', 0)} / "
        f"採用 {model_info.get('positive_count', 0)}件）"
    )[:260]


def build_output_values(
    rows: list[ArticleRecord],
    row_count: int,
    ranking_scores: list[int],
    model_info: dict[str, Any],
) -> list[list[Any]]:
    by_row_index: dict[int, list[Any]] = {}
    for row, score in zip(rows, ranking_scores, strict=True):
        by_row_index[row.row_index] = [
            score,
            score_reason(score, model_info),
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

    print(
        f"[selection-ml-ranker] archive_files={archive_file_count} "
        f"archive_rows={len(archive_records)} "
        f"positives={sum(1 for row in archive_records if int(row.label or 0) > 0)} "
        "rules=off priority_topics=off prev_day=off recency_weight=off "
        "model=lightgbm_ranker manual_order=on"
    )

    model = train_model(archive_records)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    raw_scores = model.raw_scores(current_rows)
    ranking_scores = normalize_ranking_scores(raw_scores)
    output_values = build_output_values(
        current_rows,
        row_count,
        ranking_scores,
        model.info,
    )
    write_predictions(sheets, spreadsheet_id, target_sheet, output_values)
    print(
        f"[selection-ml-ranker] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)} "
        f"rank_training_rows={model.info.get('rank_training_rows', 0)} "
        f"rank_groups={model.info.get('rank_groups', 0)}"
    )


if __name__ == "__main__":
    run()
