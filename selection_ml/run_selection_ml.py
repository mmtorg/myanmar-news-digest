#!/usr/bin/env python3
"""Train from monthly archive spreadsheets and score current sheet rows."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
ARCHIVE_FILE_PREFIX = "prod_"
ARCHIVE_SHEET_NAME = "prod"
MODEL_VERSION = "selection-ml-v1-char-tfidf-logreg"
OUTPUT_HEADERS = ["ML採用確率スコア", "ML判定補足", "MLモデルバージョン"]
MIN_COLUMNS = 32


@dataclass(frozen=True)
class TrainingRow:
    text: str
    label: int


@dataclass(frozen=True)
class CurrentRow:
    row_index: int
    text: str


class ConstantProbabilityModel:
    """Fallback used when archive labels contain only one class."""

    def __init__(self, probability: float):
        self.probability = probability

    def positive_probabilities(self, texts: list[str]) -> list[float]:
        return [self.probability] * len(texts)


class SklearnProbabilityModel:
    def __init__(self, pipeline: Any):
        self.pipeline = pipeline

    def positive_probabilities(self, texts: list[str]) -> list[float]:
        probabilities = self.pipeline.predict_proba(texts)
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


def model_text(row: list[Any]) -> str:
    values = padded_row(row)
    return "\n".join(
        str(values[index] or "").strip() for index in (4, 5, 6, 8)
    ).strip()


def load_all_training_rows(
    drive: Any,
    sheets: Any,
    archive_folder_id: str,
) -> tuple[list[TrainingRow], int]:
    from googleapiclient.errors import HttpError

    archive_files = list_archive_spreadsheets(drive, archive_folder_id)
    rows: list[TrainingRow] = []

    for archive_file in archive_files:
        try:
            values = read_sheet_values(
                sheets,
                archive_file["id"],
                f"{ARCHIVE_SHEET_NAME}!A:AF",
            )
        except HttpError as exc:
            print(
                f"[selection-ml] skip {archive_file['name']} / "
                f"{ARCHIVE_SHEET_NAME}: {exc}",
                file=sys.stderr,
            )
            continue

        for raw_row in values[1:]:
            values_row = padded_row(raw_row)
            text = model_text(values_row)
            if not text:
                continue
            label = int(str(values_row[10] or "").strip().lower() == "a")
            rows.append(TrainingRow(text=text, label=label))

    return rows, len(archive_files)


def load_current_rows(
    sheets: Any,
    spreadsheet_id: str,
    sheet_name: str,
) -> tuple[list[CurrentRow], int]:
    values = read_sheet_values(sheets, spreadsheet_id, f"{sheet_name}!A:AF")
    sheet_row_count = max(0, len(values) - 1)
    rows = []

    for row_index, raw_row in enumerate(values[1:], start=2):
        text = model_text(raw_row)
        if text:
            rows.append(CurrentRow(row_index=row_index, text=text))

    return rows, sheet_row_count


def train_model(rows: list[TrainingRow]) -> ConstantProbabilityModel | SklearnProbabilityModel:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline

    if not rows:
        raise RuntimeError("No usable training rows were found in the archive folder")

    labels = [row.label for row in rows]
    positive_rate = sum(labels) / len(labels)
    if len(set(labels)) == 1:
        print(
            "[selection-ml] archive labels contain one class; "
            f"using constant probability {positive_rate:.4f}"
        )
        return ConstantProbabilityModel(positive_rate)

    pipeline = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    analyzer="char_wb",
                    ngram_range=(2, 5),
                    min_df=2,
                    max_features=50_000,
                    sublinear_tf=True,
                ),
            ),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=1000,
                    random_state=42,
                ),
            ),
        ]
    )
    try:
        pipeline.fit([row.text for row in rows], labels)
    except ValueError as exc:
        print(
            "[selection-ml] model training could not build a vocabulary; "
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
    return f"{band}（教師データ {training_count}件 / 採用 {positive_count}件）"


def build_output_values(
    rows: list[CurrentRow],
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
            "range": f"{sheet_name}!AA1:AC1",
            "values": [OUTPUT_HEADERS],
        }
    ]
    if values:
        data.append(
            {
                "range": f"{sheet_name}!AA2:AC{len(values) + 1}",
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

    training_rows, archive_file_count = load_all_training_rows(
        drive,
        sheets,
        archive_folder_id,
    )
    positive_count = sum(row.label for row in training_rows)
    print(
        f"[selection-ml] archive_files={archive_file_count} "
        f"training_rows={len(training_rows)} positives={positive_count}"
    )
    model = train_model(training_rows)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    probabilities = model.positive_probabilities([row.text for row in current_rows])
    output_values = build_output_values(
        current_rows,
        row_count,
        probabilities,
        positive_count,
        len(training_rows),
    )
    write_predictions(sheets, spreadsheet_id, target_sheet, output_values)
    print(
        f"[selection-ml] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)}"
    )


if __name__ == "__main__":
    run()
