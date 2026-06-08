#!/usr/bin/env python3
"""Train from monthly archive spreadsheets and score current sheet rows.

v2 changes:
- E/F/G/I are vectorized as separate fields instead of one joined text.
- A compact subset of selection.js priority-topic logic is used as flag features.
- Recent archive rows receive larger sample weights.
- Current/training rows are compared with previous-day K=a adopted articles to
  down-rank same-content duplicates and boost continuations/different angles.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any, Iterable

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

ARCHIVE_FILE_PREFIX = "prod_"
ARCHIVE_SHEET_NAME = "prod"
MODEL_VERSION = "selection-ml-v2-structured-flags-recency-prevday"
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

PREV_DAY_DUPLICATE_CAP = 0.42
PREV_DAY_DUPLICATE_MULTIPLIER = 0.55
PREV_DAY_CONTINUATION_BONUS = 0.10
PREV_DAY_DIFFERENT_ANGLE_BONUS = 0.07
PREV_DAY_RELATED_BONUS = 0.03


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
    priority_tags: tuple[str, ...] = ()
    prev_day_relation: str = "none"
    prev_day_note: str = ""

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

    # YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, and strings containing those forms.
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


def previous_date_key(key: str) -> str:
    if not key:
        return ""
    try:
        d = datetime.strptime(key, "%Y%m%d").date()
    except ValueError:
        return ""
    return (d - timedelta(days=1)).strftime("%Y%m%d")


def make_article_record(raw_row: Iterable[Any], row_index: int, label: int | None) -> ArticleRecord:
    values = padded_row(raw_row)
    record = ArticleRecord(
        row_index=row_index,
        date_key=date_key(values[COL_DATE]),
        media=cell_text(values[COL_MEDIA]),
        headline_a=cell_text(values[COL_E]),
        headline_final=cell_text(values[COL_F]),
        headline_body=cell_text(values[COL_G]),
        summary=cell_text(values[COL_I]),
        url=cell_text(values[COL_URL]),
        duplicate_key=cell_text(values[COL_Q_KEY]),
        label=label,
    )
    return replace(record, priority_tags=priority_topic_tags(record.full_text))


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
                f"[selection-ml] skip {archive_file['name']} / "
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


def archive_sheet_name_for_target(target_sheet: str) -> str:
    if target_sheet == "prod":
        return "archive_prod"
    if target_sheet == "dev":
        return "archive_dev"
    return f"archive_{target_sheet}"


def load_current_spreadsheet_adopted_archive_records(
    sheets: Any,
    spreadsheet_id: str,
    target_sheet: str,
) -> list[ArticleRecord]:
    from googleapiclient.errors import HttpError

    sheet_name = archive_sheet_name_for_target(target_sheet)
    try:
        values = read_sheet_values(sheets, spreadsheet_id, f"{sheet_name}!A:AF")
    except HttpError as exc:
        print(f"[selection-ml] current archive sheet skipped: {sheet_name}: {exc}")
        return []

    out: list[ArticleRecord] = []
    for row_index, raw_row in enumerate(values[1:], start=2):
        values_row = padded_row(raw_row)
        if cell_text(values_row[COL_ADOPTED]).lower() != "a":
            continue
        record = make_article_record(values_row, row_index, 1)
        if is_usable_article(record):
            out.append(record)
    return out


def dedupe_adopted_records(records: list[ArticleRecord]) -> list[ArticleRecord]:
    seen: set[str] = set()
    out: list[ArticleRecord] = []
    for record in records:
        key = "|".join(
            [
                record.date_key,
                record.url,
                record.duplicate_key,
                normalize_comparable_text(
                    " ".join([record.headline_a, record.headline_final, record.headline_body])
                )[:120],
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(record)
    return out


def recency_weight(row_date_key: str, latest_date_key: str) -> float:
    if not row_date_key or not latest_date_key:
        return 1.0
    try:
        row_date = datetime.strptime(row_date_key, "%Y%m%d").date()
        latest_date = datetime.strptime(latest_date_key, "%Y%m%d").date()
    except ValueError:
        return 1.0

    days = (latest_date - row_date).days
    if days < 0:
        return 1.0
    if days <= 30:
        return 1.6
    if days <= 90:
        return 1.3
    if days <= 180:
        return 1.1
    return 1.0


def attach_training_weights(records: list[ArticleRecord]) -> list[ArticleRecord]:
    date_keys = [r.date_key for r in records if r.date_key]
    latest = max(date_keys) if date_keys else ""
    return [replace(r, sample_weight=recency_weight(r.date_key, latest)) for r in records]


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
# selection.js-derived compact priority-topic flags
# ---------------------------------------------------------------------------


def has_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def has_latin_term(text: str, terms: list[str]) -> bool:
    return any(re.search(rf"(^|[^A-Za-z0-9_]){re.escape(term)}(?=$|[^A-Za-z0-9_])", text, re.I) for term in terms)


def has_direct_myanmar_relevance(text: str) -> bool:
    s = text or ""
    if has_any(
        s,
        [
            "ミャンマー",
            "ビルマ",
            "Myanmar",
            "Burma",
            "Burmese",
            "မြန်မာ",
            "ဗမာ",
            "ヤンゴン",
            "Yangon",
            "ရန်ကုန်",
            "ネピドー",
            "Naypyidaw",
            "マンダレー",
            "Mandalay",
            "エーヤワディ",
            "Ayeyarwady",
            "バゴー",
            "Bago",
            "サガイン",
            "Sagaing",
            "ラカイン",
            "Rakhine",
            "カレン",
            "Kayin",
            "カチン",
            "Kachin",
            "シャン",
            "Shan",
            "モン",
            "Mon",
            "チン",
            "Chin",
            "カヤー",
            "Kayah",
            "マグウェー",
            "Magway",
            "タニンダーリ",
            "Tanintharyi",
            "ミャワディ",
            "Myawaddy",
            "ムセ",
            "Muse",
            "国軍",
            "軍事政権",
            "軍政",
            "SAC",
            "NUG",
            "PDF",
            "抵抗勢力",
            "民主派",
            "民族武装勢力",
            "ミンアウンフライン",
            "アウンサンスーチー",
            "Aung San Suu Kyi",
            "Min Aung Hlaing",
            "အောင်ဆန်းစုကြည်",
            "မင်းအောင်လှိုင်",
        ],
    ):
        return True

    if has_any(
        s,
        [
            "CBM",
            "中央銀行",
            "ミャンマー中央銀行",
            "チャット",
            "キヤット",
            "Kyat",
            "MMK",
            "CMP",
            "MIC",
            "DICA",
            "UMFCCI",
            "OWIC",
            "UID",
            "YCDC",
            "MCDC",
            "連邦議会",
            "人民代表院",
            "民族代表院",
            "Pyidaungsu Hluttaw",
            "Hluttaw",
            "လွှတ်တော်",
            "ミャンマー港湾局",
            "港湾局",
            "投資委員会",
            "商業省",
            "電力省",
            "建設省",
            "運輸・通信省",
            "国営紙",
            "国営メディア",
        ],
    ):
        return True

    has_system = has_any(
        s,
        [
            "外貨規制",
            "外貨管理",
            "外貨売却",
            "外貨配分",
            "外貨供給",
            "ドル売却",
            "ドル供給",
            "輸入ライセンス",
            "輸出ライセンス",
            "国境貿易",
            "燃料輸入",
            "行政手続き",
            "旅券申請",
            "海外就労者証明書",
        ],
    )
    has_context = has_any(
        s,
        ["大統領", "政府", "当局", "省", "省庁", "委員会", "局", "庁", "議会", "法案", "規制", "通達", "告示", "公告"],
    )
    return has_system and has_context


def has_government_authority(text: str) -> bool:
    return has_any(
        text,
        [
            "ミャンマー政府",
            "政府",
            "当局",
            "省",
            "省庁",
            "委員会",
            "局",
            "庁",
            "大臣",
            "副大臣",
            "長官",
            "管区政府",
            "州政府",
            "YCDC",
            "MCDC",
            "MIC",
            "DICA",
            "建設省",
            "運輸・通信省",
            "電力省",
            "工業省",
            "ミャンマー港湾局",
            "港湾局",
            "Myanmar Port Authority",
            "MPA",
            "国営紙",
            "国営メディア",
            "軍事政権",
            "SAC",
            "連邦議会",
            "人民代表院",
            "民族代表院",
            "議会",
            "Pyidaungsu Hluttaw",
            "Hluttaw",
            "လွှတ်တော်",
        ],
    )


def has_announcement_action(text: str) -> bool:
    return has_any(
        text,
        [
            "発表",
            "公表",
            "声明",
            "通達",
            "告示",
            "公告",
            "入札公告",
            "明らかに",
            "述べ",
            "説明",
            "承認",
            "提出",
            "上程",
            "審議",
            "可決",
            "成立",
            "採択",
            "許可",
            "認可",
            "決定",
            "指示",
            "命令",
            "開始",
            "着工",
            "開通",
            "再開",
            "予定",
            "スケジュール",
            "停止",
            "廃止",
            "実施",
            "導入",
            "announced",
            "submitted",
            "approved",
            "decided",
            "notification",
            "directive",
        ],
    )


def has_policy_topic(text: str) -> bool:
    return has_any(
        text,
        [
            "施策",
            "政策",
            "方針",
            "計画",
            "規制",
            "制限",
            "禁止",
            "緩和",
            "解除",
            "法改正",
            "法律改正",
            "法案",
            "法律案",
            "改正案",
            "法案提出",
            "議会提出",
            "厳罰化",
            "罰則強化",
            "制度変更",
            "行政手続き",
            "オンライン申請",
            "許認可",
            "ライセンス",
            "税制",
            "関税",
            "輸出入",
            "輸出制限",
            "輸入制限",
            "出国",
            "入国",
            "ビザ",
            "旅券",
            "海外就労",
            "労働",
            "雇用",
            "企業活動",
            "事業許可",
            "投資認可",
            "品質基準",
            "衛生基準",
            "policy",
            "regulation",
            "restriction",
            "law",
            "amendment",
            "bill",
            "procedure",
            "permit",
            "license",
            "approval",
            "tax",
            "tariff",
            "import",
            "export",
            "visa",
            "passport",
            "business permit",
        ],
    )


def has_prices_fuel_forex(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    if has_any(
        text,
        [
            "物価",
            "インフレ",
            "燃料価格",
            "ガソリン価格",
            "軽油価格",
            "為替",
            "為替レート",
            "外貨規制",
            "外貨管理",
            "価格統制",
            "外貨使用制限",
            "中央銀行",
            "CBM",
            "exchange rate",
            "foreign currency",
            "fuel price",
            "inflation",
        ],
    ):
        return True
    has_currency = has_any(text, ["チャット", "ドル", "MMK", "Kyat", "USD", "dollar", "Dollar"])
    has_market = has_any(text, ["レート", "外貨", "通貨", "両替", "価格", "市場", "銀行", "規制", "制限", "輸入", "輸出", "決済", "送金", "market", "rate", "currency", "forex", "bank", "restriction"])
    has_fuel = has_any(text, ["燃料", "ガソリン", "軽油", "diesel", "petrol", "fuel"])
    has_fuel_market = has_any(text, ["価格", "値上げ", "値下げ", "不足", "供給", "販売", "輸入", "規制", "制限", "配給", "市場", "price", "shortage", "supply", "import"])
    return (has_currency and has_market) or (has_fuel and has_fuel_market)


def has_central_bank_forex_sale_allocation(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    has_central_bank = has_any(text, ["中央銀行", "ミャンマー中央銀行", "CBM", "Central Bank of Myanmar"])
    has_sale = has_any(text, ["外貨売却", "外貨を売却", "外貨販売", "外貨配分", "外貨供給", "ドル売却", "ドル販売", "ドル供給", "為替市場に売却", "外貨オークション", "foreign currency sale", "foreign exchange sale", "USD sale", "dollar sale"])
    has_use = has_any(text, ["CMP企業", "輸出企業", "輸入業者", "食用油輸入", "燃料輸入", "医薬品輸入", "生活必需品輸入", "輸入決済", "食用油", "燃料", "医薬品", "essential goods", "edible oil", "fuel import", "importers", "CMP"])
    return has_central_bank and has_sale and has_use


def has_port_container_shipping(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    has_port = has_any(text, ["ヤンゴン港", "ティラワ港", "港湾局", "ミャンマー港湾局", "港湾ターミナル", "港湾", "Yangon Port", "Thilawa Port", "Myanmar Port Authority", "MPA", "port terminal"])
    has_vessel = has_any(text, ["コンテナ船", "コンテナ貨物", "貨物船", "船舶", "大型船", "隻", "入港", "寄港", "着岸", "container vessel", "container ship", "cargo vessel", "vessel", "ship"])
    has_context = has_any(text, ["入港予定", "寄港予定", "入港スケジュール", "船舶スケジュール", "航路", "海上貿易", "海上物流", "港湾能力", "浚渫", "輸入増加", "輸出促進", "輸出", "輸入", "貿易", "物流", "需要", "schedule", "shipping route", "maritime trade", "logistics", "port capacity", "dredging", "import", "export", "trade"])
    has_official_or_numeric = has_government_authority(text) or has_announcement_action(text) or has_quantitative_evidence(text)
    return has_port and has_vessel and has_context and has_official_or_numeric


def has_law_revision(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    return has_any(text, ["法案", "法律案", "改正案", "法案提出", "議会提出", "法律改正", "規則改定", "制度変更", "法改正", "厳罰化", "罰則強化", "刑罰強化", "処罰強化", "draft law", "draft bill"]) or bool(re.search(r"(^|[^A-Za-z])bill(s)?(?=$|[^A-Za-z])", text, re.I))


def has_myanmar_leadership_policy(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    has_leader = has_any(text, ["ミンアウンフライン", "ミンアウンフライン大統領", "国軍総司令官", "ミャンマー政府", "ミャンマー省庁", "ミャンマー当局", "政府", "省庁", "省", "当局", "委員会", "局", "庁", "大統領", "大臣", "副大臣", "長官", "連邦議会", "人民代表院", "民族代表院", "議会", "SAC", "Pyidaungsu Hluttaw", "Hluttaw", "မင်းအောင်လှိုင်"])
    has_action = has_any(text, ["政策", "提案", "発表", "公表", "声明", "表明", "方針", "計画", "指示", "要請", "説明", "通達", "告示", "公告", "提出", "上程", "審議", "承認", "決定", "実施", "導入", "開始", "推進", "proposal", "policy", "announced", "statement", "directive", "submitted", "approved", "decided"])
    has_impact = has_any(text, ["政策", "制度", "規制", "制限", "禁止", "緩和", "解除", "法案", "法律", "法改正", "税", "税制", "関税", "燃料", "物価", "為替", "外貨", "輸出", "輸入", "貿易", "物流", "港湾", "コンテナ船", "投資", "企業", "中小企業", "事業", "労働", "雇用", "旅券", "ビザ", "出国", "入国", "電力", "発電", "インフラ", "開発計画", "農業", "製造業", "環境", "森林", "気候変動", "災害", "教育", "医療", "通信", "監視", "情報統制", "品質基準", "policy", "regulation", "law", "tax", "fuel", "trade", "export", "import", "investment", "labour", "labor", "employment", "electricity", "infrastructure", "environment"])
    ceremony_only = has_any(text, ["表敬訪問", "視察", "式典", "記念式典", "開会式", "閉会式", "挨拶", "祝辞", "芳名録", "報奨金", "courtesy call", "ceremony", "inspection"]) and not has_impact
    return has_leader and has_action and has_impact and not ceremony_only


def has_power_project(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    has_power = has_any(text, ["電力需要増", "電力需要", "電力需給", "電力不足", "電力供給", "電力", "停電", "発電", "発電所", "送電", "配電", "変電所", "送電網", "配電網", "電力網", "electricity demand", "power demand", "power supply", "power generation", "transmission", "grid"])
    has_project = has_any(text, ["需要増", "需要増加", "不足に対応", "供給拡大", "増強", "整備", "拡張", "新設", "建設", "改修", "計画", "プロジェクト", "入札", "開発", "投資", "rising demand", "shortage", "expansion", "upgrade", "project", "plan", "tender"])
    return has_power and has_project and has_government_authority(text) and has_announcement_action(text)


def has_business_sme(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    strong = has_any(text, ["中小企業", "零細企業", "小規模事業者", "小規模企業", "MSME", "MSMEs", "SME", "SMEs", "small and medium", "micro small and medium"])
    business = has_any(text, ["ビジネス環境", "ビジネス", "商工業者", "商工会議所", "企業支援", "事業支援", "事業者支援", "企業活動", "民間企業", "起業", "スタートアップ", "business environment", "chamber of commerce", "private sector", "businesses"])
    context = has_any(text, ["支援", "融資", "資金繰り", "税", "関税", "規制", "制度", "許可", "認可", "登録", "投資", "輸出", "輸入", "貿易", "市場", "経済", "雇用", "産業", "振興", "育成", "会議", "協議", "policy", "support", "loan", "tax", "regulation", "investment", "trade", "market", "employment"])
    pr_only = has_any(text, ["寄付", "慈善", "誕生日", "俳優", "女優", "芸能", "映画", "歌手", "charity", "donation", "birthday", "actress", "actor"]) and not strong and not context
    return (strong or (business and context)) and not pr_only


def has_investment_permit(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["外国投資", "海外投資", "国内投資", "投資認可", "MIC", "事業許可", "企業登録", "投資制限", "投資委員会", "investment", "business permit"])


def has_import_export_logistics(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["輸出入実務", "通関", "国境物流", "港湾", "陸路物流", "越境輸送", "輸出", "輸入", "貿易", "物流", "国境貿易", "customs", "border trade", "logistics", "import", "export"])


def has_migration_passport_visa(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["海外就労者", "出国", "海外在住ミャンマー人", "旅券", "パスポート", "OWIC", "ビザ", "相互ビザ免除協定", "入国", "滞在制度", "海外就労", "passport", "visa", "overseas worker"])


def has_labor_policy(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["雇用創出", "職業訓練", "労働者支援", "労働組合", "ストライキ", "賃金", "労働条件", "労使紛争", "労働力不足", "雇用", "labor", "labour", "worker", "employment", "strike"])


def has_telecom_control(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["通信規制", "監視", "インターネット制限", "SNS規制", "情報統制", "通信遮断", "internet restriction", "surveillance"])


def has_food_medicine_quality(text: str) -> bool:
    return has_direct_myanmar_relevance(text) and has_any(text, ["食品", "医薬品", "品質基準", "衛生基準", "認証", "検査", "流通規制", "medicine", "quality standard", "certification"])


def has_official_policy_regulation(text: str) -> bool:
    if not has_direct_myanmar_relevance(text):
        return False
    if not (has_government_authority(text) and has_announcement_action(text) and has_policy_topic(text)):
        return False
    ceremony_only = has_any(text, ["表敬訪問", "視察", "式典", "記念式典", "開会式", "閉会式", "挨拶", "祝辞", "会合", "協議", "meeting", "ceremony", "inspection"]) and not has_any(text, ["規制", "制限", "禁止", "法改正", "法案提出", "議会提出", "改正案", "厳罰化", "制度変更", "通達", "告示", "公告", "提出", "上程", "審議", "可決", "成立", "許認可", "ライセンス", "税制", "関税", "輸出入", "開始", "導入", "停止", "廃止", "施行", "実施", "regulation", "restriction", "amendment", "bill", "submitted", "notification", "directive", "permit", "license"])
    return not ceremony_only


def priority_topic_tags(text: str) -> tuple[str, ...]:
    checks: list[tuple[str, bool]] = [
        ("official_policy_regulation_announcement", has_official_policy_regulation(text)),
        ("prices_fuel_forex", has_prices_fuel_forex(text)),
        ("central_bank_forex_sale_allocation", has_central_bank_forex_sale_allocation(text)),
        ("myanmar_leadership_policy_statement", has_myanmar_leadership_policy(text)),
        ("law_revision", has_law_revision(text)),
        ("port_container_shipping_logistics", has_port_container_shipping(text)),
        ("power_demand_project_plan", has_power_project(text)),
        ("business_sme", has_business_sme(text)),
        ("investment_business_permit", has_investment_permit(text)),
        ("import_export_border_logistics", has_import_export_logistics(text)),
        ("migration_passport_visa", has_migration_passport_visa(text)),
        ("labor_policy_relations", has_labor_policy(text)),
        ("telecom_surveillance_information_control", has_telecom_control(text)),
        ("food_medicine_quality_standard", has_food_medicine_quality(text)),
    ]
    return tuple(tag for tag, ok in checks if ok)


def has_quantitative_evidence(text: str) -> bool:
    return bool(re.search(r"[0-9０-９]+", text)) or has_any(text, ["％", "%", "万", "千", "億", "トン", "人", "世帯", "社", "件", "チャット", "ドル"])


# ---------------------------------------------------------------------------
# Previous-day adopted article relation detection
# ---------------------------------------------------------------------------


def normalize_comparable_text(value: str) -> str:
    return (
        (value or "")
        .lower()
        .replace("\u3000", " ")
        .translate(str.maketrans({c: " " for c in "|｜:：,，.。()（）[]「」『』、\"'’‘“”!?！？/\\"}))
    )


def similarity_tokens(value: str) -> set[str]:
    s = normalize_comparable_text(value)
    words = re.findall(r"[A-Za-z0-9_]{2,}|[\u1000-\u109F]{2,}|[\u3040-\u30ff\u3400-\u9fff]{2,}", s)
    stop = {
        "ミャンマー",
        "ビルマ",
        "myanmar",
        "burma",
        "記事",
        "発表",
        "報道",
        "述べ",
        "した",
        "する",
        "される",
        "について",
        "など",
        "news",
        "report",
        "update",
    }
    tokens: set[str] = set()
    for word in words:
        if word in stop:
            continue
        tokens.add(word)
        # Japanese/Myanmar chunks often have no spaces; char n-grams improve recall.
        if re.search(r"[\u1000-\u109F\u3040-\u30ff\u3400-\u9fff]", word) and len(word) >= 4:
            for n in (2, 3):
                for i in range(0, max(0, len(word) - n + 1)):
                    tokens.add(word[i : i + n])
    return tokens


def text_similarity(a: str, b: str) -> float:
    a_tokens = similarity_tokens(a)
    b_tokens = similarity_tokens(b)
    if not a_tokens or not b_tokens:
        return 0.0
    overlap = len(a_tokens & b_tokens)
    return overlap / max(1, min(len(a_tokens), len(b_tokens)))


def title_text(record: ArticleRecord) -> str:
    return " ".join([record.headline_a, record.headline_final, record.headline_body]).strip()


def has_update_signal(text: str) -> bool:
    return has_any(
        text,
        [
            "続報",
            "進展",
            "新た",
            "追加",
            "更新",
            "増加",
            "減少",
            "拡大",
            "悪化",
            "改善",
            "再開",
            "停止",
            "開始",
            "承認",
            "決定",
            "可決",
            "成立",
            "発表",
            "公表",
            "声明",
            "反応",
            "被害",
            "死亡",
            "負傷",
            "逮捕",
            "拘束",
            "避難",
            "数字",
            "update",
            "new",
            "additional",
            "increased",
            "approved",
            "announced",
            "passed",
            "killed",
            "injured",
        ],
    ) or has_quantitative_evidence(text)


def focus_tags_for_relation(record: ArticleRecord) -> set[str]:
    text = record.full_text
    tags = set(record.priority_tags)
    if has_any(text, ["戦闘", "攻撃", "空爆", "砲撃", "衝突", "国軍", "PDF", "抵抗勢力", "民族武装勢力", "attack", "airstrike", "clash"]):
        tags.add("conflict")
    if has_any(text, ["市民", "民間人", "住民", "子ども", "避難民", "死亡", "負傷", "civilian", "residents", "children"]):
        tags.add("civilian_damage")
    if has_any(text, ["投資", "貿易", "輸出", "輸入", "企業", "ビジネス", "中小企業", "investment", "trade", "business"]):
        tags.add("business_economy")
    if has_any(text, ["法案", "法律", "規制", "制度", "政策", "税", "関税", "law", "regulation", "policy", "tax"]):
        tags.add("policy_law")
    if has_any(text, ["港湾", "物流", "コンテナ船", "船舶", "国境", "logistics", "port", "border"]):
        tags.add("logistics")
    return tags


def previous_day_relation(current: ArticleRecord, adopted: ArticleRecord) -> tuple[str, float, str]:
    if current.duplicate_key and current.duplicate_key == adopted.duplicate_key:
        return "duplicate_same_content", 1.0, "Q列重複キーが前日採用記事と一致"
    if current.url and adopted.url and current.url == adopted.url:
        return "duplicate_same_content", 1.0, "URLが前日採用記事と一致"

    current_title = normalize_comparable_text(title_text(current)).replace(" ", "")
    adopted_title = normalize_comparable_text(title_text(adopted)).replace(" ", "")
    full_sim = text_similarity(current.full_text, adopted.full_text)
    title_sim = text_similarity(title_text(current), title_text(adopted))
    similarity = max(full_sim, title_sim)

    if current_title and adopted_title and current_title == adopted_title:
        if has_update_signal(current.summary):
            return "continuation_update", 0.88, "前日採用記事と見出しは近いが、要約に更新・進展シグナルあり"
        return "duplicate_same_content", 0.95, "前日採用記事と見出し中核が一致"

    if similarity >= 0.82 and not has_update_signal(current.full_text):
        return "duplicate_same_content", similarity, "前日採用記事と本文・見出しの類似度が非常に高い"

    if similarity >= 0.32:
        current_focus = focus_tags_for_relation(current)
        adopted_focus = focus_tags_for_relation(adopted)
        new_focus = current_focus - adopted_focus
        if has_update_signal(current.full_text):
            return "continuation_update", similarity, "前日採用記事と同系統だが、進展・新情報の可能性あり"
        if new_focus:
            return "different_angle", similarity, "前日採用記事と同系統だが、焦点・観点が異なる可能性あり"
        return "related_but_different", similarity, "前日採用記事と関連するが、同一内容とは断定できない"

    if similarity >= 0.22:
        return "related_but_different", similarity, "前日採用記事と弱い関連あり"

    return "unrelated", similarity, ""


def best_previous_day_relation(
    current: ArticleRecord,
    adopted_by_date: dict[str, list[ArticleRecord]],
) -> tuple[str, str]:
    prev_key = previous_date_key(current.date_key)
    if not prev_key:
        return "none", ""

    candidates = adopted_by_date.get(prev_key, [])
    if not candidates:
        return "none", ""

    rank = {
        "duplicate_same_content": 5,
        "continuation_update": 4,
        "different_angle": 3,
        "related_but_different": 2,
        "unrelated": 1,
        "none": 0,
    }
    best_relation = "none"
    best_score = 0.0
    best_note = ""

    for adopted in candidates:
        relation, score, note = previous_day_relation(current, adopted)
        if rank.get(relation, 0) > rank.get(best_relation, 0) or (
            relation == best_relation and score > best_score
        ):
            best_relation = relation
            best_score = score
            best_note = note

    if best_relation == "unrelated":
        return "none", ""
    return best_relation, best_note


def attach_previous_day_relations(
    records: list[ArticleRecord],
    adopted_records: list[ArticleRecord],
) -> list[ArticleRecord]:
    adopted_by_date: dict[str, list[ArticleRecord]] = {}
    for record in adopted_records:
        if record.label != 1 or not record.date_key:
            continue
        adopted_by_date.setdefault(record.date_key, []).append(record)

    out: list[ArticleRecord] = []
    for record in records:
        relation, note = best_previous_day_relation(record, adopted_by_date)
        out.append(replace(record, prev_day_relation=relation, prev_day_note=note))
    return out


# ---------------------------------------------------------------------------
# scikit-learn feature extraction
# ---------------------------------------------------------------------------


def extract_headline_a(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_a for row in rows]


def extract_headline_final(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_final for row in rows]


def extract_headline_body(rows: list[ArticleRecord]) -> list[str]:
    return [row.headline_body for row in rows]


def extract_summary(rows: list[ArticleRecord]) -> list[str]:
    return [row.summary for row in rows]


def numeric_feature_dicts(rows: list[ArticleRecord]) -> list[dict[str, float | str]]:
    features: list[dict[str, float | str]] = []
    for row in rows:
        text = row.full_text
        item: dict[str, float | str] = {
            "has_direct_myanmar_relevance": float(has_direct_myanmar_relevance(text)),
            "priority_topic_count": float(len(row.priority_tags)),
            "has_quantitative_evidence": float(has_quantitative_evidence(text)),
            "has_url": float(bool(row.url)),
            "has_duplicate_key": float(bool(row.duplicate_key)),
            "headline_a_len_bin": min(len(row.headline_a) // 20, 10),
            "headline_final_len_bin": min(len(row.headline_final) // 20, 10),
            "headline_body_len_bin": min(len(row.headline_body) // 20, 10),
            "summary_len_bin": min(len(row.summary) // 80, 10),
            f"media={row.media[:80]}": 1.0,
            f"prev_day_relation={row.prev_day_relation}": 1.0,
        }
        for tag in row.priority_tags:
            item[f"priority_tag={tag}"] = 1.0
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
            "[selection-ml] archive labels contain one class; "
            f"using constant probability {positive_rate:.4f}"
        )
        return ConstantProbabilityModel(positive_rate)

    char_kwargs = dict(
        analyzer="char",
        ngram_range=(2, 5),
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
                "flags",
                Pipeline(
                    [
                        ("select", FunctionTransformer(numeric_feature_dicts, validate=False)),
                        ("dict", DictVectorizer()),
                    ]
                ),
            ),
        ]
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

    sample_weights = [row.sample_weight for row in rows]
    try:
        pipeline.fit(rows, labels, classifier__sample_weight=sample_weights)
    except ValueError as exc:
        print(
            "[selection-ml] model training could not build a vocabulary; "
            f"using archive positive rate {positive_rate:.4f}: {exc}",
            file=sys.stderr,
        )
        return ConstantProbabilityModel(positive_rate)
    return SklearnProbabilityModel(pipeline)


def apply_previous_day_probability_adjustment(probability: float, row: ArticleRecord) -> float:
    relation = row.prev_day_relation
    adjusted = probability
    if relation == "duplicate_same_content":
        adjusted = min(probability * PREV_DAY_DUPLICATE_MULTIPLIER, PREV_DAY_DUPLICATE_CAP)
    elif relation == "continuation_update":
        adjusted = min(1.0, probability + PREV_DAY_CONTINUATION_BONUS)
    elif relation == "different_angle":
        adjusted = min(1.0, probability + PREV_DAY_DIFFERENT_ANGLE_BONUS)
    elif relation == "related_but_different":
        adjusted = min(1.0, probability + PREV_DAY_RELATED_BONUS)
    return max(0.0, min(1.0, adjusted))


def score_reason(
    score: int,
    positive_count: int,
    training_count: int,
    row: ArticleRecord,
) -> str:
    if score >= 70:
        band = "過去の手動採用記事に強く類似"
    elif score >= 40:
        band = "過去の採用傾向に一部類似"
    else:
        band = "過去の手動採用傾向との類似度は低め"

    notes = [band]
    if row.priority_tags:
        notes.append("重要トピック=" + ",".join(row.priority_tags[:4]))
    if row.prev_day_relation == "duplicate_same_content":
        notes.append("前日採用済み同一内容の可能性が高いため抑制")
    elif row.prev_day_relation == "continuation_update":
        notes.append("前日採用記事の続編・進展として加点")
    elif row.prev_day_relation == "different_angle":
        notes.append("前日採用記事と同系統だが別観点として加点")
    elif row.prev_day_relation == "related_but_different":
        notes.append("前日採用記事と関連する別事象として微加点")
    if row.prev_day_note:
        notes.append(row.prev_day_note)

    notes.append(f"教師データ {training_count}件 / 採用 {positive_count}件")
    return "（".join([notes[0], " / ".join(notes[1:]) + "）"] if len(notes) > 1 else notes)[:260]


def build_output_values(
    rows: list[ArticleRecord],
    row_count: int,
    probabilities: list[float],
    positive_count: int,
    training_count: int,
) -> list[list[Any]]:
    by_row_index: dict[int, list[Any]] = {}
    for row, probability in zip(rows, probabilities, strict=True):
        adjusted = apply_previous_day_probability_adjustment(probability, row)
        score = max(0, min(100, round(adjusted * 100)))
        by_row_index[row.row_index] = [
            score,
            score_reason(score, positive_count, training_count, row),
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
    current_archive_adopted = load_current_spreadsheet_adopted_archive_records(
        sheets,
        spreadsheet_id,
        target_sheet,
    )
    adopted_records = dedupe_adopted_records(
        [record for record in archive_records if record.label == 1] + current_archive_adopted
    )

    training_rows = attach_previous_day_relations(archive_records, adopted_records)
    training_rows = attach_training_weights(training_rows)

    positive_count = sum(int(row.label or 0) for row in training_rows)
    recent_weighted_count = sum(1 for row in training_rows if row.sample_weight > 1.0)
    priority_tag_count = sum(1 for row in training_rows if row.priority_tags)
    prev_day_related_count = sum(1 for row in training_rows if row.prev_day_relation != "none")

    print(
        f"[selection-ml] archive_files={archive_file_count} "
        f"training_rows={len(training_rows)} positives={positive_count} "
        f"recent_weighted={recent_weighted_count} priority_tagged={priority_tag_count} "
        f"prev_day_related={prev_day_related_count}"
    )

    model = train_model(training_rows)

    current_rows, row_count = load_current_rows(sheets, spreadsheet_id, target_sheet)
    current_rows = attach_previous_day_relations(current_rows, adopted_records)
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
        f"[selection-ml] sheet={target_sheet} "
        f"sheet_rows={row_count} scored_rows={len(current_rows)}"
    )


if __name__ == "__main__":
    run()
