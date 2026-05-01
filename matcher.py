from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import pandas as pd


TOKEN_RE = re.compile(r"[a-z0-9]+(?:\.[a-z0-9]+)?")
NUMBER_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?%?")
MONTHS = {
    "jan": "01",
    "january": "01",
    "feb": "02",
    "february": "02",
    "mar": "03",
    "march": "03",
    "apr": "04",
    "april": "04",
    "may": "05",
    "jun": "06",
    "june": "06",
    "jul": "07",
    "july": "07",
    "aug": "08",
    "august": "08",
    "sep": "09",
    "sept": "09",
    "september": "09",
    "oct": "10",
    "october": "10",
    "nov": "11",
    "november": "11",
    "dec": "12",
    "december": "12",
}
EXCLUDED_COLUMN_HINTS = {
    "fund id",
    "fund name",
    "period",
    "fund date",
    "funddate",
    "fund_date",
    "fund-date",
    "quarter",
    "found",
    "found y n",
    "gics y n",
    "geography y n",
    "asset type y n",
    "market cap y n",
    "holdings y n",
}
PREFERRED_DATA_COLUMNS = {
    "label",
    "manager provided label",
    "holding name",
    "long",
    "long pct",
    "long percent",
    "long %",
    "short",
    "short pct",
    "short percent",
    "short %",
    "net",
    "net pct",
    "net percent",
    "net %",
    "of nav",
    "% of nav",
    "base value usd",
}
LABEL_ALIASES = (
    "label",
    "manager provided label",
    "holding name",
    "holding_name",
    "investment name",
    "investment_name",
    "issuer",
    "security",
    "company",
    "asset",
    "value",
)
PERIOD_ALIASES = ("period",)
QUARTER_ALIASES = ("quarter",)
DATE_ALIASES = (
    "fund date",
    "fund-date",
    "fund_date",
    "funddate",
    "fundate",
    "report_as_of_date",
)


@dataclass(frozen=True)
class SourceInfo:
    period: str = ""
    quarter: str = ""
    date: str = ""


@dataclass(frozen=True)
class ExtractedTable:
    name: str
    dataframe: pd.DataFrame
    source_file: str = ""
    source_period: str = ""
    source_quarter: str = ""
    source_date: str = ""


@dataclass(frozen=True)
class TableIndex:
    name: str
    source_file: str
    text: str
    tokens: set[str]


@dataclass(frozen=True)
class RowIndex:
    table_name: str
    source_file: str
    row_number: int
    text: str
    tokens: set[str]
    numbers: tuple[float, ...]
    numbers_by_role: dict[str, tuple[float, ...]]
    source_period: str = ""
    source_quarter: str = ""
    source_date: str = ""


@dataclass(frozen=True)
class JsonPoint:
    source_file: str
    table_name: str
    row_number: int
    category: str
    row_label: str
    row_label_key: str
    role: str
    value: str
    normalized_value: str
    numeric_values: tuple[float, ...]
    source_period: str = ""
    source_quarter: str = ""
    source_date: str = ""


def load_json(uploaded_file: Any) -> Any:
    raw = uploaded_file.getvalue()
    if isinstance(raw, str):
        text = raw
    else:
        text = raw.decode("utf-8-sig")
    return json.loads(text)


def load_excel(uploaded_file: Any) -> dict[str, pd.DataFrame]:
    return pd.read_excel(uploaded_file, sheet_name=None, dtype=object)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    cleaned = df.copy()
    cleaned = cleaned.dropna(how="all")
    cleaned = cleaned.dropna(axis=1, how="all")
    return cleaned.fillna("")


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict, tuple, set)):
        return False
    try:
        missing = pd.isna(value)
    except TypeError:
        return False
    return bool(missing) if isinstance(missing, bool) else False


def display_value(value: Any) -> str:
    if is_empty(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_value(value: Any) -> str:
    text = display_value(value).lower()
    text = text.replace("_", " ")
    text = re.sub(r"[^a-z0-9.]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize(value: Any) -> list[str]:
    return TOKEN_RE.findall(normalize_value(value))


def expanded_tokens(value: Any) -> set[str]:
    tokens = set(tokenize(value))
    expanded = set(tokens)
    for token in tokens:
        if len(token) > 4 and token.endswith("s"):
            expanded.add(token[:-1])
    return expanded


def canonical_name(value: Any) -> str:
    text = normalize_value(value).replace(".", " ")
    return re.sub(r"\s+", " ", text).strip()


def label_key(value: Any) -> str:
    return canonical_name(value)


def normalize_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.10f}".rstrip("0").rstrip(".")


def numeric_values(value: Any) -> tuple[float, ...]:
    text = display_value(value)
    values: set[float] = set()

    for match in NUMBER_RE.findall(text):
        has_percent = match.endswith("%")
        raw = match[:-1] if has_percent else match
        raw = raw.replace(",", "")
        try:
            number = float(raw)
        except ValueError:
            continue

        values.add(number)
        if has_percent:
            values.add(number / 100.0)
        elif abs(number) <= 1:
            values.add(number * 100.0)
        elif 1 < abs(number) <= 100:
            values.add(number / 100.0)

    return tuple(sorted(values))


def numeric_variants(value: Any) -> set[str]:
    return {normalize_number(number) for number in numeric_values(value)}


def numbers_match(left: float, right: float, tolerance: float) -> bool:
    allowed = max(tolerance, tolerance * max(abs(left), abs(right), 1.0))
    return abs(left - right) <= allowed


def any_number_matches(left_values: Iterable[float], right_values: Iterable[float], tolerance: float) -> bool:
    return any(numbers_match(left, right, tolerance) for left in left_values for right in right_values)


def is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list, tuple))


def dict_of_equal_lists(node: dict[str, Any]) -> bool:
    if not node:
        return False
    list_lengths = [len(value) for value in node.values() if isinstance(value, list)]
    if len(list_lengths) != len(node):
        return False
    return len(set(list_lengths)) == 1


def table_from_node(node: Any) -> pd.DataFrame | None:
    if isinstance(node, list):
        if not node:
            return None
        if all(isinstance(item, dict) for item in node):
            return pd.json_normalize(node)
        if all(isinstance(item, list) for item in node):
            return pd.DataFrame(node)
        if all(is_scalar(item) for item in node):
            return pd.DataFrame({"value": node})

    if isinstance(node, dict):
        if not node:
            return None
        if dict_of_equal_lists(node):
            return pd.DataFrame(node)
        if all(is_scalar(value) for value in node.values()):
            return pd.DataFrame([node])

    return None


def normalize_period(value: Any) -> str:
    text = display_value(value)
    if not text:
        return ""

    direct = re.search(r"\b(20\d{2})[-_/](0?[1-9]|1[0-2])\b", text)
    if direct:
        return f"{direct.group(1)}-{int(direct.group(2)):02d}"

    parsed = pd.to_datetime(text, errors="coerce")
    if not pd.isna(parsed):
        return parsed.strftime("%Y-%m")

    month_text = text.lower()
    for month, month_number in MONTHS.items():
        if month in month_text:
            year = re.search(r"\b(20\d{2})\b", month_text)
            if year:
                return f"{year.group(1)}-{month_number}"

    return ""


def normalize_date(value: Any) -> str:
    text = display_value(value)
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def normalize_quarter(value: Any) -> str:
    text = display_value(value).replace("_", " ").upper()
    match = re.search(r"\bQ([1-4])\b", text)
    if match:
        return f"Q{match.group(1)}"

    match = re.search(r"\b([1-4])Q(?:\s*20?\d{2})?\b", text)
    if match:
        return f"Q{match.group(1)}"

    quarter_words = {
        "FIRST": "Q1",
        "SECOND": "Q2",
        "THIRD": "Q3",
        "FOURTH": "Q4",
    }
    for word, quarter in quarter_words.items():
        if re.search(rf"\b{word}\s+QUARTER\b", text):
            return quarter
    return ""


def quarter_from_period(period: str) -> str:
    if not period:
        return ""
    try:
        month = int(period.split("-")[1])
    except (IndexError, ValueError):
        return ""
    return f"Q{((month - 1) // 3) + 1}"


def source_info_from_json(data: Any, source_file: str) -> SourceInfo:
    info_node: dict[str, Any] = {}
    if isinstance(data, dict):
        payload = data.get("data", data)
        if isinstance(payload, dict):
            candidate = payload.get("section_1_document_identification")
            if isinstance(candidate, dict):
                info_node = candidate

    period = ""
    for key in ("report_period", "period", "file_name"):
        period = normalize_period(info_node.get(key, ""))
        if period:
            break

    if not period:
        period = normalize_period(source_file)

    date = normalize_date(info_node.get("report_as_of_date", ""))
    quarter = (
        normalize_quarter(info_node.get("quarter", ""))
        or normalize_quarter(info_node.get("report_period", ""))
        or normalize_quarter(info_node.get("file_name", ""))
        or normalize_quarter(source_file)
        or quarter_from_period(period)
    )
    return SourceInfo(period=period, quarter=quarter, date=date)


def extract_json_tables(data: Any, source_file: str = "") -> list[ExtractedTable]:
    tables: list[ExtractedTable] = []
    seen_names: dict[str, int] = {}
    source_info = source_info_from_json(data, source_file)

    def add_table(path: list[str], df: pd.DataFrame) -> None:
        cleaned = clean_dataframe(df)
        if cleaned.empty:
            return

        base_name = ".".join(path) if path else "root"
        seen_names[base_name] = seen_names.get(base_name, 0) + 1
        name = base_name if seen_names[base_name] == 1 else f"{base_name}_{seen_names[base_name]}"
        tables.append(
            ExtractedTable(
                name=name,
                dataframe=cleaned,
                source_file=source_file,
                source_period=source_info.period,
                source_quarter=source_info.quarter,
                source_date=source_info.date,
            )
        )

    def walk(node: Any, path: list[str]) -> None:
        table = table_from_node(node)
        if table is not None:
            add_table(path, table)

        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, (dict, list)):
                    walk(value, path + [str(key)])
        elif isinstance(node, list):
            if all(isinstance(item, dict) for item in node):
                for row_index, row in enumerate(node):
                    for key, value in row.items():
                        if isinstance(value, (dict, list)):
                            walk(value, path + [str(row_index), str(key)])
            else:
                for index, value in enumerate(node):
                    if isinstance(value, (dict, list)):
                        walk(value, path + [str(index)])

    walk(data, [])
    return tables


def build_table_index(table: ExtractedTable) -> TableIndex:
    parts: list[str] = []
    token_parts: list[str] = []
    for column in table.dataframe.columns:
        normalized_column = normalize_value(column)
        if normalized_column:
            parts.append(normalized_column)
            token_parts.append(normalized_column)

    for value in table.dataframe.to_numpy().ravel():
        normalized_value = normalize_value(value)
        if normalized_value:
            parts.append(normalized_value)
            token_parts.append(normalized_value)
        token_parts.extend(numeric_variants(value))

    text = " ".join(parts)
    tokens = {token for part in token_parts for token in expanded_tokens(part)}
    return TableIndex(name=table.name, source_file=table.source_file, text=f" {text} ", tokens=tokens)


def column_role(column: Any) -> str:
    name = canonical_name(column)
    if name in {
        "label",
        "manager provided label",
        "holding name",
        "investment name",
        "issuer",
        "security",
        "company",
        "asset",
        "value",
    }:
        return "label"
    if "long" in name:
        return "long"
    if "short" in name:
        return "short"
    if name in {"of nav", "pct of nav", "percent of nav"} or name.endswith("of nav"):
        return "long"
    if name == "net" or "net pct" in name or "net percent" in name or "net exposure" in name:
        return "net"
    if (
        "nav" in name
        or "asset value" in name
        or "base value" in name
        or "market value" in name
        or "fair value" in name
        or "remaining cost" in name
        or "invested capital" in name
        or "realization" in name
        or "realisation" in name
        or "contribution" in name
        or "liabilities" in name
        or "assets" in name
        or "capital" in name
        or name == "extracted value"
    ):
        return "value"
    return ""


def build_row_indexes(tables: Iterable[ExtractedTable]) -> list[RowIndex]:
    rows: list[RowIndex] = []

    for table in tables:
        df = clean_dataframe(table.dataframe)
        for row_position, (_, row) in enumerate(df.iterrows(), start=1):
            text_parts: list[str] = [table.name]
            numbers: list[float] = []
            numbers_by_role: defaultdict[str, list[float]] = defaultdict(list)

            for column, value in row.items():
                normalized_column = normalize_value(column)
                normalized_value = normalize_value(value)
                if normalized_column:
                    text_parts.append(normalized_column)
                if normalized_value:
                    text_parts.append(normalized_value)

                role = column_role(column)
                current_numbers = list(numeric_values(value))
                numbers.extend(current_numbers)
                if role:
                    numbers_by_role[role].extend(current_numbers)

            text = " ".join(text_parts)
            tokens = {token for part in text_parts for token in expanded_tokens(part)}
            rows.append(
                RowIndex(
                    table_name=table.name,
                    source_file=table.source_file,
                    row_number=row_position,
                    text=f" {text} ",
                    tokens=tokens,
                    numbers=tuple(numbers),
                    numbers_by_role={
                        role: tuple(values) for role, values in numbers_by_role.items()
                    },
                    source_period=table.source_period,
                    source_quarter=table.source_quarter,
                    source_date=table.source_date,
                )
            )

    return rows


def value_is_present(value: Any, table_index: TableIndex, match_mode: str, numeric_tolerance: float = 0.001) -> bool:
    normalized = normalize_value(value)
    if not normalized:
        return False

    tokens = expanded_tokens(normalized)
    phrase_hit = f" {normalized} " in table_index.text
    if match_mode == "Exact phrase":
        return phrase_hit

    if tokens and (phrase_hit or tokens.issubset(table_index.tokens)):
        return True

    variants = numeric_variants(value)
    if not variants:
        return False

    return any(variant in table_index.tokens for variant in variants)


def available_columns(sheets: dict[str, pd.DataFrame], selected_sheets: Iterable[str] | None = None) -> list[str]:
    selected = set(selected_sheets or sheets.keys())
    columns: list[str] = []
    seen: set[str] = set()

    for sheet_name, frame in sheets.items():
        if sheet_name not in selected:
            continue
        for column in frame.columns:
            label = str(column)
            if label not in seen:
                columns.append(label)
                seen.add(label)

    return columns


def default_score_columns(columns: Iterable[str]) -> list[str]:
    preferred: list[str] = []
    fallback: list[str] = []

    for column in columns:
        canonical = canonical_name(column)
        if canonical in PREFERRED_DATA_COLUMNS:
            preferred.append(column)
        elif canonical not in EXCLUDED_COLUMN_HINTS and not canonical.endswith(" y n"):
            fallback.append(column)

    return preferred or fallback


def filter_column(frame: pd.DataFrame, aliases: Iterable[str]) -> str | None:
    targets = {canonical_name(alias) for alias in aliases}
    for column in frame.columns:
        if canonical_name(column) in targets:
            return column
    return None


def filter_values(
    sheets: dict[str, pd.DataFrame],
    selected_sheets: Iterable[str] | None,
    aliases: Iterable[str],
) -> list[str]:
    selected = set(selected_sheets or sheets.keys())
    values: set[str] = set()

    for sheet_name, frame in sheets.items():
        if sheet_name not in selected:
            continue
        column = filter_column(frame, aliases)
        if column is None:
            continue
        for value in frame[column].dropna().tolist():
            shown = display_value(value)
            if shown:
                values.add(shown)

    return sorted(values)


def apply_filters(df: pd.DataFrame, filters: dict[str, set[str]]) -> pd.DataFrame:
    filtered = df
    alias_map = {
        "period": PERIOD_ALIASES,
        "quarter": QUARTER_ALIASES,
        "fund_date": DATE_ALIASES,
    }

    for key, selected_values in filters.items():
        if not selected_values:
            continue
        column = filter_column(filtered, alias_map[key])
        if column is None:
            continue
        mask = filtered[column].map(display_value).isin(selected_values)
        filtered = filtered.loc[mask]

    return filtered


def first_row_value(row: pd.Series, aliases: Iterable[str]) -> str:
    for alias in aliases:
        for column in row.index:
            if canonical_name(column) == canonical_name(alias):
                shown = display_value(row[column])
                if shown:
                    return shown
    return ""


def label_from_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) >= 2 and parts[-1] in {"extracted_value", "source_location", "found", "explanation"}:
        return parts[-2].replace("_", " ")
    return parts[-1].replace("_", " ") if parts else ""


def build_json_points(tables: Iterable[ExtractedTable]) -> list[JsonPoint]:
    points: list[JsonPoint] = []

    for table in tables:
        category = table_category(table.name)
        df = clean_dataframe(table.dataframe)
        for row_position, (_, row) in enumerate(df.iterrows(), start=1):
            row_label = first_row_value(row, LABEL_ALIASES) or label_from_table_name(table.name)
            row_label_key = label_key(row_label)

            for column, value in row.items():
                role = column_role(column)
                if not role or is_empty(value):
                    continue

                shown = display_value(value)
                normalized = normalize_value(value)
                if not normalized:
                    continue

                points.append(
                    JsonPoint(
                        source_file=table.source_file,
                        table_name=table.name,
                        row_number=row_position,
                        category=category,
                        row_label=row_label,
                        row_label_key=row_label_key,
                        role=role,
                        value=shown,
                        normalized_value=normalized,
                        numeric_values=numeric_values(value),
                        source_period=table.source_period,
                        source_quarter=table.source_quarter,
                        source_date=table.source_date,
                    )
                )

    return points


def collect_excel_points(
    sheets: dict[str, pd.DataFrame],
    selected_sheets: Iterable[str] | None = None,
    selected_columns: Iterable[str] | None = None,
    filters: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    selected_sheet_set = set(selected_sheets or sheets.keys())
    selected_column_set = set(selected_columns or available_columns(sheets, selected_sheet_set))

    for sheet_name, frame in sheets.items():
        if sheet_name not in selected_sheet_set:
            continue
        df = clean_dataframe(frame)
        if filters:
            df = apply_filters(df, filters)

        for row_position, (_, row) in enumerate(df.iterrows(), start=2):
            row_label = first_row_value(row, LABEL_ALIASES)
            fund_date = normalize_date(first_row_value(row, DATE_ALIASES))
            period = normalize_period(first_row_value(row, PERIOD_ALIASES)) or normalize_period(fund_date)
            quarter = normalize_quarter(first_row_value(row, QUARTER_ALIASES))

            for column in df.columns:
                if str(column) not in selected_column_set:
                    continue
                raw_value = row[column]
                normalized = normalize_value(raw_value)
                if not normalized:
                    continue

                points.append(
                    {
                        "baseline_sheet": sheet_name,
                        "excel_row": row_position,
                        "excel_column": str(column),
                        "baseline_value": display_value(raw_value),
                        "normalized_value": normalized,
                        "baseline_category": sheet_category(sheet_name),
                        "row_label": row_label,
                        "row_label_key": label_key(row_label),
                        "baseline_period": period,
                        "baseline_quarter": quarter,
                        "baseline_date": fund_date,
                        "column_role": column_role(column),
                        "numeric_values": numeric_values(raw_value),
                    }
                )

    return points


def text_matches(value: Any, row_index: RowIndex, match_mode: str) -> bool:
    normalized = normalize_value(value)
    if not normalized:
        return False

    phrase_hit = f" {normalized} " in row_index.text
    if match_mode == "Exact phrase":
        return phrase_hit

    tokens = expanded_tokens(normalized)
    return bool(tokens and (phrase_hit or tokens.issubset(row_index.tokens)))


def sheet_category(sheet_name: str) -> str:
    canonical = canonical_name(sheet_name)
    if canonical.endswith(" mg"):
        canonical = canonical[:-3]
    if "schedule" in canonical or "investment" in canonical:
        return "holdings"
    if "gics" in canonical:
        return "gics"
    if "geography" in canonical:
        return "geography"
    if "asset type" in canonical:
        return "asset_type"
    if "market cap" in canonical:
        return "market_cap"
    if "holding" in canonical:
        return "holdings"
    if "balance sheet" in canonical or "net assets" in canonical:
        return "balance_sheet"
    if canonical == "fund":
        return "fund"
    return ""


def table_category(table_name: str) -> str:
    canonical = canonical_name(table_name)
    if "document identification" in canonical:
        return "fund"
    if "section 2 schedule of investments" in canonical or "investments extracted value" in canonical:
        return "holdings"
    if "section 3 balance sheet" in canonical:
        return "balance_sheet"
    if "gics" in canonical:
        return "gics"
    if "geography" in canonical:
        return "geography"
    if "asset type" in canonical:
        return "asset_type"
    if "market cap" in canonical:
        return "market_cap"
    if "top holding" in canonical or "holding" in canonical or "investment" in canonical:
        return "holdings"
    return ""


def categories_match(sheet_name: str, table_name: str) -> bool:
    source = sheet_category(sheet_name)
    target = table_category(table_name)
    return not source or not target or source == target


def periods_match(point: dict[str, Any], row_index: RowIndex) -> bool:
    point_period = point.get("baseline_period", "")
    point_date_period = normalize_period(point.get("baseline_date", ""))
    expected_period = point_period or point_date_period

    if expected_period and row_index.source_period:
        return expected_period == row_index.source_period

    point_quarter = point.get("baseline_quarter", "")
    if point_quarter and row_index.source_quarter:
        return point_quarter == row_index.source_quarter

    return True


def role_numbers(row_index: RowIndex, role: str) -> tuple[float, ...]:
    if role and role in row_index.numbers_by_role:
        return row_index.numbers_by_role[role]
    if role == "long":
        return row_index.numbers_by_role.get("long", ()) or row_index.numbers_by_role.get("net", ())
    if role == "net":
        return row_index.numbers_by_role.get("net", ()) or row_index.numbers_by_role.get("long", ())
    return row_index.numbers


def row_matches_point(
    point: dict[str, Any],
    row_index: RowIndex,
    match_mode: str,
    numeric_tolerance: float,
    require_row_context: bool,
    category_matching: bool,
) -> tuple[bool, str]:
    if category_matching and not categories_match(point["baseline_sheet"], row_index.table_name):
        return False, ""

    if not periods_match(point, row_index):
        return False, ""

    point_numbers = point.get("numeric_values", ())
    role = point.get("column_role", "")

    if point_numbers:
        candidate_numbers = role_numbers(row_index, role)
        row_label = point.get("row_label", "")
        label_matches = not row_label or text_matches(row_label, row_index, match_mode)

        if role == "short" and not candidate_numbers and any(numbers_match(number, 0.0, numeric_tolerance) for number in point_numbers):
            if not require_row_context or label_matches:
                return True, "implicit zero short row match"

        if not candidate_numbers or not any_number_matches(point_numbers, candidate_numbers, numeric_tolerance):
            return False, ""

        if require_row_context and row_label and role != "label" and not label_matches:
            return False, ""

        return True, "numeric row match"

    if text_matches(point["baseline_value"], row_index, match_mode):
        return True, "text row match"

    return False, ""


def loose_match_point(
    point: dict[str, Any],
    indexes: Iterable[TableIndex],
    match_mode: str,
    numeric_tolerance: float,
) -> tuple[str, str]:
    for table_index in indexes:
        if value_is_present(point["baseline_value"], table_index, match_mode, numeric_tolerance):
            return table_index.source_file, table_index.name
    return "", ""


def is_extraction_table(table: ExtractedTable) -> bool:
    name = table.name
    if name.startswith("data."):
        return True
    if name.startswith(("section_1_", "section_2_", "section_3_", "section_4_")):
        return True
    return False


def point_is_in_source_scope(point: dict[str, Any], tables: Iterable[ExtractedTable]) -> bool:
    source_periods = {table.source_period for table in tables if table.source_period}
    source_quarters = {table.source_quarter for table in tables if table.source_quarter}
    source_dates = {table.source_date for table in tables if table.source_date}

    if not source_periods and not source_quarters and not source_dates:
        return True

    point_period = point.get("baseline_period", "")
    point_quarter = point.get("baseline_quarter", "")
    point_date = point.get("baseline_date", "")

    if point_period and source_periods:
        return point_period in source_periods
    if point_date and source_dates:
        return point_date in source_dates
    if point_date and source_periods:
        return normalize_period(point_date) in source_periods
    if point_quarter and source_quarters:
        return point_quarter in source_quarters

    return True


def json_point_period_matches(point: dict[str, Any], json_point: JsonPoint) -> bool:
    point_period = point.get("baseline_period", "")
    point_date = point.get("baseline_date", "")
    point_quarter = point.get("baseline_quarter", "")

    if point_period and json_point.source_period:
        return point_period == json_point.source_period
    if point_date and json_point.source_date:
        return point_date == json_point.source_date
    if point_date and json_point.source_period:
        return normalize_period(point_date) == json_point.source_period
    if point_quarter and json_point.source_quarter:
        return point_quarter == json_point.source_quarter
    return True


def point_value_matches(point: dict[str, Any], json_point: JsonPoint, numeric_tolerance: float) -> bool:
    point_numbers = point.get("numeric_values", ())
    if point_numbers or json_point.numeric_values:
        return bool(
            point_numbers
            and json_point.numeric_values
            and any_number_matches(point_numbers, json_point.numeric_values, numeric_tolerance)
        )
    return label_key(point.get("baseline_value", "")) == label_key(json_point.value)


def structured_point_matches(
    point: dict[str, Any],
    json_point: JsonPoint,
    numeric_tolerance: float,
    category_matching: bool,
    require_row_context: bool,
) -> tuple[bool, str]:
    if category_matching:
        point_category = point.get("baseline_category", "")
        if not point_category or not json_point.category or point_category != json_point.category:
            return False, ""

    if not json_point_period_matches(point, json_point):
        return False, ""

    point_role = point.get("column_role", "")
    if point_role != json_point.role:
        return False, ""

    point_label_key = point.get("row_label_key", "")
    if point_role == "label":
        if label_key(point.get("baseline_value", "")) != json_point.row_label_key:
            return False, ""
    elif require_row_context:
        if not point_label_key or not json_point.row_label_key or point_label_key != json_point.row_label_key:
            return False, ""

    if not point_value_matches(point, json_point, numeric_tolerance):
        return False, ""

    return True, "structured exact match"


def value_point_matches(
    point: dict[str, Any],
    json_point: JsonPoint,
    numeric_tolerance: float,
    category_matching: bool,
) -> tuple[bool, str]:
    if category_matching:
        point_category = point.get("baseline_category", "")
        if not point_category or not json_point.category or point_category != json_point.category:
            return False, ""

    if not json_point_period_matches(point, json_point):
        return False, ""

    if point.get("column_role", "") != json_point.role:
        return False, ""

    if not point_value_matches(point, json_point, numeric_tolerance):
        return False, ""

    return True, "value match"


def json_point_is_in_comparison_scope(
    json_point: JsonPoint,
    excel_points: Iterable[dict[str, Any]],
    category_matching: bool,
) -> bool:
    points = list(excel_points)
    if not points:
        return False

    roles = {point.get("column_role", "") for point in points if point.get("column_role", "")}
    if json_point.role not in roles:
        return False

    if category_matching:
        categories = {
            point.get("baseline_category", "")
            for point in points
            if point.get("baseline_category", "")
        }
        if json_point.category not in categories:
            return False

    periods = {point.get("baseline_period", "") for point in points if point.get("baseline_period", "")}
    dates = {point.get("baseline_date", "") for point in points if point.get("baseline_date", "")}
    quarters = {point.get("baseline_quarter", "") for point in points if point.get("baseline_quarter", "")}

    if json_point.source_period and periods:
        return json_point.source_period in periods
    if json_point.source_date and dates:
        return json_point.source_date in dates
    if json_point.source_period and dates:
        return any(normalize_period(date) == json_point.source_period for date in dates)
    if json_point.source_quarter and quarters:
        return json_point.source_quarter in quarters

    return True


def compare_baseline_to_json(
    baseline_sheets: dict[str, pd.DataFrame],
    json_tables: Iterable[ExtractedTable],
    selected_table_names: Iterable[str] | None = None,
    selected_json_files: Iterable[str] | None = None,
    selected_sheets: Iterable[str] | None = None,
    selected_columns: Iterable[str] | None = None,
    filters: dict[str, set[str]] | None = None,
    match_mode: str = "Word subset",
    require_row_context: bool = True,
    category_matching: bool = True,
    numeric_tolerance: float = 0.001,
    scope_baseline_to_json_periods: bool = True,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    selected_names = set(selected_table_names or [])
    selected_files = set(selected_json_files or [])
    selected_tables = [
        table
        for table in json_tables
        if (not selected_names or table.name in selected_names)
        and (not selected_files or table.source_file in selected_files)
    ]
    table_indexes = [build_table_index(table) for table in selected_tables]
    json_points = build_json_points(selected_tables)
    rows: list[dict[str, Any]] = []

    points = collect_excel_points(
        baseline_sheets,
        selected_sheets=selected_sheets,
        selected_columns=selected_columns,
        filters=filters,
    )
    if scope_baseline_to_json_periods:
        points = [point for point in points if point_is_in_source_scope(point, selected_tables)]
    comparable_json_points = [
        json_point
        for json_point in json_points
        if json_point_is_in_comparison_scope(json_point, points, category_matching)
    ]

    for point in points:
        matched_table = ""
        matched_file = ""
        matched_row = ""
        match_reason = ""
        value_matched_table = ""
        value_matched_file = ""
        value_matched_row = ""

        if require_row_context or category_matching:
            for json_point in comparable_json_points:
                matched, reason = structured_point_matches(
                    point,
                    json_point,
                    numeric_tolerance=numeric_tolerance,
                    category_matching=category_matching,
                    require_row_context=require_row_context,
                )
                if matched:
                    matched_table = json_point.table_name
                    matched_file = json_point.source_file
                    matched_row = str(json_point.row_number)
                    match_reason = reason
                    break
        else:
            matched_file, matched_table = loose_match_point(
                point,
                table_indexes,
                match_mode=match_mode,
                numeric_tolerance=numeric_tolerance,
            )
            match_reason = "loose table scan" if matched_table else ""

        for json_point in comparable_json_points:
            value_matched, _ = value_point_matches(
                point,
                json_point,
                numeric_tolerance=numeric_tolerance,
                category_matching=category_matching,
            )
            if value_matched:
                value_matched_table = json_point.table_name
                value_matched_file = json_point.source_file
                value_matched_row = str(json_point.row_number)
                break

        rows.append(
            {
                **{key: value for key, value in point.items() if key != "numeric_values"},
                "present_in_json": bool(matched_table),
                "matched_json_file": matched_file,
                "matched_json_table": matched_table,
                "matched_json_row": matched_row,
                "match_reason": match_reason,
                "value_present_in_json": bool(value_matched_table),
                "value_matched_json_file": value_matched_file,
                "value_matched_json_table": value_matched_table,
                "value_matched_json_row": value_matched_row,
            }
        )

    results = pd.DataFrame(rows)
    total_points = len(results)
    matched_points = int(results["present_in_json"].sum()) if total_points else 0
    value_matched_points = int(results["value_present_in_json"].sum()) if total_points else 0
    accuracy = (matched_points / total_points * 100.0) if total_points else 0.0
    value_accuracy = (value_matched_points / total_points * 100.0) if total_points else 0.0
    json_data_points = len(comparable_json_points)

    summary = {
        "total_points": total_points,
        "matched_points": matched_points,
        "missing_points": total_points - matched_points,
        "accuracy": accuracy,
        "value_matched_points": value_matched_points,
        "value_missing_points": total_points - value_matched_points,
        "value_accuracy": value_accuracy,
        "json_tables": len(selected_tables),
        "json_files": len({table.source_file for table in selected_tables if table.source_file}),
        "json_data_points": json_data_points,
        "data_point_delta": json_data_points - total_points,
        "data_point_counts_equal": int(json_data_points == total_points),
    }
    return results, summary


def summarize_by_json_file(
    baseline_sheets: dict[str, pd.DataFrame],
    json_tables: Iterable[ExtractedTable],
    selected_json_files: Iterable[str],
    selected_table_names: Iterable[str] | None = None,
    selected_sheets: Iterable[str] | None = None,
    selected_columns: Iterable[str] | None = None,
    filters: dict[str, set[str]] | None = None,
    match_mode: str = "Word subset",
    require_row_context: bool = True,
    category_matching: bool = True,
    numeric_tolerance: float = 0.001,
    scope_baseline_to_json_periods: bool = True,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for source_file in selected_json_files:
        file_tables = [table for table in json_tables if table.source_file == source_file]
        results, summary = compare_baseline_to_json(
            baseline_sheets=baseline_sheets,
            json_tables=json_tables,
            selected_table_names=selected_table_names,
            selected_json_files=[source_file],
            selected_sheets=selected_sheets,
            selected_columns=selected_columns,
            filters=filters,
            match_mode=match_mode,
            require_row_context=require_row_context,
            category_matching=category_matching,
            numeric_tolerance=numeric_tolerance,
            scope_baseline_to_json_periods=scope_baseline_to_json_periods,
        )
        json_periods = sorted({table.source_period for table in file_tables if table.source_period})
        json_quarters = sorted({table.source_quarter for table in file_tables if table.source_quarter})
        json_dates = sorted({table.source_date for table in file_tables if table.source_date})
        excel_periods = sorted(results["baseline_period"].dropna().astype(str).loc[lambda values: values != ""].unique()) if not results.empty else []
        excel_quarters = sorted(results["baseline_quarter"].dropna().astype(str).loc[lambda values: values != ""].unique()) if not results.empty else []
        excel_dates = sorted(results["baseline_date"].dropna().astype(str).loc[lambda values: values != ""].unique()) if not results.empty else []
        rows.append(
            {
                "json_file": source_file,
                "json_period": ", ".join(json_periods),
                "json_quarter": ", ".join(json_quarters),
                "json_date": ", ".join(json_dates),
                "excel_period": ", ".join(excel_periods),
                "excel_quarter": ", ".join(excel_quarters),
                "excel_date": ", ".join(excel_dates),
                "accuracy": summary["accuracy"],
                "matched_points": summary["matched_points"],
                "total_points": summary["total_points"],
                "missing_points": summary["missing_points"],
                "value_accuracy": summary["value_accuracy"],
                "value_matched_points": summary["value_matched_points"],
                "tables_searched": summary["json_tables"],
                "json_data_points": summary["json_data_points"],
                "data_point_delta": summary["data_point_delta"],
                "left_right_equal": bool(summary["data_point_counts_equal"]),
            }
        )

    return pd.DataFrame(rows)
