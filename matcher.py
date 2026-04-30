from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd


TOKEN_RE = re.compile(r"[a-z0-9]+(?:\.[a-z0-9]+)?")


@dataclass(frozen=True)
class ExtractedTable:
    name: str
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class TableIndex:
    name: str
    text: str
    tokens: set[str]


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
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_value(value: Any) -> str:
    text = display_value(value).lower()
    text = re.sub(r"[^a-z0-9.]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize(value: Any) -> list[str]:
    return TOKEN_RE.findall(normalize_value(value))


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


def extract_json_tables(data: Any) -> list[ExtractedTable]:
    tables: list[ExtractedTable] = []
    seen_names: dict[str, int] = {}

    def add_table(path: list[str], df: pd.DataFrame) -> None:
        cleaned = clean_dataframe(df)
        if cleaned.empty:
            return

        base_name = ".".join(path) if path else "root"
        seen_names[base_name] = seen_names.get(base_name, 0) + 1
        name = base_name if seen_names[base_name] == 1 else f"{base_name}_{seen_names[base_name]}"
        tables.append(ExtractedTable(name=name, dataframe=cleaned))

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
    for column in table.dataframe.columns:
        normalized_column = normalize_value(column)
        if normalized_column:
            parts.append(normalized_column)

    for value in table.dataframe.to_numpy().ravel():
        normalized_value = normalize_value(value)
        if normalized_value:
            parts.append(normalized_value)

    text = " ".join(parts)
    tokens = {token for part in parts for token in tokenize(part)}
    return TableIndex(name=table.name, text=f" {text} ", tokens=tokens)


def value_is_present(value: Any, table_index: TableIndex, match_mode: str) -> bool:
    normalized = normalize_value(value)
    if not normalized:
        return False

    tokens = tokenize(normalized)
    if not tokens:
        return False

    phrase_hit = f" {normalized} " in table_index.text
    if match_mode == "Exact phrase":
        return phrase_hit

    return phrase_hit or all(token in table_index.tokens for token in tokens)


def collect_excel_points(sheets: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []

    for sheet_name, frame in sheets.items():
        df = clean_dataframe(frame)
        for row_position, (_, row) in enumerate(df.iterrows(), start=2):
            for column in df.columns:
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
                    }
                )

    return points


def compare_baseline_to_json(
    baseline_sheets: dict[str, pd.DataFrame],
    json_tables: Iterable[ExtractedTable],
    selected_table_names: Iterable[str] | None = None,
    match_mode: str = "Word subset",
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    selected_names = set(selected_table_names or [])
    selected_tables = [
        table
        for table in json_tables
        if not selected_names or table.name in selected_names
    ]
    indexes = [build_table_index(table) for table in selected_tables]
    rows: list[dict[str, Any]] = []

    for point in collect_excel_points(baseline_sheets):
        matched_table = ""
        for table_index in indexes:
            if value_is_present(point["baseline_value"], table_index, match_mode):
                matched_table = table_index.name
                break

        rows.append(
            {
                **point,
                "present_in_json": bool(matched_table),
                "matched_json_table": matched_table,
            }
        )

    results = pd.DataFrame(rows)
    total_points = len(results)
    matched_points = int(results["present_in_json"].sum()) if total_points else 0
    accuracy = (matched_points / total_points * 100.0) if total_points else 0.0

    summary = {
        "total_points": total_points,
        "matched_points": matched_points,
        "missing_points": total_points - matched_points,
        "accuracy": accuracy,
        "json_tables": len(selected_tables),
    }
    return results, summary

