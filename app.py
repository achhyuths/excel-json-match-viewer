from __future__ import annotations

import pandas as pd
import streamlit as st

from matcher import (
    available_columns,
    build_json_points,
    compare_baseline_to_json,
    default_score_columns,
    extract_json_tables,
    filter_values,
    is_extraction_table,
    load_excel,
    load_json,
    summarize_by_json_file,
)


st.set_page_config(page_title="Excel JSON Match Viewer", layout="wide")

st.title("Excel JSON Match Viewer")


JSON_POINT_COLUMNS = [
    "source_file",
    "table_name",
    "row_number",
    "category",
    "row_label",
    "role",
    "value",
    "source_period",
    "source_quarter",
    "source_date",
]


def json_points_dataframe(tables) -> pd.DataFrame:
    rows = [
        {
            "source_file": point.source_file,
            "table_name": point.table_name,
            "row_number": point.row_number,
            "category": point.category,
            "row_label": point.row_label,
            "role": point.role,
            "value": point.value,
            "source_period": point.source_period,
            "source_quarter": point.source_quarter,
            "source_date": point.source_date,
        }
        for point in build_json_points(tables)
    ]
    return pd.DataFrame(rows, columns=JSON_POINT_COLUMNS)

with st.sidebar:
    st.header("Files")
    excel_file = st.file_uploader("Baseline Excel", type=["xlsx", "xls"])
    json_files = st.file_uploader(
        "Extraction JSON files",
        type=["json"],
        accept_multiple_files=True,
    )

    st.header("Matching")
    match_mode = st.radio("Match mode", ["Word subset", "Exact phrase"], horizontal=True)
    require_row_context = st.checkbox("Require same-row label context", value=True)
    category_matching = st.checkbox("Match Excel sheet to JSON section", value=True)
    scope_baseline_to_json_periods = st.checkbox(
        "Scope baseline to JSON periods",
        value=True,
    )
    numeric_tolerance = st.number_input(
        "Numeric tolerance",
        min_value=0.0,
        max_value=1.0,
        value=0.001,
        step=0.001,
        format="%.4f",
    )
    include_metadata = st.checkbox("Search metadata tables", value=False)
    include_manager_tabs = st.checkbox("Include *_MG sheets", value=False)
    run_clicked = st.button("Run comparison", type="primary", use_container_width=True)


baseline_sheets: dict[str, pd.DataFrame] = {}
json_tables = []
excel_error = ""
json_errors: list[str] = []

if excel_file is not None:
    try:
        baseline_sheets = load_excel(excel_file)
    except Exception as exc:
        excel_error = str(exc)

if json_files:
    for json_file in json_files:
        try:
            json_data = load_json(json_file)
            tables = extract_json_tables(json_data, source_file=json_file.name)
            if not include_metadata:
                tables = [table for table in tables if is_extraction_table(table)]
            json_tables.extend(tables)
        except Exception as exc:
            json_errors.append(f"{json_file.name}: {exc}")


selected_sheets: list[str] = []
selected_columns: list[str] = []
selected_json_files: list[str] = []
selected_table_names: list[str] = []
filters: dict[str, set[str]] = {}

if baseline_sheets:
    st.sidebar.header("Baseline Scope")
    sheet_names = list(baseline_sheets.keys())
    non_empty_sheets = [
        name for name, frame in baseline_sheets.items() if not frame.dropna(how="all").empty
    ]
    default_sheets = [
        name
        for name in non_empty_sheets
        if name.lower() != "fund" and (include_manager_tabs or not name.upper().endswith("_MG"))
    ]
    selected_sheets = st.sidebar.multiselect(
        "Sheets to validate",
        sheet_names,
        default=default_sheets or non_empty_sheets or sheet_names,
    )

    columns = available_columns(baseline_sheets, selected_sheets)
    selected_columns = st.sidebar.multiselect(
        "Excel columns to count",
        columns,
        default=default_score_columns(columns),
    )

    period_options = filter_values(baseline_sheets, selected_sheets, ["period"])
    if period_options:
        selected_periods = st.sidebar.multiselect("Period", period_options, default=period_options)
        filters["period"] = set(selected_periods)

    quarter_options = filter_values(baseline_sheets, selected_sheets, ["quarter"])
    if quarter_options:
        selected_quarters = st.sidebar.multiselect("Quarter", quarter_options, default=quarter_options)
        filters["quarter"] = set(selected_quarters)

    date_options = filter_values(
        baseline_sheets,
        selected_sheets,
        ["fund date", "fund-date", "fund_date", "funddate"],
    )
    if date_options:
        selected_dates = st.sidebar.multiselect("Fund Date", date_options, default=date_options)
        filters["fund_date"] = set(selected_dates)

if json_tables:
    st.sidebar.header("JSON Scope")
    json_file_names = sorted({table.source_file for table in json_tables})
    selected_json_files = st.sidebar.multiselect(
        "JSON files to search",
        json_file_names,
        default=json_file_names,
    )
    table_names = sorted(
        {
            table.name
            for table in json_tables
            if not selected_json_files or table.source_file in selected_json_files
        }
    )
    selected_table_names = st.sidebar.multiselect(
        "JSON tables to search",
        table_names,
        default=table_names,
    )


left, right = st.columns(2)

with left:
    st.subheader("Baseline Excel")
    if excel_error:
        st.error(excel_error)
    elif baseline_sheets:
        preview_sheet = st.selectbox("Preview sheet", list(baseline_sheets.keys()))
        st.dataframe(baseline_sheets[preview_sheet], use_container_width=True, height=430)
    else:
        st.info("Upload one analyst baseline workbook.")

with right:
    st.subheader("Extraction JSON Tables")
    if json_errors:
        for error in json_errors:
            st.error(error)
    if json_tables:
        preview_file = st.selectbox(
            "Preview JSON file",
            sorted({table.source_file for table in json_tables}),
        )
        preview_tables = [table for table in json_tables if table.source_file == preview_file]
        preview_table_name = st.selectbox(
            "Preview table",
            [table.name for table in preview_tables],
        )
        preview_table = next(table for table in preview_tables if table.name == preview_table_name)
        preview_points = json_points_dataframe([preview_table])
        st.caption(
            f"Raw rows: {len(preview_table.dataframe):,} | "
            f"Parsed scoring points: {len(preview_points):,}"
        )
        st.dataframe(preview_table.dataframe, use_container_width=True, height=430)
        with st.expander("Parsed scoring points for this JSON table", expanded=False):
            if preview_points.empty:
                st.warning(
                    "No scoreable values were parsed from this table. "
                    "That usually means the JSON row has labels only, or fields do not map "
                    "to label/long/short/net/value columns."
                )
            else:
                numeric_roles = {"long", "short", "net", "value"}
                if not set(preview_points["role"]).intersection(numeric_roles):
                    st.warning(
                        "This JSON table produced label points only. Numeric Excel values "
                        "for this same category will be marked missing unless another "
                        "selected JSON table has the matching long/short/net/value fields."
                    )
                role_counts = (
                    preview_points["role"]
                    .value_counts()
                    .rename_axis("role")
                    .reset_index(name="points")
                )
                st.dataframe(role_counts, use_container_width=True, height=140)
                st.dataframe(preview_points, use_container_width=True, height=260)
    elif not json_errors:
        st.info("Upload one or more extraction JSON files.")


if json_tables:
    scoped_json_tables = [
        table
        for table in json_tables
        if (not selected_json_files or table.source_file in selected_json_files)
        and (not selected_table_names or table.name in selected_table_names)
    ]
    scoped_json_points = json_points_dataframe(scoped_json_tables)
    with st.expander("All selected parsed JSON data points", expanded=False):
        st.write(
            "These are the exact JSON points used by the strict matcher before "
            "Excel category, side-heading, role, and period checks are applied."
        )
        st.metric("Parsed JSON data points", len(scoped_json_points))
        if scoped_json_points.empty:
            st.warning("No scoreable JSON data points are currently selected.")
        else:
            role_counts = (
                scoped_json_points["role"]
                .value_counts()
                .rename_axis("role")
                .reset_index(name="points")
            )
            st.dataframe(role_counts, use_container_width=True, height=160)
            st.dataframe(scoped_json_points, use_container_width=True, height=360)


if baseline_sheets:
    with st.expander("Baseline data point preview", expanded=False):
        st.write(
            "Only the selected sheets, filters, and Excel columns are counted as validation data points."
        )
        preview_results, preview_summary = compare_baseline_to_json(
            baseline_sheets=baseline_sheets,
            json_tables=[],
            selected_sheets=selected_sheets,
            selected_columns=selected_columns,
            filters=filters,
            match_mode=match_mode,
            require_row_context=require_row_context,
            category_matching=category_matching,
            numeric_tolerance=numeric_tolerance,
            scope_baseline_to_json_periods=False,
        )
        st.metric("Selected baseline data points", preview_summary["total_points"])
        if preview_results.empty:
            st.info("No baseline data points are selected.")
        else:
            st.dataframe(
                preview_results[
                    ["baseline_sheet", "excel_row", "excel_column", "baseline_value"]
                ].head(250),
                use_container_width=True,
                height=260,
            )


if run_clicked:
    if not baseline_sheets:
        st.warning("Load a baseline Excel file first.")
    elif not json_tables:
        st.warning("Load at least one JSON extraction file first.")
    elif not selected_sheets:
        st.warning("Select at least one baseline sheet.")
    elif not selected_columns:
        st.warning("Select at least one Excel column to count.")
    elif not selected_json_files:
        st.warning("Select at least one JSON file.")
    elif not selected_table_names:
        st.warning("Select at least one JSON table.")
    else:
        results, summary = compare_baseline_to_json(
            baseline_sheets=baseline_sheets,
            json_tables=json_tables,
            selected_table_names=selected_table_names,
            selected_json_files=selected_json_files,
            selected_sheets=selected_sheets,
            selected_columns=selected_columns,
            filters=filters,
            match_mode=match_mode,
            require_row_context=require_row_context,
            category_matching=category_matching,
            numeric_tolerance=numeric_tolerance,
            scope_baseline_to_json_periods=scope_baseline_to_json_periods,
        )
        by_file = summarize_by_json_file(
            baseline_sheets=baseline_sheets,
            json_tables=json_tables,
            selected_json_files=selected_json_files,
            selected_table_names=selected_table_names,
            selected_sheets=selected_sheets,
            selected_columns=selected_columns,
            filters=filters,
            match_mode=match_mode,
            require_row_context=require_row_context,
            category_matching=category_matching,
            numeric_tolerance=numeric_tolerance,
            scope_baseline_to_json_periods=scope_baseline_to_json_periods,
        )

        metric_cols = st.columns(6)
        metric_cols[0].metric("Overall accuracy", f"{summary['accuracy']:.2f}%")
        metric_cols[1].metric("Matched", f"{summary['matched_points']}")
        metric_cols[2].metric("Excel points", f"{summary['total_points']}")
        metric_cols[3].metric("Missing", f"{summary['missing_points']}")
        metric_cols[4].metric("JSON points", f"{summary['json_data_points']}")
        metric_cols[5].metric("JSON files", f"{summary['json_files']}")

        value_cols = st.columns(5)
        value_cols[0].metric("Value-only accuracy", f"{summary['value_accuracy']:.2f}%")
        value_cols[1].metric("Value matched", f"{summary['value_matched_points']}")
        value_cols[2].metric("Left points", f"{summary['total_points']}")
        value_cols[3].metric("Right points", f"{summary['json_data_points']}")
        value_cols[4].metric(
            "Left = Right",
            "Yes" if summary["data_point_counts_equal"] else "No",
            delta=f"{summary['data_point_delta']:+}",
        )

        st.progress(summary["accuracy"] / 100.0)

        st.subheader("Accuracy by JSON File")
        if not by_file.empty:
            by_file_display = by_file.copy()
            by_file_display["accuracy"] = by_file_display["accuracy"].map(lambda value: f"{value:.2f}%")
            by_file_display["value_accuracy"] = by_file_display["value_accuracy"].map(lambda value: f"{value:.2f}%")
            by_file_display["left_right_equal"] = by_file_display["left_right_equal"].map(
                lambda value: "Yes" if value else "No"
            )
            st.dataframe(by_file_display, use_container_width=True, height=260)

        st.subheader("Comparison Results")
        tabs = st.tabs(["All data points", "Strict missing", "Strict matched", "Value missing"])

        with tabs[0]:
            st.dataframe(results, use_container_width=True, height=440)
        with tabs[1]:
            st.dataframe(
                results.loc[~results["present_in_json"]],
                use_container_width=True,
                height=440,
            )
        with tabs[2]:
            st.dataframe(
                results.loc[results["present_in_json"]],
                use_container_width=True,
                height=440,
            )
        with tabs[3]:
            st.dataframe(
                results.loc[~results["value_present_in_json"]],
                use_container_width=True,
                height=440,
            )

        st.download_button(
            "Download comparison CSV",
            results.to_csv(index=False).encode("utf-8"),
            file_name="comparison_results.csv",
            mime="text/csv",
        )
