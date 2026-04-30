from __future__ import annotations

import pandas as pd
import streamlit as st

from matcher import (
    compare_baseline_to_json,
    extract_json_tables,
    load_excel,
    load_json,
)


st.set_page_config(page_title="Excel JSON Match Viewer", layout="wide")

st.title("Excel JSON Match Viewer")

with st.sidebar:
    st.header("Files")
    excel_file = st.file_uploader("Baseline Excel", type=["xlsx", "xls"])
    json_file = st.file_uploader("JSON", type=["json"])
    match_mode = st.radio("Match mode", ["Word subset", "Exact phrase"], horizontal=True)
    run_clicked = st.button("Run comparison", type="primary", use_container_width=True)


baseline_sheets: dict[str, pd.DataFrame] = {}
json_tables = []
excel_error = ""
json_error = ""

if excel_file is not None:
    try:
        baseline_sheets = load_excel(excel_file)
    except Exception as exc:
        excel_error = str(exc)

if json_file is not None:
    try:
        json_data = load_json(json_file)
        json_tables = extract_json_tables(json_data)
    except Exception as exc:
        json_error = str(exc)


left, right = st.columns(2)

with left:
    st.subheader("Baseline Excel")
    if excel_error:
        st.error(excel_error)
    elif baseline_sheets:
        sheet_names = list(baseline_sheets.keys())
        selected_sheet = st.selectbox("Sheet", sheet_names)
        st.dataframe(baseline_sheets[selected_sheet], use_container_width=True, height=420)
    else:
        st.info("No Excel file loaded.")

with right:
    st.subheader("JSON Tables")
    if json_error:
        st.error(json_error)
    elif json_tables:
        table_names = [table.name for table in json_tables]
        selected_preview_table = st.selectbox("Table", table_names)
        preview = next(table for table in json_tables if table.name == selected_preview_table)
        st.dataframe(preview.dataframe, use_container_width=True, height=420)
    else:
        st.info("No JSON tables loaded.")


selected_table_names: list[str] = []
if json_tables:
    selected_table_names = st.multiselect(
        "JSON tables to search",
        [table.name for table in json_tables],
        default=[table.name for table in json_tables],
    )


if run_clicked:
    if not baseline_sheets:
        st.warning("Load a baseline Excel file first.")
    elif not json_tables:
        st.warning("Load a JSON file with at least one table first.")
    elif not selected_table_names:
        st.warning("Select at least one JSON table to search.")
    else:
        results, summary = compare_baseline_to_json(
            baseline_sheets=baseline_sheets,
            json_tables=json_tables,
            selected_table_names=selected_table_names,
            match_mode=match_mode,
        )

        metric_cols = st.columns(4)
        metric_cols[0].metric("Accuracy", f"{summary['accuracy']:.2f}%")
        metric_cols[1].metric("Matched", f"{summary['matched_points']}")
        metric_cols[2].metric("Data points", f"{summary['total_points']}")
        metric_cols[3].metric("Missing", f"{summary['missing_points']}")

        st.progress(summary["accuracy"] / 100.0)

        st.subheader("Comparison Results")
        st.dataframe(results, use_container_width=True, height=460)
        st.download_button(
            "Download results CSV",
            results.to_csv(index=False).encode("utf-8"),
            file_name="comparison_results.csv",
            mime="text/csv",
        )
