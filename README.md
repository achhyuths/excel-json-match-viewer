# Excel JSON Match Viewer

Small Streamlit app that compares one analyst baseline Excel file against one or many JSON extraction outputs.

The default matching algorithm is row-aware: selected Excel cells are counted as validation data points, normalized into words/numbers, then matched against compatible JSON rows. Numeric matching handles common percentage forms such as `0.943` and `94.3%`.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run Locally

```powershell
streamlit run app.py
```

## Streamlit Cloud

Push these files to GitHub and set the Streamlit entry point to:

```
app.py
```

## Inputs

- Baseline Excel: `.xlsx` or `.xls`
- Extraction JSON outputs: one or many `.json` files

The validator supports both older wrapped Llama outputs with a top-level `data` object and the v6 direct schema with top-level `section_1_document_identification`, `section_2_schedule_of_investments`, `section_3_balance_sheet`, and `section_4_manager_breakdowns`.

## Score

```text
accuracy = matched Excel data points / selected Excel baseline data points
```

The app also shows the number of comparable JSON data points found in the uploaded extraction files. Comparable means the JSON point is in the selected Excel categories, selected column roles, and selected/auto-scoped periods. A match requires the same period when available, same Excel/JSON category, same side heading or row label, same column meaning, and the same value.

## Workflow

1. Upload the analyst baseline workbook.
2. Upload all JSON files produced from the related source PDFs.
3. Select the baseline sheets and Excel columns to count.
4. Optionally filter baseline rows by period, quarter, or fund date.
5. Click `Run comparison`.

The app shows combined accuracy across the selected JSON files, accuracy by JSON file, and a downloadable CSV with each matched or missing baseline data point.

## Matching Controls

- `Require same-row label context`: a numeric value only matches if the side heading / row label is the same in the JSON row.
- `Match Excel sheet to JSON section`: `Geography` matches geography tables, `GICS` matches GICS tables, and so on.
- `Scope baseline to JSON periods`: baseline rows are counted only when their period/date/quarter matches the selected JSON files.
- `Numeric tolerance`: allowed numeric difference for percentages and decimal values.

For v6 JSON, the app reads `*.extracted_value` arrays in Section 4 as validation tables and maps fields like `label`, `holding_name`, `investment_name`, `long_pct`, `short_pct`, `net_pct`, and `market_value` to the equivalent Excel baseline columns.
