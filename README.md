# Excel JSON Match Viewer

Small Streamlit app that compares a baseline Excel file against tables extracted from JSON.

The matching algorithm is intentionally simple: every non-empty Excel cell is counted as one data point, normalized into words, then searched across the extracted JSON tables.

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
- JSON output: `.json`

## Score

```text
accuracy = matched Excel data points / total Excel data points
```

The results table shows each Excel data point, whether it was present in JSON, and which JSON table matched it.

- `.md` for Markdown
- `.json` for the full Docling document representation

Optional formats are `html`, `doctags`, and `txt`.
