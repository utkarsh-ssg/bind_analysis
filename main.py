from fastapi import FastAPI, UploadFile, File, Form
from typing import List
from datetime import datetime
import pandas as pd
from io import BytesIO
import uvicorn

app = FastAPI()

SKIP_VALUES = {
    "collection efficiency",
    "sales (# units)",
    "sales (area in sq ft)",
    "sales (in cr.)",
}

SKIP_COLUMNS = {
    "Total - Project",
    "Actual Incurred (Oct'20- Sep'23)",
    "Balance (Oct'23 Onwards)",
    "Pre-Tribeca Bal"
}

def get_date_range_from_header(header: str):
    header = str(header).strip()

    
    if "to" in header.lower():
        try:
            parts = header.lower().split("to")
            start_part = parts[0].strip().title()
            end_part = parts[1].strip().title()

            start_date = datetime.strptime(start_part, "%b %y")
            end_date = datetime.strptime(end_part, "%B %y")

            start = datetime(start_date.year, start_date.month, 1)
            end = datetime(end_date.year, end_date.month, 1) + pd.offsets.MonthEnd(0)

            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        except:
            return "", ""

    
    if header.lower().startswith("fy "):
        try:
            year = int(header.split(" ")[1].split("-")[0]) + 2000
            start = datetime(year, 4, 1)
            end = datetime(year + 1, 3, 31)
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        except:
            return "", ""

    
    try:
        date = pd.to_datetime(header)
        start = datetime(date.year, date.month, 1)
        if date.month == 12:
            end = datetime(date.year + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            end = datetime(date.year, date.month + 1, 1) - pd.Timedelta(days=1)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    except:
        return "", ""


def detect_financial_type(header: str):
    header = str(header).strip().lower()
    if "to" in header:
        return "Semi-Annual"
    if header.startswith("fy"):
        return "Annual"
    return "Monthly"

@app.post("/process-excel/")
async def process_excel(file: UploadFile = File(...), project_id: int = Form(...)):
    contents = await file.read()
    df_full = pd.read_excel(BytesIO(contents), sheet_name="CF Summary Resi- CTC", header=None)

    
    df = df_full.iloc[5:, 4:].copy()
    df.reset_index(drop=True, inplace=True)

    
    df.columns = df.iloc[0]
    df = df[1:]
    df.reset_index(drop=True, inplace=True)

    
    df = df.loc[:, ~df.columns.isin(SKIP_COLUMNS)]

    results = []
    current_type = ""
    
    for _, row in df.iterrows():
        first_cell_raw = row.iloc[0]

        if pd.isna(first_cell_raw):
            continue

        first_cell_str = str(first_cell_raw).strip()
        if first_cell_str.lower() in SKIP_VALUES:
            continue

        if not first_cell_str.lower().startswith("tower"):
            current_type = first_cell_str
            tower_name = ""
        else:
            tower_name = first_cell_str


        for col in df.columns[1:]:
            if str(col).strip().lower() in SKIP_VALUES:
                continue

            value = row[col]
            if isinstance(value, pd.Series):
                continue
            if pd.isna(value):
                value = 0.0

            start_date, end_date = get_date_range_from_header(col)
            financial_type = detect_financial_type(col)

            results.append({
                "towerName": tower_name,
                "projectId": int(project_id),
                "type": current_type,
                "startDate": start_date,
                "endDate": end_date,
                "financialType": financial_type,
                "value": float(value) if isinstance(value, (int, float)) else 0.0
            })

    return results

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
