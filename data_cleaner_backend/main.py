"""
main.py — Data Cleaner Backend (FastAPI)
-----------------------------------------
FIXES APPLIED:
  1. WindowsSelectorEventLoopPolicy → fixes WinError 10054 + Invalid HTTP request errors
  2. Added /api/report endpoint → exposes cleaning stats to frontend
  3. Removed unused generate_report import (now actually used)
  4. Improved error messages
"""

import asyncio
import sys

  # FIX 1 : Only apply on Windows AND Python versions below 3.14 (where ProactorLoop bug exists)
# Python 3.14+ deprecated this API as they fixed the underlying issue
if sys.platform == "win32" and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
from io import BytesIO, StringIO

from cleaner import clean_dataframe, generate_report  # ✅ FIX 3: generate_report now used below

app = FastAPI(title="Data Cleaner API", version="1.0.0")

# CORS — allow all origins (fine for local dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "Data Cleaner API", "status": "running"}


@app.post("/api/analyze")
async def analyze_file(file: UploadFile = File(...)):
    """Returns basic stats about the uploaded CSV (missing values, duplicates, shape)."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        contents = await file.read()
        df = pd.read_csv(BytesIO(contents))

        analysis = {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns.tolist(),
            "missing_values": {},
            "duplicates": int(df.duplicated().sum()),
        }

        for col in df.columns:
            missing_count = int(df[col].isnull().sum())
            if missing_count > 0:
                analysis["missing_values"][col] = {
                    "count": missing_count,
                    "percentage": round((missing_count / len(df)) * 100, 2),
                }

        return analysis

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")


@app.post("/api/clean")
async def clean_file(file: UploadFile = File(...)):
    """Cleans the uploaded CSV and returns the cleaned file as a download."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        contents = await file.read()
        df_original = pd.read_csv(BytesIO(contents))

        df_clean, stats = clean_dataframe(df_original)

        output = StringIO()
        df_clean.to_csv(output, index=False)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=cleaned_{file.filename}"
            },
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleaning error: {str(e)}")


# ✅ FIX 2: New endpoint — returns cleaning report as JSON so frontend can show stats
@app.post("/api/report")
async def report_file(file: UploadFile = File(...)):
    """Cleans the CSV and returns a JSON report of what was changed (no file download)."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        contents = await file.read()
        df_original = pd.read_csv(BytesIO(contents))

        df_clean, stats = clean_dataframe(df_original)
        report = generate_report(df_original, df_clean, stats)

        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    # ✅ Run directly — policy is already set at the top of the file
    uvicorn.run(app, host="0.0.0.0", port=8000)