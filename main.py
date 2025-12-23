"""
FastAPI backend for Acid Rain event ingestion
Receives events from frontend and writes to GCS as gzip-compressed JSONL
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import storage
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path
import json
import gzip
import os
import uuid

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
APP_DIR = BASE_DIR / "app"

# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
app = FastAPI(title="Acid Rain Event Collector")

# -------------------------------------------------------------------
# CORS
# -------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------------
# Static frontend
# -------------------------------------------------------------------
app.mount(
    "/static",
    StaticFiles(directory=APP_DIR),
    name="static"
)

@app.get("/")
async def serve_index():
    return FileResponse(APP_DIR / "index.html")

@app.get("/dashboard")
async def serve_dashboard():
    return FileResponse(APP_DIR / "dashboard.html")



# -------------------------------------------------------------------
# GCS configuration
# -------------------------------------------------------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "acidrain-events-raw")
ENABLE_GCS = os.getenv("ENABLE_GCS", "false").lower() == "true"

if ENABLE_GCS:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
    except Exception:
        ENABLE_GCS = False

# -------------------------------------------------------------------
# Health
# -------------------------------------------------------------------
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "acidrain-event-collector",
        "gcs_enabled": ENABLE_GCS,
        "bucket": GCS_BUCKET_NAME if ENABLE_GCS else None
    }

# -------------------------------------------------------------------
# Ingestion endpoint
# -------------------------------------------------------------------
@app.post("/events")
async def ingest_events(events: List[Dict[str, Any]], request: Request):
    """
    One session = one request = one gzip-compressed JSONL file
    Append-only raw ingestion (no validation)
    """

    if not events:
        raise HTTPException(status_code=400, detail="No events provided")

    # Geo headers (Cloud Run / App Engine)
    country = request.headers.get("X-Appengine-Country", "UNKNOWN")
    region = request.headers.get("X-Appengine-Region", "UNKNOWN")

    for event in events:
        event.setdefault("web_context", {})
        event["web_context"]["country"] = country
        event["web_context"]["region"] = region

    if not ENABLE_GCS:
        return {
            "status": "success",
            "events_received": len(events),
            "mode": "local_dev"
        }

    try:
        now = datetime.utcnow()
        file_uuid = str(uuid.uuid4())

        blob_name = (
            f"raw/{now.year}/{now.month:02d}/{now.day:02d}"
            f"/events_{file_uuid}.json.gz"
        )

        ndjson = "\n".join(json.dumps(event) for event in events)
        compressed = gzip.compress(ndjson.encode("utf-8"))

        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            compressed,
            content_type="application/gzip"
        )

        return {
            "status": "success",
            "events_received": len(events),
            "gcs_path": f"gs://{GCS_BUCKET_NAME}/{blob_name}"
        }

    except Exception as e:
        return {
            "status": "partial_success",
            "events_received": len(events),
            "error": str(e)
        }

# -------------------------------------------------------------------
# Global error handler
# -------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"status": "error", "detail": str(exc)}
    )

# -------------------------------------------------------------------
# Local run
# -------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )
