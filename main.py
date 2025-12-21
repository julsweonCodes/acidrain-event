"""
FastAPI backend for Acid Rain event ingestion
Receives events from frontend and writes to GCS as gzip-compressed JSONL
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import storage
from datetime import datetime
from typing import List, Dict, Any
import json
import gzip
import os
import uuid

app = FastAPI(title="Acid Rain Event Collector")

# CORS configuration (allow frontend to POST from localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GCS Configuration
# Bucket directory structure: gs://bucket-name/raw/YYYY/MM/DD/events_<uuid>.json.gz
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "acidrain-events-raw")
ENABLE_GCS = os.getenv("ENABLE_GCS", "false").lower() == "true"

# Initialize GCS client (only if enabled)
if ENABLE_GCS:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
    except Exception as e:
        ENABLE_GCS = False


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "acidrain-event-collector",
        "gcs_enabled": ENABLE_GCS,
        "bucket": GCS_BUCKET_NAME if ENABLE_GCS else None
    }


@app.post("/events")
async def ingest_events(events: List[Dict[str, Any]], request: Request):
    """
    Stateless ingestion endpoint - receives events and writes to GCS
    
    Writes: gs://{bucket}/raw/YYYY/MM/DD/events_{uuid}.json.gz
    Format: gzip-compressed newline-delimited JSON (.jsonl)
    
    No validation, transformation, or deduplication - append-only immutable storage
    """
    
    if not events:
        raise HTTPException(status_code=400, detail="No events provided")
    
    # Extract geographic info from Cloud Run headers
    country = request.headers.get("X-Appengine-Country", "UNKNOWN")
    region = request.headers.get("X-Appengine-Region", "UNKNOWN")
    city = request.headers.get("X-Appengine-City", "UNKNOWN")
    
    # Enrich web_context with country data (session-level attribute)
    for event in events:
        if "web_context" not in event:
            event["web_context"] = {}
        event["web_context"]["country"] = country
        event["web_context"]["region"] = region
        event["web_context"]["city"] = city
    
    # Write to GCS if enabled
    if ENABLE_GCS:
        try:
            # UTC-based date partitioning
            now = datetime.utcnow()
            file_uuid = str(uuid.uuid4())
            blob_name = f"raw/{now.year}/{now.month:02d}/{now.day:02d}/events_{file_uuid}.json.gz"
            
            # Serialize as newline-delimited JSON
            ndjson_content = "\n".join(json.dumps(event) for event in events)
            
            # Compress with gzip at write time (in-memory compression)
            compressed_content = gzip.compress(ndjson_content.encode('utf-8'))
            compressed_size_kb = len(compressed_content) / 1024
            
            # Upload compressed file to GCS (immutable, append-only)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(
                compressed_content,
                content_type="application/gzip"
            )
            
            # Log upload details
            print(f"Uploaded: {blob_name} ({compressed_size_kb:.2f} KB)")
            
            return {
                "status": "success",
                "events_received": len(events),
                "gcs_path": f"gs://{GCS_BUCKET_NAME}/{blob_name}"
            }
        
        except Exception as e:
            return {
                "status": "partial_success",
                "events_received": len(events),
                "error": "GCS upload failed"
            }
    
    else:
        # Local dev mode - acknowledge receipt only
        return {
            "status": "success",
            "events_received": len(events),
            "mode": "local_dev"
        }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler"""
    return JSONResponse(
        status_code=500,
        content={"status": "error", "detail": str(exc)}
    )


# Serve static files (frontend) - must be last
app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )
