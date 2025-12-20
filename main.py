"""
FastAPI backend for Acid Rain event ingestion
Receives events from frontend and writes to GCS
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import storage
from datetime import datetime
from typing import List, Dict, Any
import json
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
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "acidrain-events-raw")
ENABLE_GCS = os.getenv("ENABLE_GCS", "false").lower() == "true"

# Initialize GCS client (only if enabled)
if ENABLE_GCS:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        print(f"✓ Connected to GCS bucket: {GCS_BUCKET_NAME}")
    except Exception as e:
        print(f"⚠ GCS connection failed: {e}")
        print("  Running in local mode (events logged only)")
        ENABLE_GCS = False
else:
    print("ℹ GCS disabled - running in local dev mode")


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
async def ingest_events(events: List[Dict[str, Any]]):
    """
    Receive batch of events from frontend and write to GCS
    
    Events are written as newline-delimited JSON (NDJSON) files
    Format: gs://bucket/events/YYYY/MM/DD/HH/{batch_id}.json
    """
    
    if not events:
        raise HTTPException(status_code=400, detail="No events provided")
    
    # Validate event structure
    for event in events:
        if not all(k in event for k in ["event_id", "session_id", "event_type", "timestamp"]):
            raise HTTPException(
                status_code=400, 
                detail="Invalid event format - missing required fields"
            )
    
    # Log events to console (always, for debugging)
    print(f"[Events] Received {len(events)} events")
    for event in events:
        print(f"  - {event['event_type']}: {event.get('metadata', {})}")
    
    # Write to GCS if enabled
    if ENABLE_GCS:
        try:
            # Generate filename with date partitioning
            now = datetime.utcnow()
            batch_id = str(uuid.uuid4())
            blob_name = f"events/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{batch_id}.json"
            
            # Write as newline-delimited JSON (one event per line)
            ndjson_content = "\n".join(json.dumps(event) for event in events)
            
            # Upload to GCS
            blob = bucket.blob(blob_name)
            blob.upload_from_string(
                ndjson_content,
                content_type="application/x-ndjson"
            )
            
            print(f"[GCS] ✓ Uploaded to gs://{GCS_BUCKET_NAME}/{blob_name}")
            
            return {
                "status": "success",
                "events_received": len(events),
                "gcs_path": f"gs://{GCS_BUCKET_NAME}/{blob_name}"
            }
        
        except Exception as e:
            print(f"[GCS] ✗ Upload failed: {e}")
            # Don't fail the request - events are still logged
            return {
                "status": "partial_success",
                "events_received": len(events),
                "error": "GCS upload failed, events logged",
                "details": str(e)
            }
    
    else:
        # Local dev mode - just acknowledge receipt
        return {
            "status": "success",
            "events_received": len(events),
            "mode": "local_dev"
        }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler"""
    print(f"[Error] {exc}")
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
