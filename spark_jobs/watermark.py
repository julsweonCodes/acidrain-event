"""
Watermark management for incremental batch processing

Stores last successful batch timestamp in GCS to enable:
- Incremental processing (only new data)
- Idempotent retries
- Recovery from failures

🔒 Atomic Updates:
Uses temp file + rename pattern to ensure atomicity:
1. Write to _metadata/watermark.json.tmp
2. Rename to _metadata/watermark.json (atomic operation)
3. Never leaves watermark in inconsistent state
"""

from google.cloud import storage
from datetime import datetime, timezone
import json
import uuid

from config import RAW_BUCKET, WATERMARK_PATH


def read_watermark():
    """
    Read last successful batch timestamp from GCS
    Returns epoch (1970-01-01) if watermark doesn't exist (first run)
    """
    try:
        client = storage.Client()
        bucket = client.bucket(RAW_BUCKET)
        blob = bucket.blob(WATERMARK_PATH)

        if not blob.exists():
            print("No watermark found, starting from epoch")
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

        data = json.loads(blob.download_as_text())
        watermark_str = data["last_successful_batch_time"]
        watermark = datetime.fromisoformat(
            watermark_str.replace("Z", "+00:00")
        )
        
        print(f"Watermark loaded: {watermark.isoformat()}")
        return watermark
        
    except Exception as e:
        print(f"Error reading watermark: {e}")
        print("Defaulting to epoch")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def write_watermark(batch_start_time):
    """
    Atomically write new watermark to GCS after successful batch
    
    Uses temp file + rename for atomic operation:
    - Write to watermark.json.tmp
    - Rename to watermark.json (atomic in GCS)
    - If process crashes, tmp file is orphaned (cleaned up next run)
    
    Args:
        batch_start_time: Timestamp when this batch started (UTC)
    """
    try:
        client = storage.Client()
        bucket = client.bucket(RAW_BUCKET)
        
        # Generate unique temp path
        temp_path = f"{WATERMARK_PATH}.tmp.{uuid.uuid4().hex[:8]}"
        temp_blob = bucket.blob(temp_path)
        final_blob = bucket.blob(WATERMARK_PATH)

        watermark_data = {
            "last_successful_batch_time": 
                batch_start_time.isoformat().replace("+00:00", "Z"),
            "updated_at": 
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }

        # Write to temp file
        temp_blob.upload_from_string(
            json.dumps(watermark_data, indent=2),
            content_type="application/json"
        )
        
        # Atomic rename (copy + delete in GCS)
        bucket.copy_blob(temp_blob, bucket, WATERMARK_PATH)
        temp_blob.delete()
        
        print(f"✓ Watermark updated atomically: {batch_start_time.isoformat()}")
        
        # Cleanup any orphaned temp files (best effort)
        _cleanup_temp_watermarks(bucket)
        
    except Exception as e:
        print(f"✗ Error writing watermark: {e}")
        raise  # Re-raise to fail the batch


def _cleanup_temp_watermarks(bucket):
    """
    Clean up orphaned temporary watermark files (best effort)
    """
    try:
        prefix = f"{WATERMARK_PATH}.tmp"
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            print(f"Cleaning up orphaned temp file: {blob.name}")
            blob.delete()
    except Exception as e:
        print(f"Warning: Failed to cleanup temp files: {e}")
        # Don't fail batch for cleanup issues
