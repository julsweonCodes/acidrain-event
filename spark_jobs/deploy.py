#!/usr/bin/env python3
"""
Deploy Spark job files to GCS using Python

No need for gsutil - uses google-cloud-storage library instead
"""

import os
import sys
from pathlib import Path
from google.cloud import storage
from config import RAW_BUCKET, BQ_PROJECT

# Configuration
BUCKET = RAW_BUCKET
DESTINATION_PREFIX = "spark_jobs/"

# Files to upload
REQUIRED_FILES = [
    "process_batch.py",
    "config.py",
    "schemas.py",
    "watermark.py",
    "validation.py",
    "__init__.py",
]

def main():
    print("📦 Deploying Spark job code to gs://{}/{}".format(BUCKET, DESTINATION_PREFIX))
    print()
    
    # Validate all files exist
    missing_files = []
    for filename in REQUIRED_FILES:
        if not Path(filename).exists():
            missing_files.append(filename)
    
    if missing_files:
        print("✗ Missing required files:")
        for filename in missing_files:
            print(f"  - {filename}")
        sys.exit(1)
    
    print("✓ All required files present")
    print()
    
    # Initialize GCS client
    try:
        client = storage.Client(project=BQ_PROJECT)
        bucket = client.bucket(BUCKET)
    except Exception as e:
        print(f"✗ Failed to connect to GCS: {e}")
        print()
        print("Make sure you're authenticated:")
        print("  gcloud auth application-default login")
        sys.exit(1)
    
    # Upload files
    print("Uploading files...")
    uploaded = []
    failed = []
    
    for filename in REQUIRED_FILES:
        try:
            blob_name = f"{DESTINATION_PREFIX}{filename}"
            blob = bucket.blob(blob_name)
            
            # Upload file
            blob.upload_from_filename(filename)
            
            # Get file size
            file_size = Path(filename).stat().st_size
            size_kb = file_size / 1024
            
            print(f"  ✓ {filename} ({size_kb:.1f} KB)")
            uploaded.append(filename)
            
        except Exception as e:
            print(f"  ✗ {filename}: {e}")
            failed.append(filename)
    
    print()
    
    # Summary
    if failed:
        print(f"⚠️  Upload completed with errors:")
        print(f"  ✓ Uploaded: {len(uploaded)}/{len(REQUIRED_FILES)}")
        print(f"  ✗ Failed: {len(failed)}/{len(REQUIRED_FILES)}")
        for filename in failed:
            print(f"    - {filename}")
        sys.exit(1)
    else:
        print(f"✓ Successfully uploaded all {len(uploaded)} files to:")
        print(f"  gs://{BUCKET}/{DESTINATION_PREFIX}")
    
    print()
    print("=" * 60)
    print("Next Steps:")
    print("=" * 60)
    print()
    print("1. Set up Cloud Scheduler (one-time):")
    print()
    print("   gcloud scheduler jobs create http acidrain-batch \\")
    print("     --location=us-central1 \\")
    print("     --schedule='0 */6 * * *' \\")
    print("     --uri='https://dataproc.googleapis.com/v1/projects/YOUR_PROJECT/regions/us-central1/batches' \\")
    print("     --http-method=POST \\")
    print("     --message-body-from-file=scheduler-example.json \\")
    print("     --oauth-service-account-email=SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com")
    print()
    print("2. Test the job manually:")
    print()
    print("   gcloud dataproc batches submit pyspark \\")
    print("     gs://{}/process_batch.py \\".format(DESTINATION_PREFIX))
    print("     --region=us-central1 \\")
    print("     --py-files=gs://{}/config.py,gs://{}/schemas.py,gs://{}/watermark.py,gs://{}/validation.py \\".format(
        DESTINATION_PREFIX, DESTINATION_PREFIX, DESTINATION_PREFIX, DESTINATION_PREFIX))
    print("     --deps-bucket={}".format(BUCKET))
    print()
    print("3. Monitor:")
    print()
    print("   gsutil cat gs://{}/_metadata/watermark.json".format(BUCKET))
    print()

if __name__ == "__main__":
    main()
