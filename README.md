# Acid Rain Event Generator

A typing game that generates behavioral event data for analytics pipelines.

## Architecture

```
Frontend (HTML/JS) → FastAPI → GCS → Data Pipeline
                                ↓
                          Raw Events (NDJSON)
```

## Local Development

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run without GCS (local testing)
```bash
# Events will be logged to console only
python main.py
```

Open: http://localhost:8080

### 3. Run with GCS (requires credentials)
```bash
# Set up GCS credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
export GCS_BUCKET_NAME="your-bucket-name"
export ENABLE_GCS="true"

python main.py
```

## Deploy to Google Cloud

### Option 1: Cloud Run (Recommended)

```bash
# Build and deploy
gcloud run deploy acidrain-event-collector \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET_NAME=your-bucket-name,ENABLE_GCS=true
```

### Option 2: App Engine

Create `app.yaml`:
```yaml
runtime: python314
entrypoint: uvicorn main:app --host 0.0.0.0 --port $PORT

env_variables:
  GCS_BUCKET_NAME: "your-bucket-name"
  ENABLE_GCS: "true"
```

Deploy:
```bash
gcloud app deploy
```

## GCS Setup

### Create Bucket
```bash
gsutil mb -l us-central1 gs://acidrain-events-raw
```

### File Structure
```
gs://acidrain-events-raw/
└── events/
    └── 2025/
        └── 12/
            └── 20/
                └── 14/
                    └── {uuid}.json
```

### Grant Service Account Access
```bash
gsutil iam ch serviceAccount:YOUR-SA@PROJECT.iam.gserviceaccount.com:objectCreator gs://acidrain-events-raw
```

## Event Schema

Each event includes:
- `event_id` (UUID)
- `session_id` (UUID)
- `event_type` (string)
- `timestamp` (ISO 8601)
- `metadata` (object)

## Next Steps

After events are in GCS, you can:
1. Use BigQuery External Tables to query directly
2. Set up Cloud Functions to process on upload
3. Use Dataflow for streaming processing
4. Schedule batch loads to BigQuery
