# Acid Rain Event Generator

A multilingual typing game (English/Korean) that generates behavioral event data for analytics pipelines.

## Features

- **Bilingual Support**: English and Korean (한국어) with proper IME handling
- **Event Tracking**: Session-based analytics with 6 event types
- **Stateless Ingestion**: Append-only, gzip-compressed JSONL storage
- **Geographic Context**: Automatic country/region/city detection
- **Immutable Data Lake**: Date-partitioned raw zone for Spark processing

## Architecture

```
Frontend (HTML/JS) → FastAPI → GCS (raw zone)
                                ↓
                    gzip-compressed JSONL
                    raw/YYYY/MM/DD/events_{uuid}.json.gz
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

### Cloud Run (Recommended)

```bash
# Build and deploy
gcloud run deploy acidrain-event-collector \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET_NAME=your-bucket-name,ENABLE_GCS=true
```

## GCS Setup

### Create Bucket
```bash
# Use 'us' multi-region for better redundancy
gsutil mb -l us gs://acidrain-events-raw
```

### File Structure (Immutable Raw Zone)
```
gs://acidrain-events-raw/
└── raw/
    └── 2025/
        └── 12/
            └── 21/
                ├── events_abc123.json.gz  (3.45 KB)
                ├── events_def456.json.gz  (2.89 KB)
                └── events_xyz789.json.gz  (4.12 KB)
```

**Format**: gzip-compressed newline-delimited JSON (`.json.gz`)  
**Partitioning**: UTC-based daily partitions (`YYYY/MM/DD`)  
**Naming**: `events_{uuid}.json.gz`

### Grant Service Account Access
```bash
gsutil iam ch serviceAccount:YOUR-SA@PROJECT.iam.gserviceaccount.com:objectCreator gs://acidrain-events-raw
```

## Event Schema

### Event Structure
```json
{
  "event_id": "uuid-v4",
  "session_id": "uuid-v4",
  "event_type": "word_typed_correct",
  "timestamp": "2025-12-21T10:30:45.123Z",
  "metadata": {
    "word": "example",
    "time_to_type_ms": 420,
    "current_speed": 1.8,
    "page_url": "https://...",
    "user_agent": "Mozilla/5.0 ..."
  },
  "web_context": {
    "timezone": "Asia/Seoul",
    "language": "en-US",
    "country": "KR",
    "region": "11"
  }
}
```

### Session-Level Attributes
`web_context` is collected **once per session** (browser page load) and reused for all events:
- `timezone`: Browser timezone (e.g., `Asia/Seoul`)
- `language`: Browser language (e.g., `en-US`, `ko-KR`)
- `country`, `region`: Added by backend from Cloud Run headers

**Design Principle**: Enables session-level enrichment in Spark, reduces data duplication

### Event Types
1. `game_start` - Game begins
2. `word_spawn` - Word appears on screen
3. `word_typed_correct` - User types word correctly
4. `word_typed_incorrect` - User types wrong word
5. `word_missed` - Word reaches bottom (game over trigger)
6. `game_over` - Game ends with final stats

## Data Processing

### Read with Spark
```python
# Read gzip-compressed JSONL directly
df = spark.read.json("gs://acidrain-events-raw/raw/2025/12/21/*.json.gz")

# Session-level aggregation example
session_stats = df.groupBy("session_id", "web_context.country") \
    .agg(
        count("*").alias("total_events"),
        countDistinct("event_type").alias("event_types"),
        max("timestamp").alias("last_activity")
    )
```

### BigQuery External Table
```sql
CREATE EXTERNAL TABLE `project.dataset.acidrain_raw`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  compression = 'GZIP',
  uris = ['gs://acidrain-events-raw/raw/*/*.json.gz']
);
```

## Game Features

### Language Support
- **English**: 195 unique words (technical terms, long words, loanwords)
- **Korean**: 350+ words (2-4 characters, single words only, proper IME composition)

### Gameplay
- 90-second timer
- Progressive difficulty (speed increases every 5 seconds)
- No duplicate words per session
- Case-insensitive matching
- Real-time score tracking

## API Endpoints

### `POST /events`
Stateless ingestion endpoint - no validation, transformation, or deduplication

**Request**: JSON array of events  
**Response**: Upload confirmation with GCS path  
**Side Effect**: Writes `raw/YYYY/MM/DD/events_{uuid}.json.gz`

### `GET /health`
Health check with GCS status

## Design Principles

✅ **Stateless**: Each request is independent  
✅ **Append-only**: Immutable raw zone  
✅ **No validation**: Accept all events as-is  
✅ **Compression at write**: gzip in-memory before upload  
✅ **Session-level context**: Collect once, reuse for all events  
✅ **UTC partitioning**: Daily date-based partitions
