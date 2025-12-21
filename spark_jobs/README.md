# Acid Rain Spark Batch Processing

Processes gzip-compressed event data from GCS raw zone every 6 hours.

## Architecture

```
Cloud Scheduler (every 6 hours)
    ↓
Dataproc Job Submission
    ↓
Spark Cluster reads:
  gs://acidrain-events-raw/raw/YYYY/MM/DD/*.json.gz
    ↓
Pydantic validation (split valid/invalid)
    ↓ (invalid events)
Quarantine to GCS → gs://.../quarantine/YYYY/MM/DD/invalid_events_{uuid}.json
    ↓ (valid events only)
Process & flatten events
    ↓
Write to BigQuery (partitioned by date)
    ↓
Update watermark in GCS
```

## Best Practice: Never Fail Batch for Bad Events

The pipeline validates each event individually:
- ✅ **Valid events**: Continue to BigQuery
- ❌ **Invalid events**: Quarantined to GCS with error details
- 📊 **Metrics**: Validation rate, error types logged
- 🚨 **Alert**: Warning if >50% events invalid (potential data quality issue)

### Quarantine Structure
```
gs://acidrain-events-raw/quarantine/
└── 2025/12/22/
    └── invalid_events_a1b2c3d4.json  # Contains:
        - batch_timestamp
        - invalid_count
        - error_summary (error type counts)
        - invalid_events (original data)
        - validation_errors (what went wrong)
```

## Files

- `process_batch.py` - Main Spark job (entry point)
- `schemas.py` - Event schemas with partial completion metrics
- `config.py` - Configuration (bucket names, BQ settings)
- `watermark.py` - Incremental processing state management
- `validation.py` - Pydantic models for event validation
- `deploy.sh` - Deployment script
- `scheduler.yaml` - Cloud Scheduler configuration

## Data Flow

### Input (GCS Raw Zone)
```
gs://acidrain-events-raw/
└── raw/
    └── 2025/12/22/
        ├── events_abc123.json.gz  (one session)
        ├── events_def456.json.gz  (one session)
        └── events_xyz789.json.gz  (one session)
```

**Format**: Gzip-compressed newline-delimited JSON

### Output (BigQuery)
```
Project: your-gcp-project
Dataset: acidrain
Table: events
  - Partitioned by: timestamp (DAY)
  - Clustered by: session_id, event_type
```

## Event Types Processed

1. **game_start** - Game initialization
2. **word_spawn** - Word appears on screen
3. **word_typed_correct** - Successful word completion
4. **word_typed_incorrect** - Failed attempt (includes partial completion)
5. **word_missed** - Word reached bottom
6. **game_over** - Session end with final stats

## Partial Completion Metrics

For `word_typed_incorrect` events:
- `attempted`: What user typed
- `closest_match`: Most similar word on screen
- `chars_matched`: Correct characters from start
- `match_ratio`: Completion percentage (0.0 - 1.0)

Example:
```json
{
  "attempted": "사",
  "closest_match": "사과",
  "chars_matched": 1,
  "match_ratio": 0.5
}
```

## Setup

### 1. Update Configuration

Edit `config.py`:
```python
BQ_PROJECT = "your-gcp-project-id"  # Your GCP project
```

### 2. Create BigQuery Dataset

```bash
bq mk --location=US acidrain
```

### 3. Deploy Spark Jobs

```bash
cd spark_jobs/
chmod +x deploy.sh
./deploy.sh your-gcp-project-id acidrain-events-raw
```

### 4. Set Up Dataproc

**Option A: Persistent Cluster**
```bash
gcloud dataproc clusters create acidrain-spark-cluster \
  --region=us-central1 \
  --num-workers=2 \
  --worker-machine-type=n1-standard-4 \
  --image-version=2.1-debian11
```

**Option B: Serverless Dataproc (Recommended)**
```bash
gcloud dataproc batches submit pyspark \
  gs://acidrain-events-raw/spark_jobs/process_batch.py \
  --region=us-central1 \
  --deps-bucket=acidrain-events-raw \
  --py-files=gs://acidrain-events-raw/spark_jobs/config.py,gs://acidrain-events-raw/spark_jobs/schemas.py,gs://acidrain-events-raw/spark_jobs/watermark.py
```

### 5. Create Cloud Scheduler Job

```bash
gcloud scheduler jobs create http acidrain-batch-processor \
  --location=us-central1 \
  --schedule="0 */6 * * *" \
  --uri="https://dataproc.googleapis.com/v1/projects/YOUR_PROJECT/regions/us-central1/batches" \
  --http-method=POST \
  --message-body='{...}' \
  --oauth-service-account-email=SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com
```

## Permissions

Service account needs:
- `roles/storage.objectViewer` on raw bucket (read events)
- `roles/storage.objectCreator` on raw bucket (write quarantine files)
- `roles/bigquery.dataEditor` on dataset
- `roles/dataproc.editor` for job submission

## Monitoring

### Check Watermark
```bash
gsutil cat gs://acidrain-events-raw/_metadata/watermark.json
```

### Check Validation Metrics (from Spark logs)
Look for in Cloud Logging or Dataproc job output:
```
📊 Validation Results:
  Total events: 1250
  ✓ Valid: 1248
  ✗ Invalid: 2
  Validation rate: 99.8%
  Error types: {'ValueError': 1, 'ValidationError': 1}
```

### Inspect Quarantined Events
```bash
# List quarantine files
gsutil ls gs://acidrain-events-raw/quarantine/2025/12/22/

# View quarantine content
gsutil cat gs://acidrain-events-raw/quarantine/2025/12/22/invalid_events_*.json | jq '.'

# Count quarantined events
gsutil cat gs://acidrain-events-raw/quarantine/**/*.json | jq '.invalid_count' | awk '{sum+=$1} END {print sum}'
```

### Alert on High Failure Rate
If validation rate drops below 50%, investigate:
1. Check quarantine files for common error patterns
2. Review recent code changes in frontend
3. Check for API schema changes

### View BigQuery Data
```sql
SELECT 
  DATE(timestamp) as date,
  event_type,
  COUNT(*) as event_count
FROM `acidrain.events`
GROUP BY date, event_type
ORDER BY date DESC, event_count DESC
```

### Analyze Incorrect Attempts
```sql
SELECT 
  attempted,
  closest_match,
  AVG(match_ratio) as avg_completion,
  COUNT(*) as attempts
FROM `acidrain.events`
WHERE event_type = 'word_typed_incorrect'
GROUP BY attempted, closest_match
HAVING COUNT(*) > 10
ORDER BY attempts DESC
```

## Troubleshooting

### No events processed
- Check watermark: May be ahead of actual data
- Verify file paths match: `raw/YYYY/MM/DD/*.json.gz`
- Check Spark logs in Dataproc

### Schema mismatch
- Verify frontend sends all required fields
- Check `web_context` includes country/region
- Ensure timestamp format is ISO 8601

### Duplicate events
- Check watermark is being updated
- Verify event_id is unique
- Use BigQuery deduplication if needed
