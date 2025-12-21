# Setup Guide: Split Tables (Sessions + Attempts)

## Overview

This is the **new two-table design** that separates player info from attempt details:

**OLD (single table):**
- ❌ `events` - All events mixed together, lots of NULL columns

**NEW (split tables):**
- ✅ `sessions` - Player info (one row per game)
- ✅ `attempts` - Word typing attempts (one row per typing attempt)

## Why Split?

**Better for analytics:**
- **Player analytics**: Query sessions table for location, scores, right/wrong counts
- **Attempt analytics**: Query attempts table for word-level patterns, visible words, mistakes
- **Cleaner schema**: No more mixing different event types in one table
- **Performance**: Smaller tables, better partitioning/clustering

## Table Structures

### 1. Sessions Table (Player Info)

**One row per game session**

```sql
CREATE TABLE `acidrain-event.acidrain_data.sessions` (
  -- Identifiers
  session_id STRING NOT NULL,
  game_start_timestamp TIMESTAMP,
  game_over_timestamp TIMESTAMP NOT NULL,
  
  -- Player context
  timezone STRING,
  language STRING,
  country STRING,
  region STRING,
  
  -- Game performance
  final_score INT64,
  words_typed INT64,      -- Correct words
  words_missed INT64,
  final_speed FLOAT64,
  active_play_time_ms INT64,
  page_dwell_time_ms INT64,
  idle_time_ms INT64,
  
  -- Processing
  processing_timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(game_over_timestamp)
CLUSTER BY session_id;
```

**Example queries:**
```sql
-- Top countries by player count
SELECT country, COUNT(DISTINCT session_id) as players
FROM `acidrain-event.acidrain_data.sessions`
GROUP BY country
ORDER BY players DESC;

-- Average scores by language
SELECT language, AVG(final_score) as avg_score
FROM `acidrain-event.acidrain_data.sessions`
GROUP BY language;
```

### 2. Attempts Table (Word Typing Attempts)

**One row per typing attempt (correct or incorrect)**

```sql
CREATE TABLE `acidrain-event.acidrain_data.attempts` (
  -- Identifiers
  attempt_id STRING NOT NULL,
  session_id STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  
  -- Attempt details
  was_correct BOOL NOT NULL,
  attempted_word STRING,
  matched_word STRING,
  time_to_type_ms INT64,
  current_speed FLOAT64,
  
  -- Context: All words visible on screen
  visible_words STRING,          -- Comma-separated list
  visible_words_count INT64,
  
  -- Partial completion (if incorrect)
  closest_match STRING,
  chars_matched INT64,
  match_ratio FLOAT64,
  
  -- Processing
  processing_timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY session_id, was_correct;
```

**Example queries:**
```sql
-- Most common mistakes
SELECT attempted_word, closest_match, COUNT(*) as mistakes
FROM `acidrain-event.acidrain_data.attempts`
WHERE was_correct = FALSE
GROUP BY attempted_word, closest_match
ORDER BY mistakes DESC
LIMIT 20;

-- Average visible words when making mistakes
SELECT AVG(visible_words_count) as avg_visible
FROM `acidrain-event.acidrain_data.attempts`
WHERE was_correct = FALSE;

-- Join sessions + attempts
SELECT s.country, a.was_correct, COUNT(*) as count
FROM `acidrain-event.acidrain_data.sessions` s
JOIN `acidrain-event.acidrain_data.attempts` a
  ON s.session_id = a.session_id
GROUP BY s.country, a.was_correct;
```

## Setup Steps

### 1. Create BigQuery Tables

```bash
# Sessions table
bq mk \
  --table \
  --time_partitioning_field game_over_timestamp \
  --time_partitioning_type DAY \
  --clustering_fields session_id \
  acidrain-event:acidrain_data.sessions \
  session_id:STRING,game_start_timestamp:TIMESTAMP,game_over_timestamp:TIMESTAMP,timezone:STRING,language:STRING,country:STRING,region:STRING,final_score:INTEGER,words_typed:INTEGER,words_missed:INTEGER,final_speed:FLOAT,active_play_time_ms:INTEGER,page_dwell_time_ms:INTEGER,idle_time_ms:INTEGER,processing_timestamp:TIMESTAMP

# Attempts table
bq mk \
  --table \
  --time_partitioning_field timestamp \
  --time_partitioning_type DAY \
  --clustering_fields session_id,was_correct \
  acidrain-event:acidrain_data.attempts \
  attempt_id:STRING,session_id:STRING,timestamp:TIMESTAMP,was_correct:BOOLEAN,attempted_word:STRING,matched_word:STRING,time_to_type_ms:INTEGER,current_speed:FLOAT,visible_words:STRING,visible_words_count:INTEGER,closest_match:STRING,chars_matched:INTEGER,match_ratio:FLOAT,processing_timestamp:TIMESTAMP
```

### 2. Update Cloud Scheduler

Use the **new split table job**: `process_batch_split.py`

```bash
gcloud scheduler jobs update http acidrain-batch-job \
  --location=us-central1 \
  --schedule="0 */6 * * *" \
  --uri="https://dataproc.googleapis.com/v1/projects/acidrain-event/regions/us-central1/jobs:submit" \
  --message-body='{
    "job": {
      "reference": { "jobId": "acidrain-batch-'$(date +%s)'" },
      "placement": { "clusterName": "acidrain-cluster" },
      "pysparkJob": {
        "mainPythonFileUri": "gs://acidrain-events-raw/spark_jobs/process_batch_split.py",
        "pythonFileUris": [
          "gs://acidrain-events-raw/spark_jobs/config.py",
          "gs://acidrain-events-raw/spark_jobs/watermark.py",
          "gs://acidrain-events-raw/spark_jobs/validation.py",
          "gs://acidrain-events-raw/spark_jobs/schemas_split.py"
        ]
      }
    }
  }' \
  --oauth-service-account-email=acidrain-dataproc@acidrain-event.iam.gserviceaccount.com
```

### 3. Deploy New Code

```bash
cd spark_jobs
python deploy.py
```

This uploads:
- ✅ `process_batch_split.py` (new main job)
- ✅ `schemas_split.py` (new table schemas)
- ✅ `config.py`, `watermark.py`, `validation.py` (existing)

### 4. Test Manually

```bash
gcloud dataproc jobs submit pyspark \
  gs://acidrain-events-raw/spark_jobs/process_batch_split.py \
  --cluster=acidrain-cluster \
  --region=us-central1 \
  --py-files=gs://acidrain-events-raw/spark_jobs/config.py,gs://acidrain-events-raw/spark_jobs/watermark.py,gs://acidrain-events-raw/spark_jobs/validation.py,gs://acidrain-events-raw/spark_jobs/schemas_split.py \
  --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.0.jar
```

## Monitoring

### Check Sessions Table
```bash
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) as total_sessions,
          COUNT(DISTINCT country) as countries,
          SUM(words_typed) as total_correct,
          SUM(words_missed) as total_missed
   FROM `acidrain-event.acidrain_data.sessions`'
```

### Check Attempts Table
```bash
bq query --use_legacy_sql=false \
  'SELECT was_correct,
          COUNT(*) as attempts,
          AVG(visible_words_count) as avg_visible
   FROM `acidrain-event.acidrain_data.attempts`
   GROUP BY was_correct'
```

### Verify Data Completeness
```bash
# Every session should have at least one attempt
bq query --use_legacy_sql=false \
  'SELECT s.session_id
   FROM `acidrain-event.acidrain_data.sessions` s
   LEFT JOIN `acidrain-event.acidrain_data.attempts` a
     ON s.session_id = a.session_id
   WHERE a.session_id IS NULL'
```

## Migration from Old Events Table

If you have existing data in the old `events` table:

```sql
-- Migrate to sessions
INSERT INTO `acidrain-event.acidrain_data.sessions`
SELECT DISTINCT
  session_id,
  MIN(CASE WHEN event_type = 'game_start' THEN timestamp END) as game_start_timestamp,
  MAX(CASE WHEN event_type = 'game_over' THEN timestamp END) as game_over_timestamp,
  ANY_VALUE(web_context.timezone) as timezone,
  ANY_VALUE(web_context.language) as language,
  ANY_VALUE(web_context.country) as country,
  ANY_VALUE(web_context.region) as region,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.final_score END) as final_score,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.words_typed END) as words_typed,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.words_missed END) as words_missed,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.final_speed END) as final_speed,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.active_play_time_ms END) as active_play_time_ms,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.page_dwell_time_ms END) as page_dwell_time_ms,
  ANY_VALUE(CASE WHEN event_type = 'game_over' THEN metadata.idle_time_ms END) as idle_time_ms,
  CURRENT_TIMESTAMP() as processing_timestamp
FROM `acidrain-event.acidrain_data.events`
GROUP BY session_id;

-- Migrate to attempts
INSERT INTO `acidrain-event.acidrain_data.attempts`
SELECT
  event_id as attempt_id,
  session_id,
  timestamp,
  (event_type = 'word_typed_correct') as was_correct,
  metadata.attempted as attempted_word,
  metadata.word as matched_word,
  metadata.time_to_type_ms,
  metadata.current_speed,
  ARRAY_TO_STRING(metadata.visible_words, ',') as visible_words,
  metadata.visible_words_count,
  metadata.closest_match,
  metadata.chars_matched,
  metadata.match_ratio,
  CURRENT_TIMESTAMP() as processing_timestamp
FROM `acidrain-event.acidrain_data.events`
WHERE event_type IN ('word_typed_correct', 'word_typed_incorrect');
```

## Done! 🎉

Your data is now split into two focused tables:
- 📊 `sessions` - Analyze player behavior, locations, scores
- 🎯 `attempts` - Analyze word-level patterns, mistakes, visible words

Much cleaner for analytics!
