"""
Configuration for Acid Rain Spark batch jobs
"""

# GCS Storage
RAW_BUCKET = "acidrain-events-raw"
RAW_PREFIX = "raw/"  # raw/YYYY/MM/DD/events_{uuid}.json.gz
QUARANTINE_PREFIX = "quarantine/"  # quarantine/YYYY/MM/DD/invalid_events_{uuid}.json
WATERMARK_PATH = "_metadata/watermark.json"

# BigQuery
BQ_PROJECT = "acidrain-event"  # Replace with your GCP project
BQ_DATASET = "acidrain"
BQ_TABLE_SESSIONS = "sessions"  # Player info (one row per game session)
BQ_TABLE_ATTEMPTS = "attempts"  # Word attempts (one row per typing attempt)

# Spark
SPARK_APP_NAME = "acidrain-batch-processor"

# Processing
BATCH_INTERVAL_HOURS = 3
MAX_PARTITION_LOOKBACK_DAYS = 7  # How far back to scan for new files

# Validation
MAX_VALIDATION_FAILURES_PERCENT = 50  # Alert if >50% events are invalid
