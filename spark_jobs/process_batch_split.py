"""
🎯 SCHEDULER ENTRYPOINT - process_batch_split.py

Spark batch job for Acid Rain event processing with SPLIT TABLE design

Architecture:
- Reads gzip-compressed JSONL from GCS
- Validates events with Pydantic
- Writes to TWO BigQuery tables:
  1. sessions - Player info (one row per game session)
  2. attempts - Word attempts (one row per typing attempt)

Best Practice: Never fail entire batch for bad events
- Invalid events → quarantine to GCS
- Valid events → split into sessions + attempts
"""

from datetime import datetime, timezone
import sys
import json
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, array_join

from config import (
    RAW_BUCKET, RAW_PREFIX, QUARANTINE_PREFIX,
    BQ_PROJECT, BQ_DATASET, BQ_TABLE_SESSIONS, BQ_TABLE_ATTEMPTS, SPARK_APP_NAME,
    MAX_VALIDATION_FAILURES_PERCENT
)
from watermark import read_watermark, write_watermark
from validation import validate_events, ValidationResult
from google.cloud import storage


def create_spark_session():
    """Create Spark session with BigQuery connector"""
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
        .getOrCreate()


def quarantine_invalid_events(validation_result: ValidationResult, batch_start_time: datetime):
    """
    Write invalid events to GCS quarantine bucket for manual review
    
    Path: gs://bucket/quarantine/YYYY/MM/DD/invalid_events_{uuid}.json
    """
    if validation_result.invalid_count == 0:
        print("✓ No invalid events to quarantine")
        return
    
    try:
        client = storage.Client(project=BQ_PROJECT)
        bucket = client.bucket(RAW_BUCKET)
        
        year = batch_start_time.year
        month = batch_start_time.month
        day = batch_start_time.day
        file_uuid = uuid.uuid4().hex[:8]
        
        quarantine_path = (
            f"{QUARANTINE_PREFIX}{year}/{month:02d}/{day:02d}/"
            f"invalid_events_{file_uuid}.json"
        )
        
        quarantine_data = {
            "batch_timestamp": batch_start_time.isoformat(),
            "invalid_count": validation_result.invalid_count,
            "error_summary": validation_result.get_error_summary(),
            "invalid_events": validation_result.invalid_events,
            "validation_errors": validation_result.validation_errors
        }
        
        blob = bucket.blob(quarantine_path)
        blob.upload_from_string(
            json.dumps(quarantine_data, indent=2),
            content_type="application/json"
        )
        
        print(f"✓ Quarantined {validation_result.invalid_count} invalid events to:")
        print(f"  gs://{RAW_BUCKET}/{quarantine_path}")
        print(f"  Error summary: {validation_result.get_error_summary()}")
        
    except Exception as e:
        print(f"⚠ Warning: Failed to quarantine invalid events: {e}")
        print("  Continuing with valid events only")


def get_incremental_path(watermark_time):
    """Generate GCS path for files after watermark"""
    base_path = f"gs://{RAW_BUCKET}/{RAW_PREFIX}"
    return f"{base_path}[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/*.json.gz"


def process_events(spark, watermark_time, batch_start_time):
    """
    Main processing logic with SPLIT TABLE design:
    1. Read & validate events
    2. Split into sessions + attempts
    3. Write to two separate BigQuery tables
    """
    
    # Read raw events
    raw_path = get_incremental_path(watermark_time)
    print(f"Reading from: {raw_path}")
    print(f"Watermark: {watermark_time.isoformat()}")
    
    df_raw = spark.read.json(raw_path)
    
    # Validate events
    print("\n📋 Validating events...")
    events_list = df_raw.toJSON().map(lambda x: json.loads(x)).collect()
    
    if not events_list:
        print("No events found to process")
        return 0
    
    validation_result = validate_events(events_list)
    
    print(f"\n📊 Validation Results:")
    print(f"  Total events: {validation_result.total_events}")
    print(f"  ✓ Valid: {validation_result.valid_count}")
    print(f"  ✗ Invalid: {validation_result.invalid_count}")
    print(f"  Validation rate: {validation_result.validation_rate:.1%}")
    
    if validation_result.invalid_count > 0:
        print(f"  Error types: {validation_result.get_error_summary()}")
    
    failure_rate = (1 - validation_result.validation_rate) * 100
    if failure_rate > MAX_VALIDATION_FAILURES_PERCENT:
        print(f"\n⚠️ WARNING: Validation failure rate ({failure_rate:.1f}%) exceeds threshold ({MAX_VALIDATION_FAILURES_PERCENT}%)")
    
    quarantine_invalid_events(validation_result, batch_start_time)
    
    if validation_result.valid_count == 0:
        print("\n⚠️ No valid events to process after validation")
        return 0
    
    print(f"\n✓ Proceeding with {validation_result.valid_count} valid events")
    
    # Convert valid events back to Spark DataFrame
    df = spark.createDataFrame(
        spark.sparkContext.parallelize(validation_result.valid_events)
    )
    
    df = df.withColumn(
        "timestamp_parsed",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    # Filter by watermark
    df_filtered = df.filter(col("timestamp_parsed") > watermark_time)
    event_count = df_filtered.count()
    
    print(f"Events after watermark filter: {event_count}")
    
    if event_count == 0:
        print("No new events to process")
        return 0
    
    # Split into sessions and attempts
    print("\n📊 Splitting into sessions and attempts...")
    
    # ===== TABLE 1: SESSIONS (Player Info) =====
    # Aggregate game_start + game_over events per session
    game_start_events = df_filtered.filter(col("event_type") == "game_start")
    game_over_events = df_filtered.filter(col("event_type") == "game_over")
    
    # Join game_start and game_over by session_id
    sessions_df = game_over_events.alias("go").join(
        game_start_events.alias("gs"),
        col("go.session_id") == col("gs.session_id"),
        "left"
    ).select(
        col("go.session_id"),
        col("gs.timestamp_parsed").alias("game_start_timestamp"),
        col("go.timestamp_parsed").alias("game_over_timestamp"),
        
        # Player context (from web_context)
        col("go.web_context.timezone").alias("timezone"),
        col("go.web_context.language").alias("language"),
        col("go.web_context.country").alias("country"),
        col("go.web_context.region").alias("region"),
        
        # Game performance (from game_over metadata)
        col("go.metadata.final_score").alias("final_score"),
        col("go.metadata.words_typed").alias("words_typed"),
        col("go.metadata.words_missed").alias("words_missed"),
        col("go.metadata.final_speed").alias("final_speed"),
        col("go.metadata.active_play_time_ms").alias("active_play_time_ms"),
        col("go.metadata.page_dwell_time_ms").alias("page_dwell_time_ms"),
        col("go.metadata.idle_time_ms").alias("idle_time_ms"),
        
        current_timestamp().alias("processing_timestamp")
    )
    
    sessions_count = sessions_df.count()
    print(f"  Sessions: {sessions_count}")
    
    # ===== TABLE 2: ATTEMPTS (Word Typing Attempts) =====
    # Only word_typed_correct and word_typed_incorrect events
    from pyspark.sql.functions import coalesce
    
    attempts_df = df_filtered.filter(
        (col("event_type") == "word_typed_correct") | 
        (col("event_type") == "word_typed_incorrect")
    ).select(
        col("event_id").alias("attempt_id"),
        col("session_id"),
        col("timestamp_parsed").alias("timestamp"),
        
        # Was it correct?
        (col("event_type") == "word_typed_correct").alias("was_correct"),
        
        # What was attempted/matched
        # For both correct and incorrect: attempted field contains what user typed
        col("metadata.attempted").alias("attempted_word"),
        # For correct: word = matched word. For incorrect: closest_match = best match
        coalesce(col("metadata.word"), col("metadata.closest_match")).alias("matched_word"),
        col("metadata.time_to_type_ms").alias("time_to_type_ms"),
        col("metadata.current_speed").alias("current_speed"),
        
        # Visible words context (convert array to comma-separated string for BQ)
        array_join(col("metadata.visible_words"), ",").alias("visible_words"),
        col("metadata.visible_words_count").alias("visible_words_count"),
        
        # Partial completion (if incorrect, these are NULL for correct)
        col("metadata.closest_match").alias("closest_match"),
        col("metadata.chars_matched").alias("chars_matched"),
        col("metadata.match_ratio").alias("match_ratio"),
        
        current_timestamp().alias("processing_timestamp")
    )
    
    attempts_count = attempts_df.count()
    print(f"  Attempts: {attempts_count}")
    
    # Write to BigQuery
    print("\n💾 Writing to BigQuery...")
    
    # Write sessions
    if sessions_count > 0:
        sessions_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_SESSIONS}"
        sessions_df.write \
            .format("bigquery") \
            .option("table", sessions_table) \
            .option("temporaryGcsBucket", RAW_BUCKET) \
            .option("partitionField", "game_over_timestamp") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "session_id") \
            .mode("append") \
            .save()
        print(f"  ✓ Sessions: {sessions_count} rows → {sessions_table}")
    
    # Write attempts
    if attempts_count > 0:
        attempts_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_ATTEMPTS}"
        attempts_df.write \
            .format("bigquery") \
            .option("table", attempts_table) \
            .option("temporaryGcsBucket", RAW_BUCKET) \
            .option("partitionField", "timestamp") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "session_id,was_correct") \
            .mode("append") \
            .save()
        print(f"  ✓ Attempts: {attempts_count} rows → {attempts_table}")
    
    return event_count


def main():
    """Main entry point for split table batch job"""
    batch_start_time = datetime.now(timezone.utc)
    
    try:
        watermark_time = read_watermark()
        print(f"\n🚀 Starting batch at {batch_start_time.isoformat()}")
        print(f"📌 Last watermark: {watermark_time.isoformat()}")
        
        spark = create_spark_session()
        event_count = process_events(spark, watermark_time, batch_start_time)
        
        if event_count > 0:
            write_watermark(batch_start_time)
            print(f"\n✓ Updated watermark to {batch_start_time.isoformat()}")
        
        spark.stop()
        print("\n✓ Batch completed successfully")
        print(f"  Processed: {event_count} valid events")
        return 0
        
    except Exception as e:
        print(f"\n✗ Batch failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
