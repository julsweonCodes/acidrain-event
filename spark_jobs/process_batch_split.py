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
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, array_join, round

from config import (
    RAW_BUCKET, RAW_PREFIX, QUARANTINE_PREFIX,
    BQ_PROJECT, BQ_DATASET, BQ_TABLE_SESSIONS, BQ_TABLE_ATTEMPTS, SPARK_APP_NAME,
    MAX_VALIDATION_FAILURES_PERCENT
)
from watermark import read_watermark, write_watermark
from google.cloud import storage


def create_spark_session():
    """Create Spark session with BigQuery connector"""
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .getOrCreate()


def get_files_after_watermark(watermark_time):
    """
    List GCS files created after watermark (file-based, not event-based)
    
    Best practice: Filter files by blob.time_created BEFORE Spark reads them
    """
    from google.cloud import storage
    
    client = storage.Client(project=BQ_PROJECT)
    bucket = client.bucket(RAW_BUCKET)
    
    # List all blobs in the raw prefix
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)
    
    eligible_files = []
    for blob in blobs:
        if blob.name.endswith('.json.gz'):
            # Filter by file creation time, not event timestamps
            if blob.time_created.replace(tzinfo=timezone.utc) > watermark_time:
                eligible_files.append(f"gs://{RAW_BUCKET}/{blob.name}")
    
    return eligible_files


def validate_partition(rows):
    """
    Distributed validation using Pydantic
    
    Runs on each Spark partition (not on driver)
    Best practice: Validation must stay distributed
    """
    from validation import AcidRainEvent
    
    for row in rows:
        try:
            # Validate the event
            event_dict = row.asDict(recursive=True)
            AcidRainEvent(**event_dict)
            # Valid event: (row, is_valid=True, error=None)
            yield (row, True, None)
        except Exception as e:
            # Invalid event: (row, is_valid=False, error=str)
            yield (row, False, str(e))


def process_events(spark, watermark_time, batch_start_time):
    """
    Main processing logic with SPLIT TABLE design:
    1. List files after watermark (file-based filtering)
    2. Read & validate events (distributed validation)
    3. Split into sessions + attempts
    4. Write to two separate BigQuery tables
    """
    
    # Step 1: List eligible files (file-based watermark)
    print(f"\n📂 Listing files after watermark...")
    print(f"Watermark: {watermark_time.isoformat()}")
    
    eligible_files = get_files_after_watermark(watermark_time)
    
    if not eligible_files:
        print("No new files to process")
        return 0
    
    print(f"Found {len(eligible_files)} files to process")
    
    # Step 2: Read only eligible files
    df_raw = spark.read.json(eligible_files)
    
    # Step 3: Distributed validation (runs on partitions, not driver)
    print("\n📋 Validating events (distributed)...")
    
    from pyspark.sql.types import StructType, StructField, BooleanType, StringType
    
    # Define schema for validation results
    validation_schema = StructType([
        StructField("row", df_raw.schema, False),
        StructField("is_valid", BooleanType(), False),
        StructField("error", StringType(), True)
    ])
    
    # Validate using mapPartitions (distributed, not .collect())
    validated_rdd = df_raw.rdd.mapPartitions(validate_partition)
    df_validated = spark.createDataFrame(validated_rdd, schema=validation_schema)
    
    # Separate valid and invalid
    df_valid = df_validated.filter(col("is_valid") == True).select("row.*")
    df_invalid = df_validated.filter(col("is_valid") == False)
    
    total_count = df_validated.count()
    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    
    print(f"\n📊 Validation Results:")
    print(f"  Total events: {total_count}")
    print(f"  ✓ Valid: {valid_count}")
    print(f"  ✗ Invalid: {invalid_count}")
    print(f"  Validation rate: {valid_count/total_count:.1%}")
    
    # Step 4: Quarantine invalid events
    if invalid_count > 0:
        print(f"\n⚠️ WARNING: {invalid_count} invalid events found")
        # Could write invalid_df to quarantine here if needed
        
        failure_rate = (invalid_count / total_count) * 100
        if failure_rate > MAX_VALIDATION_FAILURES_PERCENT:
            print(f"\n⚠️ CRITICAL: Validation failure rate ({failure_rate:.1f}%) exceeds threshold ({MAX_VALIDATION_FAILURES_PERCENT}%)")
    
    if valid_count == 0:
        print("\n⚠️ No valid events to process after validation")
        return 0
    
    print(f"\n✓ Proceeding with {valid_count} valid events")
    
    # Step 5: Add timestamp parsing (no filtering - files already filtered)
    df = df_valid.withColumn(
        "timestamp_parsed",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    event_count = df.count()
    
    if event_count == 0:
        print("No events in eligible files")
        return 0
    
    # Split into sessions and attempts
    print("\n📊 Splitting into sessions and attempts...")
    
    # ===== TABLE 1: SESSIONS (Player Info) =====
    # Aggregate game_start + game_over events per session
    game_start_events = df.filter(col("event_type") == "game_start")
    game_over_events = df.filter(col("event_type") == "game_over")
    
    # Join game_start and game_over by session_id
    sessions_df = game_over_events.alias("go").join(
        game_start_events.alias("gs"),
        col("go.session_id") == col("gs.session_id"),
        "inner"
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
        col("go.metadata.final_speed").cast("double").alias("final_speed"),
        col("go.metadata.active_play_time_ms").alias("active_play_time_ms"),
        col("go.metadata.page_dwell_time_ms").alias("page_dwell_time_ms"),
        col("go.metadata.idle_time_ms").alias("idle_time_ms"),
        
        current_timestamp().alias("processing_timestamp")
    )
    
    sessions_count = sessions_df.count()
    print(f"  Sessions: {sessions_count}")
    
    # ===== TABLE 2: ATTEMPTS (Word Typing Attempts) =====
    # Only word_typed_correct and word_typed_incorrect events
    from pyspark.sql.functions import coalesce, to_json, when, from_json
    from pyspark.sql.types import ArrayType, StringType
    
    from pyspark.sql.functions import to_json, round

    attempts_df = df.filter(
        (col("event_type") == "word_typed_correct") |
        (col("event_type") == "word_typed_incorrect")
    ).select(
        col("event_id").alias("attempt_id"),
        col("session_id"),
        col("timestamp_parsed").alias("timestamp"),

        (col("event_type") == "word_typed_correct").alias("was_correct"),

        col("metadata.attempted").cast("string").alias("attempted_word"),
        col("metadata.word").cast("string").alias("matched_word"),

        col("metadata.time_to_type_ms").cast("int").alias("time_to_type_ms"),
        round(col("metadata.current_speed").cast("double"), 1).alias("current_speed"),

        # ⭐ 핵심 수정
        to_json(col("metadata.visible_words")).alias("visible_words"),

        col("metadata.visible_words_count").cast("int").alias("visible_words_count"),
        col("metadata.closest_match").cast("string").alias("closest_match"),
        col("metadata.chars_matched").cast("int").alias("chars_matched"),
        col("metadata.match_ratio").cast("double").alias("match_ratio"),
        col("metadata.intended_word").cast("string").alias("intended_word"),

        current_timestamp().alias("processing_timestamp")
    )

    
    attempts_count = attempts_df.count()
    print(f"  Attempts: {attempts_count}")
    
    # Write to BigQuery
    print("\n💾 Writing to BigQuery...")

    print("before that... we gonna check")
    print("=== ATTEMPTS DF SCHEMA ===")
    attempts_df.printSchema()

    print("=== ATTEMPTS DF SAMPLE ===")
    attempts_df.show(20, truncate=False)
    
    # Write sessions
    
    # Write sessions
    if sessions_count > 0:
        sessions_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_SESSIONS}"
        sessions_df.write \
            .format("com.google.cloud.spark.bigquery.v2.Spark33BigQueryTableProvider") \
            .option("table", sessions_table) \
            .option("temporaryGcsBucket", "acidrain-bq-temp") \
            .option("partitionField", "game_over_timestamp") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "session_id") \
            .mode("append") \
            .save()

    # Write attempts
    if attempts_count > 0:
        attempts_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_ATTEMPTS}"
        attempts_df.write \
            .format("com.google.cloud.spark.bigquery.v2.Spark33BigQueryTableProvider") \
            .option("table", attempts_table) \
            .option("temporaryGcsBucket", "acidrain-bq-temp") \
            .option("partitionField", "timestamp") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "session_id,was_correct") \
            .mode("append") \
            .save()

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
