"""
New BigQuery schemas for split table design

Two tables:
1. sessions - Player info (one row per game session)
2. attempts - Word attempts (one row per typing attempt)
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, TimestampType, ArrayType, BooleanType
)


# Sessions table schema (player info)
sessions_schema = StructType([
    StructField("session_id", StringType(), False),  # Primary key
    StructField("game_start_timestamp", TimestampType(), True),
    StructField("game_over_timestamp", TimestampType(), True),
    
    # Player context
    StructField("timezone", StringType(), True),
    StructField("language", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    
    # Game performance
    StructField("final_score", IntegerType(), True),
    StructField("words_typed", IntegerType(), True),  # Correct
    StructField("words_missed", IntegerType(), True),
    StructField("final_speed", FloatType(), True),
    StructField("active_play_time_ms", IntegerType(), True),
    StructField("page_dwell_time_ms", IntegerType(), True),
    StructField("idle_time_ms", IntegerType(), True),
    
    # Processing metadata
    StructField("processing_timestamp", TimestampType(), False),
])


# Attempts table schema (word typing attempts)
attempts_schema = StructType([
    StructField("attempt_id", StringType(), False),  # event_id as PK
    StructField("session_id", StringType(), False),  # Foreign key
    StructField("timestamp", TimestampType(), False),
    
    # Attempt details
    StructField("was_correct", BooleanType(), False),
    StructField("attempted_word", StringType(), True),  # What user typed
    StructField("matched_word", StringType(), True),    # If correct, what word matched
    StructField("time_to_type_ms", IntegerType(), True),  # If correct
    StructField("current_speed", FloatType(), True),    # Speed at attempt
    
    # Context: All words visible on screen (comma-separated string for BigQuery)
    StructField("visible_words", StringType(), True),
    StructField("visible_words_count", IntegerType(), True),
    
    # Partial completion metrics (if incorrect)
    StructField("closest_match", StringType(), True),
    StructField("chars_matched", IntegerType(), True),
    StructField("match_ratio", FloatType(), True),
    
    # Processing metadata
    StructField("processing_timestamp", TimestampType(), False),
])


# BigQuery table definitions (for reference)
SESSIONS_BQ_SCHEMA = """
session_id:STRING,
game_start_timestamp:TIMESTAMP,
game_over_timestamp:TIMESTAMP,
timezone:STRING,
language:STRING,
country:STRING,
region:STRING,
final_score:INTEGER,
words_typed:INTEGER,
words_missed:INTEGER,
final_speed:FLOAT,
active_play_time_ms:INTEGER,
page_dwell_time_ms:INTEGER,
idle_time_ms:INTEGER,
processing_timestamp:TIMESTAMP
"""

ATTEMPTS_BQ_SCHEMA = """
attempt_id:STRING,
session_id:STRING,
timestamp:TIMESTAMP,
was_correct:BOOLEAN,
attempted_word:STRING,
matched_word:STRING,
time_to_type_ms:INTEGER,
current_speed:FLOAT,
visible_words:STRING,
visible_words_count:INTEGER,
closest_match:STRING,
chars_matched:INTEGER,
match_ratio:FLOAT,
processing_timestamp:TIMESTAMP
"""
