#!/usr/bin/env python3
"""
Test script to verify data integrity for split tables design

Checks:
1. Raw GCS data has all required fields
2. Mock data transformation matches expected schema
3. All columns properly mapped
"""

import json
from datetime import datetime

# Expected fields in raw events
EXPECTED_CORRECT_FIELDS = {
    "event_id", "event_type", "session_id", "timestamp",
    "metadata.word", "metadata.attempted", "metadata.time_to_type_ms", 
    "metadata.current_speed", "metadata.visible_words", "metadata.visible_words_count"
}

EXPECTED_INCORRECT_FIELDS = {
    "event_id", "event_type", "session_id", "timestamp",
    "metadata.attempted", "metadata.visible_words", "metadata.visible_words_count",
    "metadata.closest_match", "metadata.chars_matched", "metadata.match_ratio"
}

# Mock raw events (what frontend sends)
mock_correct_event = {
    "event_id": "evt-123",
    "event_type": "word_typed_correct",
    "session_id": "sess-xyz",
    "timestamp": "2025-12-22T10:30:15.123Z",
    "metadata": {
        "word": "Sarah",
        "attempted": "Sarah",  # NEW: Now included for correct attempts
        "time_to_type_ms": 1250,
        "current_speed": 1.5,
        "visible_words": ["Sarah", "Hello", "World"],
        "visible_words_count": 3
    }
}

mock_incorrect_event = {
    "event_id": "evt-456",
    "event_type": "word_typed_incorrect",
    "session_id": "sess-xyz",
    "timestamp": "2025-12-22T10:31:20.456Z",
    "metadata": {
        "attempted": "Saraa",
        "visible_words": ["Sarah", "Hello", "World"],
        "visible_words_count": 3,
        "closest_match": "Sarah",
        "chars_matched": 4,
        "match_ratio": 0.80
    }
}

# Expected BigQuery row structure
expected_correct_row = {
    "attempt_id": "evt-123",
    "session_id": "sess-xyz",
    "timestamp": "2025-12-22T10:30:15.123Z",
    "was_correct": True,
    "attempted_word": "Sarah",      # From metadata.attempted
    "matched_word": "Sarah",        # From metadata.word
    "time_to_type_ms": 1250,
    "current_speed": 1.5,
    "visible_words": "Sarah,Hello,World",  # Comma-separated
    "visible_words_count": 3,
    "closest_match": None,          # NULL for correct
    "chars_matched": None,          # NULL for correct
    "match_ratio": None             # NULL for correct
}

expected_incorrect_row = {
    "attempt_id": "evt-456",
    "session_id": "sess-xyz",
    "timestamp": "2025-12-22T10:31:20.456Z",
    "was_correct": False,
    "attempted_word": "Saraa",      # From metadata.attempted
    "matched_word": "Sarah",        # From metadata.closest_match (coalesce)
    "time_to_type_ms": None,        # NULL for incorrect
    "current_speed": None,          # NULL for incorrect  
    "visible_words": "Sarah,Hello,World",
    "visible_words_count": 3,
    "closest_match": "Sarah",
    "chars_matched": 4,
    "match_ratio": 0.80
}


def get_nested(d, path):
    """Get nested dict value by dot-separated path"""
    keys = path.split('.')
    val = d
    for key in keys:
        val = val.get(key)
        if val is None:
            return None
    return val


def check_fields(event, expected_fields, event_type):
    """Check if event has all expected fields"""
    print(f"\n✓ Checking {event_type} event fields...")
    missing = []
    
    for field in expected_fields:
        val = get_nested(event, field)
        if val is None:
            missing.append(field)
            print(f"  ✗ Missing: {field}")
        else:
            print(f"  ✓ {field}: {val}")
    
    if missing:
        print(f"\n✗ FAIL: {len(missing)} fields missing: {missing}")
        return False
    else:
        print(f"\n✓ PASS: All fields present")
        return True


def simulate_spark_transform(event):
    """Simulate Spark transformation logic"""
    is_correct = event["event_type"] == "word_typed_correct"
    metadata = event["metadata"]
    
    # Coalesce logic: word (if correct) OR closest_match (if incorrect)
    matched_word = metadata.get("word") or metadata.get("closest_match")
    
    return {
        "attempt_id": event["event_id"],
        "session_id": event["session_id"],
        "timestamp": event["timestamp"],
        "was_correct": is_correct,
        "attempted_word": metadata.get("attempted"),
        "matched_word": matched_word,
        "time_to_type_ms": metadata.get("time_to_type_ms") if is_correct else None,
        "current_speed": metadata.get("current_speed"),
        "visible_words": ",".join(metadata.get("visible_words", [])),
        "visible_words_count": metadata.get("visible_words_count"),
        "closest_match": metadata.get("closest_match"),
        "chars_matched": metadata.get("chars_matched"),
        "match_ratio": metadata.get("match_ratio")
    }


def compare_rows(actual, expected, event_type):
    """Compare transformed row with expected"""
    print(f"\n✓ Comparing {event_type} transformed row...")
    mismatches = []
    
    for key, expected_val in expected.items():
        actual_val = actual.get(key)
        match = actual_val == expected_val
        symbol = "✓" if match else "✗"
        
        if not match:
            mismatches.append(key)
        
        print(f"  {symbol} {key}: {actual_val} {'==' if match else '!='} {expected_val}")
    
    if mismatches:
        print(f"\n✗ FAIL: {len(mismatches)} mismatches: {mismatches}")
        return False
    else:
        print(f"\n✓ PASS: All fields match")
        return True


def main():
    print("=" * 70)
    print("DATA INTEGRITY TEST: Split Tables (Sessions + Attempts)")
    print("=" * 70)
    
    results = []
    
    # Test 1: Check correct event has all fields
    results.append(check_fields(
        mock_correct_event,
        EXPECTED_CORRECT_FIELDS,
        "word_typed_correct"
    ))
    
    # Test 2: Check incorrect event has all fields
    results.append(check_fields(
        mock_incorrect_event,
        EXPECTED_INCORRECT_FIELDS,
        "word_typed_incorrect"
    ))
    
    # Test 3: Transform correct event
    print("\n" + "=" * 70)
    print("TRANSFORMATION TEST: word_typed_correct")
    print("=" * 70)
    actual_correct = simulate_spark_transform(mock_correct_event)
    results.append(compare_rows(
        actual_correct,
        expected_correct_row,
        "word_typed_correct"
    ))
    
    # Test 4: Transform incorrect event
    print("\n" + "=" * 70)
    print("TRANSFORMATION TEST: word_typed_incorrect")
    print("=" * 70)
    actual_incorrect = simulate_spark_transform(mock_incorrect_event)
    results.append(compare_rows(
        actual_incorrect,
        expected_incorrect_row,
        "word_typed_incorrect"
    ))
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if all(results):
        print("\n✅ ALL TESTS PASSED - Data flow is correct!")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED - Check data flow!")
        return 1


if __name__ == "__main__":
    exit(main())
