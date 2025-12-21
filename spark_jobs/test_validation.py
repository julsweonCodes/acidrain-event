#!/usr/bin/env python3
"""
Test validation logic locally before deploying to Spark

Usage:
  python test_validation.py
"""

import sys
from validation import validate_events

# Sample valid events
valid_events = [
    {
        "event_id": "evt_001",
        "session_id": "sess_abc123",
        "event_type": "game_start",
        "timestamp": "2025-12-22T14:00:00.000Z",
        "web_context": {
            "timezone": "America/Los_Angeles",
            "language": "en",
            "country": "US",
            "region": "CA"
        },
        "metadata": {
            "page_url": "https://example.com/game",
            "user_agent": "Mozilla/5.0...",
            "page_dwell_time_ms": 5000
        }
    },
    {
        "event_id": "evt_002",
        "session_id": "sess_abc123",
        "event_type": "word_spawn",
        "timestamp": "2025-12-22T14:00:05.000Z",
        "metadata": {
            "word": "test",
            "spawn_x": 150.5,
            "initial_speed": 1.2
        }
    },
    {
        "event_id": "evt_003",
        "session_id": "sess_abc123",
        "event_type": "word_typed_correct",
        "timestamp": "2025-12-22T14:00:08.000Z",
        "metadata": {
            "word": "test",
            "time_to_type_ms": 1500,
            "current_speed": 1.3
        }
    }
]

# Sample invalid events
invalid_events = [
    {
        "event_id": "evt_bad_001",
        "session_id": "sess_abc123",
        "event_type": "word_spawn",
        "timestamp": "2025-12-22T14:00:10.000Z",
        "metadata": {
            "word": "",  # ❌ Empty word (min_length=1)
            "spawn_x": 150.5,
            "initial_speed": 1.2
        }
    },
    {
        "event_id": "evt_bad_002",
        "session_id": "sess_abc123",
        "event_type": "word_typed_correct",
        "timestamp": "not-a-valid-timestamp",  # ❌ Invalid ISO 8601
        "metadata": {
            "word": "test",
            "time_to_type_ms": 1500,
            "current_speed": 1.3
        }
    },
    {
        "event_id": "evt_bad_003",
        "session_id": "sess_abc123",
        "event_type": "word_spawn",
        "timestamp": "2025-12-22T14:00:15.000Z",
        "metadata": {
            "word": "test",
            "spawn_x": -10,  # ❌ Negative spawn_x (must be >= 0)
            "initial_speed": 1.2
        }
    },
    {
        "event_id": "evt_bad_004",
        "session_id": "sess_abc123",
        "event_type": "game_over",
        "timestamp": "2025-12-22T14:00:20.000Z",
        "metadata": {
            "final_score": -100,  # ❌ Negative score (must be >= 0)
            "words_typed": 10,
            "words_missed": 5,
            "final_speed": 2.0,
            "active_play_time_ms": 60000
        }
    }
]

# Mix valid and invalid events
all_events = valid_events + invalid_events

print("=" * 60)
print("Testing Validation Logic")
print("=" * 60)

print(f"\nInput: {len(all_events)} events")
print(f"  Expected valid: {len(valid_events)}")
print(f"  Expected invalid: {len(invalid_events)}")

# Run validation
result = validate_events(all_events)

print(f"\n📊 Validation Results:")
print(f"  Total events: {result.total_events}")
print(f"  ✓ Valid: {result.valid_count}")
print(f"  ✗ Invalid: {result.invalid_count}")
print(f"  Validation rate: {result.validation_rate:.1%}")

if result.invalid_count > 0:
    print(f"\n❌ Invalid Events:")
    for error in result.validation_errors:
        print(f"  - {error['event_id']}: {error['error_type']}")
        print(f"    → {error['error_message']}")
    
    print(f"\n📈 Error Summary:")
    for error_type, count in result.get_error_summary().items():
        print(f"  {error_type}: {count}")

# Verify results
assert result.valid_count == len(valid_events), "Valid count mismatch"
assert result.invalid_count == len(invalid_events), "Invalid count mismatch"

print("\n" + "=" * 60)
print("✅ All tests passed!")
print("=" * 60)
