# Validation Layer - Best Practices Implementation

## Overview

**Core Principle**: Never fail entire batch for a few bad events

Instead, the pipeline:
1. ✅ Validates each event individually
2. ✅ Splits valid vs invalid events
3. ✅ Continues processing valid events
4. ✅ Quarantines invalid events to GCS
5. ✅ Logs validation errors with metrics

## Implementation

### 1. Pydantic Models (`validation.py`)

Created type-safe models for all 6 event types:
- `game_start`
- `word_spawn`
- `word_typed_correct`
- `word_typed_incorrect` (with partial completion)
- `word_missed`
- `game_over`

**Features**:
- Field-level validation (min/max lengths, numeric ranges)
- ISO 8601 timestamp validation
- Event-type-specific metadata validation
- Descriptive error messages

### 2. Validation Function

```python
def validate_events(events: List[Dict]) -> ValidationResult:
    """
    Validates each event independently:
    - Invalid events → captured for quarantine
    - Valid events → continue processing
    - Validation errors → logged with details
    """
```

**Returns**:
- `valid_events`: List of events passing validation
- `invalid_events`: List of events failing validation
- `validation_errors`: Error details for each failure
- `validation_rate`: Percentage of valid events

### 3. Quarantine Logic

Invalid events written to GCS:
```
gs://acidrain-events-raw/quarantine/
└── YYYY/MM/DD/
    └── invalid_events_{uuid}.json
```

**Quarantine File Contains**:
```json
{
  "batch_timestamp": "2025-12-22T14:30:00Z",
  "invalid_count": 5,
  "error_summary": {
    "ValueError": 3,
    "ValidationError": 2
  },
  "invalid_events": [...],
  "validation_errors": [
    {
      "event_id": "abc123",
      "event_type": "word_spawn",
      "error_type": "ValueError",
      "error_message": "word must be at least 1 character",
      "timestamp": "2025-12-22T14:25:00Z"
    }
  ]
}
```

### 4. Structured Logging

Console output includes:
```
📋 Validating events...

📊 Validation Results:
  Total events: 1250
  ✓ Valid: 1248
  ✗ Invalid: 2
  Validation rate: 99.8%
  Error types: {'ValueError': 1, 'ValidationError': 1}

✓ Quarantined 2 invalid events to:
  gs://acidrain-events-raw/quarantine/2025/12/22/invalid_events_a1b2c3d4.json
  Error summary: {'ValueError': 1, 'ValidationError': 1}

✓ Proceeding with 1248 valid events
```

### 5. Alert Threshold

If validation failure rate exceeds 50%:
```
⚠️ WARNING: Validation failure rate (65.0%) exceeds threshold (50%)
   This may indicate a data quality issue
```

This suggests:
- Frontend code changes breaking schema
- API changes not reflected in validation
- Data corruption issues

## Validation Rules

### All Events
- `event_id`: Required, non-empty string
- `session_id`: Required, non-empty string
- `event_type`: Must be one of 6 valid types
- `timestamp`: Valid ISO 8601 format

### Event-Specific Rules

**word_spawn**:
- `word`: 1-100 characters
- `spawn_x`: ≥ 0
- `initial_speed`: > 0

**word_typed_correct**:
- `word`: 1-100 characters
- `time_to_type_ms`: ≥ 0
- `current_speed`: > 0

**word_typed_incorrect** (with partial completion):
- `attempted`: max 100 characters
- `available_words`: array
- `chars_matched`: ≥ 0 (if present)
- `match_ratio`: 0.0 - 1.0 (if present)

**word_missed**:
- `word`: 1-100 characters
- `speed`: > 0

**game_over**:
- `final_score`: ≥ 0
- `words_typed`: ≥ 0
- `words_missed`: ≥ 0
- `final_speed`: > 0
- `active_play_time_ms`: ≥ 0

## Monitoring Quarantine

### List Quarantine Files
```bash
gsutil ls gs://acidrain-events-raw/quarantine/**/*.json
```

### View Quarantine Content
```bash
gsutil cat gs://acidrain-events-raw/quarantine/2025/12/22/invalid_events_*.json | jq '.'
```

### Count Total Invalid Events
```bash
gsutil cat gs://acidrain-events-raw/quarantine/**/*.json | \
  jq '.invalid_count' | \
  awk '{sum+=$1} END {print sum}'
```

### Group Errors by Type
```bash
gsutil cat gs://acidrain-events-raw/quarantine/**/*.json | \
  jq -r '.validation_errors[] | .error_type' | \
  sort | uniq -c | sort -rn
```

## Benefits

1. **Reliability**: Batch never fails due to a few bad events
2. **Visibility**: Know exactly what went wrong and when
3. **Debuggability**: Invalid events preserved with error context
4. **Metrics**: Track data quality over time
5. **Alerting**: High failure rate triggers investigation
6. **Compliance**: All invalid data quarantined, not dropped

## Error Recovery

When invalid events are found:

1. **Immediate**: Valid events continue processing
2. **Investigation**: Review quarantine files for patterns
3. **Root Cause**: Identify source (frontend bug, API change, etc.)
4. **Fix**: Deploy code changes
5. **Reprocessing**: If needed, fix invalid events and resubmit

## Configuration

In `config.py`:
```python
QUARANTINE_PREFIX = "quarantine/"  # GCS path for invalid events
MAX_VALIDATION_FAILURES_PERCENT = 50  # Alert threshold
```

## Next Steps

1. **Set up alerting**: Monitor validation rate in Cloud Logging
2. **Dashboard**: Track validation metrics over time
3. **Reprocessing**: Build tool to fix and resubmit quarantined events
4. **Schema evolution**: Version validation rules as API evolves
