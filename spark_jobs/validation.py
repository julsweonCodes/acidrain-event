"""
Pydantic validation for Acid Rain events

Best Practice: Never fail entire batch for a few bad events

Instead:
1. Validate each event individually
2. Split into valid vs invalid
3. Continue processing valid events
4. Quarantine invalid events to GCS
5. Log validation errors with metrics
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import json


class EventType(str, Enum):
    """Valid event types"""
    GAME_START = "game_start"
    WORD_SPAWN = "word_spawn"
    WORD_TYPED_CORRECT = "word_typed_correct"
    WORD_TYPED_INCORRECT = "word_typed_incorrect"
    WORD_MISSED = "word_missed"
    GAME_OVER = "game_over"


class WebContext(BaseModel):
    """Session-level web context"""
    timezone: Optional[str] = None
    language: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None


class BaseMetadata(BaseModel):
    """Base metadata present in all events"""
    page_url: Optional[str] = None
    user_agent: Optional[str] = None


class GameStartMetadata(BaseMetadata):
    """game_start specific fields"""
    page_dwell_time_ms: Optional[int] = None


class WordSpawnMetadata(BaseMetadata):
    """word_spawn specific fields"""
    word: str = Field(..., min_length=1, max_length=100)
    spawn_x: float = Field(..., ge=0)
    initial_speed: float = Field(..., gt=0)


class WordTypedCorrectMetadata(BaseMetadata):
    """word_typed_correct specific fields"""
    word: str = Field(..., min_length=1, max_length=100)
    time_to_type_ms: int = Field(..., ge=0)
    current_speed: float = Field(..., gt=0)


class WordTypedIncorrectMetadata(BaseMetadata):
    """word_typed_incorrect specific fields with partial completion"""
    attempted: str = Field(..., max_length=100)
    available_words: List[str] = Field(default_factory=list)
    closest_match: Optional[str] = None
    chars_matched: Optional[int] = Field(None, ge=0)
    match_ratio: Optional[float] = Field(None, ge=0.0, le=1.0)


class WordMissedMetadata(BaseMetadata):
    """word_missed specific fields"""
    word: str = Field(..., min_length=1, max_length=100)
    speed: float = Field(..., gt=0)


class GameOverMetadata(BaseMetadata):
    """game_over specific fields"""
    final_score: int = Field(..., ge=0)
    words_typed: int = Field(..., ge=0)
    words_missed: int = Field(..., ge=0)
    final_speed: float = Field(..., gt=0)
    active_play_time_ms: int = Field(..., ge=0)
    page_dwell_time_ms: Optional[int] = Field(None, ge=0)
    idle_time_ms: Optional[int] = Field(None, ge=0)


class AcidRainEvent(BaseModel):
    """Main event model - validates all event types"""
    event_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    event_type: EventType
    timestamp: str  # ISO 8601 format
    web_context: Optional[WebContext] = None
    metadata: Dict[str, Any]  # Flexible dict, validated separately

    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is valid ISO 8601"""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError(f"Invalid ISO 8601 timestamp: {v}")

    def validate_metadata_by_type(self) -> bool:
        """Validate metadata based on event_type"""
        try:
            metadata_models = {
                EventType.GAME_START: GameStartMetadata,
                EventType.WORD_SPAWN: WordSpawnMetadata,
                EventType.WORD_TYPED_CORRECT: WordTypedCorrectMetadata,
                EventType.WORD_TYPED_INCORRECT: WordTypedIncorrectMetadata,
                EventType.WORD_MISSED: WordMissedMetadata,
                EventType.GAME_OVER: GameOverMetadata,
            }
            
            model_class = metadata_models.get(self.event_type)
            if model_class:
                model_class(**self.metadata)
            return True
        except Exception:
            return False


class ValidationResult:
    """Result of validating a batch of events"""
    
    def __init__(self):
        self.valid_events: List[Dict[str, Any]] = []
        self.invalid_events: List[Dict[str, Any]] = []
        self.validation_errors: List[Dict[str, str]] = []
    
    @property
    def total_events(self) -> int:
        return len(self.valid_events) + len(self.invalid_events)
    
    @property
    def valid_count(self) -> int:
        return len(self.valid_events)
    
    @property
    def invalid_count(self) -> int:
        return len(self.invalid_events)
    
    @property
    def validation_rate(self) -> float:
        """Percentage of valid events (0.0 - 1.0)"""
        if self.total_events == 0:
            return 1.0
        return self.valid_count / self.total_events
    
    def get_error_summary(self) -> Dict[str, int]:
        """Count errors by type"""
        error_counts = {}
        for error in self.validation_errors:
            error_type = error.get('error_type', 'unknown')
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
        return error_counts


def validate_events(events: List[Dict[str, Any]]) -> ValidationResult:
    """
    Validate a list of events, splitting into valid and invalid
    
    Best Practice: Never fail the entire batch
    - Each event validated independently
    - Invalid events captured for quarantine
    - Valid events continue processing
    
    Args:
        events: List of event dictionaries from GCS
    
    Returns:
        ValidationResult with valid/invalid splits and error details
    """
    result = ValidationResult()
    
    for event in events:
        try:
            # Validate structure
            validated = AcidRainEvent(**event)
            
            # Validate metadata for specific event type
            if not validated.validate_metadata_by_type():
                raise ValueError(f"Invalid metadata for {validated.event_type}")
            
            # Valid event
            result.valid_events.append(event)
            
        except Exception as e:
            # Invalid event - quarantine it
            result.invalid_events.append(event)
            result.validation_errors.append({
                'event_id': event.get('event_id', 'unknown'),
                'event_type': event.get('event_type', 'unknown'),
                'error_type': type(e).__name__,
                'error_message': str(e),
                'timestamp': event.get('timestamp', 'unknown')
            })
    
    return result
