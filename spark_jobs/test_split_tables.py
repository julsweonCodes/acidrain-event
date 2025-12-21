"""
Test script to verify split table logic before deploying

This script:
1. Reads sample events from GCS
2. Simulates the split table transformation
3. Shows what data would go to sessions vs attempts tables
"""

import json
import gzip
from google.cloud import storage
from datetime import datetime

def fetch_sample_events(bucket_name, prefix, max_files=1):
    """Fetch sample events from GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=max_files))
    
    if not blobs:
        print(f"No files found in gs://{bucket_name}/{prefix}")
        return []
    
    print(f"\n📁 Reading from: gs://{bucket_name}/{blobs[0].name}")
    
    all_events = []
    for blob in blobs:
        content = blob.download_as_bytes()
        decompressed = gzip.decompress(content).decode('utf-8')
        
        for line in decompressed.strip().split('\n'):
            if line:
                all_events.append(json.loads(line))
    
    return all_events


def split_events(events):
    """Split events into sessions and attempts"""
    
    # Group events by session_id
    sessions_map = {}
    attempts_list = []
    
    for event in events:
        session_id = event.get('session_id')
        event_type = event.get('event_type')
        
        # Initialize session data structure
        if session_id not in sessions_map:
            sessions_map[session_id] = {
                'session_id': session_id,
                'game_start_timestamp': None,
                'game_over_timestamp': None,
                'timezone': None,
                'language': None,
                'country': None,
                'region': None,
                'final_score': None,
                'words_typed': None,
                'words_missed': None,
                'final_speed': None,
                'active_play_time_ms': None,
                'page_dwell_time_ms': None,
                'idle_time_ms': None,
            }
        
        session = sessions_map[session_id]
        
        # Extract session info
        if event_type == 'game_start':
            session['game_start_timestamp'] = event.get('timestamp')
            
        elif event_type == 'game_over':
            session['game_over_timestamp'] = event.get('timestamp')
            web_context = event.get('web_context', {})
            metadata = event.get('metadata', {})
            
            session['timezone'] = web_context.get('timezone')
            session['language'] = web_context.get('language')
            session['country'] = web_context.get('country')
            session['region'] = web_context.get('region')
            
            session['final_score'] = metadata.get('final_score')
            session['words_typed'] = metadata.get('words_typed')
            session['words_missed'] = metadata.get('words_missed')
            session['final_speed'] = metadata.get('final_speed')
            session['active_play_time_ms'] = metadata.get('active_play_time_ms')
            session['page_dwell_time_ms'] = metadata.get('page_dwell_time_ms')
            session['idle_time_ms'] = metadata.get('idle_time_ms')
        
        # Extract attempt info
        elif event_type in ['word_typed_correct', 'word_typed_incorrect']:
            metadata = event.get('metadata', {})
            visible_words = metadata.get('visible_words', [])
            
            attempt = {
                'attempt_id': event.get('event_id'),
                'session_id': session_id,
                'timestamp': event.get('timestamp'),
                'was_correct': event_type == 'word_typed_correct',
                'attempted_word': metadata.get('attempted'),
                'matched_word': metadata.get('word'),
                'time_to_type_ms': metadata.get('time_to_type_ms'),
                'current_speed': metadata.get('current_speed'),
                'visible_words': ','.join(visible_words) if visible_words else None,
                'visible_words_count': metadata.get('visible_words_count'),
                'closest_match': metadata.get('closest_match'),
                'chars_matched': metadata.get('chars_matched'),
                'match_ratio': metadata.get('match_ratio'),
            }
            attempts_list.append(attempt)
    
    sessions_list = list(sessions_map.values())
    return sessions_list, attempts_list


def print_sessions_sample(sessions, limit=3):
    """Print sample sessions data"""
    print(f"\n{'='*80}")
    print(f"📊 SESSIONS TABLE (Player Info)")
    print(f"{'='*80}")
    print(f"Total sessions: {len(sessions)}")
    
    for i, session in enumerate(sessions[:limit]):
        print(f"\n--- Session {i+1} ---")
        print(f"  session_id: {session['session_id']}")
        print(f"  game_start: {session['game_start_timestamp']}")
        print(f"  game_over: {session['game_over_timestamp']}")
        print(f"  country: {session['country']}, region: {session['region']}")
        print(f"  language: {session['language']}")
        print(f"  final_score: {session['final_score']}")
        print(f"  words_typed: {session['words_typed']}, words_missed: {session['words_missed']}")
        print(f"  final_speed: {session['final_speed']}")


def print_attempts_sample(attempts, limit=5):
    """Print sample attempts data"""
    print(f"\n{'='*80}")
    print(f"🎯 ATTEMPTS TABLE (Word Typing Attempts)")
    print(f"{'='*80}")
    print(f"Total attempts: {len(attempts)}")
    
    correct_count = sum(1 for a in attempts if a['was_correct'])
    incorrect_count = len(attempts) - correct_count
    print(f"  ✓ Correct: {correct_count}")
    print(f"  ✗ Incorrect: {incorrect_count}")
    
    # Show incorrect attempts first (more interesting)
    incorrect_attempts = [a for a in attempts if not a['was_correct']][:limit]
    print(f"\n--- Sample INCORRECT attempts ---")
    for i, attempt in enumerate(incorrect_attempts):
        print(f"\n  Attempt {i+1}:")
        print(f"    matched_word: {attempt['matched_word']}")  # word name
        print(f"    attempted_word: {attempt['attempted_word']}")  # what user typed
        print(f"    was_correct: {attempt['was_correct']}")  # FALSE
        print(f"    chars_matched: {attempt['chars_matched']}")  # how many chars matched
        print(f"    match_ratio: {attempt['match_ratio']}")
        print(f"    visible_words: {attempt['visible_words']}")
        print(f"    visible_words_count: {attempt['visible_words_count']}")
    
    # Show correct attempts
    correct_attempts = [a for a in attempts if a['was_correct']][:3]
    print(f"\n--- Sample CORRECT attempts ---")
    for i, attempt in enumerate(correct_attempts):
        print(f"\n  Attempt {i+1}:")
        print(f"    matched_word: {attempt['matched_word']}")
        print(f"    attempted_word: {attempt['attempted_word']}")
        print(f"    was_correct: {attempt['was_correct']}")  # TRUE
        print(f"    time_to_type_ms: {attempt['time_to_type_ms']}")
        print(f"    visible_words: {attempt['visible_words']}")
        print(f"    visible_words_count: {attempt['visible_words_count']}")


def main():
    """Main test logic"""
    print("🧪 Testing Split Table Logic\n")
    
    # Fetch sample events
    bucket = "acidrain-events-raw"
    prefix = "raw/2025/12/21/"  # Most recent day
    
    print(f"Fetching sample events from gs://{bucket}/{prefix}")
    events = fetch_sample_events(bucket, prefix, max_files=1)
    
    if not events:
        print("❌ No events found. Make sure you have recent data in GCS.")
        return
    
    print(f"✓ Loaded {len(events)} events")
    
    # Count event types
    event_types = {}
    for event in events:
        event_type = event.get('event_type', 'unknown')
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    print("\nEvent type breakdown:")
    for event_type, count in sorted(event_types.items()):
        print(f"  {event_type}: {count}")
    
    # Split into sessions and attempts
    sessions, attempts = split_events(events)
    
    # Print samples
    print_sessions_sample(sessions)
    print_attempts_sample(attempts)
    
    # Validation
    print(f"\n{'='*80}")
    print("✅ VALIDATION")
    print(f"{'='*80}")
    
    # Check if all sessions have game_over
    sessions_with_game_over = sum(1 for s in sessions if s['game_over_timestamp'])
    print(f"Sessions with game_over: {sessions_with_game_over}/{len(sessions)}")
    
    # Check if attempts have visible_words
    attempts_with_visible = sum(1 for a in attempts if a['visible_words'])
    print(f"Attempts with visible_words: {attempts_with_visible}/{len(attempts)}")
    
    # Check incorrect attempts have partial completion metrics
    incorrect_attempts = [a for a in attempts if not a['was_correct']]
    with_closest_match = sum(1 for a in incorrect_attempts if a['closest_match'])
    print(f"Incorrect attempts with closest_match: {with_closest_match}/{len(incorrect_attempts)}")
    
    print("\n✓ Test complete! Review the output above.")
    print("\nIf this looks good, proceed with:")
    print("  1. cd spark_jobs && python deploy.py")
    print("  2. Create BigQuery tables (see SETUP_SPLIT_TABLES.md)")
    print("  3. Run full Spark batch job")


if __name__ == "__main__":
    main()
