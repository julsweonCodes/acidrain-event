from google.cloud import bigquery
import json

PROJECT_ID = "acidrain-event"
DATASET = "acidrain"

def get_dashboard_data(request):
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
      COUNT(DISTINCT session_id) AS total_sessions,
      COUNT(*) AS total_attempts,
      COUNTIF(was_correct = true) AS correct_attempts
    FROM `{PROJECT_ID}.{DATASET}.attempts`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """

    job = client.query(query)
    row = list(job.result())[0]

    response = {
        "total_sessions": row.total_sessions,
        "total_attempts": row.total_attempts,
        "correct_attempts": row.correct_attempts,
        "accuracy": (
            round(row.correct_attempts / row.total_attempts, 3)
            if row.total_attempts > 0 else 0
        )
    }

    return (
        json.dumps(response),
        200,
        {"Content-Type": "application/json"}
    )
