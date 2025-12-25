from google.cloud import bigquery
import json

PROJECT_ID = "acidrain-event"
DATASET = "acidrain"

def get_dashboard_data(request):
    # --- CORS preflight ---
    if request.method == "OPTIONS":
        return (
            "",
            204,
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            },
        )

    client = bigquery.Client(project=PROJECT_ID)

    # --- Query 1: Top rankings ---
    top_rankings_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.sessions`
    ORDER BY final_score DESC
    LIMIT 10
    """

    # --- Query 2: Word statistics ---
    word_stats_query = f"""
    SELECT
      b.lang,
      a.intended_word,
      COUNT(a.intended_word) AS cnt,
      AVG(a.match_ratio) AS avg_match_ratio
    FROM `{PROJECT_ID}.{DATASET}.attempts` a
    INNER JOIN `{PROJECT_ID}.{DATASET}.word_catalog` b
      ON a.intended_word = b.word
    WHERE a.intended_word IS NOT NULL
    GROUP BY a.intended_word, b.lang
    ORDER BY b.lang, cnt DESC
    """

    top_rankings_job = client.query(top_rankings_query)
    word_stats_job = client.query(word_stats_query)

    top_rankings = [dict(row) for row in top_rankings_job.result()]
    word_stats = [dict(row) for row in word_stats_job.result()]

    response = {
        "top_rankings": top_rankings,
        "word_stats": word_stats,
    }

    return (
        json.dumps(response),
        200,
        {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
    )
