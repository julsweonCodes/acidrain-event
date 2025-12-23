import time
from google.cloud import dataproc_v1

REGION = "us-central1"
PROJECT_ID = "acidrain-event"

MAIN_PYSPARK_URI = "gs://acidrain-events-raw/spark_jobs/process_batch_split.py"

PY_FILES = [
    "gs://acidrain-events-raw/spark_jobs/config.py",
    "gs://acidrain-events-raw/spark_jobs/schemas_split.py",
    "gs://acidrain-events-raw/spark_jobs/watermark.py",
    "gs://acidrain-events-raw/spark_jobs/validation.py",
]


BATCH_SA = "acidrain-batch-sa@acidrain-event.iam.gserviceaccount.com"
SUBNETWORK_URI = "projects/acidrain-event/regions/us-central1/subnetworks/default"


def trigger_batch(request):
    """
    HTTP Cloud Function (2nd gen)
    - Called by Cloud Scheduler
    - Creates Dataproc Serverless Batch
    """
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com"}
    )

    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    batch_id = f"acidrain-batch-{int(time.time())}"

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": MAIN_PYSPARK_URI,
            "python_file_uris": PY_FILES,
        },
        "runtime_config": {
            "properties": {
                "spark.executor.memory": "4g",
                "spark.executor.cores": "4",
                "spark.dynamicAllocation.enabled": "true",
            }
        },
        "environment_config": {
            "execution_config": {
                "service_account": BATCH_SA,
                "subnetwork_uri": SUBNETWORK_URI,
            }
        },
    }

    op = client.create_batch(parent=parent, batch=batch, batch_id=batch_id)
    # Don't wait; just return immediately
    return (f"Started Dataproc batch: {batch_id}\nOperation: {op.operation.name}\n", 200)
