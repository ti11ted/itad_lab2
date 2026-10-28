"""DAG загрузки Seoul Bike Sharing Demand в raw."""

from urllib.parse import urlsplit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from ingestion.load_raw import load_to_raw

SOURCE_URL = (
    "https://archive.ics.uci.edu/"
    "ml/machine-learning-databases/00560/"
    "SeoulBikeData.csv"
)

SOURCE_FILENAME = "SeoulBikeData.csv"

DATASET_SLUG = "seoul_bike_sharing_demand"

HTTP_CONN_ID = "source_http_conn"

S3_CONN_ID = "s3_conn"


parsed_url = urlsplit(SOURCE_URL)

endpoint = parsed_url.path or "/"

if parsed_url.query:
    endpoint = f"{endpoint}?{parsed_url.query}"


with DAG(
    dag_id="ingest_raw",
    description="Загрузка Seoul Bike Sharing Demand в raw.",
    start_date=days_ago(2),
    schedule="@daily",
    catchup=True,
    tags=["raw", "seoul-bike"],
    max_active_runs=1,
) as dag:

    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=HTTP_CONN_ID,
        endpoint=endpoint,
        method="HEAD",
        poke_interval=60,
        timeout=600,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_filename": SOURCE_FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_to_raw_task
