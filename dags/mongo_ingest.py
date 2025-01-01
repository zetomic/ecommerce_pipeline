from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'batch_ingest_to_mongo',
    default_args=default_args,
    description='Batch ingest data from Kafka to MongoDB via NiFi',
    schedule_interval=timedelta(days=1),  # Daily ingestion
    catchup=False,
) as dag:

    ingest_task = BashOperator(
        task_id='trigger_nifi_ingest',
        bash_command='curl -X POST http://nifi:8080/nifi-api/processors/<PROCESSOR_ID>/run-status -H "Content-Type: application/json" -d \'{"revision":{"version":0},"state":"RUNNING"}\''
    )

    ingest_task
