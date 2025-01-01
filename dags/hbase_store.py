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
    'batch_store_hbase',
    default_args=default_args,
    description='Batch store data from Kafka to HBase via Spark',
    schedule_interval=timedelta(days=1),  # Daily batch storage
    catchup=False,
) as dag:

    store_hbase = BashOperator(
        task_id='run_spark_hbase_ingest',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/kafka_hbase.py'
    )

    store_hbase
