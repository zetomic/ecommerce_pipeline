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
    'real_time_eda',
    default_args=default_args,
    description='Run Spark EDA job for real-time analytics',
    schedule_interval=None,  # Trigger manually or via other DAGs
    catchup=False,
) as dag:

    run_eda = BashOperator(
        task_id='run_spark_eda',
        bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 eda.py'
    )

    run_eda
