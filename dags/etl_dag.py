# etl_dag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
with DAG('pakwheels_etl_pipeline',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    # Task 1: Run Kafka Producer (scraper)
    run_kafka_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python /opt/airflow/producer/kafka_producer.py'
    )

    # Task 2: Run Spark Consumer (streamer)
    run_spark_streamer = BashOperator(
        task_id='run_spark_streamer',
        bash_command='spark-submit /opt/airflow/spark/spark_streamer.py'
    )

    run_kafka_producer >> run_spark_streamer
